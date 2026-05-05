//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/offline_dedup_install_sink.h"

#include <algorithm>
#include <cinttypes>
#include <memory>
#include <utility>

#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dedup/dedup_context.h"
#include "db/dedup/dedup_flush_adapter.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "rocksdb/file_system.h"
#include "rocksdb/table.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

OfflineDedupInstallSink::OfflineDedupInstallSink(
    DBImpl* db, ColumnFamilyData* cfd, uint64_t uvl_file_number,
    std::shared_ptr<Logger> info_log)
    : db_(db),
      cfd_(cfd),
      uvl_file_number_(uvl_file_number),
      info_log_(std::move(info_log)) {}

OfflineDedupInstallSink::~OfflineDedupInstallSink() = default;

Status OfflineDedupInstallSink::EmitValue(const Slice& key, SequenceNumber seq,
                                          const DGDResult& result) {
  PendingEntry e;
  e.user_key.assign(key.data(), key.size());
  e.seq = seq;
  e.type = kTypeBlobIndex;
  EncodeUvlBlobIndex(result, &e.value);
  pending_.push_back(std::move(e));
  return Status::OK();
}

Status OfflineDedupInstallSink::EmitDelete(const Slice& key, SequenceNumber seq,
                                           ValueType type) {
  PendingEntry e;
  e.user_key.assign(key.data(), key.size());
  e.seq = seq;
  e.type = type;
  pending_.push_back(std::move(e));
  return Status::OK();
}

Status OfflineDedupInstallSink::Finish() {
  if (pending_.empty()) {
    // Nothing to install — successful no-op. The UVL file still
    // exists; it'll be either immediately reclaimed (no references)
    // or kept for a future compaction that drops to zero refcount
    // (UVL GC, ITEM-18). For ITEM-09b we leave it; the worker's
    // caller decides whether to unlink.
    return Status::OK();
  }
  return BuildAndInstall();
}

Status OfflineDedupInstallSink::BuildAndInstall() {
  // Sort ascending by internal-key: user_key ASC, seq DESC, type DESC
  // (matches RocksDB's internal key comparator). The drain has
  // already deduped so each user_key appears at most once; a stable
  // sort by user_key suffices.
  const Comparator* ucmp = cfd_->internal_comparator().user_comparator();
  std::stable_sort(pending_.begin(), pending_.end(),
                   [ucmp](const PendingEntry& a, const PendingEntry& b) {
                     return ucmp->CompareWithoutTimestamp(Slice(a.user_key),
                                                          Slice(b.user_key)) <
                            0;
                   });

  // Allocate SST file number + open output file.
  const auto& ioptions = cfd_->ioptions();
  FileSystem* fs = ioptions.fs.get();
  SystemClock* clock = ioptions.clock;
  assert(!ioptions.cf_paths.empty());

  // Capture and allocate file number inside the DB mutex so
  // VersionSet::next_file_number stays monotone relative to other
  // background jobs.
  uint64_t sst_file_number = 0;
  {
    InstrumentedMutexLock lock(db_->mutex());
    sst_file_number = db_->GetVersionSet()->NewFileNumber();
  }

  const std::string sst_path =
      TableFileName(ioptions.cf_paths, sst_file_number, /*path_id=*/0);
  std::unique_ptr<FSWritableFile> fs_file;
  FileOptions fo;
  Status s = NewWritableFile(fs, sst_path, &fs_file, fo);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(fs_file), sst_path, fo, clock));

  // Build TableBuilderOptions mirroring WriteLevel0TableForRecovery's
  // shape. The drained keys are value→BlobIndex already, so SST-level
  // compression stays off by default (these are tiny pointer entries).
  const MutableCFOptions mcfo = cfd_->GetLatestMutableCFOptions();
  ReadOptions ro(Env::IOActivity::kFlush);
  WriteOptions wo(Env::IO_HIGH, Env::IOActivity::kFlush);
  int64_t now = 0;
  clock->GetCurrentTime(&now).PermitUncheckedError();
  const uint64_t current_time = static_cast<uint64_t>(now);

  std::string db_id;
  std::string db_session_id;
  db_->GetDbIdentity(db_id).PermitUncheckedError();
  db_->GetDbSessionId(db_session_id).PermitUncheckedError();

  TableBuilderOptions tboptions(
      cfd_->ioptions(), mcfo, ro, wo, cfd_->internal_comparator(),
      cfd_->internal_tbl_prop_coll_factories(),
      kNoCompression /* output_compression */, mcfo.compression_opts,
      cfd_->GetID(), cfd_->GetName(), 0 /* level */,
      current_time /* newest_key_time */, false /* is_bottommost */,
      TableFileCreationReason::kFlush, current_time /* oldest_key_time */,
      current_time /* file_creation_time */, db_id, db_session_id,
      0 /* target_file_size */, sst_file_number, kMaxSequenceNumber);

  std::unique_ptr<TableBuilder> builder(
      mcfo.table_factory->NewTableBuilder(tboptions, file_writer.get()));

  InternalKey smallest_ikey;
  InternalKey largest_ikey;
  SequenceNumber smallest_seq = kMaxSequenceNumber;
  SequenceNumber largest_seq = 0;
  std::string encoded_ikey;

  for (const auto& e : pending_) {
    encoded_ikey.clear();
    AppendInternalKey(&encoded_ikey, ParsedInternalKey(e.user_key, e.seq, e.type));
    Slice ikey_slice(encoded_ikey);
    builder->Add(ikey_slice, Slice(e.value));
    if (!builder->status().ok()) {
      s = builder->status();
      break;
    }
    if (smallest_ikey.size() == 0) {
      smallest_ikey.DecodeFrom(ikey_slice);
    }
    largest_ikey.DecodeFrom(ikey_slice);
    if (e.seq < smallest_seq) smallest_seq = e.seq;
    if (e.seq > largest_seq) largest_seq = e.seq;
  }

  if (!s.ok()) {
    builder->Abandon();
    file_writer.reset();
    IOOptions io;
    fs->DeleteFile(sst_path, io, nullptr).PermitUncheckedError();
    return s;
  }

  s = builder->Finish();
  if (!s.ok()) {
    file_writer.reset();
    IOOptions io;
    fs->DeleteFile(sst_path, io, nullptr).PermitUncheckedError();
    return s;
  }

  const uint64_t sst_file_size = builder->FileSize();
  builder.reset();

  IOOptions sync_io;
  if (file_writer) {
    const bool use_fsync = db_->GetDBOptions().use_fsync;
    IOStatus ios = file_writer->Sync(sync_io, use_fsync);
    if (!ios.ok()) {
      file_writer.reset();
      fs->DeleteFile(sst_path, sync_io, nullptr).PermitUncheckedError();
      return ios;
    }
    ios = file_writer->Close(sync_io);
    file_writer.reset();
    if (!ios.ok()) {
      fs->DeleteFile(sst_path, sync_io, nullptr).PermitUncheckedError();
      return ios;
    }
  }

  // Build VersionEdit and install. The edit carries the L0 SST plus a
  // UvlFileAddition so the MANIFEST records the UVL alongside.
  VersionEdit edit;
  edit.SetColumnFamily(cfd_->GetID());
  FileMetaData meta;
  meta.fd = FileDescriptor(sst_file_number, 0, sst_file_size);
  meta.smallest = smallest_ikey;
  meta.largest = largest_ikey;
  meta.fd.smallest_seqno = smallest_seq;
  meta.fd.largest_seqno = largest_seq;
  meta.oldest_ancester_time = current_time;
  meta.file_creation_time = current_time;
  meta.epoch_number = cfd_->NewEpochNumber();
  edit.AddFile(
      0 /* level */, meta.fd.GetNumber(), meta.fd.GetPathId(),
      meta.fd.GetFileSize(), meta.smallest, meta.largest,
      meta.fd.smallest_seqno, meta.fd.largest_seqno, meta.marked_for_compaction,
      meta.temperature, meta.oldest_blob_file_number, meta.oldest_ancester_time,
      meta.file_creation_time, meta.epoch_number, meta.file_checksum,
      meta.file_checksum_func_name, meta.unique_id,
      meta.compensated_range_deletion_size, meta.tail_size,
      meta.user_defined_timestamps_persisted, meta.min_timestamp,
      meta.max_timestamp);
  if (uvl_total_records_ > 0) {
    edit.AddUvlFile(uvl_file_number_, cfd_->GetID(), uvl_total_records_,
                    uvl_total_bytes_, current_time);
    // ITEM-18f: register the UVL's total size for the auto-GC scheduler.
    const auto& dctx = cfd_->dedup_context();
    if (dctx) {
      dctx->RegisterUvlFile(uvl_file_number_, uvl_total_bytes_);
    }
  }

  InstrumentedMutexLock lock(db_->mutex());
  s = db_->GetVersionSet()->LogAndApply(cfd_, ro, wo, &edit, db_->mutex(),
                                        db_->GetDbDir());
  if (!s.ok()) {
    if (info_log_) {
      ROCKS_LOG_ERROR(info_log_,
                      "[DedupKV] LogAndApply failed for offline SST #%" PRIu64
                      ": %s",
                      sst_file_number, s.ToString().c_str());
    }
    // Best-effort unlink the orphaned SST; the UVL is owned by the
    // caller.
    IOOptions io;
    fs->DeleteFile(sst_path, io, nullptr).PermitUncheckedError();
    return s;
  }
  // Refresh SuperVersion so readers see the newly installed L0 SST.
  SuperVersionContext sv_context(true /* create_superversion */);
  db_->InstallSuperVersionAndScheduleWork(cfd_, &sv_context);
  sv_context.Clean();
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
