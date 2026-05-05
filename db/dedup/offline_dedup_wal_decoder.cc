//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/offline_dedup_wal_decoder.h"

#include <memory>
#include <utility>

#include "db/log_reader.h"
#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// WriteBatch handler that collects eligible entries into a vector. Seq
// tracking: the batch's starting seq is `batch_start_seq`; each entry
// bumps by 1 (matching RocksDB's internal convention).
class CollectHandler : public WriteBatch::Handler {
 public:
  CollectHandler(uint32_t target_cf_id, SequenceNumber batch_start_seq,
                 std::vector<OfflineWalRecord>* out,
                 OfflineDedupWalDecodeStats* stats)
      : target_cf_id_(target_cf_id),
        cur_seq_(batch_start_seq),
        out_(out),
        stats_(stats) {}

  Status PutCF(uint32_t cf, const Slice& key, const Slice& value) override {
    if (stats_) ++stats_->batch_entries_decoded;
    if (cf != target_cf_id_) {
      if (stats_) ++stats_->entries_skipped_cf;
      ++cur_seq_;
      return Status::OK();
    }
    OfflineWalRecord r;
    r.seq = cur_seq_++;
    r.type = kTypeValue;
    r.key.assign(key.data(), key.size());
    r.value.assign(value.data(), value.size());
    out_->push_back(std::move(r));
    if (stats_) ++stats_->entries_kept;
    return Status::OK();
  }

  Status DeleteCF(uint32_t cf, const Slice& key) override {
    return HandleDelete(cf, key, kTypeDeletion);
  }
  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    return HandleDelete(cf, key, kTypeSingleDeletion);
  }

  // Unsupported today (DedupKV disallows Merge; TimedPut / DeleteRange
  // are not part of the paper's workloads). Count & skip.
  Status MergeCF(uint32_t /*cf*/, const Slice& /*key*/,
                 const Slice& /*value*/) override {
    if (stats_) {
      ++stats_->batch_entries_decoded;
      ++stats_->entries_unsupported;
    }
    ++cur_seq_;
    return Status::OK();
  }
  Status TimedPutCF(uint32_t /*cf*/, const Slice& /*key*/,
                    const Slice& /*value*/, uint64_t /*write_time*/) override {
    if (stats_) {
      ++stats_->batch_entries_decoded;
      ++stats_->entries_unsupported;
    }
    ++cur_seq_;
    return Status::OK();
  }
  Status DeleteRangeCF(uint32_t /*cf*/, const Slice& /*begin*/,
                       const Slice& /*end*/) override {
    if (stats_) {
      ++stats_->batch_entries_decoded;
      ++stats_->entries_unsupported;
    }
    ++cur_seq_;
    return Status::OK();
  }

 private:
  Status HandleDelete(uint32_t cf, const Slice& key, ValueType type) {
    if (stats_) ++stats_->batch_entries_decoded;
    if (cf != target_cf_id_) {
      if (stats_) ++stats_->entries_skipped_cf;
      ++cur_seq_;
      return Status::OK();
    }
    OfflineWalRecord r;
    r.seq = cur_seq_++;
    r.type = type;
    r.key.assign(key.data(), key.size());
    out_->push_back(std::move(r));
    if (stats_) ++stats_->entries_kept;
    return Status::OK();
  }

  const uint32_t target_cf_id_;
  SequenceNumber cur_seq_;
  std::vector<OfflineWalRecord>* const out_;
  OfflineDedupWalDecodeStats* const stats_;
};

// Swallow log::Reader corruption reports rather than aborting the
// process; the worker treats any decode error as a drain failure and
// leaves the WAL in place.
class SilentReporter : public log::Reader::Reporter {
 public:
  void Corruption(size_t /*bytes*/, const Status& s,
                  uint64_t /*log_number*/) override {
    last_status_ = s;
  }
  const Status& last_status() const { return last_status_; }

 private:
  Status last_status_ = Status::OK();
};

}  // namespace

Status DecodeWalFileForOfflineDedup(FileSystem* fs, SystemClock* clock,
                                    const std::string& wal_path,
                                    uint64_t log_number,
                                    uint32_t target_cf_id,
                                    std::vector<OfflineWalRecord>* records,
                                    std::shared_ptr<Logger> info_log,
                                    OfflineDedupWalDecodeStats* stats) {
  if (records == nullptr) {
    return Status::InvalidArgument("DecodeWalFileForOfflineDedup: null records");
  }
  if (fs == nullptr) {
    return Status::InvalidArgument("null FileSystem");
  }
  records->clear();

  std::unique_ptr<FSSequentialFile> fs_file;
  Status s =
      fs->NewSequentialFile(wal_path, FileOptions(), &fs_file, /*dbg=*/nullptr);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<SequentialFileReader> sfr(new SequentialFileReader(
      std::move(fs_file), wal_path, nullptr /*io_tracer*/));

  SilentReporter reporter;
  log::Reader reader(info_log, std::move(sfr), &reporter,
                     /*checksum=*/true, log_number);

  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch,
                           WALRecoveryMode::kTolerateCorruptedTailRecords)) {
    if (stats) ++stats->wal_records_read;
    WriteBatch batch;
    s = WriteBatchInternal::SetContents(&batch, record);
    if (!s.ok()) {
      return s;
    }
    SequenceNumber batch_seq = WriteBatchInternal::Sequence(&batch);
    CollectHandler handler(target_cf_id, batch_seq, records, stats);
    s = batch.Iterate(&handler);
    if (!s.ok()) {
      return s;
    }
  }
  // log::Reader reports corruption via the Reporter; surface it as a
  // drain error so the worker leaves the WAL for human triage.
  if (!reporter.last_status().ok()) {
    return reporter.last_status();
  }
  (void)clock;  // reserved for future throttling; unused today
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
