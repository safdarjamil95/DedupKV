//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_gc.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/dedup/cit.h"
#include "db/dedup/uvl_file_builder.h"
#include "db/dedup/uvl_file_reader.h"
#include "db/dedup/uvl_garbage_meter.h"
#include "db/dedup/uvl_log_format.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/system_clock.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Read the whole UVL body (past the 24-byte header) into `buf`. The
// body is bounded by write_buffer_size × duplicate ratio in practice —
// same size class as the memtable snapshot whose flush produced this
// UVL — so buffering the whole thing is the same memory profile the
// offline worker already accepts (AMBIGUITY-005).
Status ReadUvlBody(const std::string& path, FileSystem* fs,
                   SystemClock* clock, std::string* body) {
  uint64_t file_size = 0;
  Status s = fs->GetFileSize(path, IOOptions(), &file_size, /*dbg=*/nullptr);
  if (!s.ok()) return s;
  if (file_size < UvlHeader::kSize) {
    return Status::Corruption("UVL file shorter than header", path);
  }
  std::unique_ptr<FSRandomAccessFile> fs_file;
  s = fs->NewRandomAccessFile(path, FileOptions(), &fs_file, /*dbg=*/nullptr);
  if (!s.ok()) return s;
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(fs_file), path, clock));
  // Open via UvlFileReader to validate the header; we throw away the
  // reader and re-read the body with Read() because UvlFileReader's
  // surface is {offset, record_size}-based and we want a streaming
  // decode.
  std::unique_ptr<UvlFileReader> ufr;
  s = UvlFileReader::Open(std::move(reader), file_size, &ufr);
  if (!s.ok()) return s;

  const size_t body_len = static_cast<size_t>(file_size - UvlHeader::kSize);
  body->clear();
  body->resize(body_len);
  if (body_len == 0) return Status::OK();

  std::unique_ptr<FSRandomAccessFile> body_file;
  s = fs->NewRandomAccessFile(path, FileOptions(), &body_file,
                              /*dbg=*/nullptr);
  if (!s.ok()) return s;
  Slice result;
  IOStatus ios = body_file->Read(UvlHeader::kSize, body_len, IOOptions(),
                                 &result, body->data(), /*dbg=*/nullptr);
  if (!ios.ok()) return ios;
  // If the FS returned a pointer into its own buffer, copy out.
  if (result.data() != body->data()) {
    std::memcpy(body->data(), result.data(), result.size());
  }
  body->resize(result.size());
  return Status::OK();
}

}  // namespace

Status UvlGcRewriter::Run(ColumnFamilyData* cfd, VersionSet* versions,
                          SystemClock* clock, CIT* cit,
                          UvlGarbageMeter* garbage_meter,
                          uint64_t old_uvl_file_number, VersionEdit* edit,
                          UvlGcRewriteResult* result) {
  if (cfd == nullptr || versions == nullptr || clock == nullptr ||
      cit == nullptr || garbage_meter == nullptr || edit == nullptr ||
      result == nullptr) {
    return Status::InvalidArgument("UvlGcRewriter::Run null argument");
  }
  if (old_uvl_file_number == 0) {
    return Status::InvalidArgument("Invalid old UVL file number");
  }

  *result = UvlGcRewriteResult{};

  const auto& cf_paths = cfd->ioptions().cf_paths;
  if (cf_paths.empty()) {
    return Status::Corruption("No cf_paths configured for UVL GC");
  }
  FileSystem* fs = cfd->ioptions().fs.get();
  if (fs == nullptr) {
    return Status::Corruption("Null FileSystem for UVL GC");
  }

  const std::string old_path =
      UvlFileName(cf_paths.front().path, old_uvl_file_number);
  std::string body;
  Status s = ReadUvlBody(old_path, fs, clock, &body);
  if (!s.ok()) return s;

  // Phase 1: decode every record from the old UVL and collect the
  // ones whose CIT refcount is still > 0 and still points at this
  // file+offset. A concurrent compaction drop that lands between
  // Phase 1 and Phase 3 is harmless — see the class-level comment.
  struct LiveRecord {
    UvlFingerprint fingerprint{};
    UvlCompression compression = UvlCompression::kRaw;
    std::string key;
    std::string value;
    uint64_t old_offset = 0;
  };
  std::vector<LiveRecord> live;
  live.reserve(64);

  Slice cursor(body);
  uint64_t offset = UvlHeader::kSize;
  bool had_small_branch = false;
  while (!cursor.empty()) {
    const size_t remaining_before = cursor.size();
    UvlRecord rec;
    s = DecodeUvlRecord(&cursor, &rec);
    if (!s.ok()) return s;
    const uint64_t consumed =
        static_cast<uint64_t>(remaining_before - cursor.size());

    // ITEM-18e: any LZ4-inline record pins the old file — small-
    // branch entries aren't in CIT, so the SST's BlobIndex pointing
    // at `(old_file, offset)` is the only way to find them and Get
    // falls back to that coordinate.
    if (rec.compression == UvlCompression::kLz4Inline) {
      had_small_branch = true;
    }

    CITEntry cit_entry;
    if (cit->Lookup(rec.fingerprint, &cit_entry, /*touch_lru=*/false)) {
      if (cit_entry.refcount > 0 && cit_entry.uvl_file == old_uvl_file_number &&
          cit_entry.offset == offset) {
        LiveRecord lr;
        lr.fingerprint = rec.fingerprint;
        lr.compression = rec.compression;
        lr.key = std::move(rec.key_buf);
        lr.value = std::move(rec.value_buf);
        lr.old_offset = offset;
        live.push_back(std::move(lr));
      }
    }
    offset += consumed;
  }

  // Phase 2: allocate + write the new UVL if there are any live
  // records to preserve.
  uint64_t new_uvl_file_number = 0;
  uint64_t bytes_written = 0;
  if (!live.empty()) {
    new_uvl_file_number = versions->NewFileNumber();
    const std::string new_path =
        UvlFileName(cf_paths.front().path, new_uvl_file_number);

    std::unique_ptr<FSWritableFile> fs_file;
    IOStatus ios = NewWritableFile(fs, new_path, &fs_file, FileOptions());
    if (!ios.ok()) return ios;
    std::unique_ptr<WritableFileWriter> writer(new WritableFileWriter(
        std::move(fs_file), new_path, FileOptions(), clock));

    const uint64_t creation_time = [&]() -> uint64_t {
      int64_t now = 0;
      clock->GetCurrentTime(&now).PermitUncheckedError();
      return static_cast<uint64_t>(now);
    }();

    UvlFileBuilder builder(std::move(writer), new_uvl_file_number,
                           cfd->GetID(), creation_time);
    s = builder.Open();
    if (!s.ok()) {
      // Best-effort cleanup of the partial file.
      fs->DeleteFile(new_path, IOOptions(), /*dbg=*/nullptr)
          .PermitUncheckedError();
      return s;
    }

    for (auto& lr : live) {
      uint64_t rec_offset = 0;
      uint64_t rec_size = 0;
      s = builder.Add(lr.fingerprint, lr.compression, Slice(lr.key),
                      Slice(lr.value), &rec_offset, &rec_size);
      if (!s.ok()) {
        builder.Abandon();
        fs->DeleteFile(new_path, IOOptions(), /*dbg=*/nullptr)
            .PermitUncheckedError();
        return s;
      }

      // Phase 3: retarget the CIT entry to the new location.
      // Conditional on CIT still naming {old_file, old_offset} —
      // handles the tiny race where a parallel thread has already
      // moved the record (e.g. a re-run of GC). If the entry moved,
      // silently skip; the record we just wrote is orphaned (counts
      // as future-GC garbage in the new file).
      cit->RetargetLocation(lr.fingerprint, old_uvl_file_number,
                            lr.old_offset, new_uvl_file_number, rec_offset);
    }

    s = builder.Finish(/*sync=*/true);
    if (!s.ok()) {
      fs->DeleteFile(new_path, IOOptions(), /*dbg=*/nullptr)
          .PermitUncheckedError();
      return s;
    }
    bytes_written = builder.total_bytes();
  }

  // Phase 4: forget the old file's charge in the meter so subsequent
  // GC scheduling doesn't immediately re-fire on the same file.
  garbage_meter->Forget(old_uvl_file_number);

  // Phase 5: populate the VersionEdit. The caller will LogAndApply.
  if (new_uvl_file_number != 0) {
    const uint64_t new_creation_time = [&]() -> uint64_t {
      int64_t now = 0;
      clock->GetCurrentTime(&now).PermitUncheckedError();
      return static_cast<uint64_t>(now);
    }();
    edit->AddUvlFile(new_uvl_file_number, cfd->GetID(),
                     static_cast<uint64_t>(live.size()), bytes_written,
                     new_creation_time);
  }
  edit->AddUvlFileGarbage(old_uvl_file_number, new_uvl_file_number,
                          cfd->GetID(), static_cast<uint64_t>(live.size()),
                          bytes_written);

  result->new_uvl_file_number = new_uvl_file_number;
  result->live_records_copied = static_cast<uint64_t>(live.size());
  result->live_bytes_copied = bytes_written;
  result->old_file_had_small_branch = had_small_branch;
  return Status::OK();
}

Status CitRebuild::Run(ColumnFamilyData* cfd, SystemClock* clock, CIT* cit,
                       std::function<void(uint64_t, uint64_t)>
                           total_bytes_registry_cb,
                       Logger* info_log) {
  if (cfd == nullptr || cit == nullptr) {
    return Status::InvalidArgument("CitRebuild::Run null argument");
  }
  const auto& cf_paths = cfd->ioptions().cf_paths;
  if (cf_paths.empty()) {
    return Status::OK();  // nothing to scan
  }
  FileSystem* fs = cfd->ioptions().fs.get();
  if (fs == nullptr) {
    return Status::OK();
  }
  const std::string& dir = cf_paths.front().path;
  std::vector<std::string> children;
  Status s = fs->GetChildren(dir, IOOptions(), &children, /*dbg=*/nullptr);
  if (!s.ok()) return s;

  for (const auto& name : children) {
    uint64_t file_number = 0;
    FileType ftype = kInfoLogFile;
    if (!ParseFileName(name, &file_number, &ftype) || ftype != kUvlFile) {
      continue;
    }
    const std::string path = UvlFileName(dir, file_number);
    std::string body;
    Status rs = ReadUvlBody(path, fs, clock, &body);
    if (!rs.ok()) {
      ROCKS_LOG_WARN(
          info_log,
          "[DedupKV] CIT rebuild: skipping unreadable UVL %" PRIu64 " (%s): %s",
          file_number, path.c_str(), rs.ToString().c_str());
      continue;
    }

    // Register the file's total size (body + header) with the
    // scheduler's registry so ITEM-18f can evaluate it on future
    // compactions.
    if (total_bytes_registry_cb) {
      total_bytes_registry_cb(file_number, body.size() + UvlHeader::kSize);
    }

    Slice cursor(body);
    uint64_t offset = UvlHeader::kSize;
    while (!cursor.empty()) {
      const size_t remaining_before = cursor.size();
      UvlRecord rec;
      Status drs = DecodeUvlRecord(&cursor, &rec);
      if (!drs.ok()) {
        ROCKS_LOG_WARN(info_log,
                       "[DedupKV] CIT rebuild: decode failure in UVL %" PRIu64
                       " at offset %" PRIu64 ": %s — truncating scan",
                       file_number, offset, drs.ToString().c_str());
        break;
      }
      const uint64_t consumed =
          static_cast<uint64_t>(remaining_before - cursor.size());

      // Skip small-branch (fp == all zeros) records: not CIT-tracked
      // by design (§4.6, AMBIGUITY-001).
      const UvlFingerprint zero{};
      if (rec.fingerprint != zero) {
        CITEntry entry;
        entry.uvl_file = file_number;
        entry.offset = offset;
        entry.size = static_cast<uint32_t>(consumed);
        entry.refcount = 1;  // placeholder per DEC-026
        entry.compression = rec.compression;
        // `Insert` is first-write-wins: if another UVL already
        // contributed this fp (possible when a GC'd file still sits
        // on disk because it pinned a small-branch record), keep the
        // first winner and don't overwrite or bump refcount.
        cit->Insert(rec.fingerprint, entry);
      }
      offset += consumed;
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
