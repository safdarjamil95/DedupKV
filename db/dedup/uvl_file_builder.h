//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Append-only writer for a DedupKV UVL (Unique Value Log) file. Emits a
// 24-byte UvlHeader on Open() then appends records encoded by the
// ITEM-01 codec (see db/dedup/uvl_log_format.h). Finish() flushes and
// optionally syncs + closes the underlying file. Abandon() releases the
// file without sync so the caller can delete it on error.
//
// Intentionally minimal compared to db/blob/blob_file_builder.h:
// no VersionSet/cache/IOTracer integration (that's wired in ITEM-14).
// Compression is performed by DGD (ITEM-07) before Add() is called;
// this builder only records the compression byte in each record header.

#pragma once

#include <cstdint>
#include <memory>

#include "db/dedup/uvl_log_format.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;

class UvlFileBuilder {
 public:
  // `writer` must be a just-opened WritableFileWriter owned by this
  // builder. `file_number` is the 6-digit number used in the filename
  // (e.g. 000123 in "000123.uvl"); stored here for observability but
  // the builder itself never reads the filename.
  UvlFileBuilder(std::unique_ptr<WritableFileWriter> writer,
                 uint64_t file_number, uint32_t column_family_id,
                 uint64_t creation_time, uint32_t flags = 0);

  UvlFileBuilder(const UvlFileBuilder&) = delete;
  UvlFileBuilder& operator=(const UvlFileBuilder&) = delete;

  ~UvlFileBuilder();

  // Writes the 24-byte UvlHeader to the file. Must be called exactly
  // once, before any Add(). Returns the underlying IO status.
  Status Open();

  // Appends a fully-encoded UVL record and returns the record's byte
  // range within the file (both outputs optional; pass nullptr to
  // discard). After the call, next_record_offset() == *record_offset +
  // *record_size.
  Status Add(const UvlFingerprint& fingerprint, UvlCompression compression,
             const Slice& key, const Slice& value,
             uint64_t* record_offset = nullptr,
             uint64_t* record_size = nullptr);

  // Flushes buffered bytes, optionally fsyncs, and closes the file.
  // After Finish() the builder is sealed; further Add() calls return
  // InvalidArgument.
  Status Finish(bool sync = true);

  // Releases the underlying writer without sync/close. Intended for
  // error-recovery paths where the caller will delete the partial file.
  void Abandon();

  uint64_t file_number() const { return file_number_; }
  uint64_t record_count() const { return record_count_; }
  // Total bytes written to the file, header included.
  uint64_t total_bytes() const { return next_offset_; }
  // Offset at which the next Add() will place its record.
  uint64_t next_record_offset() const { return next_offset_; }
  bool is_open() const { return opened_ && !finished_ && writer_ != nullptr; }
  bool is_finished() const { return finished_; }

 private:
  std::unique_ptr<WritableFileWriter> writer_;
  const uint64_t file_number_;
  const uint32_t column_family_id_;
  const uint64_t creation_time_;
  const uint32_t flags_;
  uint64_t next_offset_ = 0;
  uint64_t record_count_ = 0;
  bool opened_ = false;
  bool finished_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
