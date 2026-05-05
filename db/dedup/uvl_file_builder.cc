//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_file_builder.h"

#include <string>
#include <utility>

#include "file/writable_file_writer.h"

namespace ROCKSDB_NAMESPACE {

UvlFileBuilder::UvlFileBuilder(std::unique_ptr<WritableFileWriter> writer,
                               uint64_t file_number,
                               uint32_t column_family_id,
                               uint64_t creation_time, uint32_t flags)
    : writer_(std::move(writer)),
      file_number_(file_number),
      column_family_id_(column_family_id),
      creation_time_(creation_time),
      flags_(flags) {}

UvlFileBuilder::~UvlFileBuilder() {
  // If neither Finish() nor Abandon() ran, drop the writer without
  // close/sync. Partial files are reclaimable because every record is
  // self-delimiting and CRC-protected (see uvl_log_format.h).
}

Status UvlFileBuilder::Open() {
  if (opened_) {
    return Status::InvalidArgument("UvlFileBuilder already opened");
  }
  if (writer_ == nullptr) {
    return Status::InvalidArgument("UvlFileBuilder has no writer");
  }

  UvlHeader header;
  header.version = kUvlVersion1;
  header.column_family_id = column_family_id_;
  header.creation_time = creation_time_;
  header.flags = flags_;

  std::string encoded;
  header.EncodeTo(&encoded);

  IOOptions io_opts;
  IOStatus s = writer_->Append(io_opts, Slice(encoded));
  if (!s.ok()) {
    return s;
  }
  next_offset_ = encoded.size();
  opened_ = true;
  return Status::OK();
}

Status UvlFileBuilder::Add(const UvlFingerprint& fingerprint,
                           UvlCompression compression, const Slice& key,
                           const Slice& value, uint64_t* record_offset,
                           uint64_t* record_size) {
  if (!opened_) {
    return Status::InvalidArgument("UvlFileBuilder not opened");
  }
  if (finished_) {
    return Status::InvalidArgument("UvlFileBuilder already finished");
  }

  // Build the record in a scratch buffer; the codec handles CRC. We
  // encode into a fresh buffer (rather than appending directly to some
  // shared buffer) so we know the exact byte count to report.
  std::string encoded;
  size_t nbytes =
      EncodeUvlRecord(&encoded, fingerprint, compression, key, value);

  IOOptions io_opts;
  IOStatus s = writer_->Append(io_opts, Slice(encoded));
  if (!s.ok()) {
    return s;
  }

  const uint64_t this_offset = next_offset_;
  next_offset_ += nbytes;
  ++record_count_;

  if (record_offset != nullptr) {
    *record_offset = this_offset;
  }
  if (record_size != nullptr) {
    *record_size = nbytes;
  }
  return Status::OK();
}

Status UvlFileBuilder::Finish(bool sync) {
  if (finished_) {
    return Status::InvalidArgument("UvlFileBuilder already finished");
  }
  if (writer_ == nullptr) {
    return Status::InvalidArgument("UvlFileBuilder has no writer");
  }
  finished_ = true;

  IOOptions io_opts;
  IOStatus s = writer_->Flush(io_opts);
  if (s.ok() && sync) {
    // use_fsync=false: fdatasync is sufficient for UVL data blocks —
    // file-size metadata is reconcilable at recovery via the per-record
    // CRC scan (see plan.md §4.8 / ITEM-19).
    s = writer_->Sync(io_opts, /*use_fsync=*/false);
  }
  if (s.ok()) {
    s = writer_->Close(io_opts);
  }
  // Release the writer whether close succeeded or not.
  writer_.reset();
  return s;
}

void UvlFileBuilder::Abandon() {
  finished_ = true;
  writer_.reset();
}

}  // namespace ROCKSDB_NAMESPACE
