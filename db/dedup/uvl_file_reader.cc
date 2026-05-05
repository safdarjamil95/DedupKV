//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_file_reader.h"

#include <cstring>
#include <string>
#include <utility>

#include "file/random_access_file_reader.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

UvlFileReader::UvlFileReader(
    std::unique_ptr<RandomAccessFileReader> file_reader, uint64_t file_size,
    const UvlHeader& header)
    : file_reader_(std::move(file_reader)),
      file_size_(file_size),
      header_(header) {}

UvlFileReader::~UvlFileReader() = default;

Status UvlFileReader::Open(std::unique_ptr<RandomAccessFileReader> file_reader,
                           uint64_t file_size,
                           std::unique_ptr<UvlFileReader>* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("UvlFileReader::Open: null out");
  }
  if (file_reader == nullptr) {
    return Status::InvalidArgument("UvlFileReader::Open: null file_reader");
  }
  if (file_size < UvlHeader::kSize) {
    return Status::Corruption("UVL file smaller than header");
  }

  char scratch[UvlHeader::kSize];
  Slice result;
  IOOptions io_opts;
  IOStatus ios = file_reader->Read(io_opts, /*offset=*/0, UvlHeader::kSize,
                                   &result, scratch, /*aligned_buf=*/nullptr);
  if (!ios.ok()) {
    return ios;
  }
  if (result.size() != UvlHeader::kSize) {
    return Status::Corruption("Short read of UVL header");
  }

  UvlHeader header;
  Status s = header.DecodeFrom(result);
  if (!s.ok()) {
    return s;
  }

  out->reset(new UvlFileReader(std::move(file_reader), file_size, header));
  return Status::OK();
}

Status UvlFileReader::GetValue(uint64_t offset, uint64_t record_size,
                               PinnableSlice* value,
                               UvlCompression* compression) const {
  if (value == nullptr || compression == nullptr) {
    return Status::InvalidArgument("UvlFileReader::GetValue: null out");
  }
  if (offset < UvlHeader::kSize) {
    return Status::InvalidArgument("Offset within UVL header region");
  }
  if (record_size == 0) {
    return Status::InvalidArgument("Zero record_size");
  }
  if (offset + record_size > file_size_) {
    return Status::InvalidArgument("Record range extends beyond file");
  }

  // Read the full record into a scratch buffer. Allocated in `value`'s
  // storage so the PinnableSlice can own it without an extra copy.
  value->Reset();
  std::string* const buf = value->GetSelf();
  buf->resize(static_cast<size_t>(record_size));

  Slice result;
  IOOptions io_opts;
  IOStatus ios = file_reader_->Read(io_opts, offset,
                                    static_cast<size_t>(record_size), &result,
                                    &(*buf)[0], /*aligned_buf=*/nullptr);
  if (!ios.ok()) {
    return ios;
  }
  if (result.size() != record_size) {
    return Status::Corruption("Short read of UVL record");
  }

  // If the filesystem returned a pointer into its own buffer rather
  // than `scratch`, copy into `buf` so we own the memory.
  if (result.data() != buf->data()) {
    buf->assign(result.data(), result.size());
  }

  // Decode — validates CRC and compression byte.
  Slice cursor(*buf);
  UvlRecord decoded;
  Status s = DecodeUvlRecord(&cursor, &decoded);
  if (!s.ok()) {
    return s;
  }
  if (!cursor.empty()) {
    return Status::Corruption("UVL record length mismatch");
  }

  *compression = decoded.compression;
  // Overwrite `buf` with just the value bytes. Safe because we already
  // copied key/value into decoded's owned buffers.
  buf->assign(decoded.value.data(), decoded.value.size());
  value->PinSelf();
  return Status::OK();
}

Status UvlFileReader::GetFingerprint(uint64_t offset,
                                     UvlFingerprint* fingerprint) const {
  if (fingerprint == nullptr) {
    return Status::InvalidArgument("UvlFileReader::GetFingerprint: null out");
  }
  if (offset < UvlHeader::kSize) {
    return Status::InvalidArgument("Offset within UVL header region");
  }
  if (offset + kUvlFingerprintSize > file_size_) {
    return Status::InvalidArgument("Fingerprint range extends beyond file");
  }

  char scratch[kUvlFingerprintSize];
  Slice result;
  IOOptions io_opts;
  IOStatus ios =
      file_reader_->Read(io_opts, offset, kUvlFingerprintSize, &result,
                         scratch, /*aligned_buf=*/nullptr);
  if (!ios.ok()) {
    return ios;
  }
  if (result.size() != kUvlFingerprintSize) {
    return Status::Corruption("Short read of UVL fingerprint");
  }
  std::memcpy(fingerprint->data(), result.data(), kUvlFingerprintSize);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
