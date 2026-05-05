//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// UVL (Unique Value Log) on-disk format — the DedupKV analog of the
// BlobDB blob file. Shared by UvlFileBuilder (ITEM-02) and
// UvlFileReader (ITEM-03). See plan.md AMBIGUITY-003 for layout
// rationale (fingerprint is stored alongside the value so the recovery
// reconciliation pass in §4.8 can rebuild the CIT without consulting
// the LSM).

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// "UVL\x01" in little-endian ASCII. Chosen so sst_dump / ldb cannot
// mistake a UVL file for a blob file (distinct magic).
constexpr uint32_t kUvlMagicNumber = 0x014c5655;
constexpr uint32_t kUvlVersion1 = 1;

// DGD (ITEM-07) emits one of these in the per-record compression byte.
// kUvlCompressionRaw → large-value branch: fingerprint-shared via CIT.
// kUvlCompressionLz4Inline → small-value branch: LZ4-compressed,
//   stored inline, never shared (no CIT entry).
enum class UvlCompression : uint8_t {
  kRaw = 0,
  kLz4Inline = 1,
};

constexpr size_t kUvlFingerprintSize = 20;  // SHA1 width
using UvlFingerprint = std::array<uint8_t, kUvlFingerprintSize>;

// clang-format off
// UVL file header (24 bytes):
//
//   +--------+---------+---------+---------------+-------+
//   | magic  | version | cf_id   | creation_time | flags |
//   +--------+---------+---------+---------------+-------+
//   | Fixed32| Fixed32 | Fixed32 | Fixed64       |Fixed32|
//   +--------+---------+---------+---------------+-------+
//
// No footer: UVLs are append-only; a truncated tail is recoverable
// because every record carries its own CRC.
// clang-format on

struct UvlHeader {
  static constexpr size_t kSize = 24;

  uint32_t version = kUvlVersion1;
  uint32_t column_family_id = 0;
  uint64_t creation_time = 0;
  uint32_t flags = 0;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice src);
};

// clang-format off
// UVL record layout:
//
//   +----+-------------+----------+-----+------------+-------+--------+
//   | fp | compression | ksz      | key | vsz        | value | crc32c |
//   +----+-------------+----------+-----+------------+-------+--------+
//   | 20B| 1 byte      | varint32 | ... | varint32   | ...   | Fixed32|
//   +----+-------------+----------+-----+------------+-------+--------+
//
// The CRC covers [fp .. value] (everything up to but not including
// itself). CRC is masked per the RocksDB convention (crc32c::Mask).
//
// For the LZ4-inline small-value path (DGD §4.6), the "value" bytes
// are already LZ4-compressed; vsz is the compressed length. The
// compression byte distinguishes branches at decode time.
// clang-format on

struct UvlRecord {
  UvlFingerprint fingerprint{};
  UvlCompression compression = UvlCompression::kRaw;
  Slice key;
  Slice value;

  // Deep copies key/value into the owned buffers so the decoded record
  // is safe to hold after `src` goes out of scope.
  std::string key_buf;
  std::string value_buf;
};

// Appends a fully-encoded record (payload + trailing CRC) to `dst`.
// Caller owns `dst`. `fingerprint`, `key`, `value` are borrowed.
// The returned byte count is dst->size() growth for this record, which
// UvlFileBuilder uses to compute the offset of the NEXT record.
size_t EncodeUvlRecord(std::string* dst,
                       const UvlFingerprint& fingerprint,
                       UvlCompression compression,
                       const Slice& key,
                       const Slice& value);

// Parses one record from the head of `input`, advancing the slice past
// the consumed bytes on success. On success `out->key`/`out->value`
// point into `out->key_buf`/`out->value_buf` (owned copies).
//
// Returns Corruption on: truncation, bad varint, CRC mismatch, or
// compression byte out of range.
Status DecodeUvlRecord(Slice* input, UvlRecord* out);

// Exposed for testing: byte offset of the CRC field within an encoded
// record whose total size is `record_size`. Equal to record_size - 4.
inline size_t UvlRecordCrcOffset(size_t record_size) {
  return record_size - sizeof(uint32_t);
}

}  // namespace ROCKSDB_NAMESPACE
