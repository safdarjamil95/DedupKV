//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Random-access reader for a DedupKV UVL (Unique Value Log) file.
// Counterpart to UvlFileBuilder (ITEM-02): validates the 24-byte
// UvlHeader on Open() and exposes two primitives:
//   * GetValue(offset, record_size, ...) — decode a full record, verify
//     its CRC, and return the value bytes plus compression byte.
//   * GetFingerprint(offset, ...) — cheap 20-byte read used by the
//     compaction refcount-decrement path (ITEM-17) and recovery
//     reconciliation (ITEM-19).
//
// LZ4 decompression of the small-value DGD branch is NOT performed
// here; callers that need raw bytes (DGD / Get path) will decompress
// using RocksDB's Compressor/Decompressor machinery once they have
// access to ImmutableOptions. See DEC-004 in plan.md.

#pragma once

#include <cstdint>
#include <memory>

#include "db/dedup/uvl_log_format.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReader;
class PinnableSlice;

class UvlFileReader {
 public:
  // Opens a UVL file over a pre-constructed RandomAccessFileReader.
  // Reads and validates the 24-byte UvlHeader; returns Corruption on
  // magic / version mismatch or IOError on read failure.
  static Status Open(std::unique_ptr<RandomAccessFileReader> file_reader,
                     uint64_t file_size,
                     std::unique_ptr<UvlFileReader>* out);

  UvlFileReader(const UvlFileReader&) = delete;
  UvlFileReader& operator=(const UvlFileReader&) = delete;

  ~UvlFileReader();

  // Reads the record that UvlFileBuilder placed at [offset,
  // offset+record_size), verifies its CRC via the ITEM-01 codec, and
  // populates *value with the decoded value bytes (owned copy in *value's
  // internal buffer so the PinnableSlice outlives this call).
  // *compression reports the per-record compression byte (raw vs.
  // LZ4-inline); callers that need the raw value bytes for LZ4-inline
  // records must decompress downstream.
  Status GetValue(uint64_t offset, uint64_t record_size,
                  PinnableSlice* value, UvlCompression* compression) const;

  // Reads only the 20-byte fingerprint prefix at `offset`. Does NOT
  // verify the full record's CRC — callers needing verification should
  // use GetValue() instead. Intended for hot compaction paths where
  // the prefix suffices to look up CIT by fingerprint.
  Status GetFingerprint(uint64_t offset, UvlFingerprint* fingerprint) const;

  const UvlHeader& header() const { return header_; }
  uint64_t file_size() const { return file_size_; }

 private:
  UvlFileReader(std::unique_ptr<RandomAccessFileReader> file_reader,
                uint64_t file_size, const UvlHeader& header);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  uint64_t file_size_;
  UvlHeader header_;
};

}  // namespace ROCKSDB_NAMESPACE
