//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// UvlFileGarbage — VersionEdit record describing a UVL file that has
// been rewritten by ITEM-18b's GC pass. Mirrors BlobFileGarbage in
// shape (file number + garbage accounting) but carries the forwarding
// reference to the rewriter's output file instead of per-blob
// reference deltas, because UVL garbage is tracked per-file by
// UvlGarbageMeter and not per-record.
//
// On-disk format (within a kUvlFileGarbage VersionEdit tag):
//   varint old_uvl_file_number
//   varint new_uvl_file_number   // 0 if rewrite produced no file
//   varint column_family_id
//   varint live_records_copied
//   varint live_bytes_copied
//   custom-fields ... varint kEndMarker
//
// Forward-compat scheme matches UvlFileAddition.

#pragma once

#include <cstdint>
#include <iosfwd>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

class UvlFileGarbage {
 public:
  UvlFileGarbage() = default;

  UvlFileGarbage(uint64_t old_uvl_file_number, uint64_t new_uvl_file_number,
                 uint32_t column_family_id, uint64_t live_records_copied,
                 uint64_t live_bytes_copied)
      : old_uvl_file_number_(old_uvl_file_number),
        new_uvl_file_number_(new_uvl_file_number),
        column_family_id_(column_family_id),
        live_records_copied_(live_records_copied),
        live_bytes_copied_(live_bytes_copied) {}

  uint64_t GetOldUvlFileNumber() const { return old_uvl_file_number_; }
  uint64_t GetNewUvlFileNumber() const { return new_uvl_file_number_; }
  uint32_t GetColumnFamilyId() const { return column_family_id_; }
  uint64_t GetLiveRecordsCopied() const { return live_records_copied_; }
  uint64_t GetLiveBytesCopied() const { return live_bytes_copied_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  uint64_t old_uvl_file_number_ = 0;
  uint64_t new_uvl_file_number_ = 0;
  uint32_t column_family_id_ = 0;
  uint64_t live_records_copied_ = 0;
  uint64_t live_bytes_copied_ = 0;
};

bool operator==(const UvlFileGarbage& lhs, const UvlFileGarbage& rhs);
bool operator!=(const UvlFileGarbage& lhs, const UvlFileGarbage& rhs);

std::ostream& operator<<(std::ostream& os, const UvlFileGarbage& garbage);
JSONWriter& operator<<(JSONWriter& jw, const UvlFileGarbage& garbage);

}  // namespace ROCKSDB_NAMESPACE
