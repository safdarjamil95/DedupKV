//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// UvlFileAddition — VersionEdit record describing a newly-installed
// DedupKV UVL file. Mirrors BlobFileAddition (db/blob/) but minus the
// checksum fields: UVL records carry per-record CRC32c so we don't
// need a per-file checksum to validate the bytes.
//
// On-disk format (within a kUvlFileAddition VersionEdit tag):
//   varint uvl_file_number
//   varint column_family_id
//   varint total_uvl_records
//   varint total_uvl_bytes
//   varint creation_time
//   custom-fields ... varint kEndMarker
//
// The custom-fields footer is forward-compatible per BlobFileAddition's
// scheme: unknown tags below kForwardIncompatibleMask are ignored,
// unknown tags at-or-above the mask trip a Corruption.

#pragma once

#include <cstdint>
#include <iosfwd>
#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

class UvlFileAddition {
 public:
  UvlFileAddition() = default;

  UvlFileAddition(uint64_t uvl_file_number, uint32_t column_family_id,
                  uint64_t total_uvl_records, uint64_t total_uvl_bytes,
                  uint64_t creation_time)
      : uvl_file_number_(uvl_file_number),
        column_family_id_(column_family_id),
        total_uvl_records_(total_uvl_records),
        total_uvl_bytes_(total_uvl_bytes),
        creation_time_(creation_time) {}

  uint64_t GetUvlFileNumber() const { return uvl_file_number_; }
  uint32_t GetColumnFamilyId() const { return column_family_id_; }
  uint64_t GetTotalUvlRecords() const { return total_uvl_records_; }
  uint64_t GetTotalUvlBytes() const { return total_uvl_bytes_; }
  uint64_t GetCreationTime() const { return creation_time_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  // Forward-compat scheme: same convention as BlobFileAddition.
  enum CustomFieldTags : uint32_t;

  uint64_t uvl_file_number_ = 0;
  uint32_t column_family_id_ = 0;
  uint64_t total_uvl_records_ = 0;
  uint64_t total_uvl_bytes_ = 0;
  uint64_t creation_time_ = 0;
};

bool operator==(const UvlFileAddition& lhs, const UvlFileAddition& rhs);
bool operator!=(const UvlFileAddition& lhs, const UvlFileAddition& rhs);

std::ostream& operator<<(std::ostream& os, const UvlFileAddition& addition);
JSONWriter& operator<<(JSONWriter& jw, const UvlFileAddition& addition);

}  // namespace ROCKSDB_NAMESPACE
