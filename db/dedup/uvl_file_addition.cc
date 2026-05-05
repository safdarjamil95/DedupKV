//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_file_addition.h"

#include <ostream>
#include <sstream>

#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Custom-field tags get persisted in the manifest. Existing values
// must never be modified or repurposed; new fields go above the
// kForwardIncompatibleMask line if they require old readers to fail,
// below if they're safely ignorable.
enum UvlFileAddition::CustomFieldTags : uint32_t {
  kEndMarker = 0,

  // Add forward-compatible (old reader can ignore) fields here.

  /////////////////////////////////////////////////////////////////////
  kForwardIncompatibleMask = 1 << 6,

  // Add forward-incompatible fields here.
};

void UvlFileAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, uvl_file_number_);
  PutVarint32(output, column_family_id_);
  PutVarint64(output, total_uvl_records_);
  PutVarint64(output, total_uvl_bytes_);
  PutVarint64(output, creation_time_);

  TEST_SYNC_POINT_CALLBACK("UvlFileAddition::EncodeTo::CustomFields", output);

  PutVarint32(output, kEndMarker);
}

Status UvlFileAddition::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "UvlFileAddition";

  if (!GetVarint64(input, &uvl_file_number_)) {
    return Status::Corruption(class_name, "Error decoding uvl_file_number");
  }
  if (!GetVarint32(input, &column_family_id_)) {
    return Status::Corruption(class_name, "Error decoding column_family_id");
  }
  if (!GetVarint64(input, &total_uvl_records_)) {
    return Status::Corruption(class_name, "Error decoding total_uvl_records");
  }
  if (!GetVarint64(input, &total_uvl_bytes_)) {
    return Status::Corruption(class_name, "Error decoding total_uvl_bytes");
  }
  if (!GetVarint64(input, &creation_time_)) {
    return Status::Corruption(class_name, "Error decoding creation_time");
  }

  while (true) {
    uint32_t custom_field_tag = 0;
    if (!GetVarint32(input, &custom_field_tag)) {
      return Status::Corruption(class_name,
                                "Error decoding custom field tag");
    }
    if (custom_field_tag == kEndMarker) {
      break;
    }
    if (custom_field_tag & kForwardIncompatibleMask) {
      return Status::Corruption(
          class_name, "Forward incompatible custom field encountered");
    }
    Slice custom_field_value;
    if (!GetLengthPrefixedSlice(input, &custom_field_value)) {
      return Status::Corruption(class_name,
                                "Error decoding custom field value");
    }
  }
  return Status::OK();
}

std::string UvlFileAddition::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

std::string UvlFileAddition::DebugJSON() const {
  JSONWriter jw;
  jw << *this;
  jw.EndObject();
  return jw.Get();
}

bool operator==(const UvlFileAddition& lhs, const UvlFileAddition& rhs) {
  return lhs.GetUvlFileNumber() == rhs.GetUvlFileNumber() &&
         lhs.GetColumnFamilyId() == rhs.GetColumnFamilyId() &&
         lhs.GetTotalUvlRecords() == rhs.GetTotalUvlRecords() &&
         lhs.GetTotalUvlBytes() == rhs.GetTotalUvlBytes() &&
         lhs.GetCreationTime() == rhs.GetCreationTime();
}

bool operator!=(const UvlFileAddition& lhs, const UvlFileAddition& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os,
                         const UvlFileAddition& addition) {
  os << "uvl_file_number: " << addition.GetUvlFileNumber()
     << " column_family_id: " << addition.GetColumnFamilyId()
     << " total_uvl_records: " << addition.GetTotalUvlRecords()
     << " total_uvl_bytes: " << addition.GetTotalUvlBytes()
     << " creation_time: " << addition.GetCreationTime();
  return os;
}

JSONWriter& operator<<(JSONWriter& jw, const UvlFileAddition& addition) {
  jw << "UvlFileNumber" << addition.GetUvlFileNumber()
     << "ColumnFamilyId" << addition.GetColumnFamilyId()
     << "TotalUvlRecords" << addition.GetTotalUvlRecords()
     << "TotalUvlBytes" << addition.GetTotalUvlBytes()
     << "CreationTime" << addition.GetCreationTime();
  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
