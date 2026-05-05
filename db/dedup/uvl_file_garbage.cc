//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_file_garbage.h"

#include <ostream>
#include <sstream>

#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

enum UvlFileGarbage::CustomFieldTags : uint32_t {
  kEndMarker = 0,

  // Forward-compatible (safely ignorable) fields below.

  /////////////////////////////////////////////////////////////////////
  kForwardIncompatibleMask = 1 << 6,

  // Forward-incompatible fields above (old readers must fail).
};

void UvlFileGarbage::EncodeTo(std::string* output) const {
  PutVarint64(output, old_uvl_file_number_);
  PutVarint64(output, new_uvl_file_number_);
  PutVarint32(output, column_family_id_);
  PutVarint64(output, live_records_copied_);
  PutVarint64(output, live_bytes_copied_);

  TEST_SYNC_POINT_CALLBACK("UvlFileGarbage::EncodeTo::CustomFields", output);

  PutVarint32(output, kEndMarker);
}

Status UvlFileGarbage::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "UvlFileGarbage";

  if (!GetVarint64(input, &old_uvl_file_number_)) {
    return Status::Corruption(class_name, "Error decoding old_uvl_file_number");
  }
  if (!GetVarint64(input, &new_uvl_file_number_)) {
    return Status::Corruption(class_name, "Error decoding new_uvl_file_number");
  }
  if (!GetVarint32(input, &column_family_id_)) {
    return Status::Corruption(class_name, "Error decoding column_family_id");
  }
  if (!GetVarint64(input, &live_records_copied_)) {
    return Status::Corruption(class_name, "Error decoding live_records_copied");
  }
  if (!GetVarint64(input, &live_bytes_copied_)) {
    return Status::Corruption(class_name, "Error decoding live_bytes_copied");
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

std::string UvlFileGarbage::DebugString() const {
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}

std::string UvlFileGarbage::DebugJSON() const {
  JSONWriter jw;
  jw << *this;
  jw.EndObject();
  return jw.Get();
}

bool operator==(const UvlFileGarbage& lhs, const UvlFileGarbage& rhs) {
  return lhs.GetOldUvlFileNumber() == rhs.GetOldUvlFileNumber() &&
         lhs.GetNewUvlFileNumber() == rhs.GetNewUvlFileNumber() &&
         lhs.GetColumnFamilyId() == rhs.GetColumnFamilyId() &&
         lhs.GetLiveRecordsCopied() == rhs.GetLiveRecordsCopied() &&
         lhs.GetLiveBytesCopied() == rhs.GetLiveBytesCopied();
}

bool operator!=(const UvlFileGarbage& lhs, const UvlFileGarbage& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os, const UvlFileGarbage& garbage) {
  os << "old_uvl_file_number: " << garbage.GetOldUvlFileNumber()
     << " new_uvl_file_number: " << garbage.GetNewUvlFileNumber()
     << " column_family_id: " << garbage.GetColumnFamilyId()
     << " live_records_copied: " << garbage.GetLiveRecordsCopied()
     << " live_bytes_copied: " << garbage.GetLiveBytesCopied();
  return os;
}

JSONWriter& operator<<(JSONWriter& jw, const UvlFileGarbage& garbage) {
  jw << "OldUvlFileNumber" << garbage.GetOldUvlFileNumber()
     << "NewUvlFileNumber" << garbage.GetNewUvlFileNumber()
     << "ColumnFamilyId" << garbage.GetColumnFamilyId()
     << "LiveRecordsCopied" << garbage.GetLiveRecordsCopied()
     << "LiveBytesCopied" << garbage.GetLiveBytesCopied();
  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
