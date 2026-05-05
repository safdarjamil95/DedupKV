//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_garbage_meter.h"

namespace ROCKSDB_NAMESPACE {

void UvlGarbageMeter::Accumulate(uint64_t uvl_file_number, uint64_t bytes) {
  if (bytes == 0) return;
  std::lock_guard<std::mutex> lk(mu_);
  invalid_bytes_[uvl_file_number] += bytes;
}

uint64_t UvlGarbageMeter::InvalidBytes(uint64_t uvl_file_number) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = invalid_bytes_.find(uvl_file_number);
  return it == invalid_bytes_.end() ? 0 : it->second;
}

void UvlGarbageMeter::Forget(uint64_t uvl_file_number) {
  std::lock_guard<std::mutex> lk(mu_);
  invalid_bytes_.erase(uvl_file_number);
}

std::unordered_map<uint64_t, uint64_t> UvlGarbageMeter::Snapshot() const {
  std::lock_guard<std::mutex> lk(mu_);
  return invalid_bytes_;
}

}  // namespace ROCKSDB_NAMESPACE
