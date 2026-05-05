//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/cit_cold_tier.h"

#include <utility>

namespace ROCKSDB_NAMESPACE {

Status InMemoryColdTier::Put(const UvlFingerprint& fp, const CITEntry& entry) {
  std::lock_guard<std::mutex> lock(mu_);
  map_[fp] = entry;
  return Status::OK();
}

Status InMemoryColdTier::Get(const UvlFingerprint& fp, CITEntry* out,
                             bool* found) const {
  if (found == nullptr) {
    return Status::InvalidArgument("found out-param required");
  }
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(fp);
  if (it == map_.end()) {
    *found = false;
    return Status::OK();
  }
  *found = true;
  if (out != nullptr) {
    *out = it->second;
  }
  return Status::OK();
}

Status InMemoryColdTier::Erase(const UvlFingerprint& fp) {
  std::lock_guard<std::mutex> lock(mu_);
  map_.erase(fp);
  return Status::OK();
}

uint64_t InMemoryColdTier::ApproximateSize() const {
  std::lock_guard<std::mutex> lock(mu_);
  return map_.size();
}

CIT::EvictionCallback MakeColdTierEvictionCallback(
    ICITColdTier* cold_tier,
    std::function<void(const UvlFingerprint&, const Status&)> on_error) {
  return [cold_tier, on_error = std::move(on_error)](
             const UvlFingerprint& fp, const CITEntry& entry) {
    if (cold_tier == nullptr) return;
    Status s = cold_tier->Put(fp, entry);
    if (!s.ok() && on_error) {
      on_error(fp, s);
    }
  };
}

}  // namespace ROCKSDB_NAMESPACE
