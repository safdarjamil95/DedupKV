//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/cit.h"

#include <algorithm>
#include <limits>

namespace ROCKSDB_NAMESPACE {

uint64_t CIT::NextLruSeqLocked() { return ++lru_counter_; }

bool CIT::Lookup(const UvlFingerprint& fp, CITEntry* out, bool touch_lru) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(fp);
  if (it == map_.end()) {
    misses_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  if (touch_lru) {
    it->second.lru_seq = NextLruSeqLocked();
  }
  if (out != nullptr) {
    *out = it->second;
  }
  hits_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

bool CIT::Insert(const UvlFingerprint& fp, const CITEntry& entry) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(fp);
  if (it != map_.end()) {
    return false;
  }
  CITEntry e = entry;
  e.lru_seq = NextLruSeqLocked();
  map_.emplace(fp, e);
  return true;
}

bool CIT::LookupOrInsert(const UvlFingerprint& fp, const CITEntry& new_entry,
                         CITEntry* out) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(fp);
  if (it != map_.end()) {
    // Hit — bump refcount + LRU, return the existing entry.
    it->second.refcount += 1;
    it->second.lru_seq = NextLruSeqLocked();
    if (out != nullptr) {
      *out = it->second;
    }
    hits_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }
  // Miss — install new_entry with refcount forced to 1.
  CITEntry e = new_entry;
  e.refcount = 1;
  e.lru_seq = NextLruSeqLocked();
  auto [iter, inserted] = map_.emplace(fp, e);
  (void)inserted;
  if (out != nullptr) {
    *out = iter->second;
  }
  misses_.fetch_add(1, std::memory_order_relaxed);
  return false;
}

uint32_t CIT::IncRefcount(const UvlFingerprint& fp) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(fp);
  if (it == map_.end()) {
    return std::numeric_limits<uint32_t>::max();
  }
  it->second.refcount += 1;
  // Inc does not bump LRU — refcount activity is decoupled from the
  // read-recency signal that drives eviction.
  return it->second.refcount;
}

uint32_t CIT::DecRefcount(const UvlFingerprint& fp) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(fp);
  if (it == map_.end()) {
    return std::numeric_limits<uint32_t>::max();
  }
  if (it->second.refcount > 0) {
    it->second.refcount -= 1;
  }
  return it->second.refcount;
}

bool CIT::RetargetLocation(const UvlFingerprint& fp,
                           uint64_t expected_old_file,
                           uint64_t expected_old_offset, uint64_t new_file,
                           uint64_t new_offset) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = map_.find(fp);
  if (it == map_.end()) {
    return false;
  }
  if (it->second.uvl_file != expected_old_file ||
      it->second.offset != expected_old_offset) {
    return false;
  }
  it->second.uvl_file = new_file;
  it->second.offset = new_offset;
  return true;
}

size_t CIT::EvictColdEntries(size_t k) {
  if (k == 0) {
    return 0;
  }
  std::lock_guard<std::mutex> lock(mu_);
  if (map_.empty()) {
    return 0;
  }
  const size_t to_evict = std::min(k, map_.size());

  // Collect iterators sorted by lru_seq ascending. O(n log n) — called
  // off the hot path (flush-time eviction, not per-Put).
  std::vector<std::unordered_map<UvlFingerprint, CITEntry,
                                 UvlFingerprintHash>::iterator>
      iters;
  iters.reserve(map_.size());
  for (auto it = map_.begin(); it != map_.end(); ++it) {
    iters.push_back(it);
  }
  std::partial_sort(iters.begin(), iters.begin() + to_evict, iters.end(),
                    [](auto a, auto b) {
                      return a->second.lru_seq < b->second.lru_seq;
                    });

  for (size_t i = 0; i < to_evict; ++i) {
    if (eviction_cb_) {
      eviction_cb_(iters[i]->first, iters[i]->second);
    }
    map_.erase(iters[i]);
  }
  evictions_.fetch_add(to_evict, std::memory_order_relaxed);
  return to_evict;
}

void CIT::SetEvictionCallback(EvictionCallback cb) {
  std::lock_guard<std::mutex> lock(mu_);
  eviction_cb_ = std::move(cb);
}

void CIT::Snapshot(
    std::vector<std::pair<UvlFingerprint, CITEntry>>* out) const {
  if (out == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(mu_);
  out->clear();
  out->reserve(map_.size());
  for (const auto& kv : map_) {
    out->emplace_back(kv.first, kv.second);
  }
}

size_t CIT::Size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return map_.size();
}

}  // namespace ROCKSDB_NAMESPACE
