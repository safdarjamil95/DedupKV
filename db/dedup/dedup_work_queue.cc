//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dedup_work_queue.h"

#include <utility>

#include "table/block_based/filter_policy_internal.h"

namespace ROCKSDB_NAMESPACE {

DWQEntry::DWQEntry(uint64_t wal_file_number, uint32_t cf_id,
                   std::string filter_backing_bytes,
                   std::unique_ptr<FilterBitsReader> bloom_filter)
    : wal_file_number_(wal_file_number),
      cf_id_(cf_id),
      filter_backing_bytes_(std::move(filter_backing_bytes)),
      bloom_filter_(std::move(bloom_filter)) {}

DWQEntry::~DWQEntry() = default;

bool DWQEntry::TransitionTo(State next) {
  State expected = state_.load(std::memory_order_acquire);
  for (;;) {
    // Strictly monotonic: next must be > current.
    if (static_cast<uint8_t>(next) <= static_cast<uint8_t>(expected)) {
      return false;
    }
    if (state_.compare_exchange_weak(expected, next,
                                     std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      return true;
    }
    // CAS raced; `expected` has been refreshed — loop and re-check.
  }
}

bool DWQEntry::KeyMightBePresent(const Slice& key) const {
  if (bloom_filter_ == nullptr) {
    // Defensive: no filter → conservatively match. In practice the
    // producer (FlushJob) always supplies a filter.
    return true;
  }
  return bloom_filter_->MayMatch(key);
}

void DWQ::Push(std::shared_ptr<DWQEntry> entry) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    entries_.push_back(std::move(entry));
  }
  cv_.notify_one();
}

std::shared_ptr<DWQEntry> DWQ::PeekHead() const {
  std::lock_guard<std::mutex> lock(mu_);
  if (entries_.empty()) {
    return nullptr;
  }
  return entries_.front();
}

std::shared_ptr<DWQEntry> DWQ::PopReady() {
  std::lock_guard<std::mutex> lock(mu_);
  if (entries_.empty()) {
    return nullptr;
  }
  auto head = std::move(entries_.front());
  entries_.pop_front();
  return head;
}

std::shared_ptr<DWQEntry> DWQ::WaitForHead() {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return shutdown_ || !entries_.empty(); });
  if (shutdown_ && entries_.empty()) {
    return nullptr;
  }
  return entries_.front();
}

void DWQ::Shutdown() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    shutdown_ = true;
  }
  cv_.notify_all();
}

bool DWQ::KeyMightBePresent(
    const Slice& key,
    std::vector<std::shared_ptr<DWQEntry>>* hits) const {
  if (hits != nullptr) {
    hits->clear();
  }
  // Snapshot the queue under the lock so the bloom-filter MayMatch()
  // calls (which may be non-trivial) run without blocking producers.
  std::vector<std::shared_ptr<DWQEntry>> snapshot;
  {
    std::lock_guard<std::mutex> lock(mu_);
    snapshot.assign(entries_.begin(), entries_.end());
  }
  bool any = false;
  for (const auto& e : snapshot) {
    if (e->KeyMightBePresent(key)) {
      any = true;
      if (hits != nullptr) {
        hits->push_back(e);
      }
    }
  }
  return any;
}

size_t DWQ::Size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return entries_.size();
}

uint64_t DWQ::EarliestWalNumber() const {
  std::lock_guard<std::mutex> lock(mu_);
  uint64_t earliest = 0;
  for (const auto& e : entries_) {
    uint64_t n = e->wal_file_number();
    if (n != 0 && (earliest == 0 || n < earliest)) {
      earliest = n;
    }
  }
  return earliest;
}

void DWQ::Clear() {
  std::lock_guard<std::mutex> lock(mu_);
  entries_.clear();
}

}  // namespace ROCKSDB_NAMESPACE
