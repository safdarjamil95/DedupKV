//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/memory_monitor.h"

namespace ROCKSDB_NAMESPACE {

DedupMemoryMonitor::DedupMemoryMonitor(uint64_t capacity_bytes)
    : capacity_bytes_(capacity_bytes) {}

void DedupMemoryMonitor::OnMemtableAlloc(uint64_t bytes) {
  memtable_bytes_.fetch_add(bytes, std::memory_order_relaxed);
  total_allocated_.fetch_add(bytes, std::memory_order_relaxed);
}

void DedupMemoryMonitor::OnMemtableFree(uint64_t bytes) {
  total_freed_.fetch_add(bytes, std::memory_order_relaxed);
  // Saturating subtract via CAS loop.
  uint64_t prev = memtable_bytes_.load(std::memory_order_relaxed);
  for (;;) {
    const uint64_t next = (prev > bytes) ? (prev - bytes) : 0;
    if (memtable_bytes_.compare_exchange_weak(prev, next,
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
      return;
    }
    // prev is refreshed by CAS — loop.
  }
}

void DedupMemoryMonitor::SetCapacity(uint64_t capacity_bytes) {
  capacity_bytes_.store(capacity_bytes, std::memory_order_relaxed);
}

double DedupMemoryMonitor::Utilization() const {
  const uint64_t cap = capacity_bytes_.load(std::memory_order_relaxed);
  if (cap == 0) {
    return 0.0;
  }
  const uint64_t used = memtable_bytes_.load(std::memory_order_relaxed);
  return static_cast<double>(used) / static_cast<double>(cap);
}

}  // namespace ROCKSDB_NAMESPACE
