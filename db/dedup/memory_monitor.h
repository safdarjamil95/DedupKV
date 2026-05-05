//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// DedupMemoryMonitor — tracks bytes charged to the active + immutable
// memtables of a dedup-enabled CF. Consumed by the elastic controller
// (ITEM-15 / plan §4.5): when utilization crosses a configured
// threshold, new FLUSHes hand off to the offline path instead of
// running inline dedup.
//
// Per AMBIGUITY-011: numerator is the running total of memtable arena
// allocations; denominator is the CF's write_buffer_size *
// max_write_buffer_number.
//
// NB: the memtable-side call-site hooks (OnMemtableAlloc /
// OnMemtableFree from db/memtable.cc, db/memtable_list.cc) are NOT
// wired here. See DEC-008 in plan.md — the hooks land in Phase III
// (ITEM-11/12) where the DBImpl dedup context exists and can route
// them to the right monitor instance. This file ships only the
// self-contained counter + test coverage.

#pragma once

#include <atomic>
#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class DedupMemoryMonitor {
 public:
  // `capacity_bytes` must match the CF's
  // write_buffer_size * max_write_buffer_number. A capacity of zero
  // makes Utilization() report 0.0 and Over() return false — safe
  // sentinel for "monitor not active yet".
  explicit DedupMemoryMonitor(uint64_t capacity_bytes = 0);

  DedupMemoryMonitor(const DedupMemoryMonitor&) = delete;
  DedupMemoryMonitor& operator=(const DedupMemoryMonitor&) = delete;

  // Called from MemTable arena-allocation points.
  void OnMemtableAlloc(uint64_t bytes);

  // Called when a memtable is destroyed / its allocations released.
  // Saturates at zero: a mismatched free (fewer allocs than expected)
  // leaves the counter at zero rather than underflowing, because
  // underflow would silently misreport utilization for the lifetime
  // of the DB.
  void OnMemtableFree(uint64_t bytes);

  // Current numerator.
  uint64_t MemtableBytes() const {
    return memtable_bytes_.load(std::memory_order_relaxed);
  }

  // Denominator. Mutable via SetCapacity for the rare case of a
  // dynamic option change; we snapshot into a std::atomic to keep
  // readers lock-free.
  uint64_t CapacityBytes() const {
    return capacity_bytes_.load(std::memory_order_relaxed);
  }

  void SetCapacity(uint64_t capacity_bytes);

  // memtable_bytes / capacity_bytes, or 0.0 if capacity is zero.
  // Can exceed 1.0 when the WriteBufferManager has temporarily
  // overrun its budget (RocksDB permits brief excursions).
  double Utilization() const;

  bool Over(double threshold) const {
    return Utilization() >= threshold;
  }

  // Observability. Monotonic counters; not reset.
  uint64_t TotalAllocated() const {
    return total_allocated_.load(std::memory_order_relaxed);
  }
  uint64_t TotalFreed() const {
    return total_freed_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<uint64_t> memtable_bytes_{0};
  std::atomic<uint64_t> capacity_bytes_{0};
  std::atomic<uint64_t> total_allocated_{0};
  std::atomic<uint64_t> total_freed_{0};
};

}  // namespace ROCKSDB_NAMESPACE
