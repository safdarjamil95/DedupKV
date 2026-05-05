//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// DWQ (Deduplication Work Queue) — FIFO of per-WAL offline-dedup
// tasks. See plan.md §Component 2 (§4.4 of the manuscript) and
// AMBIGUITY-006 (strict FIFO, single consumer).
//
// Each DWQEntry wraps a WAL file number, the cf_id that produced the
// IMT, a Bloom filter built from the IMT's keys (for Get redirection),
// and a monotone state flag. Producers are FlushJob threads that took
// the elastic controller's "offline" branch; the consumer is the
// dedicated offline dedup thread (ITEM-09). Get paths (ITEM-16)
// inspect DWQ entries without removing them.
//
// Lifetime invariant: an entry is visible to Get (via
// KeyMightBePresent) from Push until the consumer calls PopReady().
// shared_ptr ownership keeps the DWQEntry object alive for any Get
// still mid-walk. WAL-file retention while entries are in-flight is
// an orthogonal concern handled in ITEM-09.
//
// DEC-006: PopReady returns shared_ptr (not unique_ptr as originally
// sketched) so the offline thread can inspect the head with Get-path
// ownership semantics. See plan.md decision log.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class FilterBitsReader;

class DWQEntry {
 public:
  enum class State : uint8_t {
    kInactive = 0,  // enqueued, not yet picked up
    kActive = 1,    // offline thread is processing
    kComplete = 2,  // processing finished; WAL file may be deleted soon
  };

  // Takes ownership of `bloom_filter`. `filter_backing_bytes` is the
  // encoded filter blob that `bloom_filter` indexes into; we retain it
  // so the FilterBitsReader's internal Slice stays valid for the
  // entry's lifetime.
  DWQEntry(uint64_t wal_file_number, uint32_t cf_id,
           std::string filter_backing_bytes,
           std::unique_ptr<FilterBitsReader> bloom_filter);

  ~DWQEntry();

  DWQEntry(const DWQEntry&) = delete;
  DWQEntry& operator=(const DWQEntry&) = delete;

  uint64_t wal_file_number() const { return wal_file_number_; }
  uint32_t cf_id() const { return cf_id_; }

  State GetState() const { return state_.load(std::memory_order_acquire); }

  // Atomic monotone transition: Inactive → Active → Complete. Returns
  // true if the new state was reached; false if `next` would move
  // backwards or equal the current state.
  bool TransitionTo(State next);

  // Probe the bloom filter. Returns true on possible match (may be a
  // false positive); false is authoritative.
  bool KeyMightBePresent(const Slice& key) const;

 private:
  const uint64_t wal_file_number_;
  const uint32_t cf_id_;
  const std::string filter_backing_bytes_;
  const std::unique_ptr<FilterBitsReader> bloom_filter_;
  std::atomic<State> state_{State::kInactive};
};

class DWQ {
 public:
  DWQ() = default;
  DWQ(const DWQ&) = delete;
  DWQ& operator=(const DWQ&) = delete;

  // Enqueue a new entry at the tail. Wakes one waiting consumer.
  void Push(std::shared_ptr<DWQEntry> entry);

  // Return the head entry without removing it. nullptr if empty.
  std::shared_ptr<DWQEntry> PeekHead() const;

  // Pop the head entry (non-blocking). Returns nullptr if empty.
  // Caller is expected to have already transitioned the entry through
  // Active → Complete before popping; this method does not enforce
  // that, it simply removes the head.
  std::shared_ptr<DWQEntry> PopReady();

  // Block until an entry is available, then return it (does not
  // remove). Used by the offline thread's work loop. Returns nullptr
  // if Shutdown() was called while waiting.
  std::shared_ptr<DWQEntry> WaitForHead();

  // Unblocks any WaitForHead caller so the offline thread can exit.
  void Shutdown();

  // Test whether any entry's bloom filter matches `key`. Hits are
  // appended in FIFO order so the caller can walk them newest-to-oldest
  // as the Get-path spec requires. Returns true if at least one hit.
  bool KeyMightBePresent(const Slice& key,
                         std::vector<std::shared_ptr<DWQEntry>>* hits) const;

  size_t Size() const;

  // ITEM-09c: lowest WAL file number still referenced by any live
  // DWQEntry. Returns 0 when the queue is empty (no pinning). Used by
  // DBImpl::MinLogNumberToKeep() to prevent RocksDB from reclaiming a
  // WAL while the offline worker still needs to drain it.
  uint64_t EarliestWalNumber() const;

  // Drop all entries. Intended for DB close / test cleanup. Any
  // shared_ptrs still held elsewhere keep their DWQEntries alive.
  void Clear();

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::shared_ptr<DWQEntry>> entries_;
  bool shutdown_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
