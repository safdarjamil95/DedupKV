//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// CIT (Chunk Index Table) — in-memory hot tier.
//
// Maps SHA1 fingerprint → CITEntry{uvl_file, offset, size, refcount,
// compression, lru_seq}. Accessed by the flush thread (inline dedup),
// the offline dedup thread, and the compaction thread (refcount
// decrement for GC). A single std::mutex serialises all operations;
// see AMBIGUITY-002 in plan.md — SHA1 and I/O happen outside the lock,
// so the critical section is only the hash-table operation itself.
//
// The combined LookupOrInsert() entry point implements the single-
// critical-section "lookup → if miss insert" that ITEM-04's review
// note calls out: under racing flush paths, at most one insert wins
// per fingerprint and the loser observes the winner's entry with
// refcount already incremented.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/dedup/uvl_log_format.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct CITEntry {
  uint64_t uvl_file = 0;     // UVL file number containing the value
  uint64_t offset = 0;       // byte offset of the record within that file
  uint32_t size = 0;         // encoded record size (header + payload + crc)
  uint32_t refcount = 0;     // number of SST BlobIndexes that reference it
  UvlCompression compression = UvlCompression::kRaw;
  uint64_t lru_seq = 0;      // last-touched monotone counter (for eviction)
};

// Hash functor for UvlFingerprint — SHA1 output is already quasi-
// uniform, so mixing beyond "take the first size_t bytes" is wasted
// work on the hot path.
struct UvlFingerprintHash {
  size_t operator()(const UvlFingerprint& fp) const noexcept {
    size_t h = 0;
    std::memcpy(&h, fp.data(), sizeof(h));
    return h;
  }
};

class CIT {
 public:
  // Callback invoked by EvictColdEntries for each entry removed from
  // the hot tier. Used by ITEM-05 to push into the cold-tier LSM.
  using EvictionCallback =
      std::function<void(const UvlFingerprint&, const CITEntry&)>;

  CIT() = default;
  CIT(const CIT&) = delete;
  CIT& operator=(const CIT&) = delete;

  // Look up `fp`. On hit, copies the entry into *out and returns true;
  // *out's refcount is left unchanged. `touch_lru=true` (default)
  // refreshes the entry's LRU sequence so concurrent eviction doesn't
  // reap it immediately.
  bool Lookup(const UvlFingerprint& fp, CITEntry* out, bool touch_lru = true);

  // Insert `entry` under `fp` if no entry exists yet. Returns true on
  // insertion. Refcount is taken from `entry` (normally 1). lru_seq in
  // `entry` is ignored; this method stamps a fresh value.
  bool Insert(const UvlFingerprint& fp, const CITEntry& entry);

  // Atomic lookup-then-increment-or-insert. Under a single critical
  // section:
  //   * If `fp` is present: increments its refcount and returns true;
  //     *out is filled with the existing entry (refcount reflects the
  //     incremented value).
  //   * If `fp` is absent: inserts `new_entry` (refcount forced to 1),
  //     returns false, *out is filled with the inserted entry.
  // This is the flush-path fast path.
  bool LookupOrInsert(const UvlFingerprint& fp, const CITEntry& new_entry,
                      CITEntry* out);

  // Adjust the refcount of `fp`. Returns the post-increment value, or
  // UINT32_MAX if `fp` was not found (out-of-band sentinel — callers
  // should check via Lookup first if a missing entry is possible).
  uint32_t IncRefcount(const UvlFingerprint& fp);
  // Dec saturates at 0; callers that need to know "refcount hit zero"
  // should compare the returned value to 0u.
  uint32_t DecRefcount(const UvlFingerprint& fp);

  // ITEM-18b: retarget the `{uvl_file, offset}` pair recorded for `fp`
  // to a new location. Refcount, size, compression, and lru_seq are
  // preserved. Returns true on success; false if `fp` is absent or
  // `expected_old_file/expected_old_offset` don't match the current
  // entry (another thread moved it first — the caller should skip).
  // The check lets GC safely skip records that have already been moved
  // by a concurrent pass.
  bool RetargetLocation(const UvlFingerprint& fp, uint64_t expected_old_file,
                        uint64_t expected_old_offset, uint64_t new_file,
                        uint64_t new_offset);

  // Remove `k` entries with the smallest lru_seq. If a callback is
  // set, it is invoked (under the CIT lock) for each evicted entry so
  // the cold tier can persist them before they disappear. Returns the
  // actual number evicted (may be < k if fewer entries exist).
  size_t EvictColdEntries(size_t k);

  // Replace any previous eviction callback.
  void SetEvictionCallback(EvictionCallback cb);

  // Copy all entries for checkpointing (ITEM-19). Produces a stable
  // snapshot under the lock; callers own the returned vector.
  void Snapshot(std::vector<std::pair<UvlFingerprint, CITEntry>>* out) const;

  // Observability. All counters are monotonic; resets are not supported.
  size_t Size() const;
  uint64_t Hits() const { return hits_.load(std::memory_order_relaxed); }
  uint64_t Misses() const { return misses_.load(std::memory_order_relaxed); }
  uint64_t Evictions() const {
    return evictions_.load(std::memory_order_relaxed);
  }

 private:
  uint64_t NextLruSeqLocked();

  mutable std::mutex mu_;
  std::unordered_map<UvlFingerprint, CITEntry, UvlFingerprintHash> map_;
  uint64_t lru_counter_ = 0;  // guarded by mu_
  EvictionCallback eviction_cb_;  // guarded by mu_

  std::atomic<uint64_t> hits_{0};
  std::atomic<uint64_t> misses_{0};
  std::atomic<uint64_t> evictions_{0};
};

}  // namespace ROCKSDB_NAMESPACE
