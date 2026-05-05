//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Cold-tier interface for evicted CIT entries. See plan.md
// AMBIGUITY-008: the paper describes a two-tier CIT but evaluates
// with cold tier disabled. DedupKV ships the cold tier gated behind
// `dedup_cold_tier_enabled` (default false); when disabled, CIT
// eviction drops entries.
//
// This header defines:
//   * ICITColdTier — abstract interface;
//   * InMemoryColdTier — concrete fake for tests / reference impl;
//   * CIT::EvictionCallback adapter that persists evicted entries
//     into a supplied cold tier.
//
// A RocksDB-backed implementation (RocksCFColdTier) will live next to
// this file once DBImpl dedup-context wiring lands in ITEM-12.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "db/dedup/cit.h"
#include "db/dedup/uvl_log_format.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Cold-tier contract. Implementations must be thread-safe — the CIT
// calls Put() under its own mutex (via the eviction callback), and
// Get()/Erase() may be invoked concurrently from Get / compaction
// paths.
class ICITColdTier {
 public:
  virtual ~ICITColdTier() = default;

  // Persist (or overwrite) an entry for `fp`. Called from the CIT's
  // eviction callback so the hot tier can shed entries without losing
  // the refcount.
  virtual Status Put(const UvlFingerprint& fp, const CITEntry& entry) = 0;

  // Look up `fp`. Returns true on hit (fills *out); false on miss.
  // Status errors surface actual backend failures (IO/corruption) —
  // a plain miss returns OK + false.
  virtual Status Get(const UvlFingerprint& fp, CITEntry* out,
                     bool* found) const = 0;

  // Remove `fp` from the cold tier. Idempotent: missing keys are OK.
  virtual Status Erase(const UvlFingerprint& fp) = 0;

  // Observability — number of entries currently persisted. Best-effort
  // for backends where this is not cheap.
  virtual uint64_t ApproximateSize() const = 0;
};

// Reference in-memory cold tier. Thread-safe, bounded only by heap.
// Used by unit tests for the ICITColdTier contract and by any caller
// that wants the cold-tier hook path exercised without a persistent
// backend.
class InMemoryColdTier : public ICITColdTier {
 public:
  InMemoryColdTier() = default;
  InMemoryColdTier(const InMemoryColdTier&) = delete;
  InMemoryColdTier& operator=(const InMemoryColdTier&) = delete;

  Status Put(const UvlFingerprint& fp, const CITEntry& entry) override;
  Status Get(const UvlFingerprint& fp, CITEntry* out,
             bool* found) const override;
  Status Erase(const UvlFingerprint& fp) override;
  uint64_t ApproximateSize() const override;

 private:
  mutable std::mutex mu_;
  std::unordered_map<UvlFingerprint, CITEntry, UvlFingerprintHash> map_;
};

// Build a CIT::EvictionCallback that forwards each evicted entry to
// `cold_tier->Put(...)`. Errors are recorded in the provided
// std::atomic<Status>* (latest error wins) — the CIT's lock is held
// during eviction so we cannot surface errors inline without widening
// the CIT signature, and dropping to an atomic bucket keeps ITEM-04
// untouched.
CIT::EvictionCallback MakeColdTierEvictionCallback(
    ICITColdTier* cold_tier,
    std::function<void(const UvlFingerprint&, const Status&)>
        on_error = nullptr);

}  // namespace ROCKSDB_NAMESPACE
