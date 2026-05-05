//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Helpers for the ITEM-15 elastic controller: building the Bloom filter
// that goes into a DWQEntry when FlushJob takes the offline branch.
//
// Factored out so the filter-building contract is unit-testable in
// isolation (unlike the FlushJob wiring, which requires a live DBImpl).
// Uses RocksDB's default Bloom filter policy at the same bits/key the
// paper's evaluation uses (§5.1, see AMBIGUITY-006 + GAP-006).

#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "rocksdb/filter_policy.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/filter_policy_internal.h"

namespace ROCKSDB_NAMESPACE {

// Result of OfflineDedupFilterBuilder::Finish: the encoded filter bytes
// and a reader whose internal Slice aliases those bytes. Callers must
// keep `backing` and `reader` together — moving one without the other
// leaves the reader pointing at invalid memory.
struct OfflineDedupFilter {
  std::string backing;
  std::unique_ptr<FilterBitsReader> reader;
  size_t num_keys = 0;
};

// Collects user keys via AddKey and materialises a Bloom-filter-backed
// FilterBitsReader in Finish. Thin wrapper around
// `NewBloomFilterPolicy(bits_per_key)` → `GetBuilderWithContext`; the
// factored layer exists so tests can drive the same code path the
// FlushJob offline branch exercises.
class OfflineDedupFilterBuilder {
 public:
  // `bits_per_key` follows RocksDB's BloomFilterPolicy default (10.0).
  // The paper's §5.1 evaluation uses the SST-level default, matched
  // here for parity.
  explicit OfflineDedupFilterBuilder(double bits_per_key = 10.0);
  ~OfflineDedupFilterBuilder();

  OfflineDedupFilterBuilder(const OfflineDedupFilterBuilder&) = delete;
  OfflineDedupFilterBuilder& operator=(const OfflineDedupFilterBuilder&) =
      delete;

  // Feed one user-key into the filter. Duplicate keys are harmless —
  // the underlying builder de-duplicates successive identical hashes.
  void AddKey(const Slice& user_key);

  size_t num_keys_added() const { return num_keys_; }

  // Seal the filter and transfer ownership into *out. Returns
  // `Status::Incomplete` if no keys were added (an empty offline-dedup
  // filter is indistinguishable from "no entry at all" to the DWQ Get
  // path, so the caller should skip the push).
  Status Finish(OfflineDedupFilter* out);

 private:
  std::shared_ptr<const FilterPolicy> policy_;
  std::unique_ptr<FilterBitsBuilder> builder_;
  size_t num_keys_ = 0;
  bool finished_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
