//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dedup_context.h"

#include <memory>

namespace ROCKSDB_NAMESPACE {

std::shared_ptr<DedupContext> MakeDedupContext(
    const DedupKVOptions& options, uint64_t memtable_capacity_bytes) {
  if (!options.enable) {
    return nullptr;
  }
  auto ctx = std::make_shared<DedupContext>();
  ctx->options_snapshot = options;
  ctx->cit = std::make_shared<CIT>();
  ctx->memory_monitor =
      std::make_shared<DedupMemoryMonitor>(memtable_capacity_bytes);
  ctx->dwq = std::make_shared<DWQ>();
  ctx->dgd_stats = std::make_shared<DGDStats>();
  ctx->uvl_garbage_meter = std::make_shared<UvlGarbageMeter>();
  if (options.cold_tier_enabled) {
    // DEC-005: InMemoryColdTier is the reference impl shipped in
    // ITEM-05. A persistent RocksCFColdTier lands later.
    ctx->cold_tier = std::make_unique<InMemoryColdTier>();
    ctx->cit->SetEvictionCallback(
        MakeColdTierEvictionCallback(ctx->cold_tier.get()));
  }
  return ctx;
}

}  // namespace ROCKSDB_NAMESPACE
