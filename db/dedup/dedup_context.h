//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// DedupContext — per-CF bag of long-lived DedupKV state held by
// DBImpl. Constructed at `DB::Open` when a CF has
// `dedupkv.enable=true`; destroyed at DB close. Any subsystem that
// needs dedup state (FlushJob, CompactionIterator, GetImpl, offline
// worker) looks up this struct via
// `DBImpl::GetDedupContext(cf_id)`.
//
// This header is intentionally free of DBImpl dependencies — only the
// foundational Phase I/II data structures. DBImpl is the owner; it
// does NOT need to know the internals of these pieces.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "db/dedup/cit.h"
#include "db/dedup/cit_cold_tier.h"
#include "db/dedup/dedup_work_queue.h"
#include "db/dedup/dgd.h"
#include "db/dedup/memory_monitor.h"
#include "db/dedup/offline_dedup_worker.h"
#include "db/dedup/uvl_garbage_meter.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Statistics;

struct DedupContext {
  // All members reachable from any thread once Init() has returned.
  // Individual members have their own synchronisation.

  // Content-addressed dedup table.
  std::shared_ptr<CIT> cit;

  // Per-CF memtable-byte monitor feeding the elastic controller.
  std::shared_ptr<DedupMemoryMonitor> memory_monitor;

  // Offline dedup work queue. Producer = FlushJob (ITEM-15), consumer
  // = OfflineDedupWorker (Phase III/IV per DEC-009).
  std::shared_ptr<DWQ> dwq;

  // Optional persistent cold tier. Null when
  // `cfo.dedupkv.cold_tier_enabled == false`.
  std::unique_ptr<ICITColdTier> cold_tier;

  // Statistics sink shared by all DGD encoders bound to this CF.
  std::shared_ptr<DGDStats> dgd_stats;

  // ITEM-20: DB-wide Statistics (from ImmutableDBOptions::statistics).
  // Borrowed; lifetime managed by DBImpl. nullptr when the user did not
  // configure statistics. DGD encoders, the offline worker, and the GC
  // rewriter forward DEDUPKV_* tickers/histograms here.
  Statistics* db_statistics = nullptr;

  // ITEM-18a: per-UVL-file invalid-byte accumulator. Fed by
  // CompactionIterator's refcount-decrement hook whenever a dedup
  // entry's refcount reaches zero.
  std::shared_ptr<UvlGarbageMeter> uvl_garbage_meter;

  // File-number generator for fresh UVL files. Incremented from the
  // same pool as SST file numbers (ITEM-14 wiring).
  std::atomic<uint64_t> next_uvl_file_number{1};

  // ITEM-11 config snapshot. Immutable for the DB's lifetime; mutable
  // sub-fields also live on the CF's MutableCFOptions, so the per-
  // flush view should read from MutableCFOptions rather than from
  // this member when up-to-date tunables matter.
  DedupKVOptions options_snapshot;

  // ITEM-09c: background worker that drains DWQ entries into L0 SSTs.
  // Spawned by DB::Open and joined by ~DBImpl (via Stop() + dtor).
  std::unique_ptr<OfflineDedupWorker> offline_worker;

  DedupContext() = default;
  DedupContext(const DedupContext&) = delete;
  DedupContext& operator=(const DedupContext&) = delete;

  // ITEM-18f: per-UVL total-byte registry. Populated by every UVL
  // creation site (inline flush, offline install sink, GC rewriter)
  // after LogAndApply commits. Consulted by `MaybeScheduleUvlGc` to
  // compute `invalid_bytes / total_bytes` against `uvl_gc_threshold`.
  // `ForgetUvlFile` removes the entry when the old file is reclaimed.
  void RegisterUvlFile(uint64_t uvl_file_number, uint64_t total_bytes) {
    std::lock_guard<std::mutex> lk(uvl_sizes_mu_);
    uvl_total_bytes_[uvl_file_number] = total_bytes;
  }
  void ForgetUvlFile(uint64_t uvl_file_number) {
    std::lock_guard<std::mutex> lk(uvl_sizes_mu_);
    uvl_total_bytes_.erase(uvl_file_number);
  }
  std::unordered_map<uint64_t, uint64_t> SnapshotUvlFileSizes() const {
    std::lock_guard<std::mutex> lk(uvl_sizes_mu_);
    return uvl_total_bytes_;
  }

 private:
  mutable std::mutex uvl_sizes_mu_;
  std::unordered_map<uint64_t, uint64_t> uvl_total_bytes_;
};

// Construct a DedupContext from the CF's options. Returns nullptr when
// dedup is disabled. Cold tier is instantiated as InMemoryColdTier
// (reference impl) when enabled — ITEM-12 ships this stub; the
// RocksCFColdTier backend arrives later per DEC-005.
std::shared_ptr<DedupContext> MakeDedupContext(
    const DedupKVOptions& options, uint64_t memtable_capacity_bytes);

}  // namespace ROCKSDB_NAMESPACE
