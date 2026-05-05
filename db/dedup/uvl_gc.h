//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-18b: UvlGcRewriter — rewrites one UVL file whose invalid-byte
// ratio has exceeded the GC threshold. Live records (CIT refcount > 0)
// are copied to a fresh UVL file; the CIT entries for those
// fingerprints are retargeted atomically to the new location. Records
// with refcount 0 are dropped.
//
// Scope of ITEM-18b (see DEC-021):
//   * Rewriter is IO-only; the caller performs the MANIFEST commit
//     (VersionSet::LogAndApply) with the produced VersionEdit.
//   * The old UVL file is NOT deleted by this pass. Deleting it
//     requires either rewriting the SST kDedupKVUvl BlobIndex entries
//     (which still name `old_file_number`) or routing Version::GetBlob
//     through CIT. Either is a larger change than 18b's budget;
//     reclamation lands in a follow-up item.
//
// Concurrency: the caller holds per-CF guarantees (no concurrent GC on
// the same file — DBImpl::TriggerUvlGcForTest enforces via a set of
// in-progress file numbers). CIT operations individually lock, so
// racing compaction DecRefcount calls during the rewrite window are
// safe: the rewriter's RetargetLocation is conditional on the CIT
// entry still naming the old file+offset, and a racing drop that takes
// refcount to zero simply means we wrote a now-orphaned record to the
// new file — next GC pass picks it up.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CIT;
class ColumnFamilyData;
class Logger;
class SystemClock;
class UvlGarbageMeter;
class VersionEdit;
class VersionSet;

struct UvlGcRewriteResult {
  // File number of the freshly-written UVL, or 0 when no records were
  // copied (in which case the VersionEdit records only the garbage tag
  // and no new UvlFileAddition).
  uint64_t new_uvl_file_number = 0;
  // Records copied (subset of old UVL's records that had CIT
  // refcount > 0 at rewrite time).
  uint64_t live_records_copied = 0;
  // Bytes written to the new UVL (sum of encoded record sizes + UVL
  // header).
  uint64_t live_bytes_copied = 0;
  // ITEM-18e: true if any record in the old UVL was LZ4-inline
  // (DGD small-value branch). Small-branch records have no CIT
  // entry and their SST BlobIndex entries fall back to the
  // `{file, offset}` coordinate on Get — so the old file MUST stay
  // on disk until those SSTs are compacted away. When this flag is
  // false, the caller can safely reclaim the old UVL.
  bool old_file_had_small_branch = false;
};

class UvlGcRewriter {
 public:
  // Reads `old_uvl_file_number` from disk, filters by CIT refcount,
  // writes surviving records to a new UVL, retargets CIT entries, and
  // populates `edit` with:
  //   * AddUvlFile(new_file, ...) when any records were copied
  //   * AddUvlFileGarbage(old_file, new_file, ...) always
  // Does NOT call LogAndApply — the caller owns MANIFEST write ordering
  // because it must be under the DB mutex and alongside any obsolete-
  // file bookkeeping the caller wants to bundle.
  //
  // On success, UvlGarbageMeter::Forget(old_uvl_file_number) has been
  // invoked — new accounting against the old number starts fresh
  // (should be zero immediately after GC).
  static Status Run(ColumnFamilyData* cfd, VersionSet* versions,
                    SystemClock* clock, CIT* cit,
                    UvlGarbageMeter* garbage_meter,
                    uint64_t old_uvl_file_number, VersionEdit* edit,
                    UvlGcRewriteResult* result);
};

// ITEM-19: reconstruct the in-memory CIT + UVL total-bytes registry
// from the UVL files that survive on disk. Intended to be called at
// DB::Open after VersionSet::Recover has settled. Iterates every
// `NNNNNN.uvl` in `cf_paths[0]`, decodes each record, and for each
// non-zero fingerprint inserts a CITEntry at `{file, offset, size,
// compression, refcount=1}`. Small-branch (fp == all-zero marker)
// records are skipped — they were never CIT-tracked.
//
// The refcount=1 default is a placeholder; proper refcount
// reconstruction requires sweeping SST BlobIndex entries (deferred;
// see plan.md DEC-026). For the paper's workload the placeholder is
// sufficient because refcount is only consumed by compaction drops
// (ITEM-17) to decide "hit zero → garbage", and the worst case of an
// under-count triggers auto-GC slightly earlier than strictly needed.
class CitRebuild {
 public:
  // `cfd` gives us `ioptions().cf_paths` + `ioptions().fs`. `cit` and
  // `total_bytes_registry_cb` are populated in-place. On any error
  // reading a single UVL file, that file is logged-and-skipped (best
  // effort — correctness is "CIT covers files we could read").
  static Status Run(ColumnFamilyData* cfd, SystemClock* clock, CIT* cit,
                    std::function<void(uint64_t file_number,
                                       uint64_t total_bytes)>
                        total_bytes_registry_cb,
                    Logger* info_log);
};

}  // namespace ROCKSDB_NAMESPACE
