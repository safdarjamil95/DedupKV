//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// DGD — Dynamic Granularity Deduplication.
//
// Per plan.md §4.6 / AMBIGUITY-001, DGD picks a write strategy per
// key-value pair based on value size:
//
//   * size(value) >= chunk_threshold  (large-value branch):
//       fp = SHA1(value)
//       if CIT has fp -> reuse the existing UVL record (dedup hit)
//       else          -> append (fp, key, value, raw) to UVL and
//                        register CIT entry with refcount=1.
//
//   * size(value) <  chunk_threshold  (small-value branch):
//       LZ4-compress the value and write inline as a
//       UvlCompression::kLz4Inline record. No fingerprint, no CIT
//       entry — small values are never shared.
//
// DGD is the inner loop of both the inline flush path (ITEM-14) and
// the offline dedup worker (ITEM-09); each run of those jobs owns a
// DGDEncoder bound to its per-job UvlFileBuilder, but all encoders
// share the DB-wide CIT.

#pragma once

#include <atomic>
#include <cstdint>

#include "db/dedup/uvl_log_format.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CIT;
class Statistics;
class UvlFileBuilder;

// Observability counters for a single DGDEncoder. Safe for concurrent
// increment across multiple DGDEncoder instances that share the same
// DGDStats object (e.g. all flush-side encoders in a DBImpl). When
// wired into ITEM-20 these roll up into RocksDB Statistics tickers.
struct DGDStats {
  std::atomic<uint64_t> dedup_hits{0};
  std::atomic<uint64_t> dedup_misses{0};
  std::atomic<uint64_t> small_value_lz4{0};
  // Bytes appended to UVL but orphaned because a concurrent writer
  // won the CIT race. GC (ITEM-18) will eventually reclaim them.
  std::atomic<uint64_t> orphaned_uvl_bytes{0};
  // Sum of bytes of LZ4-compressed values (the `value` portion, not
  // record overhead). Used for Fig 12's bytes-per-mode split.
  std::atomic<uint64_t> lz4_compressed_bytes{0};
  // Raw (pre-LZ4) bytes — how much the small-value branch saw.
  std::atomic<uint64_t> lz4_input_bytes{0};
};

struct DGDResult {
  uint64_t uvl_file = 0;
  uint64_t offset = 0;
  uint32_t size = 0;
  UvlCompression compression = UvlCompression::kRaw;
  // True if the value was satisfied by an existing CIT entry (no UVL
  // append), OR if we lost a CIT-insert race (our UVL record is
  // orphaned and the winner's coordinates are returned).
  bool was_hit = false;
  // ITEM-18c: SHA1 fingerprint of the value — SHA1(value) for the
  // large-value branch, all zeros for the small-value LZ4 branch.
  // Threaded into the BlobIndex via EncodeUvlBlobIndex so consumers
  // (Get, compaction refcount decrement) can look up CIT directly.
  UvlFingerprint fingerprint{};
};

class DGDEncoder {
 public:
  // `cit`, `uvl_builder`, and `stats` must outlive the encoder. The
  // encoder does not own them. `db_stats` (ITEM-20) is the optional
  // DB-wide Statistics sink — when non-null, DEDUPKV_* tickers and
  // histograms are emitted in addition to the per-CF DGDStats counters.
  DGDEncoder(CIT* cit, UvlFileBuilder* uvl_builder,
             uint32_t chunk_threshold_bytes, DGDStats* stats,
             Statistics* db_stats = nullptr);

  DGDEncoder(const DGDEncoder&) = delete;
  DGDEncoder& operator=(const DGDEncoder&) = delete;

  // Runs the DGD pipeline for one key/value pair. On success, *out
  // identifies the UVL record the SST entry should point at.
  Status Process(const Slice& key, const Slice& value, DGDResult* out);

  uint32_t chunk_threshold_bytes() const { return chunk_threshold_bytes_; }

 private:
  CIT* const cit_;
  UvlFileBuilder* const uvl_builder_;
  const uint32_t chunk_threshold_bytes_;
  DGDStats* const stats_;
  Statistics* const db_stats_;
};

// ITEM-16a: decompress an LZ4-inline UVL value (the small-value DGD
// branch). `max_uncompressed_bytes` bounds the output buffer — callers
// supply a per-record ceiling. Returns NotSupported when LZ4 is not
// compiled in.
Status Lz4DecompressSlice(const Slice& compressed,
                          size_t max_uncompressed_bytes, std::string* output);

}  // namespace ROCKSDB_NAMESPACE
