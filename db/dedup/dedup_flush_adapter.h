//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// DedupFlushAdapter — ties together DGD (ITEM-07), UvlFileBuilder
// (ITEM-02), and the kDedupKVUvl BlobIndex subtype (ITEM-10) into the
// single per-KV `Add(key, value, &blob_index)` API that FlushJob
// (ITEM-14b) will plug into BuildTable.
//
// One instance per inline FLUSH (per AMBIGUITY-002 — single-threaded
// per FLUSH thread, but the underlying CIT is shared across flushes
// and serialises internally). Owns its UvlFileBuilder; the CIT and
// stats sink are borrowed from the DBImpl-owned DedupContext.
//
// Risk-bound scoping (DEC-010): this header delivers the adapter and
// its end-to-end test. The actual FlushJob/BuildTable wiring + the
// VersionEdit::added_uvl_files MANIFEST extension lands in a
// follow-up (ITEM-14b) so the on-disk-format change can be reviewed
// in isolation.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/dedup/dgd.h"
#include "db/dedup/uvl_file_builder.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CIT;
class Statistics;

class DedupFlushAdapter {
 public:
  // Takes ownership of `uvl_builder` (already Open()-ed by the caller
  // so the file is on-disk and reachable). `cit` and `stats` are
  // borrowed and must outlive the adapter. `db_stats` (ITEM-20) is the
  // optional DB-wide Statistics sink; when non-null, DEDUPKV_* tickers
  // and histograms are emitted alongside the per-CF DGDStats counters.
  DedupFlushAdapter(CIT* cit, std::unique_ptr<UvlFileBuilder> uvl_builder,
                    uint32_t chunk_threshold_bytes, DGDStats* stats,
                    Statistics* db_stats = nullptr);

  DedupFlushAdapter(const DedupFlushAdapter&) = delete;
  DedupFlushAdapter& operator=(const DedupFlushAdapter&) = delete;

  ~DedupFlushAdapter();

  // Run one KV through the dedup pipeline. On success, *out_blob_index
  // contains a kDedupKVUvl-typed BlobIndex (encode/decode contract
  // owned by ITEM-10) that the SST builder should emit alongside `key`
  // with ValueType::kTypeBlobIndex.
  //
  // The DGD branch picked (large vs. small value) and any CIT race
  // outcome are reflected in the encoded BlobIndex's compression
  // byte and offset fields.
  Status Add(const Slice& key, const Slice& value,
             std::string* out_blob_index);

  // Flush+sync the UVL file. Must be called once after the last Add()
  // for this flush. Caller is responsible for installing the UVL into
  // VersionEdit (ITEM-14b).
  Status Finish(bool sync = true);

  // Drop the UVL file without sync — for error recovery on a failed
  // flush. Caller deletes the partial file.
  void Abandon();

  uint64_t uvl_file_number() const;
  uint64_t uvl_record_count() const;
  uint64_t uvl_total_bytes() const;

 private:
  std::unique_ptr<UvlFileBuilder> uvl_builder_;
  DGDEncoder encoder_;
  Statistics* const db_stats_;
};

// Encode a DGDResult as a kDedupKVUvl BlobIndex. Helper extracted so
// callers that already drive DGDEncoder directly (e.g. the offline
// worker, ITEM-09's eventual Phase III/IV wiring) can use the same
// encoding without going through DedupFlushAdapter.
//
// The CompressionType field of the resulting BlobIndex maps from the
// per-record UvlCompression byte: kRaw → kNoCompression, kLz4Inline →
// kLZ4Compression. The Get path (ITEM-16) reverses this mapping.
void EncodeUvlBlobIndex(const DGDResult& result, std::string* out);

}  // namespace ROCKSDB_NAMESPACE
