//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-18a: UvlGarbageMeter tracks invalid-byte counts per UVL file.
// Mirrors db/blob/blob_garbage_meter for UVL files.
//
// Invalid bytes accrue when CompactionIterator drops a dedup key and
// its CIT refcount hits 0 — the UVL record it was pointing at is now
// orphaned and contributes to the file's garbage fraction. The
// meter's `InvalidRatio(file_number, total_bytes)` is the input to
// ITEM-18b's rewrite-vs-keep decision.

#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class UvlGarbageMeter {
 public:
  UvlGarbageMeter() = default;
  UvlGarbageMeter(const UvlGarbageMeter&) = delete;
  UvlGarbageMeter& operator=(const UvlGarbageMeter&) = delete;

  // Charge `bytes` of invalid storage to `uvl_file_number`. Safe for
  // concurrent accumulation across compaction threads.
  void Accumulate(uint64_t uvl_file_number, uint64_t bytes);

  // Current invalid-byte total for the file (0 if unknown).
  uint64_t InvalidBytes(uint64_t uvl_file_number) const;

  // Forget a file — called when ITEM-18b's GC rewrites the file and
  // the old number goes away.
  void Forget(uint64_t uvl_file_number);

  // For tests / observability: snapshot the full table.
  std::unordered_map<uint64_t, uint64_t> Snapshot() const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<uint64_t, uint64_t> invalid_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE
