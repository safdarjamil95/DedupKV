//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-09a: WAL → OfflineWalRecord decoder. Opens a `.log` file via
// log::Reader, iterates every embedded WriteBatch, and emits one
// OfflineWalRecord per (key, value) entry that belongs to the target
// column family. The decoder is deliberately stateless and does NOT
// touch CIT / UVL / VersionSet — that's the worker's job (ITEM-09a)
// or the production sink's job (ITEM-09b).
//
// AMBIGUITY-005: records are returned in WAL append order (head →
// tail). The worker reverses the vector to implement tail→head dedup.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "db/dedup/offline_dedup.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class FileSystem;
class SystemClock;
class Logger;

struct OfflineDedupWalDecodeStats {
  uint64_t wal_records_read = 0;       // log::Reader records
  uint64_t batch_entries_decoded = 0;  // entries across all batches
  uint64_t entries_kept = 0;           // matched cf_id filter
  uint64_t entries_skipped_cf = 0;     // wrong cf_id
  uint64_t entries_unsupported = 0;    // Merge, Put-with-ts, etc.
};

// Opens `wal_path` and produces one OfflineWalRecord per eligible
// entry. `target_cf_id` filters entries by column family. `*records`
// is cleared before population.
Status DecodeWalFileForOfflineDedup(
    FileSystem* fs, SystemClock* clock, const std::string& wal_path,
    uint64_t log_number, uint32_t target_cf_id,
    std::vector<OfflineWalRecord>* records,
    std::shared_ptr<Logger> info_log = nullptr,
    OfflineDedupWalDecodeStats* stats = nullptr);

}  // namespace ROCKSDB_NAMESPACE
