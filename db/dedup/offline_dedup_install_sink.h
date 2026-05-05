//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-09b: production OfflineDedupSink that emits an L0 SST and
// installs it via VersionSet::LogAndApply.
//
// Usage: one sink per DWQEntry. `Finish()` sorts the buffered
// emissions by user-key, constructs a TableBuilder, writes a
// kTypeBlobIndex (kDedupKVUvl subtype) entry per Put and a tombstone
// per Delete, closes the SST, builds a VersionEdit with AddFile +
// AddUvlFile, then calls LogAndApply under the DB mutex. On failure
// the partial SST is unlinked; the UVL file's fate is owned by the
// worker (it Finishes the UVL before calling sink->Finish()).

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "db/dedup/offline_dedup.h"
#include "db/dbformat.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;
class ColumnFamilyData;
class Logger;

class OfflineDedupInstallSink : public OfflineDedupSink {
 public:
  OfflineDedupInstallSink(DBImpl* db, ColumnFamilyData* cfd,
                          uint64_t uvl_file_number,
                          std::shared_ptr<Logger> info_log);
  ~OfflineDedupInstallSink() override;

  OfflineDedupInstallSink(const OfflineDedupInstallSink&) = delete;
  OfflineDedupInstallSink& operator=(const OfflineDedupInstallSink&) = delete;

  Status EmitValue(const Slice& key, SequenceNumber seq,
                   const DGDResult& result) override;
  Status EmitDelete(const Slice& key, SequenceNumber seq,
                    ValueType type) override;
  Status Finish() override;

  // Populated by the worker after it finalises the UVL — recorded in
  // the VersionEdit so reopen can account the UVL.
  void SetUvlFileStats(uint64_t total_records, uint64_t total_bytes) {
    uvl_total_records_ = total_records;
    uvl_total_bytes_ = total_bytes;
  }

 private:
  struct PendingEntry {
    std::string user_key;
    SequenceNumber seq = 0;
    ValueType type = kTypeValue;
    std::string value;  // BlobIndex bytes for Puts; empty for Deletes
  };

  Status BuildAndInstall();

  DBImpl* const db_;
  ColumnFamilyData* const cfd_;
  const uint64_t uvl_file_number_;
  std::shared_ptr<Logger> info_log_;

  std::vector<PendingEntry> pending_;
  uint64_t uvl_total_records_ = 0;
  uint64_t uvl_total_bytes_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
