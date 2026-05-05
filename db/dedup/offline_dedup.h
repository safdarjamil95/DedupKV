//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Offline dedup — core algorithm.
//
// Consumes the (decoded) records of a single WAL file, walks them in
// tail→head order, skips duplicate keys via a KeyArray tracker, and
// dispatches each unique key through DGD to produce UVL records plus
// SST entries. The SST-building and VersionSet-installing side of
// ITEM-09 is deferred to Phase III/IV wiring (DEC-009 in plan.md) —
// this file owns only the pure algorithm, which is the reviewable
// correctness surface. The eventual `OfflineDedupWorker` thread will
// call `OfflineDedupDrain()` once per DWQ entry.

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "db/dedup/dgd.h"
#include "db/dbformat.h"  // ValueType, SequenceNumber
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// One decoded WAL record. Populated by whichever layer opens the WAL
// (log::Reader in production, test harness here).
struct OfflineWalRecord {
  SequenceNumber seq = 0;
  ValueType type = kTypeValue;
  std::string key;
  std::string value;  // empty for deletion variants
};

// Abstract receiver for per-key output. ITEM-14's caller will
// implement this to feed a TableBuilder; the test harness checks that
// the right keys / sequence numbers / DGD results are emitted.
class OfflineDedupSink {
 public:
  virtual ~OfflineDedupSink() = default;

  // Emit a Put whose value is already represented in UVL.
  virtual Status EmitValue(const Slice& key, SequenceNumber seq,
                           const DGDResult& result) = 0;

  // Emit a deletion (kTypeDeletion / kTypeSingleDeletion / range
  // tombstone). DGD is not involved.
  virtual Status EmitDelete(const Slice& key, SequenceNumber seq,
                            ValueType type) = 0;

  // ITEM-09a: called after the last Emit* for this WAL. Implementations
  // use this to close a TableBuilder, install a VersionEdit, or (in
  // tests) mark the capture complete. Default: no-op.
  virtual Status Finish() { return Status::OK(); }
};

struct OfflineDedupStats {
  uint64_t records_seen = 0;          // total WAL records processed
  uint64_t duplicate_keys_dropped = 0;  // older versions of same key
  uint64_t values_emitted = 0;
  uint64_t deletes_emitted = 0;
  uint64_t unsupported_types_skipped = 0;  // merge, etc.
};

// Runs the tail→head dedup-and-sink pipeline for one WAL's records.
//
//   `records` : decoded WAL records in WAL append order (head → tail).
//   `encoder` : DGD encoder bound to this WAL's UvlFileBuilder + CIT.
//   `sink`    : receiver for emitted (key, seq, result) tuples.
//   `stats`   : optional counters (nullptr skips accounting).
//
// Tail→head iteration ensures the newest version of each key wins;
// older versions are dropped via the internal KeyArray tracker. See
// plan.md AMBIGUITY-005 for why in-memory reverse iteration is chosen
// over streaming reverse I/O.
Status OfflineDedupDrain(const std::vector<OfflineWalRecord>& records,
                         DGDEncoder* encoder, OfflineDedupSink* sink,
                         OfflineDedupStats* stats = nullptr);

}  // namespace ROCKSDB_NAMESPACE
