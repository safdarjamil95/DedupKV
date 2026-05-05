//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/offline_dedup.h"

#include <unordered_set>

namespace ROCKSDB_NAMESPACE {

Status OfflineDedupDrain(const std::vector<OfflineWalRecord>& records,
                         DGDEncoder* encoder, OfflineDedupSink* sink,
                         OfflineDedupStats* stats) {
  if (encoder == nullptr || sink == nullptr) {
    return Status::InvalidArgument("OfflineDedupDrain: null encoder/sink");
  }

  // KeyArray — "seen already emitted once at its tail-most version"
  // (plan §4.4). std::unordered_set<std::string> per AMBIGUITY-005.
  std::unordered_set<std::string> seen;
  seen.reserve(records.size());

  // Iterate tail → head so the first encounter of each key is the
  // newest version and therefore the one to keep.
  for (auto it = records.rbegin(); it != records.rend(); ++it) {
    const OfflineWalRecord& rec = *it;
    if (stats != nullptr) stats->records_seen += 1;

    // If we've already emitted this key, this is an older version — skip.
    if (seen.find(rec.key) != seen.end()) {
      if (stats != nullptr) stats->duplicate_keys_dropped += 1;
      continue;
    }
    // Mark key as emitted BEFORE dispatching — on sink failure we
    // still don't want to re-emit the older version.
    seen.insert(rec.key);

    switch (rec.type) {
      case kTypeValue: {
        DGDResult r;
        Status s = encoder->Process(Slice(rec.key), Slice(rec.value), &r);
        if (!s.ok()) return s;
        s = sink->EmitValue(Slice(rec.key), rec.seq, r);
        if (!s.ok()) return s;
        if (stats != nullptr) stats->values_emitted += 1;
        break;
      }
      case kTypeDeletion:
      case kTypeSingleDeletion: {
        Status s = sink->EmitDelete(Slice(rec.key), rec.seq, rec.type);
        if (!s.ok()) return s;
        if (stats != nullptr) stats->deletes_emitted += 1;
        break;
      }
      default:
        // Merge (GAP-016 / AMBIGUITY-007 disallows it), range
        // deletions, column-family-specific types, etc. — skip for
        // now. The counter lets tests / observability catch
        // unexpected types.
        if (stats != nullptr) stats->unsupported_types_skipped += 1;
        break;
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
