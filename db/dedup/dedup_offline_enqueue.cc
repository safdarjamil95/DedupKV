//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dedup_offline_enqueue.h"

#include <utility>

#include "rocksdb/table.h"
#include "table/block_based/filter_policy_internal.h"

namespace ROCKSDB_NAMESPACE {

OfflineDedupFilterBuilder::OfflineDedupFilterBuilder(double bits_per_key)
    : policy_(NewBloomFilterPolicy(bits_per_key)) {
  BlockBasedTableOptions tbl_opts;
  FilterBuildingContext fbc(tbl_opts);
  builder_.reset(policy_->GetBuilderWithContext(fbc));
}

OfflineDedupFilterBuilder::~OfflineDedupFilterBuilder() = default;

void OfflineDedupFilterBuilder::AddKey(const Slice& user_key) {
  if (finished_ || builder_ == nullptr) {
    return;
  }
  builder_->AddKey(user_key);
  ++num_keys_;
}

Status OfflineDedupFilterBuilder::Finish(OfflineDedupFilter* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("OfflineDedupFilterBuilder::Finish: null out");
  }
  if (finished_) {
    return Status::InvalidArgument(
        "OfflineDedupFilterBuilder::Finish called twice");
  }
  if (num_keys_ == 0) {
    return Status::Incomplete("no keys added to offline dedup filter");
  }
  finished_ = true;

  std::unique_ptr<const char[]> buf;
  Status finish_status = Status::OK();
  Slice encoded = builder_->Finish(&buf, &finish_status);
  if (!finish_status.ok()) {
    return finish_status;
  }

  // `encoded` aliases `buf`. Copy the bytes into a persistent std::string
  // owned by OfflineDedupFilter so the returned reader's internal Slice
  // stays valid for the DWQEntry's lifetime.
  out->backing.assign(encoded.data(), encoded.size());
  out->num_keys = num_keys_;
  out->reader.reset(policy_->GetFilterBitsReader(Slice(out->backing)));
  if (out->reader == nullptr) {
    return Status::Corruption(
        "OfflineDedupFilterBuilder: empty reader after Finish");
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
