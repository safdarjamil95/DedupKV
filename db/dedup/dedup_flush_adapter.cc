//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dedup_flush_adapter.h"

#include <utility>

#include "db/blob/blob_index.h"
#include "db/dedup/cit.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/statistics.h"

namespace ROCKSDB_NAMESPACE {

namespace {

CompressionType ToBlobCompression(UvlCompression c) {
  switch (c) {
    case UvlCompression::kRaw:
      return kNoCompression;
    case UvlCompression::kLz4Inline:
      return kLZ4Compression;
  }
  return kNoCompression;
}

}  // namespace

DedupFlushAdapter::DedupFlushAdapter(
    CIT* cit, std::unique_ptr<UvlFileBuilder> uvl_builder,
    uint32_t chunk_threshold_bytes, DGDStats* stats, Statistics* db_stats)
    : uvl_builder_(std::move(uvl_builder)),
      encoder_(cit, uvl_builder_.get(), chunk_threshold_bytes, stats,
               db_stats),
      db_stats_(db_stats) {}

DedupFlushAdapter::~DedupFlushAdapter() = default;

Status DedupFlushAdapter::Add(const Slice& key, const Slice& value,
                              std::string* out_blob_index) {
  if (out_blob_index == nullptr) {
    return Status::InvalidArgument("DedupFlushAdapter::Add: null out");
  }
  DGDResult r;
  Status s = encoder_.Process(key, value, &r);
  if (!s.ok()) {
    return s;
  }
  EncodeUvlBlobIndex(r, out_blob_index);
  RecordTick(db_stats_, DEDUPKV_INLINE_OPS);
  return Status::OK();
}

Status DedupFlushAdapter::Finish(bool sync) {
  return uvl_builder_->Finish(sync);
}

void DedupFlushAdapter::Abandon() { uvl_builder_->Abandon(); }

uint64_t DedupFlushAdapter::uvl_file_number() const {
  return uvl_builder_->file_number();
}
uint64_t DedupFlushAdapter::uvl_record_count() const {
  return uvl_builder_->record_count();
}
uint64_t DedupFlushAdapter::uvl_total_bytes() const {
  return uvl_builder_->total_bytes();
}

void EncodeUvlBlobIndex(const DGDResult& result, std::string* out) {
  // ITEM-18c: emit V2 (fingerprint-embedded) BlobIndex. Every call site
  // (inline flush adapter + offline install sink) threads the fp
  // through DGDResult.fingerprint; the small-value branch propagates
  // an all-zero fp, which is correct for a record that's not
  // CIT-tracked.
  BlobIndex::EncodeDedupKVUvlV2(out, result.fingerprint, result.uvl_file,
                                result.offset, result.size,
                                ToBlobCompression(result.compression));
}

}  // namespace ROCKSDB_NAMESPACE
