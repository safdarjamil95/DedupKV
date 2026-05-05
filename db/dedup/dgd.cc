//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dgd.h"

#include <cassert>
#include <cstring>
#include <string>

#include "db/dedup/cit.h"
#include "db/dedup/uvl_file_builder.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "util/sha1.h"
#include "util/stop_watch.h"

#ifdef LZ4
#include <lz4.h>
#endif

namespace ROCKSDB_NAMESPACE {

namespace {

// Convert a Sha1Digest to a UvlFingerprint (both are 20-byte arrays).
UvlFingerprint ToUvlFingerprint(const Sha1Digest& d) {
  UvlFingerprint fp{};
  std::memcpy(fp.data(), d.data(), kUvlFingerprintSize);
  return fp;
}

UvlFingerprint SliceSha1(const Slice& s) {
  Sha1 h;
  if (s.size() > 0) {
    h.Update(reinterpret_cast<const uint8_t*>(s.data()), s.size());
  }
  Sha1Digest d;
  h.Final(&d);
  return ToUvlFingerprint(d);
}

Status Lz4CompressSlice(const Slice& input, std::string* output) {
#ifdef LZ4
  if (input.size() > static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) {
    return Status::NotSupported("Input exceeds LZ4_MAX_INPUT_SIZE");
  }
  const int input_len = static_cast<int>(input.size());
  const int bound = LZ4_compressBound(input_len);
  if (bound <= 0) {
    return Status::Corruption("LZ4_compressBound returned non-positive");
  }
  output->resize(static_cast<size_t>(bound));
  const int compressed = LZ4_compress_default(
      input.data(), &(*output)[0], input_len, bound);
  if (compressed <= 0) {
    return Status::Corruption("LZ4_compress_default failed");
  }
  output->resize(static_cast<size_t>(compressed));
  return Status::OK();
#else
  (void)input;
  (void)output;
  return Status::NotSupported("LZ4 support not compiled in");
#endif
}

}  // namespace

Status Lz4DecompressSlice(const Slice& compressed,
                          size_t max_uncompressed_bytes,
                          std::string* output) {
#ifdef LZ4
  if (output == nullptr) {
    return Status::InvalidArgument("Lz4DecompressSlice: null output");
  }
  if (compressed.size() > static_cast<size_t>(LZ4_MAX_INPUT_SIZE)) {
    return Status::NotSupported("Compressed input exceeds LZ4_MAX_INPUT_SIZE");
  }
  if (max_uncompressed_bytes == 0) {
    output->clear();
    return Status::OK();
  }
  output->resize(max_uncompressed_bytes);
  const int n = LZ4_decompress_safe(compressed.data(), &(*output)[0],
                                    static_cast<int>(compressed.size()),
                                    static_cast<int>(max_uncompressed_bytes));
  if (n < 0) {
    return Status::Corruption("LZ4_decompress_safe failed");
  }
  output->resize(static_cast<size_t>(n));
  return Status::OK();
#else
  (void)compressed;
  (void)max_uncompressed_bytes;
  (void)output;
  return Status::NotSupported("LZ4 support not compiled in");
#endif
}

DGDEncoder::DGDEncoder(CIT* cit, UvlFileBuilder* uvl_builder,
                       uint32_t chunk_threshold_bytes, DGDStats* stats,
                       Statistics* db_stats)
    : cit_(cit),
      uvl_builder_(uvl_builder),
      chunk_threshold_bytes_(chunk_threshold_bytes),
      stats_(stats),
      db_stats_(db_stats) {
  assert(cit_ != nullptr);
  assert(uvl_builder_ != nullptr);
  assert(stats_ != nullptr);
}

Status DGDEncoder::Process(const Slice& key, const Slice& value,
                           DGDResult* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("DGDEncoder::Process: null out");
  }

  SystemClock* const clock = SystemClock::Default().get();

  if (value.size() >= chunk_threshold_bytes_) {
    // Large-value branch — content-addressed dedup via CIT.
    UvlFingerprint fp;
    {
      StopWatch sw(clock, db_stats_, DEDUPKV_SHA1_MICROS);
      fp = SliceSha1(value);
    }

    // Fast path: non-atomic lookup. Avoids paying the UVL append cost
    // when we'd just reuse an existing entry.
    CITEntry existing;
    bool found;
    {
      StopWatch sw(clock, db_stats_, DEDUPKV_CIT_LOOKUP_MICROS);
      found = cit_->Lookup(fp, &existing);
    }
    if (found) {
      const uint32_t new_rc = cit_->IncRefcount(fp);
      (void)new_rc;
      stats_->dedup_hits.fetch_add(1, std::memory_order_relaxed);
      RecordTick(db_stats_, DEDUPKV_DUPLICATE_HITS);
      out->uvl_file = existing.uvl_file;
      out->offset = existing.offset;
      out->size = existing.size;
      out->compression = existing.compression;
      out->was_hit = true;
      out->fingerprint = fp;
      return Status::OK();
    }

    // Miss path. Append to UVL, then race to claim the CIT slot.
    uint64_t rec_offset = 0, rec_size = 0;
    {
      StopWatch sw(clock, db_stats_, DEDUPKV_UVL_WRITE_MICROS);
      Status s = uvl_builder_->Add(fp, UvlCompression::kRaw, key, value,
                                   &rec_offset, &rec_size);
      if (!s.ok()) {
        return s;
      }
    }
    RecordTick(db_stats_, DEDUPKV_UVL_BYTES_WRITTEN, rec_size);

    CITEntry candidate;
    candidate.uvl_file = uvl_builder_->file_number();
    candidate.offset = rec_offset;
    candidate.size = static_cast<uint32_t>(rec_size);
    candidate.refcount = 1;
    candidate.compression = UvlCompression::kRaw;

    CITEntry chosen;
    bool existed;
    {
      StopWatch sw(clock, db_stats_, DEDUPKV_CIT_LOOKUP_MICROS);
      existed = cit_->LookupOrInsert(fp, candidate, &chosen);
    }
    if (existed) {
      // Lost the race: keep the winner's coordinates, mark our bytes
      // as orphaned garbage for GC.
      stats_->orphaned_uvl_bytes.fetch_add(rec_size,
                                           std::memory_order_relaxed);
      stats_->dedup_hits.fetch_add(1, std::memory_order_relaxed);
      RecordTick(db_stats_, DEDUPKV_DUPLICATE_HITS);
      RecordTick(db_stats_, DEDUPKV_UVL_ORPHANED_BYTES, rec_size);
      out->uvl_file = chosen.uvl_file;
      out->offset = chosen.offset;
      out->size = chosen.size;
      out->compression = chosen.compression;
      out->was_hit = true;
    } else {
      stats_->dedup_misses.fetch_add(1, std::memory_order_relaxed);
      RecordTick(db_stats_, DEDUPKV_DUPLICATE_MISSES);
      out->uvl_file = candidate.uvl_file;
      out->offset = candidate.offset;
      out->size = candidate.size;
      out->compression = candidate.compression;
      out->was_hit = false;
    }
    out->fingerprint = fp;
    return Status::OK();
  }

  // Small-value branch — LZ4-compress inline, no CIT entry.
  std::string lz4_buf;
  {
    StopWatch sw(clock, db_stats_, DEDUPKV_LZ4_COMPRESS_MICROS);
    Status s = Lz4CompressSlice(value, &lz4_buf);
    if (!s.ok()) {
      return s;
    }
  }
  stats_->lz4_input_bytes.fetch_add(value.size(), std::memory_order_relaxed);
  stats_->lz4_compressed_bytes.fetch_add(lz4_buf.size(),
                                         std::memory_order_relaxed);

  // Fingerprint is unused for small-value records but the codec still
  // writes 20 bytes. Per DEC-007 we use a deterministic "marker" pattern
  // (size-encoded zeros) rather than all-zeros to aid recovery-scan
  // diagnostics — any all-zeros fingerprint at a kLz4Inline record is
  // already a distinguishable case from a large-value kRaw record.
  UvlFingerprint small_fp{};

  uint64_t rec_offset = 0, rec_size = 0;
  {
    StopWatch sw(clock, db_stats_, DEDUPKV_UVL_WRITE_MICROS);
    Status s = uvl_builder_->Add(small_fp, UvlCompression::kLz4Inline, key,
                                 Slice(lz4_buf), &rec_offset, &rec_size);
    if (!s.ok()) {
      return s;
    }
  }
  RecordTick(db_stats_, DEDUPKV_UVL_BYTES_WRITTEN, rec_size);
  stats_->small_value_lz4.fetch_add(1, std::memory_order_relaxed);

  out->uvl_file = uvl_builder_->file_number();
  out->offset = rec_offset;
  out->size = static_cast<uint32_t>(rec_size);
  out->compression = UvlCompression::kLz4Inline;
  out->was_hit = false;
  out->fingerprint = small_fp;  // all zeros — small-branch records are
                                // not CIT-tracked; the embedded fp is
                                // diagnostic only.
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
