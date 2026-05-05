//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <array>
#include <cstring>
#include <sstream>
#include <string>

#include "db/dedup/uvl_log_format.h"
#include "rocksdb/compression_type.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// BlobIndex is a pointer to the blob and metadata of the blob. The index is
// stored in base DB as ValueType::kTypeBlobIndex.
// There are four types of blob index:
//
//    kInlinedTTL:
//      +------+------------+---------------+
//      | type | expiration | value         |
//      +------+------------+---------------+
//      | char | varint64   | variable size |
//      +------+------------+---------------+
//
//    kBlob:
//      +------+-------------+----------+----------+-------------+
//      | type | file number | offset   | size     | compression |
//      +------+-------------+----------+----------+-------------+
//      | char | varint64    | varint64 | varint64 | char        |
//      +------+-------------+----------+----------+-------------+
//
//    kBlobTTL:
//      +------+------------+-------------+----------+----------+-------------+
//      | type | expiration | file number | offset   | size     | compression |
//      +------+------------+-------------+----------+----------+-------------+
//      | char | varint64   | varint64    | varint64 | varint64 | char        |
//      +------+------------+-------------+----------+----------+-------------+
//
//    kDedupKVUvl (ITEM-10): byte-identical layout to kBlob; the distinct type
//    byte tells the Get path (ITEM-16) to dispatch to UvlFileReader instead
//    of BlobFileReader. No TTL variant — DedupKV does not use expirations.
//
//    kDedupKVUvlV2 (ITEM-18c): same as kDedupKVUvl plus a 20-byte SHA1
//    fingerprint immediately after the type byte. Embedding the fp lets
//    Get and compaction-refcount-decrement look up CIT by fp without
//    opening the UVL file — prerequisite for the old-UVL reclamation
//    scheduled in ITEM-18d/e.
//
//      +------+----+-------------+----------+----------+-------------+
//      | type | fp | file number | offset   | size     | compression |
//      +------+----+-------------+----------+----------+-------------+
//      | char | 20B| varint64    | varint64 | varint64 | char        |
//      +------+----+-------------+----------+----------+-------------+
//
// There isn't a kInlined (without TTL) type since we can store it as a plain
// value (i.e. ValueType::kTypeValue).
class BlobIndex {
 public:
  enum class Type : unsigned char {
    kInlinedTTL = 0,
    kBlob = 1,
    kBlobTTL = 2,
    kDedupKVUvl = 3,
    kDedupKVUvlV2 = 4,
    kUnknown = 5,
  };

  BlobIndex() : type_(Type::kUnknown) {}

  BlobIndex(const BlobIndex&) = default;
  BlobIndex& operator=(const BlobIndex&) = default;

  bool IsInlined() const { return type_ == Type::kInlinedTTL; }

  bool HasTTL() const {
    return type_ == Type::kInlinedTTL || type_ == Type::kBlobTTL;
  }

  // True for kBlob / kBlobTTL — the BlobDB native blob file. False
  // for kInlinedTTL and kDedupKVUvl.
  bool IsBlob() const {
    return type_ == Type::kBlob || type_ == Type::kBlobTTL;
  }

  // True if this index points into a DedupKV UVL file (ITEM-10, 18c).
  // Matches both the V1 (fingerprint-less) and V2 (fingerprint-embedded)
  // tags. Callers that need the fp must check `HasFingerprint()`.
  bool IsDedupKVUvl() const {
    return type_ == Type::kDedupKVUvl || type_ == Type::kDedupKVUvlV2;
  }

  // True if this index carries an embedded 20-byte fingerprint (i.e. V2).
  // V1 entries (written pre-ITEM-18c) return false and `fingerprint()`
  // returns all zeros.
  bool HasFingerprint() const { return type_ == Type::kDedupKVUvlV2; }

  // Returns the 20-byte SHA1 fingerprint embedded in a V2 BlobIndex.
  // For V1 and non-dedupkv types, returns an all-zero fingerprint.
  const UvlFingerprint& fingerprint() const { return fingerprint_; }

  uint64_t expiration() const {
    assert(HasTTL());
    return expiration_;
  }

  const Slice& value() const {
    assert(IsInlined());
    return value_;
  }

  uint64_t file_number() const {
    assert(!IsInlined());
    return file_number_;
  }

  uint64_t offset() const {
    assert(!IsInlined());
    return offset_;
  }

  uint64_t size() const {
    assert(!IsInlined());
    return size_;
  }

  CompressionType compression() const {
    assert(!IsInlined());
    return compression_;
  }

  Status DecodeFrom(Slice slice) {
    const char* kErrorMessage = "Error while decoding blob index";
    assert(slice.size() > 0);
    type_ = static_cast<Type>(*slice.data());
    if (type_ >= Type::kUnknown) {
      return Status::Corruption(kErrorMessage,
                                "Unknown blob index type: " +
                                    std::to_string(static_cast<char>(type_)));
    }
    slice = Slice(slice.data() + 1, slice.size() - 1);
    if (HasTTL()) {
      if (!GetVarint64(&slice, &expiration_)) {
        return Status::Corruption(kErrorMessage, "Corrupted expiration");
      }
    }
    // ITEM-18c: V2 dedupkv records carry a 20-byte fingerprint before
    // the file number. V1 records do not.
    if (type_ == Type::kDedupKVUvlV2) {
      if (slice.size() < kUvlFingerprintSize) {
        return Status::Corruption(kErrorMessage, "Truncated V2 fingerprint");
      }
      std::memcpy(fingerprint_.data(), slice.data(), kUvlFingerprintSize);
      slice = Slice(slice.data() + kUvlFingerprintSize,
                    slice.size() - kUvlFingerprintSize);
    } else {
      fingerprint_.fill(0);
    }
    if (IsInlined()) {
      value_ = slice;
    } else {
      if (GetVarint64(&slice, &file_number_) && GetVarint64(&slice, &offset_) &&
          GetVarint64(&slice, &size_) && slice.size() == 1) {
        compression_ = static_cast<CompressionType>(*slice.data());
      } else {
        return Status::Corruption(kErrorMessage, "Corrupted blob offset");
      }
    }
    return Status::OK();
  }

  std::string DebugString(bool output_hex) const {
    std::ostringstream oss;

    if (IsInlined()) {
      oss << "[inlined blob] value:" << value_.ToString(output_hex);
    } else {
      const char* tag;
      if (type_ == Type::kDedupKVUvlV2) {
        tag = "[uvl ref v2]";
      } else if (type_ == Type::kDedupKVUvl) {
        tag = "[uvl ref]";
      } else {
        tag = "[blob ref]";
      }
      oss << tag << " file:" << file_number_ << " offset:" << offset_
          << " size:" << size_
          << " compression: " << CompressionTypeToString(compression_);
    }

    if (HasTTL()) {
      oss << " exp:" << expiration_;
    }

    return oss.str();
  }

  // Encode this blob index into dst based on its type.
  void EncodeTo(std::string* dst) const {
    if (IsInlined()) {
      EncodeInlinedTTL(dst, expiration_, value_);
    } else if (HasTTL()) {
      EncodeBlobTTL(dst, expiration_, file_number_, offset_, size_,
                    compression_);
    } else if (type_ == Type::kDedupKVUvlV2) {
      EncodeDedupKVUvlV2(dst, fingerprint_, file_number_, offset_, size_,
                         compression_);
    } else if (type_ == Type::kDedupKVUvl) {
      EncodeDedupKVUvl(dst, file_number_, offset_, size_, compression_);
    } else {
      EncodeBlob(dst, file_number_, offset_, size_, compression_);
    }
  }

  static void EncodeInlinedTTL(std::string* dst, uint64_t expiration,
                               const Slice& value) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(1 + kMaxVarint64Length + value.size());
    dst->push_back(static_cast<char>(Type::kInlinedTTL));
    PutVarint64(dst, expiration);
    dst->append(value.data(), value.size());
  }

  static void EncodeBlob(std::string* dst, uint64_t file_number,
                         uint64_t offset, uint64_t size,
                         CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 3 + 2);
    dst->push_back(static_cast<char>(Type::kBlob));
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

  // DedupKV UVL pointer — identical byte layout to kBlob, just a
  // different type tag. The `compression` field here encodes the
  // per-record UvlCompression byte: kNoCompression ↔ UvlCompression::kRaw,
  // kLZ4Compression ↔ UvlCompression::kLz4Inline.
  static void EncodeDedupKVUvl(std::string* dst, uint64_t file_number,
                               uint64_t offset, uint64_t size,
                               CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 3 + 2);
    dst->push_back(static_cast<char>(Type::kDedupKVUvl));
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

  // ITEM-18c: V2 DedupKV UVL pointer — 20-byte SHA1 fingerprint
  // embedded immediately after the type byte. Every fresh dedupkv write
  // site emits V2; V1 decode is retained for records written before
  // this change landed.
  static void EncodeDedupKVUvlV2(std::string* dst, const UvlFingerprint& fp,
                                 uint64_t file_number, uint64_t offset,
                                 uint64_t size, CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(1 + kUvlFingerprintSize + kMaxVarint64Length * 3 + 1);
    dst->push_back(static_cast<char>(Type::kDedupKVUvlV2));
    dst->append(reinterpret_cast<const char*>(fp.data()), kUvlFingerprintSize);
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

  static void EncodeBlobTTL(std::string* dst, uint64_t expiration,
                            uint64_t file_number, uint64_t offset,
                            uint64_t size, CompressionType compression) {
    assert(dst != nullptr);
    dst->clear();
    dst->reserve(kMaxVarint64Length * 4 + 2);
    dst->push_back(static_cast<char>(Type::kBlobTTL));
    PutVarint64(dst, expiration);
    PutVarint64(dst, file_number);
    PutVarint64(dst, offset);
    PutVarint64(dst, size);
    dst->push_back(static_cast<char>(compression));
  }

 private:
  Type type_ = Type::kUnknown;
  uint64_t expiration_ = 0;
  Slice value_;
  uint64_t file_number_ = 0;
  uint64_t offset_ = 0;
  uint64_t size_ = 0;
  CompressionType compression_ = kNoCompression;
  // ITEM-18c: SHA1 fingerprint, populated only for V2 dedupkv entries.
  UvlFingerprint fingerprint_{};
};

}  // namespace ROCKSDB_NAMESPACE
