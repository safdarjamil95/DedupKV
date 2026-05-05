//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_log_format.h"

#include <cassert>
#include <cstring>

#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE {

void UvlHeader::EncodeTo(std::string* dst) const {
  assert(dst != nullptr);
  dst->clear();
  dst->reserve(UvlHeader::kSize);
  PutFixed32(dst, kUvlMagicNumber);
  PutFixed32(dst, version);
  PutFixed32(dst, column_family_id);
  PutFixed64(dst, creation_time);
  PutFixed32(dst, flags);
}

Status UvlHeader::DecodeFrom(Slice src) {
  const char* kErrorMessage = "Error while decoding UVL header";
  if (src.size() != UvlHeader::kSize) {
    return Status::Corruption(kErrorMessage, "Unexpected UVL header size");
  }
  uint32_t magic_number = 0;
  if (!GetFixed32(&src, &magic_number) || !GetFixed32(&src, &version) ||
      !GetFixed32(&src, &column_family_id) ||
      !GetFixed64(&src, &creation_time) || !GetFixed32(&src, &flags)) {
    return Status::Corruption(kErrorMessage, "Error decoding fields");
  }
  if (magic_number != kUvlMagicNumber) {
    return Status::Corruption(kErrorMessage, "Magic number mismatch");
  }
  if (version != kUvlVersion1) {
    return Status::Corruption(kErrorMessage, "Unknown UVL header version");
  }
  return Status::OK();
}

size_t EncodeUvlRecord(std::string* dst, const UvlFingerprint& fingerprint,
                       UvlCompression compression, const Slice& key,
                       const Slice& value) {
  assert(dst != nullptr);
  const size_t start = dst->size();
  dst->append(reinterpret_cast<const char*>(fingerprint.data()),
              kUvlFingerprintSize);
  dst->push_back(static_cast<char>(compression));
  PutLengthPrefixedSlice(dst, key);
  PutLengthPrefixedSlice(dst, value);
  // CRC covers fingerprint + compression + lp-prefixed key + lp-prefixed
  // value (everything written since `start`).
  uint32_t crc = crc32c::Value(dst->data() + start, dst->size() - start);
  crc = crc32c::Mask(crc);
  PutFixed32(dst, crc);
  return dst->size() - start;
}

Status DecodeUvlRecord(Slice* input, UvlRecord* out) {
  assert(input != nullptr);
  assert(out != nullptr);
  const char* kErrorMessage = "Error while decoding UVL record";

  // Minimum record size: fp(20) + compression(1) + ksz(1) + vsz(1) +
  // crc(4) = 27 bytes with zero-length key/value.
  constexpr size_t kMinRecordSize =
      kUvlFingerprintSize + 1 + 1 + 1 + sizeof(uint32_t);
  if (input->size() < kMinRecordSize) {
    return Status::Corruption(kErrorMessage, "Record truncated");
  }

  // Snapshot the record start so we can CRC the payload once the
  // payload length is known.
  const char* record_start = input->data();
  Slice cursor = *input;

  // Fingerprint (fixed 20 bytes).
  std::memcpy(out->fingerprint.data(), cursor.data(), kUvlFingerprintSize);
  cursor.remove_prefix(kUvlFingerprintSize);

  // Compression byte.
  uint8_t comp_byte = static_cast<uint8_t>(cursor.data()[0]);
  if (comp_byte != static_cast<uint8_t>(UvlCompression::kRaw) &&
      comp_byte != static_cast<uint8_t>(UvlCompression::kLz4Inline)) {
    return Status::Corruption(kErrorMessage, "Unknown compression byte");
  }
  out->compression = static_cast<UvlCompression>(comp_byte);
  cursor.remove_prefix(1);

  // Length-prefixed key.
  Slice key_slice;
  if (!GetLengthPrefixedSlice(&cursor, &key_slice)) {
    return Status::Corruption(kErrorMessage, "Bad key encoding");
  }

  // Length-prefixed value.
  Slice value_slice;
  if (!GetLengthPrefixedSlice(&cursor, &value_slice)) {
    return Status::Corruption(kErrorMessage, "Bad value encoding");
  }

  // Trailing CRC.
  if (cursor.size() < sizeof(uint32_t)) {
    return Status::Corruption(kErrorMessage, "Truncated CRC");
  }
  const size_t payload_len =
      static_cast<size_t>(cursor.data() - record_start);
  uint32_t stored_crc = 0;
  // GetFixed32 advances cursor past the CRC bytes.
  if (!GetFixed32(&cursor, &stored_crc)) {
    return Status::Corruption(kErrorMessage, "Failed to read CRC");
  }
  uint32_t expected = crc32c::Value(record_start, payload_len);
  expected = crc32c::Mask(expected);
  if (expected != stored_crc) {
    return Status::Corruption(kErrorMessage, "CRC mismatch");
  }

  // Copy key/value into owned buffers so `out` outlives `input`.
  out->key_buf.assign(key_slice.data(), key_slice.size());
  out->value_buf.assign(value_slice.data(), value_slice.size());
  out->key = Slice(out->key_buf);
  out->value = Slice(out->value_buf);

  // Commit consumption.
  *input = cursor;
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
