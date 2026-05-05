//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_log_format.h"

#include <cstdint>
#include <string>

#include "test_util/testharness.h"
#include "util/crc32c.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

UvlFingerprint MakeFingerprint(uint8_t seed) {
  UvlFingerprint fp{};
  for (size_t i = 0; i < fp.size(); ++i) {
    fp[i] = static_cast<uint8_t>(seed + i);
  }
  return fp;
}

std::string RandomBytes(Random* rnd, size_t n) {
  std::string out;
  out.resize(n);
  for (size_t i = 0; i < n; ++i) {
    out[i] = static_cast<char>(rnd->Uniform(256));
  }
  return out;
}

struct RoundTripCase {
  UvlFingerprint fp;
  UvlCompression compression;
  std::string key;
  std::string value;
};

void VerifyRoundTrip(const RoundTripCase& c) {
  std::string buf;
  size_t record_size =
      EncodeUvlRecord(&buf, c.fp, c.compression, c.key, c.value);
  ASSERT_EQ(record_size, buf.size());
  ASSERT_EQ(record_size, UvlRecordCrcOffset(record_size) + sizeof(uint32_t));

  Slice input(buf);
  UvlRecord decoded;
  ASSERT_OK(DecodeUvlRecord(&input, &decoded));
  EXPECT_EQ(input.size(), 0U);  // exact consumption
  EXPECT_EQ(decoded.fingerprint, c.fp);
  EXPECT_EQ(decoded.compression, c.compression);
  EXPECT_EQ(decoded.key.ToString(), c.key);
  EXPECT_EQ(decoded.value.ToString(), c.value);
}

}  // namespace

class UvlLogFormatTest : public testing::Test {};

TEST_F(UvlLogFormatTest, HeaderRoundTrip) {
  UvlHeader h;
  h.column_family_id = 42;
  h.creation_time = 1700000000ULL;
  h.flags = 0;

  std::string encoded;
  h.EncodeTo(&encoded);
  ASSERT_EQ(encoded.size(), UvlHeader::kSize);

  UvlHeader decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(encoded)));
  EXPECT_EQ(decoded.version, kUvlVersion1);
  EXPECT_EQ(decoded.column_family_id, h.column_family_id);
  EXPECT_EQ(decoded.creation_time, h.creation_time);
  EXPECT_EQ(decoded.flags, h.flags);
}

TEST_F(UvlLogFormatTest, HeaderRejectsBadMagic) {
  UvlHeader h;
  std::string encoded;
  h.EncodeTo(&encoded);
  // Flip the magic's low byte.
  encoded[0] ^= 0xff;
  UvlHeader decoded;
  ASSERT_TRUE(decoded.DecodeFrom(Slice(encoded)).IsCorruption());
}

TEST_F(UvlLogFormatTest, HeaderRejectsWrongSize) {
  UvlHeader decoded;
  std::string short_buf(UvlHeader::kSize - 1, '\0');
  ASSERT_TRUE(decoded.DecodeFrom(Slice(short_buf)).IsCorruption());
}

TEST_F(UvlLogFormatTest, RecordRoundTripRawSmall) {
  VerifyRoundTrip({MakeFingerprint(0x10), UvlCompression::kRaw, "k", "v"});
}

TEST_F(UvlLogFormatTest, RecordRoundTripRawLarge) {
  uint32_t seed = static_cast<uint32_t>(testing::UnitTest::GetInstance()
                                             ->random_seed());
  Random rnd(seed);
  SCOPED_TRACE("seed=" + std::to_string(seed));
  VerifyRoundTrip({MakeFingerprint(0x20), UvlCompression::kRaw,
                   RandomBytes(&rnd, 37), RandomBytes(&rnd, 4096)});
}

TEST_F(UvlLogFormatTest, RecordRoundTripLz4Inline) {
  // DGD small-value branch: value carries a blob of "compressed" bytes
  // (opaque to the codec; compression byte records the branch).
  VerifyRoundTrip({MakeFingerprint(0x30), UvlCompression::kLz4Inline,
                   "small-key", std::string("\x01\x02\x03\x04", 4)});
}

TEST_F(UvlLogFormatTest, RecordRoundTripEmptyValue) {
  // Zero-length value is legal (tombstone-like records in the offline
  // path may pass through this codec in future items).
  VerifyRoundTrip(
      {MakeFingerprint(0x40), UvlCompression::kRaw, "only-key", ""});
}

TEST_F(UvlLogFormatTest, MultipleRecordsStreamDecode) {
  std::string buf;
  RoundTripCase cases[3] = {
      {MakeFingerprint(0x50), UvlCompression::kRaw, "a", "alpha"},
      {MakeFingerprint(0x51), UvlCompression::kLz4Inline, "bb", "beta"},
      {MakeFingerprint(0x52), UvlCompression::kRaw, "ccc", "gamma"},
  };
  for (const auto& c : cases) {
    EncodeUvlRecord(&buf, c.fp, c.compression, c.key, c.value);
  }
  Slice input(buf);
  for (const auto& c : cases) {
    UvlRecord decoded;
    ASSERT_OK(DecodeUvlRecord(&input, &decoded));
    EXPECT_EQ(decoded.fingerprint, c.fp);
    EXPECT_EQ(decoded.compression, c.compression);
    EXPECT_EQ(decoded.key.ToString(), c.key);
    EXPECT_EQ(decoded.value.ToString(), c.value);
  }
  EXPECT_EQ(input.size(), 0U);
}

TEST_F(UvlLogFormatTest, CorruptedCrcDetected) {
  std::string buf;
  EncodeUvlRecord(&buf, MakeFingerprint(0x60), UvlCompression::kRaw,
                  "key", "value");
  // Flip a byte inside the value region (before the CRC) and expect
  // the CRC to catch it.
  const size_t crc_off = UvlRecordCrcOffset(buf.size());
  ASSERT_GT(crc_off, 1U);
  buf[crc_off - 1] ^= 0x01;
  Slice input(buf);
  UvlRecord decoded;
  ASSERT_TRUE(DecodeUvlRecord(&input, &decoded).IsCorruption());
}

TEST_F(UvlLogFormatTest, CorruptedFingerprintDetected) {
  // Verifies CRC covers the fingerprint (not just key+value).
  std::string buf;
  EncodeUvlRecord(&buf, MakeFingerprint(0x70), UvlCompression::kRaw,
                  "k", "v");
  buf[0] ^= 0x55;  // first fingerprint byte
  Slice input(buf);
  UvlRecord decoded;
  ASSERT_TRUE(DecodeUvlRecord(&input, &decoded).IsCorruption());
}

TEST_F(UvlLogFormatTest, TruncatedRecordDetected) {
  std::string buf;
  EncodeUvlRecord(&buf, MakeFingerprint(0x80), UvlCompression::kRaw,
                  "key", "value");
  // Drop the trailing CRC bytes.
  buf.resize(buf.size() - 1);
  Slice input(buf);
  UvlRecord decoded;
  ASSERT_TRUE(DecodeUvlRecord(&input, &decoded).IsCorruption());
}

TEST_F(UvlLogFormatTest, UnknownCompressionByteRejected) {
  std::string buf;
  EncodeUvlRecord(&buf, MakeFingerprint(0x90), UvlCompression::kRaw,
                  "k", "v");
  // Compression byte sits at offset kUvlFingerprintSize.
  buf[kUvlFingerprintSize] = static_cast<char>(0x7f);
  // Rewrite the CRC to match the tampered payload so only the
  // compression-byte check fires (not the CRC check).
  const size_t crc_off = UvlRecordCrcOffset(buf.size());
  uint32_t new_crc = crc32c::Value(buf.data(), crc_off);
  new_crc = crc32c::Mask(new_crc);
  for (int i = 0; i < 4; ++i) {
    buf[crc_off + i] = static_cast<char>((new_crc >> (8 * i)) & 0xff);
  }
  Slice input(buf);
  UvlRecord decoded;
  ASSERT_TRUE(DecodeUvlRecord(&input, &decoded).IsCorruption());
}

TEST_F(UvlLogFormatTest, DecodedRecordOutlivesInput) {
  // Encode into a buffer, decode, then destroy the encode buffer.
  // The decoded record must still be readable because it owns its
  // key/value storage.
  UvlRecord decoded;
  {
    std::string buf;
    EncodeUvlRecord(&buf, MakeFingerprint(0xA0), UvlCompression::kRaw,
                    "persisted-key", "persisted-value");
    Slice input(buf);
    ASSERT_OK(DecodeUvlRecord(&input, &decoded));
  }
  EXPECT_EQ(decoded.key.ToString(), "persisted-key");
  EXPECT_EQ(decoded.value.ToString(), "persisted-value");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
