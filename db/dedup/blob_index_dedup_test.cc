//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Tests for the DedupKV extension to BlobIndex (ITEM-10): a new
// BlobIndex::Type::kDedupKVUvl type tag with the same payload layout
// as kBlob but distinct dispatch semantics.

#include <cstdint>
#include <string>

#include "db/blob/blob_index.h"
#include "db/dedup/uvl_log_format.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class BlobIndexDedupKVTest : public testing::Test {};

TEST_F(BlobIndexDedupKVTest, UvlTypeRoundTrips) {
  std::string encoded;
  BlobIndex::EncodeDedupKVUvl(&encoded, /*file_number=*/42, /*offset=*/1024,
                              /*size=*/256, kNoCompression);
  ASSERT_FALSE(encoded.empty());
  // First byte is the type tag — must be kDedupKVUvl (3), distinct from
  // kBlob (1).
  EXPECT_EQ(static_cast<unsigned char>(encoded[0]),
            static_cast<unsigned char>(BlobIndex::Type::kDedupKVUvl));

  BlobIndex decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(encoded)));
  EXPECT_FALSE(decoded.IsInlined());
  EXPECT_FALSE(decoded.IsBlob());
  EXPECT_TRUE(decoded.IsDedupKVUvl());
  EXPECT_FALSE(decoded.HasTTL());
  EXPECT_EQ(decoded.file_number(), 42U);
  EXPECT_EQ(decoded.offset(), 1024U);
  EXPECT_EQ(decoded.size(), 256U);
  EXPECT_EQ(decoded.compression(), kNoCompression);
}

TEST_F(BlobIndexDedupKVTest, UvlTypeRoundTripsWithLz4Marker) {
  // kLZ4Compression maps to UvlCompression::kLz4Inline in the DGD
  // small-value branch. We reuse CompressionType here rather than
  // introducing a parallel enum — see DEC-004 rationale.
  std::string encoded;
  BlobIndex::EncodeDedupKVUvl(&encoded, 7, 64, 128, kLZ4Compression);

  BlobIndex decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(encoded)));
  EXPECT_TRUE(decoded.IsDedupKVUvl());
  EXPECT_EQ(decoded.compression(), kLZ4Compression);
}

TEST_F(BlobIndexDedupKVTest, EncodeToDispatchesOnInternalType) {
  // Build an index via field assignment + EncodeTo (the path
  // CompactionIterator uses during re-emit), then decode and check
  // type preservation.
  std::string first;
  BlobIndex::EncodeDedupKVUvl(&first, 9, 9, 9, kNoCompression);
  BlobIndex idx;
  ASSERT_OK(idx.DecodeFrom(Slice(first)));
  ASSERT_TRUE(idx.IsDedupKVUvl());

  std::string reencoded;
  idx.EncodeTo(&reencoded);
  EXPECT_EQ(first, reencoded);  // stable across decode→encode
}

TEST_F(BlobIndexDedupKVTest, BlobAndUvlAreDistinguishable) {
  std::string blob_encoded;
  BlobIndex::EncodeBlob(&blob_encoded, 1, 2, 3, kNoCompression);
  std::string uvl_encoded;
  BlobIndex::EncodeDedupKVUvl(&uvl_encoded, 1, 2, 3, kNoCompression);

  // Same payload, different tag byte.
  EXPECT_EQ(blob_encoded.size(), uvl_encoded.size());
  EXPECT_NE(blob_encoded[0], uvl_encoded[0]);
  EXPECT_EQ(blob_encoded.substr(1), uvl_encoded.substr(1));

  BlobIndex b, u;
  ASSERT_OK(b.DecodeFrom(Slice(blob_encoded)));
  ASSERT_OK(u.DecodeFrom(Slice(uvl_encoded)));
  EXPECT_TRUE(b.IsBlob());
  EXPECT_FALSE(b.IsDedupKVUvl());
  EXPECT_TRUE(u.IsDedupKVUvl());
  EXPECT_FALSE(u.IsBlob());
}

TEST_F(BlobIndexDedupKVTest, ExistingTypesUnchangedByEnumShift) {
  // Regression: adding kDedupKVUvl=3 pushed kUnknown to 4. Confirm
  // the three pre-existing type bytes still decode correctly.
  {
    std::string enc;
    BlobIndex::EncodeInlinedTTL(&enc, /*expiration=*/123, Slice("val"));
    BlobIndex d;
    ASSERT_OK(d.DecodeFrom(Slice(enc)));
    EXPECT_TRUE(d.IsInlined());
    EXPECT_TRUE(d.HasTTL());
    EXPECT_FALSE(d.IsDedupKVUvl());
  }
  {
    std::string enc;
    BlobIndex::EncodeBlob(&enc, 10, 20, 30, kNoCompression);
    BlobIndex d;
    ASSERT_OK(d.DecodeFrom(Slice(enc)));
    EXPECT_TRUE(d.IsBlob());
    EXPECT_FALSE(d.IsDedupKVUvl());
  }
  {
    std::string enc;
    BlobIndex::EncodeBlobTTL(&enc, 99, 10, 20, 30, kNoCompression);
    BlobIndex d;
    ASSERT_OK(d.DecodeFrom(Slice(enc)));
    EXPECT_TRUE(d.IsBlob());
    EXPECT_TRUE(d.HasTTL());
    EXPECT_FALSE(d.IsDedupKVUvl());
  }
}

TEST_F(BlobIndexDedupKVTest, UnknownTypeByteStillCorrupts) {
  // ITEM-18c: kUnknown shifted from 4 to 5 to make room for
  // kDedupKVUvlV2=4. Byte value 5 and beyond must still be rejected.
  char bytes[2] = {5, 0};
  BlobIndex d;
  EXPECT_TRUE(d.DecodeFrom(Slice(bytes, 1)).IsCorruption());
  bytes[0] = 127;
  EXPECT_TRUE(d.DecodeFrom(Slice(bytes, 1)).IsCorruption());
}

// ITEM-18c: V2 BlobIndex embeds the SHA1 fingerprint so Get /
// compaction refcount decrement can look up CIT by fp without opening
// the old UVL file (prerequisite for safe old-file deletion in 18d+).
TEST_F(BlobIndexDedupKVTest, V2EmbedsFingerprintRoundTrip) {
  UvlFingerprint fp{};
  for (size_t i = 0; i < fp.size(); ++i) {
    fp[i] = static_cast<uint8_t>(0xA0 + i);
  }

  std::string encoded;
  BlobIndex::EncodeDedupKVUvlV2(&encoded, fp, /*file_number=*/77,
                                /*offset=*/4096, /*size=*/512,
                                kNoCompression);
  ASSERT_FALSE(encoded.empty());
  EXPECT_EQ(static_cast<unsigned char>(encoded[0]),
            static_cast<unsigned char>(BlobIndex::Type::kDedupKVUvlV2));

  BlobIndex decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(encoded)));
  EXPECT_TRUE(decoded.IsDedupKVUvl());  // V1 + V2 both classify as UVL
  EXPECT_TRUE(decoded.HasFingerprint());
  EXPECT_EQ(decoded.fingerprint(), fp);
  EXPECT_EQ(decoded.file_number(), 77U);
  EXPECT_EQ(decoded.offset(), 4096U);
  EXPECT_EQ(decoded.size(), 512U);
  EXPECT_EQ(decoded.compression(), kNoCompression);
}

TEST_F(BlobIndexDedupKVTest, V1EntriesDecodeWithZeroFingerprint) {
  // V1 records on disk (pre-18c) must still decode; fingerprint comes
  // back all-zero and HasFingerprint() reports false so callers can
  // fall back to the file-open path.
  std::string v1_encoded;
  BlobIndex::EncodeDedupKVUvl(&v1_encoded, 11, 22, 33, kNoCompression);
  BlobIndex decoded;
  ASSERT_OK(decoded.DecodeFrom(Slice(v1_encoded)));
  EXPECT_TRUE(decoded.IsDedupKVUvl());
  EXPECT_FALSE(decoded.HasFingerprint());
  const UvlFingerprint zero{};
  EXPECT_EQ(decoded.fingerprint(), zero);
}

TEST_F(BlobIndexDedupKVTest, V2EncodeToIsStableOnReemit) {
  // EncodeTo after decoding must reproduce the exact V2 bytes so
  // CompactionIterator pass-throughs don't silently lose fp.
  UvlFingerprint fp{};
  for (size_t i = 0; i < fp.size(); ++i) fp[i] = static_cast<uint8_t>(i);
  std::string first;
  BlobIndex::EncodeDedupKVUvlV2(&first, fp, 5, 50, 500, kLZ4Compression);
  BlobIndex idx;
  ASSERT_OK(idx.DecodeFrom(Slice(first)));
  std::string reencoded;
  idx.EncodeTo(&reencoded);
  EXPECT_EQ(first, reencoded);
}

TEST_F(BlobIndexDedupKVTest, V2TruncatedFingerprintIsCorruption) {
  // A V2 tag followed by fewer than 20 fp bytes must fail — not be
  // silently interpreted as a V1 payload.
  std::string truncated;
  truncated.push_back(
      static_cast<char>(BlobIndex::Type::kDedupKVUvlV2));
  truncated.append(10, '\0');  // only 10 of the 20 fp bytes
  BlobIndex d;
  EXPECT_TRUE(d.DecodeFrom(Slice(truncated)).IsCorruption());
}

TEST_F(BlobIndexDedupKVTest, DebugStringHasDistinctPrefix) {
  std::string blob_encoded;
  BlobIndex::EncodeBlob(&blob_encoded, 1, 2, 3, kNoCompression);
  std::string uvl_encoded;
  BlobIndex::EncodeDedupKVUvl(&uvl_encoded, 1, 2, 3, kNoCompression);

  BlobIndex b, u;
  ASSERT_OK(b.DecodeFrom(Slice(blob_encoded)));
  ASSERT_OK(u.DecodeFrom(Slice(uvl_encoded)));
  const auto b_dbg = b.DebugString(/*output_hex=*/false);
  const auto u_dbg = u.DebugString(/*output_hex=*/false);
  EXPECT_NE(b_dbg.find("[blob ref]"), std::string::npos);
  EXPECT_NE(u_dbg.find("[uvl ref]"), std::string::npos);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
