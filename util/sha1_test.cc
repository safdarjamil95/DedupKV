//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/sha1.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

std::string HexOf(const Sha1Digest& d) {
  static const char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(kSha1DigestSize * 2);
  for (uint8_t b : d) {
    out.push_back(kHex[b >> 4]);
    out.push_back(kHex[b & 0xf]);
  }
  return out;
}

Sha1Digest HashStr(const std::string& s) {
  return Sha1Hash(reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

}  // namespace

class Sha1Test : public testing::Test {};

// RFC 3174 test vectors.

TEST_F(Sha1Test, EmptyString) {
  // SHA1("") = da39a3ee5e6b4b0d3255bfef95601890afd80709
  EXPECT_EQ(HexOf(HashStr("")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
}

TEST_F(Sha1Test, Abc) {
  // SHA1("abc") = a9993e364706816aba3e25717850c26c9cd0d89d
  EXPECT_EQ(HexOf(HashStr("abc")),
            "a9993e364706816aba3e25717850c26c9cd0d89d");
}

TEST_F(Sha1Test, FiftySixByteInput) {
  const std::string msg =
      "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
  ASSERT_EQ(msg.size(), 56U);
  EXPECT_EQ(HexOf(HashStr(msg)),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1");
}

TEST_F(Sha1Test, MillionA) {
  // SHA1("a" * 1_000_000) = 34aa973cd4c4daa4f61eeb2bdbad27316534016f
  Sha1 s;
  std::vector<uint8_t> chunk(10000, 'a');
  for (int i = 0; i < 100; ++i) {
    s.Update(chunk.data(), chunk.size());
  }
  Sha1Digest out;
  s.Final(&out);
  EXPECT_EQ(HexOf(out), "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
}

TEST_F(Sha1Test, UpdateEquivalentToSingleShot) {
  const std::string msg =
      "The quick brown fox jumps over the lazy dog";
  const auto full = HashStr(msg);

  // Feed byte-by-byte and compare.
  Sha1 s;
  for (char c : msg) {
    uint8_t b = static_cast<uint8_t>(c);
    s.Update(&b, 1);
  }
  Sha1Digest byte_by_byte;
  s.Final(&byte_by_byte);
  EXPECT_EQ(full, byte_by_byte);
}

TEST_F(Sha1Test, ZeroFilledBuffer1MB) {
  // Regression check against precomputed SHA1 of 1 MiB of 0x00 bytes.
  // Computed externally with: python3 -c "import hashlib; print(hashlib.sha1(b'\x00'*1048576).hexdigest())"
  std::vector<uint8_t> zeros(1024 * 1024, 0);
  const auto d = Sha1Hash(zeros.data(), zeros.size());
  EXPECT_EQ(HexOf(d), "3b71f43ff30f4b15b5cd85dd9e95ebc7e84eb5a3");
}

TEST_F(Sha1Test, DifferentInputsGiveDifferentHashes) {
  EXPECT_NE(HashStr("a"), HashStr("b"));
  EXPECT_NE(HashStr("abc"), HashStr("abcd"));
  EXPECT_NE(HashStr("hello world"), HashStr("Hello world"));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
