//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// SHA-1 implementation following the RFC 3174 pseudocode. Round
// constants and f()/K() definitions mirror FIPS-180-4 §6.1.2 and
// RFC 3174 §5.

#include "util/sha1.h"

#include <cassert>
#include <cstring>

namespace ROCKSDB_NAMESPACE {

namespace {

inline uint32_t RotL32(uint32_t x, int n) {
  return (x << n) | (x >> (32 - n));
}

}  // namespace

Sha1::Sha1() {
  // Initial hash values per RFC 3174 §6.1.
  h_[0] = 0x67452301u;
  h_[1] = 0xEFCDAB89u;
  h_[2] = 0x98BADCFEu;
  h_[3] = 0x10325476u;
  h_[4] = 0xC3D2E1F0u;
  bit_count_ = 0;
  buffer_len_ = 0;
  finalised_ = false;
}

void Sha1::Transform(const uint8_t block[64]) {
  uint32_t w[80];
  for (int i = 0; i < 16; ++i) {
    w[i] = (static_cast<uint32_t>(block[i * 4 + 0]) << 24) |
           (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
           (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
           (static_cast<uint32_t>(block[i * 4 + 3]));
  }
  for (int i = 16; i < 80; ++i) {
    w[i] = RotL32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
  }

  uint32_t a = h_[0], b = h_[1], c = h_[2], d = h_[3], e = h_[4];
  for (int i = 0; i < 80; ++i) {
    uint32_t f, k;
    if (i < 20) {
      f = (b & c) | ((~b) & d);
      k = 0x5A827999u;
    } else if (i < 40) {
      f = b ^ c ^ d;
      k = 0x6ED9EBA1u;
    } else if (i < 60) {
      f = (b & c) | (b & d) | (c & d);
      k = 0x8F1BBCDCu;
    } else {
      f = b ^ c ^ d;
      k = 0xCA62C1D6u;
    }
    uint32_t t = RotL32(a, 5) + f + e + k + w[i];
    e = d;
    d = c;
    c = RotL32(b, 30);
    b = a;
    a = t;
  }

  h_[0] += a;
  h_[1] += b;
  h_[2] += c;
  h_[3] += d;
  h_[4] += e;
}

void Sha1::Update(const uint8_t* data, size_t len) {
  assert(!finalised_);
  bit_count_ += static_cast<uint64_t>(len) * 8;

  // Top up a partially-filled buffer first.
  if (buffer_len_ > 0) {
    const uint32_t need = 64 - buffer_len_;
    const uint32_t take = len < need ? static_cast<uint32_t>(len) : need;
    std::memcpy(buffer_ + buffer_len_, data, take);
    buffer_len_ += take;
    data += take;
    len -= take;
    if (buffer_len_ == 64) {
      Transform(buffer_);
      buffer_len_ = 0;
    }
  }

  // Absorb whole blocks directly from the input.
  while (len >= 64) {
    Transform(data);
    data += 64;
    len -= 64;
  }

  // Buffer the tail.
  if (len > 0) {
    std::memcpy(buffer_, data, len);
    buffer_len_ = static_cast<uint32_t>(len);
  }
}

void Sha1::Final(Sha1Digest* out) {
  assert(!finalised_);
  assert(out != nullptr);

  // Pad with 0x80 then zero bytes, then append the 64-bit bit count
  // big-endian. Total padded length is a multiple of 64.
  const uint64_t total_bits = bit_count_;
  buffer_[buffer_len_++] = 0x80;
  if (buffer_len_ > 56) {
    // Not enough room for the length field in this block — pad to 64,
    // transform, then start a fresh all-zero block.
    while (buffer_len_ < 64) {
      buffer_[buffer_len_++] = 0;
    }
    Transform(buffer_);
    buffer_len_ = 0;
  }
  while (buffer_len_ < 56) {
    buffer_[buffer_len_++] = 0;
  }
  for (int i = 7; i >= 0; --i) {
    buffer_[buffer_len_++] = static_cast<uint8_t>((total_bits >> (i * 8)) & 0xff);
  }
  Transform(buffer_);

  for (int i = 0; i < 5; ++i) {
    (*out)[i * 4 + 0] = static_cast<uint8_t>((h_[i] >> 24) & 0xff);
    (*out)[i * 4 + 1] = static_cast<uint8_t>((h_[i] >> 16) & 0xff);
    (*out)[i * 4 + 2] = static_cast<uint8_t>((h_[i] >> 8) & 0xff);
    (*out)[i * 4 + 3] = static_cast<uint8_t>(h_[i] & 0xff);
  }
  finalised_ = true;
}

Sha1Digest Sha1Hash(const uint8_t* data, size_t len) {
  Sha1 s;
  if (len > 0) {
    s.Update(data, len);
  }
  Sha1Digest out;
  s.Final(&out);
  return out;
}

}  // namespace ROCKSDB_NAMESPACE
