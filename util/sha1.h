//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// SHA-1 per RFC 3174. Added for DedupKV's content-addressed dedup
// (plan.md ITEM-07 / AMBIGUITY-001). RocksDB otherwise uses XXH3 /
// XXH32 for non-cryptographic hashing; SHA-1 is needed because the
// paper's evaluation compares against ZFS's content-addressed dedup
// and requires a 20-byte fingerprint with collision resistance well
// beyond 64-bit hashes.
//
// NOT for security-critical use. SHA-1 is cryptographically broken
// against chosen-prefix collisions; here it serves purely as a
// dedup-index hash where the storage engine treats a collision as a
// correctness bug rather than an attacker-exploitable flaw. Users who
// need collision-resistance guarantees against adversarial inputs
// should wrap the key+value with an HMAC or switch to SHA-256 via a
// future DedupKV option.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

constexpr size_t kSha1DigestSize = 20;
using Sha1Digest = std::array<uint8_t, kSha1DigestSize>;

class Sha1 {
 public:
  Sha1();

  // Absorb `len` bytes from `data`. May be called repeatedly.
  void Update(const uint8_t* data, size_t len);

  // Finalise and write the 20-byte digest. After Final(), the Sha1
  // object is in a "finalised" state; further Update() calls are a
  // bug (will trip an assert in debug builds).
  void Final(Sha1Digest* out);

 private:
  void Transform(const uint8_t block[64]);

  uint32_t h_[5];
  uint8_t buffer_[64];
  uint64_t bit_count_;
  uint32_t buffer_len_;  // 0..63
  bool finalised_;
};

// One-shot helper for the common case — equivalent to `Sha1 s; s.Update;
// s.Final`. Returns the digest directly rather than via an out-param
// because it's a pure function of the input.
Sha1Digest Sha1Hash(const uint8_t* data, size_t len);

}  // namespace ROCKSDB_NAMESPACE
