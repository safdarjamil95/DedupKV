//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/cit.h"

#include <atomic>
#include <cstdint>
#include <limits>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

UvlFingerprint MakeFingerprint(uint8_t seed) {
  UvlFingerprint fp{};
  for (size_t i = 0; i < fp.size(); ++i) {
    fp[i] = static_cast<uint8_t>(seed + i * 7);
  }
  return fp;
}

CITEntry MakeEntry(uint64_t file, uint64_t offset, uint32_t size,
                   uint32_t refcount = 1,
                   UvlCompression compression = UvlCompression::kRaw) {
  CITEntry e;
  e.uvl_file = file;
  e.offset = offset;
  e.size = size;
  e.refcount = refcount;
  e.compression = compression;
  return e;
}

}  // namespace

class CITTest : public testing::Test {};

TEST_F(CITTest, InsertAndLookup) {
  CIT cit;
  const auto fp = MakeFingerprint(0x01);
  const auto entry = MakeEntry(10, 64, 128);

  EXPECT_EQ(cit.Size(), 0U);
  EXPECT_TRUE(cit.Insert(fp, entry));
  EXPECT_EQ(cit.Size(), 1U);

  CITEntry got;
  ASSERT_TRUE(cit.Lookup(fp, &got));
  EXPECT_EQ(got.uvl_file, entry.uvl_file);
  EXPECT_EQ(got.offset, entry.offset);
  EXPECT_EQ(got.size, entry.size);
  EXPECT_EQ(got.refcount, entry.refcount);

  // Lookup of an absent fp returns false and leaves `got` well-defined
  // (not checked — the contract says it's unspecified).
  EXPECT_FALSE(cit.Lookup(MakeFingerprint(0x99), &got));
}

TEST_F(CITTest, DuplicateInsertRejected) {
  CIT cit;
  const auto fp = MakeFingerprint(0x02);
  ASSERT_TRUE(cit.Insert(fp, MakeEntry(1, 0, 10)));
  EXPECT_FALSE(cit.Insert(fp, MakeEntry(2, 0, 20)));
  CITEntry got;
  ASSERT_TRUE(cit.Lookup(fp, &got));
  EXPECT_EQ(got.uvl_file, 1U);  // first-writer-wins
  EXPECT_EQ(got.size, 10U);
}

TEST_F(CITTest, LookupOrInsertMissThenHit) {
  CIT cit;
  const auto fp = MakeFingerprint(0x03);
  CITEntry out;
  EXPECT_FALSE(cit.LookupOrInsert(fp, MakeEntry(1, 0, 10), &out));
  EXPECT_EQ(out.refcount, 1U);
  EXPECT_EQ(out.uvl_file, 1U);

  // Subsequent LookupOrInsert hits; refcount increments to 2.
  EXPECT_TRUE(cit.LookupOrInsert(fp, MakeEntry(99, 99, 99), &out));
  EXPECT_EQ(out.refcount, 2U);
  EXPECT_EQ(out.uvl_file, 1U);  // existing entry wins, not `new_entry`.
}

TEST_F(CITTest, LookupOrInsertMissForcesRefcountOne) {
  CIT cit;
  const auto fp = MakeFingerprint(0x04);
  CITEntry out;
  // Caller passes refcount=42; CIT must clamp to 1 on insert.
  EXPECT_FALSE(
      cit.LookupOrInsert(fp, MakeEntry(1, 0, 10, /*refcount=*/42), &out));
  EXPECT_EQ(out.refcount, 1U);
  CITEntry got;
  ASSERT_TRUE(cit.Lookup(fp, &got));
  EXPECT_EQ(got.refcount, 1U);
}

TEST_F(CITTest, IncRefcountOnMiss) {
  CIT cit;
  const auto fp = MakeFingerprint(0x05);
  EXPECT_EQ(cit.IncRefcount(fp), std::numeric_limits<uint32_t>::max());
}

TEST_F(CITTest, IncAndDecRefcount) {
  CIT cit;
  const auto fp = MakeFingerprint(0x06);
  ASSERT_TRUE(cit.Insert(fp, MakeEntry(1, 0, 10, /*refcount=*/3)));
  EXPECT_EQ(cit.IncRefcount(fp), 4U);
  EXPECT_EQ(cit.IncRefcount(fp), 5U);
  EXPECT_EQ(cit.DecRefcount(fp), 4U);
  EXPECT_EQ(cit.DecRefcount(fp), 3U);
}

TEST_F(CITTest, DecSaturatesAtZero) {
  CIT cit;
  const auto fp = MakeFingerprint(0x07);
  ASSERT_TRUE(cit.Insert(fp, MakeEntry(1, 0, 10, /*refcount=*/1)));
  EXPECT_EQ(cit.DecRefcount(fp), 0U);
  EXPECT_EQ(cit.DecRefcount(fp), 0U);  // does not underflow
}

TEST_F(CITTest, EvictLeastRecentlyUsed) {
  CIT cit;
  // Insert 5 distinct fps in order fp0..fp4.
  for (uint8_t i = 0; i < 5; ++i) {
    ASSERT_TRUE(
        cit.Insert(MakeFingerprint(static_cast<uint8_t>(0x10 + i)),
                   MakeEntry(1 + i, 0, 10)));
  }
  EXPECT_EQ(cit.Size(), 5U);

  // Touch fp0 so it becomes MRU, then evict 2 oldest.
  CITEntry tmp;
  ASSERT_TRUE(cit.Lookup(MakeFingerprint(0x10), &tmp));
  EXPECT_EQ(cit.EvictColdEntries(2), 2U);
  EXPECT_EQ(cit.Size(), 3U);

  // fp1 and fp2 were oldest after fp0's touch — they should be gone.
  EXPECT_FALSE(cit.Lookup(MakeFingerprint(0x11), &tmp));
  EXPECT_FALSE(cit.Lookup(MakeFingerprint(0x12), &tmp));
  // fp0, fp3, fp4 survive.
  EXPECT_TRUE(cit.Lookup(MakeFingerprint(0x10), &tmp));
  EXPECT_TRUE(cit.Lookup(MakeFingerprint(0x13), &tmp));
  EXPECT_TRUE(cit.Lookup(MakeFingerprint(0x14), &tmp));
}

TEST_F(CITTest, EvictionCallbackFiresWithFullEntry) {
  CIT cit;
  std::vector<std::pair<UvlFingerprint, CITEntry>> evicted;
  cit.SetEvictionCallback(
      [&](const UvlFingerprint& fp, const CITEntry& e) {
        evicted.emplace_back(fp, e);
      });

  ASSERT_TRUE(cit.Insert(MakeFingerprint(0x20), MakeEntry(1, 0, 10)));
  ASSERT_TRUE(cit.Insert(MakeFingerprint(0x21), MakeEntry(2, 0, 20)));
  EXPECT_EQ(cit.EvictColdEntries(1), 1U);
  ASSERT_EQ(evicted.size(), 1U);
  // First-inserted fp (0x20) was LRU — it must be the one reported.
  EXPECT_EQ(evicted[0].first, MakeFingerprint(0x20));
  EXPECT_EQ(evicted[0].second.uvl_file, 1U);
  EXPECT_EQ(cit.Evictions(), 1U);
}

TEST_F(CITTest, SnapshotCopiesEverything) {
  CIT cit;
  for (uint8_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(cit.Insert(MakeFingerprint(static_cast<uint8_t>(0x30 + i)),
                           MakeEntry(1 + i, i * 64, 32)));
  }
  std::vector<std::pair<UvlFingerprint, CITEntry>> snap;
  cit.Snapshot(&snap);
  ASSERT_EQ(snap.size(), 4U);
  // Map to fp → uvl_file for order-independent checks.
  std::unordered_map<uint64_t, uint64_t> fp_first_byte_to_file;
  for (const auto& [fp, entry] : snap) {
    fp_first_byte_to_file[fp[0]] = entry.uvl_file;
  }
  EXPECT_EQ(fp_first_byte_to_file[0x30], 1U);
  EXPECT_EQ(fp_first_byte_to_file[0x31], 2U);
  EXPECT_EQ(fp_first_byte_to_file[0x32], 3U);
  EXPECT_EQ(fp_first_byte_to_file[0x33], 4U);
}

TEST_F(CITTest, StatsCountersReflectOps) {
  CIT cit;
  const auto fp = MakeFingerprint(0x40);
  CITEntry got;
  EXPECT_FALSE(cit.Lookup(fp, &got));  // miss
  EXPECT_EQ(cit.Misses(), 1U);
  ASSERT_TRUE(cit.Insert(fp, MakeEntry(1, 0, 10)));
  EXPECT_TRUE(cit.Lookup(fp, &got));   // hit
  EXPECT_EQ(cit.Hits(), 1U);

  // LookupOrInsert should register a miss on insert, hit on reuse.
  const auto fp2 = MakeFingerprint(0x41);
  EXPECT_FALSE(cit.LookupOrInsert(fp2, MakeEntry(2, 0, 10), &got));
  EXPECT_EQ(cit.Misses(), 2U);
  EXPECT_TRUE(cit.LookupOrInsert(fp2, MakeEntry(99, 99, 99), &got));
  EXPECT_EQ(cit.Hits(), 2U);
}

TEST_F(CITTest, ConcurrentDedupNoDuplicates) {
  // Stress test: N threads race to LookupOrInsert the same K fps; final
  // refcount for each fp must be exactly N (one hit per thread per fp).
  CIT cit;
  constexpr int kNumThreads = 8;
  constexpr int kNumFingerprints = 64;

  std::vector<UvlFingerprint> fps(kNumFingerprints);
  for (int i = 0; i < kNumFingerprints; ++i) {
    fps[i] = MakeFingerprint(static_cast<uint8_t>(i));
  }

  std::atomic<bool> start{false};
  std::vector<std::thread> workers;
  workers.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; ++t) {
    workers.emplace_back([&, t]() {
      uint32_t seed = static_cast<uint32_t>(t * 17 + 1);
      Random rnd(seed);
      while (!start.load(std::memory_order_acquire)) {
      }
      // Each thread visits every fp exactly once in a shuffled order.
      std::vector<int> order(kNumFingerprints);
      for (int i = 0; i < kNumFingerprints; ++i) order[i] = i;
      for (int i = kNumFingerprints - 1; i > 0; --i) {
        std::swap(order[i],
                  order[static_cast<int>(rnd.Uniform(i + 1))]);
      }
      for (int idx : order) {
        CITEntry out;
        cit.LookupOrInsert(fps[idx],
                           MakeEntry(static_cast<uint64_t>(idx), 0, 64), &out);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& w : workers) w.join();

  // Every fp is present exactly once; refcount == kNumThreads.
  EXPECT_EQ(cit.Size(), static_cast<size_t>(kNumFingerprints));
  for (int i = 0; i < kNumFingerprints; ++i) {
    CITEntry e;
    ASSERT_TRUE(cit.Lookup(fps[i], &e));
    EXPECT_EQ(e.refcount, static_cast<uint32_t>(kNumThreads))
        << "fp index " << i;
    // First-writer-wins on uvl_file: whichever thread raced in first
    // stored its own idx; all threads passed idx == i, so uvl_file == i.
    EXPECT_EQ(e.uvl_file, static_cast<uint64_t>(i));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
