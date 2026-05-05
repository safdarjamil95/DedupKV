//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/cit_cold_tier.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "db/dedup/cit.h"
#include "db/dedup/uvl_log_format.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

UvlFingerprint MakeFingerprint(uint8_t seed) {
  UvlFingerprint fp{};
  for (size_t i = 0; i < fp.size(); ++i) {
    fp[i] = static_cast<uint8_t>(seed + i * 3);
  }
  return fp;
}

CITEntry MakeEntry(uint64_t file, uint64_t offset, uint32_t size,
                   uint32_t refcount = 1) {
  CITEntry e;
  e.uvl_file = file;
  e.offset = offset;
  e.size = size;
  e.refcount = refcount;
  e.compression = UvlCompression::kRaw;
  return e;
}

}  // namespace

class InMemoryColdTierTest : public testing::Test {};

TEST_F(InMemoryColdTierTest, PutGetErase) {
  InMemoryColdTier cold;
  const auto fp = MakeFingerprint(0x01);

  CITEntry got;
  bool found = true;
  ASSERT_OK(cold.Get(fp, &got, &found));
  EXPECT_FALSE(found);

  const auto entry = MakeEntry(17, 256, 128, /*refcount=*/3);
  ASSERT_OK(cold.Put(fp, entry));
  ASSERT_OK(cold.Get(fp, &got, &found));
  ASSERT_TRUE(found);
  EXPECT_EQ(got.uvl_file, entry.uvl_file);
  EXPECT_EQ(got.offset, entry.offset);
  EXPECT_EQ(got.size, entry.size);
  EXPECT_EQ(got.refcount, entry.refcount);

  ASSERT_OK(cold.Erase(fp));
  ASSERT_OK(cold.Get(fp, &got, &found));
  EXPECT_FALSE(found);
  // Erase of a missing key is idempotent.
  ASSERT_OK(cold.Erase(fp));
}

TEST_F(InMemoryColdTierTest, PutOverwrites) {
  InMemoryColdTier cold;
  const auto fp = MakeFingerprint(0x02);
  ASSERT_OK(cold.Put(fp, MakeEntry(1, 0, 10)));
  ASSERT_OK(cold.Put(fp, MakeEntry(2, 100, 20)));

  CITEntry got;
  bool found = false;
  ASSERT_OK(cold.Get(fp, &got, &found));
  ASSERT_TRUE(found);
  EXPECT_EQ(got.uvl_file, 2U);
  EXPECT_EQ(got.offset, 100U);
  EXPECT_EQ(got.size, 20U);
}

TEST_F(InMemoryColdTierTest, ApproximateSizeTracksPopulation) {
  InMemoryColdTier cold;
  EXPECT_EQ(cold.ApproximateSize(), 0U);
  ASSERT_OK(cold.Put(MakeFingerprint(0x10), MakeEntry(1, 0, 10)));
  ASSERT_OK(cold.Put(MakeFingerprint(0x11), MakeEntry(2, 0, 10)));
  EXPECT_EQ(cold.ApproximateSize(), 2U);
  ASSERT_OK(cold.Erase(MakeFingerprint(0x10)));
  EXPECT_EQ(cold.ApproximateSize(), 1U);
}

TEST_F(InMemoryColdTierTest, EvictionCallbackForwardsEvictedEntries) {
  // Wire the adapter into a live CIT: inserting past capacity should
  // move cold entries into the cold tier via the callback.
  InMemoryColdTier cold;
  CIT cit;
  cit.SetEvictionCallback(MakeColdTierEvictionCallback(&cold));

  // Insert three fps; evict two cold ones; confirm both ended up in the
  // cold tier with their full entry contents.
  ASSERT_TRUE(cit.Insert(MakeFingerprint(0x20), MakeEntry(1, 0, 10)));
  ASSERT_TRUE(cit.Insert(MakeFingerprint(0x21), MakeEntry(2, 100, 20)));
  ASSERT_TRUE(cit.Insert(MakeFingerprint(0x22), MakeEntry(3, 200, 30)));

  EXPECT_EQ(cit.EvictColdEntries(2), 2U);
  EXPECT_EQ(cold.ApproximateSize(), 2U);

  CITEntry got;
  bool found = false;
  ASSERT_OK(cold.Get(MakeFingerprint(0x20), &got, &found));
  ASSERT_TRUE(found);
  EXPECT_EQ(got.uvl_file, 1U);
  ASSERT_OK(cold.Get(MakeFingerprint(0x21), &got, &found));
  ASSERT_TRUE(found);
  EXPECT_EQ(got.uvl_file, 2U);
  // The MRU entry survived in the hot tier.
  ASSERT_OK(cold.Get(MakeFingerprint(0x22), &got, &found));
  EXPECT_FALSE(found);
}

TEST_F(InMemoryColdTierTest, NullColdTierCallbackIsSafe) {
  // Adapter with nullptr cold tier must not crash during eviction.
  auto cb = MakeColdTierEvictionCallback(nullptr);
  CIT cit;
  cit.SetEvictionCallback(std::move(cb));
  ASSERT_TRUE(cit.Insert(MakeFingerprint(0x30), MakeEntry(1, 0, 10)));
  EXPECT_EQ(cit.EvictColdEntries(1), 1U);
}

TEST_F(InMemoryColdTierTest, GetRequiresFoundOutParam) {
  InMemoryColdTier cold;
  CITEntry got;
  EXPECT_TRUE(
      cold.Get(MakeFingerprint(0x40), &got, /*found=*/nullptr)
          .IsInvalidArgument());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
