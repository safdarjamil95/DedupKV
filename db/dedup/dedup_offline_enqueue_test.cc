//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Unit tests for OfflineDedupFilterBuilder — the Bloom-filter-builder
// wrapper FlushJob's ITEM-15 offline branch uses to populate DWQEntry.

#include "db/dedup/dedup_offline_enqueue.h"

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class OfflineDedupFilterBuilderTest : public testing::Test {};

TEST_F(OfflineDedupFilterBuilderTest, EmptyBuilderFinishReturnsIncomplete) {
  OfflineDedupFilterBuilder b;
  OfflineDedupFilter out;
  Status s = b.Finish(&out);
  EXPECT_TRUE(s.IsIncomplete()) << s.ToString();
  EXPECT_EQ(b.num_keys_added(), 0u);
}

TEST_F(OfflineDedupFilterBuilderTest, AddedKeysAreReportedAsPresent) {
  OfflineDedupFilterBuilder b;
  const std::vector<std::string> keys{"alpha", "beta",   "gamma",
                                      "delta", "epsilon"};
  for (const auto& k : keys) {
    b.AddKey(Slice(k));
  }
  OfflineDedupFilter out;
  ASSERT_OK(b.Finish(&out));
  ASSERT_NE(out.reader, nullptr);
  EXPECT_EQ(out.num_keys, keys.size());

  for (const auto& k : keys) {
    EXPECT_TRUE(out.reader->MayMatch(Slice(k)))
        << "added key " << k << " must report as a possible match";
  }
}

TEST_F(OfflineDedupFilterBuilderTest, FalsePositiveRateIsLow) {
  OfflineDedupFilterBuilder b(10.0);
  for (int i = 0; i < 1000; ++i) {
    std::string k = "present-" + std::to_string(i);
    b.AddKey(Slice(k));
  }
  OfflineDedupFilter out;
  ASSERT_OK(b.Finish(&out));

  // The classical 10-bit Bloom FP rate is ~1%; count false positives
  // over a disjoint key set and give the filter generous slack.
  int fp = 0;
  constexpr int kProbes = 1000;
  for (int i = 0; i < kProbes; ++i) {
    std::string k = "absent-" + std::to_string(i);
    if (out.reader->MayMatch(Slice(k))) {
      ++fp;
    }
  }
  // Sanity ceiling at 5% — 5x the theoretical FP rate.
  EXPECT_LT(fp, kProbes / 20) << "unexpectedly many false positives";
}

TEST_F(OfflineDedupFilterBuilderTest, DuplicateAddsAreHarmless) {
  OfflineDedupFilterBuilder b;
  for (int i = 0; i < 100; ++i) {
    b.AddKey(Slice("same"));
  }
  OfflineDedupFilter out;
  ASSERT_OK(b.Finish(&out));
  EXPECT_EQ(out.num_keys, 100u);  // AddKey counter is raw, not unique
  EXPECT_TRUE(out.reader->MayMatch(Slice("same")));
}

TEST_F(OfflineDedupFilterBuilderTest, FinishTwiceRejected) {
  OfflineDedupFilterBuilder b;
  b.AddKey(Slice("x"));
  OfflineDedupFilter out1;
  ASSERT_OK(b.Finish(&out1));
  OfflineDedupFilter out2;
  EXPECT_TRUE(b.Finish(&out2).IsInvalidArgument());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
