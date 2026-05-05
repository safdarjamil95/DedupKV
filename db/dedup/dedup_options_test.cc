//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Tests for the DedupKV option surface (ITEM-11).
//
// Covers defaults (dedup off preserves baseline), the OPTIONS-file
// roundtrip (settable_test handles the wider coverage), and the
// per-field defaults agreed in plan.md AMBIGUITY-009/010/011.

#include <string>

#include "rocksdb/advanced_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DedupOptionsTest : public testing::Test {};

TEST_F(DedupOptionsTest, DefaultsMatchPlan) {
  DedupKVOptions d;
  EXPECT_FALSE(d.enable);
  EXPECT_EQ(d.mode, DedupMode::kElastic);
  EXPECT_DOUBLE_EQ(d.memory_threshold_pct, 0.80);
  EXPECT_EQ(d.chunk_threshold_bytes, 64u);
  EXPECT_DOUBLE_EQ(d.uvl_gc_threshold, 0.5);
  EXPECT_FALSE(d.cold_tier_enabled);
  EXPECT_EQ(d.cit_checkpoint_every_flushes, 1u);
}

TEST_F(DedupOptionsTest, ColumnFamilyOptionsDefaultsDisableDedup) {
  ColumnFamilyOptions cfo;
  EXPECT_FALSE(cfo.dedupkv.enable);
  EXPECT_EQ(cfo.dedupkv.mode, DedupMode::kElastic);
}

TEST_F(DedupOptionsTest, SetOptionsFromStringRoundTrips) {
  ColumnFamilyOptions base;
  ColumnFamilyOptions parsed;
  ConfigOptions co;
  co.input_strings_escaped = false;
  co.ignore_unknown_options = false;

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      co, base,
      "dedupkv={enable=true;mode=kOfflineOnly;memory_threshold_pct=0.9;"
      "chunk_threshold_bytes=256;uvl_gc_threshold=0.7;"
      "cold_tier_enabled=true;cit_checkpoint_every_flushes=3}",
      &parsed));

  EXPECT_TRUE(parsed.dedupkv.enable);
  EXPECT_EQ(parsed.dedupkv.mode, DedupMode::kOfflineOnly);
  EXPECT_DOUBLE_EQ(parsed.dedupkv.memory_threshold_pct, 0.9);
  EXPECT_EQ(parsed.dedupkv.chunk_threshold_bytes, 256u);
  EXPECT_DOUBLE_EQ(parsed.dedupkv.uvl_gc_threshold, 0.7);
  EXPECT_TRUE(parsed.dedupkv.cold_tier_enabled);
  EXPECT_EQ(parsed.dedupkv.cit_checkpoint_every_flushes, 3u);
}

TEST_F(DedupOptionsTest, PartialUpdatePreservesOtherFields) {
  ColumnFamilyOptions base;
  // Non-default starting value so we can see what gets overwritten.
  base.dedupkv.enable = true;
  base.dedupkv.chunk_threshold_bytes = 512;
  base.dedupkv.cit_checkpoint_every_flushes = 7;

  ColumnFamilyOptions parsed;
  ConfigOptions co;
  co.input_strings_escaped = false;
  co.ignore_unknown_options = false;

  // Only touch memory_threshold_pct.
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      co, base,
      "dedupkv={memory_threshold_pct=0.55}",
      &parsed));

  EXPECT_TRUE(parsed.dedupkv.enable);            // preserved from base
  EXPECT_EQ(parsed.dedupkv.chunk_threshold_bytes, 512u);  // preserved
  EXPECT_EQ(parsed.dedupkv.cit_checkpoint_every_flushes, 7u);  // preserved
  EXPECT_DOUBLE_EQ(parsed.dedupkv.memory_threshold_pct, 0.55);  // updated
}

TEST_F(DedupOptionsTest, UnknownModeStringIsRejected) {
  ColumnFamilyOptions base;
  ColumnFamilyOptions parsed;
  ConfigOptions co;
  co.input_strings_escaped = false;
  co.ignore_unknown_options = false;
  EXPECT_NOK(GetColumnFamilyOptionsFromString(
      co, base, "dedupkv={mode=kNotARealMode}", &parsed));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
