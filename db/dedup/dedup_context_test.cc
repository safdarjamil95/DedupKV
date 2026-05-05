//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Tests for DedupContext factory (unit) and the DBImpl hook that
// populates `dedup_contexts_` at Open (integration).

#include "db/dedup/dedup_context.h"

#include <memory>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DedupContextFactoryTest : public testing::Test {};

TEST_F(DedupContextFactoryTest, DisabledReturnsNullptr) {
  DedupKVOptions opts;  // enable=false by default
  auto ctx = MakeDedupContext(opts, /*capacity=*/1 << 20);
  EXPECT_EQ(ctx, nullptr);
}

TEST_F(DedupContextFactoryTest, EnabledBuildsAllMembers) {
  DedupKVOptions opts;
  opts.enable = true;
  auto ctx = MakeDedupContext(opts, /*capacity=*/1 << 20);
  ASSERT_NE(ctx, nullptr);
  EXPECT_NE(ctx->cit, nullptr);
  EXPECT_NE(ctx->memory_monitor, nullptr);
  EXPECT_NE(ctx->dwq, nullptr);
  EXPECT_NE(ctx->dgd_stats, nullptr);
  EXPECT_EQ(ctx->cold_tier, nullptr);  // cold_tier_enabled=false default
  EXPECT_EQ(ctx->memory_monitor->CapacityBytes(), 1ULL << 20);
  EXPECT_EQ(ctx->options_snapshot.enable, true);
}

TEST_F(DedupContextFactoryTest, ColdTierInstantiatedWhenEnabled) {
  DedupKVOptions opts;
  opts.enable = true;
  opts.cold_tier_enabled = true;
  auto ctx = MakeDedupContext(opts, 1 << 20);
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->cold_tier, nullptr);
  // Evicting a hot-tier entry should land in the cold tier via the
  // wired eviction callback.
  CITEntry e;
  e.uvl_file = 1;
  e.offset = 0;
  e.size = 32;
  e.refcount = 1;
  UvlFingerprint fp{};
  fp[0] = 0xAB;
  ASSERT_TRUE(ctx->cit->Insert(fp, e));
  EXPECT_EQ(ctx->cit->EvictColdEntries(1), 1u);
  EXPECT_EQ(ctx->cold_tier->ApproximateSize(), 1u);
}

// Integration side: open a DB with a dedup-enabled CF and confirm the
// DBImpl wired a DedupContext for it.
class DedupContextDBIntegrationTest : public DBTestBase {
 public:
  DedupContextDBIntegrationTest()
      : DBTestBase("dedup_context_db_test", /*env_do_fsync=*/true) {}
};

TEST_F(DedupContextDBIntegrationTest, ContextAbsentWhenDisabled) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  ASSERT_OK(TryReopen(options));
  DBImpl* impl = static_cast<DBImpl*>(db_.get());
  // default CF id is 0.
  EXPECT_EQ(impl->GetDedupContext(/*cf_id=*/0), nullptr);
}

TEST_F(DedupContextDBIntegrationTest, ContextPresentWhenEnabledOnDefaultCF) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.chunk_threshold_bytes = 128;  // non-default sanity check
  ASSERT_OK(TryReopen(options));
  DBImpl* impl = static_cast<DBImpl*>(db_.get());
  auto ctx = impl->GetDedupContext(/*cf_id=*/0);
  ASSERT_NE(ctx, nullptr);
  EXPECT_TRUE(ctx->options_snapshot.enable);
  EXPECT_EQ(ctx->options_snapshot.chunk_threshold_bytes, 128u);
  EXPECT_NE(ctx->cit, nullptr);
  EXPECT_NE(ctx->memory_monitor, nullptr);
  // Capacity must match write_buffer_size * max_write_buffer_number.
  const uint64_t expected_cap =
      static_cast<uint64_t>(options.write_buffer_size) *
      static_cast<uint64_t>(options.max_write_buffer_number);
  EXPECT_EQ(ctx->memory_monitor->CapacityBytes(), expected_cap);
}

TEST_F(DedupContextDBIntegrationTest, ContextPresentOnlyForEnabledCF) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // Default CF: dedup OFF.
  // Additional CF: dedup ON.
  ASSERT_OK(TryReopen(options));

  // Create extra CF with dedup enabled.
  ColumnFamilyOptions cfo_on;
  cfo_on.dedupkv.enable = true;
  ColumnFamilyHandle* h_on = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(cfo_on, "dedup_cf", &h_on));
  const uint32_t on_id = h_on->GetID();
  ASSERT_OK(db_->DestroyColumnFamilyHandle(h_on));

  // Reopen so Open-time wiring runs for both CFs.
  std::vector<ColumnFamilyDescriptor> cfds;
  cfds.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions(options));
  cfds.emplace_back("dedup_cf", cfo_on);
  std::vector<ColumnFamilyHandle*> handles;
  Close();
  ASSERT_OK(DB::Open(options, dbname_, cfds, &handles, &db_));

  DBImpl* impl = static_cast<DBImpl*>(db_.get());
  EXPECT_EQ(impl->GetDedupContext(/*default*/ 0), nullptr);
  auto ctx = impl->GetDedupContext(on_id);
  ASSERT_NE(ctx, nullptr);
  EXPECT_TRUE(ctx->options_snapshot.enable);

  for (auto* h : handles) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
