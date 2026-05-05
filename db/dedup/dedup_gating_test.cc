//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-13 gating tests: DedupKV is incompatible with Merge and with
// BlobDB's native blob GC. When dedup is enabled these paths are
// refused / suppressed.

#include <string>

#include "db/db_test_util.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class DedupGatingTest : public DBTestBase {
 public:
  DedupGatingTest() : DBTestBase("dedup_gating_test", /*env_do_fsync=*/true) {}
};

TEST_F(DedupGatingTest, MergeRejectedWhenDedupEnabled) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  // Don't set merge_operator — we're testing that Merge() rejects at
  // the runtime API layer on a dedup CF. (A DB opened with both set
  // is refused earlier by ITEM-13's open-time validator; see the
  // next test.)
  ASSERT_OK(TryReopen(options));

  const Status s = db_->Merge(WriteOptions(), "k", "partial");
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  EXPECT_NE(s.ToString().find("DedupKV"), std::string::npos);
}

TEST_F(DedupGatingTest, OpenRefusesDedupPlusMergeOperator) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.merge_operator = MergeOperators::CreatePutOperator();

  Status s = TryReopen(options);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(s.ToString().find("DedupKV"), std::string::npos);

  // Clean state so the harness's tear-down doesn't find a half-open DB.
  Close();
  options.merge_operator = nullptr;
  options.dedupkv.enable = false;
  ASSERT_OK(TryReopen(options));
}

TEST_F(DedupGatingTest, SanitizeForcesBlobGcOffUnderDedup) {
  // User requests both blob GC and dedup; sanitize must silently
  // drop blob GC so the two schedulers don't race.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.enable_blob_files = true;
  options.enable_blob_garbage_collection = true;

  ASSERT_OK(TryReopen(options));

  // Read back the effective option off the CF; sanitization copies
  // through to the MutableCFOptions held by cfd.
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  ColumnFamilyDescriptor desc;
  ASSERT_OK(cfh->GetDescriptor(&desc));
  EXPECT_FALSE(desc.options.enable_blob_garbage_collection);
}

TEST_F(DedupGatingTest, MergeAllowedWhenDedupDisabled) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = false;  // explicit
  options.merge_operator = MergeOperators::CreatePutOperator();
  ASSERT_OK(TryReopen(options));

  ASSERT_OK(db_->Put(WriteOptions(), "k", "base"));
  ASSERT_OK(db_->Merge(WriteOptions(), "k", "override"));
  std::string v;
  ASSERT_OK(db_->Get(ReadOptions(), "k", &v));
  EXPECT_EQ(v, "override");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
