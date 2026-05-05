//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/offline_dedup.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/dedup/cit.h"
#include "db/dedup/dgd.h"
#include "db/dedup/uvl_file_builder.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct EmittedValue {
  std::string key;
  SequenceNumber seq;
  DGDResult result;
};

struct EmittedDelete {
  std::string key;
  SequenceNumber seq;
  ValueType type;
};

class RecordingSink : public OfflineDedupSink {
 public:
  Status EmitValue(const Slice& key, SequenceNumber seq,
                   const DGDResult& result) override {
    values.push_back({key.ToString(), seq, result});
    return Status::OK();
  }
  Status EmitDelete(const Slice& key, SequenceNumber seq,
                    ValueType type) override {
    deletes.push_back({key.ToString(), seq, type});
    return Status::OK();
  }

  std::vector<EmittedValue> values;
  std::vector<EmittedDelete> deletes;
};

OfflineWalRecord Put(SequenceNumber seq, std::string key, std::string value) {
  OfflineWalRecord r;
  r.seq = seq;
  r.type = kTypeValue;
  r.key = std::move(key);
  r.value = std::move(value);
  return r;
}

OfflineWalRecord Del(SequenceNumber seq, std::string key,
                     ValueType type = kTypeDeletion) {
  OfflineWalRecord r;
  r.seq = seq;
  r.type = type;
  r.key = std::move(key);
  return r;
}

}  // namespace

class OfflineDedupTest : public testing::Test {
 protected:
  OfflineDedupTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    clock_ = mock_env_->GetSystemClock().get();
  }

  std::unique_ptr<UvlFileBuilder> MakeBuilder(const char* subname) {
    std::string path =
        test::PerThreadDBPath(mock_env_.get(),
                              std::string("OfflineDedupTest_") + subname) +
        "_" + subname + ".uvl";
    std::unique_ptr<FSWritableFile> file;
    FileOptions fo;
    EXPECT_OK(NewWritableFile(fs_, path, &file, fo));
    std::unique_ptr<WritableFileWriter> writer(
        new WritableFileWriter(std::move(file), path, fo, clock_));
    auto b = std::make_unique<UvlFileBuilder>(std::move(writer), 1, 0, 0);
    EXPECT_OK(b->Open());
    return b;
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_ = nullptr;
  SystemClock* clock_ = nullptr;
};

TEST_F(OfflineDedupTest, TailWinsAmongDuplicateKeys) {
  // WAL has PUT key=k value=v1 at seq 10, then PUT key=k value=v2 at
  // seq 20. Offline dedup must emit only v2 (at seq 20).
  auto builder = MakeBuilder("TailWins");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), /*chunk_threshold=*/8, &dgd_stats);
  RecordingSink sink;
  OfflineDedupStats stats;

  std::vector<OfflineWalRecord> wal = {
      Put(10, "k", "value-one-long"),
      Put(20, "k", "value-two-long"),
  };
  ASSERT_OK(OfflineDedupDrain(wal, &enc, &sink, &stats));

  ASSERT_EQ(sink.values.size(), 1U);
  EXPECT_EQ(sink.values[0].key, "k");
  EXPECT_EQ(sink.values[0].seq, 20U);  // tail-most wins
  EXPECT_EQ(stats.records_seen, 2U);
  EXPECT_EQ(stats.duplicate_keys_dropped, 1U);
  EXPECT_EQ(stats.values_emitted, 1U);
}

TEST_F(OfflineDedupTest, EmitsInTailToHeadOrder) {
  // 3 distinct keys PUT in WAL order k1,k2,k3 — emit order is k3,k2,k1
  // because we iterate tail→head.
  auto builder = MakeBuilder("Order");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;

  std::vector<OfflineWalRecord> wal = {
      Put(1, "k1", "value-one-alpha"),
      Put(2, "k2", "value-two-betas"),
      Put(3, "k3", "value-three-gga"),
  };
  ASSERT_OK(OfflineDedupDrain(wal, &enc, &sink, nullptr));
  ASSERT_EQ(sink.values.size(), 3U);
  EXPECT_EQ(sink.values[0].key, "k3");
  EXPECT_EQ(sink.values[1].key, "k2");
  EXPECT_EQ(sink.values[2].key, "k1");
}

TEST_F(OfflineDedupTest, DeleteAtTailSuppressesOlderPut) {
  // PUT then DELETE of same key — only the delete must be emitted
  // (DGD not invoked, no UVL record).
  auto builder = MakeBuilder("DeleteTail");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;
  OfflineDedupStats stats;

  std::vector<OfflineWalRecord> wal = {
      Put(10, "k", "value-to-be-deleted"),
      Del(20, "k"),
  };
  ASSERT_OK(OfflineDedupDrain(wal, &enc, &sink, &stats));

  EXPECT_EQ(sink.values.size(), 0U);
  ASSERT_EQ(sink.deletes.size(), 1U);
  EXPECT_EQ(sink.deletes[0].key, "k");
  EXPECT_EQ(sink.deletes[0].seq, 20U);
  EXPECT_EQ(sink.deletes[0].type, kTypeDeletion);
  EXPECT_EQ(cit.Size(), 0U);  // DGD never touched
  EXPECT_EQ(stats.deletes_emitted, 1U);
  EXPECT_EQ(stats.values_emitted, 0U);
  EXPECT_EQ(stats.duplicate_keys_dropped, 1U);
}

TEST_F(OfflineDedupTest, PutAfterDeleteOverridesDelete) {
  // DEL then PUT at later seq — PUT wins (tail-most).
  auto builder = MakeBuilder("PutAfterDelete");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;

  std::vector<OfflineWalRecord> wal = {
      Del(10, "k"),
      Put(20, "k", "resurrected-value"),
  };
  ASSERT_OK(OfflineDedupDrain(wal, &enc, &sink, nullptr));
  ASSERT_EQ(sink.values.size(), 1U);
  ASSERT_EQ(sink.deletes.size(), 0U);
  EXPECT_EQ(sink.values[0].seq, 20U);
}

TEST_F(OfflineDedupTest, SingleDeletionPassedThrough) {
  auto builder = MakeBuilder("SingleDel");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;

  std::vector<OfflineWalRecord> wal = {Del(10, "k", kTypeSingleDeletion)};
  ASSERT_OK(OfflineDedupDrain(wal, &enc, &sink, nullptr));
  ASSERT_EQ(sink.deletes.size(), 1U);
  EXPECT_EQ(sink.deletes[0].type, kTypeSingleDeletion);
}

TEST_F(OfflineDedupTest, UnsupportedTypesCountedButSkipped) {
  auto builder = MakeBuilder("Unsupported");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;
  OfflineDedupStats stats;

  std::vector<OfflineWalRecord> wal = {
      Put(1, "kv", "normal-value-abc"),
  };
  // Add a merge record (unsupported under dedup per AMBIGUITY-007).
  OfflineWalRecord m;
  m.seq = 2;
  m.type = kTypeMerge;
  m.key = "m";
  m.value = "ignored";
  wal.push_back(m);

  ASSERT_OK(OfflineDedupDrain(wal, &enc, &sink, &stats));
  EXPECT_EQ(sink.values.size(), 1U);  // only the normal PUT
  EXPECT_EQ(stats.unsupported_types_skipped, 1U);
}

TEST_F(OfflineDedupTest, DgdHitsAreSurfacedViaResult) {
  // Duplicate values across two WAL entries (same content, different
  // keys) should yield a DGD hit on the second — was_hit=true and
  // identical uvl_file/offset.
  auto builder = MakeBuilder("DgdHits");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;

  // In WAL order: k1 first, k2 second. Tail→head processing emits k2
  // first (miss on content), then k1 (hit on same content).
  std::vector<OfflineWalRecord> wal = {
      Put(1, "k1", "shared-content-xy"),
      Put(2, "k2", "shared-content-xy"),
  };
  ASSERT_OK(OfflineDedupDrain(wal, &enc, &sink, nullptr));
  ASSERT_EQ(sink.values.size(), 2U);
  EXPECT_FALSE(sink.values[0].result.was_hit);
  EXPECT_TRUE(sink.values[1].result.was_hit);
  EXPECT_EQ(sink.values[0].result.offset, sink.values[1].result.offset);
}

TEST_F(OfflineDedupTest, NullSinkOrEncoderRejected) {
  auto builder = MakeBuilder("Null");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;
  std::vector<OfflineWalRecord> wal = {Put(1, "k", "value-abc-xyz")};
  EXPECT_TRUE(OfflineDedupDrain(wal, nullptr, &sink, nullptr).IsInvalidArgument());
  EXPECT_TRUE(OfflineDedupDrain(wal, &enc, nullptr, nullptr).IsInvalidArgument());
}

TEST_F(OfflineDedupTest, EmptyWalIsNoOp) {
  auto builder = MakeBuilder("Empty");
  CIT cit;
  DGDStats dgd_stats;
  DGDEncoder enc(&cit, builder.get(), 8, &dgd_stats);
  RecordingSink sink;
  OfflineDedupStats stats;

  ASSERT_OK(OfflineDedupDrain({}, &enc, &sink, &stats));
  EXPECT_EQ(sink.values.size(), 0U);
  EXPECT_EQ(stats.records_seen, 0U);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
