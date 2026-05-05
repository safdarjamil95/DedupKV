//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dgd.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/dedup/cit.h"
#include "db/dedup/uvl_file_builder.h"
#include "db/dedup/uvl_file_reader.h"
#include "db/dedup/uvl_log_format.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DGDTest : public testing::Test {
 protected:
  DGDTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    clock_ = mock_env_->GetSystemClock().get();
  }

  // Build a fresh UvlFileBuilder. Caller owns the returned builder.
  std::unique_ptr<UvlFileBuilder> MakeBuilder(const char* subname,
                                              uint64_t file_number = 1,
                                              std::string* out_path = nullptr) {
    std::string path =
        test::PerThreadDBPath(mock_env_.get(), std::string("DGDTest_") + subname) +
        "_" + subname + ".uvl";
    if (out_path) *out_path = path;

    std::unique_ptr<FSWritableFile> file;
    FileOptions fo;
    EXPECT_OK(NewWritableFile(fs_, path, &file, fo));
    std::unique_ptr<WritableFileWriter> writer(
        new WritableFileWriter(std::move(file), path, fo, clock_));
    auto b = std::make_unique<UvlFileBuilder>(std::move(writer), file_number,
                                              /*cf_id=*/0,
                                              /*creation_time=*/0);
    EXPECT_OK(b->Open());
    return b;
  }

  std::unique_ptr<UvlFileReader> OpenReader(const std::string& path) {
    uint64_t file_size = 0;
    EXPECT_OK(fs_->GetFileSize(path, IOOptions(), &file_size, nullptr));
    std::unique_ptr<FSRandomAccessFile> raf;
    EXPECT_OK(fs_->NewRandomAccessFile(path, FileOptions(), &raf, nullptr));
    std::unique_ptr<RandomAccessFileReader> fr(
        new RandomAccessFileReader(std::move(raf), path, clock_));
    std::unique_ptr<UvlFileReader> reader;
    EXPECT_OK(UvlFileReader::Open(std::move(fr), file_size, &reader));
    return reader;
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_ = nullptr;
  SystemClock* clock_ = nullptr;
};

TEST_F(DGDTest, LargeValueFirstWriteIsMiss) {
  std::string path;
  auto builder = MakeBuilder("FirstWrite", /*file_number=*/1, &path);
  CIT cit;
  DGDStats stats;
  DGDEncoder enc(&cit, builder.get(), /*chunk_threshold=*/8, &stats);

  DGDResult r;
  ASSERT_OK(enc.Process(Slice("k"), Slice("a-long-enough-value"), &r));
  EXPECT_FALSE(r.was_hit);
  EXPECT_EQ(r.compression, UvlCompression::kRaw);
  EXPECT_EQ(r.uvl_file, 1U);
  EXPECT_GT(r.size, 0U);
  EXPECT_EQ(stats.dedup_misses.load(), 1U);
  EXPECT_EQ(stats.dedup_hits.load(), 0U);
  EXPECT_EQ(cit.Size(), 1U);

  // CIT entry points at what we just wrote.
  std::string raw_value = "a-long-enough-value";
  EXPECT_OK(builder->Finish(/*sync=*/false));
  auto reader = OpenReader(path);
  PinnableSlice got;
  UvlCompression comp = UvlCompression::kRaw;
  ASSERT_OK(reader->GetValue(r.offset, r.size, &got, &comp));
  EXPECT_EQ(comp, UvlCompression::kRaw);
  EXPECT_EQ(got.ToString(), raw_value);
}

TEST_F(DGDTest, LargeValueDuplicateWriteIsHit) {
  auto builder = MakeBuilder("Duplicate");
  CIT cit;
  DGDStats stats;
  DGDEncoder enc(&cit, builder.get(), /*chunk_threshold=*/8, &stats);

  DGDResult r1, r2;
  ASSERT_OK(enc.Process(Slice("k1"), Slice("duplicate-value"), &r1));
  ASSERT_OK(enc.Process(Slice("k2"), Slice("duplicate-value"), &r2));
  EXPECT_FALSE(r1.was_hit);
  EXPECT_TRUE(r2.was_hit);
  EXPECT_EQ(r1.offset, r2.offset);
  EXPECT_EQ(r1.size, r2.size);
  EXPECT_EQ(r1.uvl_file, r2.uvl_file);
  EXPECT_EQ(stats.dedup_misses.load(), 1U);
  EXPECT_EQ(stats.dedup_hits.load(), 1U);
  EXPECT_EQ(cit.Size(), 1U);

  // Refcount incremented on hit.
  std::vector<std::pair<UvlFingerprint, CITEntry>> snap;
  cit.Snapshot(&snap);
  ASSERT_EQ(snap.size(), 1U);
  EXPECT_EQ(snap[0].second.refcount, 2U);
}

TEST_F(DGDTest, DistinctLargeValuesYieldDistinctRecords) {
  auto builder = MakeBuilder("Distinct");
  CIT cit;
  DGDStats stats;
  DGDEncoder enc(&cit, builder.get(), /*chunk_threshold=*/8, &stats);

  DGDResult a, b;
  ASSERT_OK(enc.Process(Slice("ka"), Slice("value-alpha-long"), &a));
  ASSERT_OK(enc.Process(Slice("kb"), Slice("value-beta--long"), &b));
  EXPECT_FALSE(a.was_hit);
  EXPECT_FALSE(b.was_hit);
  EXPECT_NE(a.offset, b.offset);
  EXPECT_EQ(cit.Size(), 2U);
  EXPECT_EQ(stats.dedup_misses.load(), 2U);
}

TEST_F(DGDTest, SmallValueUsesLz4BranchAndSkipsCIT) {
  std::string path;
  auto builder = MakeBuilder("SmallLz4", 1, &path);
  CIT cit;
  DGDStats stats;
  // Threshold 64 — anything below goes the LZ4-inline path per
  // AMBIGUITY-001.
  DGDEncoder enc(&cit, builder.get(), /*chunk_threshold=*/64, &stats);

  // Use a repetitive small value so LZ4 actually compresses it.
  const std::string small(32, 'x');
  DGDResult r;
  ASSERT_OK(enc.Process(Slice("tiny"), Slice(small), &r));
  EXPECT_FALSE(r.was_hit);
  EXPECT_EQ(r.compression, UvlCompression::kLz4Inline);
  EXPECT_EQ(cit.Size(), 0U);  // small-value branch never touches CIT
  EXPECT_EQ(stats.small_value_lz4.load(), 1U);
  EXPECT_EQ(stats.lz4_input_bytes.load(), small.size());
  EXPECT_LT(stats.lz4_compressed_bytes.load(), small.size());

  // Duplicate small-values also skip dedup (new record each time).
  DGDResult r2;
  ASSERT_OK(enc.Process(Slice("tiny2"), Slice(small), &r2));
  EXPECT_NE(r.offset, r2.offset);
  EXPECT_EQ(cit.Size(), 0U);
  EXPECT_EQ(stats.small_value_lz4.load(), 2U);
}

TEST_F(DGDTest, BoundaryAtChunkThreshold) {
  auto builder = MakeBuilder("Boundary");
  CIT cit;
  DGDStats stats;
  constexpr uint32_t kThreshold = 16;
  DGDEncoder enc(&cit, builder.get(), kThreshold, &stats);

  // Exactly at the threshold → large branch.
  std::string at_threshold(kThreshold, 'a');
  DGDResult r;
  ASSERT_OK(enc.Process(Slice("k"), Slice(at_threshold), &r));
  EXPECT_EQ(r.compression, UvlCompression::kRaw);
  EXPECT_EQ(cit.Size(), 1U);

  // Just below → small branch.
  std::string below(kThreshold - 1, 'b');
  DGDResult r2;
  ASSERT_OK(enc.Process(Slice("k2"), Slice(below), &r2));
  EXPECT_EQ(r2.compression, UvlCompression::kLz4Inline);
  EXPECT_EQ(cit.Size(), 1U);  // unchanged
}

TEST_F(DGDTest, DuplicateDetectionIsContentBased) {
  // Same value, different keys — should dedup. Different values,
  // same key — should not dedup.
  auto builder = MakeBuilder("ContentBased");
  CIT cit;
  DGDStats stats;
  DGDEncoder enc(&cit, builder.get(), /*chunk_threshold=*/8, &stats);

  DGDResult r1, r2, r3;
  ASSERT_OK(enc.Process(Slice("K"), Slice("content-XYZ"), &r1));
  ASSERT_OK(enc.Process(Slice("K"), Slice("content-ABC"), &r2));
  ASSERT_OK(enc.Process(Slice("other-key"), Slice("content-XYZ"), &r3));

  EXPECT_FALSE(r1.was_hit);
  EXPECT_FALSE(r2.was_hit);  // different value → new record
  EXPECT_TRUE(r3.was_hit);   // same value → dedup hit
  EXPECT_EQ(r1.offset, r3.offset);
  EXPECT_EQ(cit.Size(), 2U);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
