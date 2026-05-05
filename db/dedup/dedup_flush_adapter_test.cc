//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dedup_flush_adapter.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/dedup/cit.h"
#include "db/dedup/uvl_file_builder.h"
#include "db/dedup/uvl_file_reader.h"
#include "db/dedup/uvl_log_format.h"
#include "env/mock_env.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DedupFlushAdapterTest : public testing::Test {
 protected:
  DedupFlushAdapterTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    clock_ = mock_env_->GetSystemClock().get();
  }

  // Build an opened UvlFileBuilder + return its on-disk path.
  std::unique_ptr<UvlFileBuilder> NewBuilder(const char* subname,
                                             uint64_t file_number,
                                             std::string* out_path) {
    *out_path = test::PerThreadDBPath(
                    mock_env_.get(),
                    std::string("DedupFlushAdapterTest_") + subname) +
                "_" + subname + ".uvl";
    std::unique_ptr<FSWritableFile> file;
    FileOptions fo;
    EXPECT_OK(NewWritableFile(fs_, *out_path, &file, fo));
    std::unique_ptr<WritableFileWriter> writer(
        new WritableFileWriter(std::move(file), *out_path, fo, clock_));
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

TEST_F(DedupFlushAdapterTest, EndToEndLargeValueRoundTrip) {
  // Wire DGD + UvlFileBuilder + BlobIndex encode → decode → fetch.
  std::string path;
  auto uvl = NewBuilder("LargeValue", /*file_number=*/42, &path);
  CIT cit;
  DGDStats stats;
  DedupFlushAdapter adapter(&cit, std::move(uvl), /*chunk_threshold=*/8,
                            &stats);

  const std::string key = "user_42";
  const std::string value = "this-is-a-deduplicated-value-payload";

  std::string blob_index;
  ASSERT_OK(adapter.Add(key, value, &blob_index));
  ASSERT_OK(adapter.Finish(/*sync=*/false));

  // Decode the BlobIndex — must be the kDedupKVUvl subtype.
  BlobIndex idx;
  ASSERT_OK(idx.DecodeFrom(Slice(blob_index)));
  EXPECT_TRUE(idx.IsDedupKVUvl());
  EXPECT_FALSE(idx.IsBlob());
  EXPECT_EQ(idx.file_number(), 42u);
  EXPECT_EQ(idx.compression(), kNoCompression);  // raw branch

  // Fetch the value bytes back through UvlFileReader.
  auto reader = OpenReader(path);
  ASSERT_NE(reader, nullptr);
  PinnableSlice got;
  UvlCompression got_compression = UvlCompression::kRaw;
  ASSERT_OK(reader->GetValue(idx.offset(), idx.size(), &got, &got_compression));
  EXPECT_EQ(got_compression, UvlCompression::kRaw);
  EXPECT_EQ(got.ToString(), value);

  EXPECT_EQ(adapter.uvl_record_count(), 1u);
}

TEST_F(DedupFlushAdapterTest, DuplicateValueProducesDedupHit) {
  // Two distinct keys with identical values should produce two
  // BlobIndexes pointing at the SAME uvl_file/offset and only one UVL
  // record.
  std::string path;
  auto uvl = NewBuilder("Duplicates", 1, &path);
  CIT cit;
  DGDStats stats;
  DedupFlushAdapter adapter(&cit, std::move(uvl), 8, &stats);

  std::string idx1, idx2;
  ASSERT_OK(adapter.Add("k1", "shared-content-payload", &idx1));
  ASSERT_OK(adapter.Add("k2", "shared-content-payload", &idx2));
  ASSERT_OK(adapter.Finish(/*sync=*/false));

  BlobIndex bi1, bi2;
  ASSERT_OK(bi1.DecodeFrom(Slice(idx1)));
  ASSERT_OK(bi2.DecodeFrom(Slice(idx2)));
  EXPECT_TRUE(bi1.IsDedupKVUvl());
  EXPECT_TRUE(bi2.IsDedupKVUvl());
  EXPECT_EQ(bi1.offset(), bi2.offset());
  EXPECT_EQ(bi1.size(), bi2.size());
  EXPECT_EQ(adapter.uvl_record_count(), 1u);  // single UVL record
  EXPECT_EQ(stats.dedup_hits.load(), 1u);
  EXPECT_EQ(stats.dedup_misses.load(), 1u);
}

TEST_F(DedupFlushAdapterTest, SmallValueUsesLz4Branch) {
  std::string path;
  auto uvl = NewBuilder("Small", 1, &path);
  CIT cit;
  DGDStats stats;
  // chunk_threshold = 64 → values < 64 bytes go LZ4-inline.
  DedupFlushAdapter adapter(&cit, std::move(uvl), 64, &stats);

  // Repetitive small value so LZ4 actually shortens it.
  const std::string small(32, 'x');
  std::string idx;
  ASSERT_OK(adapter.Add("tiny", small, &idx));
  ASSERT_OK(adapter.Finish(/*sync=*/false));

  BlobIndex bi;
  ASSERT_OK(bi.DecodeFrom(Slice(idx)));
  EXPECT_TRUE(bi.IsDedupKVUvl());
  EXPECT_EQ(bi.compression(), kLZ4Compression);

  // The reader returns the LZ4-compressed bytes (per DEC-004 — caller
  // decompresses). Verify the round-trip integrity by reading back
  // and comparing to the original LZ4 output produced by DGD.
  auto reader = OpenReader(path);
  ASSERT_NE(reader, nullptr);
  PinnableSlice got;
  UvlCompression compression = UvlCompression::kRaw;
  ASSERT_OK(reader->GetValue(bi.offset(), bi.size(), &got, &compression));
  EXPECT_EQ(compression, UvlCompression::kLz4Inline);
  EXPECT_LT(got.size(), small.size());  // LZ4 actually shortened it
  EXPECT_EQ(stats.small_value_lz4.load(), 1u);
}

TEST_F(DedupFlushAdapterTest, NullBlobIndexOutRejected) {
  std::string path;
  auto uvl = NewBuilder("Null", 1, &path);
  CIT cit;
  DGDStats stats;
  DedupFlushAdapter adapter(&cit, std::move(uvl), 8, &stats);
  EXPECT_TRUE(adapter.Add("k", "value-bytes-here", /*out=*/nullptr)
                  .IsInvalidArgument());
}

TEST_F(DedupFlushAdapterTest, EncodeUvlBlobIndexHelperMatchesAdapter) {
  // ITEM-18c: EncodeUvlBlobIndex now emits the V2 BlobIndex with
  // fingerprint embedded. The helper is shared by the inline flush
  // adapter and the offline install sink; both must produce the same
  // V2 bytes given identical DGDResult.
  DGDResult r;
  r.uvl_file = 9;
  r.offset = 1234;
  r.size = 567;
  r.compression = UvlCompression::kRaw;
  r.was_hit = false;
  for (size_t i = 0; i < r.fingerprint.size(); ++i) {
    r.fingerprint[i] = static_cast<uint8_t>(i + 1);
  }

  std::string a, b;
  EncodeUvlBlobIndex(r, &a);
  BlobIndex::EncodeDedupKVUvlV2(&b, r.fingerprint, r.uvl_file, r.offset,
                                r.size, kNoCompression);
  EXPECT_EQ(a, b);

  // V1 output must NOT match — V2 includes the 20-byte fp header.
  std::string v1;
  BlobIndex::EncodeDedupKVUvl(&v1, r.uvl_file, r.offset, r.size,
                              kNoCompression);
  EXPECT_NE(a, v1);
  EXPECT_EQ(a.size(), v1.size() + kUvlFingerprintSize);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
