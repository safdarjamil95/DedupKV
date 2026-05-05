//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_file_builder.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/dedup/uvl_log_format.h"
#include "env/mock_env.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

UvlFingerprint MakeFingerprint(uint8_t seed) {
  UvlFingerprint fp{};
  for (size_t i = 0; i < fp.size(); ++i) {
    fp[i] = static_cast<uint8_t>(seed + i);
  }
  return fp;
}

struct AddedRecord {
  UvlFingerprint fp;
  UvlCompression compression;
  std::string key;
  std::string value;
  uint64_t offset = 0;
  uint64_t size = 0;
};

}  // namespace

class UvlFileBuilderTest : public testing::Test {
 protected:
  UvlFileBuilderTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    clock_ = mock_env_->GetSystemClock().get();
  }

  // Opens a fresh WritableFileWriter at `path`; returns nullptr on
  // failure (caller uses ASSERT_NE).
  std::unique_ptr<WritableFileWriter> NewWriter(const std::string& path) {
    std::unique_ptr<FSWritableFile> file;
    FileOptions fo;
    IOStatus s = NewWritableFile(fs_, path, &file, fo);
    if (!s.ok()) {
      return nullptr;
    }
    return std::unique_ptr<WritableFileWriter>(new WritableFileWriter(
        std::move(file), path, fo, clock_));
  }

  // Reads the whole file contents into `out`. Uses the filesystem API
  // directly so the test exercises the on-disk bytes the builder
  // produced (independent of the future UvlFileReader).
  void ReadWholeFile(const std::string& path, std::string* out) {
    uint64_t size = 0;
    ASSERT_OK(fs_->GetFileSize(path, IOOptions(), &size, nullptr));
    std::unique_ptr<FSRandomAccessFile> file;
    ASSERT_OK(fs_->NewRandomAccessFile(path, FileOptions(), &file, nullptr));
    out->assign(size, '\0');
    Slice result;
    ASSERT_OK(file->Read(0, static_cast<size_t>(size), IOOptions(), &result,
                         &(*out)[0], nullptr));
    // Normalize in case the filesystem returned a pointer into its own
    // buffer rather than into ours.
    if (result.data() != out->data()) {
      out->assign(result.data(), result.size());
    } else {
      out->resize(result.size());
    }
  }

  std::string TestPath(const char* subname) {
    return test::PerThreadDBPath(mock_env_.get(),
                                 std::string("UvlFileBuilderTest_") + subname) +
           "_" + subname + ".uvl";
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_ = nullptr;
  SystemClock* clock_ = nullptr;
};

TEST_F(UvlFileBuilderTest, HeaderThenRecordsRoundTrip) {
  const std::string path = TestPath("RoundTrip");

  constexpr uint64_t kFileNumber = 42;
  constexpr uint32_t kCfId = 7;
  constexpr uint64_t kCreationTime = 1700001234ULL;

  auto writer = NewWriter(path);
  ASSERT_NE(writer, nullptr);
  UvlFileBuilder builder(std::move(writer), kFileNumber, kCfId, kCreationTime);
  ASSERT_OK(builder.Open());
  EXPECT_TRUE(builder.is_open());
  EXPECT_EQ(builder.total_bytes(), UvlHeader::kSize);

  std::vector<AddedRecord> records = {
      {MakeFingerprint(0x10), UvlCompression::kRaw, "alpha", "value-a"},
      {MakeFingerprint(0x20), UvlCompression::kLz4Inline, "b",
       std::string("\x01\x02\x03", 3)},
      {MakeFingerprint(0x30), UvlCompression::kRaw, "gamma-key",
       std::string(512, 'g')},
  };
  for (auto& rec : records) {
    ASSERT_OK(builder.Add(rec.fp, rec.compression, rec.key, rec.value,
                          &rec.offset, &rec.size));
  }
  const uint64_t pre_finish_total = builder.total_bytes();
  EXPECT_EQ(builder.record_count(), records.size());
  ASSERT_OK(builder.Finish(/*sync=*/true));
  EXPECT_TRUE(builder.is_finished());
  EXPECT_FALSE(builder.is_open());

  // Read back the file and verify header + records byte-for-byte.
  std::string bytes;
  ReadWholeFile(path, &bytes);
  ASSERT_EQ(bytes.size(), pre_finish_total);

  UvlHeader header;
  ASSERT_OK(header.DecodeFrom(Slice(bytes.data(), UvlHeader::kSize)));
  EXPECT_EQ(header.version, kUvlVersion1);
  EXPECT_EQ(header.column_family_id, kCfId);
  EXPECT_EQ(header.creation_time, kCreationTime);

  Slice cursor(bytes.data() + UvlHeader::kSize,
               bytes.size() - UvlHeader::kSize);
  for (const auto& expected : records) {
    UvlRecord decoded;
    const size_t before = cursor.size();
    ASSERT_OK(DecodeUvlRecord(&cursor, &decoded));
    const size_t consumed = before - cursor.size();
    EXPECT_EQ(consumed, expected.size);
    // The builder-reported offset locates this record within the full
    // file. Bytes consumed prior to this record == file position of the
    // record's first byte.
    EXPECT_EQ(bytes.size() - before, expected.offset);
    EXPECT_EQ(decoded.fingerprint, expected.fp);
    EXPECT_EQ(decoded.compression, expected.compression);
    EXPECT_EQ(decoded.key.ToString(), expected.key);
    EXPECT_EQ(decoded.value.ToString(), expected.value);
  }
  EXPECT_EQ(cursor.size(), 0U);
}

TEST_F(UvlFileBuilderTest, OffsetsAreContiguous) {
  const std::string path = TestPath("Offsets");
  auto writer = NewWriter(path);
  ASSERT_NE(writer, nullptr);
  UvlFileBuilder builder(std::move(writer), /*file_number=*/1,
                         /*column_family_id=*/0, /*creation_time=*/0);
  ASSERT_OK(builder.Open());

  uint64_t prev_end = UvlHeader::kSize;
  for (int i = 0; i < 5; ++i) {
    uint64_t off = 0, sz = 0;
    ASSERT_OK(builder.Add(MakeFingerprint(static_cast<uint8_t>(i)),
                          UvlCompression::kRaw, "k" + std::to_string(i),
                          "v" + std::to_string(i), &off, &sz));
    EXPECT_EQ(off, prev_end);
    EXPECT_GT(sz, 0U);
    prev_end = off + sz;
    EXPECT_EQ(builder.next_record_offset(), prev_end);
  }
  ASSERT_OK(builder.Finish(/*sync=*/false));
}

TEST_F(UvlFileBuilderTest, AddBeforeOpenFails) {
  const std::string path = TestPath("AddBeforeOpen");
  auto writer = NewWriter(path);
  ASSERT_NE(writer, nullptr);
  UvlFileBuilder builder(std::move(writer), 1, 0, 0);
  // No Open() yet.
  EXPECT_TRUE(
      builder.Add(MakeFingerprint(0), UvlCompression::kRaw, "k", "v")
          .IsInvalidArgument());
  // Clean up — builder destructor drops the writer without finalize.
}

TEST_F(UvlFileBuilderTest, DoubleOpenFails) {
  const std::string path = TestPath("DoubleOpen");
  auto writer = NewWriter(path);
  ASSERT_NE(writer, nullptr);
  UvlFileBuilder builder(std::move(writer), 1, 0, 0);
  ASSERT_OK(builder.Open());
  EXPECT_TRUE(builder.Open().IsInvalidArgument());
}

TEST_F(UvlFileBuilderTest, AddAfterFinishFails) {
  const std::string path = TestPath("AddAfterFinish");
  auto writer = NewWriter(path);
  ASSERT_NE(writer, nullptr);
  UvlFileBuilder builder(std::move(writer), 1, 0, 0);
  ASSERT_OK(builder.Open());
  ASSERT_OK(builder.Add(MakeFingerprint(0), UvlCompression::kRaw, "k", "v"));
  ASSERT_OK(builder.Finish(/*sync=*/false));
  EXPECT_TRUE(
      builder.Add(MakeFingerprint(1), UvlCompression::kRaw, "k", "v")
          .IsInvalidArgument());
}

TEST_F(UvlFileBuilderTest, AbandonReleasesWithoutSync) {
  const std::string path = TestPath("Abandon");
  auto writer = NewWriter(path);
  ASSERT_NE(writer, nullptr);
  UvlFileBuilder builder(std::move(writer), 1, 0, 0);
  ASSERT_OK(builder.Open());
  ASSERT_OK(builder.Add(MakeFingerprint(0), UvlCompression::kRaw, "k", "v"));
  builder.Abandon();
  EXPECT_TRUE(builder.is_finished());
  // Subsequent Finish() on an abandoned builder should error rather than
  // crash (writer is null).
  EXPECT_FALSE(builder.Finish(/*sync=*/false).ok());
}

TEST_F(UvlFileBuilderTest, FilenameHelpersAndParse) {
  EXPECT_EQ(UvlFileName(123), "000123.uvl");
  EXPECT_EQ(UvlFileName("dir", 7), "dir/000007.uvl");

  uint64_t number = 0;
  FileType type = kTableFile;
  ASSERT_TRUE(ParseFileName("000042.uvl", &number, &type));
  EXPECT_EQ(number, 42U);
  EXPECT_EQ(type, kUvlFile);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
