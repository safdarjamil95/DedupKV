//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/uvl_file_reader.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/dedup/uvl_file_builder.h"
#include "db/dedup/uvl_log_format.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
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

struct Built {
  uint64_t offset = 0;
  uint64_t size = 0;
  UvlFingerprint fp{};
  UvlCompression compression = UvlCompression::kRaw;
  std::string key;
  std::string value;
};

}  // namespace

class UvlFileReaderTest : public testing::Test {
 protected:
  UvlFileReaderTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    clock_ = mock_env_->GetSystemClock().get();
  }

  // Builds a UVL file containing the given records, returns file path
  // and fills each record's file offset/size.
  std::string BuildFile(const char* subname, std::vector<Built>* records,
                        uint32_t cf_id = 0, uint64_t creation_time = 0) {
    std::string path =
        test::PerThreadDBPath(mock_env_.get(),
                              std::string("UvlFileReaderTest_") + subname) +
        "_" + subname + ".uvl";

    std::unique_ptr<FSWritableFile> file;
    FileOptions fo;
    EXPECT_OK(NewWritableFile(fs_, path, &file, fo));
    std::unique_ptr<WritableFileWriter> writer(
        new WritableFileWriter(std::move(file), path, fo, clock_));

    UvlFileBuilder builder(std::move(writer), /*file_number=*/1, cf_id,
                           creation_time);
    EXPECT_OK(builder.Open());
    for (auto& rec : *records) {
      EXPECT_OK(builder.Add(rec.fp, rec.compression, rec.key, rec.value,
                            &rec.offset, &rec.size));
    }
    EXPECT_OK(builder.Finish(/*sync=*/false));
    return path;
  }

  std::unique_ptr<UvlFileReader> OpenReader(const std::string& path) {
    uint64_t file_size = 0;
    EXPECT_OK(fs_->GetFileSize(path, IOOptions(), &file_size, nullptr));
    std::unique_ptr<FSRandomAccessFile> raf;
    EXPECT_OK(fs_->NewRandomAccessFile(path, FileOptions(), &raf, nullptr));
    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(raf), path, clock_));
    std::unique_ptr<UvlFileReader> reader;
    EXPECT_OK(
        UvlFileReader::Open(std::move(file_reader), file_size, &reader));
    return reader;
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_ = nullptr;
  SystemClock* clock_ = nullptr;
};

TEST_F(UvlFileReaderTest, OpenValidatesHeader) {
  std::vector<Built> records = {
      {0, 0, MakeFingerprint(0x11), UvlCompression::kRaw, "k", "v"}};
  std::string path = BuildFile("OpenValidatesHeader", &records,
                               /*cf_id=*/17, /*creation_time=*/424242);

  auto reader = OpenReader(path);
  ASSERT_NE(reader, nullptr);
  EXPECT_EQ(reader->header().version, kUvlVersion1);
  EXPECT_EQ(reader->header().column_family_id, 17U);
  EXPECT_EQ(reader->header().creation_time, 424242U);
  EXPECT_GT(reader->file_size(), UvlHeader::kSize);
}

TEST_F(UvlFileReaderTest, GetValueRoundTrip) {
  std::vector<Built> records = {
      {0, 0, MakeFingerprint(0x10), UvlCompression::kRaw, "alpha", "value-a"},
      {0, 0, MakeFingerprint(0x20), UvlCompression::kLz4Inline, "b",
       std::string("\xde\xad\xbe\xef", 4)},
      {0, 0, MakeFingerprint(0x30), UvlCompression::kRaw, "gkey",
       std::string(1024, 'g')},
  };
  std::string path = BuildFile("GetValueRoundTrip", &records);
  auto reader = OpenReader(path);
  ASSERT_NE(reader, nullptr);

  for (const auto& rec : records) {
    PinnableSlice value;
    UvlCompression compression = UvlCompression::kRaw;
    ASSERT_OK(
        reader->GetValue(rec.offset, rec.size, &value, &compression));
    EXPECT_EQ(compression, rec.compression);
    EXPECT_EQ(value.ToString(), rec.value);
  }
}

TEST_F(UvlFileReaderTest, GetFingerprintRoundTrip) {
  std::vector<Built> records = {
      {0, 0, MakeFingerprint(0x11), UvlCompression::kRaw, "a", "aa"},
      {0, 0, MakeFingerprint(0x22), UvlCompression::kRaw, "b", "bbb"},
  };
  std::string path = BuildFile("GetFingerprintRoundTrip", &records);
  auto reader = OpenReader(path);
  ASSERT_NE(reader, nullptr);

  for (const auto& rec : records) {
    UvlFingerprint got{};
    ASSERT_OK(reader->GetFingerprint(rec.offset, &got));
    EXPECT_EQ(got, rec.fp);
  }
}

TEST_F(UvlFileReaderTest, GetValueRejectsOutOfRange) {
  std::vector<Built> records = {
      {0, 0, MakeFingerprint(0x11), UvlCompression::kRaw, "k", "v"}};
  std::string path = BuildFile("OutOfRange", &records);
  auto reader = OpenReader(path);
  ASSERT_NE(reader, nullptr);

  PinnableSlice value;
  UvlCompression compression = UvlCompression::kRaw;

  // Offset inside the header region.
  EXPECT_TRUE(reader->GetValue(0, 10, &value, &compression).IsInvalidArgument());

  // Size zero.
  EXPECT_TRUE(reader
                  ->GetValue(records[0].offset, 0, &value, &compression)
                  .IsInvalidArgument());

  // Range extends beyond EOF.
  EXPECT_TRUE(reader
                  ->GetValue(records[0].offset, reader->file_size() * 2,
                             &value, &compression)
                  .IsInvalidArgument());
}

TEST_F(UvlFileReaderTest, GetValueDetectsCorruption) {
  std::vector<Built> records = {
      {0, 0, MakeFingerprint(0x55), UvlCompression::kRaw, "k",
       std::string("payload")}};
  std::string path = BuildFile("Corruption", &records);

  // Corrupt the UVL file on the MockEnv FS: flip one byte inside the
  // record's value region. We use NewWritableFile to fully overwrite
  // since MockEnv lacks a direct "patch byte" API; easier is to open
  // for read, patch in-memory, and rewrite.
  uint64_t size = 0;
  ASSERT_OK(fs_->GetFileSize(path, IOOptions(), &size, nullptr));
  std::string bytes(size, '\0');
  {
    std::unique_ptr<FSRandomAccessFile> raf;
    ASSERT_OK(fs_->NewRandomAccessFile(path, FileOptions(), &raf, nullptr));
    Slice r;
    ASSERT_OK(raf->Read(0, static_cast<size_t>(size), IOOptions(), &r,
                        &bytes[0], nullptr));
    if (r.data() != bytes.data()) {
      bytes.assign(r.data(), r.size());
    }
  }
  // Flip a byte inside the value portion (anywhere after fp+compression).
  ASSERT_GT(bytes.size(), UvlHeader::kSize + kUvlFingerprintSize + 2);
  const size_t target = UvlHeader::kSize + kUvlFingerprintSize + 4;
  bytes[target] ^= 0xFF;
  {
    std::unique_ptr<FSWritableFile> w;
    ASSERT_OK(NewWritableFile(fs_, path, &w, FileOptions()));
    ASSERT_OK(w->Append(Slice(bytes), IOOptions(), nullptr));
    ASSERT_OK(w->Close(IOOptions(), nullptr));
  }

  auto reader = OpenReader(path);
  ASSERT_NE(reader, nullptr);
  PinnableSlice value;
  UvlCompression compression = UvlCompression::kRaw;
  EXPECT_TRUE(reader->GetValue(records[0].offset, records[0].size, &value,
                               &compression)
                  .IsCorruption());
}

TEST_F(UvlFileReaderTest, OpenRejectsShortFile) {
  std::string path =
      test::PerThreadDBPath(mock_env_.get(), "UvlFileReaderTest_Short") +
      "_Short.uvl";
  {
    std::unique_ptr<FSWritableFile> w;
    ASSERT_OK(NewWritableFile(fs_, path, &w, FileOptions()));
    std::string only_a_bit(UvlHeader::kSize - 5, '\0');
    ASSERT_OK(w->Append(Slice(only_a_bit), IOOptions(), nullptr));
    ASSERT_OK(w->Close(IOOptions(), nullptr));
  }
  uint64_t file_size = 0;
  ASSERT_OK(fs_->GetFileSize(path, IOOptions(), &file_size, nullptr));
  std::unique_ptr<FSRandomAccessFile> raf;
  ASSERT_OK(fs_->NewRandomAccessFile(path, FileOptions(), &raf, nullptr));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(raf), path, clock_));
  std::unique_ptr<UvlFileReader> reader;
  EXPECT_TRUE(UvlFileReader::Open(std::move(file_reader), file_size, &reader)
                  .IsCorruption());
}

TEST_F(UvlFileReaderTest, OpenRejectsBadMagic) {
  std::string path =
      test::PerThreadDBPath(mock_env_.get(), "UvlFileReaderTest_BadMagic") +
      "_BadMagic.uvl";
  {
    std::unique_ptr<FSWritableFile> w;
    ASSERT_OK(NewWritableFile(fs_, path, &w, FileOptions()));
    std::string garbage(UvlHeader::kSize, '\xff');
    ASSERT_OK(w->Append(Slice(garbage), IOOptions(), nullptr));
    ASSERT_OK(w->Close(IOOptions(), nullptr));
  }
  uint64_t file_size = 0;
  ASSERT_OK(fs_->GetFileSize(path, IOOptions(), &file_size, nullptr));
  std::unique_ptr<FSRandomAccessFile> raf;
  ASSERT_OK(fs_->NewRandomAccessFile(path, FileOptions(), &raf, nullptr));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(raf), path, clock_));
  std::unique_ptr<UvlFileReader> reader;
  EXPECT_TRUE(UvlFileReader::Open(std::move(file_reader), file_size, &reader)
                  .IsCorruption());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
