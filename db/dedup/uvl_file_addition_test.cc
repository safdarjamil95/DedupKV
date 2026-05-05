//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Tests for ITEM-14b: UvlFileAddition codec + VersionEdit
// integration (kUvlFileAddition tag).

#include "db/dedup/uvl_file_addition.h"

#include <string>

#include "db/blob/blob_file_addition.h"
#include "db/version_edit.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class UvlFileAdditionTest : public testing::Test {};

TEST_F(UvlFileAdditionTest, RoundTrip) {
  UvlFileAddition addition(/*file_number=*/123, /*cf_id=*/7,
                           /*records=*/450, /*bytes=*/1024 * 1024,
                           /*creation_time=*/1700001234ULL);
  std::string buf;
  addition.EncodeTo(&buf);
  ASSERT_FALSE(buf.empty());

  UvlFileAddition decoded;
  Slice input(buf);
  ASSERT_OK(decoded.DecodeFrom(&input));
  EXPECT_EQ(input.size(), 0u);
  EXPECT_EQ(decoded, addition);
}

TEST_F(UvlFileAdditionTest, ZeroValuesRoundTrip) {
  UvlFileAddition addition;  // all zero
  std::string buf;
  addition.EncodeTo(&buf);
  UvlFileAddition decoded;
  Slice input(buf);
  ASSERT_OK(decoded.DecodeFrom(&input));
  EXPECT_EQ(decoded, addition);
}

TEST_F(UvlFileAdditionTest, EqualityIgnoresOrderingOfFields) {
  UvlFileAddition a(1, 2, 3, 4, 5);
  UvlFileAddition b(1, 2, 3, 4, 5);
  EXPECT_EQ(a, b);
  UvlFileAddition c(1, 2, 3, 4, 6);
  EXPECT_NE(a, c);
}

TEST_F(UvlFileAdditionTest, CorruptedTagRejected) {
  // Manually craft a bytestream that ends with a forward-incompatible
  // custom-field tag and confirm DecodeFrom returns Corruption.
  std::string buf;
  PutVarint64(&buf, 1);   // file_number
  PutVarint32(&buf, 0);   // cf_id
  PutVarint64(&buf, 10);  // records
  PutVarint64(&buf, 100); // bytes
  PutVarint64(&buf, 0);   // creation_time
  // Forward-incompatible custom field tag: bit 6 (mask = 1<<6 = 64).
  PutVarint32(&buf, /*incompatible*/ 64);
  PutLengthPrefixedSlice(&buf, "x");

  UvlFileAddition decoded;
  Slice input(buf);
  EXPECT_TRUE(decoded.DecodeFrom(&input).IsCorruption());
}

TEST_F(UvlFileAdditionTest, ForwardCompatibleCustomFieldIgnored) {
  // A custom-field tag below the forward-incompatible mask should be
  // skipped silently.
  std::string buf;
  PutVarint64(&buf, 7);
  PutVarint32(&buf, 0);
  PutVarint64(&buf, 1);
  PutVarint64(&buf, 1);
  PutVarint64(&buf, 0);
  // Hypothetical future tag value (not yet defined, below the mask).
  PutVarint32(&buf, /*compatible_unknown=*/3);
  PutLengthPrefixedSlice(&buf, "future-data");
  // End marker.
  PutVarint32(&buf, /*kEndMarker=*/0);

  UvlFileAddition decoded;
  Slice input(buf);
  ASSERT_OK(decoded.DecodeFrom(&input));
  EXPECT_EQ(decoded.GetUvlFileNumber(), 7u);
}

TEST_F(UvlFileAdditionTest, VersionEditEncodeDecodeRoundTrip) {
  // Full VersionEdit roundtrip via the kUvlFileAddition tag.
  VersionEdit edit;
  edit.SetColumnFamily(3);
  edit.AddUvlFile(/*number=*/100, /*cf_id=*/3, /*records=*/77,
                  /*bytes=*/4096, /*creation_time=*/42);
  edit.AddUvlFile(/*number=*/101, /*cf_id=*/3, /*records=*/19,
                  /*bytes=*/512, /*creation_time=*/99);

  EXPECT_EQ(edit.GetUvlFileAdditions().size(), 2u);
  EXPECT_EQ(edit.NumEntries(), 2u);

  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded, /*ts_sz=*/0));

  VersionEdit decoded;
  ASSERT_OK(decoded.DecodeFrom(encoded));
  ASSERT_EQ(decoded.GetUvlFileAdditions().size(), 2u);
  EXPECT_EQ(decoded.GetUvlFileAdditions()[0],
            UvlFileAddition(100, 3, 77, 4096, 42));
  EXPECT_EQ(decoded.GetUvlFileAdditions()[1],
            UvlFileAddition(101, 3, 19, 512, 99));
}

TEST_F(UvlFileAdditionTest, CoexistsWithBlobFileAdditionInVersionEdit) {
  // BlobDB and DedupKV are not used together (ITEM-13 sanitises blob
  // GC off when dedup is on), but the MANIFEST format must still
  // tolerate both kinds of records in the same edit so a future
  // mixed-mode CF can be added without breaking the wire.
  VersionEdit edit;
  edit.SetColumnFamily(0);
  edit.AddBlobFile(/*number=*/200, /*count=*/5, /*bytes=*/256,
                   /*method=*/"crc32c", /*value=*/std::string(4, '\x01'));
  edit.AddUvlFile(/*number=*/300, /*cf_id=*/0, /*records=*/10,
                  /*bytes=*/2048, /*creation_time=*/0);

  EXPECT_EQ(edit.NumEntries(), 2u);

  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded, /*ts_sz=*/0));
  VersionEdit decoded;
  ASSERT_OK(decoded.DecodeFrom(encoded));
  EXPECT_EQ(decoded.GetBlobFileAdditions().size(), 1u);
  EXPECT_EQ(decoded.GetUvlFileAdditions().size(), 1u);
  EXPECT_EQ(decoded.GetUvlFileAdditions()[0].GetUvlFileNumber(), 300u);
  EXPECT_EQ(decoded.GetBlobFileAdditions()[0].GetBlobFileNumber(), 200u);
}

TEST_F(UvlFileAdditionTest, EmptyEditDoesNotEmitUvlTag) {
  // Backward-compat check: a VersionEdit that never touches the UVL
  // list must produce the same bytes as the same edit encoded by an
  // older (pre-ITEM-14b) build would have. We approximate that
  // contract here by verifying the encoded edit is byte-identical to
  // its decoded-and-re-encoded form, AND that the decoded edit's
  // UvlFileAdditions list is empty.
  VersionEdit edit;
  edit.SetColumnFamily(0);
  edit.SetLogNumber(7);
  edit.SetNextFile(8);
  edit.SetLastSequence(99);
  EXPECT_EQ(edit.GetUvlFileAdditions().size(), 0u);

  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded, /*ts_sz=*/0));

  VersionEdit decoded;
  ASSERT_OK(decoded.DecodeFrom(encoded));
  EXPECT_EQ(decoded.GetUvlFileAdditions().size(), 0u);

  std::string reencoded;
  ASSERT_TRUE(decoded.EncodeTo(&reencoded, 0));
  EXPECT_EQ(encoded, reencoded);
}

TEST_F(UvlFileAdditionTest, DebugStringContainsAllFields) {
  UvlFileAddition addition(99, 4, 50, 8192, 1700);
  const std::string s = addition.DebugString();
  EXPECT_NE(s.find("uvl_file_number: 99"), std::string::npos);
  EXPECT_NE(s.find("column_family_id: 4"), std::string::npos);
  EXPECT_NE(s.find("total_uvl_records: 50"), std::string::npos);
  EXPECT_NE(s.find("total_uvl_bytes: 8192"), std::string::npos);
  EXPECT_NE(s.find("creation_time: 1700"), std::string::npos);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
