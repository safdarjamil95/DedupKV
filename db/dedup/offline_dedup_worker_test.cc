//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-09a: end-to-end test for OfflineDedupWorker.
//
// Writes a synthetic WAL file via log::Writer, enqueues a DWQEntry,
// runs the worker against a capturing sink, and asserts the emitted
// (key, seq, type) tuples match tail→head dedup semantics. Also
// exercises the shutdown path and the success/failure completion
// callback.

#include "db/dedup/offline_dedup_worker.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "db/dedup/cit.h"
#include "db/dedup/dedup_work_queue.h"
#include "db/dedup/dgd.h"
#include "db/dedup/offline_dedup.h"
#include "db/dedup/uvl_file_builder.h"
#include "db/dedup/uvl_file_reader.h"
#include "db/log_writer.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "table/block_based/filter_policy_internal.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Test sink that writes emissions into a test-owned Capture struct,
// so the assertion phase can inspect them even after the worker has
// destroyed the sink.
struct Emission {
  std::string key;
  SequenceNumber seq = 0;
  ValueType type = kTypeValue;
  DGDResult result;
};
struct Capture {
  std::vector<Emission> emissions;
  bool finished = false;
};
class CapturingSink : public OfflineDedupSink {
 public:
  explicit CapturingSink(Capture* capture) : capture_(capture) {}

  Status EmitValue(const Slice& key, SequenceNumber seq,
                   const DGDResult& result) override {
    Emission e;
    e.key = key.ToString();
    e.seq = seq;
    e.type = kTypeValue;
    e.result = result;
    capture_->emissions.push_back(std::move(e));
    return Status::OK();
  }

  Status EmitDelete(const Slice& key, SequenceNumber seq,
                    ValueType type) override {
    Emission e;
    e.key = key.ToString();
    e.seq = seq;
    e.type = type;
    capture_->emissions.push_back(std::move(e));
    return Status::OK();
  }

  Status Finish() override {
    capture_->finished = true;
    return Status::OK();
  }

 private:
  Capture* const capture_;
};

// Stub BF reader that matches every probe (sufficient for Worker tests
// that don't exercise BF-filtered Get paths).
class AlwaysTrueFilter : public FilterBitsReader {
 public:
  using FilterBitsReader::MayMatch;
  bool MayMatch(const Slice&) override { return true; }
};

class OfflineDedupWorkerTest : public testing::Test {
 protected:
  void SetUp() override {
    env_ = Env::Default();
    fs_ = env_->GetFileSystem().get();
    clock_ = env_->GetSystemClock().get();
    dir_ = test::PerThreadDBPath(env_, "OfflineDedupWorkerTest");
    ASSERT_OK(fs_->CreateDirIfMissing(dir_, IOOptions(), nullptr));
  }

  // Write a single-batch WAL file containing the supplied entries.
  // Returns the WAL file number. `batch_seq` is the batch's start seq.
  uint64_t WriteSyntheticWal(
      uint64_t wal_number, SequenceNumber batch_seq,
      const std::vector<std::pair<std::string, std::string>>& puts,
      uint32_t cf_id = 0) {
    WriteBatch batch;
    for (const auto& kv : puts) {
      EXPECT_OK(batch.Put(kv.first, kv.second));
    }
    WriteBatchInternal::SetSequence(&batch, batch_seq);
    WriteBatchInternal::SetCount(&batch, static_cast<uint32_t>(puts.size()));
    // Note: puts default to CF 0; `cf_id` != 0 would require explicit
    // ColumnFamilyHandle in this shim — out of scope for the tests
    // below (all use CF 0).
    (void)cf_id;

    std::string path = LogFileName(dir_, wal_number);
    std::unique_ptr<FSWritableFile> fs_file;
    EXPECT_OK(NewWritableFile(fs_, path, &fs_file, FileOptions()));
    std::unique_ptr<WritableFileWriter> writer(new WritableFileWriter(
        std::move(fs_file), path, FileOptions(), clock_));
    log::Writer log_writer(std::move(writer), wal_number,
                           /*recycle_log_files=*/false);
    WriteOptions wo;
    EXPECT_OK(log_writer.AddRecord(wo, WriteBatchInternal::Contents(&batch),
                                   batch_seq));
    EXPECT_OK(log_writer.file()->Flush(IOOptions()));
    return wal_number;
  }

  Env* env_ = nullptr;
  FileSystem* fs_ = nullptr;
  SystemClock* clock_ = nullptr;
  std::string dir_;
};

TEST_F(OfflineDedupWorkerTest, DrainsSyntheticWalAndCapturesEmissions) {
  const uint64_t wal_number = 42;
  // Keys "a" and "b" appear twice — tail→head dedup keeps only the
  // latest version of each.
  std::vector<std::pair<std::string, std::string>> puts = {
      {"a", std::string(128, 'A')},    // seq 100
      {"b", std::string(128, 'B')},    // seq 101
      {"a", std::string(128, 'A')},    // seq 102 (dup of same value)
      {"c", std::string(128, 'C')},    // seq 103
      {"b", std::string(128, 'Z')},    // seq 104
  };
  WriteSyntheticWal(wal_number, /*batch_seq=*/100, puts);

  auto cit = std::make_shared<CIT>();
  auto stats = std::make_shared<DGDStats>();
  auto dwq = std::make_shared<DWQ>();

  std::atomic<uint64_t> next_uvl{7000};

  std::promise<Status> done_promise;
  auto done_future = done_promise.get_future();
  std::atomic<bool> promise_set{false};

  // Test-owned capture outlives the worker's unique_ptr<Sink>.
  Capture capture;

  OfflineDedupWorker::Options opts;
  opts.dwq = dwq.get();
  opts.cit = cit.get();
  opts.dgd_stats = stats.get();
  opts.fs = fs_;
  opts.clock = clock_;
  opts.wal_dir = dir_;
  opts.uvl_dir = dir_;
  opts.chunk_threshold_bytes = 8;  // all values take the large branch
  opts.next_uvl_file_number = [&]() { return next_uvl.fetch_add(1); };
  opts.sink_factory = [&](uint64_t /*wal*/, uint64_t /*uvl*/)
      -> std::unique_ptr<OfflineDedupSink> {
    return std::make_unique<CapturingSink>(&capture);
  };
  opts.on_complete = [&](uint64_t /*wal*/, const Status& status) {
    if (!promise_set.exchange(true)) {
      done_promise.set_value(status);
    }
  };
  opts.delete_wal_on_success = false;  // keep WAL for test inspection

  // Enqueue the DWQEntry.
  std::string bf_backing;  // empty; our AlwaysTrueFilter doesn't use it
  auto entry = std::make_shared<DWQEntry>(
      wal_number, /*cf_id=*/0, bf_backing,
      std::make_unique<AlwaysTrueFilter>());
  dwq->Push(entry);

  OfflineDedupWorker worker(std::move(opts));
  worker.Start();

  // Wait (up to 5s) for the completion callback.
  auto wait_status = done_future.wait_for(std::chrono::seconds(5));
  ASSERT_EQ(wait_status, std::future_status::ready);
  ASSERT_OK(done_future.get());

  worker.Stop();

  // After tail→head dedup, we expect one emission per distinct key.
  // Order: tail→head with first-seen wins → b(seq=104, value=Z),
  // c(seq=103), a(seq=102), then b(older) and a(older) skipped.
  ASSERT_TRUE(capture.finished);
  ASSERT_EQ(capture.emissions.size(), 3u);
  EXPECT_EQ(capture.emissions[0].key, "b");
  EXPECT_EQ(capture.emissions[0].seq, 104u);
  EXPECT_EQ(capture.emissions[1].key, "c");
  EXPECT_EQ(capture.emissions[1].seq, 103u);
  EXPECT_EQ(capture.emissions[2].key, "a");
  EXPECT_EQ(capture.emissions[2].seq, 102u);

  // CIT should reflect distinct values inserted. "a" appears twice
  // with identical value → single CIT entry. So 3 distinct values
  // overall ("a", "b" at 104 with 'Z', "c"), but tail→head means b's
  // seq=104 value ('Z') was emitted — that's a fresh CIT entry.
  EXPECT_EQ(cit->Size(), 3u);
}

TEST_F(OfflineDedupWorkerTest, StopUnblocksIdleWorker) {
  auto dwq = std::make_shared<DWQ>();
  OfflineDedupWorker::Options opts;
  opts.dwq = dwq.get();
  opts.sink_factory = [](uint64_t, uint64_t) {
    return std::unique_ptr<OfflineDedupSink>(nullptr);
  };
  opts.next_uvl_file_number = []() { return 1u; };
  OfflineDedupWorker worker(std::move(opts));

  worker.Start();
  // Queue is empty; Stop should unblock the WaitForHead quickly.
  auto start = std::chrono::steady_clock::now();
  worker.Stop();
  auto elapsed = std::chrono::steady_clock::now() - start;
  EXPECT_LT(elapsed, std::chrono::seconds(1))
      << "Stop() should unblock the idle worker promptly";
}

TEST_F(OfflineDedupWorkerTest, MissingWalReportedAsFailure) {
  // DWQEntry referencing a WAL that doesn't exist on disk should
  // surface as a completion failure (not a crash).
  auto dwq = std::make_shared<DWQ>();
  auto cit = std::make_shared<CIT>();
  auto stats = std::make_shared<DGDStats>();

  std::atomic<uint64_t> next_uvl{9000};
  std::promise<Status> done_promise;
  auto done_future = done_promise.get_future();
  std::atomic<bool> promise_set{false};

  OfflineDedupWorker::Options opts;
  opts.dwq = dwq.get();
  opts.cit = cit.get();
  opts.dgd_stats = stats.get();
  opts.fs = fs_;
  opts.clock = clock_;
  opts.wal_dir = dir_;
  opts.uvl_dir = dir_;
  opts.next_uvl_file_number = [&]() { return next_uvl.fetch_add(1); };
  Capture missing_capture;
  opts.sink_factory = [&](uint64_t,
                          uint64_t) -> std::unique_ptr<OfflineDedupSink> {
    return std::make_unique<CapturingSink>(&missing_capture);
  };
  opts.on_complete = [&](uint64_t, const Status& s) {
    if (!promise_set.exchange(true)) {
      done_promise.set_value(s);
    }
  };

  std::string bf_backing;
  dwq->Push(std::make_shared<DWQEntry>(
      /*wal_number=*/9999999, 0, bf_backing,
      std::make_unique<AlwaysTrueFilter>()));

  OfflineDedupWorker worker(std::move(opts));
  worker.Start();
  ASSERT_EQ(done_future.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  Status s = done_future.get();
  EXPECT_FALSE(s.ok()) << "missing WAL should surface as failure status";
  worker.Stop();
}

}  // namespace

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
