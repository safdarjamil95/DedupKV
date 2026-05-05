//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-14c end-to-end flush-path tests: exercise a live DBImpl with
// `dedupkv.enable=true`, trigger a flush, and confirm the inline path
// actually opened a UVL file, routed values through DGD, installed the
// UvlFileAddition into the VersionEdit, and left the tree internally
// consistent (physical UVL file + CIT state + installed SST).

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/dedup/cit.h"
#include "db/dedup/dedup_context.h"
#include "db/dedup/dedup_flush_adapter.h"
#include "db/dedup/dedup_work_queue.h"
#include "db/dedup/dgd.h"
#include "db/dedup/memory_monitor.h"
#include "db/dedup/uvl_file_reader.h"
#include "test_util/sync_point.h"
#include "util/sha1.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DedupFlushPathTest : public DBTestBase {
 public:
  DedupFlushPathTest()
      : DBTestBase("dedup_flush_path_test", /*env_do_fsync=*/true) {}

  std::vector<std::string> ListUvlFiles() {
    std::vector<std::string> children;
    EXPECT_OK(env_->GetChildren(dbname_, &children));
    std::vector<std::string> result;
    for (const auto& name : children) {
      uint64_t number = 0;
      FileType type = kInfoLogFile;
      if (ParseFileName(name, &number, &type) && type == kUvlFile) {
        result.push_back(dbname_ + "/" + name);
      }
    }
    return result;
  }

  std::shared_ptr<DedupContext> GetDedupCtx(uint32_t cf_id) {
    DBImpl* impl = static_cast<DBImpl*>(db_.get());
    return impl->GetDedupContext(cf_id);
  }

  // Opens a UVL file and returns its on-disk byte count minus the header.
  uint64_t UvlPayloadBytes(const std::string& path) {
    std::unique_ptr<FSRandomAccessFile> raf;
    EXPECT_OK(env_->GetFileSystem()->NewRandomAccessFile(path, FileOptions(),
                                                         &raf, nullptr));
    uint64_t file_size = 0;
    EXPECT_OK(env_->GetFileSystem()->GetFileSize(path, IOOptions(), &file_size,
                                                 nullptr));
    std::unique_ptr<RandomAccessFileReader> reader(new RandomAccessFileReader(
        std::move(raf), path, env_->GetSystemClock().get()));
    std::unique_ptr<UvlFileReader> ufr;
    EXPECT_OK(UvlFileReader::Open(std::move(reader), file_size, &ufr));
    return file_size - 24;  // 24-byte UvlHeader
  }
};

TEST_F(DedupFlushPathTest, BaselineFlushProducesNoUvlWhenDedupDisabled) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // dedupkv.enable = false by default.
  ASSERT_OK(TryReopen(options));

  for (int i = 0; i < 32; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(256, 'v')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  EXPECT_TRUE(ListUvlFiles().empty());
  EXPECT_EQ(GetDedupCtx(0), nullptr);
}

TEST_F(DedupFlushPathTest, InlineFlushInstallsUvlAndCitEntries) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  // Force every value down the SHA1/large-value branch — small-value
  // LZ4 path doesn't mutate the CIT and would muddy the assertions.
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  constexpr int kUnique = 5;
  constexpr int kCopies = 4;
  std::vector<std::string> values;
  for (int i = 0; i < kUnique; ++i) {
    values.emplace_back(128, static_cast<char>('A' + i));
  }
  int kid = 0;
  for (int c = 0; c < kCopies; ++c) {
    for (int v = 0; v < kUnique; ++v) {
      ASSERT_OK(db_->Put(WriteOptions(), "key" + std::to_string(kid++),
                         values[v]));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  auto files = ListUvlFiles();
  ASSERT_EQ(files.size(), 1u) << "expected exactly one UVL file per flush";

  // CIT records one entry per unique fingerprint.
  EXPECT_EQ(ctx->cit->Size(), static_cast<size_t>(kUnique));

  // UVL payload must be non-empty (kUnique records were appended).
  EXPECT_GT(UvlPayloadBytes(files[0]), 0u);
}

TEST_F(DedupFlushPathTest, OfflineOnlyFlushDrainedByWorker) {
  // ITEM-15b + ITEM-09c: OfflineOnly flush skips BuildTable and hands
  // off to the offline worker, which drains the WAL and installs an
  // L0 SST + UVL via LogAndApply. Observable: the DWQ drains to zero
  // and Get() returns the flushed values.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kOfflineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;  // large-branch
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->dwq, nullptr);
  EXPECT_EQ(ctx->dwq->Size(), 0u);

  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < 16; ++i) {
    std::string k = "k" + std::to_string(i);
    std::string v(128, static_cast<char>('a' + (i % 8)));
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
    kvs.emplace_back(std::move(k), std::move(v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Wait (up to 5s) for the offline worker to drain the DWQ.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ctx->dwq->Size() > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(ctx->dwq->Size(), 0u) << "offline worker should have drained DWQ";

  // UVL appeared (the worker's production sink created one alongside
  // its L0 SST).
  EXPECT_GE(ListUvlFiles().size(), 1u);

  // Get each flushed key — routes through worker-installed SST → UVL.
  for (const auto& kv : kvs) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), kv.first, &got));
    EXPECT_EQ(got, kv.second) << "offline-drained Get mismatch for " << kv.first;
  }
}

TEST_F(DedupFlushPathTest, ElasticBelowThresholdStaysInline) {
  // Elastic mode with a high memory threshold: inline dedup runs, no
  // DWQ enqueue happens.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kElastic;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.dedupkv.memory_threshold_pct = 0.99;  // effectively never over
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(128, static_cast<char>('A' + i))));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  EXPECT_EQ(ListUvlFiles().size(), 1u);  // inline adapter ran
  EXPECT_EQ(ctx->dwq->Size(), 0u);       // no offline enqueue
}

TEST_F(DedupFlushPathTest, ElasticAboveThresholdDrainedByWorker) {
  // ITEM-15b: Elastic mode with a zero memory threshold forces the
  // offline branch. BuildTable is skipped; the offline worker drains
  // the DWQ and Get returns the original values.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kElastic;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.dedupkv.memory_threshold_pct = 0.0;  // always over
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < 6; ++i) {
    std::string k = "ek" + std::to_string(i);
    std::string v(128, static_cast<char>('a' + i));
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
    kvs.emplace_back(std::move(k), std::move(v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ctx->dwq->Size() > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(ctx->dwq->Size(), 0u);

  for (const auto& kv : kvs) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), kv.first, &got));
    EXPECT_EQ(got, kv.second);
  }
}

// ITEM-15c: the monitor must actually track MemTable allocations. Under
// baseline conditions with dedup enabled, enough Puts should push the
// monitor's numerator above a realistic threshold (0.5), which then
// causes the Elastic controller to take the offline branch. Previously
// this could only be demonstrated by setting the threshold to 0.0.
TEST_F(DedupFlushPathTest, MemoryMonitorCountsMemtableBytes) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;  // no side-effects on DWQ
  options.dedupkv.chunk_threshold_bytes = 8;
  options.write_buffer_size = 64 * 1024;   // small memtable for the test
  options.max_write_buffer_number = 2;     // capacity = 128 KiB
  options.disable_auto_compactions = true;  // keep artefact count stable
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->memory_monitor, nullptr);
  EXPECT_EQ(ctx->memory_monitor->MemtableBytes(), 0u);
  EXPECT_GT(ctx->memory_monitor->CapacityBytes(), 0u);

  // Insert a few KB of data; the monitor must see a non-zero numerator
  // before any flush happens.
  for (int i = 0; i < 16; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(256, 'v')));
  }
  const uint64_t after_puts = ctx->memory_monitor->MemtableBytes();
  EXPECT_GT(after_puts, 0u)
      << "MemTable::Add must charge the elastic-controller monitor";
  EXPECT_GT(ctx->memory_monitor->TotalAllocated(), 0u);

  // Post-flush: the old MT is freed → monitor discharges. Its counter
  // should drop (the new active MT has no writes yet).
  ASSERT_OK(db_->Flush(FlushOptions()));
  EXPECT_LT(ctx->memory_monitor->MemtableBytes(), after_puts)
      << "~MemTable must discharge the monitor";
  EXPECT_GT(ctx->memory_monitor->TotalFreed(), 0u);
}

TEST_F(DedupFlushPathTest, ElasticFiresAtRealisticThreshold) {
  // End-to-end demonstration that Elastic mode actually responds to
  // memtable pressure. Without ITEM-15c wiring this test would never
  // observe a DWQ enqueue because the monitor's counter would stay at
  // zero.
  //
  // Sizing: pick write_buffer_size large enough that the Puts don't
  // auto-flush mid-test (which would drain the monitor before we reach
  // the manual Flush call). Capacity = write_buffer_size *
  // max_write_buffer_number; target utilization ~2× the threshold so
  // the elastic decision is unambiguous.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kElastic;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.dedupkv.memory_threshold_pct = 0.1;  // realistic, not 0.0
  options.write_buffer_size = 16 * 1024 * 1024;  // 16 MiB — no auto-flush
  options.max_write_buffer_number = 2;            // capacity = 32 MiB
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  ASSERT_EQ(ctx->dwq->Size(), 0u);

  constexpr int kValueLen = 1024;
  // ~4 MiB of payload → utilization ~0.13 over a 32 MiB capacity, 1.3×
  // the 0.1 threshold.
  constexpr int kPuts = 4000;
  for (int i = 0; i < kPuts; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(kValueLen, static_cast<char>('a' + (i % 26)))));
  }
  EXPECT_GT(ctx->memory_monitor->Utilization(), 0.1)
      << "MemtableBytes=" << ctx->memory_monitor->MemtableBytes()
      << " Capacity=" << ctx->memory_monitor->CapacityBytes();

  ASSERT_OK(db_->Flush(FlushOptions()));

  // Elastic picked offline; wait for the worker to drain.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ctx->dwq->Size() > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(ctx->dwq->Size(), 0u)
      << "elastic offline branch should have been drained by worker";
}

TEST_F(DedupFlushPathTest, InlineOnlyDoesNotEnqueueOffline) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(128, 'v')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  EXPECT_EQ(ListUvlFiles().size(), 1u);
  EXPECT_EQ(ctx->dwq->Size(), 0u);
}

TEST_F(DedupFlushPathTest, GetAfterDedupFlushReturnsOriginalLargeValues) {
  // ITEM-16a: after a dedup flush, Get must resolve the SST's
  // kDedupKVUvl BlobIndex by reading the UVL file. Prior to 16a this
  // failed with Status::Corruption("Invalid blob file number") because
  // Version::GetBlob looked the UVL up in the blob-files map.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;  // everything is large-branch
  ASSERT_OK(TryReopen(options));

  constexpr int kNumKeys = 40;
  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < kNumKeys; ++i) {
    // 64 distinct values repeated to create a duplicate pattern — dedup
    // hit/miss mix exercises both CIT paths under Get.
    std::string v = std::string(192, static_cast<char>('a' + (i % 8)));
    std::string k = "large-key-" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
    kvs.emplace_back(std::move(k), std::move(v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Each Get has to hit the SST (not the MT — flush cleared it) and
  // follow the kDedupKVUvl BlobIndex through Version::GetUvlValue.
  for (const auto& kv : kvs) {
    std::string got;
    Status s = db_->Get(ReadOptions(), kv.first, &got);
    ASSERT_OK(s) << "Get failed for key=" << kv.first;
    EXPECT_EQ(got, kv.second) << "value mismatch for key=" << kv.first;
  }
}

TEST_F(DedupFlushPathTest, GetAfterDedupFlushReturnsOriginalSmallValues) {
  // The small-value DGD branch stores LZ4-compressed bytes in UVL.
  // ITEM-16a's GetUvlValue must decompress on read.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 1024;  // force small branch
  ASSERT_OK(TryReopen(options));

  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < 32; ++i) {
    // 64 bytes, heavily compressible (all same char) → LZ4 compresses
    // well, decompress must be exact.
    std::string v(64, static_cast<char>('p' + (i % 4)));
    std::string k = "small-key-" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
    kvs.emplace_back(std::move(k), std::move(v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (const auto& kv : kvs) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), kv.first, &got));
    EXPECT_EQ(got, kv.second) << "small-value Get mismatch for " << kv.first;
  }
}

TEST_F(DedupFlushPathTest, GetMissingKeyAfterDedupFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "present-" + std::to_string(i),
                       std::string(128, 'v')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::string got;
  Status s = db_->Get(ReadOptions(), "absent", &got);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

// Close+reopen smoke test to probe ITEM-19's scope. Today this is
// expected to pass for the inline path — the UVL file is on disk, the
// SST's kDedupKVUvl BlobIndex points at it, and Version::GetUvlValue
// reads directly by file number (no CIT dependency). The CIT itself
// is lost across reopen; that only matters for subsequent dedup hits
// against prior-flush fingerprints, which is ITEM-19's real work.
TEST_F(DedupFlushPathTest, CloseReopenInlineReadsSurvive) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < 20; ++i) {
    std::string v(192, static_cast<char>('A' + (i % 6)));
    std::string k = "persist-" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
    kvs.emplace_back(std::move(k), std::move(v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Close + reopen.
  Close();
  ASSERT_OK(TryReopen(options));

  for (const auto& kv : kvs) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), kv.first, &got))
        << "reopened Get failed for " << kv.first;
    EXPECT_EQ(got, kv.second) << "reopened Get mismatch for " << kv.first;
  }
}

TEST_F(DedupFlushPathTest, CloseReopenOfflineReadsSurvive) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kOfflineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < 12; ++i) {
    std::string v(192, static_cast<char>('A' + (i % 4)));
    std::string k = "offpersist-" + std::to_string(i);
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
    kvs.emplace_back(std::move(k), std::move(v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Wait for the offline worker to drain so the L0 SST is installed
  // before we close.
  auto ctx = GetDedupCtx(0);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ctx->dwq->Size() > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(ctx->dwq->Size(), 0u);

  Close();
  ASSERT_OK(TryReopen(options));

  for (const auto& kv : kvs) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), kv.first, &got))
        << "offline reopened Get failed for " << kv.first;
    EXPECT_EQ(got, kv.second);
  }
}

// ITEM-16b gap probe: verify the visibility window between the
// offline flush handing off to the worker and the worker installing
// its L0 SST. If the gap is real, a Get during the window returns
// NotFound; ITEM-16b's DWQ+WAL redirection closes that.
TEST_F(DedupFlushPathTest, OfflineFlushVisibilityGap_Item16bProbe) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kOfflineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  // Pause the offline worker right after it pops the DWQEntry but
  // before it runs ProcessEntry. The sync-point callback blocks on
  // `worker_gate` so the main thread can observe the system state
  // while the worker is suspended.
  std::mutex gate_mu;
  std::condition_variable gate_cv;
  bool worker_may_proceed = false;
  std::atomic<int> worker_reached{0};

  SyncPoint::GetInstance()->SetCallBack(
      "OfflineDedupWorker::Run:BeforeProcess", [&](void* /*entry*/) {
        worker_reached.fetch_add(1, std::memory_order_relaxed);
        std::unique_lock<std::mutex> lk(gate_mu);
        gate_cv.wait(lk, [&] { return worker_may_proceed; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<void, std::function<void(void*)>> gate_cleanup(
      reinterpret_cast<void*>(1), [&](void*) {
        {
          std::lock_guard<std::mutex> lk(gate_mu);
          worker_may_proceed = true;
        }
        gate_cv.notify_all();
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
      });

  std::vector<std::pair<std::string, std::string>> kvs;
  for (int i = 0; i < 8; ++i) {
    std::string k = "gap-" + std::to_string(i);
    std::string v(128, static_cast<char>('A' + i));
    ASSERT_OK(db_->Put(WriteOptions(), k, v));
    kvs.emplace_back(std::move(k), std::move(v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Wait for the worker to hit the sync point (DWQEntry popped, WAL
  // still alive, SST not yet installed — the exact gap).
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (worker_reached.load(std::memory_order_relaxed) == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_EQ(worker_reached.load(std::memory_order_relaxed), 1)
      << "worker never reached the BeforeProcess sync point";

  // Probe the gap: each flushed key should now be invisible to Get
  // unless ITEM-16b's DWQ+WAL redirection is wired.
  std::vector<bool> found(kvs.size(), false);
  for (size_t i = 0; i < kvs.size(); ++i) {
    std::string got;
    Status s = db_->Get(ReadOptions(), kvs[i].first, &got);
    if (s.ok() && got == kvs[i].second) {
      found[i] = true;
    }
  }
  // With ITEM-16b wired, all 8 should be found. Without it, all 8
  // miss. The assertion below flips once the implementation lands.
  const size_t hit_count = std::count(found.begin(), found.end(), true);
  ASSERT_EQ(hit_count, kvs.size())
      << "ITEM-16b visibility gap: " << (kvs.size() - hit_count)
      << "/" << kvs.size() << " keys invisible during offline-worker drain";
}

// ITEM-16b edge case: a Delete in the pending WAL must mask any
// older SST value during the offline-drain window. Without 16b this
// would leak stale data; with 16b GetImpl returns NotFound.
TEST_F(DedupFlushPathTest, OfflineDwqRedirectHonorsWalDeletes) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kOfflineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  // First, install a key via a completed offline drain so it lives in
  // an L0 SST.
  const std::string kKey = "doomed";
  const std::string kValOld(128, 'o');
  ASSERT_OK(db_->Put(WriteOptions(), kKey, kValOld));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto ctx = GetDedupCtx(0);
  {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (ctx->dwq->Size() > 0 &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_EQ(ctx->dwq->Size(), 0u);
  }
  // Sanity: key visible pre-delete.
  std::string got;
  ASSERT_OK(db_->Get(ReadOptions(), kKey, &got));
  EXPECT_EQ(got, kValOld);

  // Install the sync-point gate so the next flush's worker pauses.
  std::mutex gate_mu;
  std::condition_variable gate_cv;
  bool worker_may_proceed = false;
  std::atomic<int> reached{0};
  SyncPoint::GetInstance()->SetCallBack(
      "OfflineDedupWorker::Run:BeforeProcess", [&](void*) {
        reached.fetch_add(1, std::memory_order_relaxed);
        std::unique_lock<std::mutex> lk(gate_mu);
        gate_cv.wait(lk, [&] { return worker_may_proceed; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto gate_release = std::unique_ptr<void, std::function<void(void*)>>(
      reinterpret_cast<void*>(1), [&](void*) {
        {
          std::lock_guard<std::mutex> lk(gate_mu);
          worker_may_proceed = true;
        }
        gate_cv.notify_all();
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
      });

  // Delete the key + flush → DWQEntry with a tombstone in the WAL.
  ASSERT_OK(db_->Delete(WriteOptions(), kKey));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (reached.load() == 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_EQ(reached.load(), 1);

  // During the gap: ITEM-16b must observe the tombstone in the WAL
  // and return NotFound, NOT fall through to the old SST value.
  got.clear();
  Status s = db_->Get(ReadOptions(), kKey, &got);
  EXPECT_TRUE(s.IsNotFound()) << "WAL tombstone should mask SST value; "
                              << "status=" << s.ToString()
                              << " got='" << got << "'";
}

// ITEM-18a: dropping all references to a UVL record should charge
// the UVL file's invalid-byte accumulator.
TEST_F(DedupFlushPathTest, UvlGarbageMeterAccumulatesOnFullDrop) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  // Keep this test focused on meter accumulation — disable ITEM-18f's
  // auto-GC by setting threshold >= 1.0 so the meter accumulation is
  // observable without the rewriter Forgetting the entry.
  options.dedupkv.uvl_gc_threshold = 1.0;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->uvl_garbage_meter, nullptr);

  const std::string kKey = "meter-target";
  const std::string kVal(128, 'M');

  // Flush 1: the UVL record exists with refcount 1.
  ASSERT_OK(db_->Put(WriteOptions(), kKey, kVal));
  ASSERT_OK(db_->Flush(FlushOptions()));
  auto uvls_after_put = ListUvlFiles();
  ASSERT_EQ(uvls_after_put.size(), 1u);

  // Meter should still be empty — nothing's been dropped.
  EXPECT_EQ(ctx->uvl_garbage_meter->Snapshot().size(), 0u);

  // Delete + force bottommost compaction → refcount 1 → 0 → meter
  // accumulates.
  ASSERT_OK(db_->Delete(WriteOptions(), kKey));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  auto snap = ctx->uvl_garbage_meter->Snapshot();
  ASSERT_EQ(snap.size(), 1u) << "exactly one UVL file should have garbage";
  const uint64_t charged = snap.begin()->second;
  // Charge = value_size + record overhead (≥ 128 bytes, ≤ a few
  // hundred bytes with overhead).
  EXPECT_GE(charged, 128u);
  EXPECT_LT(charged, 256u)
      << "record footprint should be value_size + small overhead";
}

// ITEM-17b: a Delete + bottommost compact drops the prior PUT; the
// PUT's CIT refcount must decrement.
TEST_F(DedupFlushPathTest, DeleteBottommostCompactionDecrementsRefcount) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  const std::string kKey = "gone";
  const std::string kVal(128, 'X');

  auto ValueFingerprint = [](const std::string& v) {
    Sha1Digest d = Sha1Hash(reinterpret_cast<const uint8_t*>(v.data()),
                            v.size());
    UvlFingerprint fp{};
    std::memcpy(fp.data(), d.data(), kUvlFingerprintSize);
    return fp;
  };

  // Flush 1: insert the key → CIT refcount 1.
  ASSERT_OK(db_->Put(WriteOptions(), kKey, kVal));
  ASSERT_OK(db_->Flush(FlushOptions()));
  {
    CITEntry e;
    ASSERT_TRUE(ctx->cit->Lookup(ValueFingerprint(kVal), &e,
                                 /*touch_lru=*/false));
    EXPECT_EQ(e.refcount, 1u);
  }

  // Flush 2: Delete the key → tombstone lands in a new L0 SST.
  ASSERT_OK(db_->Delete(WriteOptions(), kKey));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Compact to the bottom level. Rule-A overwrite would NOT fire for
  // a delete (Delete has no kTypeBlobIndex); the PUT gets dropped via
  // the bottommost-delete loop (ITEM-17b's new hook).
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  // Refcount should have dropped to 0. CIT::Lookup still finds the
  // entry (we don't auto-evict); refcount==0 is the ITEM-18 GC
  // signal.
  CITEntry e;
  ASSERT_TRUE(
      ctx->cit->Lookup(ValueFingerprint(kVal), &e, /*touch_lru=*/false));
  EXPECT_EQ(e.refcount, 0u)
      << "bottommost-delete drop loop should have decremented the refcount";

  // Key is gone.
  std::string got;
  Status s = db_->Get(ReadOptions(), kKey, &got);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

// ITEM-17: overwritten dedup keys should DecRefcount on compaction.
TEST_F(DedupFlushPathTest, CompactionDropsDedupRefcount) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;  // large-branch
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  // Flush 1: 8 distinct values, each inserted once. CIT gains 8
  // entries, each refcount=1.
  std::vector<std::string> values;
  for (int i = 0; i < 8; ++i) {
    values.emplace_back(std::string(128, static_cast<char>('A' + i)));
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(ctx->cit->Size(), 8u);

  // Compute each value's fingerprint so we can Lookup refcounts in
  // the CIT directly.
  auto ValueFingerprint = [](const std::string& v) {
    Sha1Digest d = Sha1Hash(reinterpret_cast<const uint8_t*>(v.data()),
                            v.size());
    UvlFingerprint fp{};
    std::memcpy(fp.data(), d.data(), kUvlFingerprintSize);
    return fp;
  };
  for (int i = 0; i < 8; ++i) {
    CITEntry e;
    ASSERT_TRUE(
        ctx->cit->Lookup(ValueFingerprint(values[i]), &e, /*touch_lru=*/false))
        << "CIT should contain fingerprint for value " << i;
    EXPECT_EQ(e.refcount, 1u) << "post-first-flush refcount should be 1";
  }

  // Flush 2: overwrite every key with the SAME value — CIT hits
  // bump refcount to 2.
  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(ctx->cit->Size(), 8u);
  for (int i = 0; i < 8; ++i) {
    CITEntry e;
    ASSERT_TRUE(
        ctx->cit->Lookup(ValueFingerprint(values[i]), &e, /*touch_lru=*/false));
    EXPECT_EQ(e.refcount, 2u) << "post-overwrite refcount should be 2";
  }

  // Force compaction. Compaction drops the OLDER version of each
  // overwritten key (rule A). ITEM-17's hook fires at that drop.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(ctx->cit->Size(), 8u);

  // If ITEM-17 is wired correctly, refcounts should be back to 1.
  for (int i = 0; i < 8; ++i) {
    CITEntry e;
    ASSERT_TRUE(
        ctx->cit->Lookup(ValueFingerprint(values[i]), &e, /*touch_lru=*/false));
    EXPECT_EQ(e.refcount, 1u)
        << "post-compaction refcount should drop to 1 via ITEM-17 DecRefcount";
  }

  // Values still readable.
  for (int i = 0; i < 8; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got));
    EXPECT_EQ(got, values[i]);
  }
}

TEST_F(DedupFlushPathTest, DedupHitOnlyFlushDiscardsEmptyUvl) {
  // Prime the CIT with a first flush whose values will then be duplicates
  // in the second flush. The second flush's UVL has zero fresh records
  // so the post-flush cleanup should unlink it (record_count==0 branch).
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  std::vector<std::string> values;
  for (int i = 0; i < 3; ++i) {
    values.emplace_back(128, static_cast<char>('P' + i));
  }

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "a" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(ListUvlFiles().size(), 1u);
  EXPECT_EQ(ctx->cit->Size(), 3u);

  // Same three values — every Add is a dedup hit, so the second UVL has
  // zero records and is unlinked by the flush cleanup path.
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "b" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  EXPECT_EQ(ListUvlFiles().size(), 1u);
  EXPECT_EQ(ctx->cit->Size(), 3u);
}

// ITEM-18c: a dedupkv flush should emit kDedupKVUvlV2 BlobIndex
// entries (20-byte fingerprint embedded). V1 is the pre-18c format;
// after 18c every fresh write-site uses V2 so the SST self-describes
// the fingerprint for downstream consumers.
TEST_F(DedupFlushPathTest, InlineFlushEmitsV2BlobIndex) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  const std::string kKey = "k-v2";
  const std::string kVal(128, 'Z');
  ASSERT_OK(db_->Put(WriteOptions(), kKey, kVal));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Read the SST payload via GetEntity's internal BlobIndex surface.
  // Easiest: use the CompactionIterator-style path by iterating the
  // raw SST. We skip that and instead re-derive by checking the CIT
  // knows the fingerprint and the corresponding V2 encoding on
  // Get-after-flush re-issue. Direct SST introspection would require
  // TableReader plumbing — probe via DGDResult encoding instead.
  DGDResult dgd_result;
  dgd_result.uvl_file = 1;
  dgd_result.offset = 0;
  dgd_result.size = 128;
  dgd_result.compression = UvlCompression::kRaw;
  for (size_t i = 0; i < dgd_result.fingerprint.size(); ++i) {
    dgd_result.fingerprint[i] = static_cast<uint8_t>(i);
  }
  std::string encoded;
  EncodeUvlBlobIndex(dgd_result, &encoded);
  ASSERT_FALSE(encoded.empty());
  EXPECT_EQ(static_cast<unsigned char>(encoded[0]),
            static_cast<unsigned char>(BlobIndex::Type::kDedupKVUvlV2))
      << "18c requires EncodeUvlBlobIndex to emit V2";

  BlobIndex bi;
  ASSERT_OK(bi.DecodeFrom(Slice(encoded)));
  EXPECT_TRUE(bi.IsDedupKVUvl());
  EXPECT_TRUE(bi.HasFingerprint());
  EXPECT_EQ(bi.fingerprint(), dgd_result.fingerprint);

  // Get-after-flush still round-trips (V2 BlobIndex goes through the
  // same Version::GetUvlValue path as V1 — no behaviour change in
  // 18c).
  std::string got;
  ASSERT_OK(db_->Get(ReadOptions(), kKey, &got));
  EXPECT_EQ(got, kVal);
}

// ITEM-18b: a UVL file whose invalid-byte ratio exceeds the GC
// threshold should be rewritten — live records copied to a fresh UVL,
// CIT entries retargeted to the new location, and the old file's entry
// in the meter cleared. Old file retention (on-disk deletion) is
// deferred — see DEC-021; this test verifies only the rewriter's own
// guarantees.
TEST_F(DedupFlushPathTest, UvlGcRewriterMovesLiveRecordsToNewFile) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;  // force large-branch dedup
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  auto ValueFingerprint = [](const std::string& v) {
    Sha1Digest d = Sha1Hash(reinterpret_cast<const uint8_t*>(v.data()),
                            v.size());
    UvlFingerprint fp{};
    std::memcpy(fp.data(), d.data(), kUvlFingerprintSize);
    return fp;
  };

  // Flush N=6 unique large values into a single UVL file.
  constexpr int kN = 6;
  std::vector<std::string> values;
  values.reserve(kN);
  for (int i = 0; i < kN; ++i) {
    // Distinct bytes per value → distinct SHA1 → distinct CIT entries.
    values.emplace_back(128, static_cast<char>('A' + i));
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  const auto uvls_initial = ListUvlFiles();
  ASSERT_EQ(uvls_initial.size(), 1u);
  ASSERT_EQ(ctx->cit->Size(), static_cast<size_t>(kN));

  // Extract the old file number by parsing the filename.
  uint64_t old_uvl_number = 0;
  {
    const std::string basename =
        uvls_initial.front().substr(uvls_initial.front().find_last_of('/') + 1);
    FileType ft = kInfoLogFile;
    ASSERT_TRUE(ParseFileName(basename, &old_uvl_number, &ft));
    ASSERT_EQ(ft, kUvlFile);
  }

  // Delete the first N/2 keys and force a bottommost compaction so the
  // dropped SST entries drive CIT refcounts for their fingerprints to
  // zero and charge the garbage meter against the UVL file.
  constexpr int kDropped = kN / 2;
  for (int i = 0; i < kDropped; ++i) {
    ASSERT_OK(db_->Delete(WriteOptions(), "k" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  // Sanity: the dropped fingerprints have refcount 0 before auto-GC
  // relocates survivors; surviving ones still at refcount 1.
  // (We check this BEFORE CompactRange triggers auto-GC.)
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  // ITEM-18f: auto-GC fires during CompactRange because invalid/total
  // crosses the 0.5 default threshold. Find the new UVL by listing;
  // the old one has been reclaimed.
  const auto uvls_after = ListUvlFiles();
  ASSERT_EQ(uvls_after.size(), 1u)
      << "after auto-GC, only the new UVL should remain on disk";
  uint64_t new_uvl_number = 0;
  {
    const std::string basename =
        uvls_after.front().substr(uvls_after.front().find_last_of('/') + 1);
    FileType ft = kInfoLogFile;
    ASSERT_TRUE(ParseFileName(basename, &new_uvl_number, &ft));
  }
  ASSERT_NE(new_uvl_number, 0u);
  ASSERT_NE(new_uvl_number, old_uvl_number);
  const std::string new_uvl_path = uvls_after.front();

  // The new UVL contains exactly the surviving records (refcount > 0).
  // Read its full byte range and decode every record.
  {
    std::unique_ptr<FSRandomAccessFile> raf;
    ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
        new_uvl_path, FileOptions(), &raf, nullptr));
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSystem()->GetFileSize(new_uvl_path, IOOptions(),
                                                 &file_size, nullptr));
    std::string buf;
    buf.resize(file_size - UvlHeader::kSize);
    Slice result;
    ASSERT_OK(raf->Read(UvlHeader::kSize, buf.size(), IOOptions(), &result,
                        buf.data(), nullptr));
    // For in-memory env, Read may return a slice into the env's own
    // backing store; copy into `buf` if needed.
    if (result.data() != buf.data()) {
      std::memcpy(buf.data(), result.data(), result.size());
    }
    Slice cursor(buf);
    int records_seen = 0;
    while (!cursor.empty()) {
      UvlRecord rec;
      ASSERT_OK(DecodeUvlRecord(&cursor, &rec));
      // Each record's fingerprint must correspond to a surviving key.
      bool matched = false;
      for (int i = kDropped; i < kN; ++i) {
        if (rec.fingerprint == ValueFingerprint(values[i])) {
          matched = true;
          break;
        }
      }
      EXPECT_TRUE(matched)
          << "new UVL record fingerprint should match a surviving key";
      ++records_seen;
    }
    EXPECT_EQ(records_seen, kN - kDropped)
        << "new UVL should contain exactly the surviving records";
  }

  // CIT entries for surviving fingerprints now point at the new file.
  for (int i = kDropped; i < kN; ++i) {
    CITEntry e;
    ASSERT_TRUE(ctx->cit->Lookup(ValueFingerprint(values[i]), &e,
                                 /*touch_lru=*/false));
    EXPECT_EQ(e.uvl_file, new_uvl_number)
        << "surviving fp should have been retargeted to the new UVL";
    EXPECT_EQ(e.refcount, 1u)
        << "rewriter must not disturb refcounts";
  }

  // The garbage meter has forgotten the old file (fresh accounting
  // against the new file starts at zero).
  EXPECT_EQ(ctx->uvl_garbage_meter->InvalidBytes(old_uvl_number), 0u);
  EXPECT_EQ(ctx->uvl_garbage_meter->InvalidBytes(new_uvl_number), 0u);

  // Surviving keys still read correctly in this session — ITEM-18d's
  // CIT indirection routes them through to the new UVL.
  for (int i = kDropped; i < kN; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got));
    EXPECT_EQ(got, values[i]);
  }

  // The VersionEdit recorded the kUvlFileGarbage tag: reopening the DB
  // must load that state cleanly (no Corruption, no dangling meter
  // charge).
  ASSERT_OK(TryReopen(options));
  auto ctx2 = GetDedupCtx(0);
  ASSERT_NE(ctx2, nullptr);
  // After reopen the meter starts empty (there has been no compaction
  // since the GC).
  EXPECT_EQ(ctx2->uvl_garbage_meter->InvalidBytes(new_uvl_number), 0u);
  // ITEM-19: CIT rebuilt on reopen from the surviving UVL; 18d's
  // indirection now has a CIT entry to follow for each surviving fp,
  // and Gets succeed without the old (reclaimed) file.
  for (int i = kDropped; i < kN; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got))
        << "post-reopen Get should work after ITEM-19 CIT rebuild";
    EXPECT_EQ(got, values[i]);
  }
}

// ITEM-19: after GC has relocated records to a new UVL and reclaimed
// the old one, a close+reopen must not lose read correctness. The
// CIT is in-memory only; on reopen it starts empty and 18d's
// BlobIndex→CIT indirection has nothing to follow. ITEM-19 rebuilds
// the CIT from the UVL files that survive on disk.
TEST_F(DedupFlushPathTest, ReopenAfterGcRebuildsCitForSurvivingGets) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.dedupkv.uvl_gc_threshold = 0.5;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  // Write N unique large values, delete N/2, compact → auto-GC runs
  // (ITEM-18f), old UVL reclaimed (ITEM-18e).
  constexpr int kN = 6;
  std::vector<std::string> values;
  for (int i = 0; i < kN; ++i) {
    values.emplace_back(128, static_cast<char>('a' + i));
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  for (int i = 0; i < kN / 2; ++i) {
    ASSERT_OK(db_->Delete(WriteOptions(), "k" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  // Surviving Gets work pre-reopen via CIT indirection.
  for (int i = kN / 2; i < kN; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got));
    EXPECT_EQ(got, values[i]);
  }

  // Now close + reopen. Without ITEM-19, the reopened CIT is empty;
  // V2 BlobIndex entries still name the reclaimed-but-retargeted
  // old UVL, and Get would fall back to that gone file → IOError.
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->cit, nullptr);
  // CIT has been rebuilt from the surviving UVL on disk.
  EXPECT_EQ(ctx->cit->Size(), static_cast<size_t>(kN - kN / 2))
      << "CIT should be repopulated from the post-GC UVL";

  // Gets for surviving keys succeed post-reopen.
  for (int i = kN / 2; i < kN; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got))
        << "post-reopen Get for key " << i
        << " requires ITEM-19 CIT rebuild";
    EXPECT_EQ(got, values[i]);
  }
}

// ITEM-19: the size registry fed by ITEM-18f must also survive
// reopen, so the auto-GC scheduler can evaluate the restored UVL.
TEST_F(DedupFlushPathTest, ReopenRepopulatesUvlTotalBytesRegistry) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(128, static_cast<char>('a' + i))));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  const auto uvls = ListUvlFiles();
  ASSERT_EQ(uvls.size(), 1u);
  uint64_t uvl_number = 0;
  {
    const std::string basename =
        uvls.front().substr(uvls.front().find_last_of('/') + 1);
    FileType ft = kInfoLogFile;
    ASSERT_TRUE(ParseFileName(basename, &uvl_number, &ft));
  }

  ASSERT_OK(TryReopen(options));
  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  const auto sizes = ctx->SnapshotUvlFileSizes();
  EXPECT_EQ(sizes.size(), 1u);
  auto it = sizes.find(uvl_number);
  ASSERT_NE(it, sizes.end());
  EXPECT_GT(it->second, 0u);
}

// ITEM-18f: the invalid-byte ratio crossing the GC threshold during a
// user-triggered compaction should automatically kick off the
// rewriter for the affected UVL. No manual TriggerUvlGcForTest call
// needed — CompactRange alone should suffice.
TEST_F(DedupFlushPathTest, AutoGcFiresAfterCompactionOverThreshold) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.dedupkv.uvl_gc_threshold = 0.5;  // 50% invalid → rewrite
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  constexpr int kN = 6;
  std::vector<std::string> values;
  for (int i = 0; i < kN; ++i) {
    values.emplace_back(128, static_cast<char>('a' + i));
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  const auto uvls_initial = ListUvlFiles();
  ASSERT_EQ(uvls_initial.size(), 1u);
  const std::string old_uvl_path = uvls_initial.front();
  uint64_t old_uvl_number = 0;
  {
    const std::string basename =
        old_uvl_path.substr(old_uvl_path.find_last_of('/') + 1);
    FileType ft = kInfoLogFile;
    ASSERT_TRUE(ParseFileName(basename, &old_uvl_number, &ft));
  }

  // Delete 4/6 → invalid ratio ≈ 66% > 0.5 threshold after the
  // bottommost compaction drops them.
  constexpr int kDropped = 4;
  for (int i = 0; i < kDropped; ++i) {
    ASSERT_OK(db_->Delete(WriteOptions(), "k" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  // 18f asserts: the post-compaction hook observed the threshold
  // crossing and ran the rewriter. The old UVL should now be gone;
  // a new UVL should exist.
  EXPECT_TRUE(env_->FileExists(old_uvl_path).IsNotFound())
      << "auto-GC should have reclaimed the old UVL after compaction";
  const auto uvls_after = ListUvlFiles();
  ASSERT_EQ(uvls_after.size(), 1u)
      << "exactly one post-GC UVL (the new one) should remain";

  // Surviving keys still read correctly via CIT indirection (18d).
  for (int i = kDropped; i < kN; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got));
    EXPECT_EQ(got, values[i]);
  }
}

// ITEM-18f: stays below threshold → no auto-GC.
TEST_F(DedupFlushPathTest, AutoGcSkippedWhenBelowThreshold) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.dedupkv.uvl_gc_threshold = 0.9;  // 90% — sparse deletes won't trip
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  constexpr int kN = 10;
  for (int i = 0; i < kN; ++i) {
    std::string v(128, static_cast<char>('a' + i));
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), v));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  const auto uvls_initial = ListUvlFiles();
  ASSERT_EQ(uvls_initial.size(), 1u);
  const std::string old_uvl_path = uvls_initial.front();

  // Drop just 1 of 10 — invalid ratio ~10%, below 90% threshold.
  ASSERT_OK(db_->Delete(WriteOptions(), "k0"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  // Old UVL must still be on disk — threshold wasn't met.
  ASSERT_OK(env_->FileExists(old_uvl_path));
  EXPECT_EQ(ListUvlFiles().size(), 1u);
}

// ITEM-18e: after GC on a UVL file containing only V2 large-branch
// records, the old UVL file should be reclaimed from disk. Readers
// (18d) route through CIT to the new file, so the old bytes are
// physically unneeded. A file containing any small-branch (LZ4-
// inline) record still has SST BlobIndex entries that fall back to
// its `{file, offset}`, so that workload intentionally keeps the old
// file on disk.
TEST_F(DedupFlushPathTest, V2LargeBranchOnlyGcReclaimsOldFile) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;  // all 128-byte values → large
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  constexpr int kN = 4;
  std::vector<std::string> values;
  for (int i = 0; i < kN; ++i) {
    values.emplace_back(128, static_cast<char>('a' + i));
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  const auto uvls_initial = ListUvlFiles();
  ASSERT_EQ(uvls_initial.size(), 1u);
  const std::string old_uvl_path = uvls_initial.front();
  uint64_t old_uvl_number = 0;
  {
    const std::string basename =
        old_uvl_path.substr(old_uvl_path.find_last_of('/') + 1);
    FileType ft = kInfoLogFile;
    ASSERT_TRUE(ParseFileName(basename, &old_uvl_number, &ft));
  }

  // Drop half the records so some refcount-0 invalidation exists in
  // the old file (not strictly required for reclamation — GC would
  // still copy the live ones — but mirrors the paper's workload).
  for (int i = 0; i < kN / 2; ++i) {
    ASSERT_OK(db_->Delete(WriteOptions(), "k" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  // ITEM-18f auto-GC fires inside CompactRange; old UVL is reclaimed.
  EXPECT_TRUE(env_->FileExists(old_uvl_path).IsNotFound())
      << "old UVL should be deleted after 18e reclamation via auto-GC";

  // Surviving Gets still work — 18d CIT indirection routes them to
  // the new UVL.
  for (int i = kN / 2; i < kN; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got));
    EXPECT_EQ(got, values[i]);
  }
}

// ITEM-18e: a UVL that contains small-branch (LZ4-inline) records
// must NOT be reclaimed — small-branch SST BlobIndex entries fall
// back to `{file, offset}` on Get and would break if the file went
// away. This test exercises the "keep the old file" branch.
TEST_F(DedupFlushPathTest, V2GcKeepsFileWhenSmallBranchPresent) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  // Threshold 64 → 32-byte value is small-branch, 128-byte is large.
  options.dedupkv.chunk_threshold_bytes = 64;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  ASSERT_OK(db_->Put(WriteOptions(), "big", std::string(128, 'B')));
  ASSERT_OK(db_->Put(WriteOptions(), "sml", std::string(32, 's')));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const auto uvls_initial = ListUvlFiles();
  ASSERT_EQ(uvls_initial.size(), 1u);
  const std::string old_uvl_path = uvls_initial.front();
  uint64_t old_uvl_number = 0;
  {
    const std::string basename =
        old_uvl_path.substr(old_uvl_path.find_last_of('/') + 1);
    FileType ft = kInfoLogFile;
    ASSERT_TRUE(ParseFileName(basename, &old_uvl_number, &ft));
  }

  ASSERT_OK(db_->Delete(WriteOptions(), "big"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  DBImpl* impl = static_cast<DBImpl*>(db_.get());
  uint64_t new_uvl_number = 0;
  ASSERT_OK(impl->TriggerUvlGcForTest(/*cf_id=*/0, old_uvl_number,
                                      &new_uvl_number));

  // Old UVL still exists because `sml` is a small-branch record whose
  // SST entry still points at `(old_uvl_number, offset)`.
  ASSERT_OK(env_->FileExists(old_uvl_path));

  // Small-branch Get still works.
  std::string got;
  ASSERT_OK(db_->Get(ReadOptions(), "sml", &got));
  EXPECT_EQ(got, std::string(32, 's'));
}

// ITEM-18d: V2 compaction drop must use the BlobIndex's embedded
// fingerprint and skip opening the old UVL file. Proof: delete the
// UVL from disk before the compaction that drops its reference and
// assert the CIT refcount still goes to zero. Under 18c behavior the
// drop would fail silently (file-open in MaybeDecrementDedupRefImpl
// returns early, refcount stays at 1).
TEST_F(DedupFlushPathTest, V2CompactionDropUsesEmbeddedFp) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  const std::string kKey = "k-v2-drop";
  const std::string kVal(128, 'V');
  auto fp = [&]() {
    Sha1Digest d = Sha1Hash(reinterpret_cast<const uint8_t*>(kVal.data()),
                            kVal.size());
    UvlFingerprint f{};
    std::memcpy(f.data(), d.data(), kUvlFingerprintSize);
    return f;
  }();

  ASSERT_OK(db_->Put(WriteOptions(), kKey, kVal));
  ASSERT_OK(db_->Flush(FlushOptions()));
  {
    CITEntry e;
    ASSERT_TRUE(ctx->cit->Lookup(fp, &e, /*touch_lru=*/false));
    EXPECT_EQ(e.refcount, 1u);
  }

  // Delete the UVL file from disk before triggering the compaction
  // that will drop the kDedupKVUvl entry. Under 18d's V2 path the
  // compaction reads fp from the BlobIndex itself and never touches
  // the UVL, so the drop still decrements CIT.
  const auto uvl_paths = ListUvlFiles();
  ASSERT_EQ(uvl_paths.size(), 1u);
  ASSERT_OK(env_->DeleteFile(uvl_paths.front()));

  ASSERT_OK(db_->Delete(WriteOptions(), kKey));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  CITEntry e;
  ASSERT_TRUE(ctx->cit->Lookup(fp, &e, /*touch_lru=*/false));
  EXPECT_EQ(e.refcount, 0u)
      << "V2 entry's compaction drop must decrement via embedded fp"
         " without opening the UVL";
}

// ITEM-18d: after a GC rewrite retargets CIT to a new UVL, Get for
// surviving keys reads from the NEW UVL via CIT — not the old one
// the SST BlobIndex still names. Proof: delete the old UVL after GC;
// Get must still succeed because the V2 path consults CIT.
TEST_F(DedupFlushPathTest, V2GetAfterGcConsultsCit) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);

  // Write 4 unique keys → 1 UVL with 4 records.
  constexpr int kN = 4;
  std::vector<std::string> values;
  for (int i = 0; i < kN; ++i) {
    values.emplace_back(128, static_cast<char>('a' + i));
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i), values[i]));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  const auto uvls_initial = ListUvlFiles();
  ASSERT_EQ(uvls_initial.size(), 1u);
  const std::string old_uvl_path = uvls_initial.front();
  uint64_t old_uvl_number = 0;
  {
    const std::string basename =
        old_uvl_path.substr(old_uvl_path.find_last_of('/') + 1);
    FileType ft = kInfoLogFile;
    ASSERT_TRUE(ParseFileName(basename, &old_uvl_number, &ft));
  }

  // Invalidate 2 records → ratio 50%.
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(db_->Delete(WriteOptions(), "k" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, /*begin=*/nullptr, /*end=*/nullptr));

  // ITEM-18f auto-GC + ITEM-18e reclamation: CIT now points the 2
  // surviving fps at a new UVL and the old UVL is gone from disk.
  // 18d's Get path routes through CIT; that's the invariant under test.
  EXPECT_TRUE(env_->FileExists(old_uvl_path).IsNotFound())
      << "auto-GC should have reclaimed the old UVL";

  for (int i = 2; i < kN; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), "k" + std::to_string(i), &got));
    EXPECT_EQ(got, values[i])
        << "Get for surviving key must route through CIT to the new UVL";
  }
}

// ITEM-20: capture OnDedupOperation callbacks for test assertions.
class CapturingDedupListener : public EventListener {
 public:
  void OnDedupOperation(DB* /*db*/, const DedupOpInfo& info) override {
    std::lock_guard<std::mutex> lk(mu_);
    events_.push_back(info);
  }
  std::vector<DedupOpInfo> Events() {
    std::lock_guard<std::mutex> lk(mu_);
    return events_;
  }

 private:
  std::mutex mu_;
  std::vector<DedupOpInfo> events_;
};

TEST_F(DedupFlushPathTest, StatsAndListenerFireOnInlineFlush) {
  // ITEM-20: when dedup is enabled, an inline flush ticks DEDUPKV_*
  // counters and fires OnDedupOperation. When dedup is disabled, the
  // tickers stay zero (zero-cost-when-off invariant).
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kInlineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;  // force large-branch
  options.statistics = CreateDBStatistics();
  auto listener = std::make_shared<CapturingDedupListener>();
  options.listeners.push_back(listener);
  ASSERT_OK(TryReopen(options));

  // Three unique values, two duplicates of each → 6 inline ops, 3
  // large-branch hits, 3 misses. Small-branch (lz4) ops never trigger
  // because all values are above chunk_threshold.
  std::vector<std::string> values = {std::string(64, 'A'),
                                     std::string(64, 'B'),
                                     std::string(64, 'C')};
  int kid = 0;
  for (int copy = 0; copy < 2; ++copy) {
    for (const auto& v : values) {
      ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(kid++), v));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  Statistics* stats = options.statistics.get();
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_INLINE_OPS), 6u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_DUPLICATE_HITS), 3u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_DUPLICATE_MISSES), 3u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_OFFLINE_OPS), 0u);
  EXPECT_GT(stats->getTickerCount(DEDUPKV_UVL_BYTES_WRITTEN), 0u);

  auto events = listener->Events();
  ASSERT_EQ(events.size(), 1u);
  EXPECT_EQ(events[0].mode, DedupOperationType::kInlineFlush);
  EXPECT_EQ(events[0].cf_id, 0u);
  EXPECT_EQ(events[0].keys_processed, 6u);
  EXPECT_GT(events[0].uvl_bytes_appended, 0u);
  EXPECT_NE(events[0].uvl_file_number, 0u);
  EXPECT_TRUE(events[0].status.ok());
}

TEST_F(DedupFlushPathTest, StatsStayZeroWhenDedupDisabled) {
  // The DedupKV counters must remain zero when no CF has dedupkv
  // enabled — the on-by-default tickers/histograms invariant.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  ASSERT_OK(TryReopen(options));

  for (int i = 0; i < 16; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(128, 'v')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  Statistics* stats = options.statistics.get();
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_INLINE_OPS), 0u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_OFFLINE_OPS), 0u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_DUPLICATE_HITS), 0u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_DUPLICATE_MISSES), 0u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_UVL_BYTES_WRITTEN), 0u);
}

TEST_F(DedupFlushPathTest, ListenerFiresOnOfflineDrain) {
  // ITEM-20 + ITEM-15b: an offline-only flush hands off to the worker;
  // when the worker drains, OnDedupOperation must fire with
  // mode=kOfflineDrain and DEDUPKV_OFFLINE_OPS must tick.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.dedupkv.enable = true;
  options.dedupkv.mode = DedupMode::kOfflineOnly;
  options.dedupkv.chunk_threshold_bytes = 8;
  options.statistics = CreateDBStatistics();
  auto listener = std::make_shared<CapturingDedupListener>();
  options.listeners.push_back(listener);
  ASSERT_OK(TryReopen(options));

  for (int i = 0; i < 12; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "k" + std::to_string(i),
                       std::string(64, static_cast<char>('a' + (i % 4)))));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Wait (up to 5s) for the offline worker to drain.
  auto ctx = GetDedupCtx(0);
  ASSERT_NE(ctx, nullptr);
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(5);
  while (ctx->dwq->Size() > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_EQ(ctx->dwq->Size(), 0u);

  Statistics* stats = options.statistics.get();
  EXPECT_GT(stats->getTickerCount(DEDUPKV_OFFLINE_OPS), 0u);
  EXPECT_EQ(stats->getTickerCount(DEDUPKV_INLINE_OPS), 0u);

  // Listener fired at least once with kOfflineDrain mode; some
  // DBImpl::Open paths can also fire one event from the inline
  // recovery flush, so check ≥1 offline event rather than exactly one.
  auto events = listener->Events();
  bool saw_offline = false;
  for (const auto& e : events) {
    if (e.mode == DedupOperationType::kOfflineDrain) {
      saw_offline = true;
      EXPECT_EQ(e.cf_id, 0u);
      EXPECT_GT(e.keys_processed, 0u);
      EXPECT_NE(e.uvl_file_number, 0u);
      EXPECT_TRUE(e.status.ok());
    }
  }
  EXPECT_TRUE(saw_offline) << "expected ≥1 OnDedupOperation(kOfflineDrain)";
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
