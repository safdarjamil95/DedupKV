//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/memory_monitor.h"

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class DedupMemoryMonitorTest : public testing::Test {};

TEST_F(DedupMemoryMonitorTest, ZeroCapacityReportsZeroUtilization) {
  DedupMemoryMonitor m(/*capacity_bytes=*/0);
  EXPECT_EQ(m.Utilization(), 0.0);
  // With zero capacity, even the typical 80% threshold should not fire
  // regardless of alloc volume.
  EXPECT_FALSE(m.Over(0.80));
  m.OnMemtableAlloc(1024);
  EXPECT_EQ(m.Utilization(), 0.0);  // still zero — capacity is zero
  EXPECT_FALSE(m.Over(0.80));
}

TEST_F(DedupMemoryMonitorTest, AllocFreeTracksBytes) {
  constexpr uint64_t kCap = 1000;
  DedupMemoryMonitor m(kCap);
  m.OnMemtableAlloc(200);
  EXPECT_EQ(m.MemtableBytes(), 200U);
  EXPECT_DOUBLE_EQ(m.Utilization(), 0.2);
  m.OnMemtableAlloc(500);
  EXPECT_DOUBLE_EQ(m.Utilization(), 0.7);
  m.OnMemtableFree(200);
  EXPECT_DOUBLE_EQ(m.Utilization(), 0.5);
  EXPECT_EQ(m.TotalAllocated(), 700U);
  EXPECT_EQ(m.TotalFreed(), 200U);
}

TEST_F(DedupMemoryMonitorTest, OverThresholdFiresAt80pct) {
  constexpr uint64_t kCap = 1000;
  DedupMemoryMonitor m(kCap);
  m.OnMemtableAlloc(799);
  EXPECT_FALSE(m.Over(0.80));
  m.OnMemtableAlloc(1);
  EXPECT_TRUE(m.Over(0.80));  // exactly at the threshold counts as over
}

TEST_F(DedupMemoryMonitorTest, UtilizationCanExceedOne) {
  constexpr uint64_t kCap = 100;
  DedupMemoryMonitor m(kCap);
  m.OnMemtableAlloc(150);
  EXPECT_GT(m.Utilization(), 1.0);
  EXPECT_TRUE(m.Over(1.0));
}

TEST_F(DedupMemoryMonitorTest, FreeSaturatesAtZero) {
  DedupMemoryMonitor m(1000);
  m.OnMemtableAlloc(100);
  // Double-free: request to release more than outstanding.
  m.OnMemtableFree(500);
  EXPECT_EQ(m.MemtableBytes(), 0U);
  // Subsequent alloc/free still works from zero.
  m.OnMemtableAlloc(50);
  EXPECT_EQ(m.MemtableBytes(), 50U);
  m.OnMemtableFree(50);
  EXPECT_EQ(m.MemtableBytes(), 0U);
}

TEST_F(DedupMemoryMonitorTest, SetCapacityUpdatesDenominator) {
  DedupMemoryMonitor m(1000);
  m.OnMemtableAlloc(500);
  EXPECT_DOUBLE_EQ(m.Utilization(), 0.5);
  m.SetCapacity(2000);
  EXPECT_DOUBLE_EQ(m.Utilization(), 0.25);
  m.SetCapacity(0);
  EXPECT_EQ(m.Utilization(), 0.0);
}

TEST_F(DedupMemoryMonitorTest, ConcurrentAllocFreeNoDrift) {
  constexpr int kThreads = 8;
  constexpr int kIters = 2000;
  constexpr uint64_t kPerOp = 64;
  DedupMemoryMonitor m(kThreads * kIters * kPerOp);

  std::atomic<bool> start{false};
  std::vector<std::thread> workers;
  for (int t = 0; t < kThreads; ++t) {
    workers.emplace_back([&]() {
      while (!start.load(std::memory_order_acquire)) {
      }
      for (int i = 0; i < kIters; ++i) {
        m.OnMemtableAlloc(kPerOp);
      }
      for (int i = 0; i < kIters; ++i) {
        m.OnMemtableFree(kPerOp);
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& w : workers) w.join();

  EXPECT_EQ(m.MemtableBytes(), 0U);
  EXPECT_EQ(m.TotalAllocated(),
            static_cast<uint64_t>(kThreads) * kIters * kPerOp);
  EXPECT_EQ(m.TotalFreed(),
            static_cast<uint64_t>(kThreads) * kIters * kPerOp);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
