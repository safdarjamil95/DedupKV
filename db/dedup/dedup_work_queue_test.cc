//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/dedup_work_queue.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "table/block_based/filter_policy_internal.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Deterministic test filter: matches exactly the set of keys it was
// constructed with. Not a real Bloom filter — but the DWQ only cares
// about the FilterBitsReader::MayMatch contract, so an exact matcher
// is both stricter than a real filter (no false positives) and
// cheaper to reason about in tests.
class ExactSetFilter : public FilterBitsReader {
 public:
  explicit ExactSetFilter(std::unordered_set<std::string> keys)
      : keys_(std::move(keys)) {}
  using FilterBitsReader::MayMatch;  // keep base's multi-key overload visible
  bool MayMatch(const Slice& entry) override {
    return keys_.find(entry.ToString()) != keys_.end();
  }

 private:
  std::unordered_set<std::string> keys_;
};

std::shared_ptr<DWQEntry> MakeEntry(uint64_t wal_number, uint32_t cf_id,
                                    std::unordered_set<std::string> keys) {
  auto reader = std::make_unique<ExactSetFilter>(std::move(keys));
  return std::make_shared<DWQEntry>(wal_number, cf_id, /*filter_backing=*/"",
                                    std::move(reader));
}

}  // namespace

class DWQTest : public testing::Test {};

TEST_F(DWQTest, PushPeekPopFifoOrder) {
  DWQ q;
  EXPECT_EQ(q.Size(), 0U);
  EXPECT_EQ(q.PeekHead(), nullptr);
  EXPECT_EQ(q.PopReady(), nullptr);

  auto e1 = MakeEntry(101, 0, {"a"});
  auto e2 = MakeEntry(102, 0, {"b"});
  auto e3 = MakeEntry(103, 0, {"c"});
  q.Push(e1);
  q.Push(e2);
  q.Push(e3);
  EXPECT_EQ(q.Size(), 3U);

  EXPECT_EQ(q.PeekHead()->wal_file_number(), 101U);
  EXPECT_EQ(q.PopReady()->wal_file_number(), 101U);
  EXPECT_EQ(q.PopReady()->wal_file_number(), 102U);
  EXPECT_EQ(q.PopReady()->wal_file_number(), 103U);
  EXPECT_EQ(q.PopReady(), nullptr);
  EXPECT_EQ(q.Size(), 0U);
}

TEST_F(DWQTest, StateTransitionMonotonic) {
  DWQEntry e(1, 0, "", std::make_unique<ExactSetFilter>(
                           std::unordered_set<std::string>{}));
  EXPECT_EQ(e.GetState(), DWQEntry::State::kInactive);

  EXPECT_TRUE(e.TransitionTo(DWQEntry::State::kActive));
  EXPECT_EQ(e.GetState(), DWQEntry::State::kActive);

  // Backward transitions rejected.
  EXPECT_FALSE(e.TransitionTo(DWQEntry::State::kInactive));
  // Equal-state transitions rejected.
  EXPECT_FALSE(e.TransitionTo(DWQEntry::State::kActive));

  EXPECT_TRUE(e.TransitionTo(DWQEntry::State::kComplete));
  EXPECT_EQ(e.GetState(), DWQEntry::State::kComplete);
  EXPECT_FALSE(e.TransitionTo(DWQEntry::State::kActive));
}

TEST_F(DWQTest, KeyMightBePresentVisitsAllEntriesInFifoOrder) {
  DWQ q;
  q.Push(MakeEntry(1, 0, {"alpha", "beta"}));
  q.Push(MakeEntry(2, 0, {"gamma"}));
  q.Push(MakeEntry(3, 0, {"alpha", "delta"}));

  std::vector<std::shared_ptr<DWQEntry>> hits;
  EXPECT_TRUE(q.KeyMightBePresent(Slice("alpha"), &hits));
  ASSERT_EQ(hits.size(), 2U);
  EXPECT_EQ(hits[0]->wal_file_number(), 1U);
  EXPECT_EQ(hits[1]->wal_file_number(), 3U);

  hits.clear();
  EXPECT_FALSE(q.KeyMightBePresent(Slice("not-present"), &hits));
  EXPECT_TRUE(hits.empty());

  hits.clear();
  EXPECT_TRUE(q.KeyMightBePresent(Slice("gamma"), &hits));
  ASSERT_EQ(hits.size(), 1U);
  EXPECT_EQ(hits[0]->wal_file_number(), 2U);
}

TEST_F(DWQTest, EntryStaysAliveAfterPop) {
  DWQ q;
  auto entry = MakeEntry(42, 0, {"k"});
  std::weak_ptr<DWQEntry> weak = entry;
  q.Push(std::move(entry));

  // A "Get path" snapshot grabs the entry shared_ptr before the
  // consumer removes it from the queue.
  std::vector<std::shared_ptr<DWQEntry>> get_path_hits;
  ASSERT_TRUE(q.KeyMightBePresent(Slice("k"), &get_path_hits));
  ASSERT_EQ(get_path_hits.size(), 1U);

  // Consumer processes and pops — does not free the entry because Get
  // path still holds a reference.
  auto popped = q.PopReady();
  ASSERT_NE(popped, nullptr);
  ASSERT_TRUE(popped->TransitionTo(DWQEntry::State::kActive));
  ASSERT_TRUE(popped->TransitionTo(DWQEntry::State::kComplete));
  popped.reset();

  EXPECT_FALSE(weak.expired());  // get_path_hits keeps it alive
  EXPECT_EQ(get_path_hits[0]->GetState(), DWQEntry::State::kComplete);

  get_path_hits.clear();
  EXPECT_TRUE(weak.expired());
}

TEST_F(DWQTest, ClearEmptiesTheQueue) {
  DWQ q;
  q.Push(MakeEntry(1, 0, {"a"}));
  q.Push(MakeEntry(2, 0, {"b"}));
  q.Clear();
  EXPECT_EQ(q.Size(), 0U);
  EXPECT_EQ(q.PopReady(), nullptr);
}

TEST_F(DWQTest, WaitForHeadWakesOnPush) {
  DWQ q;
  std::atomic<bool> returned{false};
  std::shared_ptr<DWQEntry> got;
  std::thread consumer([&] {
    got = q.WaitForHead();
    returned.store(true, std::memory_order_release);
  });
  // Small spin to let consumer reach cv_.wait without using sleep.
  while (q.Size() != 0 || !returned.load()) {
    if (!returned.load()) {
      q.Push(MakeEntry(7, 0, {"k"}));
      break;
    }
  }
  consumer.join();
  ASSERT_NE(got, nullptr);
  EXPECT_EQ(got->wal_file_number(), 7U);
  // Head is still in queue (WaitForHead does not pop).
  EXPECT_EQ(q.Size(), 1U);
}

TEST_F(DWQTest, ShutdownReleasesWaiters) {
  DWQ q;
  std::atomic<bool> returned{false};
  std::shared_ptr<DWQEntry> got;
  std::thread consumer([&] {
    got = q.WaitForHead();
    returned.store(true, std::memory_order_release);
  });
  q.Shutdown();
  consumer.join();
  EXPECT_TRUE(returned.load());
  EXPECT_EQ(got, nullptr);
}

TEST_F(DWQTest, ConcurrentProducerConsumer) {
  DWQ q;
  constexpr int kNumItems = 256;

  std::thread producer([&] {
    for (int i = 0; i < kNumItems; ++i) {
      q.Push(MakeEntry(static_cast<uint64_t>(i + 1), 0, {std::to_string(i)}));
    }
  });

  std::vector<uint64_t> consumed;
  std::thread consumer([&] {
    for (int i = 0; i < kNumItems; ++i) {
      // Busy-poll the head; in production the offline thread uses
      // WaitForHead, but for this test we exercise PeekHead/PopReady
      // under contention.
      std::shared_ptr<DWQEntry> head;
      while ((head = q.PopReady()) == nullptr) {
      }
      consumed.push_back(head->wal_file_number());
    }
  });

  producer.join();
  consumer.join();
  ASSERT_EQ(consumed.size(), static_cast<size_t>(kNumItems));
  // FIFO preserved: consumed[i] == i+1.
  for (int i = 0; i < kNumItems; ++i) {
    EXPECT_EQ(consumed[i], static_cast<uint64_t>(i + 1));
  }
  EXPECT_EQ(q.Size(), 0U);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
