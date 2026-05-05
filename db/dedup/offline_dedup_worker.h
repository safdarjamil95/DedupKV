//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-09a: the offline dedup thread.
//
// Owns a std::thread that blocks on DWQ::WaitForHead(), picks each
// DWQEntry off the queue, decodes its WAL into OfflineWalRecords
// (WAL decoder, ITEM-09a), opens a fresh UVL file, runs
// OfflineDedupDrain (ITEM-09 pure algorithm), then transitions the
// entry Complete and removes it from the queue.
//
// The L0 SST emission / VersionSet install side is NOT in this class —
// it's abstracted behind the caller-supplied sink_factory. Tests inject
// a capturing sink; production (ITEM-09b) will inject a sink that
// feeds a TableBuilder and calls VersionSet::LogAndApply.
//
// Shutdown: Stop() sets a flag and calls dwq->Shutdown(), which
// unblocks WaitForHead so the thread can exit cleanly. The destructor
// joins if Stop() was not called explicitly.

#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "db/dedup/offline_dedup.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CIT;
class DB;
class DWQ;
class DWQEntry;
class EventListener;
class FileSystem;
class SystemClock;
class Logger;
class Statistics;
class UvlFileBuilder;
struct DGDStats;

class OfflineDedupWorker {
 public:
  // Per-WAL sink. The factory is called once per DWQEntry; the
  // returned sink is fed by OfflineDedupDrain, then its Finish()
  // method is called, then it's destroyed. A null factory result
  // aborts the drain with NotSupported.
  using SinkFactory =
      std::function<std::unique_ptr<OfflineDedupSink>(uint64_t wal_number,
                                                      uint64_t uvl_number)>;

  // Called after each DWQEntry is processed (successfully or not).
  // `wal_number` is the entry's WAL. Intended for test observability;
  // production will route this to stats / listeners.
  using CompletionCallback =
      std::function<void(uint64_t wal_number, const Status& status)>;

  struct Options {
    DWQ* dwq = nullptr;
    CIT* cit = nullptr;
    DGDStats* dgd_stats = nullptr;
    // ITEM-20: optional DB-wide Statistics + listener notification
    // context. When `db_statistics` is null no DEDUPKV_* tickers are
    // emitted; when `listeners` is null or empty no OnDedupOperation
    // callback fires. cf_id/cf_name/db_name are only used to populate
    // DedupOpInfo for the listener.
    Statistics* db_statistics = nullptr;
    const std::vector<std::shared_ptr<EventListener>>* listeners = nullptr;
    DB* db_for_listeners = nullptr;
    uint32_t cf_id = 0;
    std::string cf_name;
    std::string db_name;
    FileSystem* fs = nullptr;
    SystemClock* clock = nullptr;
    // Absolute paths for WAL lookup and UVL creation. WAL files are
    // expected at `wal_dir + "/" + LogFileName(number)`; UVLs are
    // created at `uvl_dir + "/" + UvlFileName(number)`.
    std::string wal_dir;
    std::string uvl_dir;
    uint32_t chunk_threshold_bytes = 64;
    // Allocator for fresh UVL file numbers. Wrapping DedupContext's
    // atomic counter is the expected production impl.
    std::function<uint64_t()> next_uvl_file_number;
    SinkFactory sink_factory;
    CompletionCallback on_complete;  // optional
    std::shared_ptr<Logger> info_log;
    // If true, successful drain deletes the WAL file. Defaults on for
    // production; tests can disable to inspect the WAL post-drain.
    bool delete_wal_on_success = true;
  };

  explicit OfflineDedupWorker(Options opts);
  ~OfflineDedupWorker();

  OfflineDedupWorker(const OfflineDedupWorker&) = delete;
  OfflineDedupWorker& operator=(const OfflineDedupWorker&) = delete;

  // Spawns the thread. Idempotent; second call is a no-op.
  void Start();

  // Signals shutdown and joins the thread. Idempotent.
  void Stop();

  bool IsRunning() const { return started_ && !stopped_; }

 private:
  void Run();
  Status ProcessEntry(const std::shared_ptr<DWQEntry>& entry);

  Options opts_;
  std::thread thread_;
  std::atomic<bool> started_{false};
  std::atomic<bool> stopped_{false};
};

}  // namespace ROCKSDB_NAMESPACE
