//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/dedup/offline_dedup_worker.h"

#include <cinttypes>
#include <memory>
#include <utility>
#include <vector>

#include "db/dedup/dedup_work_queue.h"
#include "db/dedup/dgd.h"
#include "db/dedup/offline_dedup_wal_decoder.h"
#include "db/dedup/uvl_file_builder.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/statistics.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Open a fresh UVL file at `uvl_dir + "/" + UvlFileName(number)` and
// wrap it in a UvlFileBuilder (header already written). Returns the
// full path via *out_path.
Status OpenUvlForWorker(FileSystem* fs, SystemClock* clock,
                        const std::string& uvl_dir, uint64_t uvl_number,
                        uint32_t cf_id,
                        std::unique_ptr<UvlFileBuilder>* out_builder,
                        std::string* out_path) {
  *out_path = UvlFileName(uvl_dir, uvl_number);
  std::unique_ptr<FSWritableFile> fs_file;
  FileOptions fo;
  Status s = NewWritableFile(fs, *out_path, &fs_file, fo);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<WritableFileWriter> writer(new WritableFileWriter(
      std::move(fs_file), *out_path, fo, clock));
  uint64_t creation_time = 0;
  if (clock) {
    int64_t now = 0;
    if (clock->GetCurrentTime(&now).ok()) {
      creation_time = static_cast<uint64_t>(now);
    }
  }
  auto b = std::make_unique<UvlFileBuilder>(std::move(writer), uvl_number,
                                            cf_id, creation_time);
  s = b->Open();
  if (!s.ok()) {
    return s;
  }
  *out_builder = std::move(b);
  return Status::OK();
}

}  // namespace

OfflineDedupWorker::OfflineDedupWorker(Options opts) : opts_(std::move(opts)) {}

OfflineDedupWorker::~OfflineDedupWorker() { Stop(); }

void OfflineDedupWorker::Start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    return;  // already started
  }
  thread_ = std::thread([this] { Run(); });
}

void OfflineDedupWorker::Stop() {
  if (!started_.load()) {
    return;
  }
  bool expected = false;
  if (!stopped_.compare_exchange_strong(expected, true)) {
    return;  // already stopped
  }
  if (opts_.dwq) {
    opts_.dwq->Shutdown();
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}

void OfflineDedupWorker::Run() {
  while (!stopped_.load(std::memory_order_relaxed)) {
    std::shared_ptr<DWQEntry> entry = opts_.dwq->WaitForHead();
    if (entry == nullptr) {
      // Queue was shut down.
      break;
    }
    // Another worker (there shouldn't be one under the
    // AMBIGUITY-006 single-consumer invariant) or a stale wakeup
    // might have already processed this entry. The TransitionTo
    // below returns false in that case and we skip.
    if (!entry->TransitionTo(DWQEntry::State::kActive)) {
      // Already past Inactive — just remove from head and move on.
      opts_.dwq->PopReady();
      continue;
    }
    // ITEM-16b probe: tests install a callback here to pause the
    // worker between "entry popped, WAL still alive" and "SST
    // installed, WAL can be deleted". That's the window where a
    // Get on one of the entry's keys needs to consult DWQ+WAL
    // rather than the SST chain.
    TEST_SYNC_POINT_CALLBACK("OfflineDedupWorker::Run:BeforeProcess",
                             entry.get());
    Status s = ProcessEntry(entry);
    entry->TransitionTo(DWQEntry::State::kComplete);
    opts_.dwq->PopReady();

    // Only unlink the WAL once we've durably emitted its dedup output;
    // on failure we leave it for human triage / retry.
    if (s.ok() && opts_.delete_wal_on_success && opts_.fs) {
      const std::string wal_path =
          LogFileName(opts_.wal_dir, entry->wal_file_number());
      IOOptions io;
      Status del = opts_.fs->DeleteFile(wal_path, io, nullptr);
      del.PermitUncheckedError();
    }

    if (opts_.on_complete) {
      opts_.on_complete(entry->wal_file_number(), s);
    }
  }
}

Status OfflineDedupWorker::ProcessEntry(
    const std::shared_ptr<DWQEntry>& entry) {
  const uint64_t wal_number = entry->wal_file_number();
  const uint32_t cf_id = entry->cf_id();
  const std::string wal_path = LogFileName(opts_.wal_dir, wal_number);

  std::vector<OfflineWalRecord> records;
  OfflineDedupWalDecodeStats dec_stats;
  Status s =
      DecodeWalFileForOfflineDedup(opts_.fs, opts_.clock, wal_path, wal_number,
                                   cf_id, &records, opts_.info_log, &dec_stats);
  if (!s.ok()) {
    if (opts_.info_log) {
      ROCKS_LOG_ERROR(opts_.info_log,
                      "[DedupKV] WAL decode failed for log=%" PRIu64 ": %s",
                      wal_number, s.ToString().c_str());
    }
    return s;
  }

  if (!opts_.next_uvl_file_number) {
    return Status::InvalidArgument(
        "OfflineDedupWorker: next_uvl_file_number not configured");
  }
  const uint64_t uvl_number = opts_.next_uvl_file_number();

  std::unique_ptr<UvlFileBuilder> uvl_builder;
  std::string uvl_path;
  s = OpenUvlForWorker(opts_.fs, opts_.clock, opts_.uvl_dir, uvl_number, cf_id,
                       &uvl_builder, &uvl_path);
  if (!s.ok()) {
    return s;
  }

  DGDEncoder encoder(opts_.cit, uvl_builder.get(), opts_.chunk_threshold_bytes,
                     opts_.dgd_stats, opts_.db_statistics);

  std::unique_ptr<OfflineDedupSink> sink;
  if (opts_.sink_factory) {
    sink = opts_.sink_factory(wal_number, uvl_number);
  }
  if (!sink) {
    uvl_builder->Abandon();
    return Status::NotSupported(
        "OfflineDedupWorker: sink_factory produced null sink");
  }

  // ITEM-20: snapshot DGD counters around the drain so we can report
  // per-pass deltas to OnDedupOperation listeners. dgd_stats is the
  // shared per-CF accumulator.
  const uint64_t dgd_hits_before =
      opts_.dgd_stats
          ? opts_.dgd_stats->dedup_hits.load(std::memory_order_relaxed)
          : 0;
  const uint64_t dgd_misses_before =
      opts_.dgd_stats
          ? opts_.dgd_stats->dedup_misses.load(std::memory_order_relaxed)
          : 0;
  const uint64_t uvl_bytes_before = uvl_builder->total_bytes();

  OfflineDedupStats drain_stats;
  s = OfflineDedupDrain(records, &encoder, sink.get(), &drain_stats);
  if (!s.ok()) {
    uvl_builder->Abandon();
    // Delete the partial UVL so it doesn't leak.
    if (opts_.fs) {
      IOOptions io;
      opts_.fs->DeleteFile(uvl_path, io, nullptr).PermitUncheckedError();
    }
    return s;
  }

  Status finish_s = uvl_builder->Finish(/*sync=*/true);
  if (!finish_s.ok()) {
    sink.reset();  // ignore downstream Finish on a broken UVL
    if (opts_.fs) {
      IOOptions io;
      opts_.fs->DeleteFile(uvl_path, io, nullptr).PermitUncheckedError();
    }
    return finish_s;
  }

  // ITEM-20: count the keys that actually reached the encoder + roll up
  // tickers/listener event. `values_emitted` is the post-dedup-merge
  // count handed to the encoder; using it instead of records.size()
  // avoids over-counting WAL entries merged by tail→head dedup. Deletes
  // are emitted by the sink directly (no DGD) so they're tracked
  // separately.
  const uint64_t pass_keys = drain_stats.values_emitted;
  const uint64_t pass_hits =
      opts_.dgd_stats
          ? opts_.dgd_stats->dedup_hits.load(std::memory_order_relaxed) -
                dgd_hits_before
          : 0;
  const uint64_t pass_misses =
      opts_.dgd_stats
          ? opts_.dgd_stats->dedup_misses.load(std::memory_order_relaxed) -
                dgd_misses_before
          : 0;
  const uint64_t pass_uvl_bytes = uvl_builder->total_bytes() - uvl_bytes_before;
  RecordTick(opts_.db_statistics, DEDUPKV_OFFLINE_OPS, pass_keys);

  // Sink finalises (closes its SST / installs VersionEdit in 09b).
  s = sink->Finish();
  if (!s.ok()) {
    return s;
  }

  if (opts_.listeners != nullptr && !opts_.listeners->empty()) {
    DedupOpInfo info;
    info.db_name = opts_.db_name;
    info.cf_name = opts_.cf_name;
    info.cf_id = opts_.cf_id;
    info.mode = DedupOperationType::kOfflineDrain;
    info.job_id = 0;
    info.keys_processed = pass_keys;
    info.dedup_hits = pass_hits;
    info.dedup_misses = pass_misses;
    info.uvl_bytes_appended = pass_uvl_bytes;
    info.uvl_file_number = uvl_number;
    info.status = Status::OK();
    for (const auto& listener : *opts_.listeners) {
      if (listener) {
        listener->OnDedupOperation(opts_.db_for_listeners, info);
      }
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
