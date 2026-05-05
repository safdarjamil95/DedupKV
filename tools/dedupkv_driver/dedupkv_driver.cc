//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// ITEM-22: dedupkv_driver — a small dataset-replay / YCSB-style driver
// for DedupKV evaluation.
//
// Two modes:
//
// 1. **Replay** (`--dataset=<file>`): each line is "<key>\t<value>".
//    The whole dataset is Put through the DB by `--fg_threads` worker
//    threads (round-robin over lines), then verified with a
//    deterministic Get sweep.
//
// 2. **YCSB** (`--workload=<file>`): two-phase load + run, with
//    op-mix sampled from the workload spec. Workload spec is a simple
//    key=value text format; recognised keys (lower-case):
//
//        record_count = <int>          # initial keys for load phase
//        op_count     = <int>          # ops per fg thread in run phase
//        value_size   = <int>          # bytes per value (default 1024)
//        read_pct     = <float 0..1>   # YCSB-style mix (must sum ≤ 1)
//        update_pct   = <float>
//        insert_pct   = <float>
//        scan_pct     = <float>        # iterator length 50
//        seed         = <int>          # PRNG seed (default = wallclock)
//
// Both modes write per-thread metrics to `--csv_output`, one row per
// (mode, op_type, thread) plus an aggregate row.
//
// Bg-thread setup: `--bg_threads` controls
// `Options::IncreaseParallelism` so flushes/compactions get the
// requested concurrency. Per the paper §5.1 default this is 8.
//
// Sizing matches the manuscript's YCSB-A workload by default
// (50/50 read+update). Override per-workload via the spec file.
//
// Output is intentionally minimal — column headers in CSV row 1, then
// rows — so it can be diffed across runs and consumed by a notebook.

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/gflags_compat.h"

DEFINE_string(db, "/tmp/dedupkv_driver", "Path to RocksDB database");
DEFINE_string(dataset, "",
              "If set, replay a tab-separated <key>\\t<value> dataset");
DEFINE_string(workload, "",
              "If set, run a YCSB-style load+run pass per the spec file");
DEFINE_int32(fg_threads, 8, "Number of foreground worker threads");
DEFINE_int32(bg_threads, 8,
             "Background parallelism (flushes + compactions combined)");
DEFINE_string(csv_output, "/tmp/dedupkv_driver.csv",
              "Per-thread metrics CSV path");

// DedupKV knobs (mirror db_bench / db_stress).
DEFINE_bool(enable_dedupkv, true,
            "Enable DedupKV on every column family");
DEFINE_string(dedup_mode, "elastic",
              "DedupKV mode: inline | offline | elastic");
DEFINE_uint32(dedup_chunk_threshold_bytes, 64,
              "Large-branch (SHA1+CIT) threshold");
DEFINE_double(dedup_memory_threshold_pct, 0.5,
              "Elastic memory utilisation threshold");
DEFINE_double(dedup_uvl_gc_threshold, 0.5,
              "UVL GC invalid-byte ratio");

// Common engine knobs.
DEFINE_int32(value_size, 1024, "YCSB value size in bytes");
DEFINE_uint64(write_buffer_size, 32ull << 20,
              "Per-CF MemTable size in bytes (default 32 MiB)");
DEFINE_int32(max_write_buffer_number, 2, "MemTable count per CF");

namespace ROCKSDB_NAMESPACE {

namespace {

struct ThreadStats {
  std::string label;
  std::atomic<uint64_t> ops{0};
  std::atomic<uint64_t> bytes{0};
  std::atomic<uint64_t> errors{0};
  std::vector<uint64_t> latencies_us;  // sampled per-op (cap kSampleCap)
  static constexpr size_t kSampleCap = 1u << 20;

  ThreadStats(const std::string& l) : label(l) {}

  void Add(uint64_t latency_us, uint64_t bytes_added, bool err) {
    ops.fetch_add(1, std::memory_order_relaxed);
    bytes.fetch_add(bytes_added, std::memory_order_relaxed);
    if (err) {
      errors.fetch_add(1, std::memory_order_relaxed);
    }
    if (latencies_us.size() < kSampleCap) {
      latencies_us.push_back(latency_us);
    }
  }
};

class CsvWriter {
 public:
  explicit CsvWriter(const std::string& path) : out_(path) {
    if (!out_) {
      fprintf(stderr, "Cannot open csv_output=%s\n", path.c_str());
      std::exit(1);
    }
    out_ << "phase,op,thread,ops,bytes,errors,p50_us,p99_us,p999_us,"
            "avg_us,wall_ms\n";
  }
  void WriteRow(const std::string& phase, const std::string& op,
                const std::string& thread, uint64_t ops_count,
                uint64_t bytes_count, uint64_t errors_count, uint64_t p50,
                uint64_t p99, uint64_t p999, uint64_t avg_us,
                uint64_t wall_ms) {
    std::lock_guard<std::mutex> lk(mu_);
    out_ << phase << "," << op << "," << thread << "," << ops_count << ","
         << bytes_count << "," << errors_count << "," << p50 << "," << p99
         << "," << p999 << "," << avg_us << "," << wall_ms << "\n";
  }

 private:
  std::ofstream out_;
  std::mutex mu_;
};

void Percentile(std::vector<uint64_t> latencies, uint64_t* p50, uint64_t* p99,
                uint64_t* p999, uint64_t* avg) {
  if (latencies.empty()) {
    *p50 = *p99 = *p999 = *avg = 0;
    return;
  }
  std::sort(latencies.begin(), latencies.end());
  auto pick = [&](double pct) {
    size_t idx = static_cast<size_t>(pct * (latencies.size() - 1));
    return latencies[idx];
  };
  *p50 = pick(0.50);
  *p99 = pick(0.99);
  *p999 = pick(0.999);
  uint64_t sum = 0;
  for (uint64_t v : latencies) sum += v;
  *avg = sum / latencies.size();
}

uint64_t NowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

Options BuildOptions() {
  Options options;
  options.create_if_missing = true;
  options.IncreaseParallelism(FLAGS_bg_threads);
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.max_write_buffer_number = FLAGS_max_write_buffer_number;
  options.statistics = CreateDBStatistics();

  if (FLAGS_enable_dedupkv) {
    options.dedupkv.enable = true;
    if (FLAGS_dedup_mode == "inline") {
      options.dedupkv.mode = DedupMode::kInlineOnly;
    } else if (FLAGS_dedup_mode == "offline") {
      options.dedupkv.mode = DedupMode::kOfflineOnly;
    } else if (FLAGS_dedup_mode == "elastic") {
      options.dedupkv.mode = DedupMode::kElastic;
    } else {
      fprintf(stderr, "Unknown --dedup_mode '%s'\n",
              FLAGS_dedup_mode.c_str());
      std::exit(1);
    }
    options.dedupkv.chunk_threshold_bytes = FLAGS_dedup_chunk_threshold_bytes;
    options.dedupkv.memory_threshold_pct = FLAGS_dedup_memory_threshold_pct;
    options.dedupkv.uvl_gc_threshold = FLAGS_dedup_uvl_gc_threshold;
  }
  return options;
}

// ---------------------------------------------------------------------
// Replay mode

struct ReplayWork {
  std::vector<std::pair<std::string, std::string>> kvs;
};

ReplayWork LoadDataset(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    fprintf(stderr, "Cannot open --dataset=%s\n", path.c_str());
    std::exit(1);
  }
  ReplayWork w;
  std::string line;
  while (std::getline(in, line)) {
    auto tab = line.find('\t');
    if (tab == std::string::npos) continue;
    w.kvs.emplace_back(line.substr(0, tab), line.substr(tab + 1));
  }
  return w;
}

void ReplayPutWorker(DB* db, const ReplayWork* work, int thread_id,
                     int total_threads, ThreadStats* stats) {
  WriteOptions wo;
  for (size_t i = thread_id; i < work->kvs.size();
       i += static_cast<size_t>(total_threads)) {
    const auto& kv = work->kvs[i];
    uint64_t t0 = NowMicros();
    Status s = db->Put(wo, kv.first, kv.second);
    uint64_t dt = NowMicros() - t0;
    stats->Add(dt, kv.first.size() + kv.second.size(), !s.ok());
  }
}

void ReplayGetWorker(DB* db, const ReplayWork* work, int thread_id,
                     int total_threads, ThreadStats* stats) {
  ReadOptions ro;
  std::string got;
  for (size_t i = thread_id; i < work->kvs.size();
       i += static_cast<size_t>(total_threads)) {
    const auto& kv = work->kvs[i];
    uint64_t t0 = NowMicros();
    Status s = db->Get(ro, kv.first, &got);
    uint64_t dt = NowMicros() - t0;
    bool err = !s.ok() || got != kv.second;
    stats->Add(dt, kv.first.size() + got.size(), err);
  }
}

int RunReplay(DB* db, CsvWriter* csv) {
  ReplayWork work = LoadDataset(FLAGS_dataset);
  fprintf(stderr, "Replay: loaded %zu kvs from %s\n", work.kvs.size(),
          FLAGS_dataset.c_str());
  std::vector<std::unique_ptr<ThreadStats>> put_stats;
  std::vector<std::unique_ptr<ThreadStats>> get_stats;
  put_stats.reserve(FLAGS_fg_threads);
  get_stats.reserve(FLAGS_fg_threads);
  for (int i = 0; i < FLAGS_fg_threads; ++i) {
    put_stats.push_back(std::make_unique<ThreadStats>(
        "put-" + std::to_string(i)));
    get_stats.push_back(std::make_unique<ThreadStats>(
        "get-" + std::to_string(i)));
  }

  // Put phase.
  uint64_t t0 = NowMicros();
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_fg_threads; ++i) {
    threads.emplace_back(ReplayPutWorker, db, &work, i, FLAGS_fg_threads,
                         put_stats[i].get());
  }
  for (auto& t : threads) t.join();
  uint64_t put_wall_ms = (NowMicros() - t0) / 1000;
  threads.clear();

  // Get phase.
  t0 = NowMicros();
  for (int i = 0; i < FLAGS_fg_threads; ++i) {
    threads.emplace_back(ReplayGetWorker, db, &work, i, FLAGS_fg_threads,
                         get_stats[i].get());
  }
  for (auto& t : threads) t.join();
  uint64_t get_wall_ms = (NowMicros() - t0) / 1000;

  for (auto& s : put_stats) {
    uint64_t p50, p99, p999, avg;
    Percentile(s->latencies_us, &p50, &p99, &p999, &avg);
    csv->WriteRow("replay", "put", s->label, s->ops.load(), s->bytes.load(),
                  s->errors.load(), p50, p99, p999, avg, put_wall_ms);
  }
  for (auto& s : get_stats) {
    uint64_t p50, p99, p999, avg;
    Percentile(s->latencies_us, &p50, &p99, &p999, &avg);
    csv->WriteRow("replay", "get", s->label, s->ops.load(), s->bytes.load(),
                  s->errors.load(), p50, p99, p999, avg, get_wall_ms);
  }
  uint64_t total_errors = 0;
  for (auto& s : get_stats) total_errors += s->errors.load();
  fprintf(stderr, "Replay: put_wall=%" PRIu64 "ms get_wall=%" PRIu64
                  "ms get_errors=%" PRIu64 "\n",
          put_wall_ms, get_wall_ms, total_errors);
  return total_errors == 0 ? 0 : 1;
}

// ---------------------------------------------------------------------
// YCSB-style mode

struct WorkloadSpec {
  uint64_t record_count = 100000;
  uint64_t op_count = 100000;
  uint32_t value_size = 1024;
  double read_pct = 0.5;
  double update_pct = 0.5;
  double insert_pct = 0.0;
  double scan_pct = 0.0;
  uint64_t seed = 0;
};

WorkloadSpec ParseWorkload(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    fprintf(stderr, "Cannot open --workload=%s\n", path.c_str());
    std::exit(1);
  }
  WorkloadSpec ws;
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty() || line[0] == '#') continue;
    auto eq = line.find('=');
    if (eq == std::string::npos) continue;
    std::string k = line.substr(0, eq);
    std::string v = line.substr(eq + 1);
    auto trim = [](std::string& s) {
      size_t a = s.find_first_not_of(" \t");
      size_t b = s.find_last_not_of(" \t");
      if (a == std::string::npos) {
        s.clear();
        return;
      }
      s = s.substr(a, b - a + 1);
    };
    trim(k);
    trim(v);
    if (k == "record_count")
      ws.record_count = std::stoull(v);
    else if (k == "op_count")
      ws.op_count = std::stoull(v);
    else if (k == "value_size")
      ws.value_size = static_cast<uint32_t>(std::stoul(v));
    else if (k == "read_pct")
      ws.read_pct = std::stod(v);
    else if (k == "update_pct")
      ws.update_pct = std::stod(v);
    else if (k == "insert_pct")
      ws.insert_pct = std::stod(v);
    else if (k == "scan_pct")
      ws.scan_pct = std::stod(v);
    else if (k == "seed")
      ws.seed = std::stoull(v);
  }
  if (ws.seed == 0) {
    ws.seed = static_cast<uint64_t>(NowMicros());
  }
  return ws;
}

std::string MakeKey(uint64_t i) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "user%020" PRIu64, i);
  return std::string(buf);
}

void YcsbLoadWorker(DB* db, uint64_t start, uint64_t end, uint32_t value_size,
                    uint64_t seed, ThreadStats* stats) {
  std::mt19937_64 rng(seed);
  std::string value(value_size, ' ');
  WriteOptions wo;
  for (uint64_t i = start; i < end; ++i) {
    // Half the values are duplicates of a 16-bucket pool to give the
    // dedup engine something to merge.
    uint64_t bucket = (rng() & 0xF);
    char ch = static_cast<char>('A' + (bucket % 26));
    std::fill(value.begin(), value.end(), ch);
    std::string key = MakeKey(i);
    uint64_t t0 = NowMicros();
    Status s = db->Put(wo, key, value);
    uint64_t dt = NowMicros() - t0;
    stats->Add(dt, key.size() + value.size(), !s.ok());
  }
}

void YcsbRunWorker(DB* db, uint64_t record_count, uint64_t op_count,
                   uint32_t value_size, double read_pct, double update_pct,
                   double insert_pct, double scan_pct, uint64_t seed,
                   ThreadStats* read_s, ThreadStats* update_s,
                   ThreadStats* insert_s, ThreadStats* scan_s) {
  std::mt19937_64 rng(seed);
  std::uniform_real_distribution<double> coin(0.0, 1.0);
  std::uniform_int_distribution<uint64_t> key_pick(0, record_count - 1);
  ReadOptions ro;
  WriteOptions wo;
  std::string value(value_size, ' ');
  std::string got;
  uint64_t insert_cursor = record_count;
  for (uint64_t i = 0; i < op_count; ++i) {
    double r = coin(rng);
    double cum = 0.0;
    if (r < (cum += read_pct)) {
      std::string key = MakeKey(key_pick(rng));
      uint64_t t0 = NowMicros();
      Status s = db->Get(ro, key, &got);
      uint64_t dt = NowMicros() - t0;
      bool err = !s.ok() && !s.IsNotFound();
      read_s->Add(dt, key.size() + got.size(), err);
    } else if (r < (cum += update_pct)) {
      char ch = static_cast<char>('A' + (rng() & 0xF));
      std::fill(value.begin(), value.end(), ch);
      std::string key = MakeKey(key_pick(rng));
      uint64_t t0 = NowMicros();
      Status s = db->Put(wo, key, value);
      uint64_t dt = NowMicros() - t0;
      update_s->Add(dt, key.size() + value.size(), !s.ok());
    } else if (r < (cum += insert_pct)) {
      char ch = static_cast<char>('A' + (rng() & 0xF));
      std::fill(value.begin(), value.end(), ch);
      std::string key = MakeKey(insert_cursor++);
      uint64_t t0 = NowMicros();
      Status s = db->Put(wo, key, value);
      uint64_t dt = NowMicros() - t0;
      insert_s->Add(dt, key.size() + value.size(), !s.ok());
    } else if (r < (cum += scan_pct)) {
      std::string key = MakeKey(key_pick(rng));
      uint64_t t0 = NowMicros();
      std::unique_ptr<Iterator> it(db->NewIterator(ro));
      it->Seek(key);
      uint64_t bytes = 0;
      for (int j = 0; it->Valid() && j < 50; ++j, it->Next()) {
        bytes += it->key().size() + it->value().size();
      }
      uint64_t dt = NowMicros() - t0;
      scan_s->Add(dt, bytes, !it->status().ok());
    }
    // else: idle op — do nothing.
  }
}

int RunYcsb(DB* db, CsvWriter* csv) {
  WorkloadSpec ws = ParseWorkload(FLAGS_workload);
  fprintf(stderr,
          "YCSB: record_count=%" PRIu64 " op_count=%" PRIu64
          " value_size=%u read=%.2f update=%.2f insert=%.2f scan=%.2f "
          "seed=%" PRIu64 "\n",
          ws.record_count, ws.op_count, ws.value_size, ws.read_pct,
          ws.update_pct, ws.insert_pct, ws.scan_pct, ws.seed);

  // Load phase.
  uint64_t per = ws.record_count / static_cast<uint64_t>(FLAGS_fg_threads);
  std::vector<std::unique_ptr<ThreadStats>> load_stats;
  for (int i = 0; i < FLAGS_fg_threads; ++i) {
    load_stats.push_back(std::make_unique<ThreadStats>(
        "load-" + std::to_string(i)));
  }
  uint64_t t0 = NowMicros();
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_fg_threads; ++i) {
    uint64_t start = per * i;
    uint64_t end = (i == FLAGS_fg_threads - 1) ? ws.record_count
                                                : per * (i + 1);
    threads.emplace_back(YcsbLoadWorker, db, start, end, ws.value_size,
                         ws.seed + i, load_stats[i].get());
  }
  for (auto& t : threads) t.join();
  threads.clear();
  uint64_t load_wall_ms = (NowMicros() - t0) / 1000;
  for (auto& s : load_stats) {
    uint64_t p50, p99, p999, avg;
    Percentile(s->latencies_us, &p50, &p99, &p999, &avg);
    csv->WriteRow("load", "put", s->label, s->ops.load(), s->bytes.load(),
                  s->errors.load(), p50, p99, p999, avg, load_wall_ms);
  }
  fprintf(stderr, "YCSB load: %" PRIu64 " keys in %" PRIu64 "ms\n",
          ws.record_count, load_wall_ms);

  // Run phase.
  std::vector<std::unique_ptr<ThreadStats>> read_stats, update_stats,
      insert_stats, scan_stats;
  for (int i = 0; i < FLAGS_fg_threads; ++i) {
    read_stats.push_back(std::make_unique<ThreadStats>(
        "read-" + std::to_string(i)));
    update_stats.push_back(std::make_unique<ThreadStats>(
        "update-" + std::to_string(i)));
    insert_stats.push_back(std::make_unique<ThreadStats>(
        "insert-" + std::to_string(i)));
    scan_stats.push_back(std::make_unique<ThreadStats>(
        "scan-" + std::to_string(i)));
  }
  t0 = NowMicros();
  for (int i = 0; i < FLAGS_fg_threads; ++i) {
    threads.emplace_back(YcsbRunWorker, db, ws.record_count, ws.op_count,
                         ws.value_size, ws.read_pct, ws.update_pct,
                         ws.insert_pct, ws.scan_pct, ws.seed + 1000 + i,
                         read_stats[i].get(), update_stats[i].get(),
                         insert_stats[i].get(), scan_stats[i].get());
  }
  for (auto& t : threads) t.join();
  uint64_t run_wall_ms = (NowMicros() - t0) / 1000;
  uint64_t total_run_ops = 0;
  uint64_t total_run_errors = 0;
  for (size_t i = 0; i < read_stats.size(); ++i) {
    for (auto* per_op : {read_stats[i].get(), update_stats[i].get(),
                          insert_stats[i].get(), scan_stats[i].get()}) {
      uint64_t p50, p99, p999, avg;
      Percentile(per_op->latencies_us, &p50, &p99, &p999, &avg);
      const char* op_kind = nullptr;
      if (per_op == read_stats[i].get()) op_kind = "read";
      else if (per_op == update_stats[i].get()) op_kind = "update";
      else if (per_op == insert_stats[i].get()) op_kind = "insert";
      else op_kind = "scan";
      csv->WriteRow("run", op_kind, per_op->label, per_op->ops.load(),
                    per_op->bytes.load(), per_op->errors.load(), p50, p99,
                    p999, avg, run_wall_ms);
      total_run_ops += per_op->ops.load();
      total_run_errors += per_op->errors.load();
    }
  }
  fprintf(stderr,
          "YCSB run: %" PRIu64 " ops in %" PRIu64 "ms (%.0f kops/sec); "
          "errors=%" PRIu64 "\n",
          total_run_ops, run_wall_ms,
          run_wall_ms == 0 ? 0.0
                           : static_cast<double>(total_run_ops) /
                                 (run_wall_ms / 1000.0) / 1000.0,
          total_run_errors);
  return total_run_errors == 0 ? 0 : 1;
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_dataset.empty() && FLAGS_workload.empty()) {
    fprintf(stderr,
            "dedupkv_driver: must set --dataset=<file> or --workload=<file>\n");
    return 1;
  }
  if (!FLAGS_dataset.empty() && !FLAGS_workload.empty()) {
    fprintf(stderr, "dedupkv_driver: --dataset and --workload are mutually "
                    "exclusive\n");
    return 1;
  }

  rocksdb::Options options = rocksdb::BuildOptions();
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::Status s = rocksdb::DB::Open(options, FLAGS_db, &db);
  if (!s.ok()) {
    fprintf(stderr, "DB::Open failed: %s\n", s.ToString().c_str());
    return 1;
  }
  rocksdb::CsvWriter csv(FLAGS_csv_output);
  int rc = 0;
  if (!FLAGS_dataset.empty()) {
    rc = rocksdb::RunReplay(db.get(), &csv);
  } else {
    rc = rocksdb::RunYcsb(db.get(), &csv);
  }
  fprintf(stderr, "DEDUPKV STATISTICS:\n%s\n",
          options.statistics->ToString().c_str());
  return rc;
}
