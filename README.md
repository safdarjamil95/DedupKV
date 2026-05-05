# DedupKV

DedupKV is a RocksDB-derived key-value store that adds value deduplication
inside the flush, read, compaction, recovery, and evaluation paths. It is
intended for duplicate-heavy workloads where many logical keys share identical
or highly compressible values.

DedupKV preserves RocksDB's public API shape and storage-engine foundation,
while adding a Unique Value Log (UVL), content-index metadata, duplicate
detection, offline drain support, and UVL garbage collection.

## Highlights

- Inline, offline, and elastic deduplication modes.
- UVL-backed value storage using `.uvl` files.
- Content Index Table (CIT) for fingerprint-to-value-location lookup.
- Duplicate Generation Detector (DGD) for SHA1-based large-value deduplication
  and LZ4-backed small-value handling.
- Dedup Work Queue (DWQ) and offline worker for background deduplication.
- Read-path support for UVL indirection and DWQ/WAL redirection.
- Compaction-time reference-count maintenance and UVL garbage collection.
- DedupKV counters, histograms, and `EventListener` hooks.
- `db_bench`, `db_stress`, and `dedupkv_driver` support for evaluation.

## Relationship To RocksDB

DedupKV is derived from RocksDB and keeps RocksDB's original licensing,
copyright headers, and attribution. The original RocksDB project is developed
by Meta/Facebook and is available at:

https://github.com/facebook/rocksdb

This repository should be treated as a research/experimental fork unless and
until you have independently validated it for your production workload.

## License

The RocksDB-derived source remains available under RocksDB's original
dual-license terms: GPLv2 or Apache License 2.0, at your option. See
`COPYING`, `LICENSE.Apache`, `LICENSE.leveldb`, and `NOTICE`.

DedupKV-specific modifications are intended to be distributed under the same
license terms unless a future release states otherwise.

## Building

DedupKV uses the RocksDB build system.

Use a compiler with C++20 support. On macOS, older Command Line Tools releases
can reject `-std=c++20`; install a newer Xcode/Command Line Tools or use a
modern LLVM/GCC toolchain.

```bash
make static_lib -j$(nproc)
```

On macOS, use the number of hardware threads reported by `sysctl`:

```bash
make static_lib -j$(sysctl -n hw.ncpu)
```

To build the dedicated evaluation driver:

```bash
make dedupkv_driver -j$(nproc)
```

## Basic Usage

Enable DedupKV through `ColumnFamilyOptions::dedupkv`:

```cpp
#include "rocksdb/db.h"
#include "rocksdb/options.h"

rocksdb::Options options;
options.create_if_missing = true;
options.dedupkv.enable = true;
options.dedupkv.mode = rocksdb::DedupMode::kElastic;
options.dedupkv.chunk_threshold_bytes = 64;
options.dedupkv.memory_threshold_pct = 0.80;
options.dedupkv.uvl_gc_threshold = 0.50;

rocksdb::DB* db = nullptr;
rocksdb::Status s = rocksdb::DB::Open(options, "/tmp/dedupkv-demo", &db);
```

DedupKV disables incompatible native blob-file behavior on DedupKV-enabled
column families.

## Benchmarking

`db_bench` exposes DedupKV flags:

```bash
./db_bench \
  --benchmarks=fillrandom,readrandom \
  --num=100000 \
  --value_size=256 \
  --same_value_percentage=50 \
  --enable_dedupkv=true \
  --dedup_mode=elastic \
  --dedup_chunk_threshold_bytes=64 \
  --dedup_memory_threshold_pct=0.80 \
  --dedup_uvl_gc_threshold=0.50
```

`db_stress` also supports DedupKV:

```bash
./db_stress \
  --enable_dedupkv=true \
  --dedup_mode=elastic \
  --dedup_chunk_threshold_bytes=64 \
  --dedup_memory_threshold_pct=0.80
```

For dataset replay and YCSB-style workloads, build and run
`tools/dedupkv_driver/dedupkv_driver.cc`:

```bash
make dedupkv_driver -j$(nproc)

./dedupkv_driver \
  --db=/tmp/dedupkv-driver \
  --dataset=/path/to/key_value_pairs.tsv \
  --enable_dedupkv=true \
  --dedup_mode=elastic
```

The dataset format is tab-separated `key<TAB>value` pairs.

## Documentation

See `docs/dedupkv.md` for a concise design overview and verification notes.
The DedupKV manuscript is available at:
https://dl.acm.org/doi/10.1145/3721145.3730424

## Current Status

The implementation includes the core DedupKV mechanisms, integration tests,
benchmark flags, stress-test flags, and a dataset/YCSB-style driver. The code
is still a research fork; review the tests and run your own workloads before
using it as a durable storage dependency.
