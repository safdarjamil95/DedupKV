# DedupKV Design Overview

DedupKV extends RocksDB with value deduplication for duplicate-heavy workloads.
The implementation is integrated into RocksDB's flush, read, compaction,
recovery, statistics, and benchmark paths.

## Core Components

DedupKV adds the following storage-engine components:

- UVL, the Unique Value Log, stores deduplicated value records in `.uvl` files.
- CIT, the Content Index Table, maps value fingerprints to canonical UVL
  locations and reference counts.
- DGD, the Duplicate Generation Detector, selects the large-value SHA1+CIT
  branch or the small-value LZ4-inline branch.
- DWQ, the Dedup Work Queue, tracks immutable memtables handed to the offline
  dedup worker.
- UVL garbage collection rewrites live UVL records and reclaims obsolete
  large-branch UVL files when safe.

## Execution Modes

`ColumnFamilyOptions::dedupkv.mode` controls how flushes use deduplication:

- `DedupMode::kInlineOnly`: run deduplication in the flush path.
- `DedupMode::kOfflineOnly`: enqueue flush work and drain it through the
  offline worker.
- `DedupMode::kElastic`: choose inline or offline per flush using memtable
  memory pressure.

The master switch is `ColumnFamilyOptions::dedupkv.enable`.

## Write And Flush Path

DedupKV charges memtable memory to a per-column-family monitor. During flush,
the elastic controller decides whether to run inline deduplication or enqueue
the immutable memtable to the DWQ.

Inline flushes use DGD and the CIT to encode SST values as DedupKV UVL blob
indexes. Offline flushes retain the relevant WAL, then the background worker
decodes WAL records, builds an L0 SST, writes UVL records, and installs both
through the MANIFEST.

## Read Path

Reads understand DedupKV UVL blob indexes. For installed SST entries, the read
path follows the UVL location, or the embedded fingerprint plus CIT indirection
for newer V2 entries. During the offline drain window, Get can consult DWQ/WAL
state so keys remain visible before the worker has installed its output SST.

## Compaction And Garbage Collection

Compaction decrements CIT reference counts when obsolete DedupKV values are
dropped. When a UVL file's invalid-byte ratio crosses the configured threshold,
the UVL GC rewriter copies live records to a fresh UVL, retargets CIT entries,
logs UVL garbage metadata, and reclaims old UVL files when the remaining SST
references can safely route through CIT.

## Recovery

DedupKV records UVL additions in MANIFEST edits. On open, surviving UVL files
are scanned to rebuild the in-memory CIT and UVL byte registry. DWQ-related WAL
retention ensures offline work can survive close/reopen sequences.

## Public Options

The main options are:

- `dedupkv.enable`: enables DedupKV for a column family.
- `dedupkv.mode`: inline, offline, or elastic mode.
- `dedupkv.chunk_threshold_bytes`: value-size threshold for large-value
  SHA1+CIT deduplication.
- `dedupkv.memory_threshold_pct`: elastic-mode threshold for offline handoff.
- `dedupkv.uvl_gc_threshold`: invalid-byte ratio that triggers UVL GC.

## Evaluation Tools

DedupKV is wired into:

- `db_bench` via `--enable_dedupkv`, `--dedup_mode`,
  `--dedup_chunk_threshold_bytes`, `--dedup_memory_threshold_pct`, and
  `--dedup_uvl_gc_threshold`.
- `db_stress` through matching DedupKV flags.
- `dedupkv_driver`, a dataset replay and YCSB-style workload driver.

## Verification Snapshot

The implementation includes component tests for UVL encoding/decoding, CIT,
DGD, DWQ, options, flush adapters, offline workers, UVL GC, and SHA1. It also
includes end-to-end DedupKV integration tests covering duplicate effectiveness,
elastic branch switching, offline recovery, Bloom false-positive behavior, and
close/reopen durability.

Before publishing a release, run a local build and at least the DedupKV-focused
test binaries relevant to the change.
