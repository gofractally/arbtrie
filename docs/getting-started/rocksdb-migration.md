# RocksDB Drop-In Migration

PsiTriRocks provides a RocksDB-compatible API backed by PsiTri. Existing applications that use `rocksdb::DB` can switch to PsiTri by relinking — no code changes required for standard operations.

## Quick Migration

**Before (RocksDB):**
```cmake
find_package(RocksDB REQUIRED)
target_link_libraries(myapp PRIVATE RocksDB::rocksdb)
```

**After (PsiTriRocks):**
```cmake
find_package(psitri REQUIRED)
target_link_libraries(myapp PRIVATE psitri::psitrirocks)
```

Your application code stays the same:

```cpp
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

rocksdb::DB* db;
rocksdb::Options options;
options.create_if_missing = true;
rocksdb::Status s = rocksdb::DB::Open(options, "/tmp/mydb", &db);

// All standard operations work unchanged
db->Put(rocksdb::WriteOptions(), "key", "value");

std::string value;
db->Get(rocksdb::ReadOptions(), "key", &value);

// Batch writes
rocksdb::WriteBatch batch;
batch.Put("key1", "val1");
batch.Put("key2", "val2");
batch.Delete("old_key");
db->Write(rocksdb::WriteOptions(), &batch);

// Iterators
auto* it = db->NewIterator(rocksdb::ReadOptions());
for (it->SeekToFirst(); it->Valid(); it->Next()) {
    // it->key(), it->value()
}
delete it;

// Snapshots
const rocksdb::Snapshot* snap = db->GetSnapshot();
rocksdb::ReadOptions ro;
ro.snapshot = snap;
db->Get(ro, "key", &value);  // reads from snapshot
db->ReleaseSnapshot(snap);

delete db;
```

## What's Supported

| API | Status | Notes |
|-----|--------|-------|
| `DB::Open` | Supported | With and without column families |
| `DB::OpenForReadOnly` | Supported | |
| `DestroyDB` | Supported | |
| `Put` / `Delete` / `SingleDelete` | Supported | With optional column family |
| `DeleteRange` | Supported | O(log n) via PsiTri's range delete |
| `Get` / `MultiGet` | Supported | `std::string` and `PinnableSlice` |
| `Write(WriteBatch)` | Supported | Applied as a single atomic transaction |
| `NewIterator` | Supported | Forward, reverse, Seek, SeekForPrev |
| `GetSnapshot` / `ReleaseSnapshot` | Supported | O(1) via COW |
| `ManagedSnapshot` | Supported | RAII wrapper |
| `Column families` | Supported | Mapped to PsiTri root indices (up to 512) |
| `GetProperty` | Supported | PsiTri-specific + RocksDB-compatible properties |
| `Flush` / `CompactRange` | Accepted | No-op (PsiTri manages this automatically) |
| `Merge` | Partial | Treated as Put (no merge operator semantics) |
| `Options` / `ReadOptions` / `WriteOptions` | Accepted | Many fields are no-ops; PsiTri self-tunes |

## What's Not Supported

These RocksDB features have no PsiTri equivalent and are silently ignored:

- **Merge operators** — `Merge()` is treated as `Put()`. Applications that depend on custom merge semantics need modification.
- **Bloom filters / block cache / table format** — PsiTri uses its own MFU cache and hash-accelerated lookups. `NewBloomFilterPolicy`, `NewLRUCache`, `NewBlockBasedTableFactory` return nullptr.
- **Statistics / perf context** — `CreateDBStatistics()` returns nullptr.
- **Compaction tuning** — Options like `level0_file_num_compaction_trigger`, `max_bytes_for_level_base`, etc. are accepted but ignored. PsiTri's compactor is self-tuning.
- **Compression** — PsiTri does not compress data. Compression options are accepted but ignored.
- **Write-ahead log** — PsiTriRocks uses PsiTri's DWAL layer, which has its own WAL format. RocksDB's WAL options (`WriteOptions::disableWAL`, `WAL_ttl_seconds`, etc.) are accepted but ignored.
- **Iterator::Refresh** — Returns `NotSupported`.

## PsiTri-Specific Properties

In addition to standard RocksDB properties, `GetProperty` supports:

| Property | Description |
|----------|-------------|
| `psitri.stats` | Detailed stats: tree shape, memory usage, segment info |
| `psitri.compact_and_truncate` | Triggers compaction and file truncation |
| `psitri.reachable-size` | Total reachable data size in bytes |
| `rocksdb.total-sst-files-size` | Total segment file size (RocksDB-compatible) |
| `rocksdb.live-sst-files-size` | Live data size (total minus freed space) |
| `rocksdb.estimate-pending-compaction-bytes` | Free space available for reclamation |

## Performance

In the [bank transaction benchmark](../benchmarks/bank.md), PsiTriRocks achieves **95% of native PsiTri performance** — the compatibility layer adds minimal overhead:

| Engine | Transfers/sec | Bulk Load |
|--------|--------------|-----------|
| PsiTri (native) | 377,197 | 2.02M ops/sec |
| **PsiTriRocks** | **356,703** | **1.80M ops/sec** |
| RocksDB | 123,153 | 2.53M ops/sec |

PsiTriRocks is **2.9x faster** than RocksDB on transactional workloads while using the same API.

## Building

PsiTriRocks is always built as part of the PsiTri project:

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release -j$(nproc)
```

The library is available as `libpsitrirocks.a` and the `psitrirocks` CMake target.

To run the RocksDB-compatible benchmark:

```bash
./build/release/bin/psitrirocks-bench --num=1000000 --benchmarks=fillrandom,readrandom,readseq
```

To compare against real RocksDB side-by-side:

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_ROCKSDB_BENCH=ON -B build/release
cmake --build build/release -j$(nproc)
cmake --build build/release --target bench-compare
```
