---
hide:
  - navigation
  - toc
---

<div class="hero-headline" markdown>
# PsiTri
</div>

<div class="hero-tagline" markdown>
A persistent, transactional key-value store with drop-in compatibility for RocksDB, MDBX, and SQLite.
Up to 46x faster than SQLite. 2.3x faster than RocksDB at scale. Node-level COW eliminates page-level write amplification.
</div>

<div class="stat-row" markdown>
<div class="stat" markdown>
<span class="stat-value">1.9M</span>
<span class="stat-label">ops/sec (bank benchmark)</span>
</div>
<div class="stat" markdown>
<span class="stat-value">46x</span>
<span class="stat-label">faster than SQLite (TATP)</span>
</div>
<div class="stat" markdown>
<span class="stat-value">2.3x</span>
<span class="stat-label">faster than RocksDB at scale</span>
</div>
<div class="stat" markdown>
<span class="stat-value">64 B</span>
<span class="stat-label">inner node COW (1 cache line)</span>
</div>
</div>

---

## Features

<div class="feature-grid" markdown>

<div class="feature-card" markdown>
### 512 Independent Tables
Each database holds up to 512 tables. Each table supports one writer and up to 64 concurrent readers -- readers never block each other or the writer.
</div>

<div class="feature-card" markdown>
### Instant Snapshots
Create a point-in-time snapshot of any table in O(1). Snapshots are free -- they share data with the live table and only diverge on writes.
</div>

<div class="feature-card" markdown>
### Nested Transactions
Transactions can spawn sub-transactions that commit back to their parent. Abort a sub-transaction without losing the parent's work.
</div>

<div class="feature-card" markdown>
### Subtrees as Values
Store an entire tree as the value of a key. Useful for hierarchical data -- user profiles with nested settings, indexes stored alongside data, or any structure that doesn't flatten well into a single key space.
</div>

<div class="feature-card" markdown>
### ART-Buffered DWAL
Writes land in an in-memory adaptive radix trie with zero locks and zero I/O. Background merge threads drain to the COW trie asynchronously. This decouples write latency from COW cost -- the writer sustains >1M ops/sec while merge amortizes node cloning in batch. [How it works :material-arrow-right:](architecture/dwal.md)
</div>

<div class="feature-card" markdown>
### Scales Beyond RAM
Databases can grow far larger than available memory. Hot data is pinned in RAM at object granularity; cold data is paged by the OS from memory-mapped files. No buffer pool tuning, no cliff -- performance degrades smoothly.
</div>

<div class="feature-card" markdown>
### O(log n) Range Counting
Count keys in any range without scanning them. The tree tracks subtree sizes, so `count_keys("a", "z")` visits only O(depth) nodes regardless of how many keys match.
</div>

<div class="feature-card" markdown>
### O(log n) Bulk Remove
Delete an entire range of keys in logarithmic time. Interior subtrees are released in O(1) -- only the two boundary paths are traversed.
</div>

<div class="feature-card" markdown>
### RocksDB Drop-in Wrapper
Migrate existing RocksDB applications with a compatibility shim. Same API, PsiTri performance underneath. [Migration guide :material-arrow-right:](getting-started/rocksdb-migration.md)
</div>

<div class="feature-card" markdown>
### MDBX Drop-in Wrapper
Migrate existing libmdbx applications with a compatibility shim. 12x faster writes than native MDBX using the same C/C++ API. [Migration guide :material-arrow-right:](getting-started/mdbx-migration.md)
</div>

<div class="feature-card" markdown>
### SQLite Drop-in Replacement
Replace SQLite's B-tree with PsiTri's DWAL -- same `sqlite3_*` API, up to 46x faster on TATP workloads. [Migration guide :material-arrow-right:](getting-started/sqlite-migration.md)
</div>

<div class="feature-card" markdown>
### DuckDB Storage Extension
Use PsiTri as a persistent storage backend for DuckDB's SQL engine. Same DuckDB API, durable storage with MVCC snapshots. [Benchmark :material-arrow-right:](benchmarks/duckdb-api.md)
</div>

</div>

---

## Quick Example

```cpp
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>

auto db  = psitri::database::create("my_database");
auto ses = db->start_write_session();

// Insert data in a transaction
auto tx = ses->start_transaction(0);
tx.upsert("user:alice", "{"balance": 1000}");
tx.upsert("user:bob",   "{"balance": 2500}");
tx.commit();  // atomic, visible to all readers instantly

// Read with snapshot isolation
auto val = tx.get<std::string>("user:alice");
```

---

## Benchmark Highlights

### Bank Transactions (TPC-B)

Each transfer performs **5 key-value operations** (2 reads + 2 updates + 1 insert) in a single atomic transaction:

| Engine | Transfers/sec | Key-Value Ops/sec | Relative |
|--------|--------------|-------------------|----------|
| **PsiTri** | **376,691** | **1,883,455** | **1.00x** |
| PsiTriRocks | 356,703 | 1,783,515 | 0.95x |
| TidesDB | 225,937 | 1,129,685 | 0.60x |
| RocksDB | 126,341 | 631,705 | 0.34x |
| MDBX | 57,138 | 285,690 | 0.15x |

[Bank benchmark details :material-arrow-right:](benchmarks/bank.md){ .md-button }

### Random Upserts at Scale (200M Keys)

Sustained random write throughput over 200 rounds. The gap widens as the dataset grows beyond RAM:

| Period | PsiTri | RocksDB | Speedup |
|--------|--------|---------|---------|
| First 30 rounds (in-RAM) | 1.59M/s | 1.16M/s | **1.37x** |
| Full 200 rounds | 1.13M/s | 713K/s | **1.58x** |
| Last 30 rounds (beyond-RAM) | 909K/s | 390K/s | **2.33x** |

MDBX hit MAP_FULL after only 20M keys (10x space amplification from page-level COW).

[Random upsert benchmark :material-arrow-right:](benchmarks/random-upsert.md){ .md-button }

### SQLite TATP (Telecom Workload)

PsiTri-SQLite replaces SQLite's btree.c with PsiTri's DWAL -- same `sqlite3_*` API:

| Configuration | PsiTri-SQLite | System SQLite | Speedup |
|---|---:|---:|---:|
| sync=off | 970,000 TPS | 21,000 TPS | **46x** |
| sync=full | 19,000 TPS | 5,000 TPS | **3.8x** |

[SQLite migration guide :material-arrow-right:](getting-started/sqlite-migration.md){ .md-button }

---

## Coming From Another Database?

PsiTri provides drop-in compatibility layers so you can migrate incrementally -- relink and benchmark before committing to a rewrite.

<div class="feature-grid" markdown>

<div class="feature-card" markdown>
### Coming from RocksDB?
Relink against `psitrirocks` -- same `rocksdb::DB` API, **2.9x faster** transactions. WriteBatch, iterators, snapshots, and column families all work. No code changes for standard operations.

[RocksDB migration :material-arrow-right:](getting-started/rocksdb-migration.md){ .md-button }
</div>

<div class="feature-card" markdown>
### Coming from MDBX / LMDB?
Relink against `psitrimdbx` -- same `mdbx_*` C API, **12x faster** writes. Three read modes let you trade freshness for throughput. Named DBIs, DUPSORT, and cursors all work.

[MDBX migration :material-arrow-right:](getting-started/mdbx-migration.md){ .md-button }
</div>

<div class="feature-card" markdown>
### Coming from SQLite?
Relink against `psitri-sqlite` -- same `sqlite3_*` API, **up to 46x faster** on TATP. Your SQL, prepared statements, and application code work unchanged. PRAGMA synchronous maps to PsiTri sync levels.

[SQLite migration :material-arrow-right:](getting-started/sqlite-migration.md){ .md-button }
</div>

<div class="feature-card" markdown>
### Using DuckDB?
PsiTri plugs in as a DuckDB storage extension -- persistent, durable storage with MVCC snapshots behind DuckDB's SQL engine. Attach a PsiTri database and your existing queries just work.

[DuckDB benchmark :material-arrow-right:](benchmarks/duckdb-api.md){ .md-button }
</div>

<div class="feature-card" markdown>
### Ready for the native API?
Once you've validated PsiTri with a compatibility layer, the native C++ API unlocks the full feature set: subtrees as values, O(log n) range operations, nested transactions, and 512 independent tables with zero-cost snapshots.

[API reference :material-arrow-right:](getting-started/api.md){ .md-button }
</div>

</div>

---

## How It Works

PsiTri combines three subsystems to deliver performance impossible with traditional architectures:

- **DWAL (Dynamic Write-Ahead Log)** absorbs writes in an in-memory ART buffer with sub-microsecond commits. Background merge threads drain to the COW trie in batch, amortizing node-cloning cost. The writer never blocks on I/O or COW mutations.
- **Radix/B-tree hybrid** routes on single-byte dividers with prefix compression (256-way fan-out), while sorted leaf nodes pack ~58 keys each. Inner node COW operates at 64-byte granularity -- ~9x less write amplification than page-level COW.
- **SAL (Segment Allocator Library)** provides persistent storage: memory-mapped segments, atomic reference counting, O(1) object relocation via control block indirection, and self-tuning MFU caching that physically promotes hot objects to mlocked RAM.

[Architecture overview :material-arrow-right:](architecture/overview.md){ .md-button }
[DWAL architecture :material-arrow-right:](architecture/dwal.md){ .md-button }

---

## Getting Started

```bash
git clone https://github.com/gofractally/arbtrie.git
cd arbtrie
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release -j8
```

[Quick start guide :material-arrow-right:](getting-started/quickstart.md){ .md-button }

---

## What Are the Tradeoffs?

PsiTri's copy-on-write model creates temporary duplicates reclaimed by background compaction. Compaction aggressiveness is **runtime-configurable** -- the real tradeoff is between file size and SSD wear, since compacting cold data rewrites live objects to reclaim space. The project is young and C++20 only.

These are real costs. If they matter for your workload, the compatibility layers let you benchmark PsiTri against your current engine with zero code changes before committing.

[Full tradeoffs & limitations :material-arrow-right:](why-psitri/tradeoffs.md){ .md-button }

---

## Status

Under active development. The core data structure, allocator, transaction model, and DWAL layer are working. Drop-in compatibility with RocksDB, MDBX, and SQLite APIs is functional. Multi-writer concurrency is in progress.
