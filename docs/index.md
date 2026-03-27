---
hide:
  - navigation
  - toc
---

<div class="hero-headline" markdown>
# PsiTri
</div>

<div class="hero-tagline" markdown>
A persistent, transactional key-value store.
3x faster than RocksDB. ~9x less write amplification than page-level COW databases.
</div>

<div class="stat-row" markdown>
<div class="stat" markdown>
<span class="stat-value">1.9M</span>
<span class="stat-label">ops/sec (bank benchmark)</span>
</div>
<div class="stat" markdown>
<span class="stat-value">64 B</span>
<span class="stat-label">inner node COW (1 cache line)</span>
</div>
<div class="stat" markdown>
<span class="stat-value">5</span>
<span class="stat-label">levels for 30M keys</span>
</div>
<div class="stat" markdown>
<span class="stat-value">~9x</span>
<span class="stat-label">less write amplification vs page COW</span>
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
Migrate existing RocksDB applications with a compatibility shim. Same API, PsiTri performance underneath.
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
tx.commit();  // atomic root swap, no WAL

// Read with snapshot isolation
auto val = tx.get<std::string>("user:alice");
```

---

## Benchmark Highlights

In a [realistic banking workload](benchmarks/bank.md) modeled after TPC-B, each transfer performs **5 key-value operations** (2 reads + 2 updates + 1 insert) in a single atomic transaction. PsiTri outperforms every engine tested:

| Engine | Transfers/sec | Key-Value Ops/sec | Relative |
|--------|--------------|-------------------|----------|
| **PsiTri** | **376,691** | **1,883,455** | **1.00x** |
| PsiTriRocks | 356,703 | 1,783,515 | 0.95x |
| TidesDB | 225,937 | 1,129,685 | 0.60x |
| RocksDB | 126,341 | 631,705 | 0.34x |
| MDBX | 57,138 | 285,690 | 0.15x |

PsiTri shows **zero write degradation** under concurrent read load. Its memory-mapped MVCC architecture means readers never block writers.

[Full benchmark results :material-arrow-right:](benchmarks/bank.md){ .md-button }

---

## How It Works

PsiTri combines ideas from radix trees and B-trees into a hybrid data structure:

- **Inner nodes** route on single-byte dividers with prefix compression, giving up to 256-way fan-out per level
- **Leaf nodes** store sorted keys with hash-accelerated binary search, packing ~58 keys per node
- **SAL (Segment Allocator Library)** provides the persistent storage layer: memory-mapped allocation, atomic reference counting, copy-on-write (copying shared data on mutation rather than modifying in place), and background compaction

[Architecture overview :material-arrow-right:](architecture/overview.md){ .md-button }

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

## Status

Under active development. The core data structure, allocator, and transaction model are working. Multi-writer concurrency is in progress.
