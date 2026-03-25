# Bank Transaction Benchmark

A realistic banking workload benchmark comparing five embedded key-value storage
engines on atomic transactional operations modeled after TPC-B.

## Workload

Each successful transfer performs **5 key-value operations** in a single atomic transaction:

1. **Read** source account balance
2. **Read** destination account balance
3. **Update** source balance (debit)
4. **Update** destination balance (credit)
5. **Insert** transaction log entry (big-endian sequence number key with transfer details)

This mirrors the TPC-B debit-credit pattern (3 updates + 1 select + 1 insert) and
exercises both random-access updates and sequential-key inserts within the same transaction.

- **1,000,000 accounts** with random names (dictionary words + synthetic binary/decimal keys)
- **10,000,000 transfer attempts** (6,856,951 successful, 3,143,049 skipped for insufficient balance)
- **Triangular access distribution** — some accounts are "hot," mimicking real-world Pareto-like skew
- **Deterministic** — identical RNG seed ensures every engine processes the exact same workload
- **Validated** — balance conservation and transaction log entry count verified after completion

### Fairness Controls

All engines use identical batching and sync parameters to ensure apples-to-apples comparison:

| Parameter | Value |
|-----------|-------|
| Batch size | 100 transfers per commit |
| Sync frequency | Every 100 commits |
| Sync mode | none (no forced durability) |
| Initial balance | 1,000,000 per account |
| RNG seed | 12345 |

## Results

### Transaction Throughput

The core metric — sustained transfers per second over 10M operations. Each successful
transfer performs 2 reads + 2 updates + 1 insert, stressing both random-access
latency and sequential-key insertion.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Transaction Throughput (transfers/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Transfers per second" 0 --> 400000
    bar [372175, 355126, 214125, 134911, 58756]
```

| Engine | Transfers/sec | Relative |
|--------|--------------|----------|
| **PsiTri** | **372,175** | **1.00x** |
| PsiTriRocks | 355,126 | 0.95x |
| TidesDB | 214,125 | 0.58x |
| RocksDB | 134,911 | 0.36x |
| MDBX | 58,756 | 0.16x |

PsiTri's adaptive radix trie uses **memory-mapped copy-on-write nodes** with an arena
allocator. A transfer touches a small number of trie nodes already in the page cache.
There is no write-ahead log, no compaction, and no memtable flush — writes go directly
to the memory-mapped data structure. Batching 100 transfers per commit amortizes the
cost of the COW root update. The RocksDB compatibility shim (PsiTriRocks) adds only
~5% overhead, confirming the shim layer is thin.

TidesDB's skip-list + SSTable architecture delivers 214K tx/sec — faster than both
RocksDB and MDBX — thanks to efficient in-memory buffering with hash-accelerated
read-your-own-writes within transactions.

RocksDB's LSM-tree must potentially check the memtable, immutable memtables, and
multiple SSTable levels on each read. The `WriteBatch` + `Get` pattern requires an
in-memory pending-write cache to support read-your-own-writes within each batch,
adding overhead per transfer.

MDBX uses a B+tree with MVCC copy-on-write. With `SAFE_NOSYNC` mode, the garbage
collector **cannot reclaim freed pages until the steady meta page advances via fsync**.
Dead COW pages accumulate between syncs (274 MB free out of 640 MB), growing the
working set beyond CPU cache. The transaction log inserts hit MDBX hardest because
each new sorted key forces B+tree page splits under COW.

### Bulk Load

Inserting 1M accounts with initial balances in a single batch transaction (or chunked
for engines with transaction size limits). This measures sequential write throughput
with no read contention.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#16A34A'}}}}%%
xychart-beta
    title "Bulk Load — 1M Accounts (ops/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Operations per second" 0 --> 2000000
    bar [1680597, 1799838, 849857, 1828189, 1892768]
```

| Engine | Time | Ops/sec |
|--------|------|---------|
| **MDBX** | 0.53s | **1.89M** |
| RocksDB | 0.55s | 1.83M |
| PsiTriRocks | 0.56s | 1.80M |
| PsiTri | 0.60s | 1.68M |
| TidesDB | 1.18s | 0.85M |

Bulk load performance is tightly clustered across the top four engines (1.68–1.89M
ops/sec). MDBX's B+tree excels at sequential insertion — keys are sorted and appended
to leaf pages with minimal page splits. RocksDB's memtable absorbs writes quickly
with WAL buffering.

TidesDB is slowest because its 100K operation transaction limit forces 12 separate
commit cycles, each flushing the write-ahead log.

### Transaction Time

Wall-clock time for the 10M transfer phase — the inverse of throughput, but
visualized to emphasize the absolute time cost difference between engines.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#DC2626'}}}}%%
xychart-beta
    title "Transaction Phase Wall Time (seconds)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Seconds" 0 --> 180
    bar [26.869, 28.159, 46.702, 74.122, 170.193]
```

| Engine | Time | vs. PsiTri |
|--------|------|-----------|
| **PsiTri** | **26.9s** | — |
| PsiTriRocks | 28.2s | +4.8% |
| TidesDB | 46.7s | +74% |
| RocksDB | 74.1s | +176% |
| MDBX | 170.2s | +533% |

The gap between PsiTri and MDBX is over 143 seconds on the same workload. For
applications running millions of transactions per hour (financial systems,
blockchain state, game servers), this translates directly into throughput
capacity. PsiTri completes the same work in one-sixth the time MDBX requires.

### Validation Scan

A full scan reading all 1M account balances plus ~6.9M transaction log entries,
verifying balance conservation and log entry counts. This measures sequential read
throughput across the entire dataset.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#9333EA'}}}}%%
xychart-beta
    title "Validation Scan — 7.9M Entries (ops/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Operations per second" 0 --> 90000000
    bar [25625412, 34508643, 1633442, 11799588, 83361880]
```

| Engine | Time | Ops/sec |
|--------|------|---------|
| **MDBX** | 0.094s | **83.4M** |
| PsiTriRocks | 0.228s | 34.5M |
| PsiTri | 0.307s | 25.6M |
| RocksDB | 0.666s | 11.8M |
| TidesDB | 4.810s | 1.63M |

MDBX dominates sequential scanning at 83.4M ops/sec — its B+tree stores keys
in sorted order with contiguous leaf pages, enabling pure sequential memory
access with excellent prefetch behavior. This is MDBX's architectural sweet
spot: the same structure that penalizes random writes rewards sequential reads.

PsiTri achieves 25.6M ops/sec via cursor-based trie traversal. While tries
don't store keys contiguously, PsiTri's memory-mapped nodes and arena layout
provide reasonable locality. PsiTriRocks is faster here (34.5M) likely due to
iterator implementation differences in the shim layer.

RocksDB must merge results across multiple SSTable levels during iteration,
which explains the 11.8M ops/sec — still fast, but the merge overhead across
the now-larger dataset (accounts + log entries) is measurable.

TidesDB's scan is **51x slower** than MDBX because its C API iterator does not
expose key/value accessors, forcing the benchmark to fall back to individual
point lookups on all known account names plus sequential log key probes. This
is an API limitation, not necessarily a reflection of TidesDB's underlying
scan capability.

### Storage Efficiency

On-disk footprint after completing all 10M transfers with ~6.9M transaction log
entries. Storage efficiency reflects each engine's data structure overhead,
compression strategy, and garbage collection behavior.

#### Reachable Data Size

The most meaningful storage comparison: bytes occupied by live, reachable objects.
PsiTri now reports this by walking the trie from its roots and summing the size
of every reachable node. This eliminates dead COW copies and allocator free space
from the measurement.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Reachable Data Size (MB)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Megabytes" 0 --> 600
    bar [509, 509, 263, 320, 366]
```

The theoretical minimum raw data size is **275 MB** — the sum of all key bytes and
value bytes with zero structural overhead. This baseline is identical for all engines:
1M account keys (~8.4 MB) + 1M balances (8 MB) + 6.9M log keys (72 MB) +
6.9M log values (~187 MB).

| Engine | Reachable Data | vs. Theoretical (275 MB) | File Size | Notes |
|--------|---------------|--------------------------|-----------|-------|
| **TidesDB** | 263 MB | 0.96x | 263 MB | Likely compressed below raw size |
| **RocksDB** | 314 MB | 1.14x | 320 MB | Block compression offsets index overhead |
| **MDBX** | 366 MB | 1.33x | 640 MB | B+tree page overhead |
| **PsiTri** | 509 MB | 1.85x | 5,344 MB | Fixed 2 KB leaf allocation (see below) |
| **PsiTriRocks** | 509 MB | 1.85x | 5,344 MB | Same engine, same footprint |

PsiTri's 1.85x overhead relative to the theoretical minimum is primarily due to
fixed-size leaf node allocation: every leaf is allocated at 2,048 bytes regardless
of content. A leaf holding a single 8-byte key + 8-byte value wastes ~2,000 bytes.
Implementing graduated leaf sizing (e.g. 128 → 256 → 512 → 1024 → 2048) would
bring PsiTri's reachable data much closer to the theoretical minimum.

RocksDB and TidesDB achieve near-theoretical or below-theoretical sizes through
block compression, which is particularly effective on the sequential log keys and
small fixed-size values in this workload.

#### File Size

The raw on-disk file size tells a different story — PsiTri's file is 10x larger
than its reachable data. This is a consequence of copy-on-write without immediate
compaction: every modified node creates a new copy, and the old version remains
in the segment file until the background compactor reclaims it. During a sustained
write workload with 6.9M transactions, dead COW copies accumulate faster than
compaction can reclaim them.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#DC2626'}}}}%%
xychart-beta
    title "On-Disk File Size (MB)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Megabytes" 0 --> 5600
    bar [5344, 5344, 263, 320, 640]
```

| Engine | File Size | Reachable | Dead/Free Space | Notes |
|--------|-----------|-----------|-----------------|-------|
| **PsiTri** | 5,344 MB | 509 MB | 4,835 MB (91%) | COW copies + allocator free space |
| **PsiTriRocks** | 5,344 MB | 509 MB | 4,835 MB (91%) | Same engine via RocksDB API shim |
| **MDBX** | 640 MB | 366 MB | 274 MB (43%) | COW pages accumulate between syncs |
| **RocksDB** | 320 MB | 314 MB | 6 MB (2%) | LSM compaction + block compression |
| **TidesDB** | 263 MB | 263 MB | 0 MB | No detailed stats exposed |

RocksDB achieves the most compact footprint thanks to LSM compaction and block
compression. The transaction log entries (sequential keys with ~30-byte values)
compress well under RocksDB's block-based scheme.

MDBX's 640 MB file is 43% free space — dead COW pages that the GC cannot reclaim
without an fsync. The transaction log inserts grow the B+tree significantly because
each new sequential key requires page allocation under COW.

PsiTri's file size is the primary area for improvement. The 91% dead space
represents an opportunity to tune the compactor's aggressiveness during sustained
write workloads, or to implement segment-level truncation after the benchmark
completes. The reachable data measurement confirms the trie structure itself is
space-competitive — the issue is garbage collection throughput, not data structure
overhead.

### Summary

| Engine | Architecture | Strength | Weakness |
|--------|-------------|----------|----------|
| **PsiTri** | Adaptive radix trie, mmap COW | Fastest transactions (372K/s) | Large file footprint (compaction lag) |
| **PsiTriRocks** | PsiTri via RocksDB API shim | Drop-in RocksDB replacement | Slight shim overhead |
| **TidesDB** | Skip-list + SSTables | Good tx speed (214K/s), compact | Slow scan, 100K txn op limit |
| **RocksDB** | LSM-tree | Compact storage (320 MB) | 2.8x slower than PsiTri |
| **MDBX** | B+tree, MVCC COW | Fastest sequential scan (83M/s) | 6.3x slower transactions |

All five engines pass validation: balance conservation verified (1,000,000,000,000
total) and transaction log entry counts match (6,856,951). Each engine processes
the same deterministic workload with identical success/skip counts.

## Reproducing

```bash
# Build all engines (from repo root)
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_ROCKSDB_BENCH=ON \
      -DBUILD_TIDESDB_BENCH=ON \
      -B build/release

cmake --build build/release -j$(nproc) --target \
      bank-bench-psitri \
      bank-bench-psitrirocks \
      bank-bench-rocksdb \
      bank-bench-mdbx \
      bank-bench-tidesdb

# Run each engine with identical parameters
for engine in psitri psitrirocks rocksdb mdbx tidesdb; do
    build/release/bin/bank-bench-${engine} \
        --num-accounts=1000000 \
        --num-transactions=10000000 \
        --batch-size=100 \
        --sync-every=100 \
        --db-path=/tmp/bb_${engine}
done
```

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--num-accounts` | 1,000,000 | Number of bank accounts |
| `--num-transactions` | 10,000,000 | Number of transfer attempts |
| `--batch-size` | 1 | Transfers per commit |
| `--sync-every` | 0 | Sync to disk every N commits (0 = never) |
| `--sync-mode` | none | Durability: `none`, `async`, `sync` |
| `--seed` | 12345 | RNG seed for reproducibility |
| `--db-path` | /tmp/bank_bench_db | Database directory |
| `--initial-balance` | 1,000,000 | Starting balance per account |

## Environment

- **Hardware**: Apple M5 Max (ARM64)
- **OS**: macOS (Darwin 25.3.0)
- **Compiler**: Clang 17 (LLVM), C++20, `-O3 -flto=thin`
- **Engine versions**: RocksDB 9.9.3, libmdbx 0.13.11, TidesDB 8.9.4
