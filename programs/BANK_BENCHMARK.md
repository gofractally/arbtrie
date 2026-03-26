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
- **10,000,000 transfer attempts per phase** (6,856,951 successful in write-only phase)
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
    title "Transaction Throughput — Write-Only (transfers/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Transfers per second" 0 --> 400000
    bar [376691, 356703, 225937, 126341, 57138]
```

| Engine | Transfers/sec | Relative |
|--------|--------------|----------|
| **PsiTri** | **376,691** | **1.00x** |
| PsiTriRocks | 356,703 | 0.95x |
| TidesDB | 225,937 | 0.60x |
| RocksDB | 126,341 | 0.34x |
| MDBX | 57,138 | 0.15x |

PsiTri's adaptive radix trie uses **memory-mapped copy-on-write nodes** with an arena
allocator. A transfer touches a small number of trie nodes already in the page cache.
There is no write-ahead log, no compaction, and no memtable flush — writes go directly
to the memory-mapped data structure. Batching 100 transfers per commit amortizes the
cost of the COW root update. The RocksDB compatibility shim (PsiTriRocks) adds only
~5% overhead, confirming the shim layer is thin.

TidesDB's skip-list + SSTable architecture delivers 226K tx/sec — faster than both
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
    y-axis "Operations per second" 0 --> 3500000
    bar [3060276, 1795315, 959912, 1961333, 3078055]
```

| Engine | Time | Ops/sec |
|--------|------|---------|
| **PsiTri** | 0.33s | **3.06M** |
| MDBX | 0.33s | 3.08M |
| RocksDB | 0.51s | 1.96M |
| PsiTriRocks | 0.56s | 1.80M |
| TidesDB | 1.04s | 0.96M |

Bulk load performance is tightly clustered across the top three engines (3.06–3.08M
ops/sec). MDBX's B+tree excels at sequential insertion — keys are sorted and appended
to leaf pages with minimal page splits. PsiTri's arena allocator writes sequentially
into memory-mapped segments with no WAL or memtable overhead. RocksDB's memtable
absorbs writes quickly with WAL buffering.

TidesDB is slowest because its 100K operation transaction limit forces 12 separate
commit cycles, each flushing the write-ahead log.

### Transaction Time

Wall-clock time for the 10M transfer phase — the inverse of throughput, but
visualized to emphasize the absolute time cost difference between engines.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#DC2626'}}}}%%
xychart-beta
    title "Transaction Phase Wall Time — Write-Only (seconds)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Seconds" 0 --> 180
    bar [26.547, 28.034, 44.260, 79.151, 175.012]
```

| Engine | Time | vs. PsiTri |
|--------|------|-----------|
| **PsiTri** | **26.5s** | — |
| PsiTriRocks | 28.0s | +5.6% |
| TidesDB | 44.3s | +66% |
| RocksDB | 79.2s | +198% |
| MDBX | 175.0s | +558% |

The gap between PsiTri and MDBX is over 148 seconds on the same workload. For
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
    y-axis "Operations per second" 0 --> 115000000
    bar [27619781, 37644460, 1843029, 14153353, 109856761]
```

| Engine | Time | Ops/sec |
|--------|------|---------|
| **MDBX** | 0.072s | **109.9M** |
| PsiTriRocks | 0.209s | 37.6M |
| PsiTri | 0.284s | 27.6M |
| RocksDB | 0.555s | 14.2M |
| TidesDB | 4.263s | 1.84M |

MDBX dominates sequential scanning at 109.9M ops/sec — its B+tree stores keys
in sorted order with contiguous leaf pages, enabling pure sequential memory
access with excellent prefetch behavior. This is MDBX's architectural sweet
spot: the same structure that penalizes random writes rewards sequential reads.

PsiTri achieves 27.6M ops/sec via cursor-based trie traversal. While tries
don't store keys contiguously, PsiTri's memory-mapped nodes and arena layout
provide reasonable locality. PsiTriRocks is faster here (37.6M) likely due to
iterator implementation differences in the shim layer.

RocksDB must merge results across multiple SSTable levels during iteration,
which explains the 14.2M ops/sec — still fast, but the merge overhead across
the now-larger dataset (accounts + log entries) is measurable.

TidesDB's scan is **60x slower** than MDBX because its C API iterator does not
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
    y-axis "Megabytes" 0 --> 400
    bar [303, 304, 263, 320, 366]
```

The theoretical minimum raw data size is **275 MB** — the sum of all key bytes and
value bytes with zero structural overhead. This baseline is identical for all engines:
1M account keys (~8.4 MB) + 1M balances (8 MB) + 6.9M log keys (72 MB) +
6.9M log values (~187 MB).

| Engine | Reachable Data | vs. Theoretical (275 MB) | File Size | Notes |
|--------|---------------|--------------------------|-----------|-------|
| **TidesDB** | 263 MB | 0.96x | 263 MB | Likely compressed below raw size |
| **PsiTri** | 303 MB | 1.10x | 1,088 MB | Graduated leaf sizing minimizes waste |
| **PsiTriRocks** | 304 MB | 1.11x | 1,210 MB | Same engine, same footprint |
| **RocksDB** | 314 MB | 1.14x | 320 MB | Block compression offsets index overhead |
| **MDBX** | 366 MB | 1.33x | 640 MB | B+tree page overhead |

PsiTri's reachable data is now only 1.10x the theoretical minimum, thanks to
graduated leaf node sizing that allocates leaves at the smallest power-of-two
cacheline boundary that fits their content. This dramatically reduces waste
compared to the previous fixed 2 KB allocation — a leaf holding a single
8-byte key + 8-byte value now uses ~128 bytes instead of 2,048.

RocksDB and TidesDB achieve near-theoretical or below-theoretical sizes through
block compression, which is particularly effective on the sequential log keys and
small fixed-size values in this workload.

#### File Size

The raw on-disk file size includes dead COW copies and allocator free space that
has not yet been reclaimed. PsiTri's background compactor continuously reclaims
dead space during the benchmark, keeping the file size within ~3x of reachable data.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#DC2626'}}}}%%
xychart-beta
    title "On-Disk File Size (MB)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Megabytes" 0 --> 1300
    bar [1088, 1210, 263, 320, 640]
```

| Engine | File Size | Reachable | Dead/Free Space | Notes |
|--------|-----------|-----------|-----------------|-------|
| **PsiTri** | 1,088 MB | 303 MB | 785 MB (72%) | COW copies + allocator free space |
| **PsiTriRocks** | 1,210 MB | 304 MB | 906 MB (75%) | Same engine via RocksDB API shim |
| **MDBX** | 640 MB | 366 MB | 274 MB (43%) | COW pages accumulate between syncs |
| **RocksDB** | 320 MB | 314 MB | 6 MB (2%) | LSM compaction + block compression |
| **TidesDB** | 263 MB | 263 MB | 0 MB | No detailed stats exposed |

RocksDB achieves the most compact footprint thanks to LSM compaction and block
compression. The transaction log entries (sequential keys with ~30-byte values)
compress well under RocksDB's block-based scheme.

MDBX's 640 MB file is 43% free space — dead COW pages that the GC cannot reclaim
without an fsync. The transaction log inserts grow the B+tree significantly because
each new sequential key requires page allocation under COW.

PsiTri's file size is 3.6x its reachable data. The dead space consists of COW copies
awaiting compaction and allocator free space within segments. The background compactor
reclaims segments as they accumulate dead objects, keeping growth bounded during
sustained write workloads. The reachable data measurement confirms the trie structure
itself is highly space-efficient at only 1.10x the theoretical minimum — the file
overhead is from in-flight garbage collection, not data structure inefficiency.

### Concurrent Read Performance

The benchmark runs a second phase of 10M transactions with a concurrent reader
thread performing Pareto-distributed point lookups. Each "read transaction"
fetches 100 account balances before refreshing its snapshot to see the latest
committed state. This measures how well each engine handles mixed read-write
workloads — a critical real-world scenario.

#### Write Throughput Under Read Load

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Write Throughput: Write-Only vs Write+Read (tx/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Transfers per second" 0 --> 400000
    bar [376691, 356703, 225937, 126341, 57138]
    bar [382416, 250564, 228892, 124864, 51858]
```

| Engine | Write-Only | Write+Read | Write Impact | Reader reads/sec |
|--------|-----------|------------|-------------|-----------------|
| **PsiTri** | 376,691 | **382,416** | **+1.5%** | 1,719,249 |
| PsiTriRocks | 356,703 | 250,564 | **-29.8%** | 1,403,776 |
| TidesDB | 225,937 | 228,892 | +1.3% | 457,579 |
| RocksDB | 126,341 | 124,864 | -1.2% | 329,917 |
| MDBX | 57,138 | 51,858 | -9.2% | 1,750,952 |

PsiTri shows **zero write degradation** from concurrent reads. Its memory-mapped
architecture means readers access the same physical pages as the writer with no
locking — each read session gets a snapshot of the current root and traverses
the trie independently. The slight speedup is within measurement noise.

PsiTriRocks suffers a **29% write penalty** because the RocksDB API shim creates
a full write session (not just a read session) per thread via `thread_local`
state. The reader thread's write session contends with the primary writer for
segment allocation. This is a shim implementation issue, not a PsiTri limitation.

MDBX takes a 9% write hit but delivers the **fastest reads** at 1.75M/sec,
consistent with its B+tree strength for point lookups. Read-only transactions
use `mdbx_txn_renew()` to minimize overhead.

RocksDB shows minimal impact (-1.2%) thanks to lock-free MVCC reads, but its
LSM-tree structure limits read throughput to 330K/sec — 5x slower than PsiTri's
reader despite lower write contention.

#### Reader Throughput

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#9333EA'}}}}%%
xychart-beta
    title "Concurrent Reader Throughput (reads/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Reads per second" 0 --> 1800000
    bar [1719249, 1403776, 457579, 329917, 1750952]
```

### Summary

| Engine | Architecture | Strength | Weakness |
|--------|-------------|----------|----------|
| **PsiTri** | Adaptive radix trie, mmap COW | Fastest transactions (377K/s), zero read contention | Larger file footprint (COW + compaction) |
| **PsiTriRocks** | PsiTri via RocksDB API shim | Drop-in RocksDB replacement | 29% write penalty with concurrent reads |
| **TidesDB** | Skip-list + SSTables | Good tx speed (226K/s), compact | Slow scan, 100K txn op limit |
| **RocksDB** | LSM-tree | Compact storage, minimal read impact | 3.0x slower than PsiTri |
| **MDBX** | B+tree, MVCC COW | Fastest scan (110M/s) and reads (1.75M/s) | 6.6x slower transactions |

All five engines pass validation: balance conservation verified (1,000,000,000,000
total) and transaction log entry counts match across both phases. Each engine processes
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
| `--reads-per-tx` | 100 | Point reads per reader thread batch |

## Environment

- **Hardware**: Apple M5 Max (ARM64)
- **OS**: macOS (Darwin 25.3.0)
- **Compiler**: Clang 17 (LLVM), C++20, `-O3 -flto=thin`
- **Engine versions**: RocksDB 9.9.3, libmdbx 0.13.11, TidesDB 8.9.4
