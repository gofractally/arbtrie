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
- **Triangular access distribution** -- some accounts are "hot," mimicking real-world Pareto-like skew
- **Deterministic** -- identical RNG seed ensures every engine processes the exact same workload
- **Validated** -- balance conservation and transaction log entry count verified after completion

### Fairness Controls

All engines use identical batching and sync parameters:

| Parameter | Value |
|-----------|-------|
| Batch size | 100 transfers per commit |
| Sync frequency | Every 100 commits |
| Sync mode | none (no forced durability) |
| Initial balance | 1,000,000 per account |
| RNG seed | 12345 |

## Results

### Transaction Throughput

The core metric -- sustained transfers per second over 10M operations. Each successful
transfer performs 2 reads + 2 updates + 1 insert.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Transaction Throughput — Write-Only (transfers/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Transfers per second" 0 --> 400000
    bar [376691, 356703, 225937, 126341, 57138]
```

| Engine | Transfers/sec | KV Ops/sec | Relative |
|--------|--------------|------------|----------|
| **PsiTri** | **376,691** | **1,883,455** | **1.00x** |
| PsiTriRocks | 356,703 | 1,783,515 | 0.95x |
| TidesDB | 225,937 | 1,129,685 | 0.60x |
| RocksDB | 126,341 | 631,705 | 0.34x |
| MDBX | 57,138 | 285,690 | 0.15x |

Each transfer performs 5 key-value operations (2 reads + 2 updates + 1 insert), so the effective KV ops/sec is 5x the transfer rate.

PsiTri uses **memory-mapped copy-on-write nodes** with an arena allocator. A transfer
touches a small number of nodes already in the page cache. There is no write-ahead log,
no compaction stalls, and no memtable flush -- writes go directly to the memory-mapped
data structure.

### Bulk Load

Inserting 1M accounts with initial balances in a single batch transaction.

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

### Transaction Time

Wall-clock time for the 10M transfer phase.

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
| **PsiTri** | **26.5s** | -- |
| PsiTriRocks | 28.0s | +5.6% |
| TidesDB | 44.3s | +66% |
| RocksDB | 79.2s | +198% |
| MDBX | 175.0s | +558% |

PsiTri completes the same work in one-sixth the time MDBX requires.

### Validation Scan

Full scan reading all 1M accounts plus ~6.9M transaction log entries.

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

MDBX dominates sequential scanning -- its B+tree stores keys in sorted order with contiguous leaf pages.

### Storage Efficiency

#### Reachable Data Size

The theoretical minimum raw data size is **275 MB**. This chart shows bytes occupied by live, reachable objects.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Reachable Data Size (MB)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Megabytes" 0 --> 400
    bar [303, 304, 263, 320, 366]
```

| Engine | Reachable Data | vs. Theoretical (275 MB) | File Size |
|--------|---------------|--------------------------|-----------|
| **TidesDB** | 263 MB | 0.96x | 263 MB |
| **PsiTri** | 303 MB | 1.10x | 1,088 MB |
| **PsiTriRocks** | 304 MB | 1.11x | 1,210 MB |
| **RocksDB** | 314 MB | 1.14x | 320 MB |
| **MDBX** | 366 MB | 1.33x | 640 MB |

PsiTri's reachable data is only 1.10x the theoretical minimum, thanks to graduated leaf node sizing.

#### File Size

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#DC2626'}}}}%%
xychart-beta
    title "On-Disk File Size (MB)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Megabytes" 0 --> 1300
    bar [1088, 1210, 263, 320, 640]
```

PsiTri's file size is 3.6x its reachable data. The dead space consists of copy-on-write duplicates awaiting compaction and allocator free space within segments. The background compactor keeps growth bounded during sustained write workloads.

### Concurrent Read Performance

The benchmark runs a second phase with a concurrent reader thread performing Pareto-distributed point lookups.

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

PsiTri shows **zero write degradation** from concurrent reads. Its memory-mapped MVCC architecture means readers access the same physical pages with no locking.

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
| **PsiTri** | Radix/B-tree hybrid, mmap copy-on-write | Fastest transactions (377K/s), zero read contention | Larger file footprint |
| **PsiTriRocks** | PsiTri via RocksDB API shim | Drop-in RocksDB replacement | 29% write penalty with concurrent reads |
| **TidesDB** | Skip-list + SSTables | Good tx speed (226K/s), compact | Slow scan, 100K txn op limit |
| **RocksDB** | LSM-tree | Compact storage, minimal read impact | 3.0x slower than PsiTri |
| **MDBX** | B+tree, MVCC copy-on-write | Fastest scan (110M/s) and reads (1.75M/s) | 6.6x slower transactions |

All five engines pass validation: balance conservation verified and transaction log entry counts match across both phases.

## Reproducing

```bash
# Build all engines (from repo root)
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_ROCKSDB_BENCH=ON \
      -DBUILD_TIDESDB_BENCH=ON \
      -B build/release

cmake --build build/release -j8 --target \
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
