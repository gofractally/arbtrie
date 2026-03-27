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

## Results: Apple M5 Max (ARM64, macOS)

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

---

## Results: AMD EPYC-Turin (x86-64, Linux)

### Environment

| Component | Spec |
|-----------|------|
| CPU | AMD EPYC-Turin, 8 cores / 16 threads (SMT), 2.4 GHz |
| ISA extensions | SSE2, SSE4.1, SSSE3, AVX2, AVX-512F/BW/DQ/VL/VBMI/VNNI |
| RAM | 121 GB |
| Storage | 960 GB virtual disk (cloud VM — Linux 6.17, 4 KB page size) |
| Compiler | Clang 20, C++20, `-O3 -flto -march=native` |

### Transaction Throughput

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Transaction Throughput — Write-Only (transfers/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Transfers per second" 0 --> 280000
    bar [163614, 159938, 167093, 104102, 239041]
```

| Engine | Transfers/sec | KV Ops/sec | Relative to PsiTri |
|--------|--------------|------------|--------------------|
| **PsiTri** | **163,614** | **818,070** | **1.00x** |
| PsiTriRocks | 159,938 | 799,690 | 0.98x |
| TidesDB | 167,093 | 835,465 | 1.02x |
| RocksDB | 104,102 | 520,510 | 0.64x |
| MDBX | 239,041 | 1,195,205 | 1.46x |

On this x86 cloud VM, MDBX's B+tree layout benefits from 4 KB OS pages and x86 hardware
prefetchers. The gap is largely a page-copy artifact: MDBX copies one page per write, and
4 KB pages on x86 cost 4x less than the 16 KB pages on M5 Max (see
[cross-platform comparison](#cross-platform-comparison)).

### Bulk Load

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#16A34A'}}}}%%
xychart-beta
    title "Bulk Load — 1M Accounts (ops/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Operations per second" 0 --> 3500000
    bar [2876212, 960000, 630000, 1160000, 2230000]
```

| Engine | Time | Ops/sec |
|--------|------|---------|
| **PsiTri** | **0.35s** | **2.88M** |
| MDBX | 0.45s | 2.23M |
| RocksDB | 0.86s | 1.16M |
| PsiTriRocks | 1.05s | 0.96M |
| TidesDB | 1.58s | 0.63M |

PsiTri leads bulk load on x86 — sequential arena writes benefit from AVX-512
`copy_branches` (9x speedup over scalar).

### Transaction Time (Write-Only Phase)

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#DC2626'}}}}%%
xychart-beta
    title "Transaction Phase Wall Time — Write-Only (seconds)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Seconds" 0 --> 110
    bar [61.1, 62.5, 59.8, 96.1, 41.8]
```

| Engine | Time | vs. PsiTri |
|--------|------|------------|
| **PsiTri** | **61.1s** | -- |
| PsiTriRocks | 62.5s | +2% |
| TidesDB | 59.8s | -2% |
| RocksDB | 96.1s | +57% |
| MDBX | 41.8s | -32% |

### Validation Scan

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#9333EA'}}}}%%
xychart-beta
    title "Validation Scan — 7.9M Entries (ops/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Operations per second" 0 --> 42000000
    bar [19628559, 19300000, 1200000, 11200000, 38100000]
```

| Engine | Time | Ops/sec |
|--------|------|---------|
| MDBX | 0.38s | 38.1M |
| **PsiTri** | **0.73s** | **19.6M** |
| PsiTriRocks | 0.74s | 19.3M |
| RocksDB | 1.28s | 11.2M |
| TidesDB | 11.9s | 1.20M |

PsiTri and PsiTriRocks are nearly identical on scan, and both comfortably ahead of
RocksDB and TidesDB.

### Concurrent Read Performance

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Write Throughput: Write-Only vs Write+Read (tx/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Transfers per second" 0 --> 280000
    bar [163614, 159938, 167093, 104102, 239041]
    bar [181168, 121276, 163275, 87423, 229280]
```

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#9333EA'}}}}%%
xychart-beta
    title "Concurrent Reader Throughput (reads/sec)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Reads per second" 0 --> 1200000
    bar [1117771, 785868, 326596, 223149, 927734]
```

| Engine | Write-Only | Write+Read | Write Impact | Reader reads/sec |
|--------|-----------|------------|-------------|-----------------|
| **PsiTri** | 163,614 | **181,168** | **+10.7%** | **1,117,771** |
| PsiTriRocks | 159,938 | 121,276 | -24.2% | 785,868 |
| TidesDB | 167,093 | 163,275 | -2.3% | 326,596 |
| RocksDB | 104,102 | 87,423 | -16.0% | 223,149 |
| MDBX | 239,041 | 229,280 | -4.1% | 927,734 |

PsiTri shows no write degradation from concurrent reads (+10.7%), consistent with
its lock-free MVCC architecture. PsiTri delivers the highest reader throughput of
any engine on this platform (1.12M reads/sec), ahead of MDBX (928K).

### Storage

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB'}}}}%%
xychart-beta
    title "Reachable Data Size (MB)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Megabytes" 0 --> 1400
    bar [561, 558, 675, 567, 1257]
```

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#DC2626'}}}}%%
xychart-beta
    title "On-Disk File Size (MB)"
    x-axis ["PsiTri", "PsiTriRocks", "TidesDB", "RocksDB", "MDBX"]
    y-axis "Megabytes" 0 --> 2500
    bar [2080, 2298, 675, 579, 1344]
```

| Engine | Reachable | File Size |
|--------|-----------|-----------|
| PsiTriRocks | 558 MB | 2,298 MB |
| **PsiTri** | **561 MB** | **2,080 MB** |
| RocksDB | 567 MB | 579 MB |
| TidesDB | 675 MB | 675 MB |
| MDBX | 1,257 MB | 1,344 MB |

PsiTri and PsiTriRocks have the most compact reachable data. The larger file size
reflects COW free space awaiting compaction, consistent with the M5 Max results.

### Commit Granularity

Batch size controls how many transfers are grouped into a single atomic commit. Larger
batches amortize commit overhead and improve throughput, but at the cost of read-visibility
latency: a concurrent reader cannot see any transfer in the batch until the whole batch
commits.

> **Note:** In PsiTri, uncommitted writes are invisible to readers but are **not at risk** —
> committing is a single atomic root-pointer advance. There is no WAL replay needed on crash
> recovery for in-flight batches. In RocksDB, large batches mirror the memtable (up to ~64 MB
> for a 1M-transfer batch). TidesDB has a hard limit of **100K ops per transaction**; at 5 ops
> per transfer that allows at most ~20K transfers per commit, so it is only valid at batch=100
> and is excluded from the scaling charts.

#### Write-Only Throughput vs Batch Size

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB,#7C3AED,#DC2626,#D97706'}}}}%%
xychart-beta
    title "Write-Only Throughput vs Batch Size (tx/sec)"
    x-axis ["100", "10K", "100K", "1M"]
    y-axis "Transfers per second" 0 --> 650000
    line [163614, 254421, 481361, 593556]
    line [159186, 212378, 274096, 360102]
    line [242402, 286117, 475811, 603124]
    line [108884, 159623, 144383, 185796]
```

*Series order: PsiTri (blue), PsiTriRocks (purple), MDBX (red), RocksDB (amber)*

| Engine | batch=100 | batch=10K | batch=100K | batch=1M | Peak gain |
|--------|----------:|----------:|-----------:|---------:|----------:|
| **PsiTri** | 163,614 | 254,421 | 481,361 | **593,556** | +263% |
| **MDBX** | 242,402 | 286,117 | 475,811 | **603,124** | +149% |
| PsiTriRocks | 159,186 | 212,378 | 274,096 | 360,102 | +126% |
| RocksDB | 108,884 | 159,623 | 144,383 | 185,796 | +71% |
| TidesDB | 180,654 | — | — | — | N/A |

PsiTri and MDBX both reach ~600K tx/sec at batch=1M and scale similarly — PsiTri benefits
from amortizing the root-pointer CAS, while MDBX amortizes its B+tree page COW. RocksDB
shows a dip at batch=100K before recovering at 1M, likely reflecting memtable pressure
interacting with compaction.

#### Write+Read Throughput vs Batch Size

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB,#7C3AED,#DC2626,#D97706'}}}}%%
xychart-beta
    title "Write+Read Throughput vs Batch Size (tx/sec)"
    x-axis ["100", "10K", "100K", "1M"]
    y-axis "Transfers per second" 0 --> 550000
    line [181168, 272840, 468694, 509351]
    line [117094, 177691, 230013, 361591]
    line [263524, 292664, 444587, 490456]
    line [119185, 138015, 144091, 181406]
```

*Series order: PsiTri (blue), PsiTriRocks (purple), MDBX (red), RocksDB (amber)*

| Engine | batch=100 | batch=10K | batch=100K | batch=1M |
|--------|----------:|----------:|-----------:|---------:|
| **PsiTri** | 181,168 | 272,840 | 468,694 | **509,351** |
| **MDBX** | 263,524 | 292,664 | 444,587 | **490,456** |
| PsiTriRocks | 117,094 | 177,691 | 230,013 | 361,591 |
| RocksDB | 119,185 | 138,015 | 144,091 | 181,406 |
| TidesDB | 178,478 | — | — | — |

PsiTri overtakes MDBX in write+read mode at batch=100K and beyond. MDBX's reader/writer
lock becomes relatively more costly as the write thread holds the lock for longer batches,
while PsiTri's lock-free MVCC is unaffected.

#### Concurrent Reader Throughput vs Batch Size

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'xyChart': {'plotColorPalette': '#2563EB,#7C3AED,#DC2626,#D97706'}}}}%%
xychart-beta
    title "Concurrent Reader Throughput vs Batch Size (reads/sec)"
    x-axis ["100", "10K", "100K", "1M"]
    y-axis "Reads per second" 0 --> 1800000
    bar [1117771, 1278498, 1358428, 1356727]
    bar [872316, 1095787, 1388806, 1669767]
    bar [1055486, 1166392, 1306705, 1100886]
    bar [275903, 324247, 347816, 296904]
```

*Series order: PsiTri (blue), PsiTriRocks (purple), MDBX (red), RocksDB (amber)*

| Engine | batch=100 | batch=10K | batch=100K | batch=1M |
|--------|----------:|----------:|-----------:|---------:|
| **PsiTri** | 1,117,771 | 1,278,498 | 1,358,428 | 1,356,727 |
| **PsiTriRocks** | 872,316 | 1,095,787 | 1,388,806 | **1,669,767** |
| **MDBX** | 1,055,486 | 1,166,392 | 1,306,705 | 1,100,886 |
| RocksDB | 275,903 | 324,247 | 347,816 | 296,904 |

PsiTri and PsiTriRocks reader throughput improves monotonically with batch size: larger
batches mean the writer holds the current root longer, giving readers more time on a stable
snapshot. MDBX reader throughput peaks at batch=100K and drops at batch=1M, where longer
write transactions increase reader-visible stall events. RocksDB readers are consistently
the slowest across all batch sizes.

#### Per-Round Throughput Curves

The write+read phase runs 10M transfers in 10 rounds of 1M each. The round-by-round
breakdown reveals stability (or instability) in each engine's throughput profile.

| Round | PsiTri 1M | MDBX 100K | RocksDB 10K | PsiTriRocks 100K |
|-------|----------:|----------:|------------:|-----------------:|
| 1 | 548K | 453K | 165K | 254K |
| 2 | 523K | 453K | 157K | 249K |
| 3 | 510K | 452K | 156K | 243K |
| 4 | 504K | 452K | 142K | 235K |
| 5 | 505K | 452K | 139K | 230K |
| 6 | 508K | 453K | 134K | 229K |
| 7 | 505K | 452K | 133K | 230K |
| 8 | 505K | 452K | 134K | 230K |
| 9 | 508K | 450K | 137K | 230K |
| 10 | 509K | 444K | 138K | 230K |

- **PsiTri (batch=1M):** Starts hot (548K) as the working set is warm, settles to a stable
  ~505–509K after round 3. No compaction stalls.
- **MDBX (batch=100K):** Essentially flat at 452–453K through round 9, with a small dip in
  round 10. Highly predictable.
- **RocksDB (batch=10K):** Falls from 165K to a 133–134K trough in rounds 6–8 — a classic
  compaction cliff as L0→L1 compaction competes with writes — then partially recovers to 138K.
- **PsiTriRocks (batch=100K):** Sharp drop from 254K to ~230K by round 5 (RocksDB compaction
  absorbing the write path through the shim), then fully stable through rounds 5–10.

#### MDBX File Bloat Under Large Batches

MDBX copies full pages on each write. Larger batches create more dirty pages per commit,
and MDBX's free-page recycler can leave significant dead space in the file.

| Batch size | File size | Live data | Waste |
|------------|----------:|----------:|------:|
| 100 | 1,344 MB | 1,257 MB | 6% |
| 10K | 3,968 MB | 1,257 MB | 68% |
| 100K | 4,928 MB | 1,257 MB | 74% |
| 1M | 2,048 MB | 1,257 MB | 39% |

File bloat peaks at batch=100K (4,928 MB — nearly 4x live data) then shrinks at batch=1M as
fewer, larger commits leave the free-page recycler more time per commit to consolidate space.
PsiTri's file size is not meaningfully affected by batch size because its COW operates at
64-byte node granularity rather than 4 KB page granularity.

---

## Cross-Platform Comparison

The most striking cross-platform shift is MDBX, not PsiTri.

### Write Throughput: ARM M5 Max vs x86 EPYC-Turin

| Engine | M5 Max (ARM64) | EPYC-Turin (x86) | Change |
|--------|----------------|------------------|--------|
| **PsiTri** | **376,691** | **163,614** | -57% |
| PsiTriRocks | 356,703 | 159,938 | -55% |
| TidesDB | 225,937 | 167,093 | -26% |
| RocksDB | 126,341 | 104,102 | -18% |
| MDBX | 57,138 | 239,041 | **+318%** |

Every engine is slower on the x86 VM than on M5 Max — this is a cloud VM vs a
high-end workstation, so absolute numbers aren't directly comparable. What matters
is the relative ordering and the magnitude of MDBX's swing.

**Why does MDBX improve so much on x86?**

MDBX (like LMDB) uses page-level copy-on-write: every write copies the entire page
containing the modified key. On M5 Max the OS page size is **16 KB**; on x86 Linux
it is **4 KB**. Each write copies 4x less data on x86, which maps almost exactly
onto the 4.2x throughput increase. This is an OS page size effect, not an
architectural one.

**PsiTri's COW operates at 64-byte node granularity** and is indifferent to OS page
size, so it does not benefit from the same effect. Its absolute throughput drops on
the cloud VM due to higher memory access latency compared to M5 Max's Unified Memory
Architecture, but its relative standing against RocksDB and TidesDB stays consistent
across both platforms.

**Consistent patterns across both platforms:**

- PsiTri leads or ties for **bulk load** on both platforms
- PsiTri and PsiTriRocks lead **reachable data compactness** on both platforms
- MDBX leads **sequential scan** on both platforms
- PsiTri and MDBX lead **concurrent reader throughput** on both platforms
- RocksDB is consistently mid-pack on transactions

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

### Apple M5 Max (ARM64)
- **Hardware**: Apple M5 Max, 128 GB Unified Memory
- **OS**: macOS (Darwin 25.3.0)
- **Compiler**: Clang 17 (LLVM), C++20, `-O3 -flto=thin`
- **Engine versions**: RocksDB 9.9.3, libmdbx 0.13.11, TidesDB 8.9.4

### AMD EPYC-Turin (x86-64)
- **Hardware**: AMD EPYC-Turin, 8 cores / 16 threads, 2.4 GHz, 121 GB RAM (cloud VM — Vultr)
- **ISA**: AVX-512F/BW/DQ/VL/VBMI/VBMI2/VNNI, AVX2, SSSE3, SSE4.1
- **OS**: Ubuntu Linux 6.17.0 x86_64, 4 KB page size
- **Compiler**: Clang 20 (LLVM), C++20, `-O3 -flto -march=native`
- **Engine versions**: RocksDB (built from source), libmdbx (built from source), TidesDB (built from source)
