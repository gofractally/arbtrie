# Tradeoffs & Limitations

Every database makes tradeoffs. This page explains PsiTri's honestly -- what you
gain, what you pay, and when you should use something else.

## What PsiTri Is Built For

PsiTri was designed for workloads that need all of the following simultaneously:

- **Fast read-modify-write transactions** -- read a value, compute on it, write it back, thousands of times per second
- **Instant snapshots under heavy write load** -- take a consistent point-in-time view while the database is being hammered with writes, without blocking either side
- **Queries concurrent with writes** -- readers never block writers, writers never block readers, even on the same data

These requirements are common in **blockchain nodes**, **financial ledgers**, **game state servers**, and **event sourcing systems** -- workloads where every state transition reads previous state, mutates it, and commits atomically, while other threads need to query the current (or historical) state.

Traditional databases struggle with this combination:

| Requirement | B-tree (MDBX/LMDB) | LSM (RocksDB) | PsiTri |
|---|---|---|---|
| Read-modify-write | Page-level COW: 4KB copies per mutation | Fast writes but read amplification for the "read" part | Node-level COW: 64B copies, ART-buffered |
| Instant snapshots | O(1) but each snapshot pins entire freelist | Requires manual `GetSnapshot()`, pins SST files | O(1), COW shares structure, no freelist pressure |
| Queries during writes | Single writer blocks all readers during commit (LMDB) | Readers may see stale data across LSM levels | MVCC: readers see consistent snapshot, zero writer impact |
| Sustained throughput at scale | Degrades as freelist grows | Compaction stalls worsen with dataset size | DWAL absorbs bursts; merge cost is O(log n) |

## What You Pay

### File Size Is a Configurable Tradeoff

PsiTri's copy-on-write model creates duplicate nodes that persist until the
background compactor reclaims them. How aggressively the compactor reclaims
space is controlled by two runtime-configurable thresholds in `runtime_config`:

```cpp
// How much free space before compacting pinned (RAM-resident) segments
uint8_t compact_pinned_unused_threshold_mb = 4;    // default: 4 MB (aggressive — RAM is precious)

// How much free space before compacting unpinned (disk-resident) segments
uint8_t compact_unpinned_unused_threshold_mb = 16;  // default: 16 MB (lazy — disk is cheap)
```

The default settings prioritize **throughput and SSD longevity over disk space**:
cold segments on disk are only compacted when 50% of a 32 MB segment is free.
This avoids rewriting data that nobody is reading -- every compaction pass
copies live objects to new segments, which means **SSD write cycles spent on
data movement rather than useful work**. For hot segments in pinned RAM,
compaction is more aggressive because every byte of cache is valuable and
the rewrite doesn't touch the SSD at all.

The fundamental tradeoff is: **smaller files = more SSD wear**. Aggressive
compaction keeps the file tight, but every byte of live data gets rewritten
each time it's relocated. Over the lifetime of the drive, this write
amplification from compaction can exceed the amplification from the actual
application writes.

**Tuning for smaller files:** Set `compact_unpinned_unused_threshold_mb` lower
(e.g., 4-8 MB) to compact more aggressively. File size stays small, but SSD
wear increases proportionally -- the compactor rewrites live data more often.

**Tuning for maximum throughput and SSD life:** Set
`compact_unpinned_unused_threshold_mb` to 32 (= segment size) to effectively
disable cold compaction. File size grows but write throughput is maximized
and SSD wear is minimized. This is the right choice when storage is abundant
and you care about drive longevity.

**The sweet spot** depends on your hardware. NVMe drives with high endurance
ratings (e.g., data center drives with 3+ DWPD) can afford aggressive
compaction. Consumer SSDs with lower endurance benefit from lazier defaults.

The `compact_and_truncate()` API forces immediate full compaction and file
truncation when you need to minimize disk footprint. For distribution or
archival, an offline export can defragment and compress the database into a
minimal file -- useful for shipping snapshots, seeding new nodes, or cold
storage. The key insight is that **runtime file size and export size are
independent concerns**: run with lazy compaction for throughput and SSD life,
then produce a tight export when you need to move the data.

There's also a hidden benefit to the "extra" space: those COW duplicates are
**prior versions of your data**. Until the compactor reclaims a segment, the
old copies of overwritten nodes still exist on disk. In a corruption or
recovery scenario, these redundant copies provide a fallback -- the recovery
system can scan segments for prior versions of any object. Aggressive
compaction destroys this redundancy. Lazy compaction preserves it for free.

### Recovery Time Proportional to Database Size

The base COW engine recovers by scanning segments and rebuilding reference
counts -- recovery time scales with the total database, not with the amount
of unflushed data. For a 100 GB database, expect recovery to take minutes,
not seconds.

The DWAL layer mitigates this: committed transactions are replayed from WAL
files (fast), and only the base-layer recovery runs if the WAL is also lost
(power failure).

### Single Writer per Root

Each of the 512 roots supports one writer at a time. Multiple roots can be
written concurrently (fully parallel, no contention), but writes to the same
root are serialized. This is by design -- PsiTri's MVCC model gives each
writer exclusive ownership of its COW path.

For workloads that need concurrent writes to the same key space, shard across
multiple roots or use the DWAL layer's batching to absorb concurrent requests
into a single writer thread.

### C++20 Only

PsiTri is a C++20 library with no C API, Python bindings, or other language
support. The drop-in compatibility layers (RocksDB, MDBX, SQLite) provide
C and C++ APIs, but native PsiTri requires a C++20 compiler (Clang 15+ or
GCC 12+).

### No Compression

Data is stored uncompressed. For workloads with highly compressible values,
this means larger files compared to RocksDB (which supports Snappy, LZ4, Zstd).
The tradeoff is simpler code, faster reads (no decompression), and predictable
latency.

### Young Project

PsiTri is under active development. It has not been battle-tested in large-scale
production deployments. The API is stabilizing but may change. If you need a
database with a decade of production history, RocksDB and SQLite are proven
choices -- PsiTri's compatibility layers let you start there and migrate later.

## When to Use Something Else

**Pure append-only / time-series workloads:** If you never update existing keys
and only append new ones, an LSM tree (RocksDB, LevelDB) avoids COW overhead
entirely. PsiTri's DWAL narrows this gap but doesn't eliminate it.

**Tiny embedded databases (<10 MB):** SQLite is simpler, has zero dependencies,
runs everywhere, and has decades of testing. For small databases, PsiTri's
architectural advantages don't matter -- everything fits in cache regardless.

**Distributed databases:** PsiTri is a single-node embedded engine. It has no
replication, sharding, or consensus. Use it as a storage engine inside a
distributed system (like RocksDB inside CockroachDB), not as a distributed
database itself.

**SQL-first applications:** If you need full SQL (joins, aggregation, window
functions, query planning), use PostgreSQL, DuckDB, or SQLite directly. PsiTri's
SQLite compatibility layer passes 83% of the test suite but is not a full SQL
database.

## The Drop-In Compatibility Question

> "If PsiTri can mimic the RocksDB/MDBX/SQLite API and come out faster,
> what's the catch?"

The catch is the tradeoffs listed above: **larger files**, **C++20 only**,
**young project**. The performance advantage is real and comes from
architectural differences (node-level COW + ART-buffered DWAL), not from
cutting corners on durability or correctness.

The compatibility layers also don't cover every feature:

- **RocksDB:** No merge operators, no bloom filters, no compression. Column
  families work (mapped to PsiTri roots).
- **MDBX:** No nested transactions via the MDBX API. No multi-DBI atomicity
  (each DBI commits independently).
- **SQLite:** No `sqlite3_backup` API, no incremental blob I/O. Database is a
  directory, not a single file. 83% test suite compatibility.

The compatibility layers are a **migration path**, not a permanent destination.
They let you verify PsiTri's performance on your workload with zero code changes.
Once validated, the native API unlocks features that have no equivalent in other
engines: subtrees as values, O(log n) range operations, and 512 independent
tables with zero-cost snapshots.

## Benchmark Reproducibility

Every benchmark claim on this site includes:

- **Exact reproduction commands** -- copy, paste, run
- **Machine specs and date** -- so you know the hardware context
- **Raw data files** -- CSV data checked into the repository under `docs/data/`
- **Comparison against real engines** -- not toy implementations or straw men

Run `bench/run_all.sh --dry-run` to see what benchmarks are available, or
run a specific suite to verify results on your own hardware.
