# Caching, Compaction & Beyond-RAM Scaling

PsiTri databases can grow far larger than available RAM while maintaining high performance on the hot working set. This page explains how.

## Why PsiTri Scales Beyond RAM

Many embedded databases are described as "memory-mapped" but perform poorly once the dataset exceeds RAM because they rely on the OS page cache with no control over what stays resident. PsiTri takes a fundamentally different approach: it **actively manages which data stays in RAM** at object granularity while letting the OS handle the rest.

Three mechanisms work together:

1. **MFU-based physical data tiering.** PsiTri physically relocates hot objects into `mlock`'d segments that are guaranteed to stay in RAM. Cold objects live in unpinned segments that the OS can page out. Unlike a traditional buffer pool, this operates at individual object granularity (objects up to 4 KB are eligible for promotion), not fixed 4-16 KB pages -- so pinned memory holds only the data that matters.

2. **Write protection enables clean eviction.** All committed segments are marked `PROT_READ` via `mprotect`. Read-only pages are cheap for the OS to evict: they can be dropped instantly and re-faulted from the memory-mapped file with no writeback. In contrast, dirty pages in a traditional database must be flushed to disk before eviction, creating I/O stalls under memory pressure.

3. **Copy-on-write keeps writes sequential.** New data is always appended to the current write segment. This means page faults from writes are sequential (one fault per new segment, amortized over 32 MB of data), not random. Sequential I/O is 10-100x faster than random I/O on both SSDs and HDDs.

The practical effect: a PsiTri database with 100 GB of data and 8 GB of RAM performs nearly as well as a fully in-memory database for the hot working set, while gracefully degrading to disk speed for cold data. There is no cliff -- performance scales smoothly with the ratio of hot data to available RAM.

### Comparison with Other Approaches

| Approach | Granularity | Eviction control | Write path | Beyond-RAM behavior |
|----------|-------------|-----------------|------------|-------------------|
| **OS page cache** (LMDB, BoltDB) | 4 KB pages | None (OS LRU) | Random I/O | Full table scan evicts working set |
| **Buffer pool** (PostgreSQL, MySQL) | 8-16 KB pages | LRU/clock sweep | Random I/O | Requires manual tuning of pool size |
| **Block cache** (RocksDB) | 4-64 KB blocks | LRU with priority | Sequential (LSM) | Good for writes, reads compete with compaction |
| **PsiTri MFU tiering** | Per-object (up to 4 KB) | Frequency-based, self-tuning | Sequential (COW) | Hot set guaranteed in RAM, cold set paged by OS |

---

The goal of caching is to minimize the number of disk page swaps because they hurt both performance and SSD wear. Frequently modified pages and most frequently read data should reside on memory segments pinned to RAM. Data should be grouped with other data that is accessed at similar frequency so that when a page swap does occur it isn't bringing along unnecessary data.

## Copy on Write

PsiTri uses a copy-on-write approach which enables all writes to be contiguous and prevents modification of data at rest. Recently written nodes are also likely to be modified again. This is both faster than scattered writes and easier to sync to disk.

## Segment Architecture

PsiTri manages memory in blocks of 32 MB. Each thread writes in append-only mode to its own 32 MB buffer. Once full and synced to disk the buffer is considered immutable and any attempt to modify a node in the buffer results in a COW. This COW is the equivalent of forcing writes from disk cache into memory cache.

## Compaction

Statistics are gathered as data is freed from each segment so that a segment can be recycled once a certain percentage is empty. This groups "modified" data and "static" data together. Compaction produces write amplification as data is copied from one place to another; however, there is minimal write amplification for compacting data from one pinned memory region to another.

Empty space in pinned segments wastes precious cache, so the compactor is more aggressive about compacting pinned memory segments (defragmenting when ~12.5% of a segment is free -- 4 MB of 32 MB), but more lazy about compacting unpinned segments (requiring ~50% free -- 16 MB of 32 MB) because disk space is relatively plentiful.

## Most Frequently Used (MFU) Caching

A linear scan can thrash an LRU cache by keeping data that will never be seen again. Even completely random access evicts likely-needed nodes for merely-recent nodes. PsiTri organizes data pinned in memory by frequency of access, minimizing page swaps.

Because the operating system already implements an LRU caching algorithm for unpinned pages, the result is a hybrid where PsiTri controls the percentage of memory reserved for most-frequently-used data vs most-recently-used data.

### Probabilistic Promotion

Every time a node is read (via a lookup) there is a "1 in N" chance it will set the node's 'read bit'. If the read bit was already set, it sets a promotion flag and pushes the address into a per-session queue for the compactor to pick up.

The compactor moves these objects to pinned write segments in the background, with little impact on query performance.

Over time all data would accumulate read bits, except that a background thread periodically clears them. For an item to be promoted to cache it must be read an average of N^2 times between clearing cycles (probability 1/N per read, requiring two hits).

### Difficulty Adjustment

The promotion difficulty self-tunes using the same principle as Bitcoin mining:

- **Promoting too fast** (filled 1/16 of cache budget before 1/16 of the time window): Increase difficulty (multiply gap by 7/8)
- **Promoting too slow** (1/16 of window elapsed without filling budget): Decrease difficulty (multiply gap by 9/8)

Larger objects are proportionally harder to promote, preventing a few large objects from monopolizing the cache.

## Segment Lifecycle

All new segments start under `mlock()`. They lose their mlock status when the compactor finishes copying the segment to a new one and pushes it into readlock purgatory.

```
young  |----     Hot   -|-   Warm  -|----     Cold      ----|- Prunable Tail  -| old
       |                            |                       |                  |
 age   |----     mlock          ----|----  MADV_RANDOM  ----|-- OS-managed   --| age
       |                                                                       |
       |-      Lowest Age Alloc     -|- Alloc Earliest -|- Alloc Earliest     -|
       |                             |                                         |
       |--- minimal compacted size --|------  prunable with compact -----------|
```

- **Hot segments**: Not considered for read-bit promotion (would cause unnecessary copying)
- **Warm segments**: Data must be read N^2 times before aging out to stay in pinned memory
- **Cold segments**: Subject to OS LRU caching
- **Prunable tail**: Can be truncated to reclaim disk space

## Age of Compacted Data

Each segment has two ages: one for recovery (version control) and a **virtual age** for the caching algorithm. The compactor creates synthetic virtual ages as the weighted average of the objects being copied. This minimizes the impact on sort order caused by compacting pinned segments.

## Segment Reuse Priority

As the database adds and removes data, segments closer to the start of the file have priority for reuse (to enable file truncation). Segments within the minimal compacted size are prioritized by youngest virtual age for allocation.

If there are no empty segments within the minimal region, the allocator picks the segment closest to the head of the file. As a last resort, the file grows.

## Preventing Blocking Delays

Calls like `mlock()` and `munlock()` can block. A background thread proactively:

- Maps segments into memory before they are needed
- Keeps spare pinned memory segments ready to be assigned
- Dynamically adjusts reserve sizes based on allocation demand

The compactor never blocks on file growth or memory management -- a separate background thread handles these operations.
