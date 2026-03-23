# PsiTrie: A New Design Point in Database Engineering

PsiTrie is a persistent, transactional key-value store that combines ideas from radix tries, B-trees, garbage collectors, and proof-of-work mining into a system that is fundamentally different from existing databases. This document explains what makes it unique and why those differences matter.

## The Problem with Existing Approaches

Every persistent key-value store makes the same set of tradeoffs:

- **B-trees** (LMDB, BoltDB, SQLite, InnoDB) operate on fixed-size pages (4KB-16KB). Copy-on-write means copying an entire page to change a single byte. Write amplification is proportional to page size, not data size.
- **LSM trees** (RocksDB, LevelDB, Cassandra) batch writes for throughput but pay for it with read amplification, compaction stalls, and space amplification from tombstones.
- **Adaptive radix tries** (ART) achieve optimal depth and cache-line efficiency, but only in memory. No persistent ART implementation exists that matches B-tree durability.

PsiTrie eliminates these tradeoffs by combining three novel subsystems:

1. **A persistent trie-B-tree hybrid with batched leaves and 67-byte copy-on-write**
2. **A relocatable object allocator with lock-free O(1) compaction moves**
3. **A self-tuning physical data layout that sorts objects by access frequency**

---

## 1. Node-Level Copy-on-Write: 60x Less Write Amplification

### The Insight

Copy-on-write (COW) is the foundation of MVCC, snapshot isolation, and crash safety. But in every existing COW database, the unit of copying is a **page** — 4KB to 16KB of data. When you change one key, you copy 4KB at every level of the tree. For a 4-level B-tree, that's 16KB of writes for a single byte change.

PsiTrie's inner nodes average **67 bytes**. Copy-on-write copies 67 bytes per level, not 4KB. For a 5-level trie, a single mutation writes ~335 bytes total — roughly **60x less** than a page-level COW B-tree.

This is made possible by a novel encoding:

### Cacheline-Shared Branch Encoding

Every persistent data structure that uses indirection (control blocks, page tables, object headers) pays for it in **cache line loads**. When a tree node has 64 children, dereferencing their control blocks touches 64 separate cache lines — each requiring a separate memory fetch. On modern hardware, each cache line load transfers 64-128 bytes from DRAM. For a single inner node traversal, that's 4-16 KB of memory bandwidth consumed just to read reference counts and locations, most of which is wasted padding around the 8 bytes you actually need.

PsiTrie solves this by **engineering the allocator to co-locate sibling nodes within shared cache lines**. This is not an observation about natural locality — it is a deliberate allocation strategy. When a node splits or children are allocated, the allocator receives **hints** (the parent's existing cacheline addresses) and preferentially places new objects adjacent to their siblings in the control block array. The result: siblings that share control block cache lines.

The inner node encoding exploits this guarantee. Instead of storing N separate 8-byte addresses, it stores up to 16 **cacheline base addresses** (8 bytes each) and encodes each branch as a **1-byte index** (4 bits selecting which cacheline, 4 bits selecting which slot within that cacheline):

```
Traditional:  16 branches × 8 bytes = 128 bytes of pointers, 16 cache line loads
PsiTrie:      4 cachelines × 8 bytes + 16 branches × 1 byte = 48 bytes, 4 cache line loads
```

The space savings (48 bytes vs 128 bytes) enables compact inner nodes, but the real win is in **memory bandwidth**: dereferencing 16 children now loads 4 cache lines instead of 16. For nodes with 64-256 children, the reduction is even more dramatic — touching 4-16 cache lines instead of 64-256. Every retain, release, and location lookup on those children benefits from the cache lines already being hot.

This encoding supports up to 256 branches (16 cachelines × 16 slots) in a node that fits in 1-2 cache lines. The result: inner nodes averaging 67 bytes with 8-9 branches each, achieving B-tree-class fan-out in trie-class space with minimal cache pressure.

### Trie-B-tree Hybrid with SIMD Routing

PsiTrie is a cross between a trie and a B-tree. Each inner node stores a sorted array of 1-byte **dividers** and an array of child branches — structurally similar to a B-tree node, but with a critical constraint: dividers are exactly one byte.

This constraint exists because it enables **constant-time branch identification via SIMD**. A single SIMD instruction can compare the search byte against all dividers in the node simultaneously, identifying the correct branch in O(1) regardless of fan-out. B-trees with variable-length keys must binary search their dividers — O(log fan-out) comparisons, each potentially touching multiple cache lines of key data. PsiTrie's 1-byte dividers fit in a single vector register, making branch selection as fast as a table lookup.

The trie-like depth comes from **path compression**. When a group of keys shares a common prefix, an `inner_prefix_node` stores the shared prefix once and strips it before delegating to a child node that routes on the next divergent byte. This is the same path compression technique used in Patricia tries and adaptive radix trees, applied to the B-tree-style sorted inner nodes:

```
inner_prefix_node("user/")
  └── inner_node [dividers: 'a', 'd', 'z']   ← SIMD compares search byte vs ['a','d','z']
        ├── branch 0: keys < 'a'
        ├── branch 1: keys in ['a', 'd')
        ├── branch 2: keys in ['d', 'z')
        └── branch 3: keys >= 'z'
```

The result is a hybrid that takes the best of both designs: B-tree-style sorted routing at each level (up to 256-way fan-out, SIMD-accelerated) with trie-style path compression across levels (O(key_length) depth, shared prefix elimination). Inner nodes never store full keys — just single-byte dividers — keeping them compact enough for node-level COW.

### Batched Leaves: Avoiding Trie Depth

A classical trie's fatal flaw is depth: one level per byte of key, so a 32-byte key requires 32 pointer dereferences. This is why pure tries are impractical for persistent storage despite their theoretical elegance.

PsiTrie avoids this by terminating the trie early at **batched leaf nodes** that pack up to **512 keys** using sorted binary search with hash-accelerated filtering. In practice, leaves hold ~58 keys each. Once the trie routes to the correct leaf, the leaf performs a binary search over its remaining key suffixes — the same strategy a B-tree page uses, but at the end of a trie-like path instead of at every level.

The result: 30M keys in only 5 levels of depth, compared to the 20-40 levels a pure trie would require for typical key lengths. Path compression collapses shared prefixes in the inner nodes; batched leaves collapse the remaining key suffixes into dense, searchable pages. The trie provides efficient routing, but the leaves do the heavy lifting.

| Metric              | B-tree (LMDB)   | Pure ART        | PsiTrie         |
|---------------------|-----------------|-----------------|-----------------|
| COW unit            | 4KB page        | N/A (in-memory) | 67 bytes        |
| Leaf keys           | 50-200 per page | 1               | ~58 per node    |
| Depth (30M keys)    | 3-4             | 5-8             | 5               |
| Write amplification | ~20KB/mutation  | N/A             | ~335B/mutation  |
| Persistent          | Yes             | No              | Yes             |

---

## 2. The Segment Allocator: Relocatable Persistent Memory

### The Problem with Persistent Allocation

Persistent data structures face a fundamental tension: you need stable addresses for pointers, but you also need to compact and reorganize data as it fragments. Traditional approaches:

- **B-trees** avoid the problem by using fixed-size pages. Fragmentation accumulates within pages.
- **LSM trees** avoid it by making files immutable. Compaction rewrites entire files.
- **Garbage collectors** solve it by tracing all references and updating all pointers. This requires stop-the-world pauses or expensive read barriers.

SAL (Segment Allocator Library) solves this with a **single level of indirection** that makes any object relocatable in O(1) time, lock-free, with zero impact on concurrent readers and writers.

### Control Block Indirection

Every allocated object has a permanent 32-bit ID (`ptr_address`). This ID indexes into an array of atomic 64-bit **control blocks** (named for their analogous role in `std::shared_ptr`, where a separate control block tracks the reference count and pointer to the managed object):

```
ptr_address (32-bit permanent ID)
     │
     ▼ array lookup (O(1))
control_block (64-bit atomic)
  ┌──────────────────────────────────────────────────────────────┐
  │ ref_count (21 bits) │ location (41 bits) │ MFU/flags (2 bits) │
  └──────────────────────────────────────────────────────────────┘
     │
     ▼ direct memory access
Object Data (at physical location in memory-mapped segment)
```

The location field is addressed in units of 64-byte cache lines, so 41 bits address 2^41 × 64 = **128 TB** of total database space — comparable to a 47-bit pointer — while fitting the entire control block in a single atomic 64-bit word. (See Section 9 for how storage, address space, and key count limits interact at scale.)

To **move an object**, the compactor:
1. Copies the object data to a new location
2. Atomically CAS-swaps the location in the control block

That's it. One atomic instruction. No pointer updates anywhere in the trie. No tracing. No stop-the-world pause. Concurrent readers that already resolved the old location continue reading valid data (the old copy persists until all read locks release). New readers pick up the new location.

| System              | Object Relocation Cost                | Concurrent? | Persistent? |
|---------------------|---------------------------------------|-------------|-------------|
| Java ZGC            | Read barrier on every pointer access  | Yes         | No          |
| Go GC               | Stop-the-world + pointer updates      | Partially   | No          |
| LMDB                | Cannot relocate (offline copy only)   | N/A         | Yes         |
| PostgreSQL VACUUM   | O(indexes) per row                    | Limited     | Yes         |
| RocksDB compaction  | Rewrite entire SST file               | Background  | Yes         |
| **SAL**             | **memcpy + 1 atomic CAS**             | **Fully**   | **Yes**     |

### Why This Matters

Cheap relocation unlocks capabilities that are impossible or prohibitively expensive in other systems:

- **Online defragmentation**: The compactor runs continuously, reclaiming space without disrupting operations
- **Physical access-pattern optimization**: Objects can be moved to where they'll be accessed fastest (see Section 3)
- **No fragmentation cliff**: Unlike page-based allocators that degrade under mixed workloads, SAL maintains consistent performance through continuous background reorganization

---

## 3. Self-Organizing Physical Layout: Bitcoin-Inspired Cache Management

### The Insight

Most databases delegate caching to the OS page cache (via mmap) or manage a fixed-size buffer pool. Both approaches have fundamental limitations:

- **OS page cache**: Uses LRU (recency), not frequency. A full table scan evicts your entire hot working set. Operates at 4KB page granularity — one hot object pins an entire page of cold neighbors.
- **Buffer pools**: Fixed size, page granularity, require manual tuning. PostgreSQL's `shared_buffers`, MySQL's `innodb_buffer_pool_size` — get the number wrong and performance degrades.

PsiTrie does something different: it **physically relocates individual objects** between RAM-guaranteed (mlocked) segments and pageable segments based on observed access frequency. Over time, the physical layout of the database file converges to match the actual workload.

### Object-Granularity MFU Tracking in 2 Bits

Tracking access frequency for millions of objects normally requires per-object counters or timestamps — expensive in both space and maintenance. SAL tracks frequency with **2 bits per object**, embedded in the existing control block at zero additional space cost:

- **`active` bit**: Set when the object is read
- **`pending_cache` bit**: Set when the object is read while already active

A background thread clears these bits on a rolling 60-second window. Objects that get re-marked between decay cycles are "hot." This is effectively a frequency estimator using two bits and a time window — it distinguishes "accessed once" from "accessed repeatedly" with minimal overhead.

### Bitcoin-Inspired Difficulty Adjustment

The cache promotion system faces the same problem as Bitcoin mining: how do you maintain a target rate of work (cache promotions) when the input rate (object accesses) varies unpredictably?

The answer is the same: **adaptive difficulty adjustment**.

Every access to a hot object rolls a probabilistic check:

```
should_promote = random_value >= (cache_difficulty × object_size_in_cachelines)
```

The `cache_difficulty` parameter is a 64-bit threshold that controls how hard it is for an object to earn promotion into a pinned (mlocked) segment. Like Bitcoin's difficulty, it adjusts based on observed throughput:

- **Promoting too fast** (filled 1/16 of cache budget before 1/16 of the time window elapsed): **Increase difficulty** — multiply the gap by 7/8, making promotion harder
- **Promoting too slow** (1/16 of the window elapsed without filling the budget): **Decrease difficulty** — multiply the gap by 9/8, making promotion easier

This is directly modeled on Bitcoin's proof-of-work difficulty adjustment, applied to cache management instead of block production. The result:

- **No configuration needed**: The system self-tunes to available RAM and workload
- **Stable promotion rate**: Regardless of whether the workload is 1K reads/sec or 1M reads/sec, the cache promotion rate converges to fill available pinned memory at a steady pace
- **Size-aware**: Larger objects are proportionally harder to promote, preventing a few large objects from monopolizing the cache
- **Cheap random sampling**: A single random number is generated per tree traversal and tested against the difficulty threshold at each node. Relaxed CAS on the activity bits (no retry on failure) adds additional natural jitter — failed updates are silently dropped, further randomizing which objects get tracked

### Physical Data Tiering

The compactor continuously moves objects between storage tiers:

```
┌──────────────────────────────────────────┐
│         Pinned Segments (mlock'd)        │
│   Hot objects — guaranteed in RAM         │
│   Promoted by passing difficulty check    │
│   Evicted when access bits decay          │
├──────────────────────────────────────────┤
│        Unpinned Segments (pageable)       │
│   Warm/cold objects — OS decides paging   │
│   Objects demoted here as they cool       │
├──────────────────────────────────────────┤
│           Freed Segment Space             │
│   Reclaimed by background compaction      │
└──────────────────────────────────────────┘
```

- **Promotion**: Hot objects are copied into mlocked segments (guaranteed RAM), controlled by difficulty
- **Demotion**: When pinned segments exceed the configured limit, the oldest (lowest virtual age) segment is munlocked and its contents become pageable
- **Self-organization**: Over time, the database's physical layout mirrors its access pattern. The working set clusters in RAM-pinned segments. Cold data drifts to pageable segments where the OS can evict it under memory pressure.

No other database physically relocates individual objects between RAM-guaranteed and pageable storage based on observed access frequency.

The obvious objection is write amplification — doesn't moving objects around wear out your SSD? In practice, the user controls this directly. The decay window duration and sync strategy together determine the rate of physical writes to disk. Data residing in pinned (mlocked) RAM is not synced by the OS under normal operation — only an explicit `msync` or `fsync` flushes it. Between syncs, promotions and demotions are pure memory operations with zero disk I/O.

When a sync does occur, the engineered co-location of sibling objects pays off again: related nodes cluster within the same pages, so a dirty page flush writes many useful objects together rather than burning an entire SSD erase block (typically 256KB-1MB) for a single 67-byte node. The user can tune the tradeoff between durability (sync frequency) and SSD wear to match their hardware and workload.

---

## 4. O(log n) Range Operations via Subtree Metadata

### Descendants Invariant

Every inner node in the trie maintains a `_descendents` field: the total number of keys in its entire subtree. This 39-bit counter (up to ~550 billion keys) is maintained during every insert, remove, and split operation. See Section 9 for how this interacts with other scaling limits.

This invariant enables two operations that are O(k) or impossible in other databases:

### Range Counting in O(log n)

`count_keys(lower, upper)` counts keys in a range without touching any leaves. The algorithm routes through the trie, summing `_descendents` for fully-contained subtrees (O(1) per subtree) and only recursing into the two boundary branches. Total work: O(log n) regardless of how many keys are in the range.

Most databases must scan every key in the range to count them. A `SELECT COUNT(*) WHERE key BETWEEN x AND y` in PostgreSQL or a range iterator in RocksDB is O(k) in the result size.

### Range Deletion in O(log n)

`remove_range(lower, upper)` deletes all keys in a range using the same routing logic as counting, but releasing fully-contained subtrees instead of counting them. Subtree release is O(1) — just decrement the root's reference count. The compactor handles cascading destruction asynchronously.

Deleting 900,000 out of 1,000,000 keys visits only O(log n) nodes along the two boundary paths. The interior of the range is never touched.

| Operation                  | B-tree            | LSM-tree               | PsiTrie                  |
|----------------------------|-------------------|------------------------|--------------------------|
| Count keys in range        | O(k) scan         | O(k) scan + tombstones | **O(log n)**             |
| Delete range               | O(k) deletes      | O(k) tombstones        | **O(log n)**             |
| Delete range space reclaim | Immediate but O(k) | Background compaction | **O(1) deferred release** |

---

## 5. Performance

### Single-Threaded Insert Throughput (30M Random Keys, Persistent)

| Keys Inserted | Inserts/sec | Tree Depth | Total Nodes |
|--------------:|------------:|-----------:|------------:|
|          100K |   3,731,343 |          3 |      35,902 |
|         1.1M  |   1,231,527 |          5 |   1,789,712 |
|         3.0M  |   1,129,943 |          5 |   4,323,554 |

Throughput degrades logarithmically — no cliff edges from compaction stalls or write amplification spikes.

### Tree Shape at 30M Keys

| Metric              | Value                                |
|---------------------|--------------------------------------|
| Max depth           | 5                                    |
| Inner nodes         | 67,075 (98% with prefix compression) |
| Leaf nodes          | 514,746 (~58 keys each)              |
| Avg inner node size | 67 bytes                             |
| Value nodes         | 0 (all values inlined)               |

### Why It's Fast

1. **Depth 5** for 30M keys — only 5 pointer dereferences per lookup
2. **67-byte COW** — copies less data per mutation than most systems copy per page header
3. **Batched leaves** — amortizes metadata overhead across ~58 keys
4. **Zero value nodes** — values up to 64 bytes inline in the leaf, no extra indirection
5. **Lock-free reads** — MVCC via atomic ref counts, no reader-writer contention
6. **Self-tuning cache** — hot nodes migrate to pinned RAM automatically

---

## 6. Composable Trees: Subtrees as First-Class Values

### The Problem

Databases model hierarchical data awkwardly. Relational databases flatten trees into rows with parent pointers and require expensive recursive CTEs to reconstruct them. Document databases (MongoDB, CouchDB) embed nested documents as opaque blobs — you can't index or query into them without application-level logic. Key-value stores force you to encode hierarchy into key prefixes, losing the ability to atomically manipulate a subtree as a unit.

What if a tree node could store another tree as its value?

### Subtrees as Values

PsiTrie allows any key's value to be **a pointer to another tree root**. A leaf node entry can hold either inline data (up to 64 bytes), a reference to a value_node (larger data), or a **subtree reference** — a pointer to an independent trie that is a fully functional tree in its own right.

This creates composable, hierarchical data structures:

```
Root tree
├── "users/alice" → { inline data }
├── "users/bob"   → { inline data }
├── "indexes"     → [subtree] ──→ Index tree
│                                  ├── "age:25" → ...
│                                  ├── "age:30" → ...
│                                  └── "name:alice" → ...
└── "metadata"    → [subtree] ──→ Metadata tree
                                   ├── "schema_version" → "3"
                                   └── "created_at" → "2024-01-15"
```

### Zero-Cost Ownership via Reference Counting

The ownership model is simple and efficient:

1. **Storing a subtree** transfers one reference count from the caller to the leaf. The tree takes ownership — no extra retain needed.
2. **Reading a subtree** returns a read cursor rooted at the subtree — the parent leaf's reference keeps the subtree alive.
3. **Replacing a subtree** value releases the old subtree root. Cascading destruction of the entire old subtree is handled asynchronously by the compactor.
4. **Deleting a key** with a subtree value releases the subtree root, triggering the same deferred cascading destruction.
5. **Copy-on-write** applies to subtrees naturally. When a tree is shared (snapshot), modifying a subtree COWs up through both the subtree and the parent tree's path to the leaf.

Because subtree roots participate in the same reference-counting and `visit_branches()` protocol as all other node references, they inherit all existing guarantees: crash-safe COW, lock-free concurrent reads, and automatic cleanup through the compactor.

### Why This Matters

| Capability                   | RDBMS                 | Document DB             | KV Store             | PsiTrie                          |
|------------------------------|-----------------------|-------------------------|----------------------|----------------------------------|
| Hierarchical data            | Flattened + joins     | Embedded blobs          | Key-prefix encoding  | Native subtrees                  |
| Atomic subtree operations    | Requires transactions | Replace entire document | Not possible         | O(1) root pointer swap           |
| Subtree snapshot             | Full table copy       | Full document copy      | Not possible         | O(1) ref count increment         |
| Subtree deletion             | O(n) row deletes      | Replace document        | O(n) key deletes     | O(1) release + deferred cascade  |
| Independent subtree indexing | Separate tables       | Application-level       | Application-level    | Each subtree is a full trie      |

Subtrees enable patterns that are impossible or prohibitively expensive in other systems:
- **Isolated namespaces** with independent key spaces sharing one database file
- **Snapshot branching** — fork a subtree for speculative writes, merge or discard in O(1)
- **Recursive data structures** — trees of trees with consistent ownership semantics at every level

---

## 7. Crash Recovery Without a Write-Ahead Log

### The Problem

Every COW database must answer: what happens when the process crashes? The standard answer is a write-ahead log (WAL) — write your intent to a journal before mutating data, then replay the journal on recovery. WALs work, but they double write amplification and add fsync latency to every transaction.

PsiTrie has no WAL. Recovery is built into the data layout itself.

### Why Control Blocks Can't Survive a Crash

Control blocks track two things: where each object lives (location) and how many references point to it (ref count). Both become untrustworthy after a crash, for different reasons.

**Reference counts include in-flight stack references.** Every `smart_ptr` on the stack or in user code holds a reference count in the control block. When the process crashes, those destructors never run — the corresponding `release()` calls never happen. Ref counts are left permanently inflated by however many `smart_ptr` instances were in flight. This is the same fundamental problem as `std::shared_ptr`: if the process dies, the control block survives but the stack-held references that were supposed to decrement it are gone forever.

**Control blocks are pinned to RAM to avoid SSD wear.** Control blocks change on every retain, release, and relocation — far too frequently to flush to disk. They are mlocked in RAM and intentionally excluded from normal sync operations. This eliminates SSD write amplification from ref count churn, but it means control block state is **volatile by design**. On clean shutdown, a `clean_shutdown` flag is set — this tells the next open that the control blocks in RAM are consistent and can be reloaded as-is without regeneration. On crash, the flag remains unset and the control blocks must be rebuilt from segments.

### How Recovery Works

The segments — append-only, ordered by allocation sequence — are the durable source of truth. Recovery rebuilds the control blocks from segments in three phases:

**Phase 1: Rebuild locations.** Clear all control block metadata. Sort segments newest-to-oldest by `_provider_sequence`. Scan each segment's objects sequentially. For each object ID encountered:
- If unseen → record its location, set ref count to 1
- If already mapped from a newer segment → skip (newer copy wins)
- If in the same segment → update (within a segment, later offset = newer)

This exploits the append-only invariant: segments never modify existing data, so scanning order determines which copy is authoritative.

**Phase 2: Retain reachable nodes.** Starting from all top roots, recursively traverse the reachable tree using `visit_branches()`. Each reachable node gets `retain()`, bumping its ref count to 2+. Unreachable nodes (leaked by the crash) remain at 1.

**Phase 3: Release unreachable.** Decrement every ref count by 1. Objects that drop to 0 are freed — their segment space becomes reclaimable. Reachable objects settle to their correct ref counts.

### Hardware-Enforced Corruption Prevention

Most databases trust application code not to scribble on committed data — a single stray pointer write can silently corrupt a B-tree page, and the corruption may not be detected until much later (or never). PsiTrie eliminates this entire class of bugs: all newly written data is marked read-only via `mprotect(PROT_READ)` **before** the atomic root pointer swap that makes it visible. By the time any reader can reach the data, it is already hardware-protected against writes.

This means committed data is physically unwritable at the hardware level. Any subsequent access — whether from a bug, a buffer overrun, or a dangling pointer — triggers an immediate CPU fault (SIGSEGV) rather than silent corruption. If the app crashes before the root swap, the data is unreachable and reclaimed during recovery — never corrupt. The writable region of each segment is tracked by an atomic `_first_writable_page` boundary:

```
Segment layout:
┌──────────────────────────────┬─────────────────────┐
│  Committed (PROT_READ)       │  Active (PROT_R|W)  │
│  mprotect'd after sync       │  New allocations     │
│  Stray write = SIGSEGV       │  only here           │
├──────────────────────────────┼─────────────────────┤
page 0            _first_writable_page          _alloc_pos
```

Legitimate modifications go through `smart_ptr::modify()`, which checks `can_modify()` on the object's location. If the object is in a read-only region, it automatically triggers copy-on-write to a writable segment — no application code needs to be aware of the protection boundary.

This is a multi-layer defense:

| Layer           | Mechanism                       | Effect                                         |
|-----------------|----------------------------------|------------------------------------------------|
| OS / MMU        | mprotect(PROT_READ)             | Stray writes fault immediately                 |
| Segment         | `_first_writable_page` boundary | Tracks mutable vs immutable regions            |
| Session         | `can_modify()` ownership check  | Only the allocating session can write           |
| Object          | `smart_ptr::modify()` COW       | Read-only objects copied to writable segments  |

This protection is enabled by default (`write_protect_on_commit = true`) even when disk sync is disabled, and is configurable across five sync levels from mprotect-only (no disk I/O) through full fsync.

Read-only protection also benefits OS paging behavior. Read-only pages are never dirty, so the OS can evict them instantly under memory pressure — no write-back to disk required. Dirty (writable) pages must be flushed before their physical frames can be reused, causing I/O storms under pressure. Since the vast majority of a PsiTrie database is committed (read-only) data, the OS can efficiently page the cold portions in and out with zero write amplification, while the self-tuning cache (Section 3) keeps hot data pinned in RAM.

The result: committed data is physically immune to application-level corruption bugs, with the MMU enforcing the invariant at hardware speed, while simultaneously being more efficient for the OS to manage.

### Why No WAL?

The segments themselves serve as the recovery log:

- **Append-only writes** guarantee that committed data is never overwritten
- **`_provider_sequence` ordering** across segments disambiguates which copy of an object is newest — the same function a WAL's LSN (log sequence number) serves
- **`visit_branches()` reachability walk** reconstructs ref counts from the tree structure — no need to journal individual retain/release operations

For **app crashes** (process dies, OS stays up), recovery is only needed to **reclaim leaked memory** — not for correctness or data protection. The mprotect + root-swap protocol guarantees that committed data is never corrupt. The tree is always in a consistent state; the only consequence of skipping recovery is that unreachable objects (from in-flight transactions that never completed their root swap) occupy space until reclaimed.

For **hardware failures or power loss**, the OS can no longer guarantee that mmap'd pages were flushed to disk. In this case, recovery must also rebuild object locations from segments, since control blocks and even recently written segment data may be partially flushed or torn. The configurable sync level (Section 7) determines how much data is at risk: `sync_type::none` relies entirely on the OS and may lose recent writes; `sync_type::full` (fsync/F_FULLFSYNC) guarantees durability to physical media at the cost of write latency. In either case, the append-only segment structure ensures that recovery converges to a consistent state — it may lose uncommitted work, but it will never produce a corrupt tree. The append-only design also means all writes are sequential within a segment — minimizing the number of dirty pages and enabling efficient SSD flush patterns, unlike B-tree page-level COW which scatters writes across the file.

The tradeoff: recovery time is proportional to database size (must scan segments + walk reachable tree), not proportional to the amount of uncommitted work. For the expected use case — long-lived databases with infrequent crashes — this is favorable: zero overhead during normal operation in exchange for a slower recovery path that only runs after a crash.

---

## 8. Lock-Free Multi-Writer Allocation

### The Problem

Concurrent writers in most databases contend on shared structures: B-tree page latches, WAL append locks, free-list mutexes. Even "lock-free" designs often funnel all writers through a single atomic counter or append point, creating a serialization bottleneck under high concurrency.

PsiTrie's allocator eliminates writer contention at three levels: node data, control block IDs, and object release.

### Node Data: Per-Session Segments

Each writer session owns its own active segment — a contiguous memory region where new objects are appended. Allocation is a simple pointer bump into that session's segment, with no locks and no contention between writers.

When a session's segment fills, it grabs a pre-initialized replacement from a ready queue. A background **segment provider thread** keeps this queue stocked by allocating, formatting, and optionally mlocking new segments ahead of demand. Writers never block on file I/O, mmap, or system calls during allocation.

```
Writer A:  [segment 7 ████████░░░░]  ← bump allocate here
Writer B:  [segment 12 ██████░░░░░░]  ← bump allocate here, independently
Writer C:  [segment 3 ██████████░░]  ← about to grab next segment from ready queue

Background provider: [...] → [seg 15] → [seg 16] → [seg 17]  (pre-initialized queue)
```

### Control Block IDs: Zone-Striped Random Probing

The control block address space is divided into **zones** of ~4 million IDs each. Each zone has its own atomic allocation counter and a bitmap tracking free slots.

**Zone selection** uses a lazily-maintained `min_alloc_zone` pointer that tracks the least-filled zone without scanning on every allocation. On allocate, if the current min zone's count exceeds the per-zone average, a rescan finds the true minimum. On free, if the freed zone drops below the current min, it becomes the new min. This is approximate — multiple threads may briefly disagree on which zone is least full — but that's harmless: it spreads allocations across zones rather than funneling them into one.

**Within a zone**, each thread uses a **thread-local RNG** to pick a random cacheline-aligned chunk of the free bitmap (512 bits / 64 bytes), then uses SIMD (`max_pop_cnt8_index64`) to find the byte within that chunk with the most free slots. A single CAS claims a bit. Since threads land on different cacheline-aligned bitmap words, CAS collisions are rare even under high concurrency.

When callers provide **allocation hints** (parent node's existing cacheline addresses), the allocator tries those cachelines first — achieving the engineered sibling co-location described in Section 1 without creating a contention hotspot. If hints are exhausted, it falls back to the zone-striped random probe.

New zones are pre-allocated by the background provider thread when average zone fill exceeds 50%, so writer threads never block on capacity expansion.

### Object Release: Per-Session Queues

When a writer releases an object (e.g., the old copy after COW), it pushes the address into its own **per-session release queue** — no contention with other writers. The compactor thread drains these queues asynchronously, batching the actual free-list updates and segment space reclamation.

### Why This Matters

| Operation          | Traditional (B-tree/LSM)          | PsiTrie                                    |
|--------------------|-----------------------------------|--------------------------------------------|
| Data allocation    | Page latch or WAL append lock     | Per-session segment bump (zero contention) |
| ID/address alloc   | Global free-list mutex            | Zone-striped random CAS (rare collisions)  |
| Object release     | Shared free-list lock             | Per-session queue (zero contention)         |
| Capacity expansion | Inline (blocks writer)            | Background provider thread (non-blocking)  |

No global locks appear on the writer hot path. The only atomic operations are CAS on zone bitmaps (spread across cachelines by randomization) and the final root pointer swap that commits a transaction. Background threads handle all infrastructure work — segment provisioning, space reclamation, cache promotion — asynchronously.

This level of attention to allocator micro-architecture is rare even in production databases. Most systems bolt on jemalloc or tcmalloc and call it a day; PsiTrie treats allocation as a first-class performance and correctness feature — the allocator isn't a utility library, it's a core subsystem that shapes cache behavior, contention profiles, and crash recovery.

---

## 9. Scaling Limits

Four hard limits constrain maximum database scale:

1. **128 TB addressable storage** — the 41-bit location field addresses 64-byte cache lines (Section 2)
2. **~4 billion object IDs** — the 32-bit `ptr_address` space (Section 2)
3. **~550 billion key descendant counter** — the 39-bit `_descendents` field per inner node (Section 4)
4. **~2 million concurrent snapshots** — the 21-bit reference count per control block (Section 2)

Which limit you hit first depends entirely on your data profile:

| Avg Key | Avg Value | Inlined? | Limiting Factor  | Max Keys | Est. DB Size |
|--------:|----------:|----------|------------------|---------:|-------------:|
|     8 B |       8 B | Yes      | 4B address space |    ~230B |       ~4 TB  |
|    32 B |      32 B | Yes      | 4B address space |    ~230B |      ~15 TB  |
|    32 B |     256 B | No       | 4B address space |      ~4B |       ~1 TB  |
|    32 B |      4 KB | No       | 4B address space |      ~4B |      ~16 TB  |
|    32 B |     32 KB | No       | 128 TB storage   |      ~4B |     ~128 TB  |
|    32 B |      1 MB | No       | 128 TB storage   |    ~130M |     ~128 TB  |

Values ≤64 bytes inline directly in the leaf — no extra object allocation per key, so ~58 keys share one leaf address. This is why small-value workloads can reach hundreds of billions of keys before exhausting the 4B address space. Once values exceed 64 bytes, each key consumes its own value_node address, making the address space the binding constraint at ~4B keys. For large values (≥32 KB), the 128 TB storage limit becomes the bottleneck before the address space fills.

The 39-bit descendant counter (550B keys) is never the binding constraint — it exceeds what either the address space or storage can support in any realistic workload.

The 21-bit reference count (2^21 ≈ 2 million) limits concurrent snapshots. Each snapshot of a tree root increments the ref count on the root node, and COW sharing means interior nodes accumulate refs from every snapshot that shares them. In practice, 2 million concurrent snapshots far exceeds typical usage — but long-lived snapshot accumulation without cleanup will eventually saturate ref counts on widely-shared nodes.

Snapshots also have a subtler impact on fan-out. COW snapshot nodes compete with live nodes for slots within shared cache lines. An inner node can reference up to 16 cache lines × 16 slots = 256 branches, but only when siblings are well-packed. If snapshot-created copies occupy slots in those same cache lines, fewer slots remain for live siblings. In the worst case — every cache line shared with snapshot nodes — an inner node degrades to 16 branches (one per cache line, no sharing). The compactor can relocate object data to new physical locations, but `ptr_address` slots are permanent — it cannot reassign siblings to new cacheline positions. Fan-out recovers only when snapshots are released and their slots become available for reuse by new allocations.

However, reduced fan-out under snapshot pressure is less costly than it first appears. COW mutations must `retain_children()` on every node along the modified path — an atomic CAS per child. The total cost is children-per-node × depth:

| Scenario                    | Fan-out | Depth | Node size | Cachelines to reach a key | COW: CAS ops | COW: bytes copied |
|-----------------------------|--------:|------:|----------:|--------------------------:|-------------:|------------------:|
| Traditional (8B pointers)   |     256 |     3 |    ~2 KB  |          ~96 (32×3)       |          768 |             6,144 |
| PsiTrie (no pressure)       |     256 |     3 |   ~650 B  |          ~33 (11×3)       |          768 |             1,950 |
| PsiTrie (snapshot pressure) |      16 |     6 |    ~67 B  |          ~12 (2×6)        |           96 |               400 |

The true measure is total cachelines loaded to reach a key: node size × depth. A 256-way PsiTrie node is ~650 bytes (11 cachelines) thanks to 1-byte branch encoding — already 3× fewer cachelines per level than a traditional 2 KB node with 8-byte pointers. Under snapshot pressure, 16-way nodes at 67 bytes (2 cachelines each) require double the depth but load 8× fewer cachelines per level — for a net reduction of nearly 3× versus the normal PsiTrie case and 8× versus traditional designs. The same pattern holds for COW mutations: fewer children per node means fewer atomic CAS operations for `retain_children()`. The system naturally trades cheap depth for expensive breadth.

---

## Summary: A New Design Point

PsiTrie occupies a region of the design space that was previously empty:

| Capability                   | B-tree            | LSM-tree                | In-memory ART | PsiTrie                                  |
|------------------------------|-------------------|-------------------------|---------------|------------------------------------------|
| Persistent + crash-safe      | Yes               | Yes                     | No            | Yes                                      |
| COW granularity              | 4KB page          | N/A                     | N/A           | 67 bytes                                 |
| Online compaction            | No (offline copy) | Background (file-level) | N/A           | Background (object-level)                |
| Object relocation cost       | Cannot            | File rewrite            | N/A           | memcpy + 1 atomic CAS                    |
| Cache management             | OS or buffer pool | OS or buffer pool       | N/A           | Self-tuning MFU with physical relocation |
| Range count                  | O(k)              | O(k)                    | O(k)          | O(log n)                                 |
| Range delete                 | O(k)              | O(k) tombstones         | O(k)          | O(log n)                                 |
| Write amplification          | High (page-level) | High (compaction)       | N/A           | Minimal (node-level, tunable)            |
| Composable hierarchical data | Flattened + joins | Embedded blobs          | N/A           | Native subtrees with O(1) operations     |

The key innovations that make this possible:

1. **Engineered sibling co-location with cacheline-shared branch encoding** — the allocator deliberately places sibling control blocks in shared cache lines, reducing memory bandwidth by 4-16x per inner node traversal while compressing 8-byte pointers into 1-byte references
2. **Control block indirection** makes every object relocatable via a single atomic CAS, enabling continuous online compaction without pointer updates or GC pauses
3. **Bitcoin-inspired difficulty adjustment** self-tunes cache promotion rates to available RAM and workload, with 2-bit per-object tracking at zero space overhead
4. **Subtree descendant tracking** enables O(log n) range counting and O(log n) range deletion with O(1) deferred subtree release
5. **Subtrees as first-class values** enable composable hierarchical data with zero-cost ownership transfer and O(1) snapshot branching
