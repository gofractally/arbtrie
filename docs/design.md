# Arbtrie: A New Design Point in Database Engineering

Arbtrie is a persistent, transactional key-value store that combines ideas from radix tries, B-trees, garbage collectors, and proof-of-work mining into a system that is fundamentally different from existing databases. This document explains what makes it unique and why those differences matter.

## The Problem with Existing Approaches

Every persistent key-value store makes the same set of tradeoffs:

- **B-trees** (LMDB, BoltDB, SQLite, InnoDB) operate on fixed-size pages (4KB-16KB). Copy-on-write means copying an entire page to change a single byte. Write amplification is proportional to page size, not data size.
- **LSM trees** (RocksDB, LevelDB, Cassandra) batch writes for throughput but pay for it with read amplification, compaction stalls, and space amplification from tombstones.
- **Adaptive radix tries** (ART) achieve optimal depth and cache-line efficiency, but only in memory. No persistent ART implementation exists that matches B-tree durability.

Arbtrie eliminates these tradeoffs by combining three novel subsystems:

1. **A persistent radix trie with batched leaves and 67-byte copy-on-write**
2. **A relocatable object allocator with lock-free O(1) compaction moves**
3. **A self-tuning physical data layout that sorts objects by access frequency**

---

## 1. Node-Level Copy-on-Write: 60x Less Write Amplification

### The Insight

Copy-on-write (COW) is the foundation of MVCC, snapshot isolation, and crash safety. But in every existing COW database, the unit of copying is a **page** — 4KB to 16KB of data. When you change one key, you copy 4KB at every level of the tree. For a 4-level B-tree, that's 16KB of writes for a single byte change.

Arbtrie's inner nodes average **67 bytes**. Copy-on-write copies 67 bytes per level, not 4KB. For a 5-level trie, a single mutation writes ~335 bytes total — roughly **60x less** than a page-level COW B-tree.

This is made possible by a novel encoding:

### Cacheline-Shared Branch Encoding

In a traditional tree, each child pointer is an 8-byte address. An inner node with 16 children costs 128 bytes in pointers alone. Arbtrie observes that sibling nodes are typically allocated near each other — within the same 64-byte cacheline. So instead of storing 16 separate 8-byte addresses, it stores up to 16 **cacheline base addresses** (8 bytes each) and encodes each branch as a **1-byte index** (4 bits for which cacheline, 4 bits for which slot within that cacheline).

```
Traditional:  16 branches × 8 bytes = 128 bytes of pointers
Arbtrie:      4 cachelines × 8 bytes + 16 branches × 1 byte = 48 bytes
```

This encoding supports up to 256 branches (16 cachelines × 16 slots) in a node that fits in 1-2 cache lines. The result: inner nodes averaging 67 bytes with 8-9 branches each, achieving B-tree-class fan-out in trie-class space.

### Batched Leaves: Trie Routing, B-tree Density

Pure radix tries store one key per leaf. This creates millions of leaf nodes for millions of keys, each with per-node overhead. Arbtrie's leaf nodes pack up to **512 keys** using sorted binary search with hash-accelerated filtering. In practice, leaves hold ~58 keys each.

The trie provides O(key_length) routing to the correct leaf. The leaf provides B-tree-class data density. You get the depth advantages of a trie (5 levels for 30M keys) with the storage efficiency of a B-tree.

| Metric | B-tree (LMDB) | Pure ART | Arbtrie |
|--------|--------------|----------|---------|
| COW unit | 4KB page | N/A (in-memory) | 67 bytes |
| Leaf keys | 50-200 per page | 1 | ~58 per node |
| Depth (30M keys) | 3-4 | 5-8 | 5 |
| Write amplification | ~20KB/mutation | N/A | ~335B/mutation |
| Persistent | Yes | No | Yes |

---

## 2. The Segment Allocator: Relocatable Persistent Memory

### The Problem with Persistent Allocation

Persistent data structures face a fundamental tension: you need stable addresses for pointers, but you also need to compact and reorganize data as it fragments. Traditional approaches:

- **B-trees** avoid the problem by using fixed-size pages. Fragmentation accumulates within pages.
- **LSM trees** avoid it by making files immutable. Compaction rewrites entire files.
- **Garbage collectors** solve it by tracing all references and updating all pointers. This requires stop-the-world pauses or expensive read barriers.

SAL (Segment Allocator Library) solves this with a **single level of indirection** that makes any object relocatable in O(1) time, lock-free, with zero impact on concurrent readers and writers.

### Control Block Indirection

Every allocated object has a permanent 32-bit ID (`ptr_address`). This ID indexes into an array of atomic 64-bit **control blocks**:

```
ptr_address (32-bit permanent ID)
     │
     ▼ array lookup (O(1))
control_block (64-bit atomic)
  ┌─────────────────────────────────────────────────────┐
  │ ref_count (21 bits) │ location (41 bits) │ MFU (2 bits) │
  └─────────────────────────────────────────────────────┘
     │
     ▼ direct memory access
Object Data (at physical location in memory-mapped segment)
```

To **move an object**, the compactor:
1. Copies the object data to a new location
2. Atomically CAS-swaps the location in the control block

That's it. One atomic instruction. No pointer updates anywhere in the trie. No tracing. No stop-the-world pause. Concurrent readers that already resolved the old location continue reading valid data (the old copy persists until all read locks release). New readers pick up the new location.

| System | Object Relocation Cost | Concurrent? | Persistent? |
|--------|----------------------|-------------|-------------|
| Java ZGC | Read barrier on every pointer access | Yes | No |
| Go GC | Stop-the-world + pointer updates | Partially | No |
| LMDB | Cannot relocate (offline copy only) | N/A | Yes |
| PostgreSQL VACUUM | O(indexes) per row | Limited | Yes |
| RocksDB compaction | Rewrite entire SST file | Background | Yes |
| **SAL** | **1 atomic CAS** | **Fully** | **Yes** |

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

Arbtrie does something different: it **physically relocates individual objects** between RAM-guaranteed (mlocked) segments and pageable segments based on observed access frequency. Over time, the physical layout of the database file converges to match the actual workload.

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
- **Naturally random sampling**: Lock contention on the control block's activity bits provides free random sampling — no explicit random number generation needed for the access tracking itself

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

---

## 4. O(log n) Range Operations via Subtree Metadata

### Descendants Invariant

Every inner node in the trie maintains a `_descendents` field: the total number of keys in its entire subtree. This 39-bit counter is maintained during every insert, remove, and split operation.

This invariant enables two operations that are O(k) or impossible in other databases:

### Range Counting in O(log n)

`count_keys(lower, upper)` counts keys in a range without touching any leaves. The algorithm routes through the trie, summing `_descendents` for fully-contained subtrees (O(1) per subtree) and only recursing into the two boundary branches. Total work: O(log n) regardless of how many keys are in the range.

Most databases must scan every key in the range to count them. A `SELECT COUNT(*) WHERE key BETWEEN x AND y` in PostgreSQL or a range iterator in RocksDB is O(k) in the result size.

### Range Deletion in O(log n)

`remove_range(lower, upper)` deletes all keys in a range using the same routing logic as counting, but releasing fully-contained subtrees instead of counting them. Subtree release is O(1) — just decrement the root's reference count. The compactor handles cascading destruction asynchronously.

Deleting 900,000 out of 1,000,000 keys visits only O(log n) nodes along the two boundary paths. The interior of the range is never touched.

| Operation | B-tree | LSM-tree | Arbtrie |
|-----------|--------|----------|---------|
| Count keys in range | O(k) scan | O(k) scan + tombstones | **O(log n)** |
| Delete range | O(k) deletes | O(k) tombstones | **O(log n)** |
| Delete range space reclaim | Immediate but O(k) | Background compaction | **O(1) deferred release** |

---

## 5. Performance

### Single-Threaded Insert Throughput (30M Random Keys, Persistent)

| Keys Inserted | Inserts/sec | Tree Depth | Total Nodes |
|--------------:|------------:|-----------:|------------:|
| 100K | 3,731,343 | 3 | 35,902 |
| 1.1M | 1,231,527 | 5 | 1,789,712 |
| 3.0M | 1,129,943 | 5 | 4,323,554 |

Throughput degrades logarithmically — no cliff edges from compaction stalls or write amplification spikes.

### Tree Shape at 30M Keys

| Metric | Value |
|--------|-------|
| Max depth | 5 |
| Inner nodes | 67,075 (98% with prefix compression) |
| Leaf nodes | 514,746 (~58 keys each) |
| Avg inner node size | 67 bytes |
| Value nodes | 0 (all values inlined) |

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

Arbtrie allows any key's value to be **a pointer to another tree root**. A leaf node entry can hold either inline data (up to 64 bytes), a reference to a value_node (larger data), or a **subtree reference** — a pointer to an independent trie that is a fully functional tree in its own right.

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

| Capability | RDBMS | Document DB | KV Store | Arbtrie |
|-----------|-------|-------------|----------|---------|
| Hierarchical data | Flattened + joins | Embedded blobs | Key-prefix encoding | Native subtrees |
| Atomic subtree operations | Requires transactions | Replace entire document | Not possible | O(1) root pointer swap |
| Subtree snapshot | Full table copy | Full document copy | Not possible | O(1) ref count increment |
| Subtree deletion | O(n) row deletes | Replace document | O(n) key deletes | O(1) release + deferred cascade |
| Independent subtree indexing | Separate tables | Application-level | Application-level | Each subtree is a full trie |

Subtrees enable patterns that are impossible or prohibitively expensive in other systems:
- **Isolated namespaces** with independent key spaces sharing one database file
- **Snapshot branching** — fork a subtree for speculative writes, merge or discard in O(1)
- **Recursive data structures** — trees of trees with consistent ownership semantics at every level

---

## Summary: A New Design Point

Arbtrie occupies a region of the design space that was previously empty:

| Capability | B-tree | LSM-tree | In-memory ART | Arbtrie |
|-----------|--------|----------|---------------|---------|
| Persistent + crash-safe | Yes | Yes | No | Yes |
| COW granularity | 4KB page | N/A | N/A | 67 bytes |
| Online compaction | No (offline copy) | Background (file-level) | N/A | Background (object-level) |
| Object relocation cost | Cannot | File rewrite | N/A | 1 atomic CAS |
| Cache management | OS or buffer pool | OS or buffer pool | N/A | Self-tuning MFU with physical relocation |
| Range count | O(k) | O(k) | O(k) | O(log n) |
| Range delete | O(k) | O(k) tombstones | O(k) | O(log n) |
| Write amplification | High (page-level) | High (compaction) | N/A | Minimal (node-level) |
| Composable hierarchical data | Flattened + joins | Embedded blobs | N/A | Native subtrees with O(1) operations |

The key innovations that make this possible:

1. **Cacheline-shared branch encoding** compresses 8-byte pointers into 1-byte references, enabling 67-byte inner nodes with B-tree-class fan-out
2. **Control block indirection** makes every object relocatable via a single atomic CAS, enabling continuous online compaction without pointer updates or GC pauses
3. **Bitcoin-inspired difficulty adjustment** self-tunes cache promotion rates to available RAM and workload, with 2-bit per-object tracking at zero space overhead
4. **Subtree descendant tracking** enables O(log n) range counting and O(log n) range deletion with O(1) deferred subtree release
5. **Subtrees as first-class values** enable composable hierarchical data with zero-cost ownership transfer and O(1) snapshot branching
