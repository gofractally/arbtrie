# Design Philosophy

PsiTri is built on three novel subsystems that work together to deliver performance characteristics impossible with traditional database architectures.

## 1. Node-Level Copy-on-Write: Dramatically Less Write Amplification

### The Insight

**Copy-on-write (COW)** is a technique where shared data is never modified in place -- instead, a copy is made before any mutation, so the original remains intact for other readers and for crash recovery. This is the foundation of snapshot isolation and crash safety. But in every existing COW database, the unit of copying is a **page** -- 4KB to 16KB of data. When you change one key, you copy 4KB at every level of the tree. For a 4-level B-tree, that's 16KB of writes for a single byte change.

PsiTri's inner nodes are allocated in 64-byte multiples. Their size depends on the number of branches and how many distinct cachelines those branches span in the control block array -- when the allocator successfully co-locates siblings on shared cachelines, fewer cacheline base addresses are needed and the node stays smaller. In practice, most inner nodes fit in **1-2 cache lines** (64-128 bytes). For a 5-level tree, a typical mutation copies 4 inner nodes plus one leaf (up to 2 KB) -- **far less** than the **5 x 4 KB = 20 KB** a page-level COW B-tree would write.

### Cacheline-Shared Branch Encoding

Every persistent data structure that uses indirection pays for it in **cache line loads**. When a tree node has 64 children, dereferencing their control blocks touches 64 separate cache lines. Most systems store reference counts and metadata inline with each node -- so traversing children means loading each child's data page just to read its refcount.

PsiTri separates the **control blocks** (reference counts, segment pointers, metadata) from the **node data** and co-locates sibling control blocks within shared cache lines. When a node splits or children are allocated, the allocator receives **hints** (the parent's existing cacheline addresses) and preferentially places new control blocks adjacent to their siblings.

Instead of storing N separate 4-byte addresses, PsiTri stores up to 16 **cacheline base addresses** (4 bytes each) and encodes each branch as a **1-byte index** (4 bits selecting which cacheline, 4 bits selecting which slot):

```
Traditional:  16 branches x 4 bytes = 64 bytes of pointers, 16 cache line loads
PsiTri:       4 cachelines x 4 bytes + 16 branches x 1 byte = 32 bytes, 4 cache line loads
```

This gives PsiTri the **lowest per-child pointer overhead of any pointer-based tree structure**: 1.25-2.0 bytes per child, compared to 4-8 bytes in B-trees, ART, HOT, Masstree, and every other design in the literature. At 256 branches with 16 cacheline groups, each child costs just 1.25 bytes. See [Control Blocks](../architecture/control-blocks.md#per-child-pointer-overhead-how-psitri-compares) for the full analysis.

### Radix/B-tree Hybrid with SIMD Routing

PsiTri combines radix tree routing with B-tree-style sorted leaves. Each inner node stores a sorted array of **1-byte dividers** and an array of child branches -- structurally similar to a B-tree node, but dividers are restricted to a single byte. This enables **constant-time branch selection via SIMD**: a single instruction compares the search byte against all dividers simultaneously.

Prefix compression collapses shared key prefixes across levels:

```
inner_prefix_node("user/")
  +-- inner_node [dividers: 'a', 'd', 'z']   <-- SIMD compares search byte
        |-- branch 0: keys < 'a'
        |-- branch 1: keys in ['a', 'd')
        |-- branch 2: keys in ['d', 'z')
        +-- branch 3: keys >= 'z'
```

### Sorted Leaf Nodes

Leaf nodes store sorted keys with hash-accelerated lookup, holding ~58 keys each in up to 2 KB. This avoids the depth explosion of pure radix trees (which need one level per byte of key) while keeping the compact inner nodes that make node-level copy-on-write practical.

| Metric              | B-tree (LMDB)   | Pure ART        | PsiTri                  |
|---------------------|-----------------|-----------------|--------------------------|
| Copy-on-write unit  | 4KB page        | N/A (in-memory) | Per-node (64 B multiples)  |
| Leaf keys           | 50-200 per page | 1               | ~58 per node             |
| Depth (30M keys)    | 3-4             | 5-8             | 5                        |
| Write amplification | ~20KB/mutation  | N/A             | ~2.3 KB/mutation (typical) |
| Persistent          | Yes             | No              | Yes                      |

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
     |
     v  array lookup (O(1))
control_block (64-bit atomic)
  +------------------------------------------------------+
  | ref_count (21 bits) | location (41 bits) | flags (2b) |
  +------------------------------------------------------+
     |
     v  direct memory access
Object Data (at physical location in memory-mapped segment)
```

The location field is addressed in units of 64-byte cache lines. The 41-bit field can theoretically address 128 TB; the configured maximum is **32 TB**.

To **move an object**, the compactor:
1. Copies the object data to a new location
2. Atomically CAS-swaps the location in the control block

One atomic instruction. No pointer updates anywhere in the tree. No tracing. No stop-the-world pause.

| System              | Object Relocation Cost                | Concurrent? | Persistent? |
|---------------------|---------------------------------------|-------------|-------------|
| Java ZGC            | Read barrier on every pointer access  | Yes         | No          |
| Go GC               | Stop-the-world + pointer updates      | Partially   | No          |
| LMDB                | Cannot relocate (offline copy only)   | N/A         | Yes         |
| PostgreSQL VACUUM   | O(indexes) per row                    | Limited     | Yes         |
| RocksDB compaction  | Rewrite entire SST file               | Background  | Yes         |
| **SAL**             | **memcpy + 1 atomic CAS**            | **Fully**   | **Yes**     |

---

## 3. Self-Organizing Physical Layout: Bitcoin-Inspired Cache Management

### The Insight

Most databases delegate caching to the OS page cache or manage a fixed-size buffer pool. Both have fundamental limitations:

- **OS page cache**: Uses LRU (recency), not frequency. A full table scan evicts your entire hot working set.
- **Buffer pools**: Fixed size, page granularity, require manual tuning.

PsiTri **physically relocates individual objects** between RAM-guaranteed (mlocked) segments and pageable segments based on observed access frequency.

### Object-Granularity MFU Tracking

SAL tracks frequency with **2 bits per object**, embedded in the existing control block at zero additional space cost:

- **`active` bit**: Set when the object is read
- **`pending_cache` bit**: Set when the object is read while already active

A background thread clears these bits on a rolling window (default 5 hours, configurable via `read_cache_window_sec`). Objects that get re-marked between decay cycles are "hot."

### Bitcoin-Inspired Difficulty Adjustment

The cache promotion system uses the same approach as Bitcoin mining: **adaptive difficulty adjustment**. Every access to a hot object rolls a probabilistic check:

```
should_promote = random_value >= (cache_difficulty x object_size_in_cachelines)
```

- **Promoting too fast**: Increase difficulty (multiply gap by 7/8)
- **Promoting too slow**: Decrease difficulty (multiply gap by 9/8)

No configuration needed. The system self-tunes to available RAM and workload.

### Physical Data Tiering

```
+------------------------------------------+
|         Pinned Segments (mlock'd)        |
|   Hot objects -- guaranteed in RAM        |
|   Promoted by passing difficulty check    |
+------------------------------------------+
|        Unpinned Segments (pageable)       |
|   Warm/cold objects -- OS decides paging  |
|   Objects demoted here as they cool       |
+------------------------------------------+
|           Freed Segment Space             |
|   Reclaimed by background compaction      |
+------------------------------------------+
```

No other database physically relocates individual objects between RAM-guaranteed and pageable storage based on observed access frequency.

---

## 4. O(log n) Range Operations

Every inner node maintains a `_descendents` field: the total number of keys in its entire subtree (39-bit counter, up to ~550 billion keys).

### Range Counting in O(log n)

`count_keys(lower, upper)` counts keys in a range without touching any leaves. The algorithm sums `_descendents` for fully-contained subtrees (O(1) per subtree) and only recurses into boundary branches.

### Range Deletion in O(log n)

`remove_range(lower, upper)` releases fully-contained subtrees in O(1) -- just decrement the root's reference count. The compactor handles cascading destruction asynchronously.

| Operation                  | B-tree            | LSM-tree               | PsiTri                  |
|----------------------------|-------------------|------------------------|--------------------------|
| Count keys in range        | O(k) scan         | O(k) scan + tombstones | **O(log n)**             |
| Delete range               | O(k) deletes      | O(k) tombstones        | **O(log n)**             |
| Delete range space reclaim | Immediate but O(k) | Background compaction | **O(1) deferred release** |

---

## 5. Composable Trees: Subtrees as First-Class Values

PsiTri allows any key's value to be **a pointer to another tree root**. This creates composable, hierarchical data structures:

```
Root tree
|-- "users/alice" -> { inline data }
|-- "users/bob"   -> { inline data }
|-- "indexes"     -> [subtree] --> Index tree
|                                  |-- "age:25" -> ...
|                                  +-- "name:alice" -> ...
+-- "metadata"    -> [subtree] --> Metadata tree
                                   +-- "schema_version" -> "3"
```

| Capability                   | RDBMS                 | Document DB             | KV Store             | PsiTri                          |
|------------------------------|-----------------------|-------------------------|----------------------|----------------------------------|
| Hierarchical data            | Flattened + joins     | Embedded blobs          | Key-prefix encoding  | Native subtrees                  |
| Atomic subtree operations    | Requires transactions | Replace entire document | Not possible         | O(1) root pointer swap           |
| Subtree snapshot             | Full table copy       | Full document copy      | Not possible         | O(1) ref count increment         |
| Subtree deletion             | O(n) row deletes      | Replace document        | O(n) key deletes     | O(1) release + deferred cascade  |

---

## 6. Crash Recovery Without a Write-Ahead Log

PsiTri has no WAL. Recovery is built into the data layout:

- **Append-only writes** guarantee committed data is never overwritten
- **Hardware write protection** (when enabled): Committed data can be marked read-only via `mprotect(PROT_READ)` -- stray writes cause an immediate SIGSEGV, not silent corruption. This is configurable via sync mode; the default (`sync_type::none`) does not call mprotect.
- **Segment scanning** reconstructs control blocks from the append-only segment structure
- **Reachability walk** from root pointers rebuilds reference counts

The segments themselves serve as the recovery log. The tradeoff: recovery time is proportional to database size, not the amount of uncommitted work. For long-lived databases with infrequent crashes, this is favorable -- zero overhead during normal operation.

### Durability Hierarchy

| Level          | Mechanism                       | App crash | Power loss | Write latency |
|----------------|---------------------------------|-----------|------------|---------------|
| `none`         | OS writes when convenient       | No        | No         | Zero          |
| `mprotect`     | Hardware write-protect pages    | Yes       | No         | ~microseconds |
| `msync_async`  | Hint to OS: flush soon          | Yes       | Probably   | ~microseconds |
| `msync_sync`   | Block until OS buffers written  | Yes       | Mostly     | ~milliseconds |
| `fsync`        | Flush OS buffers to drive       | Yes       | Yes*       | ~milliseconds |
| `full`         | F_FULLFSYNC / flush drive cache | Yes       | Yes        | ~10s of ms    |
