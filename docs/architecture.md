# PsiTrie Architecture & Design

This document provides a comprehensive technical reference for the psitrie project: a persistent, transactional key-value store built on an adaptive radix trie. It is intended to bring developers (human or AI) up to speed quickly and guide future development.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Layer Architecture](#layer-architecture)
3. [SAL: Segment Allocator Library](#sal-segment-allocator-library)
4. [Psitri: Persistent Trie](#psitri-persistent-trie)
5. [Node Types](#node-types)
6. [Trie Operations](#trie-operations)
7. [Cursor & Iteration](#cursor--iteration)
8. [Transaction Model](#transaction-model)
9. [Performance Characteristics](#performance-characteristics)
10. [Current Status & Roadmap](#current-status--roadmap)

---

## Project Overview

PsiTrie is a persistent, ACID-compliant key-value store that uses an adaptive radix trie as its core data structure. It is designed for:

- **High write throughput**: 1-3.7M inserts/sec (persistent, single-threaded)
- **Low read latency**: O(key_length) lookups with max depth ~5 for 30M keys
- **Concurrent access**: Up to 64 threads with MVCC isolation
- **Crash safety**: Configurable sync modes from `none` to `F_FULLFSYNC`
- **Space efficiency**: 67-byte average inner nodes, ~58 keys per leaf

### What Makes It Unique

1. **Batched leaves**: Unlike ART (1 key per leaf), psitri packs ~58 keys per leaf node, dramatically reducing per-key overhead and total node count
2. **Node-level COW**: Copy-on-write operates on compact 67-byte inner nodes rather than 4KB+ B-tree pages, yielding ~60x less write amplification per mutation
3. **Prefix compression at every level**: Inner prefix nodes collapse shared key prefixes, keeping tree depth to ~5 for 30M random keys
4. **Cacheline-aware addressing**: Branch pointers share cacheline bases (8-byte aligned), allowing 16 branches to share a single 8-byte address via 4-bit indexing
5. **Persistent + checksummed**: Full durability with per-node checksums, yet matching in-memory-only ART performance

### Comparison with Alternatives

| System          | Insert/sec (persistent) | Depth (30M keys) | Persistent | COW | Node Size |
|-----------------|-------------------------|-------------------|------------|-----|-----------|
| **psitri**      | **1.1-3.7M**            | **5**             | Yes        | Yes | 67B avg   |
| LMDB            | 0.3-0.8M                | 3-4               | Yes        | Yes | 4KB pages |
| RocksDB         | 0.5-1.5M                | N/A (LSM)         | Yes        | No  | variable  |
| ART (in-memory) | 5-10M                   | 5-8               | No         | No  | 52-2048B  |
| SQLite          | 0.1-0.5M                | 3-4               | Yes        | No  | 4KB pages |

---

## Layer Architecture

The codebase is organized into four clean layers with no circular dependencies:

```
┌────────────────────────────────────────────────────┐
│                    Applications                    │
│          (arbtrie_sql, benchmarks, tests)          │
└────────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────┐
│              PSITRI (Persistent Trie)              │
│     Trie logic: nodes, insert, remove, cursor      │
│                 libraries/psitri/                  │
└────────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────┐
│          SAL (Segment Allocator Library)           │
│      Memory-mapped allocation, ref counting,       │
│          COW, compaction, MVCC read locks          │
│                   libraries/sal/                   │
└────────────────────────────────────────────────────┘
                  │                │
                  ▼                ▼
┌────────────────────────┐  ┌────────────────────────┐
│ HASH (header-only)     │  │ UCC (header-only)      │
│ xxhash, lehmer64, xxh32│  │ SIMD lower_bound,      │
│ libraries/hash/        │  │ hierarchical bitmap,   │
│                        │  │ fast_memcpy, typed_int │
│                        │  │ libraries/ucc/         │
└────────────────────────┘  └────────────────────────┘
```

### Build Targets

| Target         | Type                    | Dependencies  | Status                    |
|----------------|-------------------------|---------------|---------------------------|
| `hash`         | INTERFACE (header-only) | none          | Working                   |
| `ucc`          | INTERFACE (header-only) | none          | Working                   |
| `sal`          | STATIC library          | hash, ucc     | Working                   |
| `psitri`       | STATIC library          | sal           | Working                   |
| `arbtrie`      | STATIC library          | sal           | **Commented out** (legacy) |
| `psitri-tests` | Executable              | psitri, Catch2 | Working                   |

### Key Files

```
libraries/
  hash/include/hash/          # xxhash.h, lehmer64.h, xxh32.hpp
  ucc/include/ucc/            # lower_bound.hpp, hierarchical_bitmap.hpp, ...
  sal/
    include/sal/              # allocator.hpp, smart_ptr.hpp, control_block.hpp, ...
    src/                      # allocator.cpp, block_allocator.cpp, ...
  psitri/
    include/psitri/           # database.hpp, tree_ops.hpp, cursor.hpp,
                              # count_keys.hpp, range_remove.hpp,
                              # write_cursor.hpp, transaction.hpp, ...
    include/psitri/node/      # leaf.hpp, inner.hpp, value_node.hpp, node.hpp
    src/                      # database.cpp, node/leaf.cpp
    tests/                    # tree_context_tests.cpp, range_remove_tests.cpp,
                              # public_api_tests.cpp, count_keys_tests.cpp, ...
```

---

## SAL: Segment Allocator Library

SAL provides persistent, memory-mapped allocation with reference counting, copy-on-write, and background compaction. It is the foundation that psitri builds on.

### Addressing Model

```
ptr_address (32-bit)              Logical object ID
     │
     ▼
control_block (8 bytes, atomic)   ID → location + ref count
     │
     ▼
location (41-bit cacheline)       Physical offset in segments
     │
     ▼
segment (32 MB)                   Contiguous mmap region
     │
     ▼
alloc_header (12 bytes)           checksum, size, type
     │
     ▼
Object Data                       User-defined node content
```

**Key constants:**

| Constant        | Value     | Notes                  |
|-----------------|-----------|------------------------|
| Segment size    | 32 MB     | Contiguous mmap region |
| Max segments    | 4,096     | 128 TB max database    |
| Max threads     | 64        | Concurrent sessions    |
| Max object size | 16 MB     | Half a segment         |
| Cacheline       | 64 bytes  | Allocation alignment   |
| Max ref count   | 2,097,088 | 21-bit field minus 64  |

### Control Block (8 bytes, atomic)

Every allocated object has a `control_block` indexed by its `ptr_address`:

```
┌────────────────────────────────────────────────────────────────────┐
│ bit 63          bit 62    bits 61..21               bits 20..0     │
│ pending_cache   active    cacheline_offset (41b)    ref_count (21b)│
└────────────────────────────────────────────────────────────────────┘
```

- **ref_count**: Atomic increment/decrement. `ref == 1` means unique ownership (in-place modification safe). `ref > 1` triggers copy-on-write.
- **cacheline_offset**: Physical location as 64-byte cacheline index. Supports up to 128 TB addressable space.
- **active/pending_cache**: Flags for LRU cache promotion by background threads.

### Object Header (alloc_header, 12 bytes)

```
┌──────────────────────────────────────────────────────────┐
│ checksum (2B)   ptr_addr_seq (4B)   size:25 | type:7 (4B)│
└──────────────────────────────────────────────────────────┘
```

- **checksum**: Optional XXH3 hash for corruption detection
- **ptr_addr_seq**: Address + sequence number for crash recovery ordering
- **size**: Object size in bytes (64-byte aligned), max 32 MB
- **type**: User-defined type ID (7 bits, 127 types)

### Copy-on-Write (COW) Decision

```
write to object
     │
     ▼
ref_count == 1? ───yes───> modify in-place (unique path)
     │
     no
     │
     ▼
allocate new object
copy data + apply modification
update control_block location
release old reference
```

The unique vs shared decision propagates through the tree: if a parent node has `ref == 1`, all modifications down to the leaf can potentially be in-place. If any ancestor is shared (`ref > 1`), the entire path must be copied.

### Background Threads

SAL runs three background threads:

1. **Segment Provider**: Pre-allocates segments, manages mlock'd (pinned) vs unpinned pools
2. **Compactor**: Defragments segments by moving live objects, promotes hot data to pinned segments
3. **Read Bit Decay**: Decays access bits over time for LRU-like cache eviction

### MVCC Read Isolation

```
Thread A (writer)            Thread B (reader)
     │                            │
     │                       read_lock rl = session.lock()
     │                            │
  modify node ──COW──>       sees old version via
  (new location)              original location
     │                            │
     │                       ~read_lock()
     │                            │
     │                       next lock sees new version
```

- Read locks are **wait-free** (atomic counter only, no mutex)
- Up to 64 concurrent readers
- Compactor respects active read locks before relocating objects

### Sync Modes

| Mode          | Durability                            | Performance     |
|---------------|---------------------------------------|-----------------|
| `none`        | Process crash safe only               | Fastest         |
| `mprotect`    | + Write protection on committed data  | Fast            |
| `msync_async` | + OS flush initiated                  | Moderate        |
| `msync_sync`  | + Block until OS writes               | Slower          |
| `fsync`       | + Block until drive acknowledges      | Slow            |
| `full`        | + F_FULLFSYNC (flush drive cache)     | Slowest, safest |

---

## Psitri: Persistent Trie

Psitri is the active trie implementation. It builds on SAL to provide a persistent, ordered key-value store with prefix compression.

### Database & Session Model

```
database
  ├── 512 independent top-level roots (atomic, mutex-protected)
  ├── start_read_session()  ──> read_session  (shared_ptr)
  └── start_write_session() ──> write_session (shared_ptr)
                                     │
                                     ├── start_transaction(root_index)
                                     │        ├── insert(key, value)
                                     │        ├── upsert(key, value)
                                     │        ├── remove(key)
                                     │        ├── remove_range(lower, upper)
                                     │        ├── count_keys(lower, upper)
                                     │        ├── commit()
                                     │        └── ~transaction() ──> auto-abort
                                     │
                                     └── cursor(root)
                                              ├── seek_begin() / seek_end()
                                              ├── lower_bound(key)
                                              ├── next() / prev()
                                              └── key() / value()
```

### Trie Structure

The trie is a byte-indexed radix tree. Each level consumes one byte of the key, with prefix compression collapsing shared prefixes:

```
                ┌────────────────────────────────────┐
                │ inner_prefix_node                  │
                │ prefix: "applicati"                │
                │ branches: [o]                      │
                └────────────────────────────────────┘
                                   │ 'o'
                ┌────────────────────────────────────┐
                │ leaf_node                          │
                │ keys: ["n"]                        │
                │ values: [...]                      │
                └────────────────────────────────────┘
```

For multiple keys sharing a prefix:

```
                ┌────────────────────────────────────┐
                │ inner_prefix_node                  │
                │ prefix: "appl"                     │
                │ branches: [e, i, y]                │
                └────────────────────────────────────┘
                   'e'│    'i'│    'y'│
                      ▼       ▼       ▼
                    leaf    leaf     leaf
                   (empty) ("cation") (empty)
```

---

## Node Types

### Leaf Node (Binary Node)

Leaf nodes store sorted key-value pairs using binary search. They are the workhorses of the trie, packing many keys into a single node.

**Memory layout (max 4096 bytes):**

```
┌────────────────────────────────────────────────────┐
│ Header (20 bytes)                                  │
│  alloc_pos(2) dead_space(2) cline_cap:9            │
│  optimal_layout:1 num_branches:9                   │
├────────────────────────────────────────────────────┤
│ key_hashes[num_branches]     (1 byte each, XXH3)   │
│ key_offsets[num_branches]    (2 bytes each)        │
│ value_branches[num_branches] (2 bytes each)        │
│ clines[cline_cap]            (8 bytes each, max 16)│
├────────────────────────────────────────────────────┤
│ ... free space ...                                 │
├────────────────────────────────────────────────────┤
│ <── allocated data (keys + inline values) grows <──│
│     from tail toward header                        │
└────────────────────────────────────────────────────┘
```

**Key lookup**: Hash-accelerated binary search. Each key has a 1-byte XXH3 hash for fast filtering, then full key comparison on match.

**Value storage**: Two modes based on size:
- **Inline** (<=64 bytes): Stored directly in the leaf's allocation area. `value_branch` points to offset.
- **External** (>64 bytes): Stored as a separate `value_node`. `value_branch` encodes a cacheline index (4-bit line + 4-bit index within line).

**Cacheline sharing**: Up to 16 cacheline base addresses (`clines[]`) can be shared by multiple branches. Each `value_branch` references a cline by 4-bit index, and encodes its offset within that cline's 16-slot range. This compresses 8-byte addresses into 1-byte references.

| Limit                   | Value                          |
|-------------------------|--------------------------------|
| Max leaf size           | 4,096 bytes                    |
| Max branches per leaf   | 512                            |
| Max cachelines per leaf | 16                             |
| Max inline value size   | 64 bytes                       |
| Key hash                | XXH3_64bits truncated to 1 byte |

### Inner Node

Inner nodes store byte-indexed branches that route traversal deeper into the trie. Two variants exist:

**inner_node** (no prefix):
```
┌───────────────────────────────────────────┐
│ Header                                    │
│  descendants:39 num_branches:9 num_cline:5│
├───────────────────────────────────────────┤
│ divisions[num_branches - 1]  (1 byte each)│
│ branches[num_branches]       (1 byte each)│
│ ... padding ...                           │
│ clines[num_cline]  (8 bytes each, at tail)│
└───────────────────────────────────────────┘
```

**inner_prefix_node** (with prefix):
```
┌───────────────────────────────────────────┐
│ Header                                    │
│  descendants:39 num_branches:9 num_cline:5│
│  prefix_len:11 prefix_cap:11              │
├───────────────────────────────────────────┤
│ prefix[prefix_cap]           (variable)   │
│ divisions[num_branches - 1]  (1 byte each)│
│ branches[num_branches]       (1 byte each)│
│ ... padding ...                           │
│ clines[num_cline]  (8 bytes each, at tail)│
└───────────────────────────────────────────┘
```

**Branch encoding** (1 byte):
```
┌──────────────┬──────────────┐
│ line (4 bit) │ index (4 bit)│
├──────────────┼──────────────┤
│  cline 0-15  │  slot 0-15   │
└──────────────┴──────────────┘
```

Each branch byte encodes which of the 16 cachelines to use (4 bits) and which of 16 slots within that cacheline (4 bits). This allows up to 256 branches (16 x 16) while keeping each branch reference to just 1 byte.

**Division bytes**: The sorted array of byte values that partition the branch space. Branch `i` handles keys whose next byte is `>= divisions[i-1]` and `< divisions[i]`.

**EOF value**: Inner nodes can optionally store a value for the empty-suffix key (the key that ends exactly at this node's prefix).

| Limit                       | Value                       |
|-----------------------------|-----------------------------|
| Max branches per inner node | 256 (16 clines x 16 slots) |
| Max cachelines per inner    | 16                          |
| Max prefix length           | 2,048 bytes                 |
| Max descendants tracked     | ~500 billion (39 bits)      |

### Value Node

Stores values too large for leaf inline storage (>64 bytes).

```
┌───────────────────────────┐
│ data_size:31  is_subtree:1│
│ data[data_size]           │
└───────────────────────────┘
```

- Allocated in 64-byte increments
- Can store arbitrary binary data up to 2 GB
- `is_subtree` flag indicates the data is a `ptr_address` pointing to a nested tree root

---

## Trie Operations

### Insert / Upsert Algorithm

The core algorithm is `tree_context::upsert<Mode>(parent_hint, node_ref, key)`, which recursively descends the trie:

```
upsert(key, value)
  │
  ▼
root is null? ───yes───> create leaf with single key
  │
  no
  │
  ▼
cast_and_call(root):
  │
  ├── leaf_node:
  │     lower_bound(key)
  │     key exists?
  │       yes: update value (if mode allows)
  │       no:  can_apply(insert)?
  │              modify:  insert in-place
  │              defrag:  realloc + insert
  │              none:    split leaf ──> 2 halves
  │                       wrap in inner_prefix if common prefix
  │                       recursively insert into correct half
  │
  ├── inner_node / inner_prefix_node:
  │     match prefix (inner_prefix only):
  │       no match:  split prefix, create new branching
  │       partial:   new inner_prefix with shorter prefix
  │       full:      consume prefix, continue
  │     find branch via lower_bound on divisions
  │     recursively upsert into child
  │     merge result:
  │       1 branch returned:  update child pointer
  │       N branches returned: need more clines?
  │         yes + available: insert new branches
  │         yes + full:      split inner node
  │
  └── value_node:
        create leaf with old value + new key
```

### Unique vs Shared Paths

```
              ┌──────────────────┐
              │ check ref count  │
              │ of current node  │
              └────────┬─────────┘
                       │
            ref==1     │     ref>1
           ┌───────────┴───────────┐
           ▼                       ▼
  ┌──────────────────┐   ┌──────────────────┐
  │ UNIQUE           │   │ SHARED (COW)     │
  │ modify in-place  │   │ retain children  │
  │ realloc if needed│   │ alloc new node   │
  │                  │   │ copy + modify    │
  │                  │   │ release old      │
  └──────────────────┘   └──────────────────┘
```

In unique mode, the entire path from root to leaf can be modified in-place if all ancestors have `ref == 1`. This is the fast path for single-writer workloads.

### Leaf Split Algorithm

When a leaf exceeds capacity:

```
1. Find split position:
   - Compute common prefix of all keys
   - Find divider byte that splits evenly

2. If common prefix exists:
   ┌──────────────────────────────────────────┐
   │ inner_prefix_node                        │
   │ prefix = common_prefix                   │
   │ branch[left_div]  ───> leaf (keys < div) │
   │ branch[right_div] ───> leaf (keys >= div)│
   └──────────────────────────────────────────┘
   Keys in child leaves have common prefix stripped.
   Recursively insert into the correct child.

3. If no common prefix:
   Split into left/right leaves.
   Return 2 branches to parent for merging.
```

### Cacheline Assignment (find_clines)

When inserting new branches into an inner node, the algorithm must assign cacheline slots:

```
for each new branch address:
  base = address & ~0x0F   (cacheline base)
  │
  ├── found in existing clines[] ──> reuse slot, increment ref
  │
  └── not found ──> find empty slot
        │
        ├── empty slot available ──> assign
        │
        └── all 16 clines full ──> SPLIT INNER NODE
```

### Remove Algorithm

Single-key removal follows the same recursive descent as upsert, using `upsert<mode::unique_remove>`:

```
remove(key)
  │
  ▼
descend to leaf via inner/inner_prefix routing
  │
  ▼
leaf_node:
  lower_bound + exact match
  unique: release value (if value_node), compact metadata arrays
  shared: clone leaf excluding removed key
  │
  ▼
return empty branch_set if leaf now empty
  │
  ▼
inner_node (post-recursion):
  child empty? → remove_branch(), update _descendents
  child survived? → update _descendents
  inner has 1 branch left? → collapse:
    - Leaf child: realloc inner → leaf (prepend prefix if inner_prefix)
    - Inner child: realloc inner → copy child (merge prefixes if inner_prefix)
    - Value child: return single branch to parent
```

### Range Remove Algorithm

`remove_range(lower, upper)` efficiently removes all keys in `[lower, upper)` in O(log n) time by leveraging the `_descendents` invariant and deferred cascading release. The algorithm mirrors the `count_keys` routing logic but mutates the tree. Implementation is in `range_remove.hpp`, included at the bottom of `tree_ops.hpp`.

```
remove_range(lower, upper)
  │
  ▼
prefix narrowing (inner_prefix_node):
  same logic as count_keys — strip prefix from bounds
  fully contained? → return {} (remove entire subtree, O(1))
  fully disjoint?  → return node (nothing to remove)
  │
  ▼
branch routing (inner_node):
  start    = lower_bound(lower)
  boundary = lower_bound(upper)
  middle   = branches in (start, boundary)  ← fully contained
  │
  ├── middle branches: release directly (O(1) each via deferred cascade)
  ├── start branch:    recurse with range (partial removal)
  └── boundary branch: recurse with range (partial removal)
  │
  ▼
rebuild node from survivors, update _descendents
```

**Key constants:**
- `max_key`: 256-byte all-`0xFF` sentinel, compares greater than any real key. `remove_range("", max_key)` removes everything.
- Bounds: `[lower, upper)` exclusive upper bound, consistent with `count_keys`

### Count Keys Algorithm

`count_keys(lower, upper)` returns the number of keys in `[lower, upper)` in O(log n) time by leveraging the `_descendents` field on inner nodes. Fully-contained subtrees return their descendents count directly without traversal. Implementation is in `count_keys.hpp`.

```
count_keys(lower, upper)
  │
  ▼
prefix narrowing → same as range_remove
  │
  ▼
branch routing:
  middle branches: sum _descendents (O(1) per branch)
  start branch:    recurse, subtract keys below lower
  boundary branch: recurse, subtract keys above upper
```

---

## Cursor & Iteration

The cursor provides ordered traversal over the trie:

```
┌───────────────────────────────────────┐
│ cursor                                │
│                                       │
│  path_stack[128]:                     │
│    [0] root       branch=2            │
│    [1] inner_pre  branch=1 prefix="ap"│
│    [2] leaf       branch=5            │
│                                       │
│  key_buf[1024]: "apple"               │
│  key_len: 5                           │
└───────────────────────────────────────┘
```

### Navigation

| Operation          | Behavior                                    |
|--------------------|---------------------------------------------|
| `seek_begin()`     | Position at first key                       |
| `seek_end()`       | Position past last key                      |
| `lower_bound(key)` | First key >= target                         |
| `upper_bound(key)` | First key > target                          |
| `next()`           | Advance to next key in sorted order         |
| `prev()`           | Move to previous key in sorted order        |
| `key()`            | Return current key (built from path + leaf) |
| `value(read_lock)` | Return current value view                   |

### Key Reconstruction

Keys are reconstructed during traversal by accumulating prefix bytes from each inner_prefix_node and the division byte at each inner node:

```
Path: root(div='a') ──> inner_prefix(prefix="ppl", div='e') ──> leaf(key="")
Reconstructed key: "a" + "ppl" + "e" + "" = "apple"
```

---

## Transaction Model

```cpp
auto session = db.start_write_session();
auto tx = session->start_transaction(root_index);

tx.insert("key1", "value1");
tx.upsert("key2", "value2");
tx.remove("key3");
tx.remove_range("key_a", "key_z");  // remove all keys in [key_a, key_z)

tx.commit();   // atomically update root
// or: tx is destroyed -> auto-abort
```

- **Write isolation**: COW ensures readers see a consistent snapshot
- **Atomicity**: Commit atomically swaps the root pointer
- **Durability**: Configurable via sync_mode (none → F_FULLFSYNC)
- **512 independent roots**: Concurrent writes to different roots don't conflict
- **Nested transactions**: Via `sub_transaction()` with parent as commit target

---

## Performance Characteristics

### Insert Throughput (30M random keys, single-threaded, persistent)

| Batch | Keys | Inserts/sec | Nodes Visited | Total Allocated |
|------:|-----:|------------:|--------------:|----------------:|
|     0 | 100K |   3,731,343 |        17,286 |          35,902 |
|     1 | 200K |   2,252,252 |        36,027 |         133,806 |
|     2 | 300K |   1,801,801 |        59,276 |         324,338 |
|     5 | 600K |   1,331,557 |       167,341 |       1,450,107 |
|    10 | 1.1M |   1,231,527 |       196,374 |       1,789,712 |
|    29 | 3.0M |   1,129,943 |       581,821 |       4,323,554 |

Throughput degrades logarithmically as tree depth increases -- no cliff edges from compaction stalls or write amplification spikes.

### Tree Shape at 30M Keys

| Metric                  | Value             |
|-------------------------|-------------------|
| Inner nodes (no prefix) | 1,283             |
| Inner prefix nodes      | 65,792            |
| Leaf nodes              | 514,746           |
| Value nodes             | 0 (all inlined)   |
| Total branches          | 581,820           |
| Total cachelines        | 301,716           |
| Max depth               | 5                 |
| Avg branches/inner      | 8.67              |
| Avg clines/inner        | 4.50              |
| Keys per leaf           | ~58               |

### Why It's Fast

1. **Depth 5** for 30M keys: only 5 pointer dereferences per lookup
2. **Batched leaves** (~58 keys): amortizes node overhead, excellent cache locality
3. **67-byte inner nodes**: fit in 1-2 cache lines, vs 4KB+ for B-tree pages
4. **Node-level COW**: copies 67 bytes per mutation, not 4KB pages
5. **Inline values**: 0 value nodes at 30M keys means no extra indirection
6. **Prefix compression**: 98% of inner nodes are prefix nodes, collapsing shared bytes
7. **Cacheline sharing**: 16 branches share 1 address via 4-bit indexing
8. **Lock-free reads**: MVCC via atomic ref counts, no reader-writer contention

---

## Current Status & Roadmap

### Branch: `redress`

**Completed:**
- Full SAL allocator library with COW, compaction, MVCC
- Psitri insert/upsert (unique and shared modes)
- Cursor with lower_bound, next/prev, seek
- Full remove (unique and shared modes, leaf through inner, with node collapse)
- O(log n) `count_keys(lower, upper)` leveraging `_descendents` invariant
- O(log n) `remove_range(lower, upper)` with deferred cascading release
- Prefix compression and node splitting
- 30M key insert test passing
- Leak/orphan detection tests for insert, remove, and range_remove

**In Progress:**
- Multi-writer concurrency (concurrent insert/remove across threads)

**Future:**
- Currency simulator (item #3 in TODO)
- Move cache processing to dedicated thread
- Grow binary nodes on COW, shrink on compact
- Recovery from inconsistent state

### Legacy Code

The original `arbtrie` implementation in `include/arbtrie/` and `src/` is preserved as reference but its CMake target is commented out. All active development is in `psitri`. The old code may be useful for guiding the inner-node remove implementation.
