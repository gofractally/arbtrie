# Architecture Overview

PsiTri is a persistent, ACID-compliant key-value store that combines ideas from adaptive radix trees and B-trees into a hybrid data structure.

## Layer Architecture

The codebase is organized into four clean layers with no circular dependencies:

```
+----------------------------------------------------+
|                    Applications                    |
|          (arbtrie_sql, benchmarks, tests)          |
+----------------------------------------------------+
                           |
                           v
+----------------------------------------------------+
|                    PSITRI                          |
|    Tree logic: nodes, insert, remove, cursor       |
|                 libraries/psitri/                  |
+----------------------------------------------------+
                           |
                           v
+----------------------------------------------------+
|          SAL (Segment Allocator Library)           |
|      Memory-mapped allocation, ref counting,       |
|          COW, compaction, MVCC read locks          |
|                   libraries/sal/                   |
+----------------------------------------------------+
                  |                |
                  v                v
+------------------------+  +------------------------+
| HASH (header-only)     |  | UCC (header-only)      |
| xxhash, lehmer64, xxh32|  | SIMD lower_bound,      |
| libraries/hash/        |  | hierarchical bitmap,   |
|                        |  | fast_memcpy, typed_int |
|                        |  | libraries/ucc/         |
+------------------------+  +------------------------+
```

### Build Targets

| Target | Type | Dependencies | Status |
|--------|------|-------------|--------|
| `hash` | INTERFACE (header-only) | none | Working |
| `ucc` | INTERFACE (header-only) | none | Working |
| `sal` | STATIC library | hash, ucc | Working |
| `psitri` | STATIC library | sal | Working |
| `psitri-tests` | Executable | psitri, Catch2 | Working |

---

## Database & Session Model

```
database
  |-- 512 independent top-level roots (atomic, mutex-protected)
  |-- start_read_session()  --> read_session  (shared_ptr)
  +-- start_write_session() --> write_session (shared_ptr)
                                     |
                                     |-- start_transaction(root_index)
                                     |        |-- insert(key, value)
                                     |        |-- upsert(key, value)
                                     |        |-- remove(key)
                                     |        |-- remove_range(lower, upper)
                                     |        |-- count_keys(lower, upper)
                                     |        |-- commit()
                                     |        +-- ~transaction() --> auto-abort
                                     |
                                     +-- cursor(root)
                                              |-- seek_begin() / seek_end()
                                              |-- lower_bound(key)
                                              |-- next() / prev()
                                              +-- key() / value()
```

---

## Addressing Model

Every allocated object is addressed through a control block indirection layer:

```
ptr_address (32-bit)              Logical object ID
     |
     v
control_block (8 bytes, atomic)   ID -> location + ref count
     |
     v
location (41-bit cacheline)       Physical offset in segments
     |
     v
segment (32 MB)                   Contiguous mmap region
     |
     v
alloc_header (12 bytes)           checksum, size, type
     |
     v
Object Data                       User-defined node content
```

**Key constants:**

| Constant | Value | Notes |
|----------|-------|-------|
| Segment size | 32 MB | Contiguous mmap region |
| Max segments | ~1 million | 32 TB max database |
| Max threads | 64 | Concurrent sessions |
| Max object size | 16 MB | Half a segment |
| Cacheline | 64 bytes | Allocation alignment |
| Max ref count | 2,097,088 | 21-bit field minus 64 |

### Control Block (8 bytes, atomic)

```
+--------------------------------------------------------------------+
| bit 63          bit 62    bits 61..21               bits 20..0      |
| pending_cache   active    cacheline_offset (41b)    ref_count (21b) |
+--------------------------------------------------------------------+
```

- **ref_count**: Atomic increment/decrement. `ref == 1` means unique ownership (in-place modification safe). `ref > 1` triggers copy-on-write.
- **cacheline_offset**: Physical location as 64-byte cacheline index. The 41-bit field can address up to 128 TB; configured limit is 32 TB.
- **active/pending_cache**: Flags for MFU cache promotion by background threads.

---

## Tree Structure

PsiTri is a hybrid of a radix tree and a B-tree. Inner nodes route on **single-byte dividers** with prefix compression collapsing shared key prefixes. Leaf nodes store **sorted key suffixes** with hash-accelerated binary search (like B-tree pages).

```
                +------------------------------------+
                | inner_prefix_node                  |
                | prefix: "appl"                     |
                | dividers: [e, i, y]                |
                +------------------------------------+
                   'e'|    'i'|    'y'|
                      v       v       v
                    leaf    leaf     leaf
              (keys: "")  ("ation") (keys: "")
```

In this example, the prefix `"appl"` is stored once in the inner node and stripped from the key before routing. The leaf under branch `'i'` stores the suffix `"ation"` -- not the full key `"application"`.

This hybrid gets the best of both designs:

- **From radix trees**: prefix compression, up to 256-way fan-out per level, compact inner nodes (avg 67 bytes)
- **From B-trees**: sorted keys in leaves (~58 keys per node), efficient range scans, hash-accelerated point lookups

## Node Types

### Leaf Node

Leaf nodes store sorted key suffixes (the portion remaining after prefix bytes are stripped by ancestor inner_prefix_nodes) with hash-accelerated binary search. Each key has a 1-byte XXH3 hash for fast filtering before full key comparison. Leaves pack ~58 keys per node in up to 2KB.

Values up to 64 bytes are stored **inline** in the leaf. Larger values are stored as separate `value_node` objects. Subtree root pointers can also be stored as values.

### Inner Node

Inner nodes store a sorted array of **1-byte dividers** that partition the key space by the first byte. A SIMD instruction can compare the search byte against all dividers simultaneously, selecting the correct branch in constant time.

Each child is referenced via a **1-byte branch encoding** into shared cacheline base addresses (4 bits for which of 16 cachelines, 4 bits for which slot). This compresses what would normally be 8-byte pointers into 1-byte references, allowing up to 256 branches per node.

There are two inner node variants:

- **inner_node**: Routes on the first byte of the key. Used when there is no common prefix among branches (rare -- only ~2% of inner nodes).
- **inner_prefix_node**: Stores a multi-byte prefix common to all keys in its subtree. This prefix is stripped from the key before routing to children. This is the primary depth-reduction mechanism -- 98% of inner nodes are prefix nodes.

### Value Node

Stores values too large for leaf inline storage (>64 bytes). Can also store a subtree root pointer.

---

## Copy-on-Write Decision

**Copy-on-write (COW)** means that shared data is never modified in place. Instead, a copy is made before any mutation, preserving the original for concurrent readers and crash recovery. PsiTri decides whether to copy based on the reference count:

```
write to object
     |
     v
ref_count == 1? ---yes---> modify in-place (unique path, no copy needed)
     |
     no (data is shared)
     |
     v
allocate new object
copy data + apply modification
update control_block location
release old reference
```

In unique mode (ref == 1, no snapshots sharing this data), the entire path from root to leaf can be modified in-place. In shared mode (ref > 1), the entire path must be copied. This decision propagates through the tree.

---

## Background Threads

SAL runs three background threads:

1. **Segment Provider**: Pre-allocates segments, manages mlock'd (pinned) vs unpinned pools
2. **Compactor**: Defragments segments by moving live objects, promotes hot data to pinned segments, validates checksums
3. **Read Bit Decay**: Decays access bits over time for MFU cache eviction

---

## MVCC Read Isolation

```
Thread A (writer)            Thread B (reader)
     |                            |
     |                       read_lock rl = session.lock()
     |                            |
  modify node (COW) -->  sees old version via
  (new location)              original location
     |                            |
     |                       ~read_lock()
     |                            |
     |                       next lock sees new version
```

- Read locks are **wait-free** (atomic counter only, no mutex)
- Up to 64 concurrent readers
- Compactor respects active read locks before relocating objects

---

## Performance Characteristics

### Tree Shape at 30M Keys

| Metric | Value |
|--------|-------|
| Inner nodes (no prefix) | 1,283 |
| Inner prefix nodes | 65,792 |
| Leaf nodes | 514,746 |
| Value nodes | 0 (all inlined) |
| Max depth | 5 |
| Avg branches/inner | 8.67 |
| Keys per leaf | ~58 |

### Why It's Fast

1. **Depth 5** for 30M keys -- only 5 pointer dereferences per lookup
2. **Batched leaves** (~58 keys) -- excellent cache locality
3. **67-byte inner nodes** -- fit in 1-2 cache lines
4. **Node-level copy-on-write** -- copies 67 bytes per mutation, not 4KB pages
5. **Inline values** -- values up to 64 bytes stored directly in leaves, no extra indirection
6. **Lock-free reads** -- MVCC via atomic ref counts
