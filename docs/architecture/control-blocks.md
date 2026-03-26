# Control Blocks & Address Allocation

## The Real Bottleneck: Cache Misses, Not CPU Cycles

The conventional wisdom in data structure design focuses on minimizing comparisons, branch mispredictions, and algorithmic complexity. PsiTri is built on a different premise: **for modern hardware, the dominant cost in tree traversal is cache line loads, not CPU instructions.**

A modern CPU can execute hundreds of instructions in the time it takes to load a single cache line from main memory (~100ns). An L1 cache hit is ~1ns. That's a 100x difference. The entire design of PsiTri's addressing system exists to minimize the number of cache lines touched during tree operations.

### The Problem with Pointer-Based Trees

Every pointer-based tree structure -- B-trees, ART, red-black trees, skip lists -- pays a hidden cost that grows with branching factor. Consider what happens when an inner node with N children needs to visit those children:

**Traditional 8-byte pointers**: Each child reference is an 8-byte pointer or ID. Dereferencing it loads a control structure (reference count, location, etc.) at an unpredictable memory address. With N children scattered across memory, that's **N cache line loads** just to read the metadata -- before touching any child data.

```
Traditional tree node with 16 children:

  Node (128+ bytes for pointers alone)
  ┌─────────────────────────────────────────────────────┐
  │ ptr[0]=0x7f3a...  ptr[1]=0x7f8c...  ptr[2]=0x7fb1..│  8 bytes each
  │ ptr[3]=0x7f22...  ptr[4]=0x7fa0...  ...             │
  └─────────────────────────────────────────────────────┘
       │                    │                   │
       v                    v                   v
   ┌────────┐          ┌────────┐          ┌────────┐
   │ ref=3  │          │ ref=1  │          │ ref=2  │     Each at a random
   │ loc=.. │          │ loc=.. │          │ loc=.. │     memory address
   └────────┘          └────────┘          └────────┘
   cacheline A         cacheline B         cacheline C    = 16 cache misses
```

This is the fundamental bottleneck in every existing persistent tree:

| Structure | Pointer size | Node with 16 children | Cache lines to visit all children |
|-----------|-------------|----------------------|----------------------------------|
| B-tree (LMDB) | 8 bytes (page ID) | 128 bytes of pointers | Up to 16 |
| ART Node256 | 8 bytes | 2,048 bytes of pointers | Up to 256 |
| `std::shared_ptr` tree | 16 bytes (ptr + control) | 256 bytes of pointers | Up to 32 |
| Red-black / AVL | 8 bytes x 2-3 | 16-24 bytes | 2-3 per node, but deep |

The node itself is also bloated by all those pointers. A B-tree node with 256 children spends 2 KB just on child pointers. That's 32 cache lines for the node alone, before any key data. Bigger nodes mean more cache lines loaded per traversal step, which means more evictions of other useful data from the CPU cache.

**This is the real write amplification problem.** Copy-on-write databases copy entire nodes on mutation. A 4 KB B-tree page holds ~200 keys but wastes most of its space on pointers and alignment. Copying 4 KB to change one key is bad, but the deeper problem is that the page was 4 KB in the first place because the pointers are so large.

### PsiTri's Approach: Compress the Indirection

PsiTri attacks this problem at the root by making the indirection layer -- the control blocks -- **dense, flat, and cache-friendly**, then exploiting that layout to compress pointers from 8 bytes to 1 byte.

---

## The Control Block Array

Every allocated object gets a permanent 32-bit ID (`ptr_address`). This ID indexes into a flat, contiguous array of **8-byte atomic control blocks** -- one per object, packed 8 per CPU cache line:

```
ptr_address (32-bit permanent ID)
     |
     v  array lookup (O(1))
control_block (64-bit atomic)
  ┌──────────────────────────────────────────────────────────────┐
  │ ref_count (21b) │ cacheline_offset (41b) │ active │ pending  │
  └──────────────────────────────────────────────────────────────┘
     |
     v  direct memory access
Object Data (at physical location in memory-mapped segment)
```

The `ptr_address` never changes for the lifetime of the object. The physical location can change at any time -- the control block is the single source of truth.

Because the array is flat and contiguous, adjacent `ptr_address` IDs share the same CPU cache line. This is not incidental -- it's the foundation of everything that follows.

---

## Engineered Cacheline Co-Location

PsiTri's allocator deliberately places sibling nodes' control blocks on the **same cache lines**.

When a node allocates a child, it passes **allocation hints** -- the `ptr_address` values of existing siblings. The allocator rounds down to the cacheline boundary and tries to place the new object on the **same 16-slot cacheline** in the control block array:

```
Random allocation (no hints):
  Control block array:
  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
  │ ..A.........   │ │ ......B.....   │ │ .C..........   │
  └── cacheline 0 ─┘ └── cacheline 1 ─┘ └── cacheline 2 ─┘
  3 siblings = 3 cache line loads

Hint-based allocation (co-located):
  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
  │ .ABC........   │ │ ..............  │ │ ..............  │
  └── cacheline 0 ─┘ └── cacheline 1 ─┘ └── cacheline 2 ─┘
  3 siblings = 1 cache line load
```

A node with 16 children that all share one cacheline does **1 cache line load** to dereference all of them, instead of 16. Even a node with the maximum 256 children (spread across up to 16 cachelines) only needs 16 cache line loads -- compared to 256 in a traditional design.

## 1-Byte Branch Encoding

Co-location enables an extreme pointer compression scheme. Instead of storing 8-byte pointers to children, inner nodes store **1-byte branch references**:

```
branch (1 byte)
┌──────────────┬──────────────┐
│ high nibble  │ low nibble   │
│ cacheline    │ slot within  │
│ index (0-15) │ line (0-15)  │
└──────────────┴──────────────┘
```

Each inner node stores up to 16 **cacheline base addresses** (4 bytes each). To resolve a branch to a full `ptr_address`:

```
full_address = clines[branch >> 4].base() + (branch & 0x0f)
```

This supports up to **256 branches** (16 cachelines x 16 slots) per node:

```
Traditional:  256 branches x 8-byte pointers            = 2,048 bytes  (32 cache lines)
PsiTri:       256 branches x 1-byte refs + 16 x 4-byte  =   320 bytes  ( 5 cache lines)
```

This is a 6.4x space reduction, but the cache impact is even larger. The traditional node *itself* spans 32 cache lines. The PsiTri node spans 5. Reading the node pollutes 5 cache lines instead of 32 -- leaving 27 more cache lines available for other data.

### The Cascade Effect

Smaller pointers create a cascade of benefits:

1. **Smaller pointers** -> smaller nodes (avg 67 bytes vs 4 KB)
2. **Smaller nodes** -> fewer cache lines loaded per traversal step
3. **Fewer cache lines per node** -> less cache pollution, more of the working set stays hot
4. **Smaller nodes** -> node-level copy-on-write becomes practical (copying 67 bytes vs 4 KB)
5. **Node-level COW** -> 60x less write amplification per mutation
6. **Co-located siblings** -> dereferencing N children costs 1-2 cache line loads, not N

None of this is possible with 8-byte pointers. The 1-byte branch encoding and the flat control block array are the enablers.

### Comparison: Cache Lines Per Tree Operation

For a point lookup traversing 5 levels in a tree with 30M keys:

| Structure | Cache lines per level | Total for 5 levels | Notes |
|-----------|----------------------|-------------------|-------|
| B-tree (4KB pages) | ~4-8 per page load | 20-40 | Plus page cache overhead |
| ART Node48 | 1-3 per node | 5-15 | In-memory only, not persistent |
| ART Node256 | 4+ per node | 20+ | 2KB per node |
| `std::map` (red-black) | 1 per node, but ~20 levels | ~20 | Deep tree, poor locality |
| **PsiTri** | **1-2 per node** | **5-10** | **Persistent, COW, crash-safe** |

PsiTri achieves cache performance competitive with in-memory-only data structures while providing full persistence, copy-on-write, and crash safety.

---

## O(1) Object Relocation

The control block indirection also enables the compactor to move any object with a single atomic instruction:

1. Copy the object data to a new location
2. Atomically swap the location in the control block (`compare_exchange`)

No pointer updates anywhere in the tree. No tracing. No pause. Concurrent readers that already resolved the old location are safe -- the reference count prevents the old copy from being freed until they release.

```
Compactor                          Reader thread
    |                                   |
    |  memcpy(new_loc, old_loc, size)   |
    |                                   |
    |  CAS(control_block,               |  ptr = control_block.load()
    |      old_loc -> new_loc)          |  // gets new location
    |                                   |
    |  (old_loc freed when              |  read(ptr)  // valid data
    |   last reader releases)           |
```

| System | Object Relocation Cost | Concurrent? | Persistent? |
|--------|------------------------|-------------|-------------|
| Java ZGC | Read barrier on every pointer access | Yes | No |
| Go GC | Stop-the-world + pointer updates | Partially | No |
| LMDB | Cannot relocate (offline copy only) | N/A | Yes |
| PostgreSQL VACUUM | O(indexes) per row | Limited | Yes |
| RocksDB compaction | Rewrite entire SST file | Background | Yes |
| **PsiTri SAL** | **memcpy + 1 atomic CAS** | **Fully** | **Yes** |

---

## Control Block Bit Layout

```
┌──────────────────────────────────────────────────────────────────┐
│ bit 63        bit 62     bits 61..21                bits 20..0   │
│ pending_cache active     cacheline_offset (41b)     ref (21b)    │
└──────────────────────────────────────────────────────────────────┘
```

- **ref (21 bits)**: Atomic reference count. `ref == 1` means unique ownership -- in-place modification is safe. `ref > 1` triggers copy-on-write. Maximum ~2 million concurrent references.
- **cacheline_offset (41 bits)**: Physical location as a 64-byte cacheline index. The 41-bit field can theoretically address 128 TB; the configured limit is 32 TB.
- **active (1 bit)**: Set when the object is accessed. Used by the MFU caching algorithm.
- **pending_cache (1 bit)**: Set when an already-active object is accessed again. Signals to the compactor that this object is hot and should be promoted to pinned memory.

Reference counting uses `fetch_add` (faster than CAS) with a buffer of 64 below the 21-bit maximum to absorb concurrent overshoots from up to 64 threads.

The zone-striped ID allocator that powers hint-based co-location is covered in detail in [Concurrent Writers](concurrent-writers.md#zone-striped-id-allocation).

---

## SIMD-Accelerated Branch Remapping

When inner nodes are split, merged, or cloned, branches must be **remapped** because the set of referenced cachelines changes. If an inner node with 16 cachelines is split into two nodes, each child node only references a subset of the original cachelines -- so the cacheline indices in each branch byte must be rewritten.

PsiTri does this with a **branchless NEON/SIMD transformation**:

1. Build a frequency table: which cachelines are referenced by this subset of branches?
2. Compute a rank lookup table: original cacheline index -> compacted index
3. Apply the LUT to all branches in a single SIMD pass:
    - High nibble (cacheline index) is replaced via the LUT
    - Low nibble (slot within cacheline) is preserved

This is 2x faster than scalar byte-by-byte transformation and completely branchless.

---

## How It All Fits Together

The control block system creates a virtuous cycle:

1. **Permanent IDs** mean pointers in the tree never need updating
2. **Flat array layout** means adjacent IDs share cache lines
3. **Hint-based allocation** places siblings on shared cache lines
4. **1-byte branch encoding** exploits shared cache lines for 6x pointer compression
5. **Compressed inner nodes** (avg 67 bytes) make node-level copy-on-write practical
6. **Node-level COW** means mutations write ~335 bytes instead of ~20KB
7. **O(1) relocation** via CAS means the compactor can defragment without pausing anything

Remove any one piece and the others lose their leverage. The control block is the keystone that holds it all together.
