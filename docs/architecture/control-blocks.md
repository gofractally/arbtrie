# Control Blocks & Address Allocation

## The Real Bottleneck: Cache Misses, Not CPU Cycles

The conventional wisdom in data structure design focuses on minimizing comparisons, branch mispredictions, and algorithmic complexity. PsiTri is built on a different premise: **for modern hardware, the dominant cost in tree traversal is cache line loads, not CPU instructions.**

A modern CPU can execute hundreds of instructions in the time it takes to load a single cache line from main memory (~100ns). An L1 cache hit is ~1ns. That's a 100x difference. The entire design of PsiTri's addressing system exists to minimize the number of cache lines touched during tree operations.

### The Problem with Pointer-Based Trees

Every pointer-based tree structure -- B-trees, ART, red-black trees, skip lists -- pays a hidden cost that grows with branching factor. Consider what happens when an inner node with N children needs to visit those children:

**Traditional 4-8 byte pointers**: Each child reference is a 4-8 byte pointer or ID. Dereferencing it loads a control structure (reference count, location, etc.) at an unpredictable memory address. With N children scattered across memory, that's **N cache line loads** just to read the metadata -- before touching any child data.

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

### PsiTri's Approach: Separate and Compress the Indirection

An obvious objection: PsiTri adds a level of indirection -- every pointer dereference must first look up the control block to find the physical location, then load the actual node. Don't you pay *more* cache misses, not fewer?

The answer is no -- and the benefit comes in two layers.

**Layer 1: Separation alone is already a partial win.** Even without intentional co-location, a flat metadata array has two advantages over inline metadata. First, some siblings will randomly land on the same cache lines -- with 8 control blocks per cache line, random placement still yields incidental sharing. Second, the control block array is compact (8 bytes per object vs 64+ byte node pages) and can be pinned to RAM, ensuring metadata access never triggers a disk page fault. When inline metadata is stored in node data pages, a cold node means the refcount access pays the worst-case cost: a disk read of an entire 4 KB page just to touch 4 bytes. With a separated, pinned control block array, metadata access is always a RAM hit -- never a disk fault.

**Layer 2: Intentional co-location turns a partial win into a decisive one.** PsiTri's allocator actively co-locates sibling control blocks on shared cachelines. When a node allocates a child, it passes hints (the `ptr_address` values of existing siblings), and the allocator places the new control block in the same 16-slot group. This converts N scattered RAM loads into a small constant number of loads -- typically 1-2 cache lines for nodes with up to 16 children.

Most persistent tree structures that support snapshots or MVCC face this same scattered-metadata problem. Systems like WiredTiger, InnoDB, PostgreSQL, and CouchDB all store reference counts or version metadata inline with each node or row. When they need to retain, release, or scan children, they pay the full cost of loading each child's data page just to touch a few bytes of metadata. None of them separate the metadata, and none of them attempt co-location -- because without the flat control block array, there is no natural structure to co-locate *into*.

**The result of separation + co-location together:**

Traditional trees *also* load metadata (reference counts, flags) on every traversal -- they just hide it by storing it inline with each node's data. When a traditional tree node with 256 children needs to retain or release those children (copy-on-write, snapshot, destruction), it must load each child's data page just to touch the refcount. That's **256 cache line loads of 64+ bytes each** -- and on modern CPUs that prefetch in 128-byte pairs, potentially **256 x 128 = 32 KB of memory bandwidth**, every byte of which evicts something useful from the CPU cache.

PsiTri separates the metadata into a dense, flat, contiguous control block array **and co-locates siblings within it**. The same 256-child retain/release operation touches at most **16 cacheline groups x 128 bytes = 2 KB** -- the control blocks of all 256 children packed into 16 groups. That is **16x less memory bandwidth** and **16x less cache pollution**. The control block array is small (8 bytes per object; ~4.6 MB for 580K nodes at 30M keys), heavily accessed, and stays hot in L2/L3 cache.

For a single lookup traversal, PsiTri loads one control block (likely already in cache because siblings keep the surrounding entries hot) and then one node. Traditional trees load one node page but that page is 4 KB. PsiTri's path touches fewer total bytes.

---

## The Control Block Array

Every allocated object gets a permanent 32-bit ID (`ptr_address`). This ID indexes into a flat, contiguous array of **8-byte atomic control blocks** -- one per object, 8 per 64-byte cache line (though the allocator operates on 16-slot groups spanning two adjacent cache lines):

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

PsiTri's allocator deliberately places sibling nodes' control blocks in the **same 16-slot group** (128 bytes, spanning two adjacent cache lines).

When a node allocates a child, it passes **allocation hints** -- the `ptr_address` values of existing siblings. The allocator rounds down to a 16-element boundary and tries to place the new object in the **same 16-slot group** in the control block array:

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

A node with 16 children that all share one 16-slot group does **1-2 cache line loads** to dereference all of them, instead of 16. Even a node with the maximum 256 children (spread across up to 16 groups) only needs 16-32 cache line loads -- compared to 256 in a traditional design.

## 1-Byte Branch Encoding

Co-location enables an extreme pointer compression scheme. Instead of storing 4-byte addresses to children, inner nodes store **1-byte branch references**:

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
Traditional:  256 branches x 4-byte addresses            = 1,024 bytes  (16 cache lines)
PsiTri:       256 branches x 1-byte refs + 16 x 4-byte  =   320 bytes  ( 5 cache lines)
```

### Per-Child Pointer Overhead: How PsiTri Compares

The 1-byte branch encoding gives PsiTri the lowest per-child pointer overhead of any pointer-based tree structure. Each child costs 1 byte (the branch) plus its share of the cacheline base table (4 bytes per cacheline group, amortized across all branches using that group). The more branches share each cacheline group, the lower the per-child cost:

**PsiTri inner_node overhead by configuration:**

| Branches | Clines | Raw bytes | Alloc size | Per-child total | Per-child pointer |
|----------|--------|-----------|------------|-----------------|-------------------|
| 2        | 1      | 27        | 64         | 32.0 B          | 3.00 B            |
| 4        | 1      | 31        | 64         | 16.0 B          | 2.00 B            |
| 8        | 2      | 43        | 64         | 8.0 B           | 2.00 B            |
| 16       | 2      | 59        | 64         | 4.0 B           | 1.50 B            |
| 32       | 4      | 99        | 128        | 4.0 B           | 1.50 B            |
| 64       | 8      | 179       | 192        | 3.0 B           | 1.50 B            |
| 128      | 12     | 323       | 384        | 3.0 B           | 1.38 B            |
| 256      | 16     | 595       | 640        | 2.5 B           | 1.25 B            |

*Raw bytes = 19 + 2×branches + clines×4 (header + dividers + branches + cline table). Alloc size = round_up to 64-byte multiple. Per-child pointer = (branches×1 + clines×4) / branches.*

The "per-child pointer" column isolates the cost of encoding child references. At 256 branches with 16 cacheline groups, each child reference costs just **1.25 bytes**. Even at typical sizes (8-32 branches), it's **1.5-2.0 bytes per child**.

**Comparison with other tree structures:**

| Data Structure | Per-Child Pointer | Total Per-Child | Source |
|---|---|---|---|
| **PsiTri (16 branches, 2 clines)** | **1.50 B** | **4.0 B** | This project |
| **PsiTri (256 branches, 16 clines)** | **1.25 B** | **2.5 B** | This project |
| InnoDB B+tree | 4 B | ~9 B + key | 32-bit page IDs |
| LMDB B+tree | 6 B | ~10 B + key | 48-bit pgno packed in 8 B header + 2 B offset |
| BoltDB B+tree | 8 B | 16 B + key | `branchPageElement` = 16 B |
| ART Node16 | 8 B | 10.0 B | 160 B / 16 children (Leis et al. 2013) |
| ART Node256 | 8 B | 8.1 B | 2,064 B / 256 children |
| ART Node48 | 8 B | 13.7 B | 656 B / 48 children (256 B index array) |
| HOT | 8 B | ~11-14 B | Binna et al. 2018 |
| Masstree | 8 B | ~15.5 B | 8 B key slice + 8 B pointer (Mao et al. 2012) |
| Red-black tree | 8 B | 12.5 B | 2 children + parent + color |
| Skip list (p=0.5) | 8 B | 16.0 B | Expected 2 pointers per node |
| Bw-tree | 8 B | 16.0 B | 8 B PID + 8 B mapping table entry |
| WiredTiger | 8 B | 56 B | In-memory `WT_REF` struct |

Every other structure pays at least **4 bytes per child** for the pointer alone -- and most pay **8 bytes** (a full 64-bit pointer or page ID). PsiTri's 1-byte branch encoding with amortized cacheline bases achieves **1.25-2.0 bytes per child**, a 4-6x reduction over the state of the art.

The key insight: by separating addressing into a flat control block array and encoding sibling references relative to shared cacheline groups, PsiTri converts what is normally a per-child cost (pointer storage) into a per-*group* cost (cacheline base address), then amortizes that cost across all children sharing the group.

### The Cascade Effect

Smaller pointers create a cascade of benefits:

1. **Smaller pointers** -> smaller nodes (typically 1-2 cache lines vs 4 KB)
2. **Smaller nodes** -> fewer cache lines loaded per traversal step
3. **Fewer cache lines per node** -> less cache pollution, more of the working set stays hot
4. **Smaller nodes** -> node-level copy-on-write becomes practical (copying 1-2 cache lines vs 4 KB)
5. **Node-level COW** -> dramatically less write amplification per mutation
6. **Co-located siblings** -> dereferencing N children costs 1-2 cache line loads, not N

None of this is possible with 4-byte pointers stored directly. The 1-byte branch encoding and the flat control block array are the enablers.

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

This is faster than scalar byte-by-byte transformation and completely branchless.

---

## How It All Fits Together

The control block system creates a virtuous cycle:

1. **Permanent IDs** mean pointers in the tree never need updating
2. **Flat array layout** means adjacent IDs share cache lines
3. **Hint-based allocation** places siblings on shared cache lines
4. **1-byte branch encoding** exploits shared cache lines for 6x pointer compression
5. **Compressed inner nodes** (typically 1-2 cache lines) make node-level copy-on-write practical
6. **Node-level COW** means mutations write ~2.3 KB (4 inner nodes + 1 leaf) instead of ~20 KB
7. **O(1) relocation** via CAS means the compactor can defragment without pausing anything

Remove any one piece and the others lose their leverage. The control block is the keystone that holds it all together.
