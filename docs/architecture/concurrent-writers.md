# Concurrent Writers

PsiTri supports multiple writer sessions operating simultaneously on independent trees with near-zero contention. This page explains why writers on different roots never block each other, and does a deep dive into the zone-striped ID allocator that makes it possible.

---

## Why Writers Don't Conflict

Every resource a writer touches is partitioned so that independent writers never compete for the same thing:

### Per-Root Locking

The database has 512 independent roots at the PsiTri layer, each protected by its own mutex. The underlying SAL allocator has 1,024 root object slots with their own mutexes:

```cpp
std::mutex _modify_lock[512];        // PsiTri layer: one per top-level root
std::array<std::mutex, 1024> _write_mutex;  // SAL layer: one per root object
```

Writer on root 0 locks `_modify_lock[0]`. Writer on root 1 locks `_modify_lock[1]`. They never touch the same mutex.

### Per-Session Segments

Each write session gets its own 32 MB segment. Data allocation is a bump pointer -- `_alloc_pos += size` -- with no atomics, no locks, no CAS:

```
Writer A:  [segment 7 ........]  <-- bump allocate here
Writer B:  [segment 12 ......]  <-- bump allocate here, independently
```

No two sessions ever write to the same segment. When a session fills its segment, a background provider hands it a fresh one from a pre-allocated pool -- writers never block on file I/O or `mmap`.

### Per-Session Release Queues

When a writer frees objects (e.g., the old version of a node after copy-on-write), it pushes them into its own 32K-entry circular buffer. No contention. The compactor drains all queues asynchronously in the background.

### Atomic Root Swap

Committing a transaction is a single `std::atomic::exchange()` on `_root_objects[root_index]`, followed by releasing the per-root mutex. Other roots are completely unaffected.

### The Full Picture

Two concurrent writers on different roots:

| Step | Writer A (Root 0) | Writer B (Root 1) |
|------|-------------------|-------------------|
| Lock | `_modify_lock[0]` | `_modify_lock[1]` |
| Allocate data | Bump in segment 7 | Bump in segment 12 |
| Allocate IDs | Random CAS in zone bitmap | Random CAS in zone bitmap |
| Free old nodes | Push to `_release_queue[0]` | Push to `_release_queue[1]` |
| Commit | Atomic swap `_root_objects[0]` | Atomic swap `_root_objects[1]` |

The only shared resource is the zone-striped ID allocator -- and as described below, it's designed so that contention is vanishingly rare even under heavy concurrent allocation.

| Operation | Traditional (B-tree/LSM) | SAL |
|-----------|-------------------------|-----|
| Data allocation | Page latch or WAL lock | Per-session bump (zero contention) |
| ID allocation | Global free-list mutex | Zone-striped random CAS |
| Object release | Shared free-list lock | Per-session queue (zero contention) |
| Capacity expansion | Inline (blocks writer) | Background provider (non-blocking) |

---

## Zone-Striped ID Allocation

The zone-striped allocator is the most innovative piece of the concurrency design. It manages ~4 billion 32-bit `ptr_address` IDs across a memory-mapped free bitmap, with concurrent allocation, automatic growth, balanced distribution, and SIMD-accelerated free-space scanning -- all without locks.

### Structure

The 4-billion-entry address space is divided into **zones** of 4,194,304 IDs each (2^22). Each zone has:

- A **free bitmap**: 65,536 atomic `uint64_t` words, each tracking 64 IDs (1 = free, 0 = allocated)
- An **allocation counter**: atomic `uint32_t` tracking how many IDs are in use in this zone
- **32 MB of control block data**: the actual 8-byte control blocks, memory-mapped contiguously

Zone size (32 MB) deliberately matches segment size (32 MB), making address-to-zone mapping a single division.

Three memory-mapped files back the allocator:

| File | Contents | Size per zone |
|------|----------|---------------|
| `zone.bin` | Control block data (8 bytes each) | 32 MB |
| `free_list.bin` | Free bitmap (1 bit per ID) | 512 KB |
| `header.bin` | Per-zone counters, global metadata | shared |

### The Allocation Algorithm

The algorithm has two phases: a **speculative scan** (non-atomic, SIMD-accelerated) to find the best candidate, then a **targeted CAS** (atomic) to claim it.

**Phase 1: Find the best candidate (speculative)**

```
1. Pick the least-filled zone (min_alloc_zone)
2. Generate a random starting offset in the zone's free bitmap
3. Round down to a 64-byte boundary (8 uint64_t = 512 bits)
4. Load all 512 bits (speculatively, without atomics)
5. SIMD popcount: find the byte with the most set (free) bits
```

The SIMD popcount (`max_pop_cnt8_index64`) is the key optimization. It examines 512 free-list bits -- representing 512 potential IDs -- in a single pass, using platform-specific SIMD:

- **ARM NEON**: `vcntq_u8` computes byte popcount, `vmaxvq_u8` finds the horizontal maximum, comparison + `move_mask_neon` locates the winning byte
- **SSE/SSSE3**: `PSHUFB` nibble-lookup popcount, `_mm_max_epu8` for horizontal max, `_mm_movemask_epi8` to extract the result
- **Scalar fallback**: loop over 64 bytes with `std::popcount`

This scan is deliberately non-atomic -- it reads 64 bytes of the free bitmap as raw memory. The result is a *heuristic*: it tells us which 8-bit region has the most free slots. A stale or torn read just means we pick a slightly suboptimal byte -- the actual claim is always done with an atomic CAS, so correctness is never at risk.

**Phase 2: Claim via atomic CAS (targeted)**

```
6. Compute the ptr_address of the best candidate byte
7. Load the atomic uint64_t containing that byte
8. Mask to the 16 bits corresponding to the target cacheline
9. countr_zero to find the first free bit within the mask
10. CAS to flip that bit from 1 (free) to 0 (allocated)
11. On CAS failure: reload and retry within the same 16-bit window
12. If window exhausted: go back to phase 1 with a new random start
```

The masking step is critical. Each `uint64_t` in the free bitmap covers 64 IDs. A CPU cacheline in the control block array holds 8 control blocks (8 x 8 bytes = 64 bytes). The allocator masks the uint64_t to a 16-bit window aligned to the target cacheline boundary (`0xffffull << base_offset`), ensuring that the allocated ID shares a cacheline with the hint address.

### Why Contention Is Vanishingly Rare

Two threads contend only if they:

1. Pick the same zone (likely if one zone is least-filled, but zones hold 4M IDs)
2. Pick the same random starting cacheline out of 8,192 cachelines per zone
3. Try to CAS the same `uint64_t` word

The probability of step 2 alone is 1/8,192 per attempt. And even when two threads do collide on the same word, the CAS loop resolves it instantly -- the loser reloads and picks the next free bit in the same word, which is still a single atomic operation.

At the 50% fill target, each 512-bit scan finds ~256 free bits. The chance of scanning 512 bits and finding zero free is astronomically low at normal fill levels. Even at 99% fill, the probability that all 512 scanned bits are taken is ~0.5%, and the retry just picks a new random starting point.

### Growth and Balanced Distribution

The allocator grows and rebalances automatically with no configuration:

**Growth trigger**: When the average allocations per zone exceeds 50%, a new zone is allocated. New zones are initialized with all bitmap bits set to 1 (free):

```
if (average_allocations() > ptrs_per_zone / 2)
    ensure_capacity(num_allocated_zones() + 1);
```

The 50% target is not arbitrary -- it ensures the SIMD scan almost always finds free bits on the first try (512 bits at 50% fill = ~256 free bits per scan), while avoiding excessive memory waste.

**Balanced steering via `min_alloc_zone`**: The allocator tracks which zone has the fewest allocations and steers new allocations there. This is maintained incrementally with two rules:

- **On allocation**: If we just allocated from the current `min_alloc_zone` and it's now above the per-zone average, rescan all zones to find the new minimum
- **On deallocation**: If the zone we freed from now has fewer allocations than the current minimum, it becomes the new minimum

```
// On allocation (simplified):
if (this_zone == min_alloc_zone && this_zone_count >= average)
    min_alloc_zone = scan_for_minimum();

// On deallocation (simplified):
if (this_zone_count < min_alloc_zone_count)
    min_alloc_zone = this_zone;
```

This creates a self-balancing feedback loop:

1. A fresh zone is added with zero allocations -- it becomes `min_alloc_zone`
2. All threads steer toward it, filling it up
3. Once it passes the average, the allocator rescans and steers to the next least-filled zone
4. Over time, all zones converge toward equal fill levels

The rescan (`update_min_zone`) is O(num_zones) but triggers infrequently -- only when the current minimum zone crosses the average. With 1024 max zones, this is a scan of 1024 atomic loads, which is negligible compared to the millions of allocations between triggers.

**Deallocation feedback**: When objects are freed (via the compactor draining release queues), `dec_alloc_count` updates the per-zone counter. If the freed zone drops below the current minimum, it becomes the new steering target. This means zones that experience heavy churn naturally attract new allocations, keeping fill levels balanced even under skewed workloads.

### Hint-Based Allocation for Cacheline Co-Location

The most powerful feature of the allocator is **hint-based allocation**, which serves a dual purpose: allocating an ID *and* engineering cache locality.

When a tree node allocates a child, it passes the `ptr_address` values of existing siblings as hints. The allocator tries each hint in order:

```
allocation alloc(const alloc_hint& hint) {
    // Try each sibling's cacheline first
    for (addr : hint) {
        if (auto result = try_alloc(addr))
            return *result;
    }
    // Fall back to random allocation in least-filled zone
    return alloc();
}
```

For each hint, `try_alloc(ptr_address)`:

1. Rounds the hint down to a 16-element cacheline boundary
2. Masks the free bitmap to just the 16 bits on that cacheline
3. CAS-claims the first free bit within the mask

If the cacheline is full, it tries the next hint. If all hints fail, it falls back to the random SIMD-guided path. The fallback deliberately avoids the hinted cachelines -- it picks from the least-filled zones, reducing the chance of stealing a slot that a future hint-based allocation might want.

### Prior Art Comparison

No existing allocator combines all of these properties:

| System | Free tracking | Concurrency | Locality control | Growth |
|--------|--------------|-------------|-----------------|--------|
| **glibc malloc** | Free lists per size class | Per-arena locks | None | mmap/sbrk |
| **jemalloc** | Bitmap + free lists | Per-thread caches | Size-class proximity | Extent allocation |
| **tcmalloc** | Free lists + spans | Per-thread + per-CPU | Size-class only | Page heap |
| **Linux buddy allocator** | Bitmap per order | Zone locks | Power-of-2 only | Cannot grow |
| **PostgreSQL** | Free space map (FSM) | Page-level locks | None | Table extension |
| **LMDB** | B-tree of free pages | Single writer only | None | File growth |
| **SAL zone allocator** | **Atomic bitmap** | **Lock-free CAS** | **Hint-based cacheline** | **Auto-balanced zones** |

The closest prior art is jemalloc's bitmap allocator within slabs, which also uses bitmaps for sub-page allocation. But jemalloc's bitmaps are per-size-class and don't support caller-directed placement hints. The combination of SIMD-guided speculative scanning, hint-based cacheline co-location, and automatic zone rebalancing is unique to SAL.

Traditional database allocators (PostgreSQL FSM, LMDB free-page B-tree) operate at page granularity and use locks or single-writer designs. None support the concept of "allocate near this sibling" because pages don't have the same co-location opportunity that a flat control block array provides.

### SIMD Popcount: Finding the Freest Region

The `max_pop_cnt8_index64` function is the engine that drives the speculative scan. Given 64 bytes of free bitmap data (512 potential IDs), it returns the index of the byte with the most set bits -- the densest free region.

**ARM NEON implementation**:

```
1. vld1q_u8_x4: Load all 64 bytes into 4 NEON registers
2. vcntq_u8:    Byte-level popcount (hardware instruction, 1 cycle)
3. vmaxq_u8:    Pairwise max across all 4 registers
4. vmaxvq_u8:   Horizontal max across single register -> scalar max value
5. vceqq_u8:    Compare all 64 popcounts against max -> match mask
6. move_mask_neon: Extract 1 bit per byte into a uint64_t
7. countr_zero: First set bit = index of winning byte
```

**SSE/SSSE3 implementation**:

```
1. _mm_load_si128 x4: Load 64 bytes into 4 SSE registers
2. PSHUFB lookup:     Nibble-based popcount via 16-byte lookup table
3. _mm_max_epu8:      Pairwise max, then horizontal reduction via shifts
4. _mm_cmpeq_epi8:    Compare all bytes against max
5. _mm_movemask_epi8: Extract 1 bit per byte into uint32_t
6. Combine 4 masks into uint64_t, countr_zero for first match
```

The PSHUFB trick splits each byte into two 4-bit nibbles, looks up each nibble's popcount in a 16-entry table stored in an SSE register, and adds the results. This computes 16 byte popcounts in 5 instructions.

Both implementations complete in ~10-15 cycles for 512 bits -- fast enough that the speculative scan adds negligible overhead to the allocation path.
