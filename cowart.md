# COWART: Copy-on-Write Adaptive Radix Trie for DWAL

## Motivation

The current DWAL architecture maintains fully independent RW and RO ART layers.
When a writer commits, the entire RW ART is frozen into the RO slot and a brand
new RW ART is allocated from scratch. This is expensive: every swap discards and
rebuilds the writer's working tree. Readers consulting the buffered layer must
acquire the RO shared_ptr under a mutex, and the "latest" read mode requires
holding a lock that blocks the next write transaction.

The COWART design keeps the two-tree structure (RW and RO) but makes them
**share subtrees within a common arena**. The key insight: the bump allocator
guarantees that newer nodes have higher arena offsets. By tagging nodes with a
COW generation counter (`cow_seq`), the writer knows which subtrees are shared
with the RO snapshot and must be copied before mutation, and which are
writer-private and can be mutated in place.

This changes the economics fundamentally:
- **Swap is near-free:** Publishing a snapshot just copies the current root
  offset to `last_root` and bumps `cow_seq`. No tree allocation or destruction.
- **No atomic reference counting:** COW decisions use a simple integer comparison
  (`node.cow_seq < current_cow_seq`), not atomic ref count operations.
- **Shared subtrees reduce duplication:** Unmodified subtrees are shared between
  RW and RO — only the path from root to the modified leaf gets COW'd.
- **"Latest" reads are non-blocking:** A reader can traverse the RW tree
  directly. Shared (RO) subtrees within it are immutable (COW-protected), so
  the reader sees a consistent view without locking the writer.
- **No forced swap deadline:** The current design must swap RW→RO before the RW
  arena fills. With shared subtrees, the RW tree can keep growing — the writer
  only COWs the nodes it actually modifies, and the RO snapshot remains valid
  in the same arena.

## Design Principles

1. **COW is expensive — avoid it when there is no contention.** When no reader
   holds a snapshot, the writer mutates nodes in place with zero COW overhead.
2. **Pool allocation means we can leak, but COW consumes the 4 GB arena faster.**
   COW copies allocate new space without freeing the original (the bump allocator
   doesn't support free). Frequent COW under contention increases arena pressure.
   The writer should COW only when a reader actually needs a snapshot, not
   pessimistically.
3. **Keep nodes small for COW, but leave room for in-place growth.** Setlist nodes
   should have spare capacity so that adding a child doesn't require reallocation
   (which is COW-equivalent in cost). Realloc to add a single branch is
   counterproductive.
4. **Setlist nodes support up to 128 children.** The current 48-child limit forces
   early promotion to node256 (1 KB+). A 128-child setlist is ~650 bytes — still
   cheaper to COW than a node256, and covers the vast majority of fan-outs
   without promotion.

## Two-Tree Architecture

```
  ┌─────────────────── Shared Arena (bump allocator, 4 GB max) ──────────────────┐
  │                                                                               │
  │   RW Root ──► [A']──► [B']──► [D']    (writer-private: cow_seq == current)   │
  │                  │       │                                                    │
  │                  │       └───► [E] ◄──────┐                                  │
  │                  │                         │  shared subtrees                 │
  │                  └───► [C] ◄──────────┐   │  (cow_seq < current)             │
  │                                        │   │                                  │
  │   RO Root ──► [A]───► [B]───► [D]     │   │                                  │
  │                  │       │       └──► [E]──┘                                  │
  │                  │       └───► ...                                            │
  │                  └───► [C]─────┘                                              │
  │                                                                               │
  └───────────────────────────────────────────────────────────────────────────────┘
```

The **RW COWART** is the writer's live tree. It contains a mix of:
- **Writer-private nodes** (`cow_seq == current_cow_seq`): allocated or COW'd
  during the current or recent write transactions. The writer mutates these
  in place.
- **Shared subtrees** (`cow_seq < current_cow_seq`): unchanged since the last
  snapshot was published. These subtrees are reachable from both the RW root
  and the RO root. The writer must COW (copy) a shared node before mutating it.

The **RO COWART** is the last published snapshot root. Readers traverse it
freely — all nodes reachable from the RO root are immutable because the writer
COWs before touching any shared node. The RO root is updated atomically by the
writer on commit (just a 32-bit store of the new root offset).

Both trees live in the **same arena**. No data is copied on "swap" — the writer
just publishes its current root as the new RO root and bumps `cow_seq`.

## Shared State

The COWART shared state lives in the `dwal_root` (or equivalent per-root
structure) and is accessed by both the writer thread and reader threads:

```cpp
struct cowart_state
{
   // ── RW COWART coordination ──
   // The writer's current root offset and flags, packed into a single
   // atomic 64-bit word for lock-free reader/writer handoff.
   std::atomic<uint64_t> root_and_flags;

   // ── RO COWART ──
   // The last committed snapshot root. Readers load this atomically.
   // All nodes reachable from this root are immutable (COW-protected).
   std::atomic<uint32_t> last_root;

   // ── Reader/writer synchronization ──
   std::mutex              notify_mutex;
   std::condition_variable writer_done_cv;
};
```

### Packed `root_and_flags` Layout (64 bits)

```
 Bits   Field              Description
 ─────  ─────────────────  ──────────────────────────────────────────────
 63–32  root_offset        RW root node offset in the arena (uint32_t)
 31     reader_waiting      A reader wants a fresh snapshot from the writer
 30     writer_active       A write transaction is in progress
 29–0   cow_seq            COW generation counter (30 bits, ~1 billion generations)
```

- **root_offset (32 bits):** The RW tree's current root offset. This is the
  writer's live, potentially mid-mutation root.
- **reader_waiting (1 bit):** Set by a reader that arrives while
  `writer_active == 1` and chooses to wait (rather than using `last_root`).
  Tells the writer to publish a new RO snapshot on commit.
- **writer_active (1 bit):** Set when a write transaction begins, cleared on
  commit/abort. Tells arriving readers that the RW root may be mid-mutation.
- **cow_seq (30 bits):** Incremented each time the writer publishes a snapshot
  to `last_root`. Nodes whose `cow_seq` is less than the current value are
  shared with the RO tree and must be COW'd before mutation.

## Address-Based COW Detection

The arena bump allocator guarantees:

> Every allocation returns a strictly increasing offset.

When the writer starts a new COW generation (because a reader requested a
snapshot), it records `snapshot_cursor = arena.bytes_used()` — the current
allocation frontier. From that point forward:

- **Node offset < snapshot_cursor:** This node exists in the snapshot. It is
  shared with readers and **must be copied** before the writer mutates it.
- **Node offset >= snapshot_cursor:** This node was allocated after the snapshot.
  It is writer-private and **can be mutated in place**.

This single comparison replaces all atomic reference counting. The check is a
trivial integer comparison on the hot path — no atomics, no cache-line bouncing.

### Why `cow_seq` is Needed in Addition to Address Comparison

Address comparison alone handles the common case (one outstanding snapshot). But
consider:

1. Reader A takes snapshot at cursor position 1000.
2. Writer allocates nodes at 1000–2000, commits, publishes snapshot.
3. Reader B takes snapshot at cursor position 2000.
4. Writer now needs to COW nodes in range 1000–2000 (shared with Reader B) but
   not nodes >2000.

Without `cow_seq`, the writer would need to track multiple snapshot cursors.
With `cow_seq`, each node carries its generation stamp. The writer simply
checks: if `node.cow_seq < current_cow_seq`, COW is required. After COW, the
new copy gets the current `cow_seq`, so it won't be COW'd again until the next
generation.

### Node-Level Generation Tag

Each inner node header gains a field to track its COW generation:

```cpp
struct node_header
{
   node_type type;
   uint8_t   num_children;
   uint16_t  partial_len;
   offset_t  value_off;
   uint32_t  cow_seq;       // NEW: generation when this node was last written
};
```

This grows `node_header` from 8 to 12 bytes (still fits comfortably in a cache
line with the rest of the node data). Leaf nodes do not need `cow_seq` because
leaves are always replaced wholesale (new key-value → new leaf allocation).

## Reader/Writer Protocol

### COW is Reactive, Not Modal

The writer never preemptively COWs. COW is triggered only when a reader signals
demand via `reader_waiting`, or when a **freshness timer** expires. The freshness
timer serves the same role as the current DWAL swap timer, but is much cheaper:
instead of forcing a full RW→RO swap (allocate new arena, freeze old), the timer
just forces a snapshot publication (store `last_root`, bump `cow_seq`).

This means COW frequency is a natural function of reader demand:
- **No readers:** Writer never COWs. Zero overhead. Plain mutable ART.
- **Occasional latest readers:** Writer COWs when signaled. Infrequent, amortized.
- **Sustained latest readers:** Writer effectively COWs every transaction —
  a fixed per-operation cost of O(depth) node copies per modified key, regardless
  of how many readers are active. Compare to current design where each additional
  latest reader holding `rw_mutex` compounds the writer's stall time.
- **Freshness timer only:** Writer publishes a snapshot every N milliseconds.
  Bounds `last_root` staleness without any reader involvement.

### Writer Lifecycle

```
begin_write_transaction():
    CAS root_and_flags to set writer_active = 1
    record current_cow_seq from flags
    check freshness_timer — if expired, set needs_snapshot = true

modify(node):
    if node.cow_seq < current_cow_seq:
        // This node is shared with a published snapshot — COW it
        new_node = arena.allocate(node.size)
        memcpy(new_node, node, node.size)
        new_node.cow_seq = current_cow_seq
        update parent pointer to new_node
        return new_node   // mutate the copy
    else:
        return node       // writer-private, mutate in place

commit_write_transaction():
    new_root = finalized root offset
    if reader_waiting or needs_snapshot:
        // Publish snapshot: store root, bump cow_seq
        last_root.store(new_root, release)
        CAS root_and_flags:
            root_offset = new_root
            writer_active = 0
            reader_waiting = 0
            cow_seq += 1              // marks all current nodes as shared
        writer_done_cv.notify_all()
        reset freshness_timer
    else:
        CAS root_and_flags:
            root_offset = new_root
            writer_active = 0
        // cow_seq unchanged — no snapshot needed, no future COW needed
```

**Key optimization:** When no reader is waiting and the freshness timer hasn't
expired, `cow_seq` does not increment. The next write transaction sees
`node.cow_seq == current_cow_seq` for all recently-written nodes and mutates
them in place — zero COW overhead. The RW tree behaves as a plain mutable ART.

When `cow_seq` does increment, only the **path from root to the next modified
leaf** gets COW'd. Unmodified subtrees remain shared — the writer never touches
them, so they serve both the RW and RO snapshots simultaneously.

### Reader Lifecycle

Readers have three strategies, matching the current DWAL read modes:

#### Buffered — Read Frozen RO Arena + Tri (Non-Blocking)

```
read_buffered():
    // Skip the RW arena entirely. Read only the frozen RO arena
    // (if one exists from a previous flip) merged with Tri.
    // 2-way merge: RO + Tri.
    ro_root = ro_arena_root.load(acquire)
    return merge_snapshot(ro_root, tri_root)
```

The cheapest read mode. Reader never touches the RW arena or its snapshots.
Staleness depends on when the last flip occurred — could be many transactions
behind, but the reader pays for only a 2-way merge (frozen RO arena + Tri).
No writer impact. No synchronization.

#### Fresh — Read Last Snapshot + RO + Tri (Non-Blocking)

```
read_fresh():
    // Take the most recent snapshot on the current RW arena (last_root).
    // This was published by a previous latest reader's signal, the
    // freshness timer, or a previous fresh reader's signal.
    // No waiting — take whatever is available.
    snapshot_root = last_root.load(acquire)
    ro_root = ro_arena_root.load(acquire)
    // 3-way merge: RW snapshot + RO + Tri
    return merge_snapshot(snapshot_root, ro_root, tri_root)
```

Fresher than buffered — sees data up to the last published snapshot on the
current RW arena, which may be 1 or more transactions behind. **No waiting,
no writer impact.** The reader does not set `reader_waiting` or trigger a
`cow_seq` bump. It simply reads whatever snapshot already exists.

The 3-way merge is needed because the RW arena snapshot only contains writes
since the last flip. The frozen RO arena contains writes from the previous
generation, and Tri has everything merged before that.

If no snapshot has been published on the current RW arena yet (`last_root`
is null or from the previous arena), fresh falls back to buffered behavior
(RO + Tri, 2-way merge).

#### Latest — Wait One Tx, Read Freshest Snapshot (Blocking)

```
read_latest():
    flags = root_and_flags.load(acquire)
    if not flags.writer_active:
        // No write in progress — last committed root is up to date.
        // But we still need to ensure a snapshot exists. If cow_seq
        // hasn't been bumped since the last read, the committed root
        // may not be in last_root yet.
        // Signal for next commit to publish a snapshot.
        CAS root_and_flags to set reader_waiting = 1
        // Fall through — if writer_active is still 0, the next
        // begin_write_transaction will see the flag.

    // Wait for at most one write transaction to complete and publish.
    unique_lock lock(notify_mutex)
    writer_done_cv.wait(lock, [&]{
        flags = root_and_flags.load(acquire)
        return !flags.writer_active
    })
    snapshot_root = last_root.load(acquire)
    ro_root = ro_arena_root.load(acquire)
    // 3-way merge: RW snapshot + RO + Tri
    return merge_snapshot(snapshot_root, ro_root, tri_root)
```

**This is the key improvement over the current design.** The writer impact is
a fixed COW cost (O(depth) node copies per modified key) that does not scale
with the number of readers. Compare to the current design where each latest
reader holds `rw_mutex` for its entire read duration, and concurrent latest
readers compound the stall.

Under sustained latest-reader load, the writer effectively COWs every
transaction. But this is a **bounded, predictable cost** — roughly 4–8 node
copies (one per ART level) of 64–704 bytes each. The reader waits at most one
transaction duration (microseconds to low milliseconds), not an entire swap
cycle.

### Multiple Concurrent Readers

The `reader_waiting` bit is a single flag, not a counter. This is intentional:

- The bit signals "at least one reader needs a snapshot." Whether it's 1 or 100
  readers doesn't change the writer's behavior — it bumps `cow_seq` once.
- All waiting readers wake on `writer_done_cv.notify_all()` and read the same
  snapshot.
- Buffered and fresh readers never touch the flag at all.

Active reader tracking (for arena reclamation) uses a separate mechanism — see
**Dual-Arena Cycling** below.

## Node Layout Changes

### Setlist: Expanded to 128 Children

```
Current:  setlist_max_children = 48    → promotes to node256 at 49
Proposed: setlist_max_children = 128   → promotes to node256 at 129
```

Compare to node256: 1,032+ bytes header + 1,024 bytes children = 2,048+ bytes.
A 128-child setlist is ~704 bytes — COW copies ~2× cheaper, ~11 cache lines
vs ~32 for node256.

### Split-Growth Layout

The current setlist layout packs keys and children contiguously:

```
Current:
  [header][cap][keys[0..n-1]  ...padding...  children[0..n-1]][prefix]
                ──────────── both grow right ──────────────►
```

Inserting at position `i` requires shifting the tail of **both** arrays right:
- keys[i..n-1] right by 1: **(n−i) bytes**
- children[i..n-1] right by 4: **(n−i)×4 bytes**
- Total: **5×(n−i) bytes moved**

The new layout grows keys forward from the header and children backward from
the tail, with free space in the middle:

```
New:
  [header][keys[0..n-1] →   FREE SPACE   ← children[n-1..0]][prefix]
```

The prefix is fixed at the tail of the allocation (it never changes after node
creation). Children are stored in reverse order just before the prefix, growing
leftward toward the center. Keys grow rightward from the header as before.

Inserting at position `i` now shifts each array **toward the center**:
- keys[i..n-1] right by 1 (into free space): **(n−i) bytes**
- children[0..i-1] left by 4 (into free space): **i×4 bytes**
- Total: **(n−i) + 4×i bytes moved**

```
  Position i:    0      n/4      n/2      3n/4     n
  ────────────────────────────────────────────────────
  Current:       5n     3.75n    2.5n     1.25n    0      bytes moved
  New:           n      2n       3n       4n       4n     (uncapped)
  New (optimal): n      1.5n     2.5n     2.5n     1.5n   (move smaller side)
```

For **insertions in the first half** (i < n/2) — which is the common case for
sorted/sequential key patterns — the new layout moves dramatically less data.
At i=0, current moves 5n bytes; new moves just n bytes (5× improvement).

If both arrays can optionally shift toward the **nearer edge** (keeping a small
gap at the key-start position), then for any insertion point you move the smaller
side of each array: `min(i, n−i) + 4×min(i, n−i) = 5×min(i, n−i)`. This caps
the worst case at 2.5n (at i=n/2) instead of the current 5n.

### Node Layout Detail

```
Offset  Contents                          Growth direction
──────  ──────────────────────────────    ────────────────
0       node_header (12 bytes + tail_gap)
13      keys[0], keys[1], ...             → rightward
13+n    ... free left ...
        ... children (centered) ...       ◄ drifts ►
        ... free right ...
A-plen  prefix[0..plen-1]                 (fixed at tail)
A       end of allocation (cacheline-aligned)
```

Where `A` = total allocation size, `plen` = prefix length, `n` = num_children.

```cpp
struct node_header
{
   node_type type;          // 1 byte
   uint8_t   num_children;  // 1 byte
   uint16_t  partial_len;   // 2 bytes
   offset_t  value_off;     // 4 bytes
   uint32_t  cow_seq;       // 4 bytes
   uint8_t   tail_gap;      // number of free 4-byte slots between
                            // children[n-1] and the prefix
};  // 13 bytes
```

`tail_gap` locates the children block within the free space. Given the
allocation size and prefix length:

```
children_end   = A - plen - tail_gap * 4
children_start = children_end - n * 4
children[i]    = children_start + i * 4
```

On insert at position `i`, pick the smaller half to shift:
- If `i < n/2`: shift children[0..i-1] left by 4 (free left grows by 0, free
  right unchanged, `tail_gap` unchanged)
- If `i >= n/2`: shift children[i..n-1] right by 4 into tail gap
  (`tail_gap -= 1`)

On initial allocation, center the children block: `tail_gap = free_slots / 2`.

**Recentering on COW:** Over time, skewed insertion patterns (e.g., mostly
ascending keys) push the children block toward one edge, reducing the benefit
of shifting the smaller half. COW naturally fixes this: since the node is
already being copied byte-by-byte into a new allocation, the copy can recenter
the children block at zero additional cost — you're writing to fresh memory
anyway, so just place the children in the middle of the free space. This means
every COW copy restores optimal insert performance for the next generation.

The total free space is `A - plen - 13 - n - 4n`. When free space reaches zero,
the node must be reallocated (or promoted to node256 at max capacity).

### Allocation with Spare Capacity

When allocating a new setlist, always round up to fill the cacheline-aligned
allocation. Since the arena rounds every allocation to the next cacheline
boundary, any bytes between the end of the used data and the boundary are
wasted padding. By sizing the allocation to fill the cacheline, the free space
in the middle is maximized — giving extra insert headroom for free:

```cpp
uint32_t setlist_alloc_size(uint8_t num_children, uint16_t prefix_len)
{
    // Minimum: header + keys + children + prefix
    uint32_t min_size = sizeof(node_header) + num_children + num_children * 4 + prefix_len;
    // Round up to cacheline boundary — free space in the middle is the bonus
    return (min_size + cacheline_size - 1) & ~(cacheline_size - 1);
}

uint8_t max_children_for_alloc(uint32_t alloc_size, uint16_t prefix_len)
{
    // Each child needs 5 bytes (1 key + 4 offset)
    uint32_t available = alloc_size - sizeof(node_header) - prefix_len;
    return std::min<uint32_t>(available / 5, setlist_max_children);
}
```

A node allocated for 3 children gets a 128-byte block. After header (12) and
prefix, there's room for ~23 children — 20 free slots from cacheline rounding
alone. Under COW, this headroom is critical: reallocation means allocating a
new node and abandoning the old one in the bump arena.

## Dual-Arena Cycling

Within a single arena, the COW mechanism (described above) allows the writer to
publish RO snapshots by bumping `cow_seq`. Old copies of COW'd nodes remain in
the arena — the bump allocator has no per-node free. Over time, these dead nodes
accumulate as "COW debris," wasting arena space.

The dual-arena design solves two problems:

1. **Bulk reclamation without reference counting.** Dead COW nodes can't be freed
   individually (no ref counts, no free list). But when an entire arena is frozen
   and merged into Tri, and all readers release their snapshots, the whole arena
   can be `clear()`'d in one shot — reclaiming all dead nodes at once.

2. **Keeping the active tree small.** A smaller tree means shorter paths, fewer
   cache misses, and faster inserts. By periodically draining the accumulated
   data into Tri and starting fresh, the RW tree stays compact.

### Arena Memory Model

Each arena reserves 4 GB of virtual address space upfront using
`mmap(MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE)`. This costs nothing — no
physical memory is committed, no RSS impact. The base pointer is fixed for the
lifetime of the arena.

The arena is implemented as a standalone, cross-platform utility class
(`ucc::vm_arena`) in the UCC library (`libraries/ucc/`). It has no dependency
on ART, psitri, or any other library — just platform headers. This makes it
reusable for any bump-allocator use case (e.g., scratch buffers, serialization
arenas, temporary allocations).

As the bump cursor advances past the current high-water mark, new pages are
faulted in on demand (or pre-faulted with `madvise(MADV_POPULATE_WRITE)` in
batches for performance). The committed region grows monotonically; it never
shrinks until the arena is freed in bulk.

```cpp
// ucc::vm_arena — libraries/ucc/include/ucc/vm_arena.hpp
class vm_arena
{
   static constexpr uint64_t reservation_size = 4ULL << 30;  // 4 GB
   static constexpr uint32_t grow_granularity = 2u << 20;    // 2 MB

   char*    _base;       // stable for arena lifetime — never moves
   uint32_t _cursor;     // bump allocator position
   uint32_t _committed;  // bytes backed by physical pages

   arena() : _cursor(0), _committed(0)
   {
#ifdef _WIN32
      // Windows: VirtualAlloc with MEM_RESERVE, commit on demand
      _base = (char*)VirtualAlloc(nullptr, reservation_size,
                                  MEM_RESERVE, PAGE_READWRITE);
#else
      // POSIX (Linux, macOS): mmap with no physical backing
      // MAP_NORESERVE is a no-op on macOS but harmless.
      _base = (char*)mmap(nullptr, reservation_size,
                          PROT_NONE,
                          MAP_ANONYMOUS | MAP_PRIVATE | MAP_NORESERVE,
                          -1, 0);
#endif
   }

   offset_t allocate(uint32_t size) {
      uint32_t rounded = (size + cacheline_size - 1) & ~(cacheline_size - 1);
      uint32_t off = _cursor;
      _cursor = off + rounded;
      if (_cursor > _committed)
         grow_committed(_cursor);
      return off;
   }

   void grow_committed(uint32_t needed) {
      uint32_t new_committed =
          (needed + grow_granularity - 1) & ~(grow_granularity - 1);
#ifdef _WIN32
      VirtualAlloc(_base + _committed, new_committed - _committed,
                   MEM_COMMIT, PAGE_READWRITE);
#else
      // POSIX: change protection from PROT_NONE to read/write.
      // Pages are zero-filled on first access (demand-paged).
      mprotect(_base + _committed, new_committed - _committed,
               PROT_READ | PROT_WRITE);
#endif
      _committed = new_committed;
   }

   void clear() {
#ifdef _WIN32
      // Decommit: releases physical pages, keeps reservation
      VirtualFree(_base, _committed, MEM_DECOMMIT);
#elif defined(__linux__)
      // Linux: MADV_DONTNEED releases pages immediately, keeps mapping
      madvise(_base, _committed, MADV_DONTNEED);
      mprotect(_base, _committed, PROT_NONE);
#else
      // macOS: madvise with MADV_FREE (lazy) or re-mmap to drop pages
      madvise(_base, _committed, MADV_FREE);
      mprotect(_base, _committed, PROT_NONE);
#endif
      _cursor = 0;
      _committed = 0;
   }

   ~arena() {
#ifdef _WIN32
      VirtualFree(_base, 0, MEM_RELEASE);
#else
      munmap(_base, reservation_size);
#endif
   }
};
```

The approach uses `PROT_NONE` on the reserved region and `mprotect` to commit
in 2 MB chunks. This works identically on Linux and macOS — both support
`mmap(MAP_ANONYMOUS)` + `mprotect`. On Windows, the equivalent is
`VirtualAlloc(MEM_RESERVE)` + `VirtualAlloc(MEM_COMMIT)`. Page release uses
the platform-appropriate mechanism: `MADV_DONTNEED` (Linux), `MADV_FREE`
(macOS), or `MEM_DECOMMIT` (Windows).

Benefits over `malloc`/`realloc`:
- **Stable base pointer:** No realloc, no copy, no dangling pointers. Latest-
  mode readers can traverse the arena concurrently without risk.
- **O(new pages) growth:** Only new pages are committed. No O(arena_size)
  memcpy on growth.
- **Bulk reclamation:** Physical pages are released back to the OS without
  unmapping. The virtual reservation stays in place for reuse.
- **THP friendly (Linux):** Growing in 2 MB chunks lets the kernel use
  transparent huge pages, reducing TLB pressure.

```
  ┌──────────────┐      ┌──────────────┐
  │   Arena A    │      │   Arena B    │
  │  (RW active) │      │   (free)    │
  │              │      │              │
  │  RW root ─┐  │      │              │
  │  RO root ─┤  │      │              │
  │  (shared   │  │      │              │
  │  subtrees) │  │      │              │
  │  + COW     │  │      │              │
  │  debris    │  │      │              │
  └──────────────┘      └──────────────┘

         │ Arena A fills
         ▼

  ┌──────────────┐      ┌──────────────┐
  │   Arena A    │      │   Arena B    │
  │  (RO frozen) │      │  (RW active) │
  │              │      │              │
  │  frozen root──merge──►into Tri     │
  │  all nodes   │      │  RW root     │
  │  immutable   │      │  (starts     │
  │              │      │   empty)     │
  └──────────────┘      └──────────────┘

         │ Merge done + readers release
         ▼

  ┌──────────────┐      ┌──────────────┐
  │   Arena A    │      │   Arena B    │
  │   (free)    │      │  (RW active) │
  │  clear()    │      │              │
  │  cursor = 0  │      │  RW root     │
  │  ready to    │      │  RO root     │
  │  reuse       │      │  growing...  │
  └──────────────┘      └──────────────┘
```

### Arena States

| State | Allocations | Mutations | Readers |
|-------|-------------|-----------|---------|
| **RW (active)** | Writer allocates new + COW'd nodes | Writer mutates private nodes in place | Latest-mode readers may traverse |
| **RO (frozen)** | None | None — all nodes immutable | Buffered readers traverse; merge thread drains to Tri |
| **Free** | None | None | None — safe to `clear()` and reuse |

### Flip Lifecycle

1. **Steady state:** Arena A is RW. Both the RW root and RO root (`last_root`)
   point into Arena A, sharing subtrees via `cow_seq`. The writer publishes
   snapshots within Arena A by bumping `cow_seq` — no arena swap needed.
   Dead COW nodes accumulate as the writer modifies shared subtrees.

2. **Arena A fills:** When A's `bytes_used()` exceeds a threshold, the writer
   triggers a **flip**:
   - The current RW root in A is published as A's final RO root.
   - Arena A is frozen — no further allocations or mutations.
   - Arena B (previously free) is activated as the new RW arena (cursor at 0).
   - The new RW tree starts **empty**. The writer begins building a fresh tree
     in B. Reads that miss in B fall through to the frozen RO tree in A, then
     to PsiTri's Tri layer.
   - The merge thread begins draining A's frozen tree into PsiTri.

3. **Temporary three-layer reads:** During the merge window, the read path is:
   RW (Arena B) → RO (Arena A) → Tri. This is the same layered fallthrough as
   the current DWAL design. Once the merge completes and Arena A is freed, reads
   return to two layers: RW (Arena B) → Tri.

4. **Merge completes + readers release:** Once A's tree is fully merged into
   PsiTri and no reader holds a snapshot rooted in A, Arena A is `clear()`'d
   (cursor reset to 0, all memory reclaimed in bulk) and becomes the free arena,
   ready to swap into the RW role when Arena B fills.

### Why Two Arenas Are Sufficient

At most one arena can be in the RO/merging state at a time. The merge must
complete (and readers must release) before the next flip. If the RW arena fills
before the previous merge finishes, the writer must either:
- **Back-pressure:** Block until the merge completes and the RO arena is freed.
- **Grow the RW arena:** Allow the current RW arena to expand beyond its soft
  threshold (trading insert speed for availability).

In practice, the merge rate should keep up with the write rate — the Tri layer
absorbs data in bulk sequential writes, which are much faster than the random
ART insertions that filled the arena.

### Reader Epoch Tracking

Each reader records the `cow_seq` at which it took its snapshot. An arena can
only transition from pending-free to free when no reader holds a `cow_seq` from
before the merge completed.

```cpp
struct reader_slot
{
   std::atomic<uint32_t> held_cow_seq;  // cow_seq at snapshot time, or 0 if idle
};

// Fixed array — one slot per concurrent reader (e.g., 64 max)
std::array<reader_slot, 64> reader_slots;
```

The writer (or merge thread) scans `reader_slots` to find the minimum held
`cow_seq`. If `min_held > flip_cow_seq` (the `cow_seq` at which the arena was
frozen), all readers have moved past that generation and the arena is safe to
reclaim.

## Writer COW Strategies (Optimistic vs Pessimistic)

The writer can operate in two modes. The choice can be made per-root or
dynamically based on workload:

### Strategy A: Optimistic (Undo Log)

The writer mutates nodes **in place** and maintains an undo log. If a reader
arrives mid-transaction:

1. Reader sets `reader_waiting = 1`.
2. Writer replays the undo log to restore the pre-transaction state.
3. Writer switches to COW mode for the remainder of the transaction.
4. On commit, the pre-transaction root (restored via undo) becomes `last_root`.

**Undo log contents:** For each in-place mutation, the log records
`{node_offset, byte_offset_within_node, old_bytes, length}`. This is compact
for typical mutations (inserting a key into a setlist touches ~20 bytes).

**Best for:** Write-heavy workloads with infrequent readers. Most transactions
complete without any reader arriving, so the undo log is never replayed and COW
is never triggered.

### Strategy B: Pessimistic (Eager COW)

The writer COWs every node it touches, unconditionally. No undo log is needed
because the original nodes are never modified.

**Best for:** Read-heavy workloads where readers frequently overlap with writers.
The COW cost is paid once per node per transaction regardless of reader timing.

### Hybrid Default

Start optimistic. If `reader_waiting` is observed during a transaction, switch
to COW for the remainder of that transaction and bump `cow_seq` on commit. Track
the reader-arrival rate; if readers consistently arrive during write windows,
switch to pessimistic mode to avoid undo log overhead.

## Comparison with Current DWAL Architecture

### Read Level Summary

| Mode | Current: Reader cost | Current: Writer impact | COWART: Reader cost | COWART: Writer impact |
|------|---------------------|----------------------|--------------------|-----------------------|
| **Buffered** | shared_ptr copy under mutex; 2-way merge (RO + Tri) | None | Atomic load; 2-way merge (frozen RO + Tri) | None |
| **Fresh** | Wait for swap or merge + current tx; 2-way merge | Swap churn (smaller batches to Tri) | Atomic load, no wait; 3-way merge (last snapshot + RO + Tri) | None |
| **Latest** | Hold `rw_mutex` for read duration; 3-way merge (RW + RO + Tri) | **Blocks next write tx; scales with reader count** | Wait ≤1 tx; 3-way merge (snapshot + RO + Tri) | **Fixed COW cost; does not scale with reader count** |

### Structural Comparison

| Aspect | Current (RW/RO split) | COWART |
|--------|----------------------|--------|
| Tree structure | Two independent ARTs, separate arenas | Two arenas cycling; RW has COW snapshots |
| Snapshot mechanism | Freeze entire RW, allocate new RW | Store `last_root`, bump `cow_seq` (near-free) |
| COW trigger | Never (RW is writer-private, swap copies everything) | Reactive: only when reader signals or freshness timer expires |
| COW cost | N/A (full tree frozen on swap) | O(depth) node copies per modified key, only when `cow_seq` bumped |
| Writer stall from readers | Yes — latest holds `rw_mutex`, blocks next tx | **Never** — COW is a fixed cost, not a lock |
| Writer stall scaling | Proportional to reader count × read duration | Constant — one `cow_seq` bump regardless of reader count |
| Fresh reader wait | Wait for swap threshold or merge completion | No wait — reads whatever snapshot exists |
| Fresh writer impact | Forces smaller swap batches → more merge churn | None |
| Arena reclamation | Free RO arena after merge + reader release | Same — free frozen arena after merge + reader release |
| Freshness timer cost | Full RW→RO swap (alloc new arena, freeze old) | Snapshot publication only (store root, bump `cow_seq`) |

## Open Questions

1. **Undo log storage:** Should the undo log live in the RW arena (simple, but
   increases arena pressure) or a separate small buffer (more complex, but
   doesn't pollute the node arena)?

2. **Reader slot sizing:** Fixed array of 64 slots is simple but limits
   concurrency. Should this be dynamically sized, or is 64 sufficient?

3. **Back-pressure vs grow:** When the RW arena fills but the previous merge
   hasn't finished, should the writer block (back-pressure) or allow the RW
   arena to grow beyond its soft threshold? Growing trades insert speed for
   availability; blocking trades latency for bounded memory.

4. **Arena capacity sizing:** What should the soft threshold be for triggering a
   flip? Smaller arenas flip more often (more merges, but the RW tree stays
   small and fast). Larger arenas amortize merge overhead but accumulate more
   COW debris and slow down inserts as the tree grows.
