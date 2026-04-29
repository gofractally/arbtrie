# DWAL: Buffered Write Mode

PsiTri offers two write modes: **direct** (COW mutations applied immediately)
and **DWAL** (writes buffered in an in-memory ART trie, drained to COW in
background batches). DWAL is the recommended mode for most workloads and is
used by all the drop-in compatibility layers (RocksDB, MDBX, SQLite, DuckDB).

## When to Use Each Mode

DWAL is the default choice for most workloads.  It is never worse than
direct mode for reads (empty buffer layers are skipped with a single
branch) and always better for write latency.  Direct mode is an
optimization for specific access patterns.

```
    Workload                     Best Mode     Why
    ─────────────────────────    ───────────   ────────────────────────────
    Many small commits           DWAL          ART buffer amortizes COW
    (blockchain, OLTP, ledger)                 cost.  ~100 ns commit
                                               vs ~2 KB of COW clones.

    Bursty writes                DWAL          Burst absorbed by ART
    (event ingestion, logging,                 buffer at memory speed.
    network message processing)                Merge thread drains to
                                               COW trie in background
                                               when pressure drops.

    Low-frequency writes that    DWAL          Even at 100 writes/sec,
    need fast acknowledgement                  sub-microsecond commit lets
                                               you ack the caller before
                                               COW work happens.

    Idle / read-only periods     DWAL (free)   When RW and RO layers are
                                               empty, the merge cursor
                                               degenerates to a single Tri
                                               cursor.  Same speed as
                                               direct PsiTri.

    Few large batch writes       Direct        COW cost is amortized
    (bulk load, ETL, migration)                across the batch.  No ART
                                               buffer or WAL overhead.

    Many readers wanting the     Direct        COW snapshots are O(1).
    freshest data under heavy                  Every reader gets an instant
    writes                                     snapshot of the committed
                                               root.  No RW/RO/Tri
                                               indirection.
```

**DWAL mode** is the recommended starting point.  The ART buffer
absorbs bursts with sub-microsecond commits, and the background merge
thread amortizes COW cost across thousands of mutations.  When the
database is idle, the empty buffer layers are free -- reads go straight
to the COW trie with a single branch check per layer.

**Direct mode** is an optimization for two cases: (1) large batch
transactions where the COW cost is already amortized by batch size and
the DWAL's per-op WAL append is pure overhead, and (2) read-dominated
workloads with many concurrent readers that all need the absolute
freshest committed state, where the DWAL's three-layer merge cursor
adds unnecessary indirection compared to a direct COW snapshot.

Both modes are part of the same database -- you can use direct mode for
bulk operations and switch to DWAL for steady-state traffic.  The
`transaction_mode` enum selects between them:

```cpp
// DWAL mode (default) -- buffered, fast commits
auto tx = dwal_db.start_write_transaction(root_index);
tx.upsert(key, value);
tx.commit();  // ~100 ns, buffered

// Direct mode -- large batch, immediate COW
auto tx = dwal_db.start_write_transaction(root_index, transaction_mode::direct);
// Flushes DWAL buffer first, then writes directly to COW trie
for (auto& [k, v] : bulk_data)
    tx.upsert(k, v);
tx.commit();  // COW commit, immediate visibility
```

## Why DWAL Mode Is Fast

PsiTri's COW trie is persistent, crash-safe, and supports instant snapshots --
but every mutation clones nodes along the root path (~2 KB per write). At high
write rates, this COW cost becomes the bottleneck. The DWAL solves this by
**decoupling write latency from COW cost**:

1. **The writer never touches the COW trie.** Writes go to a writer-private
   ART map -- a lock-free, in-memory radix trie with a bump-allocated arena.
   Commit is a buffered append to the WAL file. No I/O, no locks, no COW
   cloning on the hot path. A single upsert costs ~100-200 ns.

2. **Background merge amortizes COW cost.** When the ART buffer fills
   (configurable, default 100K entries), the writer swaps it to a frozen
   read-only slot and allocates a fresh buffer. A background merge thread
   drains the frozen buffer into the COW trie in a single batch transaction.
   Batching amortizes per-key COW overhead: instead of cloning the root path
   for each of 100K keys, the merge walks the trie once and produces one new
   root.

3. **Readers never block writers.** The ART buffer is writer-private --
   external readers never touch it. Readers see committed data through the
   frozen RO snapshot (one ART map behind) or the COW trie (one merge behind).
   No shared mutex on the write path means zero contention under concurrent
   read load.

### Why the Gap with RocksDB Widens at Scale

In the [random upsert benchmark](../benchmarks/random-upsert.md), PsiTri
starts 1.37x faster than RocksDB and finishes **2.33x faster** after 200M keys.
The reason is architectural:

- **RocksDB's LSM tree** defers write cost into background compaction. As the
  dataset grows, compaction must merge increasingly large SST files. Beyond
  ~100M keys, compaction falls behind write throughput and creates sustained
  stalls (85-400K ops/sec vs 1.1M ops/sec steady-state).

- **PsiTri's DWAL** defers write cost into background merge, but the merge
  target is a COW trie with O(log n) depth, not a leveled file hierarchy.
  Merge cost grows logarithmically with dataset size, not linearly. The ART
  buffer absorbs burst writes during merge stalls, so the writer thread
  maintains consistent throughput even as the trie grows.

- **MDBX's page-level COW** has no write buffer at all. Every commit copies
  4KB pages at every level of the B-tree, causing 10x space amplification on
  random workloads. MDBX hit MAP_FULL after only 20M keys in the benchmark.

### The ART Buffer: Why Not a std::map or Skip List?

The write buffer is an [adaptive radix trie](https://db.in.tum.de/~leis/papers/ART.pdf)
(`art::art_map`), not a `std::map` or skip list. This matters for three reasons:

1. **Cache efficiency.** ART nodes are 4-256 bytes depending on fan-out,
   compared to 40+ bytes per node in a red-black tree. The ART's
   path-compressed keys eliminate redundant comparisons. Lookups visit
   fewer cache lines.

2. **Arena allocation.** Keys and trie nodes live in a bump-allocated arena
   with 32-bit offset addressing. No per-entry `malloc`/`free`. The entire
   buffer is freed as a unit when the layer is discarded after merge --
   zero fragmentation, zero deallocation cost.

3. **Sorted iteration for merge.** The merge thread iterates the ART in
   sorted order to drain into the COW trie. ART provides sorted iteration
   natively (it's a trie -- keys emerge in lexicographic order). No sort
   step, no intermediate data structure.

| Buffer | Insert | Sorted Iter | Memory | Dealloc |
|--------|--------|-------------|--------|---------|
| `std::map` | O(log n), ~40 B/node, malloc per entry | O(n) | Fragmented heap | Per-node free |
| Skip list | O(log n), ~40 B/node, malloc per entry | O(n) | Fragmented heap | Per-node free |
| **ART** | **O(k), 4-256 B/node, arena bump** | **O(n)** | **Contiguous arena** | **Free entire arena** |

Where k = key length. ART insert is O(key length), not O(log n).

## Three-Layer Architecture

Reads and writes flow through three layers:

```
┌─────────────────────────────────────┐
│  Read-Write ART map (hot head)      │  ← active writes land here
│  art::art_map, writer-private       │
├─────────────────────────────────────┤
│  Read-Only ART map (frozen)         │  ← background thread merges to PsiTri
│  art::art_map, immutable            │     no contention with writers
├─────────────────────────────────────┤
│  PsiTri COW tree   (trie)           │  ← on-disk, segment-allocated
│  concurrent readers via sessions    │
└─────────────────────────────────────┘
```

| Layer | Mutability | Who writes | Who reads | Synchronization |
|-------|-----------|------------|-----------|-----------------|
| **RW map** | read-write | single writer thread | writer thread only | none (writer-private) |
| **RO map** | immutable | nobody (frozen) | merge thread, readers | `shared_mutex` on `buffered_ptr` (copy shared_ptr, then release) |
| **PsiTri tree** | COW | merge thread | everyone | none (COW isolation) |

The RW map is **writer-private** — external readers never access it. This
eliminates the need for a shared_mutex on the hot write path. Readers see
committed data through the frozen RO map (up to one swap behind) or
PsiTri (up to one merge behind). The `buffered_mutex` is only held
briefly to copy the `shared_ptr<btree_layer>`, not during iteration.

### Memory Management: Dual Arena Design

The ART map and bump allocator pool work together to provide cache-friendly,
zero-fragmentation storage:

- **ART arena** — keys are stored in the ART's internal arena (cacheline-aligned
  bump allocator with 32-bit offset addressing). Key bytes live alongside trie
  nodes for spatial locality during lookups.
- **PMR pool** — value data (`string_view` payloads) is copied into a
  `std::pmr::monotonic_buffer_resource` for stable pointers. The pool is freed
  as a unit when the layer is discarded.

There is no per-entry deallocation in either allocator. Both are bump allocators
freed as a unit when the btree layer is discarded.

```cpp
/// Compare string_views using unsigned byte ordering to match PsiTri's
/// UCC (unsigned character comparison). std::string_view::operator<
/// uses signed char on most platforms, producing wrong sort order for
/// bytes > 127.
struct ucc_less
{
   bool operator()(std::string_view a, std::string_view b) const noexcept
   {
      return ucc::compare(a, b) < 0;
   }
};

/// Btree values can be live data, a subtree reference, or a tombstone.
/// Tombstones shadow lower layers — a key with a tombstone value is
/// treated as deleted even if it exists in the RO map or PsiTri.
struct btree_value
{
   enum class kind : uint8_t { data, subtree, tombstone };

   kind             type = kind::data;
   std::string_view data;              // pool-backed (kind::data only)
   sal::ptr_address subtree_root = {}; // PsiTri subtree address (kind::subtree only)
};

struct btree_layer
{
   using map_type = art::art_map<btree_value>;

   std::pmr::monotonic_buffer_resource pool;
   map_type                            map;
   range_tombstone_list                tombstones;
   uint32_t                            generation = 0;

   /// Copy value data into the pool, insert key/value into ART map.
   void store_data(std::string_view key, std::string_view value)
   {
      auto pool_val = store_string(value);
      map.upsert(key, btree_value::make_data(pool_val));
   }

   /// Insert a tombstone for a key.
   void store_tombstone(std::string_view key)
   {
      map.upsert(key, btree_value::make_tombstone());
   }
};
```

Keys are stored in the ART map's internal arena. Value data points into
the PMR pool. On swap, the entire layer is frozen and handed to the merge
thread. On release, both the ART arena and `pool.release()` free memory
in one pass. Subtree refs require a walk — see "Subtree Reference
Counting" below.

```
┌──────────────────────────────────────┐
│  btree_layer                         │
│                                      │
│  art::art_map<btree_value>           │── keys in ART arena         │
│  range_tombstone_list                │── values point into ──┐     │
│  pmr::monotonic_buffer_resource pool │◄───────────────────────┘
│  uint32_t generation                 │   pool.release() frees all
└──────────────────────────────────────┘   memory — no per-entry cost
```

#### Per-Root RO Slot

Each root has exactly **one RO slot**. The lifecycle is:

```
free → merging → draining → free
         │          │
         │          └── merged, waiting for readers to release (epoch-based)
         └── merge thread draining to PsiTri
```

```cpp
struct ro_slot
{
   btree_layer*  layer      = nullptr;  // null = free
   uint32_t      generation = 0;        // assigned at swap time
   enum state_t { free, merging, draining } state = free;
};
```

On swap, the writer moves the RW map_layer into the root's RO slot
(must be free — writer blocks otherwise). On merge completion, the slot
transitions to `draining` until all readers release the generation, then
the layer is destroyed (subtree refs released, pool freed) and the slot
returns to `free`.

#### Subtree Reference Counting

When the app upserts a subtree, `smart_ptr::take()` consumes one ref.
That ref is logically held by whichever structure stores the address.
Transfer = save the address elsewhere before discarding.

Subtree ref counting follows the same pattern as direct COW: one ref,
one owner at a time, transferred (not copied) at each stage.

```
app.take() → ART map → merge transfers to PsiTri → PsiTri owns it
```

The merge thread creates `smart_ptr(session, addr, inc_ref=false)` —
claiming the existing ref, not adding one — and passes it to PsiTri's
`upsert`, which takes it via `.take()`. No `inc_ref` at any point.

After merge, readers who encounter subtrees in the RO map are
safe because their read transaction holds a `smart_ptr` to the PsiTri
root. PsiTri's COW semantics keep the old root (and transitively all
its subtrees) alive via the reader's snapshot ref.

**Release responsibilities:**

| When | What to release |
|------|----------------|
| **Abort** (undo replay) | New subtree displaced from ART map |
| **Commit** (undo discard) | Old subtree saved in undo entry |
| **RW layer destruction** (unmerged, e.g. shutdown) | Walk ART map, release `kind::subtree` entries |
| **RO layer destruction** (fully merged) | Nothing — all refs were transferred to PsiTri |

**Example** — upsert subtree_B overwriting subtree_A in ART map:
```
upsert:  map[key] = B, save A in undo             [map:B] [undo:A]
abort:   restore A to map, B displaced             → release(B)
commit:  undo discarded                           → release(A)
merge:   transfer B to PsiTri (no inc_ref)        [psitri:B]
destroy: nothing to release (RO, fully merged)    [psitri:B] ✓
```

**COW-sourced subtrees** (key exists only in PsiTri): no ref management
in the DWAL layer. PsiTri owns the ref. On overwrite, the ART map
shadows it; on abort, the map entry is erased and reads fall
through to PsiTri. On merge, PsiTri's COW handles releasing the old
subtree when the new value replaces it.

### Large Transaction Fallback

The RW map has a size cap (pool memory or key count) that determines
when swaps occur. This cap also imposes a **maximum buffered transaction
size**. A transaction that exceeds the cap cannot fit in the RW map.

Large transactions **fall back to direct COW writes** against PsiTri:

1. **Before the transaction begins**, the writer flushes: triggers a
   swap of the current RW map (if non-empty), then **blocks until
   the merge completes** — the RO slot must be free and PsiTri must
   be fully up to date. Everything prior to this transaction is now
   in the COW tree. The flush cannot happen mid-transaction — that
   would commit partial work.
2. The transaction proceeds as a direct PsiTri `write_cursor` operation.
   No btree buffering, no WAL — the COW tree provides its own atomicity
   and durability. The undo log is replaced by PsiTri's native
   sub-transaction support.
3. On commit, the PsiTri root is updated directly. On abort, the
   write_cursor is discarded (COW — original tree unchanged).

```cpp
enum class transaction_mode { buffered, direct };

auto tx = dwal.start_write_transaction(transaction_mode::direct);
// Flushes RW map, waits for merge, then writes directly to PsiTri.
// No size limit — bounded only by disk space.
```

The writer thread is single-threaded per root, so the RW map is
quiescent during a direct transaction. After the direct transaction
commits, normal buffered writes resume.

This means the DWAL is an optimization for the common case (many small
transactions), not a constraint. Bulk loads, migrations, and large
batch operations use `direct` mode and get PsiTri's full COW semantics
with no artificial size limit.

### Concurrency: Writer-Private RW, Lock-Free Reads

The RW map is **writer-private** — only the single writer thread accesses
it. There is no shared_mutex on the RW map. This eliminates lock
contention on the hot write path entirely.

Writers on independent roots never contend — they touch completely
independent data structures (ART map, pool, WAL file, RO slot).

- **Write transactions** are single-writer-per-root. The writer thread
  owns the RW map exclusively. Multiple writers to the *same root*
  must serialize externally (one `dwal_transaction` at a time per root).
  Writers to *different roots* run fully in parallel.
- **Readers** never access the RW map. They read from the frozen RO
  map (via `buffered_mutex`, held only to copy a `shared_ptr`) and/or
  PsiTri (via COW sessions). No reader ever blocks a writer.
- **The `create_cursor` API** encapsulates all locking. The caller passes
  a `read_mode` and receives an `owned_merge_cursor` that holds shared_ptr
  copies of the relevant layers. No application code touches mutexes directly.

```cpp
// Writer: single-threaded access, no locks on RW map
auto tx = dwal.start_write_transaction(root_index);
tx.upsert(key, value);   // writes to RW map + WAL
auto result = tx.get(key); // reads RW → RO → Tri
tx.commit();

// Reader: create_cursor handles all locking internally
auto mc = dwal.create_cursor(root_index, read_mode::buffered);
mc->seek_begin();
while (!mc->is_end()) {
   // iterate over RO + Tri layers
   mc->next();
}
```

### Merge Cursor

The DWAL exposes a **merge cursor** that presents a unified sorted view
across all three layers. It mirrors the PsiTri `cursor` API: positioned
iteration, seeking, counting — but merges results from the RW map,
RO map, and PsiTri cursor, filtering through tombstones and range
tombstones.

```cpp
class merge_cursor
{
   // Three source iterators, each positioned at their current key
   using btree_iter = btree_layer::iterator;  // art::art_map iterator

   const btree_layer* _rw;                    // RW map (null if excluded)
   const btree_layer* _ro;                    // RO map (null if excluded)
   btree_iter         _rw_it, _rw_end;        // RW map iterators
   btree_iter         _ro_it, _ro_end;        // RO map iterators
   std::optional<psitri::cursor> _tri;        // PsiTri COW cursor

   enum class source { rw, ro, tri, none };
   source _source;

public:
   // -- Positioning --
   bool seek_begin();                         // first live key
   bool seek_last();                          // last live key
   bool lower_bound(std::string_view key);    // first key >= key
   bool upper_bound(std::string_view key);    // first key > key
   bool seek(std::string_view key);           // exact match
   bool find(std::string_view key);           // exact match, end on miss

   // -- Navigation --
   bool next();
   bool prev();

   // -- Access --
   bool             is_end() const;
   bool             is_rend() const;
   std::string_view key() const;              // current key
   const btree_value& current_value() const;  // RW/RO layer value
   source           current_source() const;
   bool             is_subtree() const;
   psitri::cursor*  tri_cursor();             // for Tri-layer value reads

   // -- Counting --
   uint64_t count_keys(std::string_view lower = {}, std::string_view upper = {});
};

/// Owns layer snapshots + cursor. Returned by dwal_database::create_cursor().
/// Application code uses this instead of accessing dwal_root internals.
class owned_merge_cursor
{
   std::shared_ptr<btree_layer> _rw, _ro;
   merge_cursor                 _cursor;
public:
   merge_cursor*       operator->()       { return &_cursor; }
   const merge_cursor* operator->() const { return &_cursor; }
};
```

#### Merge Logic

The cursor maintains a position in each of the three sources. At any
point, the "current" key is the **smallest** key across all sources that
is not tombstoned.

```
advance():
   candidates = {rw_it.key, ro_it.key, tri_cursor.key}  // skip exhausted sources

   winner = min(candidates)

   // Tombstone filtering: higher layers shadow lower layers
   if winner is from tri_cursor:
      if ro_tombstones.is_deleted(winner) OR rw_tombstones.is_deleted(winner):
         skip, advance tri_cursor, retry
   if winner is from ro:
      if rw_tombstones.is_deleted(winner):
         skip, advance ro_it, retry

   // Deduplication: if multiple sources have the same key,
   // take the highest layer (RW > RO > PsiTri), advance the others
   if rw_it.key == winner: take RW value, advance ro_it and tri_cursor past winner
   elif ro_it.key == winner: take RO value, advance tri_cursor past winner
   else: take PsiTri value

   _current_source = winner's source
```

**`lower_bound(key)`:** Position each source at its own `lower_bound(key)`,
then run the merge logic to find the first live key.

**`prev()`:** Same three-way merge but tracking the largest key less than
current. Each source is positioned via its own `prev()` / reverse iterator.

**`count_keys(lower, upper)`:** Cannot simply sum counts from the three
sources due to overlap and tombstones. Two strategies:

1. **Small ranges:** iterate with `next()` and count. Exact.
2. **Large ranges:** `tri_cursor.count_keys(lower, upper)` gives the base
   count. Adjust by scanning the btree layers for insertions (add) and
   tombstones (subtract) in the range. Approximate for range tombstones
   that partially overlap the query range — fall back to iteration if
   precision is required.

### Swap and Merge Lifecycle

The **writer** decides when to swap — it pushes work to the merge stage:

1. Writes go to the **RW ART map** (the hot head), backed by the WAL for
   durability. Keys are stored in the ART arena, value data in the PMR pool.
   The writer thread owns the RW map exclusively (no lock needed).

2. A swap is triggered by either condition:
   - **Map size:** the writer determines the RW map is full (entry count)
     — checked inline during commit.
   - **WAL size:** the WAL file exceeds a threshold. Handles the case
     where the map stays small (repeated overwrites) but the WAL grows
     large (every overwrite is a new entry). Checked at commit time.
   - **Time-based:** a background timer detects an RW map that hasn't
     been touched for longer than `idle_flush_interval`. This ensures
     idle roots drain to PsiTri promptly — without it, a burst of
     writes followed by silence would leave data stranded in the RW
     map, invisible to `trie`-mode readers.

   The swap procedure (writer thread only):
   - Check that this root's RO slot is free (previous merge completed
     and readers drained). If not, the writer **blocks** until the
     merge finishes. This is the correct backpressure: the WAL exists
     to batch COW writes and amortize their cost — it cannot sustain
     throughput beyond what PsiTri can absorb. Buffering more (multiple
     RO slots, unbounded RW growth) just defers the stall while
     consuming the same memory and adding read cursor complexity.
   - Note the current PsiTri root — this is the **base root** of the new
     RO map. All keys in the RO map are relative to this snapshot.
   - The RW map (shared_ptr) is moved to `buffered_ptr` under an exclusive
     `buffered_mutex` lock — this is the only write to `buffered_ptr`.
   - A fresh empty ART map becomes the new RW map (allocated outside the lock).
   - The WAL is rotated — old WAL file covers the now-frozen RO map.
   - Increment the generation counter (atomic store, release).
   - Signal the merge pool (condition variable).

3. A merge pool thread wakes, picks up the root, and drains the RO
   btree into PsiTri. When complete, it publishes a **new PsiTri root**
   via atomic store (release).

4. The RO map pool **cannot be freed yet** — readers may still hold
   pointers into it. The pool waits for all readers to release
   (epoch-based reclamation).

5. Once all readers have released, the pool is freed and the slot is recycled.

### Merge Thread Pool

Merging RO maps into PsiTri is the throughput bottleneck — every
write ultimately flows through this path. A single merge thread would
serialize all roots, capping global write throughput regardless of how
many roots are active.

Instead, a **bounded thread pool** drains RO maps. Each drain runs
one root to completion on one thread (no interleaving — cache-friendly).
Multiple roots drain concurrently on different threads.

```
Pool size = min(configured_merge_threads, available_sal_sessions)
```

```
merge_pool_loop(thread):
   ws = database.start_write_session()     // one write session per pool thread
                                           // sessions are root-independent

   while running:
      root = wait_for_signal()             // block until a root needs draining
      tx = ws.start_transaction(root)      // transaction is root-specific
      drain_ro_btree(root, tx)             // upsert/remove all RO entries
      tx.commit()                          // atomic PsiTri root update
```

| Constraint | Impact on pool size |
|------------|-------------------|
| Write sessions | one per pool thread (sessions are root-independent, reused across roots) |
| CPU cores | diminishing returns past core count |
| Active roots | no benefit from more threads than active roots |
| Compactor thread | shares the session budget |

**Typical sizing:** 2–4 merge threads. Most workloads have a handful of
hot roots. The pool scales global merge throughput linearly with thread
count while keeping per-root drains sequential and cache-friendly.

**No interleaving within a root:** Each drain runs to completion. The
PsiTri root swap happens once at the end — partial drains don't produce
a publishable result, and switching mid-drain thrashes cache.

**Writer-driven push:** The writer decides when the RW map is full
(by entry count or WAL size) and performs the swap. It moves the RW map
to `buffered_ptr` (brief exclusive lock on `buffered_mutex`), allocates a
fresh RW map, and signals the merge pool. A pool thread wakes, picks up
the root, and drains it.

The merge pool threads don't scan or poll; they block on a condition
variable and are woken by the writer's signal.

**No deconfliction needed between pool threads.** Each root is an
independent PsiTri tree with its own root pointer. Two threads draining
different roots operate on completely separate COW trees — different
root pointers, different COW paths. They share only the SAL allocator,
which is already designed for concurrent sessions (per-session
allocation, lock-free release queues). Each root has at most one
pending RO map, so at most one merge thread works on a given root.

### RO Btree Staleness Detection

Each RO map carries the **PsiTri root it was built from** (the base root).
Readers use this to detect whether the merge has completed:

```cpp
// Per-root state in dwal_root:
std::shared_ptr<btree_layer> buffered_ptr;     // frozen RO layer (or null)
std::atomic<uint32_t>        ro_base_root;     // PsiTri root at swap time
std::atomic<uint32_t>        generation;       // epoch for pool reclamation
```

#### The Ordering Problem

Two independent threads write to two atomic variables:

- **Merge thread** stores `tri_root` (when merge completes)
- **Writer thread** stores `ro_ptr` (when swap occurs)

A reader must load both and compare. A **false skip** (concluding the
RO map is merged when it hasn't been) causes data loss. A **false
include** (redundantly reading a merged RO map) is harmless — the
merge cursor deduplicates.

#### Why Load Order Matters

**Wrong order — `tri_root` first, then `ro_ptr`:**

```
Reader loads stale tri_root = R1
                                    Merge completes: tri_root ← R2
                                    Writer swaps: new_RO.base_root = R2, publishes ro_ptr
Reader loads fresh ro_ptr → new_RO (base_root = R2)
Reader: R2 != R1 → skips RO ← WRONG, this RO was just created, not merged
```

**Correct order — `ro_ptr` first, then `tri_root`:**

The writer's store sequence creates a happens-before chain:

```
Merge thread:  tri_root.store(R2, release)           ─┐
                                                       │ syncs-with
Writer thread: tri_root.load(acquire) → R2            ─┘
               new_RO.base_root = R2
               ro_ptr.store(new_RO, release)          ─┐
                                                       │ syncs-with
Reader:        ro_ptr.load(acquire) → new_RO          ─┘
               tri_root.load(acquire) → guaranteed ≥ R2
```

By transitivity: the merge thread's store of R2 happens-before the
reader's load of `tri_root`. The reader is guaranteed to see
`tri_root >= base_root`. Therefore:

- `base_root == tri_root` → RO not yet merged → **include** ✓
- `base_root != tri_root` → RO has been merged → **skip** ✓

No lock, no CAS — just ordered atomic loads.

#### Protocol

```
// === Writer thread (swap) ===
writer_swap():
   snapshot = tri_root.load(acquire)       // (1) syncs with merge thread
   new_ro = freeze current RW map
   new_ro.base_root = snapshot
   ro_ptr.store(new_ro, release)           // (2) carries chain to readers
   allocate fresh RW map

// === Merge thread (merge complete) ===
merge_complete():
   tri_root.store(new_root, release)       // (0) origin of the chain

// === Reader (any thread) ===
start_read_transaction():
   ro   = ro_ptr.load(acquire)             // (3) syncs with writer's (2)
   root = smart_ptr(tri_root.load(acquire), inc_ref=true)
                                           // (4) guaranteed to see ≥ writer's (1)
                                           // smart_ptr keeps PsiTri snapshot alive —
                                           // transitively protects all subtrees

   if ro == null OR ro.base_root != root.address():
      ro = null                            // skip — already merged

   return read_transaction{ro, std::move(root)}
```

This is the **common steady-state path**: the merge finishes, readers see
`base_root != root`, and skip the RO map. They read PsiTri directly
with zero overhead from the DWAL layer.

The RO map is only consulted during the window between a swap and the
completion of the subsequent merge — a transient state under write pressure.

### Range Tombstones

Range deletes cannot be efficiently represented as per-key tombstones in the
ART map (the range may cover millions of keys in PsiTri). Instead, each
layer maintains a **separate sorted list of deleted ranges**.

```cpp
/// Non-overlapping, sorted by low key. Gaps between ranges are live.
struct range_tombstone_list
{
   struct range { std::string low; std::string high; };  // [low, high)
   std::vector<range> ranges;  // sorted, non-overlapping, merged on insert

   /// O(log R) — binary search for the range containing key.
   bool is_deleted(std::string_view key) const;

   /// Add a range deletion. Merges with adjacent/overlapping ranges.
   void add(std::string low, std::string high);

   /// Split a range when a key is inserted within it.
   /// [A, Z) + insert("M") → [A, M) + [M+1, Z)
   void split_at(std::string_view key);
};
```

Each btree layer (RW and RO) has its own `range_tombstone_list`. The list is
small (range deletes are rare) and non-overlapping (ranges are merged on
insert), so binary search is fast.

**On upsert within a deleted range:** the range is split around the inserted
key. The key becomes live in the ART map, and the two remaining sub-ranges
stay as tombstones.

**On merge to PsiTri:** range tombstones are applied directly as
`remove_range` calls on the COW tree. After merge, the range tombstone list
is discarded with the RO map.

### Read Path

#### Point Lookups (fast path)

Point lookups (`get`) do **not** use the merge cursor. They are a direct
layered find — check each layer in priority order, short-circuit on
first hit or tombstone:

```
get(key):  // writer thread: get_latest() reads all 3 layers
   // Layer 1: RW map (writer-private, no lock)
   if auto* v = rw_map.get(key):
      if v->is_tombstone: return not_found
      return v->value
   if rw_tombstones.is_deleted(key): return not_found

   // Layer 2: RO map (copy shared_ptr under buffered_mutex, then release)
   if ro:
      if auto* v = ro->map.get(key):
         if v->is_tombstone: return not_found
         return v->value
      if ro->tombstones.is_deleted(key): return not_found

   // Layer 3: PsiTri
   return tri_cursor.get(key)
```

In the common case (RW map small/empty, RO null), this is one ART lookup
miss + one PsiTri lookup. No iterator state, no merge overhead.

#### Range Scans (merge cursor)

Range operations (`lower_bound`, `next`, `prev`, `count_keys`) use
the **merge cursor** which maintains positioned iterators across all
active layers. See "Merge Cursor" section below.

The merge cursor checks layers in priority order: RW map > RO map
> PsiTri. Tombstones and range tombstones in higher layers shadow lower
layers.

#### Read Modes: Freshness vs Cost

The caller chooses a read mode that trades freshness for cost.  All
modes see only committed data.  The difference is how recent that
committed data is and what cost the reader imposes.

```
    Mode       Layers          Reader waits for   Writer impact
    ─────────  ──────────────  ─────────────────  ──────────────────
    trie       Tri only        nothing            none
    buffered   RO + Tri        nothing            none
    fresh      RO + Tri        merge + swap       none (self-imposed)
    latest     RW + RO + Tri   current tx finish  blocks next tx start
```

**`trie`** — reads only the persistent COW trie.  Zero DWAL overhead.
Staleness bounded by the merge cycle (how long it takes the merge
thread to drain RO into Tri).  Best for read-heavy workloads that
tolerate seconds of lag.

**`buffered`** — reads the frozen RO snapshot plus Tri.  No writer
interaction.  Staleness bounded by `max_flush_delay` (configurable) or
the writer's natural commit-driven swap frequency.  Best for most
external readers.

**`fresh`** — reader signals that it wants the latest committed data by
setting an atomic flag (`readers_want_swap`), then waits on a condition
variable.  The writer (on its next commit) or the merge thread (if the
writer is idle) sees the flag, swaps RW→RO, and wakes all waiting
readers.  The reader then reads from the freshly frozen RO + Tri.
**The reader blocks only itself — the writer and other readers are
completely unaffected.**  Best for readers that need guaranteed-fresh
data but can tolerate a brief wait (up to one merge cycle + one
commit).

**`latest`** — reader acquires a shared lock on `rw_mutex`, blocking
until the writer's current transaction finishes.  Reads from the live
RW map + RO + Tri via a three-way merge cursor.  While the reader holds
the shared lock, the writer cannot start a new transaction (exclusive
lock blocked).  **This is the only mode that imposes cost on the
writer.**  Best for same-connection reads (writer reading its own
writes within a transaction) or when the reader needs the absolute
freshest view and accepts the writer impact.

```cpp
enum class read_mode { trie, buffered, fresh, latest };

// Reader thread — zero writer impact, brief self-imposed wait
auto mc = dwal.create_cursor(root_index, read_mode::fresh);

// Writer thread — reading own writes, no lock needed
auto mc = dwal.create_cursor(root_index, read_mode::latest, skip_rw_lock);
```

#### Swap Coordination

The RW→RO swap can be triggered by two actors:

- **Writer in `commit()`** — checks `should_swap()` (buffer full, WAL
  full, or `max_flush_delay` elapsed) and also checks
  `readers_want_swap`.  The writer already holds exclusive `rw_mutex`
  (if enabled) or is between mutations, so the swap has zero additional
  lock cost.  This is the preferred path.

- **Merge thread** — after completing an RO→Tri drain, checks
  `readers_want_swap`.  If set and the writer is idle (`try_lock`
  succeeds), the merge thread performs the swap.  If the writer is
  active (`try_lock` fails), the merge thread skips and lets the writer
  handle it on the next commit.

After a swap, `swap_cv.notify_all()` wakes all readers waiting in
`fresh` mode.  The freshly frozen RO snapshot is immediately available
to `buffered` and `fresh` readers.

### Why Three Layers

- The **RW map** is writer-private — no lock, no contention. The single
  writer thread reads and writes it freely. External readers never see it.
- The **RO map is immutable**. Readers copy the `shared_ptr` under a brief
  `shared_lock`, then iterate lock-free. The `buffered_mutex` protects only
  the pointer swap, not iteration.
- The **PsiTri tree** uses COW isolation — concurrent readers via sessions.
- The **merge thread** only touches the RO map (reading) and PsiTri
  (writing via COW). It never touches the RW map.

### Epoch-Based Pool Reclamation

The RO map pool must outlive all readers that hold pointers into it. PsiTri's
SAL layer already solves an analogous problem for segment reclamation using an
epoch-based read-lock mechanism. We adapt the same pattern for pool lifetime.

#### How SAL's Read-Lock Works

Each `allocator_session` has a `session_rlock` — a single padded 64-bit atomic
split into two 32-bit halves:

- **Low 32 bits (R):** the session's read position (set on lock, cleared on unlock)
- **High 32 bits (E):** the compactor's end pointer (broadcast to all sessions)

```
Lock:    copy E → R  (one atomic store, no CAS)
Unlock:  set R = 0xFFFFFFFF  (one atomic store)
Reclaim: safe when min(all R) > pool's generation
```

Readers pay exactly **one atomic store** to lock and **one** to unlock.
No CAS loops, no contention, no cache-line bouncing between readers.

#### Adapting for Pool Reclamation

Each RO map pool is assigned a monotonically increasing **generation number**
at swap time. The DWAL layer maintains a parallel set of per-session generation
atomics, identical in structure to SAL's `session_rlock`:

```cpp
struct dwal_session_lock
{
   // Low 32 bits:  generation the reader is currently observing
   // High 32 bits: latest generation (broadcast on each swap)
   ucc::padded_atomic<uint64_t> _gen_ptr{uint64_t(-1)};

   void lock()   { ucc::set_low_bits(_gen_ptr, _gen_ptr.load() >> 32); }
   void unlock() { ucc::set_low_bits(_gen_ptr, uint32_t(-1)); }
};
```

**On read start:** the reader copies the current generation to its low bits
(one relaxed store). This pins all pools with generation ≥ that value.

**On read end:** the reader sets its low bits to `0xFFFFFFFF` (one relaxed
store). It no longer pins anything.

**On pool free attempt:** the merge thread checks
`min(all session low bits) > pool_generation`. If true, no reader can
still be referencing that pool — safe to free. If false, defer and retry.

```
Session locks:    [S0: gen=7]  [S1: gen=9]  [S2: 0xFFFF]  [S3: 0xFFFF]

Pool generations: [4: freed] [5: freed] [6: freed] [7: merged, waiting]
                                                         ^^ can't free yet,
                                                            S0 still at gen 7

When S0 unlocks → min = 9 → pool 7 freed, pool 8 freed
```

#### Why Not Just Use an Atomic Refcount?

A per-pool `atomic<uint32_t>` refcount would work correctly but has a
critical performance flaw: every reader increments and decrements the
**same cache line** as every other reader. Under high read concurrency
this causes cache-line bouncing across cores.

The epoch approach gives each session its **own cache line** (padded atomic).
Readers never touch each other's memory. The only shared write is the
generation broadcast from the merge thread, which is infrequent (once per
swap, not once per read).

| Approach | Reader cost | Writer cost | Contention |
|----------|-------------|-------------|------------|
| Atomic refcount | 2 atomic RMW (inc + dec) per read | none | high (shared cache line) |
| Epoch generation | 2 atomic stores per read | 1 broadcast per swap | none between readers |

#### Integration with SAL Sessions

Since DWAL sessions already require an `allocator_session` for PsiTri access,
the `dwal_session_lock` can be embedded directly in the existing session
infrastructure. The same session creation/destruction lifecycle manages both
the SAL read-lock and the DWAL generation lock.

```cpp
class dwal_read_session
{
   std::shared_ptr<read_session>  _psitri_session;  // SAL session
   dwal_session_lock&             _gen_lock;          // pool generation lock

   auto lock()
   {
      _gen_lock.lock();       // pin current RO pool generation
      return _psitri_session; // SAL session handles its own locking
   }

   ~dwal_read_session() { _gen_lock.unlock(); }
};
```

## Internal Structures

| Structure | Purpose | Storage | Lifetime |
|-----------|---------|---------|----------|
| **WAL file** | Durability of *committed* transactions | Disk (append-only) | Until checkpoint (RO map fully merged) |
| **Undo log** | Rollback of *in-flight* transactions | Memory only | Single transaction |

These are completely independent. The WAL records what happened *after* commit.
The undo log records what to reverse *before* commit if the transaction aborts.

---

## WAL File Format

### File Layout

```
┌──────────────────────────────────┐
│  Header (64 bytes)               │
├──────────────────────────────────┤
│  Entry 1                         │
├──────────────────────────────────┤
│  Entry 2                         │
├──────────────────────────────────┤
│  ...                             │
├──────────────────────────────────┤
│  Checkpoint Record               │
└──────────────────────────────────┘
```

Each root has its own WAL file. WAL files are independent — they
fsync independently and can be rotated independently.

### Header (64 bytes)

```
Offset  Size  Field
──────  ────  ─────
0       4     magic: 0x44574C31 ("DWL1")
4       4     version: 1
8       8     sequence_base: first sequence number in this file
16      8     created_timestamp: nanoseconds since epoch (human debugging only)
24      2     root_index: which root this WAL belongs to
26      2     flags: (bit 0 = clean close)
28      36    reserved (zero-filled)
```

### Entry

Each entry is one committed transaction. Entries are append-only, written
sequentially, with the hash at the end so the write can stream without seeking.

```
Offset  Size       Field
──────  ────       ─────
0       4          entry_size: total bytes including trailing hash
4       8          sequence: monotonically increasing
12      2          op_count: number of operations
14      ...        operations[]
...     8          xxh3_64: covers bytes [0, entry_size - 8)
```

14 bytes of header + operations + 8 bytes trailing hash.

### Operations

```
op_type  0x01 = upsert (data)
op_type  0x02 = remove
op_type  0x03 = remove_range
op_type  0x04 = upsert_subtree
```

**upsert (data):**
```
0       1          op_type (0x01)
1       2          key_len
3       key_len    key_data
...     4          value_len
...     value_len  value_data
```

**remove:**
```
0       1          op_type (0x02)
1       2          key_len
3       key_len    key_data
```

**remove_range:**
```
0       1          op_type (0x03)
1       2          low_key_len
3       low_len    low_key_data
...     2          high_key_len
...     high_len   high_key_data
```

**upsert_subtree:**
```
0       1          op_type (0x04)
1       2          key_len
3       key_len    key_data
...     4          subtree_root (sal::ptr_address, uint32_t)
```

Subtree upserts store the PsiTri subtree root address directly.
On WAL replay, the address is used to reconstruct the btree_value
with `kind::subtree`. The ref is already persisted in SAL — no
`inc_ref` needed during replay. The merge thread transfers the ref
to PsiTri (no `inc_ref`) via `smart_ptr(session, addr, false)`.

### WAL Lifecycle

Each WAL file is tied to a **btree_layer**. The file is created when
the layer is created (fresh RW map after swap) and deleted when the
layer is discarded (merge complete + readers drained). At most **two
WAL files exist per root** at any time:

```
root-003/wal-rw.dwal    ← current RW map's WAL (being appended to)
root-003/wal-ro.dwal    ← RO map's WAL (frozen, needed for crash recovery)
```

**On swap:**
1. Close the current WAL file (it now covers the frozen RO map)
2. Rename it to `wal-ro.dwal`
3. Open a fresh `wal-rw.dwal` for the new RW map

**On merge complete + readers drained:**
1. Delete `wal-ro.dwal` — all its entries are now in PsiTri
2. The RO slot is freed

**File size** is bounded by the data written between swaps. Since swaps
are triggered by btree size, time, or WAL size, the file cannot grow
unboundedly:

| Swap trigger | Bounds |
|-------------|--------|
| **Btree size** | WAL ≈ btree data (keys overwritten multiple times are larger in WAL than btree) |
| **Time-based** (`idle_flush_interval`) | WAL bounded by write rate × interval |
| **WAL size** | Direct cap — if WAL exceeds threshold, trigger swap regardless of btree size |

The WAL size trigger handles the case where the btree stays small
(repeated overwrites of the same keys) but the WAL grows large
(every overwrite is a new WAL entry).

### Recovery

On startup, for each root:

1. If `wal-ro.dwal` exists: the previous RO map was not fully merged
   before crash. Replay it into an ART map, then drain to PsiTri
   before accepting new writes.
2. If `wal-rw.dwal` exists: replay valid entries into the RW ART map.
   Entry with bad XXH3 or truncated size = crash boundary, stop replay.
3. Resume normal operation.

```
recovery(root):
   if exists wal-ro.dwal:
      ro_map = replay(wal-ro.dwal)    // full replay, all entries valid
      drain ro_map into PsiTri        // complete the interrupted merge
      delete wal-ro.dwal

   if exists wal-rw.dwal:
      rw_map = replay(wal-rw.dwal)    // stop at first bad entry
      // rw_map becomes the live RW map

   // ready for new transactions
```

No checkpoint records are needed. The WAL file boundary (rw vs ro) is
the checkpoint — everything in `wal-ro.dwal` is pre-swap, everything
in `wal-rw.dwal` is post-swap.

### Durability Model

The WAL `write()` is a buffered append — no fsync per transaction.
The writer thread performs the ART map mutation + buffered WAL write
with no I/O. Since the RW map is writer-private (no lock), the entire
write path is lock-free, eliminating the group commit problem entirely.

**`fdatasync` is periodic and application-driven.** The app calls
`flush()` when it needs a durability boundary (e.g. every N seconds,
on checkpoint, before responding to a client that requires durability).
Each root's WAL fsyncs independently.

```
commit():
   mutate ART map           // writer-private, no lock needed
   append WAL entry         // buffered write, no I/O

flush():                    // app calls periodically, or never
   for each dirty root WAL:
      fdatasync()
```

**Crash semantics:** Unflushed WAL entries are lost on crash. The
ART map is rebuilt from the durable portion of the WAL. This is the
standard WAL tradeoff — applications that need per-transaction
durability call `flush()` after commit. Applications that tolerate
losing the last few seconds of writes (the common case) flush
periodically or not at all.

---

## Undo Log (In-Memory Only)

The undo log enables transaction rollback *before* the WAL entry is written.
As a transaction mutates the ART map, the undo log records how to reverse
each mutation. On abort, the undo log is replayed in reverse. On commit,
it is discarded.

### Why It Can Be Memory-Only

The undo log never needs disk persistence because:

- If the process crashes mid-transaction, the transaction was never committed
  to the WAL, so there is nothing to undo — the ART map is rebuilt from
  the WAL on recovery.
- The undo log exists only for explicit `abort()` calls during normal operation.

### Old Value Sources

When a transaction modifies a key, the old value is in one of two places:

1. **In the ART map** — a prior buffered write that hasn't drained yet.
   The old `btree_value` is saved in the undo entry (data values are
   copied into the undo arena since btree rebalancing invalidates pointers;
   subtree values just copy the `ptr_address`).
2. **In the PsiTri COW tree** — the key was never buffered. The undo entry
   needs **only the key** — on abort, erasing the map entry causes
   reads to fall through to PsiTri (or RO map + PsiTri), which still has
   the original value. No coordinates, no data copy, no read lock.

This works because the RO map + PsiTri together always represent the
pre-write-transaction state, even if the merge thread modifies PsiTri
concurrently (the staleness detection ensures readers see the correct
merged view).

### Arena-Backed Value Storage

Old values must be copied into an arena owned by the undo log — a bump
allocator that is freed as a unit when the undo log is discarded.

```cpp
/// Bump allocator for undo value storage.
/// Freed entirely on commit or after abort replay.
using undo_arena = std::pmr::monotonic_buffer_resource;

std::string_view arena_copy(undo_arena& arena, std::string_view src)
{
   auto* buf = static_cast<char*>(arena.allocate(src.size()));
   std::memcpy(buf, src.data(), src.size());
   return {buf, src.size()};
}
```

### Undo Entry Types

```cpp
struct undo_entry
{
   enum class kind : uint8_t
   {
      /// Key was inserted into map (didn't exist anywhere before).
      /// Undo: erase from map.
      insert,

      /// Key existed in map, value was overwritten.
      /// Undo: restore old btree_value.
      overwrite_buffered,

      /// Key existed only in PsiTri/RO, was shadowed by map entry.
      /// Undo: erase from map (reads fall through to lower layers).
      overwrite_cow,

      /// Key existed in map, was removed/tombstoned.
      /// Undo: re-insert old btree_value.
      erase_buffered,

      /// Key existed only in PsiTri/RO, tombstone added to map.
      /// Undo: erase tombstone from map.
      erase_cow,

      /// Range was erased.
      erase_range,
   };

   kind             type;
   std::string_view key;           // always arena-backed

   /// Old value — only for buffered types (overwrite_buffered, erase_buffered).
   /// For data values: old_value.data is arena-backed.
   /// For subtree values: old_value.subtree_root is the ptr_address.
   /// COW types (overwrite_cow, erase_cow) don't need old values —
   /// erasing from map restores visibility to lower layers.
   btree_value      old_value;

   // For erase_range only:
   struct range_data
   {
      std::string_view low;   // arena-backed
      std::string_view high;  // arena-backed

      // Only ART map keys need per-key undo entries.
      // PsiTri keys are undone by removing the range tombstone —
      // reads fall through to the original values in lower layers.
      struct buffered_entry
      {
         std::string_view key;        // arena-backed
         btree_value      old_value;
      };
      std::vector<buffered_entry> buffered_keys;  // ART map keys only
   };
   std::unique_ptr<range_data> range;  // erase_range only, null otherwise
};
```

### Nested Transactions

PsiTri supports nested transactions (savepoints). This affects the undo log
but **not** the WAL — only the outermost commit writes a WAL entry.

The undo log is organized as a **stack of frames**. Each nesting level pushes
a new frame. The frame boundary is just an index into a single flat vector
of undo entries.

```cpp
struct undo_log
{
   std::vector<undo_entry>  entries;       // flat list of all entries
   std::vector<uint32_t>    frame_starts;  // stack of frame boundaries

   void push_frame()  { frame_starts.push_back(entries.size()); }
   void pop_frame()   { frame_starts.pop_back(); }

   uint32_t current_frame_start() const { return frame_starts.back(); }
};
```

**Inner abort** — replay entries from `current_frame_start()` to end in
reverse, then truncate `entries` back to `current_frame_start()` and pop
the frame. Only the inner transaction's mutations are undone.

**Inner commit** — pop the frame boundary. The entries stay in the vector
and become the parent frame's responsibility. If the parent later aborts,
it will undo everything including the inner transaction's mutations.

**Outermost commit** — serialize all operations to WAL, discard entire
undo log.

**Outermost abort** — replay all entries in reverse, discard entire undo log.

### Undo Log Lifecycle

```
begin_transaction()          (depth 0 → 1)
   → push_frame()

   upsert(key, value)
      → record undo entry in current frame
      → apply mutation to ART map

   begin_transaction()       (depth 1 → 2, nested)
      → push_frame()

      upsert(key2, value2)
         → record undo entry in current frame

      abort()                (depth 2 → 1, inner abort)
         → replay entries [frame_start..end) in reverse
         → truncate entries to frame_start
         → pop_frame()
         — ART map is restored to state before inner transaction

   begin_transaction()       (depth 1 → 2, retry)
      → push_frame()

      upsert(key3, value3)
         → record undo entry in current frame

      commit()               (depth 2 → 1, inner commit)
         → pop_frame()
         — entries merge into parent frame
         — ART map retains inner transaction's mutations

commit()                     (depth 1 → 0, outermost)
   → serialize all operations to WAL entry
   → fdatasync (or group commit)
   → discard entire undo_log

--- or ---

abort()                      (depth 1 → 0, outermost)
   → replay ALL entries in reverse
   → discard entire undo_log
```

### Operation Recording

```
upsert(key, value)
   → if key doesn't exist (not in map, not in lower layers):
        push {kind::insert, arena.copy(key)}
   → if key exists in map:
        push {kind::overwrite_buffered, arena.copy(key), old_btree_value}
        // data values: arena.copy(old_value.data)
        // subtree values: copy ptr_address (8 bytes, no arena needed)
   → if key exists only in lower layers (RO/PsiTri):
        push {kind::overwrite_cow, arena.copy(key)}
        // no old value needed — erase from map restores visibility
   → apply mutation to map

remove(key)
   → if key exists in map:
        push {kind::erase_buffered, arena.copy(key), old_btree_value}
   → if key exists only in lower layers:
        push {kind::erase_cow, arena.copy(key)}
   → if key doesn't exist anywhere: no-op
   → insert tombstone into map

remove_range(low, high)
   → collect map keys in [low, high): copy key+value to arena
   → push {kind::erase_range, range={low, high, buffered_keys}}
   → erase map keys in range
   → add range tombstone to range_tombstone_list
   // PsiTri keys in the range are NOT enumerated — the range
   // tombstone shadows them. On abort, removing the tombstone
   // restores visibility. O(map keys in range), not
   // O(PsiTri keys in range).
```

### Undo Replay (Reverse Order)

Abort is the slow path — we optimize for zero-copy recording on the
write path. COW entries require a brief SAL read lock to dereference.

```
insert:
   → erase key from map

overwrite_buffered:
   → restore old btree_value in map
     (data: arena-backed string_view; subtree: ptr_address)

overwrite_cow:
   → erase key from map
   // reads fall through to lower layers which have the original

erase_buffered:
   → re-insert key with old btree_value into map

erase_cow:
   → erase tombstone from map
   // reads fall through to lower layers which have the original

erase_range:
   → remove range tombstone [low, high) from range_tombstone_list
   → for each buffered_key in reverse:
        re-insert key with old btree_value into map
   // Lower-layer keys are automatically visible again once the
   // range tombstone is removed — no per-key work needed.
```

### Memory Safety

All string_views in undo entries point into the `undo_arena`, not into the
ART map. The arena is a bump allocator — allocation is a pointer bump,
and the entire arena is freed as a unit on commit or after abort replay.

**Arena string_views** (keys and buffered data values) are stable for the
undo log's lifetime — the arena is freed as a unit on commit or after abort.

**COW-type undo entries** (overwrite_cow, erase_cow) store only the key.
No PsiTri pointers or coordinates are held. On abort, the map entry
is erased and reads fall through to lower layers which have the original
value. This avoids any lifetime issues with PsiTri node addresses.

### Memory Cost

Per undo entry: ~64 bytes (kind + 2 string_views + padding).
Arena cost: proportional to total key+value bytes modified.
A transaction with 1000 mutations of 100-byte keys+values: ~100 KB arena + ~64 KB entries.
Freed instantly on commit or abort — no per-entry deallocation.
