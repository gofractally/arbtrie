# DWAL: Dynamic Write-Ahead Log

`dwal_database` is an opt-in wrapper around `database` that adds an adaptive
write buffer with WAL durability. Under low write pressure it passes through
directly to the COW tree. Under high write pressure it batches writes in an
`absl::btree_map` backed by a WAL file, then drains to the COW tree when
pressure drops.

## Three-Layer Architecture

Reads and writes flow through three layers:

```
┌─────────────────────────────────────┐
│  Read-Write btree  (hot head)       │  ← active writes land here
│  absl::btree_map, single-writer     │
├─────────────────────────────────────┤
│  Read-Only btree   (frozen)         │  ← background thread merges to PsiTri
│  absl::btree_map, immutable         │     no contention with writers
├─────────────────────────────────────┤
│  PsiTri COW tree   (persistent)     │  ← on-disk, segment-allocated
│  concurrent readers via sessions    │
└─────────────────────────────────────┘
```

| Layer | Mutability | Who writes | Who reads | Synchronization |
|-------|-----------|------------|-----------|-----------------|
| **RW btree** | read-write | writer threads (exclusive lock) | writer + reader threads (shared lock) | `std::shared_mutex` |
| **RO btree** | immutable | nobody (frozen) | merge thread, readers | none (immutable) |
| **PsiTri tree** | COW | merge thread | everyone | none (COW isolation) |

### Memory Management: Bump Allocator Pools

The `absl::btree_map` stores `std::string_view` keys and values that point
into an external **bump allocator pool**. The pool is a linked list of
fixed-size blocks. Allocation is a pointer bump. There is no per-entry
deallocation — the entire pool is released as a unit when its btree layer
is discarded.

The pool is a `std::pmr::monotonic_buffer_resource` — a standard bump
allocator with no per-entry deallocation. `release()` frees all memory at
once.

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
/// treated as deleted even if it exists in the RO btree or PsiTri.
struct btree_value
{
   enum class kind : uint8_t { data, subtree, tombstone };

   kind             type = kind::data;
   std::string_view data;              // pool-backed (kind::data only)
   sal::ptr_address subtree_root = {}; // PsiTri subtree address (kind::subtree only)
};

struct btree_layer
{
   std::pmr::monotonic_buffer_resource                    pool;
   absl::btree_map<std::string_view, btree_value, ucc_less> map;
   range_tombstone_list                                   tombstones;
   uint32_t                                               generation = 0;

   /// Copy key/value into the pool, return stable views.
   std::pair<std::string_view, std::string_view>
   store(std::string_view key, std::string_view value)
   {
      auto* kbuf = static_cast<char*>(pool.allocate(key.size()));
      std::memcpy(kbuf, key.data(), key.size());
      auto* vbuf = static_cast<char*>(pool.allocate(value.size()));
      std::memcpy(vbuf, value.data(), value.size());
      return {{kbuf, key.size()}, {vbuf, value.size()}};
   }

   /// Copy key into the pool, insert as tombstone.
   std::string_view store_tombstone(std::string_view key)
   {
      auto* kbuf = static_cast<char*>(pool.allocate(key.size()));
      std::memcpy(kbuf, key.data(), key.size());
      return {kbuf, key.size()};
   }
};
```

Keys and values in the btree_map are `std::string_view` pointing into the pool.
On swap, the entire layer is frozen and handed to the merge thread. On
release, `pool.release()` frees key/value data in one pass. Subtree refs
require a walk — see "Subtree Reference Counting" below.

```
┌──────────────────────────────────────┐
│  btree_layer                         │
│                                      │
│  btree_map<string_view,              │── keys/values point into ──┐
│            string_view>              │                             │
│  range_tombstone_list                │                             │
│  pmr::monotonic_buffer_resource pool │◄────────────────────────────┘
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

On swap, the writer moves the RW btree_layer into the root's RO slot
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
app.take() → btree_map → merge transfers to PsiTri → PsiTri owns it
```

The merge thread creates `smart_ptr(session, addr, inc_ref=false)` —
claiming the existing ref, not adding one — and passes it to PsiTri's
`upsert`, which takes it via `.take()`. No `inc_ref` at any point.

After merge, readers who encounter subtrees in the RO btree_map are
safe because their read transaction holds a `smart_ptr` to the PsiTri
root. PsiTri's COW semantics keep the old root (and transitively all
its subtrees) alive via the reader's snapshot ref.

**Release responsibilities:**

| When | What to release |
|------|----------------|
| **Abort** (undo replay) | New subtree displaced from btree_map |
| **Commit** (undo discard) | Old subtree saved in undo entry |
| **RW layer destruction** (unmerged, e.g. shutdown) | Walk btree_map, release `kind::subtree` entries |
| **RO layer destruction** (fully merged) | Nothing — all refs were transferred to PsiTri |

**Example** — upsert subtree_B overwriting subtree_A in btree_map:
```
upsert:  btree_map[key] = B, save A in undo      [btree:B] [undo:A]
abort:   restore A to btree_map, B displaced      → release(B)
commit:  undo discarded                           → release(A)
merge:   transfer B to PsiTri (no inc_ref)        [psitri:B]
destroy: nothing to release (RO, fully merged)    [psitri:B] ✓
```

**COW-sourced subtrees** (key exists only in PsiTri): no ref management
in the DWAL layer. PsiTri owns the ref. On overwrite, the btree_map
shadows it; on abort, the btree_map entry is erased and reads fall
through to PsiTri. On merge, PsiTri's COW handles releasing the old
subtree when the new value replaces it.

### Large Transaction Fallback

The RW btree has a size cap (pool memory or key count) that determines
when swaps occur. This cap also imposes a **maximum buffered transaction
size**. A transaction that exceeds the cap cannot fit in the RW btree.

Large transactions **fall back to direct COW writes** against PsiTri:

1. **Before the transaction begins**, the writer flushes: triggers a
   swap of the current RW btree (if non-empty), then **blocks until
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
// Flushes RW btree, waits for merge, then writes directly to PsiTri.
// No size limit — bounded only by disk space.
```

The writer still holds the exclusive lock for the duration, so the RW
btree is quiescent during a direct transaction. After the direct
transaction commits, normal buffered writes resume.

This means the DWAL is an optimization for the common case (many small
transactions), not a constraint. Bulk loads, migrations, and large
batch operations use `direct` mode and get PsiTri's full COW semantics
with no artificial size limit.

### Concurrency: Per-Root Reader/Writer Lock

Each root has its own `std::shared_mutex` protecting its RW btree.
Writers on independent roots never contend — they acquire different
locks and touch completely independent data structures (btree_map,
pool, WAL file, RO slot).

- **Write transactions** acquire the root's exclusive lock for the
  duration of the transaction. Multiple writers to the *same root*
  serialize. Writers to *different roots* run fully in parallel.
- **Read transactions** (`latest` mode) acquire the root's shared lock.
  Multiple readers hold the shared lock concurrently without blocking
  each other. Readers on different roots never contend.
- **The transaction API is the same** for read and write — a read
  transaction simply doesn't expose mutation methods.

```cpp
class dwal_transaction
{
   std::shared_mutex&  _root_lock;  // per-root lock
   // Writer holds unique_lock for entire transaction lifetime.
   // Reader holds shared_lock for entire transaction lifetime.
   std::variant<std::unique_lock<std::shared_mutex>,
                std::shared_lock<std::shared_mutex>> _lock;

   // Writer: lock already held, no per-mutation locking
   void upsert(key, value) {
      _rw_btree->insert_or_assign(key, value);
      _undo_log.record(...);
   }

   // Reader: lock already held
   std::optional<value> get(key) {
      if (auto it = _rw_btree->find(key); it != _rw_btree->end())
         return it->second;
      // Fall through to RO btree, then PsiTri
      ...
   }

   // Returns a merge cursor over all three layers
   dwal_cursor cursor() const;
};
```

### Merge Cursor

The DWAL exposes a **merge cursor** that presents a unified sorted view
across all three layers. It mirrors the PsiTri `cursor` API: positioned
iteration, seeking, counting — but merges results from the RW btree,
RO btree, and PsiTri cursor, filtering through tombstones and range
tombstones.

```cpp
class dwal_cursor
{
   // Three source iterators, each positioned at their current key
   using btree_iter = absl::btree_map<std::string_view, std::string_view>::const_iterator;

   btree_iter                rw_it, rw_end;       // RW btree (if lock held)
   btree_iter                ro_it, ro_end;        // RO btree (if not stale)
   psitri::cursor            tri_cursor;           // PsiTri COW tree
   const range_tombstone_list* rw_tombstones;
   const range_tombstone_list* ro_tombstones;

   // Which source(s) produced the current position
   enum class source { rw, ro, tri, end };
   source _current_source;

public:
   // -- Positioning --
   bool seek_begin();         // position at first live key
   bool seek_last();          // position at last live key
   bool seek_end();           // position past last key
   bool seek_rend();          // position before first key

   bool lower_bound(key_view key);   // first key >= key
   bool upper_bound(key_view key);   // first key > key
   bool seek(key_view key);          // exact match

   // -- Navigation --
   bool next();               // advance to next live key
   bool prev();               // retreat to previous live key

   // -- Access --
   bool       is_end() const;
   bool       is_rend() const;
   key_view   key() const;           // current key
   template <ConstructibleBuffer T>
   std::optional<T> value() const;   // current value
   int32_t    value_size() const;

   // -- Counting --
   uint64_t count_keys(key_view lower = {}, key_view upper = {}) const;

   // -- Subtree support --
   bool is_subtree() const;
   sal::smart_ptr<sal::alloc_header> subtree() const;
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

1. Writes go to the **RW btree** (the hot head), backed by the WAL for
   durability. Keys and values are bump-allocated from the RW pool.
   Writers hold the exclusive lock for the full transaction.

2. A swap is triggered by either condition:
   - **Btree size:** the writer determines the RW btree is full (pool
     memory or entry count) — checked inline during the write transaction,
     under the exclusive lock it already holds.
   - **WAL size:** the WAL file exceeds a threshold. Handles the case
     where the btree stays small (repeated overwrites) but the WAL grows
     large (every overwrite is a new entry). Checked at commit time.
   - **Time-based:** a background timer detects an RW btree that hasn't
     been touched for longer than `idle_flush_interval`. The timer
     thread acquires the exclusive lock and performs the swap. This
     ensures idle roots drain to PsiTri promptly — without it, a burst
     of writes followed by silence would leave data stranded in the RW
     btree, invisible to `persistent`-mode readers.

   The swap procedure (under exclusive lock):
   - Check that this root's RO slot is free (previous merge completed
     and readers drained). If not, the writer **blocks** until the
     merge finishes. This is the correct backpressure: the WAL exists
     to batch COW writes and amortize their cost — it cannot sustain
     throughput beyond what PsiTri can absorb. Buffering more (multiple
     RO slots, unbounded RW growth) just defers the stall while
     consuming the same memory and adding read cursor complexity.
   - Note the current PsiTri root — this is the **base root** of the new
     RO btree. All keys in the RO btree are relative to this snapshot.
   - The RW btree becomes the new **RO btree**, tagged with the base root.
     Its pool is assigned a slot and a generation number.
   - A fresh empty btree with a new pool becomes the new RW btree.
   - The WAL is rotated — old WAL file covers the now-frozen RO btree.
   - Publish the new RO btree pointer (atomic store, release).
   - Signal the merge pool (condition variable).

3. A merge pool thread wakes, picks up the root, and drains the RO
   btree into PsiTri. When complete, it publishes a **new PsiTri root**
   via atomic store (release).

4. The RO btree pool **cannot be freed yet** — readers may still hold
   pointers into it. The pool waits for all readers to release
   (epoch-based reclamation).

5. Once all readers have released, the pool is freed and the slot is recycled.

### Merge Thread Pool

Merging RO btrees into PsiTri is the throughput bottleneck — every
write ultimately flows through this path. A single merge thread would
serialize all roots, capping global write throughput regardless of how
many roots are active.

Instead, a **bounded thread pool** drains RO btrees. Each drain runs
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

**Writer-driven push:** The writer decides when the RW btree is full
(by size, entry count, or pool memory pressure) and performs the swap
under the exclusive lock it already holds. It freezes the RW btree into
the root's RO slot and signals the merge pool (condition variable).
A pool thread wakes, picks up the root, and drains it.

The writer already holds the exclusive lock at swap time — no extra
synchronization needed. The merge pool threads don't scan or poll;
they block on a condition variable and are woken by the writer's signal.

**No deconfliction needed between pool threads.** Each root is an
independent PsiTri tree with its own root pointer. Two threads draining
different roots operate on completely separate COW trees — different
root pointers, different COW paths. They share only the SAL allocator,
which is already designed for concurrent sessions (per-session
allocation, lock-free release queues). Each root has at most one
pending RO btree, so at most one merge thread works on a given root.

### RO Btree Staleness Detection

Each RO btree carries the **PsiTri root it was built from** (the base root).
Readers use this to detect whether the merge has completed:

```cpp
struct ro_btree_snapshot
{
   absl::btree_map<...>*   map;
   range_tombstone_list*    tombstones;
   sal::ptr_address         base_root;   // PsiTri root at swap time
   uint32_t                 generation;  // for epoch-based pool reclamation
};
```

#### The Ordering Problem

Two independent threads write to two atomic variables:

- **Merge thread** stores `tri_root` (when merge completes)
- **Writer thread** stores `ro_ptr` (when swap occurs)

A reader must load both and compare. A **false skip** (concluding the
RO btree is merged when it hasn't been) causes data loss. A **false
include** (redundantly reading a merged RO btree) is harmless — the
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
// === Writer thread (swap, under exclusive lock) ===
writer_swap():
   snapshot = tri_root.load(acquire)       // (1) syncs with merge thread
   new_ro = freeze current RW btree
   new_ro.base_root = snapshot
   ro_ptr.store(new_ro, release)           // (2) carries chain to readers
   allocate fresh RW btree

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
`base_root != root`, and skip the RO btree. They read PsiTri directly
with zero overhead from the DWAL layer.

The RO btree is only consulted during the window between a swap and the
completion of the subsequent merge — a transient state under write pressure.

### Range Tombstones

Range deletes cannot be efficiently represented as per-key tombstones in the
btree_map (the range may cover millions of keys in PsiTri). Instead, each
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
key. The key becomes live in the btree_map, and the two remaining sub-ranges
stay as tombstones.

**On merge to PsiTri:** range tombstones are applied directly as
`remove_range` calls on the COW tree. After merge, the range tombstone list
is discarded with the RO btree.

### Read Path

#### Point Lookups (fast path)

Point lookups (`get`) do **not** use the merge cursor. They are a direct
layered find — check each layer in priority order, short-circuit on
first hit or tombstone:

```
get(key):
   // Layer 1: RW btree (lock already held by transaction)
   if auto it = rw_map.find(key):
      if it->is_tombstone: return not_found
      return it->value
   if rw_tombstones.is_deleted(key): return not_found

   // Layer 2: RO btree (if active, immutable)
   if ro:
      if auto it = ro->map.find(key):
         if it->is_tombstone: return not_found
         return it->value
      if ro->tombstones.is_deleted(key): return not_found

   // Layer 3: PsiTri
   return tri_cursor.get(key)
```

In the common case (RW btree small/empty, RO null), this is one
btree miss + one PsiTri lookup. No iterator state, no merge overhead.

#### Range Scans (merge cursor)

Range operations (`lower_bound`, `next`, `prev`, `count_keys`) use
the **merge cursor** which maintains positioned iterators across all
active layers. See "Merge Cursor" section below.

The merge cursor checks layers in priority order: RW btree > RO btree
> PsiTri. Tombstones and range tombstones in higher layers shadow lower
layers.

#### Layer Selection: Latency vs Freshness

The caller chooses which layers to include when starting a read
transaction, trading freshness for latency:

| Mode | Layers | Lock | Staleness | Use case |
|------|--------|------|-----------|----------|
| **latest** | RW + RO + Tri | shared lock on RW | none — sees all committed writes | interactive queries needing freshest state |
| **buffered** | RO + Tri | none | up to one swap behind | bulk reads, avoids writer contention |
| **persistent** | Tri only | none | up to one merge behind | cheapest read, no DWAL overhead |

```cpp
enum class read_mode { latest, buffered, persistent };

auto txn = dwal.start_read_transaction(read_mode::buffered);
auto cur = txn.cursor();  // merge cursor over RO + Tri only, no lock
```

All three modes see only committed data — the exclusive lock held by
writers for the full transaction duration guarantees readers never see
partial writes. The difference is recency: `latest` includes the most
recent commits to the RW btree, `buffered` lags by at most one swap
interval, and `persistent` lags by at most one merge cycle.

The `persistent` mode is equivalent to reading PsiTri directly — the
DWAL layer is invisible and there is zero DWAL overhead. This is the
**default** — most reads don't need sub-swap-interval freshness. The
`buffered` mode avoids the `shared_mutex` entirely, which matters under
heavy write contention. The `latest` mode is for the rare case where
the caller needs the absolute freshest committed state.

### Why Three Layers

- The **RW btree** is guarded by `shared_mutex`. Writers serialize
  (exclusive lock for full transaction), readers share (shared lock
  for full transaction).
- The **RO btree is immutable**. Any thread reads it without locks.
- The **PsiTri tree** uses COW isolation — concurrent readers via sessions.
- The **merge thread** only touches the RO btree (reading) and PsiTri
  (writing via COW). It never touches the RW btree.

### Epoch-Based Pool Reclamation

The RO btree pool must outlive all readers that hold pointers into it. PsiTri's
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

Each RO btree pool is assigned a monotonically increasing **generation number**
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
| **WAL file** | Durability of *committed* transactions | Disk (append-only) | Until checkpoint (RO btree fully merged) |
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
the layer is created (fresh RW btree after swap) and deleted when the
layer is discarded (merge complete + readers drained). At most **two
WAL files exist per root** at any time:

```
root-003/wal-rw.dwal    ← current RW btree's WAL (being appended to)
root-003/wal-ro.dwal    ← RO btree's WAL (frozen, needed for crash recovery)
```

**On swap:**
1. Close the current WAL file (it now covers the frozen RO btree)
2. Rename it to `wal-ro.dwal`
3. Open a fresh `wal-rw.dwal` for the new RW btree

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

1. If `wal-ro.dwal` exists: the previous RO btree was not fully merged
   before crash. Replay it into a btree_map, then drain to PsiTri
   before accepting new writes.
2. If `wal-rw.dwal` exists: replay valid entries into the RW btree_map.
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
      // rw_map becomes the live RW btree

   // ready for new transactions
```

No checkpoint records are needed. The WAL file boundary (rw vs ro) is
the checkpoint — everything in `wal-ro.dwal` is pre-swap, everything
in `wal-rw.dwal` is post-swap.

### Durability Model

The WAL `write()` is a buffered append — no fsync per transaction.
The exclusive lock covers only the btree mutation + buffered WAL write,
no I/O. This keeps the lock hold time minimal and eliminates the
group commit problem entirely (no fsync to batch).

**`fdatasync` is periodic and application-driven.** The app calls
`flush()` when it needs a durability boundary (e.g. every N seconds,
on checkpoint, before responding to a client that requires durability).
Each root's WAL fsyncs independently.

```
commit():
   mutate btree_map         // under exclusive lock
   append WAL entry         // buffered write, no I/O
   release exclusive lock   // fast — no fsync in critical path

flush():                    // app calls periodically, or never
   for each dirty root WAL:
      fdatasync()
```

**Crash semantics:** Unflushed WAL entries are lost on crash. The
btree_map is rebuilt from the durable portion of the WAL. This is the
standard WAL tradeoff — applications that need per-transaction
durability call `flush()` after commit. Applications that tolerate
losing the last few seconds of writes (the common case) flush
periodically or not at all.

---

## Undo Log (In-Memory Only)

The undo log enables transaction rollback *before* the WAL entry is written.
As a transaction mutates the `absl::btree_map`, the undo log records how to
reverse each mutation. On abort, the undo log is replayed in reverse. On
commit, it is discarded.

### Why It Can Be Memory-Only

The undo log never needs disk persistence because:

- If the process crashes mid-transaction, the transaction was never committed
  to the WAL, so there is nothing to undo — the btree_map is rebuilt from
  the WAL on recovery.
- The undo log exists only for explicit `abort()` calls during normal operation.

### Old Value Sources

When a transaction modifies a key, the old value is in one of two places:

1. **In the btree_map** — a prior buffered write that hasn't drained yet.
   The old `btree_value` is saved in the undo entry (data values are
   copied into the undo arena since btree rebalancing invalidates pointers;
   subtree values just copy the `ptr_address`).
2. **In the PsiTri COW tree** — the key was never buffered. The undo entry
   needs **only the key** — on abort, erasing the btree_map entry causes
   reads to fall through to PsiTri (or RO btree + PsiTri), which still has
   the original value. No coordinates, no data copy, no read lock.

This works because the RO btree + PsiTri together always represent the
pre-write-transaction state, even if the merge thread modifies PsiTri
concurrently (the staleness detection ensures readers see the correct
merged view).

### Arena-Backed Value Storage

Values in `absl::btree_map` can be invalidated by node splits/merges during
subsequent mutations in the same transaction. So old values must be copied
into an arena owned by the undo log — a bump allocator that is freed as a
unit when the undo log is discarded.

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
      /// Key was inserted into btree_map (didn't exist anywhere before).
      /// Undo: erase from btree_map.
      insert,

      /// Key existed in btree_map, value was overwritten.
      /// Undo: restore old btree_value.
      overwrite_buffered,

      /// Key existed only in PsiTri/RO, was shadowed by btree_map entry.
      /// Undo: erase from btree_map (reads fall through to lower layers).
      overwrite_cow,

      /// Key existed in btree_map, was removed/tombstoned.
      /// Undo: re-insert old btree_value.
      erase_buffered,

      /// Key existed only in PsiTri/RO, tombstone added to btree_map.
      /// Undo: erase tombstone from btree_map.
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
   /// erasing from btree_map restores visibility to lower layers.
   btree_value      old_value;

   // For erase_range only:
   struct range_data
   {
      std::string_view low;   // arena-backed
      std::string_view high;  // arena-backed

      // Only btree_map keys need per-key undo entries.
      // PsiTri keys are undone by removing the range tombstone —
      // reads fall through to the original values in lower layers.
      struct buffered_entry
      {
         std::string_view key;        // arena-backed
         btree_value      old_value;
      };
      std::vector<buffered_entry> buffered_keys;  // btree_map keys only
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
      → apply mutation to btree_map

   begin_transaction()       (depth 1 → 2, nested)
      → push_frame()

      upsert(key2, value2)
         → record undo entry in current frame

      abort()                (depth 2 → 1, inner abort)
         → replay entries [frame_start..end) in reverse
         → truncate entries to frame_start
         → pop_frame()
         — btree_map is restored to state before inner transaction

   begin_transaction()       (depth 1 → 2, retry)
      → push_frame()

      upsert(key3, value3)
         → record undo entry in current frame

      commit()               (depth 2 → 1, inner commit)
         → pop_frame()
         — entries merge into parent frame
         — btree_map retains inner transaction's mutations

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
   → if key doesn't exist (not in btree_map, not in lower layers):
        push {kind::insert, arena.copy(key)}
   → if key exists in btree_map:
        push {kind::overwrite_buffered, arena.copy(key), old_btree_value}
        // data values: arena.copy(old_value.data)
        // subtree values: copy ptr_address (8 bytes, no arena needed)
   → if key exists only in lower layers (RO/PsiTri):
        push {kind::overwrite_cow, arena.copy(key)}
        // no old value needed — erase from btree_map restores visibility
   → apply mutation to btree_map

remove(key)
   → if key exists in btree_map:
        push {kind::erase_buffered, arena.copy(key), old_btree_value}
   → if key exists only in lower layers:
        push {kind::erase_cow, arena.copy(key)}
   → if key doesn't exist anywhere: no-op
   → insert tombstone into btree_map

remove_range(low, high)
   → collect btree_map keys in [low, high): copy key+value to arena
   → push {kind::erase_range, range={low, high, buffered_keys}}
   → erase btree_map keys in range
   → add range tombstone to range_tombstone_list
   // PsiTri keys in the range are NOT enumerated — the range
   // tombstone shadows them. On abort, removing the tombstone
   // restores visibility. O(btree_map keys in range), not
   // O(PsiTri keys in range).
```

### Undo Replay (Reverse Order)

Abort is the slow path — we optimize for zero-copy recording on the
write path. COW entries require a brief SAL read lock to dereference.

```
insert:
   → erase key from btree_map

overwrite_buffered:
   → restore old btree_value in btree_map
     (data: arena-backed string_view; subtree: ptr_address)

overwrite_cow:
   → erase key from btree_map
   // reads fall through to lower layers which have the original

erase_buffered:
   → re-insert key with old btree_value into btree_map

erase_cow:
   → erase tombstone from btree_map
   // reads fall through to lower layers which have the original

erase_range:
   → remove range tombstone [low, high) from range_tombstone_list
   → for each buffered_key in reverse:
        re-insert key with old btree_value into btree_map
   // Lower-layer keys are automatically visible again once the
   // range tombstone is removed — no per-key work needed.
```

### Memory Safety

All string_views in undo entries point into the `undo_arena`, not into the
btree_map. The arena is a bump allocator — allocation is a pointer bump,
and the entire arena is freed as a unit on commit or after abort replay.

**Arena string_views** (keys and buffered data values) are stable for the
undo log's lifetime — the arena is freed as a unit on commit or after abort.

**COW-type undo entries** (overwrite_cow, erase_cow) store only the key.
No PsiTri pointers or coordinates are held. On abort, the btree_map entry
is erased and reads fall through to lower layers which have the original
value. This avoids any lifetime issues with PsiTri node addresses.

### Memory Cost

Per undo entry: ~64 bytes (kind + 2 string_views + padding).
Arena cost: proportional to total key+value bytes modified.
A transaction with 1000 mutations of 100-byte keys+values: ~100 KB arena + ~64 KB entries.
Freed instantly on commit or abort — no per-entry deallocation.
