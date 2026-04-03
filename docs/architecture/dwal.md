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

| Layer | Mutability | Who writes | Who reads | Contention |
|-------|-----------|------------|-----------|------------|
| **RW btree** | read-write | application thread | application thread, readers | writer-only (single writer) |
| **RO btree** | immutable | nobody (frozen) | merge thread, readers | none |
| **PsiTri tree** | COW | merge thread | everyone | none (COW isolation) |

### Memory Management: Bump Allocator Pools

Each btree layer owns a **bump allocator pool** — a contiguous arena that
keys and values are allocated from. The entire pool is freed as a unit when
the btree is discarded.

There are **N pool slots** (e.g. 4), allowing pipelining: while one RO btree
is being merged, another swap can occur if the merge is slow.

```
Pool slots:   [0]  [1]  [2]  [3]
               │    │
               │    └── RO btree being merged (generation 5)
               └── RO btree waiting for readers to drain (generation 4)

RW btree uses its own pool (not in the slot array).
```

### Lifecycle

1. Writes go to the **RW btree** (the hot head), backed by the WAL for durability.
   Keys and values are bump-allocated from the RW pool.
2. When the RW btree reaches a size/time threshold, **swap**:
   - The RW btree becomes the new **RO btree** (just a pointer swap, zero cost).
     Its pool is assigned a slot and a generation number.
   - A fresh empty btree with a new pool becomes the new RW btree.
   - The WAL is rotated — old WAL file covers the now-frozen RO btree.
3. A **background merge thread** drains the RO btree into the PsiTri COW tree.
   No lock contention with the writer — it's working on a different btree.
4. When the merge completes, the RO btree pool **cannot be freed yet** — readers
   may still hold pointers into it. The pool waits for all readers to release.
5. Once all readers have released, the pool is freed and the slot is recycled.

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

A read must check all three layers, newest first:

```
read(key):
   if key in RW btree:
      return RW btree value (or tombstone)
   if RW range_tombstones.is_deleted(key):
      return not_found
   if RO btree exists and key in RO btree:
      return RO btree value (or tombstone)
   if RO range_tombstones.is_deleted(key):
      return not_found
   return PsiTri tree lookup
```

Point tombstones in the btree shadow individual deletes. Range tombstones
shadow bulk deletes that haven't merged to PsiTri yet.

### Why Three Layers

Two layers (RW btree + PsiTri) would require the merge thread to lock the
RW btree or copy it before merging. Three layers eliminate this:

- The writer never touches the RO btree.
- The merge thread never touches the RW btree.
- Readers see a consistent view: RW → RO → PsiTri, checked in order.

This is analogous to LSM-tree's active memtable + immutable memtable + L0,
but using btree_maps and a PsiTri tree instead of sorted runs.

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

### Header (64 bytes)

```
Offset  Size  Field
──────  ────  ─────
0       4     magic: 0x44574C31 ("DWL1")
4       4     version: 1
8       8     sequence_base: first sequence number in this file
16      8     created_timestamp: nanoseconds since epoch (human debugging only)
24      8     last_checkpoint_seq: last fully drained sequence
32      4     root_count: number of roots this WAL covers (≤512)
36      4     flags: (bit 0 = clean close)
40      24    reserved (zero-filled)
```

### Entry

Each entry is one committed transaction. Entries are append-only, written
sequentially, with the hash at the end so the write can stream without seeking.

```
Offset  Size       Field
──────  ────       ─────
0       4          entry_size: total bytes including trailing hash
4       8          sequence: monotonically increasing
12      2          root_index: which top-level root (0–511)
14      2          op_count: number of operations
16      ...        operations[]
...     8          xxh3_64: covers bytes [0, entry_size - 8)
```

20 bytes of header + operations + 8 bytes trailing hash.

### Operations

```
op_type  0x01 = upsert
op_type  0x02 = remove
op_type  0x03 = remove_range
```

**upsert:**
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

### Checkpoint Record

Written when the btree_map is fully drained to the COW tree:

```
0       4          entry_size (fixed: 20)
4       8          sequence: last drained sequence
12      1          record_type: 0xFF
13      7          reserved
```

After a checkpoint, all entries before that sequence are redundant.

### Recovery

1. Read header, verify magic/version
2. Find `last_checkpoint_seq` from header
3. Scan forward from checkpoint, replaying valid entries into the btree_map
4. Entry with bad XXH3 or truncated size = crash boundary, stop replay
5. Resume normal operation

### File Rotation

When the WAL exceeds a size threshold (e.g. 64 MB):

1. Drain all buffered writes to the COW tree
2. Write checkpoint record
3. Start new WAL file with `sequence_base` = last sequence + 1
4. Delete old WAL file

File naming: `data/wal/wal-000001.dwal`, `data/wal/wal-000002.dwal`, ...

### Group Commit

Multiple threads batch into a single `fdatasync`:

1. Writer serializes entry into thread-local staging buffer
2. Sync leader collects pending entries, writes contiguously, calls `fdatasync` once
3. All waiting writers unblocked

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

When a transaction modifies a key, the old value lives in one of two places:

1. **In the btree_map** — a prior buffered write that hasn't drained yet.
   The old value is **copied into the undo arena** (btree nodes can split
   during subsequent mutations, invalidating pointers).
2. **In the PsiTri COW tree** — the key was never buffered. The undo entry
   stores a **`(leaf_address, branch_index)`** coordinate pair — just two
   integers, no data copy, no read lock held.

The design principle: **optimize the write path, not the abort path.** Aborts
are rare; every write pays the cost of undo recording. So:

- **Btree_map values**: must be copied (btree rebalancing invalidates pointers).
- **PsiTri values**: store coordinates only. Zero copy on write. On abort,
  a brief SAL read lock is taken to dereference the coordinates and restore
  the value. This is acceptable because abort is the slow path.

The PsiTri coordinates are stable because the COW tree's leaf structure is
immutable from the reader's perspective — the compactor may relocate the leaf,
but the control block indirection resolves to the current address. The
`branch_index` within the leaf is fixed (leaf content is immutable once
committed).

### Arena-Backed Value Storage

Values in `absl::btree_map` can be invalidated by node splits/merges during
subsequent mutations in the same transaction. So old values must be copied
into an arena owned by the undo log — a bump allocator that is freed as a
unit when the undo log is discarded.

```cpp
/// Simple bump allocator for undo value storage.
/// Freed entirely on commit or after abort replay.
struct undo_arena
{
   std::vector<std::unique_ptr<char[]>> blocks;
   char*    cursor = nullptr;
   size_t   remaining = 0;

   std::string_view copy(std::string_view src)
   {
      if (src.size() > remaining)
         grow(src.size());
      char* dst = cursor;
      std::memcpy(dst, src.data(), src.size());
      cursor += src.size();
      remaining -= src.size();
      return {dst, src.size()};
   }
};
```

### Undo Entry Types

```cpp
/// Lightweight reference to a value in the PsiTri COW tree.
/// Just coordinates — no data copy, no read lock to store.
/// Dereference requires a brief read lock (only on abort).
struct cow_ref
{
   sal::ptr_address leaf_address;
   uint16_t         branch_index;
};

struct undo_entry
{
   enum class kind : uint8_t
   {
      /// Key was inserted into btree_map (didn't exist anywhere before).
      /// Undo: erase from btree_map.
      insert,

      /// Key existed in btree_map, value was overwritten.
      /// Undo: restore old_value from arena.
      overwrite_buffered,

      /// Key existed only in PsiTri, was shadowed by btree_map entry.
      /// Undo: erase from btree_map (reads fall through to PsiTri).
      overwrite_cow,

      /// Key existed in btree_map, was removed/tombstoned.
      /// Undo: re-insert with old_value from arena.
      erase_buffered,

      /// Key existed only in PsiTri, tombstone added to btree_map.
      /// Undo: erase tombstone from btree_map.
      erase_cow,

      /// Range was erased — mixed sources.
      erase_range,
   };

   kind             type;
   std::string_view key;        // always arena-backed

   union
   {
      std::string_view old_value;  // arena-backed (overwrite_buffered, erase_buffered)
      cow_ref          old_cow;    // PsiTri coordinates (overwrite_cow, erase_cow)
   };

   // For erase_range: the removed entries.
   struct range_entry
   {
      std::string_view key;         // arena-backed
      bool             from_buffer; // true = old_value valid, false = cow valid
      union
      {
         std::string_view old_value;
         cow_ref          old_cow;
      };
   };
   std::vector<range_entry> removed_entries;  // erase_range only
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
   → if key doesn't exist (not in btree_map, not in PsiTri):
        push {kind::insert, arena.copy(key)}
   → if key exists in btree_map:
        push {kind::overwrite_buffered, arena.copy(key), arena.copy(old_value)}
   → if key exists only in PsiTri:
        push {kind::overwrite_cow, arena.copy(key), cow_ref{leaf_addr, branch_idx}}
        // leaf_addr and branch_idx known from the lookup — no extra work
   → apply mutation to btree_map

remove(key)
   → if key exists in btree_map:
        push {kind::erase_buffered, arena.copy(key), arena.copy(old_value)}
   → if key exists only in PsiTri:
        push {kind::erase_cow, arena.copy(key), cow_ref{leaf_addr, branch_idx}}
   → if key doesn't exist anywhere: no-op
   → insert tombstone into btree_map

remove_range(low, high)
   → for each affected key:
        add range_entry with arena.copy(key) + source info
   → push {kind::erase_range, removed_entries: [...]}
   → apply range deletion
```

### Undo Replay (Reverse Order)

Abort is the slow path — we optimize for zero-copy recording on the
write path. COW entries require a brief SAL read lock to dereference.

```
insert:
   → erase key from btree_map

overwrite_buffered:
   → restore old_value (from arena) in btree_map

overwrite_cow:
   → erase key from btree_map
   // reads now fall through to PsiTri, which still has the original

erase_buffered:
   → re-insert key with old_value (from arena) into btree_map

erase_cow:
   → erase tombstone from btree_map
   // reads now fall through to PsiTri, which still has the original

erase_range:
   → for each removed_entry in reverse:
        if from_buffer: re-insert key with old_value from arena
        else: erase tombstone from btree_map
```

### Memory Safety

All string_views in undo entries point into the `undo_arena`, not into the
btree_map. The arena is a bump allocator — allocation is a pointer bump,
and the entire arena is freed as a unit on commit or after abort replay.

**Arena string_views** (btree_map-sourced values) are stable for the undo
log's lifetime — the arena is freed as a unit on commit or after abort.

**cow_ref coordinates** (PsiTri-sourced values) are stable because:

- The leaf's `ptr_address` resolves through the control block, which follows
  relocations by the compactor. The address remains valid.
- The `branch_index` within the leaf is immutable (leaf content doesn't
  change after commit).
- No read lock is held to *store* a cow_ref. A brief read lock is taken
  only on abort to dereference it — and abort is the rare slow path.

### Memory Cost

Per undo entry: ~64 bytes (kind + 2 string_views + padding).
Arena cost: proportional to total key+value bytes modified.
A transaction with 1000 mutations of 100-byte keys+values: ~100 KB arena + ~64 KB entries.
Freed instantly on commit or abort — no per-entry deallocation.
