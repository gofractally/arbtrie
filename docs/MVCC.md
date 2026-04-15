MVCC Design 

A tree is identified as (rootptr, version) where rootptr is a reference counted tree.

In a COW-only mode, existing impl, every mutation creates a new rootptr and version is unnecessay

In MVCC a rootptr contains data from multiple versions and the version number identifies which view this
tree is looking at. 

This means all values are "versioned" if there are more than 1, an "unversioned" value implies all versions
prior to the lowest versioned value. 

If you are mutating (root, v1) you need to know two things:
   a. is root unique? if so then you can modify in place like we do today and v1 changing i soptional, but for consistency / sequence it can be incremented. 

   b. if root is not unique then we either have to create a new root (COW) updating eery node from the leaf to the root... or we need to increment the version number and *insert* a new value into the shared tree.

   If the value is currently stored in a valuenode... 
        a. COW the value node inserting a second value with version and keeping the
            same address/controlblock, atomically updating the location. This atomic update
            will end up modifying the tree as seen by everyone who shares the old root; however,
            readers of the old tree will have a version < the new version and thus still return 
            the old value. 

        b. if the value node grows bigger than a page 4096 containing all of the values; then
           it converts into a node that is an array of (value_node_ptr, version) 
                - these are stored as array of versions and an array of ptrs... so that
                version scanning is quick and easy and they are stored in sequental order
    
        c. if the number of (value_node_ptr, versions) grows beyond what fits in 1 page.. then we start
           a subtree where version = key and value_node_ptr = value... 


Some things to consider:

     a. historically when a thread would commit it was sufficient to only mark their own segments as read-only because anything they changed exited on their own segments and no other readers could see their writes. 

     b. but with MVCC other threads can technically update the "read only tree" by adding new records with higher versions.. this means that part of the old "protected" tree is now "exposed" to stray writes (only bugs)..no different than typical database exposure.. 

     c. the impact of this is more profound, it means one thread committing forces all other threads to commit their current page and start COW all over again if they keep modifying.  It is technically correct and thread safe but will dramatically slow down the other writers. 

     d. as a result of this it is all the more imporant that any write that may abort be cached through a transaction with a look-aside key/value mapping and that only things we are committed to get committed. 


The impact of adding MVCC value nodes is that the reference count on the tree_ptr is not enough to clean up old versions, and thus the value nodes will bloat and never be reclaimed; therefore, we need to know what "versions" are still active, which means what versions have a tree_root (ptr,version) with their version... in effect every tree has 2 reference counts, one on the tree structure and 1 on the version number. 

   So where does the version number's reference count live? lets look at our data structure:

   smart_ptr
       u32 address -> control_block -> node 
       u64 version 

   Options:
    
   smart_ptr
         u32 address -> control_block -> node loc
         u32 ver_addres -> control_block -> version (stored in loc) -> makes version 41 bits 
          (70 years at 1000 versions/sec increment... 25 days at 1 million / sec increment)


    The second approach may be the most correct, less likely for version to get out of sync, 
     no need for a separate map to look up a reference count from version, we can go from address to version when we need it, and it is already in pinned state, cost 1 extra control_block per version, which is likely less than the number of extra control blocks required for a COW of N nodes from leaf to root previously required. 

    The question is how do we track the set of versions in flight so that the garbage collector
    can reclaim the space?   There is no way looking at a control block to know whether it 
    is pointing at memory or a version number.. 

     Rather than tracking "good versions" we can track "retired" versions because retired versions
     don't need to be kept sorted.  Thus when a version ref-count goes to 0, it gets pushed to a 
     thread local queue which gets processed by the compacotr, the compactor then maintains a 
     bloom filter of all retired versions and its own unique kvmap of released versions. 
     It also tracks the low-water mark... basically it knows the oldest version still being referenced.
     Actually it doesn't have to use a kvmap directly, but a list of ranges that have been freed.

     Now whenever the tree needs to do a COW copy of a value node or leaf node (during the MFU update)
     it can exclude copying anything that is in a deleted range. This keeps stale data out of RAM.
     The compactor can also remove them during normal compaction. 

     The issue remains that a compactor might not even know about the free space until it looks into
     the infrequently read nodes. This is where a maitenance/defrag job can run on demand to recapture
     and any segment that hasn't been scanned by the compactor in X period of time can be checked for
     free space... and read threads that stumble across a cold segment (based on time since scan), 
     can check the node they are already reading against the free list and update the free_space stats 
     which the compactor already sees.  



Leaf Node Version Table
───────────────────────

The leaf node maintains a shared version table. Each entry in the table is a
version number (6 bytes). Every key with an inline value has a 1-byte index
into this table, stored in a per-key `ver_indices[]` array.

Multiple keys committed in the same transaction share one version table entry.
This amortizes the 6-byte version cost across all keys that share a version.

```
┌──────────────────────────────────────────┐
│ node header                              │
│ num_versions  (uint8_t)                  │  ← max 255 unique versions per leaf
├──────────────────────────────────────────┤
│ version_table[0..M-1]                    │  ← 6 bytes each: uint48_t version
├──────────────────────────────────────────┤
│ ver_indices[0..N-1]  (uint8_t each)      │  ← index into version_table (0xFF = n/a)
│ key_hashes[0..N-1]   (uint8_t each)      │
│ key_offsets[0..N-1]  (uint16_t each)     │
│ key_sizes[0..N-1]    (uint16_t each)     │
│ value_offsets[0..N-1](uint16_t each)     │  ← value_branch
├──────────────────────────────────────────┤
│         [free space]                     │
├──────────────────────────────────────────┤
│ ← alloc_pos                             │
│ inline value data (no embedded version)  │
│ uint16_t size | data[size] ...           │
└──────────────────────────────── tail ────┘
```

`ver_indices` comes before `key_hashes` because it generally only grows —
new keys appended to the leaf get the latest version index. The version table
itself only grows when a new unique version is committed to this leaf.
Shrinkage occurs when an inline value is promoted to a value_node (rare).

Keys pointing to a value_node or subtree (out-of-line) set `ver_indices[i]`
to 0xFF — version tracking for those keys lives entirely in the value_node.

Version cost per key (amortized):

| Scenario                           | Cost per key        |
|------------------------------------|---------------------|
| 50 keys, 1 transaction             | 6/50 + 1 = 1.1 B   |
| 100 keys, 5 transactions           | 30/100 + 1 = 1.3 B |
| 1 key, 1 transaction               | 6 + 1 = 7 B        |

Version liveness is determined by the global live range map (see "Compaction
and Version Reclamation" below), not by per-entry metadata in the leaf.

Inline Value Format (in leaf alloc area)
────────────────────────────────────────

Inline values no longer embed a version — the version is in the shared version
table, indexed by `ver_indices[i]`. The inline value in the alloc area is just:

```
uint16_t size       — 2 bytes, data length
uint8_t  data[size] — variable
```

This is the same format as COW-only mode. The version overhead is entirely in
the version table + 1-byte index, not in the value data.


Value Node Structure
────────────────────

The value_node is a versioned container for a single key's values across multiple
versions. It is structured as a sorted array of (version, value) pairs — a B-tree
node over the version dimension.

```
┌─────────────────────────────────────┐
│ alloc_header (12 bytes)             │
│ alloc_pos    (uint16_t)             │  ← delta from end of record
│ num_versions (uint16_t)             │
├─────────────────────────────────────┤
│ versions[0..N-1]  (uint48_t each)   │  ← global version numbers, sorted ascending
│ offsets[0..N-1]   (int16_t each)    │  ← value type encoded in offset
├─────────────────────────────────────┤
│         [free space]                │
├─────────────────────────────────────┤
│ ← alloc_pos                        │
│ value_size | value_data ...         │  ← values grow toward tail
│ value_size | value_data ...         │
└─────────────────────────── tail ────┘
```

Per-entry overhead: 6 (version) + 2 (offset) = 8 bytes.

B-tree semantics: a value written at versions[i] is visible to all readers with
snapshot V where versions[i] ≤ V < versions[i+1]. Lookup is a binary search for
the largest versions[i] ≤ V.

Offset encoding (int16_t):

| Offset | Type               | Alloc area usage                      |
|--------|--------------------|---------------------------------------|
| -2     | Tombstone          | nothing (key deleted at this version)  |
| -1     | Null               | nothing (key exists, value is null)    |
|  0     | Reserved           | —                                     |
|  1     | tree_id            | 8 bytes {root, ver} at alloc position  |
|  2     | Zero-length value  | nothing (key exists, empty data)       |
| ≥ 3    | Data               | uint16_t size + data[size]             |

The offset doubles as a type discriminator. A data record requires at least 3 bytes
(uint16_t size + 1 byte data), so |offset| ≤ 2 can never point to valid data,
making small values available as sentinels.

Escalation tiers when a value_node overflows:

| Tier | Condition                        | Structure                                  |
|------|----------------------------------|--------------------------------------------|
| 1    | Single version, small value      | Inline in leaf (no value_node)             |
| 2    | Multiple versions, fits in 1 page| value_node with versions[] + offsets[]     |
| 3    | Versions overflow one page       | Array of (value_node_ptr, version) in page |
| 4    | That array overflows             | Subtree keyed by version number            |

Split strategy: when a value_node overflows, the most recent version is
isolated into its own value_node. All older versions stay together in the
other child. The current node becomes the parent (tier 3 index).

```
Before (one full value_node):
  [v1, v2, v3, ... v98, v99, v100]

After split:
  Parent: [(ptr_old, v1), (ptr_new, v100)]
    ├─→ ptr_old: [v1, v2, v3, ... v98, v99]   ← cold, rarely touched
    └─→ ptr_new: [v100]                        ← hot, receives all new writes
```

This is optimal because:
- New versions always append to the most recent child — small, hot, fast COW
- Historical versions are sealed — never modified, excellent cache behavior
- The compactor can reclaim the cold child entirely when old versions retire
- Repeated splits always produce a small "current" node, avoiding the cost
  of COW-ing a large node just to append one entry

The hot node follows an append-fill-split cycle:

```
[v1] → [v1,v2] → ... → [v1...vN] FULL → split → cold:[v1...vN-1] hot:[vN]
[vN] → [vN,vN+1] → ... → [vN...v2N] FULL → split → cold:[vN...v2N-1] hot:[v2N]
...
```

The hot node cycles between 1 entry and full, so it is 50% full on average.
Every write is a cheap COW of a single half-full node. Cold nodes are 100%
full, sealed, and never touched again until the compactor reclaims them after
all their versions retire.


Concurrency Model: Optimistic Concurrency Control with Striped Locking
══════════════════════════════════════════════════════════════════════════

Overview
────────

MVCC eliminates the root-level serialization bottleneck of COW. In COW mode,
every write clones the path from leaf to root — two writers updating unrelated
keys still conflict at the root. In MVCC mode, the shared tree structure (inner
nodes) is never modified. Writers only touch leaf nodes and value_nodes,
enabling parallel writes to different parts of the tree with zero contention.

The concurrency model has two tiers:

1. **Immediate mode** — single key operations (upsert, remove) that grab one
   lock, apply one write, and release. No transaction overhead.

2. **Transaction mode** — multi-key operations using optimistic concurrency
   control (OCC). Writes are buffered during the transaction body (no locks
   held). At commit time, locks are acquired in deterministic order, the
   read+write set is validated, and writes are applied atomically.


Locking Granularity
───────────────────

Locks are per-leaf or per-key, not per-root. A fixed-size array of mutexes
is indexed by hash:

    lock_index = hash(key) % NUM_LOCKS     (per-key granularity)
    lock_index = leaf_address % NUM_LOCKS   (per-leaf granularity)

Hash collisions cause false contention, not correctness issues — same principle
as a concurrent hash map's bucket locks. The lock table is sized to minimize
false sharing (e.g., 4096 cache-line-padded mutexes).


Immediate Mode (Single Key Operations)
───────────────────────────────────────

For operations that touch a single key without transaction semantics:

    1. Traverse trie → find leaf (read-only, no locks)
    2. lock = acquire(hash(key) % NUM_LOCKS)
    3. Read current version at key position
    4. global_version.fetch_add(1) → ver_num
    5. Apply write (append to value_node or promote inline → value_node)
    6. Release lock

One lock, one atomic increment, one or two pages written. Maximum throughput
for fire-and-forget writes like event ingestion or logging.


Transaction Mode (Optimistic Concurrency Control)
──────────────────────────────────────────────────

For multi-key operations requiring atomicity and consistency:

Phase 1: Read + Buffer (no locks held)
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌

    snapshot_version = current global_version (reader's snapshot)

    For each operation:
      read(K)  → traverse trie → cache leaf_ptr
              → read value at snapshot_version
              → record (K, leaf_ptr, observed_version) in read set

      write(K, V) → buffer (K, V) in write set
                  → if K not in read set, also read and record its version

    Key optimization: leaf_ptr addresses are cached. The trie structure is
    frozen in MVCC mode, so cached pointers remain valid for the entire
    transaction lifetime. Subsequent reads to the same leaf skip the trie
    traversal entirely — O(1) instead of O(log N).

Phase 2: Acquire Locks (deterministic order)
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌

    touched_keys = read_set ∪ write_set
    sorted_locks = sort_and_dedup(hash(K) % NUM_LOCKS for K in touched_keys)

    for lock_index in sorted_locks:
        acquire(lock_index)

    Sorted acquisition prevents deadlocks. Deduplication avoids self-deadlock
    when multiple keys hash to the same lock.

Phase 3: Validate (conflict detection)
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌

    for each (K, leaf_ptr, observed_version) in read_set ∪ write_set:
        current_version = read version at K using cached leaf_ptr
        if current_version != observed_version:
            ABORT → release all locks, retry transaction

    This detects:
    - Write-write conflicts: another writer updated a key we want to write
    - Write skew: another writer updated a key we only read, but our write
      decision depended on that read

    Validation is O(1) per key — no trie re-traversal, just version comparison
    at cached leaf/value_node addresses.

Phase 4: Commit (apply writes)
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌

    ver_num = global_version.fetch_add(1) + 1

    for each (K, V) in write_set:
        apply_write(K, V, ver_num)
          - Inline value → promote to value_node with 2 entries
          - Existing value_node → COW with appended version entry
          - New key → insert into leaf with inline value at ver_num
          - Delete → append tombstone entry (offset = -2) at ver_num

    alloc_custom_cb(ver_num) → ver_adr
    Publish tree_id = {root, ver_adr} to root slot

Phase 5: Release Locks
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌

    for lock_index in sorted_locks:
        release(lock_index)

    Total critical section: validate + fetch_add + apply writes.
    No I/O, no trie traversal, no allocation of version until success is certain.


Abort Behavior
──────────────

On conflict detection (Phase 3 failure):

    - Release all locks
    - Discard write buffer
    - No version number consumed (fetch_add hasn't happened yet)
    - No tree modifications (writes weren't applied)
    - Retry with a new snapshot

No rollback, no undo log, no wasted version numbers. The transaction body
was entirely speculative — only cached leaf pointers and a private write buffer
existed, both of which are simply discarded.


Write Amplification Comparison
──────────────────────────────

| Operation      | COW (current)    | MVCC immediate | MVCC transaction |
|----------------|------------------|----------------|------------------|
| Update 1 key   | O(log N) pages   | 1-2 pages      | 1-2 pages        |
| Update K keys  | O(K × log N)     | K × (1-2)      | K × (1-2)        |
| Concurrent     | Serialized at    | Parallel to    | Parallel to      |
| writers        | root             | leaf/key       | leaf/key         |

In COW mode, two writers updating unrelated keys both modify the root.
In MVCC mode, they touch only their respective leaves — zero contention
in the tree structure.


Operating Modes
───────────────

All committed trees are shared — the database itself holds a reference and
any thread can snapshot it at any time. The only truly unique trees are
transient in-flight mutations before commit. COW kicks in on an epoch basis
to perform structural maintenance.

| Property           | Within Epoch (MVCC)     | Epoch Boundary (COW)    | Transient (pre-commit) |
|--------------------|-------------------------|-------------------------|------------------------|
| Mutation style     | Value_node append       | COW stale path to root  | In-place, same version |
| Version tracking   | Per-value in value_node | Per-value in value_node | Existing versions      |
| Inner nodes        | Frozen, never modified  | COW'd with maintenance  | Modified in-place      |
| Merge/collapse     | Deferred                | Performed during COW    | Immediate              |
| Write amplification| O(1) pages              | O(log N) for first write| O(1) in-place          |
| Writer concurrency | 1 per key               | 1 per root (brief)      | 1 per tree             |
| Read overhead      | Version check per value | Version check per value | Version check per value|
| count(*)           | Requires iteration      | Requires iteration      | Requires iteration     |
| _epoch field       | Epoch number            | Updated to current      | Epoch number           |

The key trade-off: MVCC sacrifices O(1) subtree counts (`_descendents` is
repurposed as `_epoch`) and efficient subrange cardinality estimation. In
return it gains near-row-level writer parallelism and O(1) write
amplification — no root traversal, no serialization between writers touching
different keys.


Read Path Performance
─────────────────────

MVCC readers never block. No locks on the read path — just follow pointers
and filter by version. The trade-offs:

- Inline values (single version): version table lookup via 1-byte index, trivial overhead
- Value_node lookup: pointer chase + binary search on versions[] — potential
  cache miss for frequently updated keys
- Trie structure may be suboptimal (no merges in shared mode) until
  compaction restructures it
- Tombstones and dead versions occupy space until compaction

For the common case (key with 1 version, small value), the overhead is just
a 1-byte index lookup + 6-byte version compare. The pain concentrates on hot keys with many
versions — which is exactly where write throughput matters most.


Version Lifecycle
─────────────────

    Allocation:
      global_version.fetch_add(1) → ver_num
      alloc_custom_cb(ver_num)    → ver_adr (ref=1, published in root slot)

    Active:
      Each reader snapshot retains ver_adr (ref > 0)
      Reader releases ver_adr when snapshot is dropped

    Retirement:
      ver_adr ref count → 0
      → Version number enters deferred queue → release thread
      → Release thread updates live range map (splits/shrinks ranges)
      → Publishes new snapshot via atomic pointer swap (RCU)

    Garbage collection:
      Compactor reads published live range map snapshot
      → Binary search: version in gap → dead
      → Dead entries stripped from value_nodes during sweep
      → COW copies exclude dead version entries
      → Single-version value_nodes demote to inline in leaf
      → Tombstoned keys with no live versions removed from leaf


Compaction and Version Reclamation
══════════════════════════════════

Live Version Range Map
──────────────────────

Version liveness is tracked by a sorted set of live version ranges, stored
in a SAL-allocated object. Two representations are maintained:

- **Working copy** — flat sorted `lows[]` + `highs[]` arrays with a small
  `pending[]` append buffer. Optimized for merging retired versions.
- **Published snapshot** — `lows[]` rearranged into an implicit B-tree
  layout (BFS order, cacheline-aligned nodes) with a flat `highs[]` array.
  Optimized for SIMD-accelerated search on the hot path.

Typical size: a few dozen ranges, rarely exceeding a few hundred. The design
scales to 87,000+ ranges (1 MB) without degrading lookup performance.

```
live_ranges: [{0,4}, {6,10}, {20,50}]

Versions 0-4:   live
Version 5:      dead (gap)
Versions 6-10:  live
Versions 11-19: dead (gap)
Versions 20-50: live
Versions 51+:   not yet allocated
```

Initial state: `[{0, maxver}]` — all versions live. As version control blocks
are freed (ver_adr refcount → 0), ranges split and gaps form. Contiguous
retirements merge adjacent gaps back together.

Three parties interact with this structure:


Party 1: Releasing Thread (any thread, hot path)
─────────────────────────────────────────────────

When any thread calls release() on a smart_ptr and the ver_adr's custom CB
refcount reaches 0 in final_release:

```
final_release(ver_adr):
    cb = get(ver_adr)
    if !is_custom_cb(cb): return     // not a version CB
    version_num = cb.location_version()
    free_cb(ver_adr)                 // return CB slot to free list

    // Report retired version to release thread
    if thread_local_queue.size < MAX_LOCAL_QUEUE:
        thread_local_queue.push(version_num)
    else:
        // Fallback: update working copy directly (rare)
        lock_guard(range_map_mutex)
        working_copy.pending.push(version_num)
        if working_copy.pending.size >= 16:
            working_copy.merge_pending()
```

The thread-local deferred queue is a simple vector/ring buffer per thread.
No contention — each thread writes only to its own queue. The queue is
bounded (e.g., 256 entries). Under normal operation the release thread
drains it well before it fills.

If the queue fills (release thread fell behind), the releasing thread takes
the `range_map_mutex` and appends directly to the working copy's pending
buffer. This mutex normally has zero contention — the release thread is
the only regular writer, and it holds it only briefly. The overflow fallback
is the backpressure path; the releasing thread always makes forward progress.

**Cost on the hot path:** one push to a thread-local queue. No atomic
operations beyond the refcount decrement that was already happening.
The queue push is a branch + store — effectively free.


Party 2: Release Thread (background, sole writer)
──────────────────────────────────────────────────

The existing SAL release thread owns the working copy and runs a
drain-merge-publish cycle:

```
release_thread_loop:
  while running:
    sleep(publish_interval)   // e.g., 10-100ms

    // 1. Drain all thread-local queues
    retired = []
    for each thread_queue:
        retired.append_all(thread_queue.drain())

    if retired.empty: continue

    // 2. Lock and append to pending buffer
    {
        lock_guard(range_map_mutex)       // brief hold
        working_copy.pending.append(retired)
        working_copy.merge_pending()      // merge if pending >= 16
    }

    // 3. Publish: build read-optimized snapshot, swap
    snapshot = build_btree_snapshot(working_copy)
    old = atomic_exchange(published_ptr, snapshot)
    sal_release(old)                      // free old snapshot
```

Working Copy Layout (write-optimized)
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌

```
┌────────────────────────────────────────────┐
│ sorted_lows[0..N-1]    (uint64_t each)     │
│ sorted_highs[0..N-1]   (uint64_t each)     │
│ num_sorted: N                              │
├────────────────────────────────────────────┤
│ pending[0..15]          (uint64_t each)     │  ← max 16, then merge
│ num_pending: K                             │
└────────────────────────────────────────────┘
```

The `merge_pending` operation:

```
merge_pending():
    if num_pending < 16: return

    sort(pending[0..K-1])

    // Apply each retirement to the sorted arrays in one pass
    for V in pending (sorted):
        i = lower_bound(sorted_lows, V)     // O(log N)
        range = {sorted_lows[i], sorted_highs[i]}

        if V == range.low and V == range.high:
            erase(i)                         // O(N) shift, range removed
        else if V == range.low:
            sorted_lows[i] = V + 1           // O(1) boundary adjust
        else if V == range.high:
            sorted_highs[i] = V - 1          // O(1) boundary adjust
        else:
            // Split: [low..V-1] gap [V+1..high]
            insert(i+1, low=V+1, high=range.high)  // O(N) shift
            sorted_highs[i] = V - 1

    num_pending = 0
```

**Merge frequency:** Every 16 retirements. At 1000 TPS: every ~16ms.
The merge sorts 16 items and does one linear pass through the sorted
arrays. Most retirements are boundary adjustments (O(1)) because versions
retire roughly in order. Splits (O(N) shift) are rare.


Published Snapshot Layout (read-optimized)
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌

The published snapshot rearranges `lows[]` into an implicit B-tree layout
for cache-optimal SIMD search. `highs[]` remains flat (accessed once per
lookup at the final index).

```
┌──────────────────────────────────────────────┐
│ header: { num_ranges, depth }                │
├──────────────────────────────────────────────┤
│ btree_lows[]  — cacheline-aligned B-tree     │
│                                              │
│   Level 0:  1 node  ×  8 keys =     8 keys  │  1 cacheline
│   Level 1:  9 nodes ×  8 keys =    72 keys  │  9 cachelines
│   Level 2: 81 nodes ×  8 keys =   648 keys  │  81 cachelines
│   Level 3: 729 nodes × 8 keys = 5,832 keys  │  729 cachelines
│   Level 4: ...                               │
│                                              │
│   Branching factor B = 9 (8 keys per node    │
│   create 9 child slots)                      │
├──────────────────────────────────────────────┤
│ flat_highs[0..N-1]  (uint64_t each)          │  ← one access per lookup
└──────────────────────────────────────────────┘
```

Each B-tree node occupies exactly one cacheline (8 × uint64_t = 64 bytes).
Search descends one level per cacheline load, using SIMD to find the child
slot within each node:

```
is_dead(V, snapshot):
    node_idx = 0
    for level in 0..snapshot.depth:
        // Load 8 keys from one cacheline
        keys = load_256x2(snapshot.btree_lows + node_idx * 8)

        // AVX2: compare V against all 8 keys simultaneously
        target = broadcast_epi64(V)
        gt_mask = cmpgt_epi64(keys, target)    // which keys > V?
        child = count_trailing_zeros(movemask(gt_mask))  // 0..8

        node_idx = node_idx * 9 + child + 1    // descend

    // Convert leaf node_idx back to sorted index
    sorted_idx = leaf_to_sorted(node_idx)
    if sorted_idx < 0: return true             // below all ranges
    return V > snapshot.flat_highs[sorted_idx]  // one final access
```

**Lookup cost:**

| Num ranges | B-tree depth | Cacheline loads | SIMD compares |
|------------|-------------|-----------------|---------------|
| ≤ 8        | 1           | 1 + 1           | 1             |
| ≤ 80       | 2           | 2 + 1           | 2             |
| ≤ 728      | 3           | 3 + 1           | 3             |
| ≤ 6,560    | 4           | 4 + 1           | 4             |
| ≤ 59,048   | 5           | 5 + 1           | 5             |

+1 is the single access to `flat_highs[]`. For the typical case (< 100
ranges): 2 cacheline loads + 2 SIMD compares. For the extreme case (87K
ranges): 5 + 1 = 6 cacheline loads. Both fast enough for every COW to
include opportunistic cleanup without impacting the hot write path.

**Build cost:** The `build_btree_snapshot` step is an O(N) permutation
of the sorted lows into BFS order, plus a memcpy of highs. Done once per
publish cycle (10-100ms). For 100 ranges ≈ 2 KB total — trivial.


Party 3: Writer/Compactor Scanning Nodes (opportunistic cleanup)
────────────────────────────────────────────────────────────────

Every writer COW is a cleanup opportunity. The liveness check is cheap
enough (2-6 cacheline loads) to run on every COW without measurably
impacting the hot path.

```
cow_with_version_filter(node):
    // 1. Load published snapshot (one atomic load)
    snapshot = atomic_load(published_ptr)

    // 2. Copy node, filtering dead entries
    for each entry in node:
        V = entry.version
        if is_dead(V, snapshot):       // B-tree search: 2-6 cachelines
            skip                       // don't copy to new node
        else:
            copy entry to new node

    // 3. Update CB location to point to filtered copy
    update_cb_location(node.address, new_copy.location)
```

This applies to:
- Value_node COW (appending a new version) — filter old dead versions
- Leaf COW (insert/update/remove) — filter dead inline version entries
- Compactor COW (segment relocation) — filter during the copy

**Staleness tolerance:** The published snapshot may be one cycle (10-100ms)
stale. Dead entries that survive one extra COW cycle get cleaned up on the
next COW. No correctness issue — just slightly delayed space reclamation.

**Cost per node entry:** 2-6 cacheline loads for the B-tree search. The
snapshot's btree_lows root node (level 0) stays hot in L1 across all
entries in the same node being filtered. Effective cost for N entries in
one node: ~1 cold root load + N × (1-5 deeper loads, mostly L2 hits).


Interaction Timeline (Example)
──────────────────────────────

```
t=0    Writer commits version 42 → alloc_custom_cb(42), publish root
t=1    Reader takes snapshot → retains ver_adr for v42 (ref=2: root + reader)
t=5    Writer commits version 43 → new root published, old root released
       Old root's v42 ref decrements (ref=1: reader still holds it)
t=10   Reader drops snapshot → releases ver_adr for v42 (ref=0)
       final_release: push v42 to thread-local queue
t=15   Release thread wakes, drains queue, appends to pending
       pending reaches 16 → merge_pending into sorted arrays
       Builds B-tree snapshot, publishes via atomic swap
t=20   Writer COWs a value_node (normal insert for a different key)
       Loads published snapshot, filters entries during COW
       is_dead(42) → B-tree search → true (in gap between ranges)
       Entry at v42 excluded from new copy — space reclaimed for free
```

Opportunistic Compaction
────────────────────────

When the compactor encounters any value_node during its normal sweep (MFU
eviction, segment compaction, read-bit decay), it loads the published live
range map snapshot and checks each entry:

```
compact_value_node(vnode):
    range_map = atomic_load(published_live_ranges)
    live_entries = []
    for i in 0..vnode.num_versions:
        if is_dead(vnode.versions[i], range_map):
            continue    // version falls in a gap → dead
        live_entries.append(i)

    if live_entries.size == vnode.num_versions:
        return          // nothing to compact

    if live_entries.empty:
        // All versions dead — key is fully dead
        nullify_cb(vnode)
        return

    // COW the value_node with only live entries
    new_vnode = cow_copy(vnode, live_entries)
    update_cb_location(vnode.address, new_vnode.location)
```

Similarly for leaves — the compactor checks each entry's inline version
against the range map and COWs the leaf excluding dead entries.

If a value_node has escalated to tier 3/4 (child pointers), the compactor
follows the children and checks them recursively.

This creates natural prioritization:
- Hot data (frequently accessed, in MFU cache) → compactor encounters it
  often → stale versions stripped quickly
- Warm data (active segments) → cleaned during normal segment sweeps
- Cold data (untouched segments) → checked when compactor eventually
  reaches that segment

The system is self-tuning: the more frequently data is accessed, the faster
stale versions are reclaimed. No separate GC pass, no synchronization on
the write path.

Control Block Nullification (The Orphan Problem)
─────────────────────────────────────────────────

When a value_node's data is reclaimed (all versions dead, segment space
freed), the control block cannot be freed because the leaf still holds a
ptr_address to it (refcount > 0). There is no parent link to walk up and
remove the leaf's reference.

Solution: the compactor sets location = null on the control block. This is
a tombstone at the CB level:

- The CB still exists (refcount > 0, leaf references it)
- But its location is null — data has been reclaimed
- Anyone following this ptr_address checks for null location before
  dereferencing
- The CB lives as a tombstone until the next epoch-boundary COW copies the
  leaf, drops the null-CB branch, and the CB's refcount → 0

This is lazy cleanup: data is reclaimed eagerly (segment space freed), CB
metadata is cleaned up lazily (freed when the leaf is COW'd at epoch
boundary and the dead branch is dropped)


Structural Maintenance via CB Indirection
═════════════════════════════════════════

CB Indirection Eliminates Root Cascade
──────────────────────────────────────

The critical insight: all pointers in the tree are ptr_addresses (control
block identities), not direct memory addresses. When a node is modified:

1. Allocate new data with the modification
2. Atomically update the node's CB location to point to the new data
3. The ptr_address (CB identity) is unchanged
4. Parent still holds the same ptr_address → no parent update needed
5. No cascade to root. Ever.

This applies to ALL modifications — value updates, leaf insert/remove,
inner node restructuring. The parent doesn't know or care that its child's
data changed location. It still holds the same CB identity.

```
Today's COW:
  modify leaf → new ptr_address → update parent → COW parent if RO
  → ... → update root = O(log N) pages dirtied

CB indirection:
  modify leaf → allocate new data → update CB location (atomic)
  = O(1), one CB write + one data allocation
```

The control block zone is always writable (never mprotect'd to RO), so the
CB location update never triggers further COW. This is a win for ALL trees —
unique or shared, MVCC or not.

Structural Maintenance Roles
────────────────────────────

The compactor and writer have complementary roles. The compactor cleans
individual nodes; tree restructuring (merging, collapsing) requires parent
context that only a tree traversal provides.

**Compactor** — walks segments, encounters individual nodes. No parent
context, no sibling pointers. Can:
- Strip dead version entries from value_nodes (COW excluding dead entries,
  update CB location, using live range map for liveness checks)
- Strip dead key entries from leaves (COW excluding entries whose value_node
  CB is null or whose inline version is dead, update CB location)
- Cannot merge siblings (no sibling pointers)
- Cannot collapse inner nodes (no parent pointers)
- Cannot demote single-version value_nodes to leaf inline (no parent leaf)

**Writer** — traverses root to leaf, has the full path cached. Can
optionally merge sparse sibling leaves or collapse single-child inner nodes
during the traversal. This is an optimization, not a requirement (see below).

**Defrag run** — traverses the entire tree from root, with full parent
context at every level. Can perform all structural maintenance: merge sparse
siblings, collapse inner nodes, demote value_nodes, strip dead entries.
The definitive cleanup mechanism.


Leaf Condensation Strategy
──────────────────────────

Tree restructuring (merging sparse siblings, collapsing inner nodes) is
NOT required for correctness. A sparse tree returns correct results — it
just wastes space and degrades read performance. Three tiers handle cleanup:

**Tier 1: Remove-path merge (optional, eager, online)**

After a remove, the writer has the parent cached and the leaf is definitely
smaller. It can check left and right siblings for merge:

```
after_remove(leaf, parent):
  if leaf.live_entries < merge_threshold:
    // Check left sibling: 2 cache lines (CB + header)
    // Check right sibling: 2 cache lines (CB + header)
    // Total: 4 cache line misses per remove
    for sibling in [left_sibling, right_sibling]:
      if sibling.is_leaf and fits_in_one_leaf(leaf, sibling):
        combined = merge(leaf, sibling)
        update leaf CB location → combined data
        COW parent (remove sibling slot) → update parent CB location
        // may cascade: check next sibling of combined leaf
```

Two CB location updates per merge. No cascade beyond parent. But 4 cache
line misses on every remove. This is an optimization trade-off — whether
to pay 4 cache misses per remove to keep the tree tight, or defer to the
defrag run. Insert/update never checks siblings (leaf grew or stayed same).

**Tier 2: Defrag run (background, online)**

A background thread traverses the tree from root, with full parent context.
At each inner node, it inspects children and merges sparse siblings,
collapses single-child nodes, and strips dead entries. This handles:
- Cold regions never touched by writers
- Accumulated structural debt from many removes without Tier 1
- Subtrees orphaned by compactor CB nullification

The defrag run can iterate old snapshots from prior epochs. It operates
on the current committed tree, using CB location updates (no root cascade).
It can run at low priority during quiet periods or be triggered when
structural debt metrics exceed a threshold.

**Tier 3: Offline compaction**

Full tree rebuild. Always available as the nuclear option. Produces an
optimally packed tree with no dead entries, no sparse leaves, no structural
debt.

Without Tier 1 (no remove-path merging), the tree is correct and performant
for writes. It accumulates structural debt proportionally to removes. The
defrag run (Tier 2) cleans it up periodically. The choice of whether to
merge on remove is a tuning decision — workloads with few removes may
never need it.


Epoch Storage in Inner Nodes
────────────────────────────

Every inner node stores the epoch number it was last touched. This replaces
the `_descendents` field, which is not maintainable in MVCC mode (updating
it would require versioning inner nodes — concurrent writers change key
counts without touching inner nodes).

```cpp
// Before (COW-only):
uint64_t _descendents : 39;  // 500 billion keys max

// After (MVCC):
uint64_t _epoch : 39;        // epoch number when node was last COW'd
```

Both `inner_prefix_node` and `inner_node` have a 39-bit `_descendents` field
in their packed bitfield layout. The repurposing is zero-cost — no change in
node size or alignment.

The epoch is derived from the global version number:

    epoch = global_version / N

where N is a configurable epoch size (minimum N ≥ 4).

Epoch number sizing: `global_version` is 41 bits (2.2 trillion versions).
The epoch number is `global_version / N`. With minimum N ≥ 4, the epoch
number requires at most 39 bits (41 - log2(4) = 39). Practical N values
are much larger, so epoch numbers are well within range:

| Min N | Epoch bits needed | Headroom in 39 bits |
|-------|-------------------|---------------------|
| 4     | 39                | exact fit           |
| 16    | 37                | 2 bits spare        |
| 256   | 33                | 6 bits spare        |
| 1024  | 31                | 8 bits spare        |

The epoch stamp serves as metadata for the defrag run and compactor —
it tells them the age of the node for prioritization. It is NOT used as
a staleness gate for writers (writers always do MVCC O(1) writes regardless
of epoch).

Reclamation via Root Release Cascade
────────────────────────────────────

When the last reader holding a snapshot of an old root releases it, the
root's refcount reaches zero, triggering a recursive release:

```
old_root refcount → 0
  └→ recursive_release walks the old tree
       ├→ old inner nodes freed (unreachable from current root)
       ├→ old leaves freed
       ├→ old value_nodes freed (stale versions die with them)
       └→ entire subtrees reclaimed in one cascade
```

This handles bulk structural cleanup. The existing SAL ref-counting
infrastructure finds and frees all unreachable nodes automatically.

Writer Safety
─────────────

Writers do not need path locks. CB identity invariant ensures safety:

- Value_node CBs are shared across tree structure versions. A writer's
  atomic update to a CB location is visible through any tree version
  that references that CB.

- When a writer's cached leaf pointer refers to a CB with null location
  (freed by compactor or cascade), the writer detects this after
  acquiring the key lock and re-traverses from root.

- Writers operating on different leaves under different parents have
  zero contention.


