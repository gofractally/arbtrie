# MVCC Version Maintenance Design

This document defines the cleanup model for PsiTri now that MVCC is the normal
write model. It supersedes the older "COW mode versus MVCC mode" language in
`docs/MVCC.md` for epoch policy, old-version cleanup, and the relationship
between public `upsert/remove` APIs and internal explicit-version write helpers.

## Core Model

MVCC has three different kinds of version-like state. They must not be treated
as the same thing.

| Name | Meaning |
|------|---------|
| `write_version` | The logical transaction version for user-visible writes. Values, tombstones, and branch-creation records are tagged with this. |
| `root_version` | The exact committed or transaction-local version that a root/tree reads as. |
| `last_unique_version` | Structural metadata on inner nodes: the root version at which this physical node was last made unique/refreshed for a root path. |

`last_unique_version` replaces the current `_epoch` concept. The value is not a
separate epoch counter. It lives in the same version-token space as root/value
versions.

The epoch boundary remains a coarse refresh trigger:

```cpp
epoch_base(root_version) =
    (root_version / epoch_interval) * epoch_interval;
```

When descending through an inner node:

```text
if node.last_unique_version is older than epoch_base(root_version):
    the node must be made unique/refreshed before descending farther
```

After the node is physically rewritten or otherwise refreshed for the current
root path:

```text
node.last_unique_version = root_version
```

That stamp means: "this structural node has been refreshed for the current
root's visibility floor." It does not create a logical write version by itself.

## COW Is Physical

Copy-on-write is a physical ownership/layout operation. It is not necessarily a
logical version creation event.

A single write transaction has exactly one logical `write_version`. Physical
rewrites performed inside that transaction reuse that version where they change
reader-visible history. Rewrites that only move, compact, expand, split, or
refresh nodes do not allocate new logical versions.

Physical COW can happen for several reasons:

| Reason | Creates logical version? | Notes |
|--------|--------------------------|-------|
| Make a shared node unique | No | Enables mutation/compaction for the current root path. |
| Refresh a stale `last_unique_version` | No | Stamps structural node with `root_version`. |
| Leaf insert/update/remove | Yes, once per transaction/write | The key/value history gets `write_version`. |
| Split/merge/collapse/prefix rewrite | No by itself | Parent notification occurs only when child address set changes. |
| Compactor or MFU relocation | No | May normalize histories while moving data. |

## Prune Floors

Old-version cleanup is driven by a `prune_floor`:

```text
prune_floor = oldest version that must still be readable for this node rewrite
```

The rewrite must preserve the state visible at `prune_floor`, but it does not
need to preserve older version labels.

For a single key history:

```text
history:
  v10 = A
  v20 = B
  v30 = C
  v40 = D

prune_floor = v25

after rewrite:
  v25 = B
  v30 = C
  v40 = D
```

Mechanically:

```text
it = first entry after prune_floor in retained version order
floor_state = predecessor(it), if any

drop entries before floor_state
if floor_state exists:
    emit floor_state's value/tombstone at version prune_floor
emit entries from it onward unchanged
```

For tombstones:

```text
history:
  v10 = A
  v20 = tombstone
  v40 = B

prune_floor = v25

after rewrite:
  v25 = tombstone
  v40 = B
```

If the key did not exist at `prune_floor`, no floor entry is emitted. If a key is
tombstoned at `prune_floor` and has no later recreation, a leaf rewrite may drop
the branch entirely.

## Who Cleans Old Versions

Cleanup is not owned by one API. Any physical rewrite can normalize history.

| Actor | When | Prune floor |
|-------|------|-------------|
| Write path | `insert`, `update`, `upsert`, `remove`, split/merge/collapse, stale-epoch refresh | `root_version` when the path from root to rewritten node has been made unique; otherwise the global oldest retained version. |
| Defrag tree walk | Explicit/background traversal with parent context | `root_version` for unique paths; otherwise global oldest retained version. |
| Segment compactor | Moving nodes out of dirty/cold segments | Global oldest retained version unless it can prove unique-to-root ownership. |
| MFU active visitor | Moving/promoting hot nodes | Same as compactor: apply the same rewrite filter while moving the node. |
| Version release thread | Does not rewrite nodes | Publishes retained-version state used by writers/compactors. |

The important implementation rule:

```text
Do not copy a node and then prune it in a second pass.
Pruning is a filter applied while copying values/branches for the rewrite.
```

A node rewrite is one composed operation:

```text
source node
  -> structural edit filter
  -> version-normalization filter
  -> layout policy
  -> destination node
```

The structural edit may be insert/update/remove/split/merge/prefix rewrite. The
version-normalization filter applies `prune_floor`. The layout policy chooses
whether the destination is hot-expanded or cold-compacted.

### Active and Passive Rewrites

Rewrite methods split into two families. Do not hide this behind a boolean mode
or broad enum; the method name should say whether the caller is allowed to make
persistent side effects.

| Family | Caller | Race behavior | Dropped references |
|--------|--------|---------------|--------------------|
| Active rewrite | Transaction/write path, defrag path with explicit ownership context | Owns the state transition and publishes through the normal writer protocol. | May queue or call `release_node` directly as it removes subtree/version references from the published state. |
| Passive rewrite | Segment compactor, MFU promotion, opportunistic relocation | Copies candidate bytes and yields if another actor releases or moves the object first. | Must append dropped subtree/version addresses to a caller-owned release vector and perform no release side effects while copying. |

This implies separate operation names, for example:

```text
active_copy_to(...)
active_compact_to(...)

passive_copy_to(..., pending_releases)
passive_compact_to(..., pending_releases)
```

The exact names can follow the local C++ style, but the contract must remain
visible at the call site. Active methods may retire references as part of the
state change. Passive methods only describe what would need to be retired if the
candidate move wins.

### Relocation Side Effects

Compactor and MFU relocation are optimistic. They copy from an old physical
location, publish the new physical location with a control-block CAS, and lose
silently when another thread releases the object or moves it first. Therefore a
relocation rewrite must be side-effect-free until the move wins.

The relocation protocol is:

```text
copy old object into candidate new object
collect any child/version references omitted by pruning
do not retain or release collected references while copying

if CAS(old_location -> new_location) fails:
    discard candidate bytes only
    discard collected references without releasing them

if CAS(old_location -> new_location) succeeds:
    retire the old physical bytes through the normal read-lock grace path
    only then release references that existed only in the stale old bytes
```

This rule matters for subtree and version references. Pruning data-only value
history has no persistent side effects and is safe in a losing relocation
attempt. Pruning subtree entries, chained value-node entries, or any other
reference-owning payload requires a post-success retire action. The compactor
does not own a dropped reference merely because it copied a node; it owns that
reference only after it has successfully deleted the stale reference from the
published object state and the old physical bytes are being retired.

## Leaf Rewrite Policy

Leaf nodes are special on the hot write path.

If descent proves the path from the transaction root to the edited leaf is
unique, the edited key should finish with only the current value visible from
that leaf. Older readers are on older roots, so carrying a private value-node
history in the unique leaf wastes memory and defeats the hot-update path.

When a write must physically rewrite or COW a leaf for active editing:

```text
allocate max_leaf_size
copy once with edit + prune_floor normalization
leave spare capacity for future edits while the leaf remains unique/hot
```

The compactor, defragger, and MFU visitor may instead choose:

```text
copy_and_compact
```

for idle, cold, or shared leaves. This packs storage tightly and may demote
single-version value nodes back inline when parent context is available.

This means `can_apply(edit)` is not enough for rewrite planning. The planner
needs to answer:

```text
Can edit + prune normalization fit in the chosen destination policy?
```

For the hot write path, the chosen policy is normally "expanded max leaf".

## Circular Version Tokens

Persisted version fields are bounded. Current relevant widths are:

| Field | Current width |
|-------|---------------|
| SAL custom control-block value | 41 bits (`control_block_data::cacheline_offset`) |
| Inner node `_epoch` field | 39 bits |
| Leaf `version48` table entries | 48 bits |
| Value-node packed entries | 48 bits |

If inner nodes store `last_unique_version` in the current 39-bit field, any
comparison involving that field must use a 39-bit version token. Wider fields may
store more bits for diagnostics or future layout changes, but correctness must
not depend on bits that are not present in every compared field.

Let:

```cpp
VERSION_BITS = 39; // until inner node layout changes
VERSION_MOD  = 1ull << VERSION_BITS;
VERSION_MASK = VERSION_MOD - 1;
```

The system invariant is:

```text
newest_version - oldest_retained_version < VERSION_MOD
```

This bounds the live MVCC window, not the database lifetime. The global
transaction counter may continue forever if persisted/recovery metadata can
recover an absolute generation, but node-local comparisons use masked version
tokens inside the active window.

Avoid unanchored comparisons such as:

```cpp
entry_version <= read_version
node_epoch < current_epoch
```

Those are wrong across wrap. Compare by distance from a known anchor.

For entries retained in `[floor, newest]`, branchless helpers can be shaped as:

```cpp
constexpr uint64_t version_mask = (1ull << VERSION_BITS) - 1;

inline uint64_t version_token(uint64_t v) noexcept
{
   return v & version_mask;
}

inline uint64_t version_distance(uint64_t from, uint64_t to) noexcept
{
   return (to - from) & version_mask;
}

inline bool version_visible_at(uint64_t entry,
                               uint64_t read,
                               uint64_t floor) noexcept
{
   return version_distance(floor, entry) <=
          version_distance(floor, read);
}

inline bool version_older_than(uint64_t a,
                               uint64_t b,
                               uint64_t newest) noexcept
{
   return version_distance(a, newest) >
          version_distance(b, newest);
}

inline bool needs_unique_refresh(uint64_t last_unique_version,
                                 uint64_t epoch_base,
                                 uint64_t root_version) noexcept
{
   return version_older_than(last_unique_version,
                             epoch_base,
                             root_version);
}
```

This uses subtraction, mask, and compare. The compare can compile without a
branch. More importantly, it uses the full token ring as the live-window bound;
it does not rely on arbitrary pairwise signed comparisons that cut the usable
range in half.

Value histories should be ordered relative to a stable base, normally their
first retained entry or the rewrite's `prune_floor`:

```text
entry_order = version_distance(history_base, entry_version)
read_order  = version_distance(history_base, read_version)
visible entry = greatest entry_order <= read_order
```

The current linear scans and binary searches that use raw numeric `<=` are
legacy and must be replaced with anchored token comparisons.

## Epoch Interval And Refcount Pressure

The epoch interval is tied to refcount capacity because it controls how many
versions can share the same physical root path before refresh creates a new
unique path.

The hard correctness bound is:

```text
active_version_span < 2^VERSION_BITS
```

where:

```text
active_version_span = newest_version - oldest_retained_version
```

The operational refcount bound is workload dependent:

```text
max retained handles sharing one physical object < control_block::max_ref_count
```

Worst case, if an application retains one tree/snapshot for every transaction
within an epoch, a root can accumulate roughly `epoch_interval` references before
the next epoch refresh creates a new physical root. In that worst case:

```text
epoch_interval <= max_ref_count - safety_margin
```

Cold children may be shared by several epoch roots. If the live retained window
spans many epochs, their refcount pressure is roughly:

```text
retained_epoch_roots_that_share_child ~= active_version_span / epoch_interval
```

So `epoch_interval` has both an upper and lower pressure:

```text
too large: many snapshots share one physical root
too small: many epoch roots share unchanged cold children
```

The implementation should track high-water refcounts and retained-window span so
the configured epoch interval can be validated instead of guessed.

## Public API Contract

The public API has only normal write verbs:

```cpp
insert
update
upsert
remove
```

"MVCC" is the storage model, not a separate user operation. Public callers must
not see or choose a separate MVCC write path. The engine may still expose
internal helpers that accept an already allocated write version so tests can
exercise exact-version behavior.

A normal write pipeline should:

```text
1. allocate or receive the write_version
2. descend with root_version, epoch_base, and prune_floor context
3. refresh stale structural nodes before descending through them
4. try same-address value/leaf mutation where valid
5. rewrite with edit + prune filters when physical COW is needed
6. report parent effects only for real structural address-set changes
```

## Implementation Checklist

Checked rows are backed by focused tests or an audit command. Open rows are not
complete; they remain here so future work cannot hide in prose.

| Status | Area | Evidence |
|--------|------|----------|
| [x] | Public `write_session` exposes `upsert/remove`, not explicit-version write helpers. | `[public-api]` has compile-time assertions for `upsert/remove` and against `upsert_at_version/remove_at_version`. |
| [x] | User-facing `mvcc_upsert/mvcc_remove` names are gone. | The legacy-name audit returns no matches under psitri headers/tests. |
| [x] | Internal exact-version tests use explicit-version names, not public-looking MVCC verbs. | Internal tests call `tree_context::upsert_at_version`, `remove_at_version`, `try_upsert_at_version`, and `try_remove_at_version`. |
| [x] | `_epoch` has been normalized to `last_unique_version` for inner-node structural freshness. | `[epoch]` covers insert, fallback, replace-branch, and collapse stamping. |
| [x] | `current_epoch()` is now version-space `current_epoch_base()`. | `database_state::current_epoch_base()` returns `(global_version / interval) * interval`; callers pass both epoch base and root version. |
| [x] | Stale inner nodes are refreshed before descent and stamped with `root_version`. | `[epoch]`, `[tree_ops][collapse][phase2]`, and `[tree_ops][shared][collapse]`. |
| [x] | Circular version-token comparisons are centralized. | `version_compare.hpp`; `[mvcc][wrap]` and `[value_node][mvcc][wrap]`. |
| [x] | Value-node `prune_floor` copies preserve predecessor, exact, tombstone, absent, and wrap cases. | `[value_node][mvcc][prune]`. |
| [x] | Hot leaf rewrites allocate max leaf size and preserve branch creation versions. | `[leaf_node][transform][mvcc]`. |
| [x] | Unique transaction updates collapse private history and demote value nodes back inline when possible. | `[per_txn][phaseA]` and `[per_txn][phaseD]`. |
| [x] | Physical COW/transaction maintenance does not allocate extra logical versions. | `[per_txn][phaseB]` and `[per_txn][phaseC]`. |
| [x] | Same-root immediate writes preserve snapshot-visible history until the version is reclaimable. | `[mvcc_write]`, `[mvcc_read]`, and `[mvcc_snapshot][stress]`. |
| [x] | Explicit-version slow helpers do not carry a duplicated structural fallback traversal. | `upsert_at_version()` / `remove_at_version()` try the stripe-safe exact-version path, then delegate to the normal structural writer on fallback; covered by `[mvcc_write]` and `[per_txn]`. |
| [x] | `live_range_map` publishes a conservative retained floor. | `snapshot::oldest_retained_floor()` reports the first version not proven dead by a contiguous dead prefix; `[live_range_map][floor]`. |
| [x] | Leaf rewrite applies value-history normalization during the leaf rebuild itself. | `leaf_rewrite_plan` preallocates normalized value nodes and leaf copy constructors consume `op::leaf_value_rewrite`; `normalize_leaf_value_nodes()` has been removed. Covered by `[per_txn][phaseA][cow][leaf_rewrite]`, `[leaf_node][transform]`, and `[per_txn]`. |
| [x] | Rewrite contexts consume retained floors where semantically safe. | `defrag_leaf()` uses `snapshot::oldest_retained_floor()` with `value_node::prune_floor_policy` for data and unchained subtree value nodes. Chained value nodes are skipped until their child-retain policy is explicit. Covered by `[defrag][floor]`, `[defrag]`, `[subtree]`, and `[live_range_map]`. |
| [x] | SAL object dispatch is allocator-instance scoped, not process-global. | `sal::object_type_ops` is installed per allocator; PsiTri owns per-database node ops objects, so future compactor/MFU relocation policy can access database-owned version state through `this` instead of SAL globals. |
| [x] | SAL relocation call sites distinguish active owner rewrites from passive opportunistic rewrites. | Writer COW calls `active_copy_to()`. Compactor/MFU relocation calls `passive_compact_to()` with caller-owned pending-release storage and discards the candidate on overflow or CAS loss. |
| [x] | Passive relocation prunes unchained value-node histories and accounts for dropped subtree references. | `psitri_value_node_ops::passive_compact_to()` applies `snapshot::oldest_retained_floor()` to non-flat, unchained value nodes. Dropped subtree `tree_id` root/version refs are appended to the pending-release list and released only if relocation wins. Covered by `[value_node][mvcc][prune][passive]` and `[value_node][mvcc][prune][passive][subtree]`. |
| [x] | Active same-CB rewrites release subtree refs omitted by dead-entry and prune-floor rewrites. | `value_node` reports dropped `tree_id` refs for dead-entry strip, prune-floor normalization, and replace-last tombstone. `tree_context` releases them after successful same-CB `mvcc_realloc`. Covered by `[value_node][mvcc][prune][subtree]`, `[value_node][mvcc][subtree]`, `[defrag]`, `[mvcc_write]`, and `[subtree]`. |
| [ ] | Chained value-node pruning should release omitted `next_ptr` children only after the winning rewrite. | Base-node data/subtree entries are handled. `num_next() != 0` histories are still copied conservatively because pruning them requires choosing the floor-visible child entry and retiring omitted child value_nodes with the same active/passive ownership rules. |

## Test Checklist

Run these before checking off the completed rows above:

```sh
cmake --build build --target psitri-tests -j 8
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[public-api]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[leaf_node][transform]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[value_node]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[value_node][mvcc][prune][passive]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[value_node][subtree]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[live_range_map]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[defrag]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[subtree]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[defrag][floor]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[epoch]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[tree_ops][collapse][phase2]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[tree_ops][shared][collapse]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[mvcc_write]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[mvcc_read]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[per_txn]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[per_txn][phaseA][cow][leaf_rewrite]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[mvcc_lifecycle]"
PSITRI_FROM_RUN_TESTS=1 build/bin/psitri-tests "[cow_prune]"
rg "mvcc_upsert|mvcc_remove|try_mvcc|append_mvcc|mvcc_find_target" \
  libraries/psitri/include/psitri libraries/psitri/tests
```
