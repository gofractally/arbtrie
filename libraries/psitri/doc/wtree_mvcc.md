# wtree MVCC design

Inline, version-packed MVCC for `psitri::wtree`. The guiding idea: **pack
multiple committed versions inside a node** (so most writes don't allocate a new
node per version), and fall back to clone-a-fresh-node + prune only when the
inline budget overflows. The layout is **reader-friendly / writer-unfriendly**,
which is fine: node-level COW already rebuilds-with-changes on every write, so
the reconstruction work folds into the copy we pay anyway.

Branch: `wtree-mvcc` off `wtree-baseline` (clean stable+wtree line). Depends on
the SAL reserved-ptr-range (cherry-pick `29e7f48` from art2). wtree-internal —
does NOT require the `smart_ptr`-drops-`ver` SAL cleanup (that's a separate
unified effort).

---

## 0. Reserved ptr_address sentinels (SAL — DONE on art2, port forward)

Top 16 of the ptr_address space are reserved (`first_reserved_ptr_address =
0xFFFFFFF0`, `is_reserved_ptr_address()`). The alloc-hint path and the
deref/prefetch helpers treat the whole range as a non-address, so a pointer
column that doubles as an alloc hint can safely hold sentinels.

Three sentinels in use for the tree:

| value | name | meaning |
|---|---|---|
| `0xFFFFFFFF` | `kNoChild` | intentional empty divider — *valid slot, create child on demand* |
| `0xFFFFFFFE` | `kSentinelChildAddr` | transient `upsert_result.right` "insert empty slot" marker |
| `0xFFFFFFFD` | `kTombstone` | deleted branch — *treated as non-existent → route to prior divider* |

**Three-way routing** in `inner_find_branch`/insert on the landed slot:
real address → descend; `kNoChild` → find=miss / insert=create-here; `kTombstone`
→ fall back to the prior divider's child (range folds left; do **not** create
here). This is what lets a delete collapse into its left neighbor with no
restructuring, distinct from the create-on-demand gap.

---

## 1. Value type encoding

Per-value 1-byte tag, stolen from a 4-bit type:

```
[ type:3 | has_alt:1 ][ value_ver:4 ... ]   // see value_region
```

`type` (3 bits, 5 used):
- `inline` — value bytes stored inline (`value[value_len]`)
- `null`   — present key, empty value (distinct from absent)
- `tomb`   — MVCC delete marker (readers at/after the version skip it)
- `big`    — `value[]` is a 4-byte `ptr_address` to an out-of-line value
- `tree`   — `value[]` is a 4-byte `ptr_address` to a subtree root **(single
             ptr — not a `tree_id{root,ver}` pair)**

See `big_values.md` for the out-of-line `big` design (immutable ≤ 2 MB blob now;
filesystem-style offset+length + chunking deferred).

`big`/`tree` are a **single `ptr_address`**. Versioned subtrees go through a
`version_node {version#, ptr_address→root}` reached by that one ptr (unversioned
= ptr straight to the root). This is the shape the `smart_ptr` cleanup converges
on (version is a first-class node, not smuggled in the pointer).

`has_alt` = this entry has an older-versions chain (see §3).

---

## 2. Leaf value_region v2

```c
struct value_region {
    uint8_t  suffix_words;                 // remaining key words below the stem
    uint8_t  type_alt;                     // [type:3 | has_alt:1 | value_ver:4? ] -- see note
    uint8_t  value_len;                    // PER-ENTRY now (0..255)
    // if has_alt: uint16_t alt_off;       // end-relative offset of the alt chain
    u32      suffix[suffix_words];
    u8       value[value_len];             // big/tree: value_len==4, value==ptr_address
};
```

Note: pack `type:3 | has_alt:1 | value_ver:4` into one byte (`value_ver` is the
4-bit index into the leaf version table, §4). Header grows 1 B → 3 B (+2 B when
`has_alt`).

**Drops the global `wtree::value_len_`** (which was unpersisted + forced
fixed-width). Each entry self-describes; varlen values fall out for free. Every
`leaf_find`/`leaf_remove`/`leaf_merge_` call that threaded `value_len_` reads the
entry's own `value_len`.

---

## 3. Alt-version chain (older versions of one key)

`alt_off` → a **contiguous run** of older versions for that key:
```
[ type/ver | value_len | data ] [ type/ver | value_len | data ] ... [ type==end_of_versions ]
```
Walked linearly by `value_len`; terminated by a `type == end_of_versions` flag
(the oldest version present in the page for that key). Newest→oldest.

Contiguity holds because **every write is a COW-rebuild** — versioning never does
an in-place append between rebuilds (which would interleave keys). The rebuild
lays each key's versions contiguous and prunes below the floor.

---

## 4. Leaf version table

Up to **15** distinct global versions per leaf, `u64` each, in the header/tail.
`value_ver:4` is the index (1..15); **`0` = "single-version / current only"** so
the common never-updated key uses no slot, `has_alt=0`, and the hot read never
touches the table or a chain.

**Read:**
- current `ver` visible to the reader's snapshot → return current (no table touch).
- else: prefetch the 15-slot table (≈1 line), find the largest version ≤ snapshot
  → its index, then walk the alt chain newest→oldest to the matching `ver`.

**Reader = active writer** (reading current): skip the table/meta entirely — zero
MVCC cost on the hot path.

**Overflow (16th distinct version):** clone a fresh-id leaf (hint from parent)
dropping all version data (keeps current; old versions remain in the old leaf,
held by old roots) → the parent must now point to {old, new} → forces inner
branch versioning (§5).

---

## 5. Inner node MVCC — versioned branches

A branch becomes `ptr_address + version + has_history`. **Keep column layout**
(the children ptr_address array is used as alloc hints, so it must stay a
contiguous column). Add a third parallel column:

```
stems[]       u32           (column)
children[]    ptr_address   (column — current child; doubles as alloc hint)
branch_meta[] u16           (column — [ value_ver:4 | history_off:12 ])
```
`history_off` (12 bits) = end-relative offset into the inner's tail history heap;
`history_off == 0` = no history (current only). 12 bits needs a stride to reach a
64 KB page — `off × 16` (history entries 16-byte aligned) — **TBD: confirm
stride**. The version `4 bits` indexes a **per-inner 15-slot version table**
(same scheme as the leaf).

History run (per branch): old `ptr_address`es + their versions, in the tail heap.
A `kTombstone` *current* with history = a deleted branch still resolvable by
older snapshots.

**Prefetch / read:**
- writer (current) → never reads `branch_meta`.
- reader → prefetch `branch_meta[pos]` **before** the div scan; the cold load is
  masked by the scan + the (inevitable) `children[pos]` child-miss. This is a
  *good* prefetch (real lead time), unlike the in-leaf stem prefetch that washed.
- column layout is viable because the **APGM predictor** gives `pos` before the
  scan → fire stems/children/meta prefetches at `pos`, then verify. (Predictor is
  now load-bearing for the prefetch strategy.)

**Propagate up** only when: out of 12-bit history heap in the 64 KB page, OR out
of 15 versions and must clone-to-prune → fresh-id clone, prune below floor,
repoint parent (which then versions *its* branch). Bounded per node; worst case
(version exhaustion converging on one path) can cascade toward root — the cost
ceiling.

---

## 6. Value/branch lifecycle (retain / release / visit)

Mirror the core engine exactly (`node/leaf.hpp`, `tree_ops::retain_children`):

- **`copy_to`** — byte clone only. **Never** touches refcounts.
- **`retain_branches()`** — explicit, called by wtree's COW/share path (NOT
  copy_to), when a clone co-exists with the original (snapshot sharing). Walks
  current + history; `session->retain(ptr)` for each `big`/`tree` address.
- **`destroy(session)`** — on node free. Walks current + history;
  `session->release(ptr)` for each `big`/`tree`. (Leaf `destroy` was a no-op —
  leaves were pointer-free; with `big`/`tree` they hold cross-node refs now.)
- **`visit_children(v)`** — current + history walk (feeds compactor/GC relocation).
- **overwrite / remove / prune** — release the dropped `big`/`tree` ptr.

Key subtlety: every walk must cover **current AND the alt/history chain** — an old
version can itself be `big`/`tree`, and a `tomb`/`kTombstone` *current* still has
live `big`/`tree`/child in history. Subtree = **single ptr** → one
retain/release each (no `tree_id`-pair handling).

**Prune floor = the read_lock horizon** (min live snapshot), already tracked by
SAL. The writer/compactor drops history below the floor on the rebuild/clone.

---

## 7. Open decisions

1. **12-bit `history_off` stride** for the 64 KB inner page (`×16` ⇒ 16-byte
   aligned history entries, reaches 64 KB).
2. **Per-node 15-slot version table** shared across all entries/branches of the
   node, `ver:4` indexes it, `0` = no history. (Assumed; confirm.)
3. **`version_node`** placement — SAL-level vs a wtree node type. Current plan:
   reached by a single `ptr_address` from a `tree`/`big` value; holds
   `{version#, root ptr}`.
4. **Inline value versioning vs promote-to-big** — does an inline value that
   gains versions stay inline with an alt chain (the §3 design), or promote to a
   `big` value_node chain? (Plan: alt chain inline for small, `big` for large.)

---

## 8. Implementation order

1. SAL reserved-ptr-range (port `29e7f48`). [foundation]
2. value_region v2 + drop global `value_len_` + single-version fast path
   (`ver=0`, `has_alt=0`). [no behavior change for non-MVCC use]
3. Leaf `destroy`/`visit_children`/`retain_branches` over current (+ stub history);
   `copy_to` untouched. [lifecycle correctness]
4. Leaf version table + alt chain + read-version-select + prune-on-rebuild.
5. `kTombstone` + three-way inner routing.
6. Inner `branch_meta` column + per-inner version table + history heap +
   predictor-driven multi-column prefetch.
7. Overflow → fresh-id clone + prune + propagate-up (leaf then inner).
8. MVCC fuzz vs a `std::map<version → map>` reference (snapshot-isolation check).
