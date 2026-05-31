# wtree unique/shared mode + COW-to-root

Review of how the **stable core engine** (`tree_ops.hpp`, `upsert_mode.hpp`)
tracks unique-vs-shared node ownership and propagates copy-on-write to a new
root, what **SAL** actually guarantees, where **wtree** stands today, and the
plan to bring the templated mode pattern (and eventually version tracking) into
wtree.

---

## 0. The one fact that drives everything: SAL does NOT fork on `ref > 1`

`allocator::can_modify(ses_num, loc)` (`sal/allocator.hpp:693`) decides in-place
vs copy using **segment state only**:

```cpp
return seg->_session_id == ses_num                       // my session owns the segment
    && seg->get_first_write_pos() <= loc.segment_offset(); // above the sync watermark
```

There is **no refcount term**. `copy_on_write` / `modify_guard::get`
(`sal/smart_ptr.hpp:465`) gate solely on `can_modify`. And the copy path calls
`control_block::move(loc)` (`sal/control_block.hpp:243`), which rewrites only the
`cacheline_offset` of the **same control block** — i.e. the **same
`ptr_address`** now points at the new bytes and the old bytes are freed
(`record_freed_space`).

**Implications:**

- A node with `ref > 1` (shared by a live snapshot) **will be mutated in place**
  by SAL if it still lives in the writer's own un-synced segment. SAL will not
  fork it.
- `cow_inner` / `cow_leaf` (`inner_node.hpp:194`, `leaf_node.hpp:297`) go through
  `modify()`, so they **preserve the `ptr_address`**. That is exactly right in
  unique mode and **fatal under sharing** — the old snapshot reaches the node
  through that same address and would observe the mutation.
- `realloc` asserts `ref()==1` (`sal/allocator_session_impl.hpp:134`);
  `mvcc_realloc` drops the assert but still `move()`s the shared CB — it does not
  preserve the old version through the same CB.

**Therefore snapshot isolation is the tree layer's responsibility.** To modify a
shared node the tree must, itself: (1) `alloc` a **brand-new node with a new
`ptr_address`**, (2) **retain** the unchanged children/siblings, (3) **rebuild
the spine to a new root** (path-copy), (4) publish the new root. SAL supplies
allocation, refcount/free, and the unique-owner in-place fast path — nothing
more.

---

## 1. Core engine pattern (the thing to mirror)

### `upsert_mode` — a constexpr NTTP (`upsert_mode.hpp`)

```cpp
struct upsert_mode {
   enum type : uint32_t { unique = 1, insert = 2, update = 4, upsert = 6,
                          remove = 16, unique_upsert = unique | upsert,
                          shared_upsert = upsert, ... };
   constexpr bool        is_unique() const { return flags & unique; }
   constexpr bool        is_shared() const { return not is_unique(); }
   constexpr upsert_mode make_shared() const { return {flags & ~unique}; }
   uint32_t flags;
};
```

Carried as `template <upsert_mode mode>` so every predicate is compile-time and
each function emits two specialized paths via `if constexpr`. **This is the
extensible "state" the user wants** — version-tracking flags become more bits in
this struct, selecting code paths at compile time with zero runtime cost.

### Demotion: one runtime line (`tree_ops.hpp:1060`)

```cpp
template <upsert_mode mode>
branch_set upsert(const sal::alloc_hint& h, smart_ref<alloc_header>& r, key_view k) {
   if constexpr (mode.is_unique())
      if (r.ref() > 1)                                  // a snapshot still holds it
         return upsert<mode.make_shared()>(h, r, k);    // permanent flip for this path
   ...
}
```

`r.ref()` is the SAL control-block refcount (`sal/smart_ptr.hpp:256`). Unique =
"started unique AND this node's `ref()==1`." Once shared, `make_shared()` only
clears the bit, so it never flips back down that path.

### COW-to-root: return-value threading (no back-pointers)

Each recursive `upsert` returns the new address(es) (`branch_set`). The parent
integrates via `merge_branches<mode>` (`tree_ops.hpp:1206`):
- **unique:** `in.modify()->apply(update)`, return **`in.address()`** (unchanged)
  → cascade stops; ancestors untouched.
- **shared:** `_session.alloc<InnerNodeType>(...)`, return a **new address** →
  bubbles up one level per stack frame, rebuilding the spine.

Terminates at the top, which publishes the new root via `_root.give()`
(`tree_ops.hpp:234`) / deferred `set_root` at commit.

### Sharing hook: `retain_children` under `is_shared` (`tree_ops.hpp:599,1385`)

```cpp
if constexpr (mode.is_shared()) retain_children(in);   // bump every child's ref
```
Run immediately before each shared clone so the new spine and the old snapshot
both own the shared subtrees. If a clone ends up not created, the matching
release rebalances.

### How a snapshot creates the sharing

A reader cursor / `make_unique_root` keeps a `smart_ptr` on the published root,
so its CB `ref >= 2`. The writer's `ref() > 1` test at the root then fires →
shared mode → path-copy. Drop the snapshot, `ref` returns to 1, writes resume
the in-place unique fast path.

---

## 2. wtree today: unique-only, but propagation already exists

| Piece | Status in wtree |
|---|---|
| `upsert_mode` available | Yes — already uses `apply<upsert_mode::unique_upsert>` (`wtree.hpp` replace_slot_, `inner_node.hpp` page insert). |
| Mode threaded through descent | **No** — `node_upsert_`, `replace_slot_`, `leaf_upsert` hardcode unique. |
| `ref() > 1` demotion | **No** — never checked. |
| Return-value threading up | **Yes** — `node_upsert_` returns `upsert_result{left,right,extra}`; parent repoints `inner_set_child` only when the child's address changed (`wtree.hpp:1147`). |
| Root reassignment | **Yes** — `root_ =` at `wtree.hpp:89,156,168,362,389`. |
| `cow_inner`/`cow_leaf` | Keep the **same ptr_address** (unique-mode correct; **wrong under sharing**). |
| `retain_children` | Leaf has it (§3, `leaf_node.hpp`); inner has `visit_children`/`destroy` but no shared-clone call site yet. |
| read_lock per op | Yes (`_session->lock()` in find/upsert/remove) — pins relocation horizon, not a fork trigger. |

So wtree has the *propagation half* of the pattern (new addresses already thread
up to a new `root_`). It lacks the *mode half*: the compile-time `upsert_mode`
on the descent, the `ref()>1` demotion, and shared specializations that
**alloc-fresh (new address) + retain_children** instead of `cow_*` (same
address).

---

## 3. Plan to add shared mode (COW-to-root) — the eager-clone approach

Because the mode **locks to shared from the first shared node downward** (§1),
we don't have to write a shared specialization for every sub-case of
`node_upsert_` (split, K=1 promote, container-wrap). At the first shared node we
**eager-clone `in` to a fresh, uniquely-owned copy and then run the existing
unique in-place logic on that private copy.** Only `node_upsert_` and
`leaf_upsert` need the `mode` template; every helper they call
(`replace_slot_`, splits, promotes, `apply<unique_upsert>`) already operates on
an already-private node and is reused unchanged.

1. **Template only the two descent entry points**: `node_upsert_<mode>` and
   `leaf_upsert<mode>`. Public `upsert` enters `node_upsert_<unique_upsert>`.
   Helpers stay non-templated (they run after the eager-clone, on a private node).
2. **Demote per step** (after the leaf check, before any mutation):
   ```cpp
   if constexpr (mode.is_unique())
       if (wsession().get_ref<inner_node>(in->address()).ref() > 1)
           return node_upsert_<mode.make_shared()>(node, key, depth, value);
   ```
3. **Eager-clone at the first shared node** (inner): after `pos`/`child` are
   computed, `if constexpr (mode.is_shared())`:
   - `in = clone_inner_fresh(in)` → NEW address, payload copied.
   - **retain the OFF-PATH siblings only** (every child except slot `pos`). The
     on-path child is NOT retained — the recursion path-copies it and the old
     child stays ref-1 owned by the snapshot's old `in`.
   - Fall through to the unchanged body: `cow_inner(in)` is now a no-op (fresh +
     unique), `replace_slot_`/split/promote all mutate the private copy
     correctly.
4. **Eager-clone leaf** (in `leaf_upsert<mode>`): replace the top `p =
   cow_leaf(p)` with `if constexpr (mode.is_shared()) { p = clone_leaf_fresh(p);
   p->retain_children(_sess); } else { p = cow_leaf(p); }`. The retain pairs with
   the in-place overwrite's `leaf_retire_value_region_` release, so the snapshot
   keeps its old big/subtree values.
5. **Recursion carries `mode`**: `node_upsert_<mode>(child, …)` and
   `leaf_upsert<mode>(…)`. On-path child sees shared → path-copies unconditionally
   (correct even at ref==1 — reachable via the snapshot's old parent).
6. **Root publish**: already handled — the new top address threads up through the
   existing `upsert_result`/repoint machinery into `root_`. (Durable SAL
   `set_root` is later, with wtree transaction commit.)
7. **Snapshot source**: a read-view holding the root `ptr_address` with the CB
   retained (`ref>=2`) is what trips step 2. Add `wtree::snapshot()` (retain
   root → a non-owning read `wtree`) for the test.

### The one fiddly primitive: `clone_*_fresh` (new address, payload copied)

`inner_node::copy_to` (`inner_node.hpp:319`) clobbers dst's 12-byte SAL header
with src's — correct for COW-*move* (address preserved via the moved CB), WRONG
for a fresh clone (dst must keep its own new address/seq/size). The clone helper
must save/restore the new node's header, exactly like `leaf_node::copy_to`
already does (`leaf_node.hpp:212-216`). So:
```cpp
inner_node* clone_inner_fresh(inner_node* src) {  // NEW address
    inner_node* dst = inner_alloc_sized(src->cap_in_cls, src->address());
    save dst header; src->copy_to(dst); restore dst header;  // payload only
    return dst;
}
```
Leaf is simpler — `leaf_node::copy_to` already preserves dst's header, so
`clone_leaf_fresh` = `leaf_alloc()` + `src->copy_to(new)`.

### Correctness guardrails

- Never call `cow_inner`/`cow_leaf` to fork — they alias the `ptr_address` (§0).
  The eager-clone uses `*_alloc` (new address) + `copy_to` payload + retain.
- Retain **off-path siblings only** for inner (on-path child path-copies); retain
  **all** value refs for the leaf (`retain_children`, §3) — the overwrite-retire
  release rebalances.
- Unique path is byte-for-byte today's code (the `if constexpr` unique branch is
  the existing body); zero hot-path regression.
- (Future, §4) `retain_children` / the clone must also cover the alt/history
  chain once leaf versioning lands.

---

## 4. Why this is the home for version tracking

The user's framing: "that templated pattern allows us to constexpr optimize
update paths based on state, and eventually version tracking will be part of that
state." Concretely — once the descent is `template <upsert_mode mode>`, the MVCC
machinery from `wtree_mvcc.md` (leaf version table, alt-chains, inner
`branch_meta`, `kTombstone` routing) becomes additional **mode bits / state**
the writer carries: e.g. a `versioned` flag selects whether a shared clone packs
a new version inline (alt-chain append + version-table slot) vs the plain
path-copy, all chosen at compile time with `if constexpr`. The shared/unique
split implemented here is the substrate that version-aware update paths plug
into.

---

### Source map

Core: `tree_ops.hpp:1060` (demote), `1206` (merge_branches unique/shared), `1385`
(retain under is_shared), `599` (retain_children), `218-237` (root publish);
`upsert_mode.hpp`. SAL: `allocator.hpp:693` (can_modify), `smart_ptr.hpp:465`
(modify_guard), `control_block.hpp:243` (move), `allocator_session_impl.hpp:134`
(realloc ref==1 assert), `160-175` (mvcc_realloc). wtree: `wtree.hpp:1147`
(repoint-if-changed), `89/156/168/362/389` (root_ =), `inner_node.hpp:194` /
`leaf_node.hpp:297` (cow_* same-address).
