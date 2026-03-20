# Remove() Implementation Plan — psitri

## Context

The `redress` branch has remove() ~60% complete: leaf-level removal works (unique + shared COW paths), but inner node branch removal aborts at tree_ops.hpp ~line 704. The goal is to complete remove() while maintaining psitri's performance characteristics (batched leaves, prefix compression, shallow depth).

### Key design decisions

1. **Unique path (ref == 1) restructures inline** — modify in-place or realloc (preserves ptr_address). Fast single-writer path.
2. **Shared path (ref > 1) defers optimization to compactor** via `restructure_queue` (SPSC circular_buffer, same pattern as release/rcache queues). Correctness is immediate; structural optimization is deferred.
3. **No dynamic memory allocation** — stack-allocated buffers with compile-time bounds. No `std::vector`, `new`, or heap alloc in hot paths.
4. **No flag bits in node headers** — sparseness determined by inspecting `num_branches`/`descendants`.
5. **control_block ≈ shared_ptr** — `ptr_address` = handle, `retain/release` = copy/destroy, `realloc` = replace managed data preserving handle, `cas_move` = thread-safe atomic variant.
6. **Three address strategies** — (a) realloc: same control block, new data (preserves ptr_address); (b) re-home: new control block, existing data (changes ptr_address, zero data copy — **new API needed**); (c) alloc: new control block + new data. Re-home is cheapest when cline affinity is the only problem (data is fine, ptr_address isn't).

### Known state

- **`_descendants` not maintained**: 39-bit field exists but is always 0 or copied from clone. No `add_descendants()` method. Phase 0 fixes this.
- **EOF values on inner nodes**: Documented in architecture.md but NOT implemented. Not relevant to current work.

---

## Level 1: Inner Node Branch Removal

**What**: When a child returns empty and the inner node has >1 branches, remove that branch.

**Where**: `inner.hpp` — implement `remove_branch()` (stub at ~line 232); `tree_ops.hpp` — replace abort at ~line 704.

### Algorithm (`remove_branch`)

Both unique (in-place) and shared (clone-minus-one) paths use the same core logic — only the mutation strategy differs:

```
remove_branch(branch_number br):
  1. Decrement cline ref for branches[br]. If ref hits 0 → compact clines (shift down, remap branch indices).
  2. Remove division: br==0 removes divisions[0]; br>0 removes divisions[br-1]. memmove remainder.
  3. Remove branch: memmove branches[br+1..] down by 1.
  4. Decrement _num_branches.
```

**Unique path**: `in.modify()->remove_branch(br)` — mutates in place.
**Shared path**: `op::inner_remove_branch{br}` constructor clones the source minus branch `br`, applying the same logic during construction. Parallels `op::leaf_remove`.

**Cline compaction** reuses `copy_branches_and_update_cline_index` pattern from split operations.

### tree_ops.hpp integration

```cpp
if (sub_branches.count() == 0) [[unlikely]]
{
  if (in->num_branches() == 1)
    return {};  // cascade empty to parent

  // Remove branch — unique modifies in-place, shared allocates clone-minus-one
  if constexpr (mode.is_unique()) {
    in.modify()->remove_branch(br);
  } else {
    in = _session.alloc<InnerNodeType>(parent_hint, in.obj(), op::inner_remove_branch{br});
  }

  // Phase 2 adds: if (in->num_branches() == 1) try_collapse_single_branch(...)
  // Phase 3 adds: if (in->descendants() <= threshold) try_collapse_subtree(...)
  return in.address();
}
```

---

## Level 2: Single-Branch Inner Node Elimination

When an inner node X drops to 1 branch with sole child Y after removal.

### Four-tier strategy (try cheap first)

**Tier 1 — Re-assume child's address** (O(1), zero allocation):
Pre-check `find_clines<remove_old_branch=true>(parent_hint, X.addr, {Y.addr}, indices)`. If Y fits in parent's clines (shares a base, or X's freed cline slot is reusable, or null slot available): return Y's address, release X.

If `insufficient_clines` → fall to Tier 2. **Critical**: this pre-check prevents triggering splits via `merge_branches`.

**Tier 2 — Re-home child under new control block** (zero data copy, new ptr_address):
Y's data is fine but Y's ptr_address doesn't fit in parent's clines. Allocate a new control block (with hint near parent's clines) pointing to Y's existing physical data. The new ptr_address may share a cline base with the parent. Release Y's old control block, release X.

This is strategy (b) — **new API needed** in SAL: `rehome(existing_location, hint)`. Returns a new `ptr_address` → existing data. 8 bytes of control block, zero data copy.

If new ptr_address still doesn't fit → fall to Tier 3.

**Tier 3 — Realloc X as replacement node** (unique path, preserves X's ptr_address):
`realloc<ReplacementType>(X, ...)` — X's ptr_address stays the same, parent's clines untouched. This works because X's ptr_address is ALREADY in the parent's clines. We're replacing X's content with Y's content (or merged content), not trying to fit Y's address.

Cases by node types:
- **A** (plain inner → child): `realloc<ChildType>(X, child_data, op::clone{})` — copy child into X.
- **B+D** (inner_prefix → inner/inner_prefix child): `realloc<inner_prefix_node>(X, merged_prefix, child_branches)` — prefix = `my_prefix + div_byte + child.prefix` (child.prefix empty for Case D). Unified.
- **C** (inner_prefix → leaf child): `realloc<leaf_node>(X, child_leaf, op::prepend_prefix{prefix})` — rebuild leaf with prepended prefix bytes. Bounded: ≤58 keys, microseconds.

**Tier 4 — Defer to compactor** (shared path, ref > 1):
`queue_restructure(X.address())` — retain X, push to restructure queue. Compactor rebuilds via `cas_move`. X remains functional (one extra indirection) until processed.

---

## Shared-Mode Cline Pressure During Remove

In shared mode, COW'd nodes get new `ptr_address` values that may not share a cacheline base with the parent's existing clines. While branch count doesn't increase, **cline pressure can**.

**Resolution order when `find_clines` fails for a COW'd child:**

Cline affinity is determined by **ptr_address** (control block location), NOT data location. Realloc preserves ptr_address → doesn't help. The strategies that change ptr_address are re-home (b) and alloc (c).

1. **Freed cline slot**: If the removed branch was the sole user of its cline, `find_clines<remove_old_branch=true>` frees that slot. The new address may use it.
2. **Re-home child (strategy b)**: Child was just allocated (ref == 1). Allocate a new control block with hint near parent's cline base, pointing to the child's existing data. Zero data copy. New ptr_address may fit. Release old control block.
3. **Re-alloc child (strategy c)**: Release the bad alloc. Alloc again with a more specific hint targeting the parent's cline base. New ptr_address + new data. Data copy required but gets a fresh chance at cline affinity.
4. **Accept the split** (Phase 1): This edge case (all 16 clines full + all strategies miss) is extremely rare. Phase 1 accepts it. Phase 4 adds compactor fallback.

---

## Level 3: Subtree Collapse to Leaf (Phase 3, deferred)

When `descendants() <= collapse_threshold` (~28), collect entries from the sparse subtree into a stack-allocated buffer and realloc the root as a single leaf. Requires accurate descendant tracking (prerequisite). Details deferred until Phase 1-2 validated.

## Restructure Queue + Compactor (Phase 4, deferred)

New SPSC `circular_buffer<ptr_address, 64K>` per session. Session retains address before push; compactor pops, inspects (bail if stale/orphaned), rebuilds via `cas_move`, releases retain. Queue-full is non-error (optimization, not correctness). `vcall::restructure` dispatches type-specific handlers. Testing uses `sleep(100ms)` to give compactor time. Details deferred until Phase 3 validated.

---

## Implementation Order

Each step is a minimal, testable change. Build and test after EVERY step.

### Phase 0: Descendant tracking

Accurate `_descendants` is an invariant we can check at every step (count should equal total keys in subtree). It's also needed for Phase 3's collapse threshold.

**Step 0.1 — `add_descendants()` method**

File: `libraries/psitri/include/psitri/node/inner_base.hpp`

Add method on `inner_node_base<Derived>`:
```cpp
void add_descendants(int64_t delta) noexcept {
    auto& d = static_cast<Derived&>(*this);
    d._descendents += delta;
}
```

**Step 0.2 — Track `_delta` through upsert/remove recursion**

File: `libraries/psitri/include/psitri/tree_ops.hpp`

Add `int _delta = 0;` member to `tree_context` (alongside `_old_value_size`). Set it at the leaf level:
- **Insert** (new key): `_delta = 1` (when `_old_value_size == -1` at leaf insert)
- **Upsert** (existing key): `_delta = 0`
- **Remove** (key found): `_delta = -1` (when remove succeeds at leaf)
- **Remove** (key not found): `_delta = 0`

After each recursive `upsert<mode>()` returns at an inner node level, apply delta:
- **Unique path**: `in.modify()->add_descendants(_delta);` (before returning `in.address()`)
- **Shared path**: The clone constructor copies `_descendants` from source. After allocation, adjust: `new_inner.modify()->add_descendants(_delta);`

Also set `_descendants` correctly when creating a new inner node from a leaf split: count = number of keys across both new children.

**Step 0.3 — Initialize `_descendants` on inner node creation from split**

When a leaf splits and creates a new inner node (in `make_inner`, `make_inner_prefix`, `split_insert`), set `_descendants` to the total key count of the children. For a leaf split that's `left_leaf.num_branches() + right_leaf.num_branches()`.

**Test**: Insert N keys, call `get_stats()`, verify `total_keys == N`. Then verify each inner node's `_descendants` matches actual subtree key count (add a `validate()` check that walks the tree and asserts `descendants == sum of child descendants/leaves`).

**How to run**: `cmake --build build/release && cd build/release && ctest -R psitri`

---

### Phase 1: Branch removal

**Step 1.1 — `remove_branch()` on inner_node (unique, in-place)**

File: `libraries/psitri/include/psitri/node/inner.hpp`

Implement the stub. Start simple: null the cline entry when ref hits 0 (defer compaction to Step 1.3).

**Test**: Insert keys creating 3-branch inner node. Remove key that empties one branch. Verify 2 branches remain, cursor finds remaining keys.

**Step 1.2 — Wire into tree_ops.hpp (unique mode only)**

File: `libraries/psitri/include/psitri/tree_ops.hpp`

Replace abort with the integration code from Level 1 section (unique branch only). Handle `num_branches == 1 → return {}` and `num_branches == 2 → remove, return as-is`.

**Test**: Run existing `tree_context-insert-remove`. Should proceed past the abort. May still fail on other edge cases — record progress.

**Step 1.3 — Cline compaction in `remove_branch`**

When cline ref hits 0: shift higher clines down, remap branch indices via `copy_branches_and_update_cline_index` (existing pattern), decrement `_num_cline`.

**Test**: Insert 10K keys, remove 5K, re-insert 5K different keys. Cursor verify all 10K. Exercises cline compaction + reuse.

**Step 1.4 — `op::inner_remove_branch` (shared mode)**

Files: `inner.hpp` (op struct + constructor), `tree_ops.hpp` (shared branch)

Constructor clones source minus branch `br`. **Shares the same core remove logic** as `remove_branch()` — extract a common helper that both paths call (e.g., `compute_layout_after_remove(br)` returns which divisions/branches/clines to keep, then each path applies it differently: in-place mutation vs clone construction).

**Test**: Hold read snapshot, remove keys from writer. Verify snapshot sees original data, writer sees removals.

**Step 1.5 — Prevent remove from triggering splits (shared mode)**

When shared-mode COW returns a changed address, use `update_branch_address` instead of `merge_branches`:
1. Try `find_clines` with new address
2. If fails: realloc child with targeted cline hint, retry
3. If still fails: accept split (rare edge case, Phase 4 adds compactor fallback)

**Test**: Hard to trigger naturally. Large test (100K+ keys with snapshots) exercises the shared path enough to validate no crashes.

### Phase 1 gate test

- `tree_context-insert-remove` passes (300K words, remove all, verify empty)
- Insert 100K random, remove 50K, cursor verify remaining 50K
- Insert 100K, remove 100K in reverse order
- Shared mode: insert, snapshot, remove, verify isolation
- After every test: `validate()` checks descendants accuracy at every inner node

---

### Phase 2: Single-branch elimination (unique path)

**Step 2.1 — Level 2 Tier 2, Case A (plain inner → sole child, realloc X)**

When unique-mode inner has 1 branch and no prefix: `realloc<ChildType>(X, child_data, op::clone{})`. Uses existing clone constructor.

**Test**: Create grandparent → inner(2 branches) → leaf, leaf. Remove one branch's keys. Verify inner eliminated, remaining keys correct, re-insert works.

**Step 2.2 — Level 2 Tier 1 (re-assume child address)**

Before Tier 2, pre-check `find_clines<true>(parent_hint, X.addr, {Y.addr}, indices)`. If fits: return Y, release X. If not: fall to Tier 2.

**Test**: Same as 2.1 but verify (via logging) which tier was taken.

**Step 2.3 — Level 2 Tier 2, Cases B+D (inner_prefix → inner/inner_prefix child, merge prefix)**

Unified: `merged_prefix = my_prefix + div_byte + child.prefix` (child.prefix may be empty for Case D). Realloc X as inner_prefix_node with merged prefix + child's branches/clines.

**Test**: Insert keys with shared prefixes creating nested inner_prefix nodes. Remove one group. Verify prefix merge, cursor correctness.

**Step 2.4 — Level 2 Tier 2, Case C (inner_prefix → leaf child, prepend prefix)**

`realloc<leaf_node>(X, child_leaf, op::prepend_prefix{prefix_bytes})` — new leaf constructor prepends prefix to all keys.

**Test**: Create inner_prefix → leaf. Remove until 1 branch (the leaf). Verify prefix prepended, cursor correct, hash lookup works.

### Phase 2 gate test

- Insert 300K words, remove 250K, verify remaining 50K
- Tree stats: depth/node counts proportional to 50K, not 300K
- Re-insert removed 250K, verify all 300K present
- Multi-level cascade: deep chain collapses to minimal depth

---

### Phase 3-5 (deferred)

- Phase 3: Subtree collapse to leaf (Level 3) — stack-allocated collection, threshold check
- Phase 4: Restructure queue + compactor integration (shared path optimization)
- Phase 5: Full integration testing (1M keys, concurrent access, performance)

Details will be planned after Phase 2 is complete and validated.

---

## Key Files to Modify

| File | Phase | Changes |
|------|-------|---------|
| `libraries/psitri/include/psitri/node/inner_base.hpp` | 0 | `add_descendants()` method |
| `libraries/psitri/include/psitri/node/inner.hpp` | 1 | `remove_branch()`, `op::inner_remove_branch`, shared helper |
| `libraries/psitri/include/psitri/tree_ops.hpp` | 0-2 | `_delta` tracking, replace abort, `update_branch_address`, Level 2 collapse |
| `libraries/sal/include/sal/allocator_session.hpp` | 2 | `rehome()` API — new control block for existing data (strategy b) |
| `libraries/psitri/include/psitri/node/leaf.hpp` | 2 | `op::prepend_prefix` declaration |
| `libraries/psitri/src/node/leaf.cpp` | 2 | `op::prepend_prefix` implementation |
| `libraries/psitri/tests/tree_context_tests.cpp` | 0-2 | Descendant validation, new remove tests |
