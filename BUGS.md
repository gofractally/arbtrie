# Known Bugs

## Open

### 1. Shared-mode update heavy: 1 object leak (ptr_alloc bookkeeping)
- **Repro**: `psitri-tests "fuzz shared mode update heavy" -c "seed=1337"` with 4000 ops
- **Bisected to**: op #3616 (upsert in shared mode after double commit_reopen)
- **Symptom**: `get_total_allocated_objects()` returns 1 after full cleanup, but `dump_live_objects()` / `for_each_allocated` finds nothing. The `_ptr_alloc.used()` counter is off by 1 vs actual allocated slots.
- **Likely cause**: Bookkeeping bug in SAL's ptr_alloc — a release decremented the ref but not the `used()` counter, or a root-table operation created an untracked allocation.
- **Pattern**: Double `commit_reopen` (making root shared via root table + cursor + snapshot) followed by shared-mode upsert. The upsert triggers COW, and somewhere in the release cascade the counter gets out of sync.

### 2. count_keys returns garbage values
- **Repro**: Various fuzz seeds; `count_keys()` returns values like 549755813886 when oracle has 12 entries. Iteration count is always correct.
- **Symptom**: `count_keys(lower, upper)` returns wildly incorrect values. Full iteration with `seek_begin/next` counts correctly.
- **Likely cause**: Descendant count tracking (`_delta_descendents`) gets corrupted during some tree restructuring operation (split, merge, or update overflow).
- **Workaround**: Fuzz tests use WARN instead of REQUIRE for count_keys checks.

### 3. Subtree collapse test too small for debug builds
- **Fixed in**: 9feae4c (N=60 instead of 60/OPS_SCALE)
- **Was**: "tree_ops: subtree collapse with set_collapse_threshold" failed with `1 < 1` because N=12 never created a multi-level tree.

## Fixed (recent)

### make_value assertion for value_node type (9feae4c)
- `make_value()` didn't handle `value_node` type, asserting on re-entry after `insert()` converted a view to value_node.

### split_insert on single-entry leaf (9feae4c)
- `get_split_pos()` asserted `nb > 1`. Two long keys (>1000 bytes each) couldn't fit in a 2048-byte leaf.

### leaf_prepend_prefix overflow (9feae4c)
- Space check for collapsing inner_prefix_node didn't account for cline slots from address-typed values.

### leaf_update cline overflow (this session)
- `can_apply` for leaf_update returned `defrag` when `_cline_cap >= 16`, but rebuild could still overflow if >16 unique clines existed. Changed to return `none` to force split.
