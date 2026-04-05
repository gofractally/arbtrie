# Known Bugs

## Open

### 1. Shared-mode update heavy: 1 object leak (ptr_alloc bookkeeping)
- **Repro**: `psitri-tests "fuzz shared mode update heavy" -c "seed=1337"` with 4000 ops
- **Bisected to**: op #3616 (upsert in shared mode after double commit_reopen)
- **Symptom**: `get_total_allocated_objects()` returns 1 after full cleanup, but `dump_live_objects()` / `for_each_allocated` finds nothing. The `_ptr_alloc.used()` counter is off by 1 vs actual allocated slots.
- **Likely cause**: Bookkeeping bug in SAL's ptr_alloc — a release decremented the ref but not the `used()` counter, or a root-table operation created an untracked allocation.
- **Pattern**: Double `commit_reopen` (making root shared via root table + cursor + snapshot) followed by shared-mode upsert. The upsert triggers COW, and somewhere in the release cascade the counter gets out of sync.

### 2. `database::get_stats().total_live_objects` returns 0 after writes
- **Repro**: `psitri-tests "database dump and get_stats"`
- **File**: `libraries/psitri/tests/coverage_gap_tests.cpp:475`
- **Symptom**: After inserting 100 keys and committing, `stats.total_live_objects` is 0.
- **Likely cause**: `get_stats()` doesn't account for objects allocated during the session, or the stat counter isn't wired up.

## Fixed (recent)

### DWAL undo replay leaves dangling pointers in RW layer
- **Repro**: write→commit→write→abort, then `get_latest()` on the restored key
- **Symptom**: After abort, the RW layer's ART map has `btree_value.data` pointing into the freed undo_log arena. Reads return garbage or crash.
- **Root cause**: `dwal_transaction::abort()` replays `overwrite_buffered`/`erase_buffered` entries by upserting `entry.old_value` directly into the btree_layer's ART map. The `old_value.data` string_view points into the undo_log's arena (monotonic_buffer_resource). When the transaction is destroyed, the undo_log and its arena are freed, but the ART map entry still holds the dangling pointer.
- **Fix**: In both outer and inner abort replay paths in `dwal_transaction.cpp`, copy restored value data into the btree_layer's pool via `layer.store_string()` before upserting back into the ART map.

### `insert()`/`update()` used throw for precondition violations — now assert/abort
- `unique_insert`/`unique_update` threw on precondition violation, corrupting root state. Throw implied recoverability — callers caught it and returned false, masking the corruption.
- `insert()` did a redundant `get()` traversal as a workaround. `update()` used try/catch for control flow.
- Fix: replaced throw with `assert + unreachable` in tree_ops.hpp (5 sites). Changed `insert()`/`update()` to void, removed all guards. Single traversal, assert on the cold path.

### count_keys returns garbage values (split_merge forwarding bug)
- **Repro**: `psitri-tests "fuzz long run balanced" -c "seed=314159"` — descendant count mismatch at depth 2.
- **Symptom**: `count_keys()` returns wildly incorrect values because inner node `_descendents` fields accumulate errors over time.
- **Root cause**: In `split_merge`, the `split` function creates new inner nodes by calling `count_subrange_keys`, which uses `count_child_keys` → `get_ref` to count each child's keys. `get_ref` follows control-block forwarding. The recursive upsert already realloc'd branch `br`, so forwarding returns the POST-upsert count for that branch. The split half containing `br` gets an inflated descendant count. Then `merge_branches` applies `_delta_descendents` on top, double-counting the change.
- **Fix**: In `split_merge`, compute the forwarding error (`forwarded_count - original_count`) and subtract it from the split half's descendant count before calling `merge_branches`.

### range_remove ghost keys when both start and boundary branches COW (this session)
- **Repro**: `psitri-tests "fuzz shared mode remove heavy" -c "seed=11332302"`
- **Symptom**: `remove_range` reported correct count but left 2 ghost keys in the tree. Iteration found keys that should have been removed.
- **Root cause**: In `range_remove_inner` unique-mode general case, when both the start and boundary branches had shared children (ref > 1) causing COW, only the start branch's new address was written into the node. The boundary branch update was in a separate `if` block that was never reached because the start block returned early.
- **Fix**: When both branches changed, apply `merge_branches` sequentially (start first, then boundary on the resulting node). Same fix applied in both the `remove_lo < remove_hi` and `remove_lo >= remove_hi` code paths.

### Subtree collapse test too small for debug builds (9feae4c)
- "tree_ops: subtree collapse with set_collapse_threshold" failed with `1 < 1` because N=12 never created a multi-level tree. Fixed: N=60.

### make_value assertion for value_node type (9feae4c)
- `make_value()` didn't handle `value_node` type, asserting on re-entry after `insert()` converted a view to value_node.

### split_insert on single-entry leaf (9feae4c)
- `get_split_pos()` asserted `nb > 1`. Two long keys (>1000 bytes each) couldn't fit in a 2048-byte leaf.

### leaf_prepend_prefix overflow (9feae4c)
- Space check for collapsing inner_prefix_node didn't account for cline slots from address-typed values.

### leaf_update cline overflow (this session)
- `can_apply` for leaf_update returned `defrag` when `_cline_cap >= 16`, but rebuild could still overflow if >16 unique clines existed. Changed to return `none` to force split.
