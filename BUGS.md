# Known Bugs

## Open

### 1. `tree_context` Dense Random SIGBUS in segment::sync
- **Repro**: `psitri-tests "tree_context"` (excluded from default runs via `[!benchmark]`)
- **File**: `libraries/psitri/tests/tree_context_tests.cpp:498` (TEST_CASE "tree_context")
- **Crash site**: `sal::mapped_memory::segment::sync` (segment_impl.hpp:12) — EXC_BAD_ACCESS code=2 (write fault) on a high-memory address (~0x7801ffffc0). Reproducible 3-of-3 runs.
- **Diagnosis**: The sync path writes a sync_header at `data + alloc_pos` and the user_data follows. The `data` pointer is per-segment; if the segment was grown via mremap and the segment's `data` field hadn't been updated to the new mapping address, this write hits an unmapped page → SIGBUS. Confirmed via lldb: stack frame is `segment::sync + 168`, address is in an unmapped high range.
- **Mitigation**: Tagged `[!benchmark]` so it doesn't gate normal test runs.
- **Out of scope for transaction-refactor**: this is a SAL-level segment-growth/remap protocol bug, independent of the COW/MVCC refactor. Fix needs to ensure all live `segment*` references see the post-mremap `data` pointer (or use a stable handle that hides the remap).

### 2. `database::get_stats().total_live_objects` returns 0 after writes
- **Repro**: `psitri-tests "database dump and get_stats"`
- **File**: `libraries/psitri/tests/coverage_gap_tests.cpp:475`
- **Symptom**: After inserting 100 keys and committing, `stats.total_live_objects` is 0.
- **Status**: FIXED — `database.hpp:get_stats()` now wires `total_live_objects` to `_allocator.total_allocated_objects()` (control-block-allocator counter) instead of the segment dump's `total_read_nodes` which double-counts/misses after compaction.

## Fixed (recent)

### Multi-tree parallel fuzz tests: Catch2 thread-safety (release-only)
- **Repro**: `psitri-tests "fuzz multi-tree parallel"` — flaky SIGABRT in release builds
- **Root cause**: `cleanup()` used `CHECK_FALSE` (Catch2 macro) to verify empty tree. Worker threads calling this raced with the main thread's Catch2 output redirect, triggering an assertion in Catch2's `OutputRedirect::activate()`.
- **Fix**: Replaced the Catch2 macro with a `throw` so the worker thread's try/catch handles it safely.

### Shared-mode update heavy: 1 object leak (ptr_alloc bookkeeping)
- **Repro**: `psitri-tests "fuzz shared mode update heavy"` with seed=1337
- **Symptom**: `get_total_allocated_objects()` returns 1 after full cleanup.
- **Root cause**: The COW leaf check in `upsert_inline()` used `cow_seq > 0` which unconditionally allocated new leaves when any readers existed, even for writer-owned leaves. This caused unnecessary leaf allocations that led to a ref-count tracking desync during the release cascade.
- **Fix**: Added `cow_seq` field to ART `leaf_header`. Changed the check to `lh->cow_seq < cow_seq` so writer-owned leaves are mutated in-place, eliminating the spurious allocations that triggered the bookkeeping mismatch.

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
