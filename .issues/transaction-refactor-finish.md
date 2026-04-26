---
id: transaction-refactor-finish
title: Finish per-txn versioning, lazy alloc, abort release, in-place coalesce
status: ready
priority: high
area: psitri
agent: ~
branch: mvcc
created: 2026-04-26
depends_on: [transaction-refactor]
blocks: []
---

## Context

The first wave of `transaction-refactor.md` landed Phases 0–4a plus a defensive
4b stub: OCC dropped, `tx_mode` renamed, `value_node` coalesce ctor in place,
coalesce branch wired into `try_mvcc_upsert`/`try_mvcc_remove` but inert
because every caller still bumps a unique version per op.

Key correction since the original plan: **the per-txn version does not need
its own field on `tree_context`.** It is already carried by the working
root's `smart_ptr<>`: `tree_id = {root, ver}`, where `ver` is a
`ptr_address` to a custom control block that holds the `uint64_t` version
number. Any code path that has the working root has the version — read it
with `_session->read_custom_cb(root.ver())`.

This issue finishes the refactor and unwinds the four shortcuts called out
in the post-mortem: (1) inert per-txn versioning, (2) `[tree_context]`
SIGBUS hidden under `[!benchmark]`, (3) `held_lock::lock` hardcoded to
`std::mutex*`, (4) coalesce always reallocates instead of memcpy-in-place
for same-or-smaller payloads.

## Phases

Each box must be checked before the phase is considered done. Don't move on
until the phase's tests pass.

### Phase A — Per-txn version threading (the actual refactor)

The flow: `start_transaction(expect_success)` allocates the txn's version
control block and attaches it to the working root's `ver` field. All
mutations through that root read the version from the smart_ptr. At commit,
`publish_root` reuses that same ver CB instead of allocating a new one.

- [ ] In `basic_write_session::start_transaction(expect_success)`: allocate
      `ver_num = global_version.fetch_add(1) + 1`, allocate
      `ver_adr = alloc_custom_cb(ver_num)`, set
      `working_root.set_ver(ver_adr)` before passing the smart_ptr to the
      `transaction` constructor.
- [ ] Add a private accessor `tree_context::txn_version() const` that
      returns `_session.read_custom_cb(_root.ver())` when `_root.ver() !=
      null_ptr_address`, else 0. (No new field, no setter.)
- [ ] `try_mvcc_upsert(key, value, version)` → drop the `version` parameter;
      call `txn_version()` internally. Same for `try_mvcc_remove`,
      `mvcc_upsert`, `mvcc_remove`.
- [ ] Update all callers: `basic_write_session::mvcc_upsert/mvcc_remove`
      stop bumping `global_version` per call; they expect the caller to
      have set up the working root's ver. Their existing one-shot semantics
      become: allocate a ver CB internally for that one op (preserving
      backward compat for direct callers), or require a pre-set version
      (cleaner). **Decision point:** keep the one-shot path as a separate
      method `mvcc_upsert_one(...)` that bumps internally, vs. let the
      one-shot caller set up the ver themselves. **Default: introduce
      `mvcc_upsert_one` for the legacy single-key API; the version-tracking
      `mvcc_upsert` is what the transaction's COW path uses.**
- [ ] Update `transaction::commit()` → `publish_root` reuses the
      pre-allocated ver instead of allocating a new one. The simplest
      shape: `publish_root` becomes a no-version-alloc swap that just
      `set_root` with the smart_ptr it was given (which already carries
      the txn's ver).
- [ ] Wire the transaction's COW write path (`tree_context::insert/upsert/
      upsert_sorted/remove/remove_range`) to the per-key value-node
      mutators that exist today. When a write path crosses a value_node,
      the version-chain append/coalesce check must use `txn_version()`
      from the working root. Verify that the existing COW value_node code
      paths now flow through `try_mvcc_upsert`-style entry points or get
      their own version reading from the root.
- [ ] Test: `100 updates to same key in one expect_success txn produces
      ≤ 2 chain entries` (initial committed entry + this txn's coalesced
      entry).
- [ ] Test: `2 expect_success txns on same key produces 3 chain entries`
      (initial + txn1 + txn2 — sanity check that coalescing is per-txn,
      not across txns).
- [ ] Test: `start_transaction(expect_success); commit;` increments
      `global_version` by exactly 1.
- [ ] Test: `start_transaction(expect_success); upsert; abort;` increments
      `global_version` by exactly 1 and the version is released
      (verifiable via `dead_versions` snapshot — see Phase C).

### Phase B — Lazy version allocation for `expect_failure`

- [ ] `start_transaction(expect_failure)` does **not** allocate a ver. The
      working root's `ver` is whatever the published root carried (could be
      null on a fresh database; could be the prior generation's ver — both
      are fine, the txn's writes never reach the tree until commit).
- [ ] Add `transaction::ensure_txn_version()` private helper. If the
      working root's ver does not yet identify a version this txn owns,
      allocate one and set it on the working root. Idempotent. Call sites:
      - `commit()` — before any `merge_buffer_to_persistent` call when at
        least one buffer is non-empty.
      - `merge_buffer_to_persistent(cs)` — at the top of the function as a
        defensive call (covers any future mid-tx flush path).
      - `do_remove_range` — in the `expect_failure` branch when the
        persistent_count exceeds `tombstone_threshold` (forced flush).
- [ ] Test: `expect_failure with no ops, abort` does not bump
      `global_version`.
- [ ] Test: `expect_failure with reads only, abort` does not bump
      `global_version`.
- [ ] Test: `expect_failure with writes, abort before commit` does not bump
      `global_version`.
- [ ] Test: `expect_failure with writes, commit` bumps `global_version`
      exactly once.
- [ ] Test: `1000 start+abort cycles in expect_failure with no writes`
      → `global_version` delta == 0.

### Phase C — Abort releases the version like any superseded version

- [ ] `transaction::abort()` calls `_db->dead_versions().add_dead_version(
      _allocated_ver)` when the txn allocated a ver (i.e. `expect_success`
      always; `expect_failure` only if `ensure_txn_version` fired).
- [ ] Release the ver CB:
      `_session->release(_allocated_ver_adr)`.
- [ ] Track the allocated ver address on the transaction itself (small new
      member, since the working root's smart_ptr might already be moved
      out by the time we're aborting in some paths). Either `tree_id
      _alloc_ver{}` or just `ptr_address _alloc_ver_adr{null_ptr_address}`.
- [ ] Test: `expect_success txn aborted` — the allocated ver appears in
      `dead_versions.load_snapshot()` after abort.
- [ ] Test: `expect_success txn with 1000 in-place mutations, then abort,
      then wait_for_compactor, then total_allocated_objects` — equals
      pre-txn count (every page tagged with the dead ver gets reclaimed).
- [ ] Test: `expect_failure aborted after a forced buffer flush` — also
      releases its lazily-allocated ver.

### Phase D — In-place coalesce fast paths (finish 4a)

- [ ] Add `value_node::can_coalesce_in_place(version, new_val) const` —
      returns true iff the topmost entry's version equals `version` AND
      the existing offset slot can hold `new_val.size()` bytes (i.e. the
      slot's stored size is ≥ `new_val.size()`).
- [ ] Add `value_node::coalesce_in_place(new_val) noexcept` — overwrites
      the top entry's value bytes via memcpy. Updates the entry's stored
      size if shrinking. **Must be `noexcept`** and must not allocate.
- [ ] Hook into `try_mvcc_upsert`'s coalesce branch: if
      `vref->can_coalesce_in_place(version, new_val)`, call
      `vref->coalesce_in_place(new_val)`; **else** fall through to the
      `mvcc_realloc<value_node>(... replace_last_tag{})` path.
- [ ] Same hook in `try_mvcc_remove` for tombstone case (a tombstone
      coalesce is always in-place — no value bytes to fit).
- [ ] Tighten `alloc_size` for `replace_last_tag` overload. Compute the
      precise size: existing entries' sizes minus the dropped entry's
      stored value bytes plus the new entry's value bytes. Stop reusing
      the append-path size.
- [ ] Test: `same-size update to a hot key does not allocate a new
      value_node` — capture allocator count before and after, expect 0
      delta.
- [ ] Test: `smaller update to a hot key does not allocate` — same
      pattern.
- [ ] Test: `larger update to a hot key allocates exactly 1 new value_node`
      (replace-last realloc path).
- [ ] Test: `tombstone coalesce on a hot key never allocates`.

### Phase E — Lift `transaction` to `basic_transaction<LockPolicy>`

The non-`std::mutex` LockPolicy time-bomb. `held_lock::lock` is
`std::mutex*`; the rest of the codebase is templated on `LockPolicy`.

- [ ] Convert `transaction` (currently in `psitri/transaction.hpp`) to a
      class template `basic_transaction<LockPolicy>`. Mirror the pattern
      used by `basic_database` / `basic_write_session`.
- [ ] `held_lock::lock` typed as `typename LockPolicy::mutex_type*`.
- [ ] Add type alias: `using transaction = basic_transaction<std_lock_policy>;`
      so the unqualified public name is unchanged.
- [ ] Update `tree_handle` and `transaction_frame_ref` similarly (they hold
      `transaction*`). Class templates with a `using tree_handle =
      basic_tree_handle<std_lock_policy>;` alias.
- [ ] Update `commit_additional_roots` / `abort_additional_roots`
      signatures in `write_session_impl.hpp` to use the templated lock type.
- [ ] Update the `held_lock.lock->unlock()` call to compile under any
      LockPolicy (it already does, since LockPolicy::mutex_type satisfies
      Lockable; just verify after the rename).
- [ ] Run the full test suite — must remain green. No external API
      breakage.
- [ ] Remove the corresponding entry from `BUGS.md`.

### Phase F — Fix the actual `[tree_context]` SIGBUS

- [ ] Reproduce reliably. Try smaller iteration counts (1M, 5M) and shorter
      runs to narrow which round triggers it.
- [ ] Capture stack via lldb: `lldb ./build/bin/psitri-tests --
      "tree_context"; (lldb) run; (lldb) bt all` after the fault.
- [ ] Identify root cause. Hypotheses ranked by likelihood: (1) mmap
      segment growth past some on-disk limit, (2) refcount race in the
      stress path, (3) cursor crossing a freed segment, (4) tree depth /
      cline allocation hitting an edge case.
- [ ] Fix the underlying issue.
- [ ] Re-tag the test back to `[tree_context]` (drop the `[!benchmark]`).
      Trim the iteration count to something a normal test run can swallow
      (under ~5s) — this is a correctness test, not a benchmark.
- [ ] Update `BUGS.md` — move the entry to "Fixed (recent)".
- [ ] Test: 3× back-to-back runs of `[tree_context]` all exit 0.

### Phase G — Verification gates (must all pass to call this done)

- [ ] `cmake --build build/release` exits 0 with no new warnings.
- [ ] `./build/bin/psitri-tests "~[!benchmark]"` exits 0 in 3 consecutive
      invocations from a fresh build.
- [ ] `./build/bin/sal-tests` exits 0.
- [ ] `grep -rn "tx_mode::batch\|tx_mode::micro\|tx_mode::occ"
      libraries/ test/` returns nothing.
- [ ] `grep -rn "occ\|read_set\|lb_entry\|track_read\|track_bound\|
      occ_conflict" libraries/psitri/include/ libraries/psitri/src/`
      returns nothing in production code (test files only — and only
      strings, never identifiers).
- [ ] `BUGS.md` "Open" section is empty or contains only out-of-scope
      items.
- [ ] All checkboxes in Phases A–F are checked.
- [ ] The `transaction-refactor.md` issue's "Status" section is updated to
      mark Phase 4b through Phase 7 as **landed**, not deferred.
- [ ] Hot-key perf check: a test that does 10000 updates to the same key
      in one `expect_success` txn allocates at most one value_node (or
      asserts an exact count of allocator events).

## What's still out of scope

- DWAL `expect_failure` mode (Phase 8 in the original plan). Tracked in a
  separate issue: `.issues/dwal-expect-failure-buffer.md` (TODO file once
  this lands).
- Concurrency mode as an explicit third axis. Stays implicit via
  reader-count detection in Phase 7's unique-root work.

## Decision points (decide before starting Phase A)

1. **`mvcc_upsert` one-shot semantics**: keep as a top-level session method
   that internally allocates a ver, or rename it to make the version
   ownership obvious? Default: introduce `mvcc_upsert_one` for the legacy
   single-key API; keep `mvcc_upsert` (taking no version arg) for
   transaction-scope callers.

2. **Where does the COW write path read the txn version?** The cleanest
   answer: `tree_context::txn_version()` reads from `_root.ver()`. But
   `tree_context` is constructed from a `smart_ptr` that may have been
   set up before the ver was allocated (esp. in `expect_failure`). If a
   `tree_context` is built and then the working root's ver changes,
   subsequent calls must re-read from the (mutated) `_root` — verify
   that's how the code works today, or update the working-root smart_ptr
   in place.

3. **Phase D in-place memcpy alignment**: do we need atomicity guarantees
   for concurrent readers that are walking the chain? If a reader is
   iterating versions in `find_version` while we memcpy bytes, are they
   guaranteed to see either the old or the new value, never a torn read?
   Verify the read path's invariants. If torn reads are possible, the
   coalesce-in-place needs a different mechanism (e.g., write a new offset
   slot atomically and update the entry).

These three should be answered by reading the relevant code before writing
the implementation, not by guessing.
