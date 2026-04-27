---
id: transaction-contract-implementation-gap-plan
title: Transaction contract implementation gap checklist
status: in_progress
priority: high
area: psitri
agent: ~
branch: main
created: 2026-04-26
depends_on: [transaction-owned-tree-state]
blocks: []
---

## Purpose

This file tracks the places where the current implementation deviates from the
user-facing transaction and cursor contract. It is a migration checklist, not a
second API design.

The contract is defined by:

- `docs/getting-started/transaction-contract.md`
- `docs/getting-started/api.md`
- `docs/getting-started/quickstart.md`
- `.issues/transaction-owned-tree-state.md`

Implementation work should make the code match those documents. If the desired
developer experience changes, update the docs first and then update this
checklist.

## Checklist Rules

- API/form gap: the public shape, names, types, or ownership model differ from
  the docs.
- Function gap: the public behavior, lifetime, allocation profile, or
  performance path differs from the docs.
- Test gap: the contract lacks an explicit regression or API test.
- No deprecated compatibility surface is required. This is green code, so old
  interfaces should be removed or made internal when the replacement is ready.
- Public behavior tests must use the documented public API unless the specific
  invariant cannot be expressed through that API. Low-level hooks are allowed
  only in explicitly internal tests or named test-support diagnostics, with a
  short reason for why the public API is insufficient.

## Current High-Level Drift

- [ ] Replace the old raw-root public model with the documented copyable
      retained `tree` handle model.
- [ ] Replace public `write_cursor` construction with documented
      `create_temporary_tree()` plus `start_write_transaction(tree, mode)`.
- [ ] Split explicit snapshot cursors from cheap current-state cursors.
- [ ] Make read-write transactions expose read-own-writes APIs without pinning a
      snapshot by accident.
- [ ] Pool heavyweight cursor storage so short-lived cursors do not allocate or
      copy large cursor objects repeatedly.
- [ ] Simplify transaction-owned tree state so nested transactions and subtree
      transactions commit a final tree back to a target, instead of using a
      broad vector-of-change-sets model.
- [ ] Update MDBX shim cursor and transaction behavior to use the same cheap
      current-state reads and pooled cursor storage.
- [x] MDBX shim first alignment slice: write cursors/read helpers now use the
      active DWAL transaction for read-own-writes, cursor handle storage is
      pooled on the MDBX transaction, and DBI catalog mutations are rollbackable.
- [x] Update the migrated public/API tests so the documented developer
      experience is the executable contract. Internal low-level tree, MVCC, and
      fuzz tests still intentionally use raw roots/write cursors until those
      hooks move behind an internal boundary.
- [ ] Audit every test that includes implementation/detail headers or uses raw
      roots/cursors, then either migrate it to the public API or label it as an
      internal invariant test with a test-support boundary.

## Low-Level Test Audit And Migration

Current implementation evidence:

- Many public-contract tests still include `database_impl.hpp`,
  `write_session_impl.hpp`, or `read_session_impl.hpp` for setup and diagnostic
  helpers, even when their mutation/read flows now use public `tree`,
  `write_transaction`, and `snapshot_cursor` APIs.
- `fuzz_tests.cpp`, `tree_ops_tests.cpp`, `mvcc_tests.cpp`,
  `per_txn_version_tests.cpp`, and parts of `dwal_tests.cpp` actively model
  production behavior through `create_write_cursor()`, `write_cursor`,
  `read_cursor()`, `tree_context`, `take_root()`, raw SAL smart pointers, or
  direct version-cursor construction.
- `tree_context_tests.cpp`, `write_buffer_tests.cpp`, `smart_ptr_tests.cpp`,
  `freed_space_tests.cpp`, recovery tests, truncate tests, and DWAL WAL/merge
  tests exercise storage internals that may need to remain internal, but should
  not look like endorsed application API.

Rules:

- Public-contract tests should compile from documented public headers and use
  the same developer experience shown in the docs.
- Public-contract tests may call named test-support diagnostics for things like
  leak checks, reachable-node counts, ref-count assertions, and allocator
  inspection. Those helpers should live behind an obvious test-only boundary
  instead of making each public test include implementation headers directly.
- Internal-invariant tests may use raw roots, `write_cursor`, `tree_context`,
  direct version cursors, or allocator hooks only when the invariant is below
  the public API boundary.
- Fuzz and regression tests default to public API unless the fuzz target is
  explicitly the internal tree engine, MVCC version chain, allocator, DWAL WAL,
  or merge layer.
- Examples, docs, and public API tests must never normalize
  `create_write_cursor()`, `take_root()`, raw SAL roots, or implicit
  `read_cursor()` snapshots as user workflows.

Checklist:

- [ ] Add a `psitri/test_support` boundary for diagnostics currently reached
      through implementation headers, including leak/ref-count checks,
      reachable-node counts, tree stats, and forced maintenance helpers.
- [ ] Remove direct implementation/detail header includes from public-contract
      tests once equivalent test-support helpers exist.
- [ ] Add a public-surface audit target that fails docs, examples, and
      public-contract tests if they use banned workflows such as
      `create_write_cursor()`, `take_root()`, raw `alloc_header` roots, or
      implicit writer `read_cursor()`.
- [ ] Tag or relocate true internal tests so their filenames, tags, or helper
      namespaces make it clear they are not public API examples.
- [ ] Require every remaining low-level test to state the internal invariant it
      is testing, either in the test name, a nearby comment, or a local helper
      name.

File migration checklist:

- [x] Public/API migration cluster operation paths:
      `public_api_tests.cpp`, `count_keys_tests.cpp`, `cursor_tests.cpp`,
      `edge_case_tests.cpp`, `coverage_gap_tests.cpp`,
      `range_remove_tests.cpp`, `integrity_tests.cpp`,
      `tree_handle_tests.cpp`, `database_test.cpp`, `subtree_tests.cpp`, and
      `zip_tests.cpp` now exercise the documented public API for normal
      mutations, snapshots, and subtree movement.
- [ ] Public/API migration cluster include cleanup: replace direct
      implementation-header diagnostics with `psitri/test_support` helpers.
- [ ] `fuzz_tests.cpp`: split into public API fuzz targets for real developer
      workflows and internal fuzz targets for raw tree-engine behavior. The
      transaction/shared-mode fuzz cases are high priority because they are
      currently failing and still model state through raw write cursors.
- [ ] `tree_ops_tests.cpp`: split public observable tree behavior from internal
      node-shape, collapse, split, unique-ref, and COW invariant tests. Migrate
      public behavior cases to `tree`/`write_transaction`; keep node-shape
      checks internal.
- [ ] `mvcc_tests.cpp` and `per_txn_version_tests.cpp`: split public MVCC
      snapshot/version contract tests from internal value-node chain,
      reclamation, epoch, and defrag tests. Public tests should use sessions,
      transactions, `tree`, and `snapshot_cursor`; chain-shape tests can stay
      internal.
- [ ] `dwal_tests.cpp`: split raw DWAL/WAL/merge-layer tests from public or
      MDBX-facing behavior. User-facing read-own-writes, multi-root, subtree,
      and recovery contracts should be covered through the public shim/API
      where possible.
- [ ] `tree_context_tests.cpp` and `write_buffer_tests.cpp`: mark as
      internal-only storage tests and keep them away from public examples.
- [ ] `smart_ptr_tests.cpp`, `freed_space_tests.cpp`, `recovery_tests.cpp`,
      `deep_recovery_tests.cpp`, and `truncate_tests.cpp`: audit each case for
      setup/readback that can move to public API while keeping allocator and
      storage diagnostics internal.
- [ ] `multi_writer_tests.cpp`: verify all writer/session behavior visible to
      users is expressed through the documented public sessions, transactions,
      and snapshot APIs.

Required tests/tools:

- [ ] Public-header compile test that includes only documented public headers
      and exercises the API shown in `transaction-contract.md`.
- [ ] Static/audit test that public docs, examples, and public-contract tests do
      not use low-level workflows.
- [ ] Internal test-support compile test proving internal diagnostics remain
      available without leaking into the end-user surface.
- [ ] Fuzz split verification: public fuzz targets pass through only documented
      API, while internal fuzz targets are clearly tagged and justified.

## Public `tree` API Gaps

Remaining implementation evidence:

- `write_cursor::get_subtree()` still exposes
  `sal::smart_ptr<sal::alloc_header>` through the low-level write cursor API.
- `tree` still exposes raw-root escape hatches (`raw_root()`, `copy_root()`,
  `take_root()`, conversion operators) for compatibility with internal tests
  and migration code.
- `write_cursor` remains public, so raw-root workflows are not fully
  internalized yet.

Checklist:

- [x] Add public `psitri::tree` as the copyable retained tree identity returned
      by `create_temporary_tree()`, `get_root()`, and `get_subtree()`.
- [x] Ensure copying `tree` retains the same tree identity and moving `tree`
      transfers the handle without extra retain/release traffic where possible.
- [x] Do not add `clone_handle()` or similar API. Copy the `tree` handle when a
      retained copy is desired.
- [x] Keep `tree` non-mutating. Mutation starts through
      `write_session::start_write_transaction(tree, mode)`.
- [x] Change `read_session::get_root(uint32_t)` to return `tree`.
- [x] Change `write_session::get_root(uint32_t)` to return `tree` if it remains
      public through inheritance or explicit exposure.
- [x] Change `transaction::get_subtree(key)` to return `tree`.
- [ ] Change cursor subtree APIs that expose public tree identity to return
      `tree`, not raw SAL smart pointers.
- [x] Change `write_session::set_root(uint32_t, tree, sync_type)` to accept
      `tree` by value.
- [x] Change `transaction::upsert_subtree(key, tree)` and sorted variants to
      accept `tree` by value.
- [x] Make storing by copy and storing by `std::move()` both legal and
      documented: copy retains a snapshot, move is the one-shot fast path.
- [ ] Keep the cycle footgun explicit in docs and tests: storing a tree inside
      itself or through an indirect cycle can leak storage.

Required tests:

- [x] Compile/API test: `create_temporary_tree()` returns a copyable `tree`.
- [x] Compile/API test: `get_root()` and `get_subtree()` return `tree`, not raw
      SAL smart pointers.
- [x] Compile/API test: `upsert_subtree(key, tree)` and `set_root(root, tree)`
      accept copied and moved trees.
- [x] Negative compile/API test or public-header audit: no public
      `clone_handle()` API.
- [x] Functional test: copying a `tree` preserves the original snapshot after a
      moved or copied tree is edited and stored elsewhere.
- [ ] Functional or diagnostic test: document and, if practical, detect obvious
      self-cycle attempts without pretending general cycle prevention is solved.

## Temporary Tree And Targetless Write Transaction Gaps

Remaining implementation evidence:

- `write_session` exposes `create_write_cursor()` and
  `create_write_cursor(root)` as the low-level way to build detached trees.
- `create_write_cursor()` currently returns `std::make_shared<write_cursor>`,
  which allocates a large cursor object for short builder sessions.
- The documented `create_temporary_tree()`, `write_transaction`, and
  `start_write_transaction(tree, mode)` APIs exist, but the old public cursor
  construction path still needs to be internalized.

Checklist:

- [x] Add `write_session::create_temporary_tree()` returning an empty detached
      `tree`.
- [x] Add `write_session::start_write_transaction(tree, tx_mode)` returning a
      targetless `write_transaction`.
- [x] Add `write_transaction::get_tree()` returning the current edited tree.
- [x] Ensure targetless write transactions never publish to a root slot or
      parent key by themselves.
- [ ] Remove public `create_write_cursor()` from the end-user API when the
      replacement is ready, or move it behind an internal/detail boundary.
- [x] Ensure detached trees can be stored with `upsert_subtree()` or
      `set_root()` without an extra commit step.
- [x] Ensure `start_write_transaction(tree)` can edit trees from
      `create_temporary_tree()`, `get_root()`, and `get_subtree()`.
- [ ] Preserve the existing version protocol: `expect_success` makes a unique
      working root at transaction start, and `expect_failure` delays that until
      a persistent tree touch or merge.

Required tests:

- [x] API test: build a detached tree, edit it with
      `start_write_transaction(std::move(tree))`, retrieve it with `get_tree()`,
      and store it under a parent key with `upsert_subtree()`.
- [x] API test: edit a root snapshot with `start_write_transaction(root_tree)`,
      retrieve the edited tree, and publish it with `set_root()`.
- [x] Functional test: a targetless write transaction does not affect any root
      or parent subtree until the returned `tree` is explicitly stored.
- [ ] Functional test: aborting or destroying a targetless write transaction
      drops unreturned/unpublished state.
- [ ] Versioning test: `expect_success` and `expect_failure` still follow the
      documented `make_unique_root()` and delayed materialization behavior.

## Read Session And Snapshot Cursor Gaps

Current implementation evidence:

- `read_session` exposes `create_cursor(root_index)`.
- The docs use `snapshot_cursor(root_index)` to make pinning explicit.
- Existing cursors retain a root smart pointer, which is correct for snapshots
  but must not be hidden behind names that look like cheap current reads.

Checklist:

- [ ] Rename or replace public `read_session::create_cursor(root)` with
      `read_session::snapshot_cursor(root)`.
- [x] Keep snapshot cursor semantics explicit: snapshot cursors pin the stable
      committed tree they are opened on.
- [ ] Ensure read-only sessions expose snapshot reads only, never current
      writer state.
- [x] Update migrated public/API examples and tests to use
      `snapshot_cursor()` everywhere a stable reader snapshot is intended.
- [ ] Remove old public `create_cursor()` naming once all call sites migrate.

Required tests:

- [x] API test: `read_session::snapshot_cursor(root)` exists and iterates a
      stable committed view.
- [x] Functional test: a read-only snapshot cursor continues to see its original
      state after later writes commit.
- [ ] Public-header audit: no end-user docs or quickstarts call
      `read_session::create_cursor()`.

## Current-State Cursor Gaps

Current implementation evidence:

- `write_cursor::read_cursor()` returns `cursor(_ctx.get_root())`.
- `transaction::read_cursor()` forwards to `write_cursor::read_cursor()`.
- `tree_handle::read_cursor()` also forwards to `write_cursor::read_cursor()`.
- `cursor` stores a retained root smart pointer plus large fixed buffers and
  path arrays.
- The name `read_cursor()` hides snapshot-like pinning behavior in writer code.

Checklist:

- [ ] Add documented current-state cursor APIs for write transactions and
      mutable tree scopes. The default writer cursor path must read current
      write state and read its own writes.
- [ ] Ensure current-state cursors borrow transaction-owned tree state instead
      of retaining a root smart pointer.
- [ ] Make current-state cursors owner-bound: they must not outlive their
      transaction/write transaction/tree edit owner.
- [ ] Invalidate current-state cursors on any write that can change the viewed
      tree state.
- [ ] Add debug-generation checks so using an invalidated current-state cursor
      is caught loudly in debug builds.
- [ ] Keep stable writer snapshots available only through an explicit
      `snapshot_cursor()` API.
- [ ] Ensure read-only `cursor` and current-state writer cursor are allowed to
      have different internal storage if that keeps the contract clearer and
      cheaper.

Required tests:

- [ ] Functional test: a write transaction cursor sees inserts, updates, and
      removes performed earlier in the same transaction.
- [ ] Functional test: current-state cursor creation inside a writer does not
      retain/pin the root in a way that forces avoidable COW on later writes.
- [ ] Debug test: using a current-state cursor after a mutating operation trips
      an invalidation assertion or returns a defined error.
- [ ] Snapshot contrast test: `tx.snapshot_cursor()` intentionally pins a stable
      view and does not see later writes in the same transaction.

## Cursor Allocation And Pooling Gaps

Current implementation evidence:

- `cursor` is a large value type with retained root, key buffer, and path array.
- Cursor copy and move constructors copy key/path storage.
- `write_session::create_write_cursor()` heap-allocates each write cursor with
  `std::make_shared<write_cursor>()`.
- MDBX cursor open used to allocate `MDBX_cursor` with `new` and owned cursor
  state through `std::unique_ptr<cursor_state>`. The shim now stores cursor
  handles in `MDBX_txn::cursor_storage`, returns closed handles through an
  intrusive `next_free` list, and stores `cursor_state` inline with
  `std::optional`.

Checklist:

- [ ] Add reusable cursor storage owned by the session or transaction owner.
- [ ] Give reusable cursor objects an intrusive free-list link, for example
      `next_free`, so returning a cursor does not allocate a separate list node.
- [ ] Add RAII cursor wrappers that return storage to the owning free list on
      destruction.
- [ ] Keep pooled cursor wrappers move-only unless copy semantics are required
      by a specific public contract.
- [ ] Avoid hidden heap allocation in the common cursor open/close path after
      warmup.
- [ ] Bound or instrument cursor pool growth so accidental cursor leaks are
      visible during tests and diagnostics.
- [ ] Ensure cursor pooling works for writer current-state cursors and MDBX shim
      cursors.
- [x] MDBX shim cursor handle/state pooling works for repeated
      `mdbx_cursor_open()`/`close()` in one transaction.
- [ ] Decide whether read-only snapshot cursors use the same pool or a separate
      simpler pool, then document that internally.

Required tests:

- [ ] Allocation-count benchmark/test: repeated short cursor sessions reuse
      cursor storage after warmup.
- [ ] Stress test: open/close many current-state cursors in one write session
      without unbounded heap growth.
- [ ] Lifetime test: destroying cursor RAII wrappers returns storage to the
      correct session/transaction pool.
- [ ] MDBX benchmark/test: repeated `mdbx_cursor_open()`/`close()` avoids
      repeated heavyweight allocations after warmup.

## Point Lookup, Zero-Copy Reads, And Ordered Search Gaps

Remaining implementation evidence:

- `write_cursor::get<T>` constructs a temporary `cursor` and copies values.
- Transaction internals sometimes use `get<std::string>(key)` to check whether
  an existing key is present.
- The zero-copy callback APIs exist, but the exact-key fast path and internal
  copy-avoidance audit are not complete.

Checklist:

- [x] Add zero-copy `get(key, lambda)` to `cursor`, `tree`, `transaction`, and
      `write_transaction` where the docs promise it.
- [x] Ensure `get(key, lambda)` is templated/inlined and does not route through
      `std::function`.
- [ ] Document and enforce that the `value_view` is valid only for the callback.
- [ ] Audit internal existence checks so known-key paths do not copy values just
      to decide whether `insert`, `update`, or remove semantics apply.
- [ ] Add exact-key fast paths for `get` and `update` when the caller knows the
      key should exist.
- [ ] Keep `lower_bound()` and cursor positioning as ordered-search APIs, not
      the default point-get implementation path.
- [ ] Ensure `upsert()` remains the general insert-or-update path and does not
      hide the faster `update()` path for known-present keys.

Required tests:

- [x] API test: `tx.get(key, [](value_view) { ... })` compiles and returns a
      boolean found/not-found result.
- [x] API test: `tree.get(key, lambda)` and `write_transaction.get(key, lambda)`
      compile.
- [ ] Lifetime test: the zero-copy callback view cannot be used outside the
      callback in documented safe examples.
- [ ] Performance regression test: exact-key `get` avoids ordered cursor
      allocation/search where the fast hash lookup applies.
- [ ] Performance regression test: `update()` avoids the extra ordered search
      work required by `upsert()` when the key is known to exist.

## Transaction-Owned State And Commit Target Gaps

Current implementation evidence:

- `transaction` stores mutable trees in `_change_sets`, a `std::vector`.
- Each `change_set` may hold `std::optional<write_cursor>`,
  `std::optional<write_buffer>`, an optional root index, and an optional parent
  link with `std::string key`.
- Transaction commit/rollback uses `std::function` callbacks.
- Subtree commit walks the vector bottom-up to store child roots back into
  parents.
- This structure hides heap allocations in `std::vector`, `std::string`, and
  `std::function`, making the hot path harder to audit.

Checklist:

- [ ] Replace broad vector-of-change-sets ownership with explicit
      transaction-owned tree state objects whose lifetime is controlled by the
      owning transaction object.
- [ ] Model root transactions, subtransactions, and subtree transactions with
      the same rule: commit writes final tree state to a target; abort drops it.
- [ ] Keep targetless `write_transaction(tree)` separate: it has no commit
      target and returns final state through `get_tree()`.
- [ ] Represent commit targets explicitly and allocation-auditably:
      top-root slot, parent tree plus key, parent transaction, or no target.
- [ ] Avoid `std::function` in transaction commit/rollback paths. Use concrete
      target state or small enums/structs.
- [ ] Avoid heap-allocating parent keys in hot transaction state when possible.
      Use inline/small key storage or a transaction-owned arena with visible
      accounting.
- [ ] Preserve existing multi-root semantics: top roots are the main place
      independent updates happen because root slots are not stored COW inside
      another tree.
- [ ] Ensure multi-root transactions publish all opened top roots atomically and
      keep each root's unique-working-state lifecycle independent.
- [ ] Keep all mutable transaction state owned by the transaction object to
      prevent shared pointers from accidentally extending transaction lifetime.

Required tests:

- [ ] Functional test: nested subtransaction commit stores final state back to
      its parent transaction.
- [ ] Functional test: nested subtransaction abort leaves parent state unchanged.
- [ ] Functional test: subtree transaction commit upserts final subtree state
      back to the parent key.
- [ ] Functional test: subtree transaction abort leaves the parent key
      unchanged.
- [ ] Multi-root test: independent top-root writes in one transaction publish
      atomically and remain independent before commit.
- [ ] Allocation audit or benchmark: transaction construction and simple commit
      do not allocate through `std::function`, parent-link `std::string`, or
      growing vectors on the common path.

## `expect_success` And `expect_failure` Behavior Gaps

Current implementation evidence:

- `start_transaction(expect_success)` already calls `make_unique_root()` at
  transaction start.
- `expect_failure` already delays materialization until the first persistent
  tree touch through merge paths.
- Some read/range paths create ordinary cursors from persistent roots and can
  fail to represent buffered writes as the documented current write state.

Checklist:

- [ ] Keep the existing `make_unique_root()` timing for
      `expect_success`.
- [ ] Keep delayed unique-root materialization for `expect_failure`.
- [ ] Audit every read path in `expect_failure` so point reads, range cursors,
      `lower_bound()`, `upper_bound()`, `count_keys()`, and subtree reads see
      the transaction's buffered changes as current write state.
- [ ] Ensure merging a buffered change set into persistent state happens before
      operations that require a real tree root, including subtree storage and
      snapshot creation.
- [ ] Avoid naming such as `ensure_txn_version()` in public or high-level
      internal APIs. Prefer a name that describes the state transition, such as
      `make_working_root_unique()` or `materialize_working_root()`, if a helper
      is needed.

Required tests:

- [ ] `expect_failure` point read sees buffered insert/update/remove before
      commit.
- [ ] `expect_failure` lower/upper-bound iteration sees buffered changes in
      sorted order.
- [ ] `expect_failure` subtree get/upsert observes buffered parent changes.
- [ ] `expect_failure` snapshot creation first materializes the intended
      working state and then pins that state explicitly.

## Subtree Identity And Snapshot Footgun Gaps

Current implementation evidence:

- PsiTri values already encode subtree identity as `tree_id`.
- DWAL `btree_value` stores a full `sal::tree_id` for subtree values.
- Public APIs still expose raw root smart pointers rather than the documented
  `tree` handle.
- The public docs now allow users to copy and move trees deliberately, which
  means cycles are possible if users store a tree inside itself or through a
  descendant.

Checklist:

- [ ] Ensure every subtree store path consumes or retains the full tree identity
      required to keep root and version references alive.
- [ ] Ensure PsiTri, DWAL, WAL write, WAL replay, undo, compaction, and
      ref-count repair all agree on full subtree `tree_id` semantics.
- [ ] Update public subtree APIs to accept and return `tree` so users cannot
      place arbitrary raw roots without going through the documented handle.
- [ ] Document that `get_root()` and `get_subtree()` create explicit retained
      snapshots and may increase later COW work if held.
- [ ] Document that movable/copyable `tree` enables application-managed
      snapshots and copying between roots/subtrees.
- [ ] Document the cycle footgun in user docs and API comments.
- [ ] Decide whether obvious self-store can be rejected cheaply at runtime. If
      yes, add a targeted diagnostic; if no, keep the warning explicit and do
      not pretend cycles are impossible.

Required tests:

- [ ] WAL/replay test: subtree values retain and restore full `tree_id`, not
      only root address.
- [ ] Ref-count test: storing a subtree, overwriting it, removing it, and
      replaying WAL balances references for both root and version state.
- [ ] Snapshot copy test: the same `tree` can be stored in two places and both
      views stay valid until released.
- [ ] Documentation test or lint: cycle warning appears near every API example
      that demonstrates copying tree handles between roots/subtrees.

## Write Session Lifecycle Gaps

Current implementation evidence:

- Docs define write sessions as thread-owned, long-lived allocator contexts.
- Each active write session owns a 32 MB write buffer.
- Docs reserve 14 of 64 allocator session slots for backend/compaction work,
  leaving at most 50 application write sessions.
- Current API comments still describe low-level write cursors as the way to
  build detached trees.

Checklist:

- [ ] Audit implementation limits and diagnostics for the documented maximum of
      50 active application write sessions.
- [ ] Ensure backend/compactor reserved session slots are enforced or clearly
      accounted for.
- [ ] Expose diagnostics that make repeated write-session creation visible.
- [ ] Update header comments and examples so write sessions are described as
      long-lived per-thread allocator contexts.
- [ ] Remove examples that create write sessions repeatedly for small
      operations.
- [ ] Ensure cursor pools and write buffers are session-owned where the docs
      promise reuse for the life of the thread.

Required tests:

- [ ] Thread-affinity test or debug assertion: write sessions are used on the
      creating thread.
- [ ] Limit test: application write session creation fails or reports clearly
      after the documented maximum.
- [ ] Resource test: closing a write session flushes/recycles its 32 MB write
      buffer and releases cursor pool storage.

## MDBX Shim Gaps

Backend architecture decision:

- Keep one MDBX-compatible contract layer: C/C++ API, DBI catalog semantics,
  DUPSORT encoding, cursor operation state machine, value lifetime rules, error
  mapping, and compatibility tests.
- Provide two concrete storage backends under that contract:
  `direct_backend`, which uses the intended PsiTri public session/transaction
  API directly, and `dwal_backend`, which adapts the same contract to DWAL.
- Treat `direct_backend` as the correctness oracle for MDBX semantics and as
  the audit path for "can this be done through the public API without grabbing
  internal endpoints?"
- Treat `dwal_backend` as the WAL/recovery/performance implementation that must
  match the direct backend's observable behavior, plus DWAL-specific recovery
  guarantees.
- Keep backend selection explicit and allocation-auditable. Prefer concrete
  structs or template instantiation over virtual backend interfaces or opaque
  concept wrappers.

Current implementation evidence:

- `MDBX_txn` now owns `MDBX_cursor` storage, and closed cursors return to an
  intrusive transaction-local free list.
- `MDBX_cursor` now stores `cursor_state` inline with `std::optional`, avoiding
  the previous per-open state allocation.
- RW MDBX transactions now create one DWAL multi-root transaction up front and
  route RW cursors through `transaction::create_cursor(root)`, so reads and
  iteration see uncommitted writes.
- DBI creation/drop catalog writes now go through the active transaction, and
  the in-memory DBI registry rolls back on abort.
- Returned C and C++ `MDBX_val` slices now copy into transaction-owned arena
  storage so short-lived cursor helpers can close/reuse cursors without
  invalidating slices that callers keep until transaction end.
- Finalized MDBX transactions now defer destruction while managed cursors are
  still alive, so normal C++ RAII usage where `txn.commit()` happens before a
  cursor variable leaves scope invalidates cursor operations without making the
  cursor destructor touch freed transaction storage.
- Impedance mismatch: MDBX allows DBIs to be discovered lazily inside a write
  transaction, but DWAL requires the root set when the transaction starts. The
  shim currently enlists every possible MDBX root for the environment to keep
  abort/commit atomic across DBIs.
- Impedance mismatch: MDBX cursors have mutation-aware operational semantics,
  while PsiTri current-state writer cursors still need explicit invalidation
  rules after writes that can move iterator state.
- Impedance mismatch: MDBX read-only transactions imply a transaction snapshot,
  while DWAL currently exposes configurable read modes and cursor creation on
  the database, not a read-session cursor snapshot API.
- Impedance mismatch: native MDBX permits larger DUPSORT duplicate values than
  the PsiTri shim can support. The shim intentionally models DUPSORT as
  `outer_key -> duplicate-value subtree`, giving the outer key and duplicate
  value separate 1 KiB PsiTri key budgets and rejecting larger duplicate values.
- RW point reads still copy values into transaction-owned arena storage to satisfy
  `MDBX_val` lifetime requirements.

Checklist:

- [x] Pool `MDBX_cursor` and `cursor_state` storage on the MDBX transaction or
      session so open/close is cheap after warmup.
- [x] Map read-write MDBX cursors to current-state writer cursors that read
      their own writes.
- [ ] Keep read-only MDBX cursors mapped to explicit snapshot/read-mode
      cursors.
- [ ] Ensure any write through an MDBX transaction invalidates active
      current-state cursors for the affected DBI/root.
- [x] Remove accidental snapshot creation from RW paths that only need current
      transaction state.
- [ ] Avoid creating fresh merge cursors for helper checks when an existing
      pooled/current cursor can answer the question.
- [x] Keep MDBX value lifetime rules correct: returned `MDBX_val` points into
      storage that remains valid for the documented MDBX operation lifetime.
- [x] Update MDBX C++ wrapper paths to use the same pooled C cursor machinery.
- [x] Make MDBX cursor cleanup safe after `commit()`/`abort()` finalizes the
      transaction but C++ cursor RAII wrappers are still unwinding.
- [ ] Replace the "enlist all possible roots" workaround if DWAL grows an API
      for dynamic root enlistment or a cheap declared root-set builder.
- [ ] Split shared MDBX compatibility code from storage-specific backend code.
- [ ] Add a `direct_backend` implementation that uses the documented public
      PsiTri API as the reference implementation.
- [ ] Keep `dwal_backend` as the WAL-backed implementation and remove
      DWAL-specific assumptions from the shared MDBX layer.
- [ ] Run the same MDBX compatibility tests against both backends.
- [ ] Add backend parity tests for transaction abort, DBI catalog updates,
      DUPSORT navigation, value lifetime, cursor renewal, and snapshot behavior.

Required tests:

- [x] MDBX RW transaction test: `mdbx_get()` sees a prior `mdbx_put()` before
      commit.
- [x] MDBX RW cursor test: cursor iteration sees writes made earlier in the
      same transaction.
- [ ] MDBX invalidation test: using a cursor after a write that invalidates it
      returns the documented error or trips a debug assertion.
- [ ] MDBX RO snapshot test: read-only transactions do not see later commits.
- [x] MDBX allocation/perf test: repeated short cursor open/close reuses cursor
      storage after warmup.
- [x] MDBX multi-DBI abort test: writes to more than one DBI/root roll back
      together.
- [x] MDBX named DBI creation abort test: catalog and in-memory DBI registry
      roll back together.
- [x] MDBX/Silkworm value lifetime test: a slice returned by a cursor remains
      valid after that cursor is closed and its storage is reused.
- [x] MDBX/Silkworm seek-current-next test: big-endian block-key iteration does
      not repeat the seek key after `MDBX_GET_CURRENT`.
- [x] MDBX/Silkworm DUPSORT test: `GET_BOTH`, `GET_BOTH_RANGE`, `NEXT_DUP`,
      `NEXT_NODUP`, `FIRST_DUP`, and `LAST_DUP` match the compatibility
      contract.
- [x] MDBX/Silkworm RAII lifetime test: a managed cursor can still destruct
      safely after `txn.commit()` has finalized the underlying transaction.

## Silkworm Port Sync

Current implementation evidence:

- `/Users/dlarimer/psiserve-agent2/external/silkworm` is a fork on
  `feature/psitri-backend` with PsiTri as `third_party/psitri`.
- That submodule carries local psitrimdbx experiments for transaction-owned
  returned slices, direct-vs-DWAL behavior, and standalone Silkworm repros.
- The standalone dangling-slice and seek/next repros are now represented as
  ordinary PsiTri MDBX compatibility tests.
- The pushed PsiTri branch is `origin/codex-mdbx-silkworm-refactor` at
  `0d9a6d3` (`Expand psitrimdbx Silkworm compatibility`).
- A simulated merge of the pushed PsiTri branch into Silkworm's
  `silkworm-psitrimdbx-fixes` submodule branch is not clean. It conflicts in
  PsiTri transaction/DWAL headers and `libraries/psitrimdbx/src/mdbx_impl.cpp`.
- A direct apply of Silkworm's dirty local `mdbx_impl.cpp` patch onto the
  pushed PsiTri branch also does not apply cleanly. A 3-way apply reaches
  conflicts, confirming that the remaining work should be ported deliberately
  rather than copied wholesale.
- Silkworm appears to rely on psitrimdbx compatibility APIs from its
  submodule branch, including `mdbx::error::throw_exception`, `env::get_path`,
  `env::get_stat/get_info/copy/check_readers`, transaction map info/stat
  helpers, generic cursor `move()`, multi-cursor aliases, `cursor::put`,
  `cursor::erase(key,value)`, `MDBX_ENODATA`, `MDBX_COALESCE`,
  `MDBX_option_t`, and `mdbx_env_get/set_option`. These are now represented in
  the PsiTri psitrimdbx headers/implementation and covered by local MDBX tests.

Checklist:

- [x] Inspect the external Silkworm fork and confirm it uses PsiTri as a
      submodule.
- [x] Identify Silkworm's relevant psitrimdbx patches instead of copying the
      monolithic local shim experiment wholesale.
- [x] Port the Silkworm dangling-slice repro into
      `libraries/psitrimdbx/tests/mdbx_tests.cpp`.
- [x] Port the Silkworm seek/current/next repro into
      `libraries/psitrimdbx/tests/mdbx_tests.cpp`.
- [x] Port the Silkworm DUPSORT cursor pattern coverage into
      `libraries/psitrimdbx/tests/mdbx_tests.cpp`.
- [x] Push the PsiTri fixes to a branch that the Silkworm fork can consume.
- [x] Fetch the pushed PsiTri branch into the Silkworm submodule.
- [x] Simulate merging the pushed PsiTri branch with Silkworm's
      `silkworm-psitrimdbx-fixes` submodule branch and record the conflict
      shape.
- [ ] Update the Silkworm `third_party/psitri` submodule to that pushed commit.
      Blocked on preserving or retiring the dirty local submodule patch in
      `/Users/dlarimer/psiserve-agent2/external/silkworm/third_party/psitri`
      (`libraries/psitrimdbx/src/mdbx_impl.cpp` plus two repro test files).
- [ ] Re-run the relevant Silkworm build/tests or document any local
      dependency/build blockers.
- [x] Port the Silkworm-required psitrimdbx API compatibility surface on top of
      the pushed PsiTri branch before moving the submodule pointer.
- [ ] Port or replace Silkworm's case-study/stress benchmark if it is still the
      intended Ethereum workload for validation.
- [ ] Decide how the Silkworm direct-COW-default experiment maps onto the
      planned `direct_backend` / `dwal_backend` split. Do not keep it as a
      monolithic `mdbx_impl.cpp` mode switch unless that becomes the explicit
      architecture.
- [ ] Evaluate Silkworm's DWAL pending-byte cap as a targeted safety guard for
      the eventual DWAL backend.
- [ ] Audit remaining Silkworm submodule patches for relevance after the
      PsiTri fixes land. Expected categories: redundant repro workarounds,
      useful direct-backend experiments, DWAL pending-byte safeguards, API
      compatibility shims, and diagnostics that should stay local or move into
      test-support tooling.

## DWAL Alignment Gaps

Current implementation evidence:

- DWAL `btree_value` already stores a `sal::tree_id` for subtree values.
- DWAL transaction APIs still expose subtree writes in terms of raw `tree_id`.
- The user-facing PsiTri docs are now written around `tree`, not raw tree IDs.

Checklist:

- [ ] Verify DWAL WAL writer and reader persist full subtree `tree_id` and
      replay it exactly.
- [ ] Decide whether DWAL remains an internal/raw `tree_id` layer or gets a
      public wrapper aligned with `tree`.
- [ ] Ensure DWAL read-own-writes behavior matches the MDBX shim expectations
      for RW transactions.
- [ ] Ensure DWAL point lookup keeps its fast path and does not require opening
      a merge cursor for exact-key reads.
- [ ] Ensure DWAL range scans still use merge cursor semantics where ordered
      iteration is required.

Required tests:

- [ ] DWAL WAL/replay test for subtree values with root and version identity.
- [ ] DWAL exact-key read-own-writes test.
- [ ] DWAL range scan test that merges committed state and pending writes in
      sorted order.
- [ ] DWAL/MDBX integration test for subtree values if MDBX exposes subtree
      behavior through any compatibility path.

## Public Documentation And Examples Gaps

Current implementation evidence:

- `docs/getting-started/transaction-contract.md` and
  `docs/getting-started/api.md` describe the desired API.
- Some architecture docs and header comments still mention old
  `write_cursor`, `tree_handle`, or raw-root workflows.
- Existing code examples may still use old API names.

Checklist:

- [ ] Search docs for public `create_write_cursor()` examples and replace them
      with `create_temporary_tree()` plus `start_write_transaction(tree)`.
- [ ] Search docs for public `read_cursor()` examples and replace snapshot uses
      with `snapshot_cursor()`.
- [ ] Search docs for public raw SAL smart-pointer subtree examples and replace
      them with `tree`.
- [ ] Keep `database::open(dir, open_mode)` as the normal example and
      `database::create(dir)` as the create-only helper.
- [ ] Ensure examples warn that `get<std::string>()` copies and that
      `get(key, lambda)` is the zero-copy path.
- [ ] Ensure examples state that long-running zero-copy callbacks can delay
      compaction/recycling while the view is held.
- [ ] Ensure examples mention `get()`/`update()` fast paths versus
      `lower_bound()`/`upsert()` ordered/general paths.
- [ ] Ensure examples demonstrate creating a transient tree with no top root
      and storing it with `upsert_subtree("key", std::move(tree))`.

Required tests:

- [ ] Documentation build passes after each API migration slice.
- [ ] Snippet compile tests cover the quickstart and transaction-contract
      examples.
- [ ] Public docs do not mention deprecated API names after replacement.

## Header And Public Surface Removal Gaps

Current implementation evidence:

- `tree_handle` is public in `transaction.hpp`.
- `transaction_frame_ref` exposes a separate save-point-like public surface.
- `write_cursor` is public in `write_cursor.hpp` and included by
  `write_session.hpp`.
- Public APIs expose `sal::smart_ptr<sal::alloc_header>` in several places.

Checklist:

- [ ] Remove or internalize `tree_handle` once `tree` plus scoped transaction
      objects cover the documented use cases.
- [ ] Replace `transaction_frame_ref` public docs/API with the current
      transaction/subtransaction model, where a save point is just another
      transaction that commits final state back to its parent.
- [ ] Remove or internalize public `write_cursor` construction after
      `write_transaction` and cursor pooling are in place.
- [ ] Remove public raw SAL smart-pointer parameters and returns from
      end-user-facing transaction/session APIs.
- [ ] Keep low-level SAL/root APIs only under explicit internal/detail headers
      or diagnostic namespaces.
- [ ] Update exported headers and install rules so the public surface matches
      the docs.

Required tests:

- [ ] Public-header compile test using only documented headers and types.
- [ ] Public-symbol/API audit showing no end-user raw
      `sal::smart_ptr<sal::alloc_header>` transaction/session APIs remain.
- [ ] Internal tests still have access to required low-level hooks through
      explicit detail/test-only boundaries.

## Suggested Migration Order

- [ ] Phase 1: Freeze docs as the reference contract and add compile tests for
      the documented API names, initially marked expected-fail if the test
      harness supports that.
- [ ] Phase 2: Introduce `tree` and update root/subtree APIs to pass `tree` by
      value.
- [ ] Phase 3: Introduce targetless `write_transaction(tree)` and
      `get_tree()`, then migrate detached-tree examples off `write_cursor`.
- [ ] Phase 4: Split explicit `snapshot_cursor()` from writer current-state
      cursors.
- [ ] Phase 5: Add cursor pooling and RAII return wrappers for PsiTri writer
      cursors.
- [ ] Phase 6: Simplify transaction-owned tree state and commit targets,
      preserving existing version materialization rules.
- [ ] Phase 7: Split MDBX compatibility code into one shared contract layer
      with `direct_backend` and `dwal_backend` concrete implementations.
- [ ] Phase 8: Update MDBX backends to use pooled current-state cursors for RW
      transactions and explicit snapshots for RO transactions.
- [ ] Phase 9: Finish DWAL alignment checks, public surface removal, and
      documentation snippet tests.

## Latest Verification

- [x] `cmake --build build` passed after the current public API/test migration
      slice.
- [x] Migrated public/API cluster passed:
      `build/bin/psitri-tests "[public-api],[count_keys],[cursor],[edge_case],[update_value],[coverage],[database],[range_remove],[zip],[integrity],[tree_handle],[subtree]" --colour-mode none`
      with 220 test cases and 793,047 assertions passing.
- [x] MDBX compatibility test target passed:
      `build/bin/mdbx-tests -r compact --colour-mode none` with 49 test cases
      and 11,144 assertions passing. This verifies the current compatibility
      behavior plus MDBX cursor pooling, multi-DBI abort, DBI catalog abort,
      Silkworm API surface, and managed-cursor/transaction-finalization
      coverage.
- [x] DWAL focused slice passed:
      `build/bin/psitri-tests "[dwal]" -r compact --colour-mode none` with 154
      test cases and 2,275 assertions passing.
- [ ] Full suite still has known failures after the migration slice:
      606 test cases run, 599 passed, 7 failed. The failures are currently in
      fuzz transaction/shared-mode value-size checks and
      `deep_recovery_tests.cpp` insert-remove-reinsert recovery. These are now
      tracked as high-priority audit targets because the fuzz failures are in
      tests that still drive production-like behavior through raw write
      cursors.

## Done Criteria

- [ ] Public docs, public headers, and quickstart examples describe the same
      API.
- [x] A user can create a long-lived write session, create a detached tree,
      edit it, get the resulting tree, and store it under a root or subtree
      without touching raw roots or write cursors.
- [ ] Writer point reads and current-state cursors read their own writes without
      opening implicit snapshots.
- [ ] Snapshot pinning is explicit through `snapshot_cursor()` or copyable
      retained `tree` handles.
- [ ] Cursor creation is cheap after warmup in PsiTri and the MDBX shim.
- [ ] Multi-root transactions preserve top-root independence and atomic publish.
- [ ] `expect_success` and `expect_failure` retain the existing unique-root
      materialization semantics.
- [ ] Tests cover API shape, read-own-writes behavior, snapshot behavior,
      subtree identity, cursor invalidation, and cursor allocation reuse.
- [ ] Public-contract tests, examples, and docs do not expose low-level
      implementation workflows as user-facing patterns.
