---
id: transaction-owned-tree-state
title: Transaction-owned tree state, cheap cursors, and explicit snapshots
status: proposed
priority: high
area: psitri
agent: ~
branch: main
created: 2026-04-26
depends_on: [transaction-refactor]
blocks: []
---

## Summary

The MVCC work is on `main`. Build this migration from `main`, preserving the
existing transaction/version protocol:

- `start_transaction(root, tx_mode::expect_success)` already calls
  `make_unique_root()` up front. The transaction's working root carries its
  private version before any COW mutation.
- `tx_mode::expect_failure` already defers that transition. The first
  persistent tree touch materializes the transaction version, primarily through
  `merge_buffer_to_persistent()`, just before replaying the buffered change set.
- This plan must not re-invent version allocation. It should make reads and
  cursor lifetimes cooperate with the protocol above.

The larger model:

- Anything stored in a top-level root slot is committed state. It is implicitly
  shared with current and future readers.
- Starting a transaction on that root creates transaction-owned working state.
  For `expect_success`, that means immediately creating a versioned working
  root. For `expect_failure`, that means keeping buffered state until a
  persistent tree touch forces version materialization.
- The longer a transaction runs, the more useful this becomes: once COW has
  produced transaction-owned branches, later writes to those branches can update
  in place under SAL's normal ownership rules.
- The transaction object should own all mutable transaction state. Handles,
  cursors, subtree scopes, and nested transactions should borrow from that
  owner or move state back into it; they should not use shared ownership that
  can accidentally keep transaction state alive after the transaction lifetime.

The immediate performance problem is that cursors are large, short-lived
objects, and APIs that look like "just read" often retain a root snapshot.
Retaining the root is semantically correct for snapshot reads, but it defeats
the unique-root fast path when the caller only wanted a quick read inside an
active writer.

## Goals

1. Cursor creation is fast and allocation-light after warmup.
2. Write transactions and write cursors expose a read interface that reads their
   own writes without requiring callers to construct snapshot cursors.
3. APIs make snapshot pinning explicit, because snapshot pinning is the thing
   that can force later COW.
4. The cheap path is correct by construction: transaction-local read cursors
   view the current write state and are invalidated by the next write. Root
   retention happens only through explicit snapshot cursors or explicit
   copyable retained `tree` handles.
5. The MDBX shim benefits from the same cursor pooling and read-own-writes
   behavior, instead of opening fresh heavyweight cursors for short sessions.
6. Transaction state lifetimes are enforced by ownership: each active tree
   scope owns its working root, buffer, cursor pool, and invalidation counters;
   child scopes are stack/RAII objects with non-owning parent pointers, not
   heap-owned children.
7. Nested transactions and subtree transactions use the same machinery: commit
   moves a final tree state back to a parent target; abort drops the child.
8. End-user documentation defines the contract first. Implementation details
   are valid only if they satisfy the documented developer experience.

## Public Contract

The reference user-facing contract for this refactor is
`docs/getting-started/transaction-contract.md`.

That document is the API promise to test against:

- Normal examples open databases with `database::open()` and explicit
  `open_mode`; `database::create()` is only the create-only helper.
- `tx.get(key, lambda)` and `tx.cursor()` inside a write transaction read
  current write state, including uncommitted writes.
- `get(key, lambda)` is the zero-copy read contract. The view is valid only
  for the lambda call; long-running callbacks can delay compaction/recycling.
  The callback path must be templated/inlined or otherwise allocation-free; do
  not route it through `std::function`.
- `get<std::string>(key)` and `get(key, buffer*)` are copying convenience APIs.
- Exact-key `get()` APIs are the fast path when the caller knows the key;
  `lower_bound()` is for ordered positioning and range scans.
- `update()` is the fast path when the caller knows the key should exist;
  `upsert()` is the general insert-or-update path.
- `create_temporary_tree()` builds a detached `tree`. It is not visible through
  any top-level root until it is stored with `upsert_subtree()` on a
  transaction that commits, or stored with `set_root()`.
- `start_write_transaction(tree)` edits a tree without giving the transaction a
  root slot or parent key to publish into. The caller asks the transaction for
  the resulting `tree` with `get_tree()`, then stores that tree explicitly.
- `get_root()` and `get_subtree()` may expose explicit copyable `tree` smart
  pointers so applications can manage their own snapshots and copy tree
  identities from place to place.
- `upsert_subtree()` and `set_root()` accept copyable `tree` smart pointers.
  Moving a tree is recommended for one-shot stores to avoid extra reference-count
  traffic, but copying is legal and retains the same tree identity. This is a
  deliberate power-user API and must be documented as a footgun: storing a tree
  inside itself, inside a descendant, or in an indirect cycle can leak storage.
- Snapshot pinning is explicit: `snapshot_cursor()` pins a stable cursor view,
  and `get_root()` / `get_subtree()` pin copyable tree handles.
- Current-state cursors are cheap, pooled, owner-bound, and invalidated by
  mutation of their owning tree.
- Subtransactions and subtree transactions are scoped transaction objects that
  commit final state back to a parent target.
- Multi-root transactions expose independent top-root handles and publish write
  roots atomically.
- The MDBX shim maps read-write transaction cursors to current-state cursors and
  read-only transactions to snapshot cursors.
- Write sessions are long-lived, thread-owned allocator contexts: at most 50
  active application write sessions, with 14 of the 64 allocator session slots
  reserved for backend and compaction work.
- Each write session owns a 32 MB write buffer that is flushed/recycled when the
  session closes; transaction and cursor APIs should make it natural to reuse
  sessions rather than churn them.

Phase 1 of the migration is to make that document good enough to affirm. Later
implementation phases should be judged by whether the code can satisfy and test
that public contract without hidden allocations or accidental snapshot pinning.

## Current Shape

- `cursor` is a fat value type with inline key/path storage and memcpy-heavy
  copy/move constructors.
- `read_session::create_cursor(root)` obtains a retained root snapshot.
- `write_cursor::read_cursor()` constructs `cursor(_ctx.get_root())`, which
  retains the current working root even when the caller only wanted a temporary
  read.
- `transaction::read_cursor()` forwards to `write_cursor::read_cursor()`, so an
  innocent transaction read cursor also has snapshot-like lifetime effects.
- In `expect_failure`, point reads check the write buffer first, but cursor/range
  reads are still rooted in the persistent cursor view and need an overlay view
  to read buffered writes correctly.
- The transaction structure is a flat vector of change sets plus parent links,
  followed by a bottom-up subtree commit scan. That works, but it is more
  complicated than the recursive model: child transactions can simply commit
  their final state back into the parent target.
- `transaction` currently exposes and stores value-like cursors/handles in ways
  that make lifetime boundaries blurry. The target shape is one owner for
  mutable state, with non-owning handles and RAII cursor wrappers.
- `mdbx_cursor_open()` allocates an `MDBX_cursor`, allocates `cursor_state`, and
  creates a DWAL cursor. RW cursors should prefer the transaction's own read
  view so they see uncommitted writes and avoid unnecessary snapshot churn.

## Heap Allocation Audit Rules

The hot transaction/cursor path should be auditable by reading the data members,
not by trusting opaque abstractions. Avoid types that can allocate, retain, or
dispatch invisibly inside core transaction state.

Forbidden in `tree_state`, `tree_transaction`, `commit_target`, current-state
cursor handles, and MDBX cursor pools:

- `std::function`, virtual callback boxes, or type-erased commit callbacks.
- `std::shared_ptr` / `std::weak_ptr` for mutable transaction lifetime. A handle
  either borrows the owning scope or moves ownership exactly once.
- `std::string`, `std::vector`, `std::map`, `std::unordered_map`, or any other
  standard container whose allocation behavior is not obvious from the field
  declaration.
- Hidden heap ownership in abstract wrapper objects. If a member can allocate, its
  owner, capacity policy, and counter belong in the struct comment or type name.

Allowed allocation sites:

- SAL node/value allocation caused by real tree mutation.
- `write_buffer` storage growth, owned by the tree transaction and counted as
  buffer allocation. Prefer transaction-owned arena/slab storage over ad hoc
  per-operation allocation.
- Cursor-pool slab growth during warmup or explicit reserve. Repeated
  acquire/release after warmup must not allocate.
- Fixed-capacity inline key copying for subtree commit targets. If a key is too
  large for the inline buffer, the API should fail or route through an explicit,
  named slow path; it should not silently allocate a `std::string`.

Every allowed allocation needs a named owner and a testable counter. The core
rule is: if a reviewer cannot find the owner and maximum/warmup behavior from
the struct definition, the design is too opaque.

## Transaction-Owned Tree State

Use one fixed-layout internal object for every mutable tree scope. The shape is
deliberately plain so allocation behavior is visible:

```cpp
struct tree_transaction
{
   tree_state state;
   commit_target target;
   cursor_pool cursors;

   tree_transaction* parent = nullptr; // non-owning; scoped lifetime
   uint32_t mutation_generation = 0;
   uint32_t active_state_cursors = 0;
};
```

The parent does not store a `vector` of children. A child transaction is a
stack/RAII object that borrows its parent and commits its final state back to
that parent. The important part is that mutable state is owned by the active
transaction scope, not by shared pointers, vectors, or erased callbacks that
outlive it.

The exact field names can vary, but these ownership rules should hold:

- A committed root slot owns committed state. Once loaded from a root slot, that
  state is shared with readers until the transaction materializes a private
  working state.
- A transaction owns its working state. Child transactions may borrow parent
  context, but they do not extend the parent's lifetime through shared
  ownership.
- Committing a child moves or applies the child's final state into the parent
  target. Aborting a child destroys the child's state.
- Transaction-scoped handles are not owners. Current-state cursor handles,
  subtree transaction handles, and nested-transaction guards should be
  non-owning views or move-only guards whose lifetime is bounded by the owning
  transaction object.
- Snapshot cursors and public copyable `tree` handles are deliberate owners:
  they retain a `tree_id` because their purpose is to survive later mutation or
  be moved into another root/subtree.

This preserves the core performance story: a transaction starts from committed,
shared state, then gradually converts the parts it touches into transaction
owned state. Long-running transactions become cheaper as more of their hot
paths have already crossed the COW boundary.

### `tree_state`

`tree_state` is the mutable state for exactly one logical tree inside a
transaction. It is not a public snapshot and it is not shared ownership. It is
the transaction-owned answer to "what does this tree look like right now?"

Responsibilities:

- Holds the working root, when there is one. The root is a full `tree_id`
  (`root + ver`). The current implementation may continue to use
  `sal::smart_ptr`, but this should be treated as an audited root handle:
  store one handle in the state, pass it by reference or move, and avoid
  incidental copies that retain/release behind the reader's back.
- Knows whether the working root's version has been materialized for this
  transaction.
- Holds the `expect_failure` write buffer in place. The wrapper for the buffer
  must not allocate; any internal buffer growth must be owned and counted by
  the tree transaction or its arena.
- Provides point reads and cursor/range reads over the current write state:
  buffer first, persistent working root second.
- Owns or borrows any per-tree cursor pool needed for current-state cursors.
- Is moved/applied into a commit target on commit, returned as a public `tree`
  by `get_tree()` for targetless write transactions, or destroyed on abort.

Concrete shape:

```cpp
struct tree_state
{
   sal::smart_ptr<sal::alloc_header> root; // current full tree_id handle

   detail::write_buffer buffer; // constructed in place; no optional wrapper
   bool buffer_enabled = false;
   bool buffer_dirty = false;

   tx_mode mode = tx_mode::expect_success;
   bool has_materialized_version = false;
};
```

`std::optional<detail::write_buffer>` is intentionally avoided here. Optional
does not allocate by itself, but it hides construction/destruction state from an
allocation audit. A plain buffer plus explicit flags makes it clear that the
member exists in the transaction object and that only the buffer's own storage
policy can allocate.

Important operations:

- `materialize_working_root()`:
  - No-op if this state already owns a transaction version.
  - For `expect_success`, normally called when the state is opened for writing.
  - For `expect_failure`, called immediately before a buffered flush or any
    direct persistent tree mutation.
  - Produces the same kind of versioned working root that
    `make_unique_root()` produces today.
- `merge_buffer_to_root()`:
  - Calls `materialize_working_root()` first.
  - Replays buffered changes into the persistent working root.
  - Clears or soft-clears the buffer according to nested transaction ownership.
- `final_tree_id()` / `get_tree()`:
  - Flushes if needed.
  - For targeted transactions, returns/transfers the final `(root, ver)` pair
    to the commit target.
  - For `start_write_transaction(tree)`, returns a public copyable `tree`
    handle because there is no commit target.

For freshly-created subtrees, `root` may start null and become a new versioned
tree only when first written or flushed. For pre-existing subtrees, `root` is
loaded from the stored subtree `tree_id`; materialization creates a
transaction-owned working state without assuming old subtree nodes are safe to
mutate in place.

### `commit_target`

`commit_target` is where a targeted `tree_transaction` sends its final tree
state when it commits. Root transactions, nested transactions, and subtree
transactions have commit targets. A write transaction opened directly from a
`tree` does not; it exposes its final state through `get_tree()`.

Use a fixed-layout tagged target, not `std::variant`, `std::function`, or a
heap-owned callback:

```cpp
static constexpr size_t max_inline_commit_key = 1024;

struct inline_commit_key
{
   uint16_t size = 0;
   std::array<std::byte, max_inline_commit_key> bytes = {};
};

enum class commit_target_kind : uint8_t
{
   root_slot,
   parent_state,
   parent_subtree,
};

struct commit_target
{
   commit_target_kind kind = commit_target_kind::root_slot;

   uint32_t root_index = 0;           // used by root_slot
   tree_transaction* parent = nullptr; // used by parent_state/parent_subtree
   inline_commit_key key;             // used by parent_subtree
};
```

This intentionally spends fixed inline space to make subtree commits
allocation-free and easy to audit. If a future API needs larger keys, that path
should be a separate named type with an explicit owner and allocation counter,
not a silent `std::string` inside the common commit target.

Semantics:

- `root_slot`: publish the final `tree_id` to the top-level root slot.
  The root slot now owns the committed state and readers can observe it.
- `parent_state`: replace or merge the parent transaction's current
  `tree_state` with the child transaction's final state. This is the normal
  nested transaction case.
- `parent_subtree`: upsert the child final `tree_id` into the parent at
  `key`. This is the subtree transaction case. The parent value slot becomes
  the owner of the child `tree_id` reference.

Rules:

- A target is non-owning with respect to its parent; parent lifetime is enforced
  structurally by the scoped transaction API.
- A child transaction cannot outlive its parent transaction.
- Commit transfers ownership exactly once. Abort transfers nothing.
- Top-root targets are coordinated by the outer multi-root transaction so locks
  and publish order are deterministic.

## Multi-Top-Root Transactions

Top-level roots are a special case. They are the primary place where independent
updates live because root slots are not themselves stored inside a COW parent
tree. A multi-root transaction should therefore not be modeled as one root tree
containing child roots. It should be modeled as a transaction coordinator that
owns multiple independent top-root tree transactions.

Shape:

- The outer `transaction` owns fixed-index top-root storage keyed by root index.
  With a 512-root limit, a bitmap plus array is more auditable than a dynamic
  map/vector:

```cpp
static constexpr uint32_t max_top_roots = 512;

struct top_root_entry
{
   bool active = false;
   tree_transaction tree;
};

struct multi_root_transaction
{
   std::bitset<max_top_roots> active_roots;
   std::array<top_root_entry, max_top_roots> roots;
};
```

If embedding every `tree_transaction` is too large, use the same indexed array
with entries pointing into an explicit transaction-owned slab. That slab must
have a reserve/warmup API and an allocation counter. Do not replace this with a
general `std::vector` or `std::map` in the hot path.

- Each top-root tree transaction has a root-slot commit target.
- Roots are locked in deterministic index order.
- Each root materializes independently:
  - `expect_success`: versioned working root at open/start.
  - `expect_failure`: buffered until that root's first persistent tree touch.
- Reads and writes name the root they target, or use an explicit primary root
  convenience handle.
- Commit publishes every participating top-root final state; abort drops all
  participating working states.

This keeps top-root independence intact while still letting one API coordinate
multiple root locks/lifetimes. Subtree and nested transactions are scoped below
one of those top-root tree transactions and commit back into their immediate
parent, not into the coordinator directly.

## API Direction

Introduce distinct read lifetimes. The public names are defined by
`docs/getting-started/transaction-contract.md`:

1. `cursor()` on a write transaction/root/subtree
   - Owner-bound view from a `transaction`, detached `tree`, or `write_cursor`.
   - Does not retain/increment the root.
   - Reads/iterates the current write state, including read-own-writes.
   - Valid only until the owner mutates, commits, aborts, or is destroyed. Any
     write may invalidate it.
   - This is the default for read-own-writes APIs.

2. `snapshot_cursor()`
   - Owns a retained `smart_ptr` root.
   - Snapshot isolated and safe across later writer mutation.
   - May force COW by keeping a root shared.
   - Only created by APIs with "snapshot" in the name.

3. Copyable `tree`
   - Owns a retained tree identity that can be stored with `upsert_subtree()`
     or `set_root()`.
   - `tree` comes from `create_temporary_tree()`, `get_root()`, or
     `get_subtree()` and is a copyable retained smart pointer.
   - A detached tree from `create_temporary_tree()` has no top-level visibility
     until it is stored.
   - `start_write_transaction(tree)` creates the mutable editing session for a
     tree and has no commit target.
   - `get_tree()` returns the edited tree from a write transaction.
   - May force COW by keeping the retained tree shared.
   - Can create storage leaks if the caller stores it cyclically.

This is green code with no compatibility debt to preserve. Avoid ambiguous
interfaces:

- Writer-owned APIs use `cursor()` for current write-state iteration.
- Read-only sessions and explicit writer snapshots use `snapshot_cursor()`.
- Snapshot APIs should include "snapshot" in the name.
- Do not keep or add deprecated aliases.
- Public docs must say that snapshot cursors are isolation handles, not the
  ordinary way to read inside a write transaction.

## Subtree Edits as Nested Tree Transactions

Model every edited tree, including subtrees, as the same kind of
`tree_transaction`. Some tree transactions have a commit target; a write
transaction opened directly from a `tree` does not.

- A top-level root commits by publishing to a root slot.
- A write transaction opened from a `tree` has no commit target. It exposes its
  final state with `get_tree()`.
- A subtree commits by updating the subtree value in its parent transaction.
- A nested transaction commits by saving its final tree state back to
  its parent transaction.

This gives one state machine for primary roots and subtrees:

- `expect_success`: materialize a versioned working root before the first
  persistent mutation. For the primary root this already happens at
  `start_transaction()` via `make_unique_root()`. For a subtree this should
  happen when the subtree transaction is opened for writing, or at the first
  subtree write if we want to avoid work for read-only opens.
- `expect_failure`: keep writes in that tree transaction's buffer. Materialize
  the subtree's working root only when that specific transaction must flush
  into the persistent tree, exactly like the primary root.
- Commit sends the child's final tree state to its parent target. For subtrees,
  the child produces a `tree_id`; the parent receives that `tree_id` through
  its normal write path. If the parent is still buffered, this is just another
  buffered upsert. If the parent is persistent, it triggers the parent's normal
  materialization/COW path.
- Abort drops the child transaction. The parent transaction remains unchanged.

This suggests the transaction structure should be recursive instead of a flat
`_change_sets` vector with parent indices and a bottom-up scan:

- `transaction` owns one root `tree_transaction`.
- `sub_transaction()` creates a child `tree_transaction` whose commit target is
  its parent transaction's current tree state.
- `start_write_transaction(tree, mode)` creates a detached `tree_transaction`
  with no commit target. `get_tree()` returns the resulting tree. Storing that
  tree with `upsert_subtree()` or `set_root()` supplies the later publication
  target. Moving the `tree` handle is recommended for one-shot stores to avoid
  extra reference-count traffic.
- `subtree_transaction(key)` creates a child `tree_transaction` whose commit
  target is `(parent_tree_transaction, key)`.
- Committing any child materializes/merges its own change set as needed,
  produces its final tree state, and moves/applies that state to its parent
  target.
- For a subtree child, that send is `parent.upsert(key, child_tree_id)`.
- For a nested child, that send replaces the parent's current working
  tree state with the child's final working tree state.
- A nested subtree uses the same rule recursively; no special global subtree
  commit order is needed because each child commits into its immediate parent.
- The outermost root transaction is just the same object with a root-slot commit
  target instead of a parent target.

In other words, published and nested tree transactions have commit targets,
while a direct write transaction over a `tree` has no target:

- root commit target: publish `tree_id` to root slot.
- direct tree write transaction: no target; `get_tree()` returns the final
  `tree`.
- subtree commit target: upsert `tree_id` into parent at key.
- nested transaction commit target: save final state back to parent transaction.

The API can keep ergonomic handles (`tx.primary().open_subtree(key)`), but the
implementation should not need a global parent-link registry, a bottom-up scan,
or a separate nested-scope stack if nested scopes and subtree edits are represented
as scoped child tree transactions that commit back to their parent transaction.

Safety rule:

- A pre-existing subtree is reachable through older parent snapshots until the
  parent value is replaced and those snapshots drain. Materializing a subtree
  working root must therefore not mean "mutate the old subtree nodes in place
  because they look locally unique." It means "attach this transaction's version
  to a working tree root, then rely on normal SAL refcounts/COW to protect old
  readers." Freshly-created subtrees may truly be unique.

Current code gaps this model should clean up:

- The lazy version materialization helper is currently transaction-primary
  oriented. It should become change-set oriented, for example
  `materialize_working_root(change_set&)`.
- Subtree storage/readback must preserve the full `tree_id` (`root + ver`).
  `value_type` and `value_node` can represent that today; inline leaf subtree
  storage needs an audit so subtree cursors do not accidentally lose the
  snapshot version and read "latest."

## Subtree Value Ownership Contract

A subtree value is a stored `tree_id`, not just a root address. The stored value
owns one implicit reference to every non-null address in that `tree_id`:

- `tree_id.root` keeps the subtree's root node alive.
- `tree_id.ver` keeps the subtree's version control block alive.

That reference is owned by the parent value slot for as long as the slot stores
that subtree. Therefore:

- Inserting or cloning a subtree value must retain both `root` and `ver` when
  the new parent node/value will also own the subtree.
- Replacing or removing a subtree value must release both `root` and `ver`.
- Moving a subtree from a smart pointer into a parent value should transfer
  ownership of the caller's reference exactly once. The caller should be left
  empty, and the parent slot becomes the owner.
- Copying a parent leaf/value node for COW creates another owner and must retain
  the full `tree_id`.
- Snapshot cursors opening a subtree must construct their subtree root from the
  stored `tree_id`, so the cursor inherits the correct snapshot version.

This mirrors the existing `smart_ptr` invariant: `(root, ver)` is the identity
of a versioned tree root. Subtree values need to preserve the same pair all the
way through leaf storage, value-node storage, COW cloning, cursor readback, and
release.

Subtree ownership must be acyclic. The public API may hand users explicit
copyable tree objects from committed roots, read sessions, snapshots, and
existing subtrees so they can manage snapshots themselves. That means cycles are
a documented caller footgun, not something the type system can fully prevent.
`upsert_subtree()` and `set_root()` copy or move `tree` smart pointers into the
destination slot. Moving is recommended for one-shot stores because it avoids
extra reference-count traffic; copying is legal and retains the same tree identity.
Code must warn clearly that storing a tree inside itself, inside a descendant,
or in an indirect cycle can keep reference counts alive forever. Low-level
unchecked internals that accept raw `tree_id` values must document acyclic
ownership as a caller precondition.

## Cursor Recycling

Add per-session cursor pools with intrusive slots. The pool is allowed to grow
by allocating explicit slabs, but acquire/release from the free list must not
allocate:

```cpp
struct pooled_cursor_slot
{
   pooled_cursor_slot* next_free = nullptr;
   cursor impl;
};

struct cursor_slab
{
   cursor_slab* next = nullptr;
   uint32_t capacity = 0;
   pooled_cursor_slot slots[/* explicit slab capacity */];
};

struct cursor_pool
{
   pooled_cursor_slot* free_list = nullptr;
   cursor_slab* slabs = nullptr;
   uint32_t live_count = 0;
   uint32_t slab_alloc_count = 0;
};

struct cursor_handle
{
   cursor_pool* pool = nullptr;
   pooled_cursor_slot* slot = nullptr;
   cursor_lifetime_kind lifetime = cursor_lifetime_kind::current_state;
};
```

The actual code can split `cursor` and `write_cursor` pools if their storage is
different. What matters is that the free-list pointer is intrusive, slab growth
is named and counted, and the RAII handle is move-only with two raw pointers.
There is no heap allocation inside the handle.

- Put an intrusive `next_free` pointer on recyclable cursor storage.
- Keep free lists on the allocator/read/write session that owns the cursor's
  thread-affine allocator session.
- Return cursors through move-only RAII handles whose destructors reset cursor
  state and return storage to the free list.
- Add `reset(...)` methods to reuse existing storage rather than reconstructing
  inline buffers and path stacks.
- Keep owning snapshot root handles and current-state owner pointers as separate
  fields/types so pooled cursors cannot accidentally retain roots.

Current-state cursor storage:

- Stores a raw pointer/reference to the owning `tree_transaction` or
  `tree_state`.
- Stores the owner's `mutation_generation` observed at acquire time.
- Does not store `sal::smart_ptr` and does not increment the root refcount.

Snapshot cursor storage:

- Stores the retained root handle explicitly.
- Releases that handle before returning the slot to the pool.
- Is created only by APIs whose name says `snapshot`.

RAII rule:

- Destroying a current-state cursor only returns storage.
- Destroying a snapshot cursor releases the retained root and then returns
  storage.
- Moving a cursor handle transfers the return-to-pool responsibility exactly
  once.

## Current-State Cursor Correctness

Current-state cursors must not outlive or race owner mutation. Their purpose is
to read or iterate the current write state; they are not stable iterators across
mutation.

Recommended guard:

- `transaction` / `write_cursor` tracks active current-state cursors in debug builds
  or all builds.
- Mutation APIs assert or return an error if a current-state cursor is live.
- Commit/abort invalidate current-state cursors.
- Snapshot cursors are exempt because they own a retained root.

For `expect_failure`:

- Point reads already consult the buffer before persistent state.
- Add a buffered overlay cursor for iteration/range reads:
  - persistent current-state cursor for the base tree
  - `write_buffer` iterator for buffered inserts/updates/tombstones
  - merge logic that makes buffer entries shadow persistent keys
- This should mirror the DWAL merge-cursor mental model, but stay local to the
  COW transaction/write-buffer layer.

## MDBX Shim Direction

- Add cursor free lists to `MDBX_txn`.
- `mdbx_cursor_open()` should acquire cursor storage from the transaction pool.
- `mdbx_cursor_close()` should reset and return the cursor to the transaction
  pool, not always delete it.
- RW MDBX cursors should be created from `txn->write_tx` / transaction-local
  merge view when available, so reads see uncommitted writes.
- RO MDBX cursors may use snapshot cursors, because MDBX read transactions are
  explicit snapshot lifetimes.

## Migration Phases

### Phase 0 - Document and test current invariants

- Add tests proving `expect_success` performs one `make_unique_root()`-style
  version transition at transaction start.
- Add tests proving `expect_failure` does not allocate a version until
  `merge_buffer_to_persistent()` or another forced persistent tree touch
  materializes the transaction version.
- Add subtree-specific versions of both tests: existing subtree edit,
  freshly-created subtree edit, and nested subtree edit.
- Add a test proving a snapshot of the parent still sees the old subtree after a
  transaction edits and commits that subtree through the parent.
- Add a test proving a subtree cursor opened from a snapshot uses the subtree's
  stored version, not "latest."
- Add ownership tests proving subtree insert/clone/replace/remove retains and
  releases both `tree_id.root` and `tree_id.ver` exactly once.
- Add a regression test showing current `read_cursor()` retains a root and can
  force COW, so the migration has a measurable target.

### Phase 1 - End-user transaction docs

- Write and review `docs/getting-started/transaction-contract.md` before
  changing public APIs.
- Treat the documented developer experience as the source of truth for naming,
  lifetimes, cursor behavior, subtree behavior, multi-root behavior, and MDBX
  shim behavior.
- Convert each "Testable Promises" item in the document into an implementation
  test or MDBX compatibility test.
- Do not accept an internal design that requires changing the docs to explain
  accidental complexity, accidental snapshot pinning, or hidden allocation.

### Phase 2 - Transaction-owned recursive tree state

- Introduce the internal `tree_transaction` / `tree_state` shape so each
  mutable tree scope owns its root, in-place buffer, cursor pool, and cursor
  invalidation counters.
- Replace optional/type-erased transaction state with explicit fields and flags.
- Introduce an outer coordinator for multi-top-root transactions. It owns
  independent top-root tree transactions keyed by root index using a fixed-index
  table or an explicit transaction-owned slab, rather than modeling roots as
  children of another COW tree.
- Move lazy/eager version materialization from a primary-root helper to a
  tree-transaction helper.
- Represent commit targets explicitly: root slot, parent transaction state, or
  parent subtree value with an inline key buffer. Direct write transactions
  opened from a `tree` remain targetless; `get_tree()` returns their final tree,
  and a later `upsert_subtree()` or `set_root()` publishes that tree.
- Add explicit public APIs that expose copyable `tree` objects from committed
  roots, snapshots, and existing subtrees. These APIs must be named and
  documented so users understand they are taking ownership of a retained tree
  identity, not doing a deep copy.
- Replace the flat subtree parent-link registry with recursive tree
  transactions where child commit moves/applies final state into its immediate
  parent target.
- Remove shared ownership from mutable transaction state. Keep handles
  non-owning or move-only so the top-level transaction controls lifetime.
- Audit inline leaf subtree storage so full `tree_id` values survive
  store/read/clone/update paths.
- Audit leaf/value-node subtree refcounting so the parent value owns exactly one
  reference to both addresses in the stored `tree_id`.

### Phase 3 - Add resettable cursor storage

- Add `cursor::reset(...)` and `write_cursor::reset(...)` without changing
  public API behavior.
- Add intrusive `next_free` hooks to cursor slots, not to a separate heap-owned
  wrapper object.
- Measure cursor open/close loops before enabling public pooling.

### Phase 4 - Add session cursor pools and RAII handles

- Implement pooled allocation/return on read/write sessions with explicit slab
  allocation counters and reserve/warmup hooks.
- Replace value-returning cursor construction with explicit current-state or
  snapshot cursor handles; no compatibility wrappers are required.
- Add leak/double-return tests for move-only RAII wrappers.

### Phase 5 - Separate current-state and snapshot APIs

- Add current-state cursor APIs for transaction and write-cursor reads.
- Add explicit `snapshot_cursor()` APIs where retained root semantics are wanted.
- Remove ambiguous cursor names rather than preserving aliases.
- Update internal transaction reads to use current-state cursors where possible.

### Phase 6 - Buffered overlay cursor for `expect_failure`

- Implement a transaction-local merge cursor over `write_buffer` plus
  persistent current-state cursor.
- Route `lower_bound`, `upper_bound`, range iteration, and count-like reads
  through the overlay when a buffer exists.
- Add read-own-writes tests for inserted, updated, tombstoned, and range-removed
  keys.

### Phase 7 - MDBX shim migration

- Pool `MDBX_cursor` and `cursor_state` on `MDBX_txn`.
- Route RW cursors through the write transaction's read-own-writes cursor view.
- Preserve RO transaction snapshot semantics.
- Add allocation-count or construction-count tests for repeated cursor sessions.

### Phase 8 - Keep docs and examples honest

- Remove ambiguous APIs rather than deprecating them.
- Make the API reference, quickstart, and MDBX migration docs link to the
  transaction contract.
- Keep docs and examples using current-state transaction reads for
  read-own-writes and explicit snapshot APIs for snapshot isolation.

## Verification Gates

- Every testable promise in `docs/getting-started/transaction-contract.md` has
  a corresponding PsiTri or MDBX-shim test.
- Write-session creation is treated as allocator-context creation: examples and
  tests do not create write sessions repeatedly for small operations.
- Tests or diagnostics should make the 32 MB per-write-session buffer cost
  visible enough that accidental session churn is caught early.
- Tight loop creating/destroying cursors allocates only during warmup.
- Current-state transaction reads do not increase root refcount and do not force COW.
- `get(key, lambda)` reads through the current write state without copying and
  clearly bounds the borrowed value view to the callback lifetime.
- Core hot-path structs do not contain `std::function`, `std::string`,
  `std::vector`, `std::map`, `std::shared_ptr`, or other hidden-allocation
  wrappers except the explicitly audited root handle.
- API tests prove `tree` handles from `create_temporary_tree()`, `get_root()`,
  and `get_subtree()` can be copied or moved into `upsert_subtree()` and
  `set_root()`.
- Documentation and diagnostics clearly warn that tree storage APIs can create
  ownership cycles and leak storage if callers store a tree inside itself, a
  descendant, or an indirect cycle.
- Explicit snapshot cursors still preserve snapshot isolation.
- `expect_success` hot-key update tests keep the existing low-allocation result.
- `expect_failure` cursor/range reads see buffered writes before commit.
- MDBX RW cursors see their own transaction writes.
- Existing `~[!benchmark]` psitri and psitrimdbx tests pass.

## Non-Goals

- Do not change the existing eager `make_unique_root()` / lazy transaction
  version materialization protocol.
- Do not weaken explicit read-session snapshot isolation.
- Do not make current-state cursors thread-safe; they inherit the owning session's
  thread affinity.
