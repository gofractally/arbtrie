# Transaction and Cursor Contract

This page defines the public transaction contract for PsiTri. The implementation
must satisfy this developer experience. Internal data structures, cursor pools,
and MDBX compatibility code should be shaped by this contract, not the other way
around.

## Mental Model

PsiTri has up to 512 top-level roots. Each root is an independent ordered key
space. A committed top-level root is visible to readers and is treated as shared
state.

A write transaction creates current write state for the root or tree it opens.
Reads inside that transaction read the current write state, including writes
that have not been turned back into a `tree` yet. This is the default and cheap
path.

A tree does not have to start from a top-level root. Use
`create_temporary_tree()` to build a detached tree, then put it somewhere by
upserting it as a subtree value or publishing it to a top-level root with
`set_root()`.

PsiTri can hand application code an explicit retained `tree` object. A `tree`
may come from `create_temporary_tree()`, a top-level root snapshot, or an
existing subtree. Passing a `tree` to `upsert_subtree()` or `set_root()` stores
that tree identity in a new place. This is powerful: applications can manage
their own snapshots and copy them between roots and subtrees without
deep-copying all keys and values.

To modify a `tree`, start a write transaction from it:

```cpp
auto edit = ws->start_write_transaction(tree_handle,
                                        psitri::tx_mode::expect_success);
edit.upsert("field", "value");
auto updated_tree = edit.get_tree();
```

`start_write_transaction(tree)` does not give the transaction a root slot or
parent key to publish into. It only edits that tree and lets the caller ask for
the resulting `tree`. Publishing is a separate step: store the returned tree
with `upsert_subtree()` or `set_root()`.

That power has a real footgun. PsiTri stores subtrees by reference-counted tree
identity. If application code stores a tree inside itself, inside one of its
descendants, or otherwise creates a cycle, the cycle may keep storage alive
forever. The public API makes tree movement explicit, but it does not try to
prove the user's ownership graph is acyclic.

Snapshot ownership is different from current-state reads. A snapshot cursor
pins a stable view so it can survive later mutation. A copyable `tree` obtained
from `get_root()` or `get_subtree()` can also retain a stable tree identity.
That is useful and correct when the application needs isolation or wants to
archive a tree, but it can make later writes copy more data.

## Write Sessions

A write session is a thread-owned allocator context. It owns a 32 MB write
buffer that is flushed and recycled when the session closes. Create the session
on the thread that will use it, keep it for the life of that worker thread, and
create many transactions from it.

```cpp
void writer_thread(std::shared_ptr<psitri::database> db)
{
   auto ws = db->start_write_session();

   while (auto job = next_job())
   {
      auto tx = ws->start_transaction(job.root, psitri::tx_mode::expect_success);
      apply_job(tx, *job);
      tx.commit();
   }
}
```

Do not create a write session for each small operation:

```cpp
for (auto& job : jobs)
{
   auto ws = db->start_write_session(); // wrong: creates allocator contexts repeatedly
   auto tx = ws->start_transaction(job.root);
   apply_job(tx, job);
   tx.commit();
}
```

Contract:

- A write session must be created by the thread that uses it.
- A write session must not be passed to another thread.
- A write session should be long-lived, normally one per worker thread.
- Each write session owns a 32 MB write buffer. Applications should budget for
  that per active write session.
- Closing a write session flushes/recycles its write buffer.
- Transactions, cursors, and subtree scopes are the short-lived objects.
- An application may have at most 50 active write sessions. The allocator has
  64 session slots total; 14 are reserved for background work such as current
  and future compaction services.
- Creating write sessions repeatedly for small operations is an API misuse and
  should show up clearly in examples, docs, and diagnostics.

## Basic Write Transaction

Use `expect_success` when the transaction is likely to commit and may perform
many writes to the same root.

```cpp
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>

auto db = psitri::database::open(
    "app.db",
    psitri::open_mode::create_or_open);
auto ws = db->start_write_session();

auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

tx.upsert("account:alice", "100");
tx.upsert("account:bob", "50");

bool found = tx.get("account:alice", [](psitri::value_view value) {
   assert(value == "100");
});
assert(found);

tx.commit();
```

Contract:

- `get()` inside a write transaction reads the transaction's current write
  state.
- `get(key, lambda)` is the zero-copy read path. The `value_view` passed to the
  lambda is valid only for the life of that lambda call.
- `get<std::string>(key)` and `get(key, buffer)` are copying convenience APIs.
  Use them when the value must outlive the read callback.
- `commit()` publishes the final state atomically.
- Destroying a live transaction aborts it.
- `expect_success` may prepare a private writable version at transaction start.
- A transaction does not hold a compactor/recycling read lock for its whole
  lifetime. PsiTri takes short internal read locks around operations that need
  them. Longer zero-copy value lifetimes must be explicit at the call site.

## Zero-Copy Reads

Use the lambda form for hot reads:

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

bool found = tx.get("user:alice", [](psitri::value_view value) {
   parse_user(value);
});
```

Contract:

- The `value_view` is borrowed directly from the tree; PsiTri does not copy the
  value into a temporary buffer.
- `get(key, lambda)` returns `true` when the key contains a normal value and
  the lambda was called. It returns `false` for a missing key; subtree values
  are accessed through subtree APIs.
- The view is valid only during the lambda call. Do not store it, return it, or
  pass it to asynchronous work.
- The lambda should be short-running. While the view is live, PsiTri may hold a
  read lock or equivalent protection that delays compaction/recycling.
- Do CPU-light parsing, comparison, or immediate serialization inside the
  lambda. Copy the value out first if later work may block, allocate heavily,
  call user plugins, wait on I/O, or take locks.

If a caller intentionally needs a borrowed value view to live beyond one
callback, make that cost visible with an explicit RAII pin:

```cpp
auto pin = tx.pin_values();
auto cur = tx.cursor();

if (cur.lower_bound("user:alice") && cur.key() == "user:alice")
{
   psitri::value_view value = cur.value(pin);
   parse_and_compare(value);
}
```

Contract:

- A value pin protects the memory backing borrowed `value_view` objects from
  compactor/recycling movement until the pin is destroyed.
- A value pin is not a snapshot. It does not make a current-state cursor stable
  across writes, commits, aborts, or cursor invalidation.
- Keep value pins tightly scoped. Holding a pin longer than necessary can delay
  compaction and segment reuse.
- Prefer `get(key, lambda)` for normal hot reads and copying reads for values
  that must survive slow work.

Copying reads are still useful when ownership matters:

```cpp
auto owned = tx.get<std::string>("user:alice"); // copies

std::string reused;
tx.get("user:alice", &reused);                  // copies into caller buffer
```

## Known-Key Fast Paths

PsiTri has separate paths for exact-key operations and ordered operations. Use
the exact-key APIs when the application already knows the key.

```cpp
// Exact-key point read. Fast lookup path; lambda form is also zero-copy.
tx.get("user:alice", [](psitri::value_view value) {
   use_user(value);
});

// Exact-key owned read. Fast lookup path, then copies into std::string.
auto owned = tx.get<std::string>("user:alice");

// Ordered search. Use this when you need the first key at or after a boundary.
auto cur = tx.cursor();
cur.lower_bound("user:alice");
```

The same distinction applies to writes:

```cpp
// Fast path when the key is expected to exist.
if (!tx.update("user:alice", new_value))
   handle_missing_user();

// General path when the key may or may not exist.
tx.upsert("user:alice", new_value);
```

Contract:

- `get(key, lambda)`, `get<T>(key)`, and `get(key, buffer*)` are point reads.
  When the exact key is known, they can use the hash lookup fast path.
- `lower_bound()` is an ordered-positioning operation. It must find the first
  key at or after the search key, so it uses ordered search semantics even when
  the caller passes an exact key.
- `update(key, value)` is the preferred write when the caller knows the key
  should already exist. It can use the existing-key fast path and reports
  missing keys with `false`.
- `upsert(key, value)` is the general insert-or-update API. Because it must
  handle both existing and missing keys, it uses ordered search semantics.

## Tree Objects

Use `create_temporary_tree()` when you want an empty tree that is not yet
attached to any top-level root. It returns a `tree`. Edit that tree by starting
a write transaction from it.

```cpp
auto profile = ws->create_temporary_tree();

auto build_profile = ws->start_write_transaction(
    std::move(profile),
    psitri::tx_mode::expect_success);

build_profile.upsert("name", "Alice");
build_profile.upsert("timezone", "America/Chicago");

profile = build_profile.get_tree();
```

The detached tree can be stored under a key:

```cpp
auto users = ws->start_transaction(0, psitri::tx_mode::expect_success);

users.upsert_subtree("user:alice:profile", std::move(profile));

users.commit();
```

Use `std::move()` when you do not need the handle afterward. Moving avoids the
extra reference-count retain/release traffic that a copy would perform.

Or it can become a top-level root:

```cpp
auto index = ws->create_temporary_tree();

auto index_edit = ws->start_write_transaction(std::move(index),
                                              psitri::tx_mode::expect_success);

index_edit.upsert("email:alice@example.com", "user:alice");
index_edit.upsert("email:bob@example.com", "user:bob");

index = index_edit.get_tree();

ws->set_root(7, std::move(index));
```

Tree handles can also come from existing roots or subtrees. This stores the
same tree identity somewhere else; it is not a deep copy of every key/value
pair.

```cpp
auto rs = db->start_read_session();

// Retains the root snapshot visible to this read session.
psitri::tree root_snapshot = rs->get_root(2);

auto archive = ws->start_transaction(7, psitri::tx_mode::expect_success);
archive.upsert_subtree("snapshots/root-2", root_snapshot);
archive.commit();
```

If the same snapshot should be installed in more than one place, copy the
`tree` handle or pass the same handle again. Copying a `tree` copies the
underlying smart pointer. It points at the same tree identity; it is not a deep
copy.

```cpp
auto root_snapshot = rs->get_root(2);
auto another_handle = root_snapshot; // copies the tree smart pointer

auto tx = ws->start_transaction(7, psitri::tx_mode::expect_success);
tx.upsert_subtree("copy-a", root_snapshot);
tx.upsert_subtree("copy-b", another_handle);
tx.commit();
```

Contract:

- A `tree` object is a copyable retained smart pointer to a tree identity.
- Copying a `tree` retains the same tree identity. It is a shallow smart-pointer
  copy, not a deep data copy.
- A tree created by `create_temporary_tree()` is detached. It is not attached
  to any top-level root slot until it is stored.
- A detached tree is edited through `start_write_transaction(tree, mode)`.
- `start_write_transaction(tree)` has no commit target. It never publishes to
  a root slot or parent key by itself.
- `get_tree()` on a write transaction returns the transaction's current tree.
  That returned `tree` can then be stored with `upsert_subtree()` or
  `set_root()`.
- A detached tree is not visible to readers until it is stored with
  `upsert_subtree()` on a transaction that eventually commits to a top-level
  root, or stored directly with `set_root()`.
- Passing a `tree` to `upsert_subtree()` or `set_root()` stores a retained copy
  of that tree identity.
- Moving a `tree` into `upsert_subtree()` or `set_root()` stores the same tree
  identity while avoiding extra reference-count traffic on the caller's handle.
- `commit()` is not meaningful for a write transaction that was opened from a
  `tree`, because there is no root slot or parent key to publish to.
- Abort or destruction releases an unstored detached tree and discards an
  unfinished write transaction.

Cycle footgun:

- A stored subtree is a reference to a tree identity, not an owned deep copy.
- Do not store a tree inside itself.
- Do not store a tree inside one of its descendants.
- Do not build an indirect cycle such as `A` contains `B`, `B` contains `C`,
  and `C` contains `A`.
- PsiTri may not be able to detect all cycles cheaply. Code that creates cycles
  can leak storage because reference counts never drain.

These shapes are legal only when the application knows they do not create a
cycle:

```cpp
auto tree = tx.get_subtree("key");
tx.upsert_subtree("key2", tree);

auto root = rs->get_root(2);
tx.upsert_subtree("data", root);
```

To make an independent deep copy, iterate the source tree into a new detached
tree, then store that tree in its destination.

## Speculative Transaction

Use `expect_failure` when abort is common or the transaction may do only a few
writes. The API contract is the same: reads still see writes made by the
transaction.

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_failure);

tx.upsert("session:42", "pending");

bool found = tx.get("session:42", [](psitri::value_view value) {
   assert(value == "pending");
});
assert(found);

if (request_is_valid())
   tx.commit();
else
   tx.abort();
```

Contract:

- `expect_failure` may buffer writes before touching the persistent tree.
- Point reads and range cursors must still read buffered writes.
- Commit produces the same visible result as `expect_success`.

## Current-State Cursors

Use `cursor()` on a write transaction when you want to read or iterate the
transaction's current write state.

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

tx.upsert("user:001", "Alice");
tx.upsert("user:002", "Bob");

{
   auto cur = tx.cursor();

   for (cur.lower_bound("user:"); !cur.is_end(); cur.next())
   {
      if (!cur.key().starts_with("user:"))
         break;

      cur.get_value([&](psitri::value_view value) {
         process_user(cur.key(), value);
      });
   }
}

tx.upsert("user:003", "Carol");
tx.commit();
```

Contract:

- `tx.cursor()` does not create a snapshot and does not pin the root.
- The cursor sees committed data plus uncommitted writes in the transaction.
- Creating and destroying current-state cursors is cheap after warmup.
- A current-state cursor is valid only until the transaction mutates, commits,
  aborts, or is destroyed.

Do not mutate while a current-state cursor is live:

```cpp
auto cur = tx.cursor();
cur.lower_bound("user:");

tx.upsert("user:004", "Dana"); // invalid: cur is still live
```

The implementation must catch this in debug builds, and may return an error in
checked release APIs. End the cursor scope before mutating:

```cpp
{
   auto cur = tx.cursor();
   cur.lower_bound("user:");
   read_some_keys(cur);
}

tx.upsert("user:004", "Dana"); // ok
```

## Snapshot Cursors

Use `snapshot_cursor()` only when you need a stable view that survives later
mutation.

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

tx.upsert("k1", "before");

auto snapshot = tx.snapshot_cursor();

tx.upsert("k1", "after");

assert(snapshot.get<std::string>("k1") == std::optional<std::string>{"before"});
assert(tx.get<std::string>("k1") == std::optional<std::string>{"after"}); // copies
```

Read-only sessions also use explicit snapshot naming:

```cpp
auto rs = db->start_read_session();
auto cur = rs->snapshot_cursor(0);
```

Contract:

- `snapshot_cursor()` pins a retained tree view.
- A snapshot cursor remains stable across later writes.
- A snapshot cursor may force later writes to copy shared nodes.
- Snapshot cursor creation is explicit in the API name.
- Retained tree snapshot ownership is explicit in the return type:
  `get_root()` and `get_subtree()` return `tree`, not `cursor`.
- A snapshot cursor is a read-only isolation handle. Use `get_root()` or
  `get_subtree()` when the application needs a retained tree handle.
- A subtree opened from a snapshot uses the subtree version stored in that
  snapshot.

Snapshot implications:

- A snapshot keeps the objects it can see alive until the snapshot is released.
- Writes after the snapshot may need to COW nodes that the snapshot still sees.
- Long-lived snapshots can delay reclamation and compaction.
- Use snapshots for stable reads, but release them promptly on write-heavy
  workloads.

There are two different ways to copy from a snapshot.

To preserve the exact tree snapshot without deep-copying every key/value pair,
move a `tree` handle:

```cpp
auto rs = db->start_read_session();
auto old_root = rs->get_root(0);

auto tx = ws->start_transaction(7, psitri::tx_mode::expect_success);
tx.upsert_subtree("archived-root-0", old_root);
tx.commit();
```

To create an independent deep copy, iterate the snapshot into a new detached
tree:

```cpp
auto rs = db->start_read_session();
auto old_profile = rs->get_root(0).get_subtree("user:alice:profile");
auto old_profile_cur = old_profile.cursor();

auto copy = ws->create_temporary_tree();
auto copy_edit = ws->start_write_transaction(std::move(copy),
                                             psitri::tx_mode::expect_success);

for (old_profile_cur.seek_begin(); !old_profile_cur.is_end(); old_profile_cur.next())
{
   old_profile_cur.get_value([&](psitri::value_view value) {
      copy_edit.upsert(old_profile_cur.key(), value);
   });
}

copy = copy_edit.get_tree();

auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);
tx.upsert_subtree("user:bob:profile", std::move(copy));
tx.commit();
```

The first form stores another reference to the same snapshot tree. The second
form creates a new tree with independent contents.

## Nested Transactions

Use `sub_transaction()` when part of a larger transaction should be committed or
aborted independently before the parent commits.

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

tx.upsert("order:100", "open");

{
   auto save = tx.sub_transaction();
   save.upsert("order:100:item:1", "book");
   save.upsert("order:100:item:2", "pen");
   save.commit();
}

assert(tx.get<std::string>("order:100:item:1") == std::optional<std::string>{"book"});

tx.commit();
```

Abort drops only the child work:

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

tx.upsert("cart:1", "open");

{
   auto save = tx.sub_transaction();
   save.upsert("cart:1:coupon", "INVALID");
   save.abort();
}

assert(!tx.get<std::string>("cart:1:coupon"));

tx.commit();
```

Contract:

- A subtransaction is move-only and cannot outlive its parent.
- Committing a subtransaction saves its final state into the parent
  transaction.
- Aborting a subtransaction leaves the parent unchanged.
- Parent commit is still required to publish the final result.

## Subtree Transactions

A subtree transaction edits a tree stored as a value under a parent key. On
commit, the subtree transaction writes its final tree identity back to that key
in the parent transaction.

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

{
   auto profile = tx.subtree_transaction(
       "user:alice:profile",
       psitri::subtree_open::create_if_missing);

   profile.upsert("name", "Alice");
   profile.upsert("timezone", "America/Chicago");
   profile.commit();
}

assert(tx.is_subtree("user:alice:profile"));

tx.commit();
```

Open an existing subtree the same way:

```cpp
auto tx = ws->start_transaction(0, psitri::tx_mode::expect_success);

{
   auto profile = tx.subtree_transaction(
       "user:alice:profile",
       psitri::subtree_open::must_exist);

   profile.upsert("timezone", "UTC");
   profile.commit();
}

tx.commit();
```

Contract:

- A subtree transaction has the same read, cursor, subtransaction, and
  `expect_success` / `expect_failure` behavior as a top-root transaction.
- A subtree commit updates the parent transaction at exactly one key.
- A subtree abort leaves the parent key unchanged.
- A stored subtree is a full `tree_id`, not just a root address.
- A freshly-created subtree can be built as a detached tree and
  then stored with `upsert_subtree()`.
- Snapshot cursors that open a subtree get a read-only snapshot view using the
  subtree version stored in their snapshot.

## Multi-Root Transactions

Use a multi-root transaction when one logical operation must update independent
top-level roots atomically.

```cpp
std::array roots = {
   psitri::root_access{0, psitri::root_mode::write}, // users
   psitri::root_access{1, psitri::root_mode::write}, // email index
   psitri::root_access{2, psitri::root_mode::read},  // config
};

auto tx = ws->start_transaction(std::span{roots},
                                psitri::tx_mode::expect_success);

auto users = tx.root(0);
auto email = tx.root(1);
auto cfg   = tx.root(2);

users.upsert("user:42", user_record);
email.upsert("alice@example.com", "user:42");

cfg.get("signup_policy", [&](psitri::value_view policy) {
   apply_policy(policy);
});

tx.commit();
```

Contract:

- Each top-level root has independent current write state.
- Write roots are published atomically by the transaction commit.
- Read roots are readable but not writable.
- Roots are locked in deterministic root-index order.
- A current-state cursor from one root is invalidated by writes to that same
  root. It is not invalidated by writes to independent roots.

## Choosing Cursor APIs

| Need | API | Copies value? | Pins root? | Sees uncommitted writes? |
|------|-----|---------------|------------|--------------------------|
| Hot point read inside writer | `tx.get(key, lambda)` | No | No | Yes |
| Owned point read | `tx.get<std::string>(key)` | Yes | No | Yes |
| Reused caller buffer | `tx.get(key, &buffer)` | Yes | No | Yes |
| Borrow view across a small local scope | `auto pin = tx.pin_values()` + `cur.value(pin)` | No | No | Yes |
| Iterate current writer state | `tx.cursor()` | No by default | No | Yes |
| Stable writer snapshot | `tx.snapshot_cursor()` | Optional per read | Yes | State at creation |
| Read-only snapshot | `read_session::snapshot_cursor(root)` | Optional per read | Yes | Committed state only |
| Movable root/subtree snapshot | `get_root()` / `get_subtree()` | No by default | Yes | State at handle creation |
| Build detached tree | `write_session::create_temporary_tree()` | No | No | Yes |
| MDBX read-write cursor | transaction current-state cursor | No by default | No | Yes |
| MDBX read-only cursor | snapshot cursor | No by default | Yes | Committed state only |

## MDBX Shim Reference

The MDBX shim uses the same contract:

| MDBX operation | PsiTri behavior |
|----------------|-----------------|
| `mdbx_get()` in a read-write transaction | Reads the current write state, including prior `mdbx_put()` and `mdbx_del()` calls in that transaction. |
| `mdbx_cursor_open()` in a read-write transaction | Acquires a pooled current-state cursor from the `MDBX_txn`. It does not open a PsiTri snapshot. |
| `mdbx_cursor_get()` in a read-write transaction | Iterates committed data plus uncommitted writes from the same transaction. |
| `mdbx_put()` / `mdbx_del()` | Invalidates current-state cursors opened earlier on the same DBI/root unless the write is performed through that cursor API and the shim explicitly repositions it. |
| `mdbx_txn_commit()` / `mdbx_txn_abort()` | Invalidates all cursors owned by that transaction and returns their storage to the transaction cursor pool. |
| `mdbx_txn_begin(... MDBX_TXN_RDONLY ...)` | Opens an explicit snapshot lifetime. Read-only cursors are snapshot cursors. |
| `mdbx_txn_reset()` / `mdbx_txn_renew()` | Releases snapshot ownership and cursor state, then reuses the transaction/session storage where possible. |

The shim must not accidentally convert read-write cursor opens into snapshot
opens. Native MDBX users expect read-write transactions to read their own
writes; PsiTri must preserve that behavior without pinning the current root on
every short cursor session.

## Testable Promises

The implementation should have tests for each promise below:

- A write transaction `get(key, lambda)` sees its own insert, update, remove,
  and range remove before commit without copying the value.
- The zero-copy value view passed to `get(key, lambda)` is invalid after the
  lambda returns.
- Long-running zero-copy callbacks are documented as delaying compaction or
  recycling while the protected view is live.
- `get<std::string>(key)` copies into an owned temporary.
- Exact-key `get()` is the documented fast path when the caller knows the key;
  `lower_bound()` is for ordered positioning and range scans.
- `update()` is the documented fast path when the caller knows the key should
  exist; `upsert()` is the general insert-or-update path.
- A write transaction `cursor()` sees its own inserts and hides its own
  tombstones before commit.
- Repeated current-state cursor acquire/release allocates only during warmup.
- Creating a current-state cursor does not retain the root and does not force
  COW.
- Mutating a tree while one of its current-state cursors is live is caught.
- A snapshot cursor remains stable after later writes.
- A snapshot cursor can open a subtree and sees the subtree version stored in
  that snapshot.
- A subtree transaction commit upserts exactly one subtree `tree_id` into its
  parent key.
- A subtree transaction abort leaves the parent key unchanged.
- `create_temporary_tree()` can build a detached `tree`; copying it retains the
  same tree identity, while moving it into `upsert_subtree()` or `set_root()`
  avoids extra reference-count traffic.
- `start_write_transaction(tree)` can modify a tree without publishing it to a
  root slot or parent key.
- `get_tree()` on a write transaction returns the edited tree, and storing that
  returned tree is the explicit publish step.
- A detached tree is not visible to readers until it is stored in a transaction
  that commits to a top-level root or is installed with `set_root()`.
- `get_root()` and `get_subtree()` return copyable retained tree objects that
  can be stored elsewhere.
- Copying or moving a snapshot tree into another root/subtree preserves the
  snapshot's tree identity; it does not deep-copy the contents.
- The docs and diagnostics clearly warn that storing trees cyclically can leak
  storage.
- A nested transaction commit saves into its parent; nested abort leaves the
  parent unchanged.
- A multi-root transaction publishes all write roots atomically.
- MDBX read-write `mdbx_get()` and `mdbx_cursor_get()` read their own writes.
- MDBX read-only transactions retain snapshot isolation.
- Creating more than 50 active application write sessions fails clearly or is
  rejected by configuration.
- Examples and benchmarks create write sessions once per worker thread, not once
  per operation.
