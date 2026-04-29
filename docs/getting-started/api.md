# API Reference

## Core Types

### `database`

The top-level handle to a PsiTri database. Thread-safe. Typically held as a `std::shared_ptr<database>`.

```cpp
namespace psitri {
enum class open_mode {
    create_or_open,  // open existing or create new
    create_only,     // create new; fail if it already exists
    open_existing,   // open existing; fail if missing
    read_only,       // reserved for future use
};

class database : public std::enable_shared_from_this<database> {
public:
    // Construction
    static std::shared_ptr<database> open(
        std::filesystem::path dir,
        open_mode mode = open_mode::create_or_open,
        const runtime_config& cfg = {},
        recovery_mode recovery = recovery_mode::none);

    static std::shared_ptr<database> create(std::filesystem::path dir,
                                            const runtime_config& = {});

    // Sessions
    std::shared_ptr<write_session> start_write_session();
    std::shared_ptr<read_session>  start_read_session();

    // Durability
    void sync();

    // Statistics
    database_stats get_stats() const;

    // Low-Level Diagnostics
    sal::seg_alloc_dump dump() const;
    uint64_t reachable_size();

    // Recovery
    bool ref_counts_stale() const;
    void reclaim_leaked_memory();
    void recover();

    // Maintenance
    void compact_and_truncate();
    bool wait_for_compactor(std::chrono::milliseconds timeout = 10000ms);
};
}
```

| Method | Description |
|--------|-------------|
| `open(dir, mode, cfg, recovery)` | Open a database using an explicit `open_mode` |
| `create(dir)` | Convenience helper for `open(dir, open_mode::create_only)`. Fails if the database already exists |
| `start_write_session()` | Create a long-lived per-thread allocator context with a 32 MB write buffer. **Thread-affine**: call from the thread that will use it |
| `start_read_session()` | Create a lightweight read-only session. **Thread-affine**: call from the thread that will use it |
| `sync()` | Flush all data to disk |
| `reachable_size()` | Total bytes of live, reachable objects |
| `compact_and_truncate()` | Wait for compaction, then shrink the data file |
| `recover()` | Full recovery: rebuild control blocks from segments |

---

### `open_mode`

Use `database::open()` for normal application code. Choose the mode that
matches the expected filesystem state.

| Mode | Behavior |
|------|----------|
| `open_mode::create_or_open` | Open an existing database, or create it if missing |
| `open_mode::create_only` | Create a new database. Fails if it already exists |
| `open_mode::open_existing` | Open an existing database. Fails if it does not exist |
| `open_mode::read_only` | Reserved for future read-only database opens |

`database::create(dir)` is only a helper for the create-only case. It is not
the general factory shown in examples.

!!! warning "Sessions are thread-affine and long-lived"
    Sessions are backed by `thread_local` storage. Always create them on the thread that will use them. Do not create a session on one thread and pass it to another -- this causes cross-thread aliasing on the underlying allocator session, which is not thread-safe.

    For multi-threaded code, pass the `database` (or a `shared_ptr` to it) to each worker thread and let each thread call `start_write_session()` or `start_read_session()` itself.

    A `write_session` is an allocator context, not a small request object. Each write session owns a 32 MB write buffer that is flushed and recycled when the session closes. Create one write session per writer thread and reuse it for many transactions. Do not create and destroy write sessions in a tight loop for small operations. Application code may have at most 50 active write sessions; the allocator has 64 session slots total, with 14 reserved for backend and compaction work.

### `write_session`

A long-lived session for reading and writing. One per logical writer thread.
Represents an independent allocator context, owns a 32 MB write buffer, and
extends `read_session`.

```cpp
class write_session : public read_session {
public:
    tree create_temporary_tree();
    write_transaction start_write_transaction(
        tree base,
        tx_mode mode = tx_mode::expect_success);
    transaction start_transaction(uint32_t root_index,
                                  tx_mode mode = tx_mode::expect_success);

    void set_root(uint32_t root_index, tree value,
                  sync_type sync = sync_type::none);

    void set_sync(sync_type sync);
    sync_type get_sync() const;

    uint64_t get_total_allocated_objects() const;
    uint64_t get_pending_release_count() const;
};
```

Calling `start_write_transaction(tree, mode)` opens an editing session over a
tree without giving it a root slot or parent key to publish into. Call
`get_tree()` on that write transaction to get the edited tree, then pass that
tree into `upsert_subtree()` or `set_root()` to publish it.

Calling `start_transaction(root_index, mode)` is the top-root convenience API:
it opens a transaction attached to a top-level root, and `commit()` publishes
back to that root.

`set_root()` accepts a tree value. That value may be a freshly-created detached
tree or a retained root/subtree snapshot.
Storing an existing snapshot does not deep-copy every key/value pair; it stores
the tree identity. Pass by copy when you want to keep using the handle; pass
with `std::move()` when this is a one-shot store and you want to avoid extra
reference-count traffic.

---

### `read_session`

A lightweight read-only session. Supports snapshot reads and cursor creation.

```cpp
class read_session : public std::enable_shared_from_this<read_session> {
public:
    tree get_root(uint32_t root_index);
    cursor snapshot_cursor(uint32_t root_index);
};
```

`get_root()` returns a copyable retained tree object for applications that want
to manage their own snapshots or copy a tree from place to place.
`snapshot_cursor()`
returns a stable read-only cursor for iteration without creating a retained tree
handle.

---

### `transaction`

An ACID transaction on a top-level root, a nested scope, or a subtree.
Auto-aborts on destruction if not committed.

```cpp
class transaction {
public:
    // Mutations
    bool insert(key_view key, value_view value);
    bool update(key_view key, value_view value);
    void upsert(key_view key, value_view value);
    void upsert_subtree(key_view key, tree subtree);
    int  remove(key_view key);
    bool remove_range_any(key_view lower, key_view upper);
    uint64_t remove_range_counted(key_view lower, key_view upper);

    // Reads
    bool get(key_view key, std::invocable<value_view> auto&& lambda) const;
    template<ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;
    int32_t get(key_view key, Buffer auto* buffer) const;

    // Subtrees
    bool is_subtree(key_view key) const;
    tree get_subtree(key_view key) const;
    transaction subtree_transaction(key_view key, subtree_open mode);

    // Control
    void commit() noexcept;
    void abort() noexcept;
    transaction sub_transaction() noexcept;

    // Read cursor from current transaction state
    cursor read_cursor() const;
};
```

| Method | Returns | Description |
|--------|---------|-------------|
| `insert(key, value)` | `bool` | Insert only if key doesn't exist |
| `update(key, value)` | `bool` | Fast path when the key is expected to exist; returns `false` if missing |
| `upsert(key, value)` | `void` | General insert-or-update path; uses ordered search semantics |
| `remove(key)` | `int` | Remove key, returns bytes freed or -1 |
| `remove_range_any(lower, upper)` | `bool` | Remove keys in [lower, upper), returns whether anything was removed |
| `remove_range_counted(lower, upper)` | `uint64_t` | Remove keys in [lower, upper), returns the exact count |
| `get(key, lambda)` | `bool` | Zero-copy read. Calls the lambda for normal values; the `value_view` is valid only for that call |
| `get<T>(key)` | `optional<T>` | Copy value into an owned object such as `std::string` |
| `get(key, buffer*)` | `int32_t` | Copy value into a caller-owned reusable buffer |
| `commit()` | `void` | Publish to this transaction's root slot or parent target |
| `abort()` | `void` | Discard all changes |
| `sub_transaction()` | `transaction` | Nested transaction that commits back to parent |

---

### `tree`

A copyable retained tree object. It may be a detached tree returned by
`write_session::create_temporary_tree()`, a top-root snapshot returned by
`read_session::get_root()`, or a subtree returned by `get_subtree()`.

A `tree` is the thing you can store. To modify it, call
`start_write_transaction(tree, mode)`, make changes in the write transaction,
then ask the transaction for the updated tree.

Pass a `tree` into `upsert_subtree()` or `set_root()` to put the same tree
identity somewhere. Pass with `std::move()` when the caller no longer needs its
handle and wants to avoid extra reference-count traffic.

```cpp
class tree {
public:
    bool get(key_view key, std::invocable<value_view> auto&& lambda) const;
    template<ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;
    cursor cursor() const;
    tree get_subtree(key_view key) const;
};
```

A `tree` handle is a copyable smart pointer to a tree identity. Copying a
`tree` retains the same tree identity; it is not a deep copy.

### `write_transaction`

A short-lived editing session over a `tree`. It has no commit target. It does
not publish to a root slot or parent key by itself; call `get_tree()` to obtain
the edited tree and store that tree explicitly.

```cpp
class write_transaction {
public:
    bool insert(key_view key, value_view value);
    bool update(key_view key, value_view value);
    void upsert(key_view key, value_view value);
    void upsert_subtree(key_view key, tree subtree);
    int  remove(key_view key);
    bool remove_range_any(key_view lower, key_view upper);
    uint64_t remove_range_counted(key_view lower, key_view upper);

    bool get(key_view key, std::invocable<value_view> auto&& lambda) const;
    template<ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;
    int32_t get(key_view key, Buffer auto* buffer) const;
    cursor cursor() const;

    write_transaction sub_transaction() noexcept;
    tree get_tree() const;
    void abort() noexcept;
};
```

`get_tree()` returns a copyable `tree` smart pointer for the current write
state. Passing that tree to `upsert_subtree()` or `set_root()` is what publishes
the edited tree somewhere.

Use `remove_range_any()` when the caller only needs success vs not-found. Use
`remove_range_counted()` only when the exact number of removed keys is part of
the application contract; counted removal may traverse fully-covered child
subtrees to compute that count.

!!! warning "Tree handles can create cycles"
    Do not store a tree inside itself, inside one of its descendants, or in any
    ownership pattern that creates an indirect cycle. PsiTri stores subtrees by
    reference-counted tree identity; cycles may keep storage alive forever.

---

### Read Forms

Prefer `get(key, lambda)` on hot paths:

```cpp
bool found = tx.get("user:alice", [](psitri::value_view value) {
    parse_user(value);
});
```

This is the zero-copy form. The view is valid only during the callback, and the
callback should finish promptly because it may delay compaction while the view
is protected.

The method returns `true` when the key contains a normal value and the callback
was called. It returns `false` for a missing key; use subtree APIs for subtree
values.

Use copying forms only when the caller needs to keep the value:

```cpp
auto owned = tx.get<std::string>("user:alice"); // copies

std::string scratch;
tx.get("user:alice", &scratch);                 // copies into reusable buffer
```

Cursor values follow the same rule: `cursor::get_value(lambda)` gives a
zero-copy positioned value view, while `cursor::value<std::string>()` copies.

When the application knows the exact key, prefer a point read:
`get(key, lambda)` for zero-copy, `get<T>(key)` for an owned copy, or
`get(key, buffer*)` for a reusable copy buffer. These APIs can use PsiTri's
hash lookup fast path. `lower_bound()` is for ordered positioning and range
iteration; it must perform ordered search even if the boundary is an exact key.
For exact cursor positioning, use `cursor::find(key)`.

Likewise, prefer `update(key, value)` when the key should already exist.
`upsert(key, value)` is the right API when the key may be missing, but it pays
for the general insert-or-update path.

---

### `cursor`

Read-only ordered iteration over a tree snapshot.

```cpp
class cursor {
public:
    static constexpr int32_t value_not_found = -1;
    static constexpr int32_t value_subtree   = -2;

    // Positioning
    bool seek_begin() noexcept;      // first key
    bool seek_last() noexcept;       // last key
    bool seek_end() noexcept;        // past last key
    bool seek_rend() noexcept;       // before first key
    bool lower_bound(key_view key) noexcept;
    bool upper_bound(key_view key) noexcept;
    bool find(key_view key) noexcept;
    bool seek(key_view key) noexcept;
    bool first(key_view prefix = {}) noexcept;
    bool last(key_view prefix = {}) noexcept;

    // Traversal
    bool next() noexcept;
    bool prev() noexcept;

    // State
    bool is_end() const noexcept;
    bool is_rend() const noexcept;
    key_view key() const noexcept;

    // Value access
    void get_value(std::invocable<value_view> auto&& lambda) const;
    int32_t get(key_view key, Buffer auto* buffer) const;
    template<ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;

    // Snapshot subtree access
    bool is_subtree(key_view key) const;
    cursor subtree_cursor(key_view key) const;
};
```

`cursor::subtree_cursor(key)` returns another snapshot cursor. It preserves the
subtree version visible in the parent snapshot and is read-only; it is not a
movable subtree value.

---

## Type Aliases

| Type | Definition |
|------|-----------|
| `key_view` | `std::string_view` |
| `value_view` | `std::string_view` |
| `tree` | Copyable retained tree object returned by `create_temporary_tree()`, `get_root()`, or `get_subtree()` |
| `database_ptr` | `std::shared_ptr<database>` |
| `write_session_ptr` | `std::shared_ptr<write_session>` |

## Recovery Modes

| Mode | When to use | What it does |
|------|-------------|-------------|
| `none` | Clean shutdown detected | No recovery needed |
| `deferred_cleanup` | App crash, fast restart | Mark ref counts stale, defer leak reclamation |
| `app_crash` | App crash, full cleanup | Reset ref counts, reclaim leaked memory |
| `power_loss` | OS or hardware crash | Validate segments, rebuild control blocks and roots |
| `full_verify` | Suspected corruption | Deep checksum verification of all objects |

## Sync Types

| Mode | Durability | Performance |
|------|-----------|-------------|
| `none` | No durability (OS flushes when convenient) | Fastest |
| `mprotect` | + Write protection on committed data | Fast |
| `msync_async` | + OS flush initiated | Moderate |
| `msync_sync` | + Block until OS writes | Slower |
| `fsync` | + Block until drive acknowledges | Slow |
| `full` | + F_FULLFSYNC (flush drive cache) | Slowest, safest |

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `num_top_roots` | 512 | Independent top-level tree roots |
| Max segments | ~1 million | 32 TB max database |
| Max application write sessions | 50 | Long-lived writer-thread allocator contexts |
| Allocator session slots | 64 | 50 application write sessions plus 14 reserved backend slots |
| Write-session buffer | 32 MB | Owned per active write session; flushed/recycled on session close |
| Max object size | 16 MB | Half a segment |
| Cacheline | 64 bytes | Allocation alignment |
| Max leaf size | 2,048 bytes | Per leaf node |
| Max branches per leaf | Limited by 2,048 byte leaf size | Keys per leaf (typically ~58) |
| Max inner branches | 256 | 16 cachelines x 16 slots |
