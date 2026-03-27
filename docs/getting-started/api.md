# API Reference

## Core Types

### `database`

The top-level handle to a PsiTri database. Thread-safe. Typically held as a `std::shared_ptr<database>`.

```cpp
namespace psitri {
class database : public std::enable_shared_from_this<database> {
public:
    // Construction
    database(const std::filesystem::path& dir,
             const runtime_config& cfg,
             recovery_mode mode = recovery_mode::none);

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
| `create(dir)` | Create or open a database with default config |
| `start_write_session()` | Create a session for reading and writing. **Thread-affine**: call from the thread that will use it |
| `start_read_session()` | Create a lightweight read-only session. **Thread-affine**: call from the thread that will use it |
| `sync()` | Flush all data to disk |
| `reachable_size()` | Total bytes of live, reachable objects |
| `compact_and_truncate()` | Wait for compaction, then shrink the data file |
| `recover()` | Full recovery: rebuild control blocks from segments |

---

!!! warning "Sessions are thread-affine"
    Sessions are backed by `thread_local` storage. Always create them on the thread that will use them. Do not create a session on one thread and pass it to another -- this causes cross-thread aliasing on the underlying allocator session, which is not thread-safe.

    For multi-threaded code, pass the `database` (or a `shared_ptr` to it) to each worker thread and let each thread call `start_write_session()` or `start_read_session()` itself.

### `write_session`

A session for reading and writing. One per logical writer thread. Extends `read_session`.

```cpp
class write_session : public read_session {
public:
    transaction start_transaction(uint32_t root_index);

    write_cursor_ptr create_write_cursor();
    write_cursor_ptr create_write_cursor(smart_ptr<alloc_header> root);

    smart_ptr<alloc_header> get_root(uint32_t root_index);
    void set_root(uint32_t root_index, smart_ptr<alloc_header> root,
                  sync_type sync = sync_type::none);

    void set_sync(sync_type sync);
    sync_type get_sync() const;

    uint64_t get_total_allocated_objects() const;
    uint64_t get_pending_release_count() const;
};
```

---

### `read_session`

A lightweight read-only session. Supports snapshot reads and cursor creation.

```cpp
class read_session : public std::enable_shared_from_this<read_session> {
public:
    smart_ptr<alloc_header> get_root(uint32_t root_index);
    cursor create_cursor(uint32_t root_index);
};
```

---

### `transaction`

An ACID transaction on a single root index. Auto-aborts on destruction if not committed.

```cpp
class transaction {
public:
    // Mutations
    bool insert(key_view key, value_view value);
    bool update(key_view key, value_view value);
    void upsert(key_view key, value_view value);
    void upsert(key_view key, smart_ptr<alloc_header> subtree_root);
    int  remove(key_view key);
    uint64_t remove_range(key_view lower, key_view upper);

    // Reads
    template<ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;
    int32_t get(key_view key, Buffer auto* buffer) const;

    // Subtrees
    bool is_subtree(key_view key) const;
    smart_ptr<alloc_header> get_subtree(key_view key) const;
    write_cursor get_subtree_cursor(key_view key) const;

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
| `update(key, value)` | `bool` | Update only if key exists |
| `upsert(key, value)` | `void` | Insert or update |
| `remove(key)` | `int` | Remove key, returns bytes freed or -1 |
| `remove_range(lower, upper)` | `uint64_t` | Remove keys in [lower, upper), returns count |
| `get<T>(key)` | `optional<T>` | Read value as type T (e.g., `std::string`) |
| `commit()` | `void` | Atomically make all changes visible |
| `abort()` | `void` | Discard all changes |
| `sub_transaction()` | `transaction` | Nested transaction that commits back to parent |

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
    int32_t get(key_view key, Buffer auto* buffer) const;
    template<ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;
};
```

---

## Type Aliases

| Type | Definition |
|------|-----------|
| `key_view` | `std::string_view` |
| `value_view` | `std::string_view` |
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
| Max threads | 64 | Concurrent sessions |
| Max object size | 16 MB | Half a segment |
| Cacheline | 64 bytes | Allocation alignment |
| Max leaf size | 2,048 bytes | Per leaf node |
| Max branches per leaf | Limited by 2,048 byte leaf size | Keys per leaf (typically ~58) |
| Max inner branches | 256 | 16 cachelines x 16 slots |
