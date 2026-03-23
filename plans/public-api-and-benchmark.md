# Plan: Complete psitri Public API + Benchmark Tool

> This plan will also be saved to `plans/public-api-and-benchmark.md` in the repo.

## Context

The psitri library has a working internal engine (`tree_context`) that handles all trie operations (insert, upsert, update, remove, iterate), but the intended public API (`write_cursor`, `read_cursor`, `transaction`) is incomplete. The architecture doc (`docs/architecture.md`) and aspirational test (`tests/database_test.cpp`) define the target API shape. Meanwhile, the old arbtrie benchmark tool (`programs/bench.cpp`) needs to be ported to psitri, and it should be written against the proper public API rather than internals.

This plan builds the public API bottom-up, then creates the benchmark as the first real consumer.

---

## Phase 1: write_cursor — Mutation API wrapping tree_context

**Goal**: Create a `write_cursor` class that wraps `tree_context` + `cursor` to provide the mutation API.

### Design

`write_cursor` owns a `tree_context` (for mutations) and can produce `cursor` instances (for reads). It does NOT inherit from `cursor` — the read cursor is constructed on demand from the current root, since mutations invalidate cursor state.

```cpp
// include/psitri/write_cursor.hpp
class write_cursor {
public:
    write_cursor(sal::allocator_session_ptr session);
    write_cursor(sal::allocator_session_ptr session, sal::smart_ptr<> root);

    // Mutations — delegate to tree_context
    bool insert(key_view key, value_view value);    // returns false if key exists
    bool update(key_view key, value_view value);    // returns false if key not found
    void upsert(key_view key, value_view value);    // always succeeds
    int  remove(key_view key);                       // returns old value size, -1 if not found

    // Read access — constructs cursor from current root
    cursor read_cursor() const;

    // Direct get without full cursor
    template <ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;
    int32_t get(key_view key, Buffer auto* buffer) const;

    // Root access
    sal::smart_ptr<> root() const;

    // Diagnostics
    stats get_stats() const;
    void  print() const;
    void  validate() const;

private:
    sal::allocator_session_ptr _session;
    tree_context               _ctx;
};
```

**Key decisions**:
- `write_cursor` does NOT inherit from `cursor`. Mutations change the root, invalidating any cursor state. The user calls `read_cursor()` to get a snapshot cursor when needed.
- `get()` is provided directly (creates a temporary cursor internally) for the common point-lookup case.
- Constructor takes `allocator_session_ptr` — the session is needed to construct `tree_context` and cursors.
- No commit/sync on write_cursor itself — that's the transaction's job. write_cursor operates on a detached tree root.

### Files to create/modify

| File | Action |
|---|---|
| `include/psitri/write_cursor.hpp` | **Create** — class definition |
| `include/psitri/write_cursor_impl.hpp` | **Create** — inline implementation |

### Implementation notes

- `insert()` calls `_ctx.upsert<upsert_mode::unique_insert>(key, value_type(value))`, returns `_ctx` result == -1 (inserted)
- `update()` calls `_ctx.upsert<upsert_mode::unique_update>(key, value_type(value))`, returns result != -1
- `upsert()` calls `_ctx.upsert<upsert_mode::unique_upsert>(key, value_type(value))`
- `remove()` calls `_ctx.remove(key)`
- `read_cursor()` returns `cursor(_ctx.get_root())`
- `get()` creates temporary `cursor(_ctx.get_root())` and calls `cursor::get()`
- `root()` returns `_ctx.get_root()`
- `get_stats()` calls `_ctx.get_stats()`

---

## Phase 2: Wire write_session to produce write_cursors

**Goal**: Connect `write_session` to create `write_cursor` instances, and add root access to sessions.

### Design

```cpp
// write_session.hpp additions
class write_session : public read_session {
public:
    // Create cursor on a transient (empty) tree
    write_cursor_ptr create_write_cursor();

    // Create cursor on an existing root
    write_cursor_ptr create_write_cursor(sal::smart_ptr<> root);

    // Root access (loads from database top_root[index])
    sal::smart_ptr<> get_root(uint32_t root_index);

    // Atomic root save (for simple non-transactional commit)
    void set_root(uint32_t root_index, sal::smart_ptr<> root,
                  sal::sync_type sync = sal::sync_type::none);
};
```

Also add to `read_session`:
```cpp
class read_session {
public:
    sal::smart_ptr<> get_root(uint32_t root_index);
    cursor create_cursor(uint32_t root_index);  // convenience
};
```

### Files to modify

| File | Action |
|---|---|
| `include/psitri/write_session.hpp` | **Modify** — add method declarations, uncomment write_cursor include |
| `include/psitri/write_session_impl.hpp` | **Modify** — update implementations |
| `include/psitri/read_session.hpp` | **Modify** — add get_root, convenience cursor factory |
| `include/psitri/read_session_impl.hpp` | **Modify** — add implementations |

### Implementation notes

- `get_root(index)` delegates to `_allocator_session->get_root<>(sal::root_object_number(index))`
- `set_root(index, root, sync)` delegates to `_allocator_session->set_root(sal::root_object_number(index), root, sync)`
- `create_write_cursor()` creates cursor with null root (empty tree): `make_shared<write_cursor>(_allocator_session)`
- `create_write_cursor(root)` creates cursor on existing root: `make_shared<write_cursor>(_allocator_session, root)`
- The session's `_allocator_session` is the bridge — write_cursor needs it for tree_context construction

### Type registration

Currently tests manually call `sal::register_type_vtable<leaf_node>()` etc. for each node type. This should be encapsulated:

```cpp
// Add to database.hpp or a new psitri/init.hpp
namespace psitri {
    void register_node_types();  // calls register_type_vtable for all 4 node types
}
```

Called once by `database::database()` constructor (idempotent).

---

## Phase 3: Transaction with full mutation API

**Goal**: Flesh out the `transaction` class to support upsert/update/remove and connect it to write_session.

### Design

```cpp
// transaction.hpp — enhanced
class transaction {
public:
    transaction(write_session& session, uint32_t root_index);
    ~transaction();  // auto-abort

    bool insert(key_view key, value_view value);
    bool update(key_view key, value_view value);
    void upsert(key_view key, value_view value);
    int  remove(key_view key);

    // Read the current state
    cursor read_cursor() const;
    template <ConstructibleBuffer T>
    std::optional<T> get(key_view key) const;

    void commit(sal::sync_type sync = sal::sync_type::none);
    void abort() noexcept;

    [[nodiscard]] transaction sub_transaction() noexcept;

private:
    write_session*               _session;
    uint32_t                     _root_index;
    std::unique_ptr<write_cursor> _cursor;  // owns the tree mutations
    bool                         _committed = false;
};
```

Add to `write_session`:
```cpp
transaction start_transaction(uint32_t root_index);
```

### Implementation notes

- Constructor: grabs `_db->modify_lock(root_index)`, loads root via `get_root(root_index)`, creates internal `write_cursor` on that root
- `insert/update/upsert/remove`: delegate to `_cursor`
- `commit(sync)`: calls `set_root(root_index, _cursor->root(), sync)`, releases lock
- `abort()`: releases lock, discards cursor (tree_context root refcount drops, COW copies are cleaned up)
- Destructor: calls `abort()` if not committed
- Lock is held for the duration of the transaction (pessimistic, consistent with SAL's model)
- `sub_transaction()`: creates a new transaction that commits back to this transaction's cursor root instead of the database

### Locking consideration

The database already has `_modify_lock[num_top_roots]` mutexes. The transaction should lock `_db->modify_lock(root_index)` on construction and unlock on commit/abort. This is exposed via a `database` friend relationship (already declared).

### Files to modify

| File | Action |
|---|---|
| `include/psitri/transaction.hpp` | **Rewrite** — replace current stub |
| `include/psitri/transaction_impl.hpp` | **Create** — implementation |
| `include/psitri/write_session.hpp` | **Modify** — add start_transaction |
| `include/psitri/write_session_impl.hpp` | **Modify** — add start_transaction impl |

---

## Phase 4: Benchmark tool

**Goal**: Port `programs/bench.cpp` to use the psitri public API, add iterate and remove benchmarks.

### Design

New file: `programs/psitri_bench.cpp` (keep old `bench.cpp` for reference)

```
psitri-benchmark [options]
  --round,-r N      Number of rounds (default 3)
  --batch,-b N      Batch size (default 512)
  --items,-i N      Number of items per round (default 1000000)
  --value-size,-s N Value size in bytes (default 8)
  --db-dir,-d PATH  Database directory (default ./psitridb)
  --bench NAME      Which benchmark: all, insert, upsert, update, get, iterate, remove
  --reset           Remove database before running
  --stat            Print stats and exit
```

### Benchmarks

1. **Insert** — big-endian seq, string number rand, dense random, little-endian seq
2. **Upsert** — same key patterns (re-inserts over existing data)
3. **Update** — same key patterns (updates existing values)
4. **Get** — sequential and random lookups via `cursor.get()`
5. **Iterate** — `cursor.seek_begin()` + `next()` full traversal, measure keys/sec
6. **Remove** — remove all keys inserted, measure removals/sec
7. **Stats** — `write_cursor.get_stats()` between operations

### Usage pattern in benchmark

```cpp
auto db  = database::create(db_dir);
auto ses = db->start_write_session();

// For each benchmark:
auto tx = ses->start_transaction(0);
for (rounds) {
    for (batches) {
        tx.upsert(key, value);  // or insert/update/remove
    }
    // no mid-round commit needed — tree_context handles everything
}
tx.commit(sal::sync_type::none);

// For get benchmark:
auto root = ses->get_root(0);
cursor cur(root);  // or ses->create_cursor(0)
for (items) {
    cur.get(key, &buffer);
}

// For iterate:
cursor cur(ses->get_root(0));
cur.seek_begin();
while (!cur.is_end()) { count++; cur.next(); }
```

### Build integration

Add to `programs/CMakeLists.txt`:
```cmake
add_executable(psitri-benchmark psitri_bench.cpp)
target_link_libraries(psitri-benchmark PUBLIC Boost::program_options psitri)
target_include_directories(psitri-benchmark PUBLIC ${Boost_INCLUDE_DIRS})
set_target_properties(psitri-benchmark PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
```

### Files to create/modify

| File | Action |
|---|---|
| `programs/psitri_bench.cpp` | **Create** — new benchmark tool |
| `programs/CMakeLists.txt` | **Modify** — add psitri-benchmark target |

---

## Phase 5: Tests for the public API

**Goal**: Add tests exercising the public API (separate from existing tree_context tests).

New test file: `libraries/psitri/tests/public_api_tests.cpp`

Tests:
1. `write_cursor` basic CRUD — insert, get, update, remove, verify
2. `write_cursor` iterate — insert N keys, iterate with cursor, verify sorted order
3. `transaction` commit — insert via transaction, commit, verify root persisted
4. `transaction` abort — insert via transaction, abort (or let destructor run), verify root unchanged
5. `transaction` sub-transaction — nested commit/abort
6. `session` get_root/set_root — round-trip root persistence
7. `database::create` + reopen — create, insert, close, reopen, verify data

### Build integration

Add to `test/CMakeLists.txt` in the `psitri-tests` sources list.

---

## Implementation Order

1. **Phase 1**: `write_cursor` (self-contained, no other API changes needed)
2. **Phase 2**: Session wiring + type registration
3. **Phase 3**: Transaction (depends on write_cursor + session)
4. **Phase 4**: Benchmark (first real consumer of the API, validates the design)
5. **Phase 5**: Public API tests (can be written alongside phases 1-3)

Phases 1-3 can be validated incrementally by building `psitri-tests` after each phase.

---

## Key Files Reference

| File | Role |
|---|---|
| `include/psitri/tree_ops.hpp` | Internal engine — tree_context with insert/upsert/remove |
| `include/psitri/cursor.hpp` | Read-only cursor with seek/next/prev/get |
| `include/psitri/upsert_mode.hpp` | Mode flags for insert/update/upsert/remove |
| `include/psitri/value_type.hpp` | Value wrapper (view, value_node, subtree, remove) |
| `include/psitri/database.hpp` | Database entry point with 512 roots |
| `include/psitri/database_impl.hpp` | database_state with top_root array |
| `include/psitri/read_session.hpp` + `_impl.hpp` | Read session with allocator_session |
| `include/psitri/write_session.hpp` + `_impl.hpp` | Write session (currently minimal) |
| `include/psitri/transaction.hpp` | Current transaction stub |
| `tests/tree_context_tests.cpp` | Existing tests using internal API |
| `tests/database_test.cpp` | Aspirational test showing target API |
| `programs/bench.cpp` | Old arbtrie benchmark (reference) |
| `sal/allocator_session.hpp` | SAL session with get_root/set_root/start_transaction |
| `sal/transaction.hpp` | SAL transaction with commit/abort |

---

## Verification

After each phase:
1. `cd build/debug && make psitri-tests && ./bin/psitri-tests` — existing tests still pass
2. After Phase 4: `make psitri-benchmark && ./bin/psitri-benchmark --items 100000 --round 1` — benchmark runs
3. After Phase 5: `./bin/psitri-tests '[public-api]'` — new tests pass

Full validation:
```bash
cd build/debug
cmake ../.. && make -j
./bin/psitri-tests
./bin/psitri-benchmark --reset --items 1000000 --round 3
./bin/psitri-benchmark --stat  # verify stats output
```
