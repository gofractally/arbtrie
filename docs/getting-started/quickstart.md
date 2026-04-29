# Quick Start

## Building

```bash
git clone https://github.com/gofractally/arbtrie.git
cd arbtrie
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release -j8
```

### Prerequisites

**Compiler:** Clang 18+ is required. GCC is not supported — the codebase uses
Clang-specific flags (`-Wno-vla-extension`) and ARM NEON code that is Clang-only
in several benchmark files.

**macOS (Apple Silicon)**
```bash
brew install llvm cmake ninja boost catch2
```
CMake picks up Homebrew LLVM automatically via the `if(APPLE)` block in
`CMakeLists.txt`.

**Linux (Ubuntu 22.04+ / Debian)**
```bash
sudo apt-get install -y \
    clang-20 \
    cmake \
    ninja-build \
    libsqlite3-dev \
    libboost-all-dev \
    libcatch2-dev
```
Pass the compiler explicitly when configuring:
```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_CXX_COMPILER=clang++-20 \
      -DCMAKE_C_COMPILER=clang-20 \
      -B build/debug
```

| Dependency | Minimum version | Purpose |
|------------|----------------|---------|
| Clang      | 18             | Compiler (GCC not supported) |
| CMake      | 3.16           | Build system |
| Ninja      | any            | Build engine (recommended) |
| SQLite3    | 3.x            | sqlite-arb-test benchmark (optional) |
| Boost      | 1.71           | `program_options` (benchmarks) |
| Catch2     | 3.x            | Unit test framework |

### Build Configurations

| Directory | Type | Flags | Use |
|-----------|------|-------|-----|
| `build/release` | Release | `-O3`, LTO | Performance testing |
| `build/debug` | Debug | `-O0 -g` | Development/debugging |
| `build/coverage` | Coverage | `-O1 -DNDEBUG -fprofile-arcs -ftest-coverage -g` | Coverage reports |

## Running Tests

```bash
cmake --build build/release -j8 --target psitri-tests
./build/release/bin/psitri-tests
```

## Your First Database

### Open a Database

```cpp
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>

// Open an existing database, or create it if it does not exist.
auto db = psitri::database::open(
    "my_database",
    psitri::open_mode::create_or_open);
```

The database stores data in memory-mapped files within the specified directory.
Use `open_mode::create_only` when the directory must not already contain a
database, and `open_mode::open_existing` when startup should fail if the
database is missing.

### Write Data

Write sessions are thread-affine allocator contexts. Each one owns a 32 MB write
buffer. Create one on the thread that will use it, keep it for the life of that
worker thread, and create many transactions from it. Do not create a new write
session for each small operation. See [Transaction and Cursor
Contract](transaction-contract.md) for details.

```cpp
auto session = db->start_write_session();

// All mutations happen inside transactions
auto tx = session->start_transaction(0);  // root index 0

tx.upsert("user:alice", "{"name": "Alice", "balance": 1000}");
tx.upsert("user:bob",   "{"name": "Bob", "balance": 2500}");
tx.insert("user:carol", "{"name": "Carol", "balance": 500}");

tx.commit();  // atomic root swap -- visible to all readers instantly
```

- `upsert` inserts or updates
- `insert` only inserts (returns `false` if key exists)
- `update` only updates (returns `false` if key doesn't exist)
- If the transaction is destroyed without `commit()`, it auto-aborts

### Read Data

```cpp
// Read within a transaction
auto tx = session->start_transaction(0);

// Zero-copy: value is valid only inside the lambda.
bool found = tx.get("user:alice", [](psitri::value_view value) {
   std::cout << "Alice: " << value << std::endl;
});

// Copy when the value must outlive the callback.
auto owned = tx.get<std::string>("user:alice");
```

Keep zero-copy callbacks short. Holding the view for a long-running function can
delay compaction while PsiTri protects the underlying storage.

### Iterate with a Cursor

```cpp
// Create a read-only cursor from a snapshot
auto read_ses = db->start_read_session();
auto cursor = read_ses->snapshot_cursor(0);

// Forward iteration
cursor.seek_begin();
while (!cursor.is_end()) {
    std::cout << cursor.key() << std::endl;
    cursor.next();
}

// Range scan
cursor.lower_bound("user:");
while (!cursor.is_end() && cursor.key().starts_with("user:")) {
    std::cout << cursor.key() << std::endl;
    cursor.next();
}
```

### Remove Data

```cpp
auto tx = session->start_transaction(0);

// Single key removal
tx.remove("user:bob");

// Range removal when only success/not-found matters.
tx.remove_range_any("log:2024-01", "log:2024-06");

// Ask for an exact count only when the count is needed.
uint64_t removed = tx.remove_range_counted("tmp:", "tmp:\xFF");

tx.commit();
```

### Count Keys in a Range

```cpp
auto cursor = read_ses->snapshot_cursor(0);

// O(log n) -- does not scan the keys
uint64_t count = cursor.count_keys("user:", "user:\xFF");
```

### Subtrees

```cpp
// Build a detached tree, then store it as a subtree value.
auto sub = session->create_temporary_tree();
auto sub_tx = session->start_write_transaction(
    std::move(sub),
    psitri::tx_mode::expect_success);
sub_tx.upsert("field1", "value1");
sub_tx.upsert("field2", "value2");
sub = sub_tx.get_tree();

auto tx = session->start_transaction(0);
tx.upsert_subtree("my_subtree", std::move(sub));

// Read back the subtree
{
   auto stored = tx.subtree_transaction("my_subtree", psitri::subtree_open::must_exist);
   stored.get("field1", [](psitri::value_view field1) {
      use_field(field1);
   });
   stored.abort();
}

tx.commit();
```

Existing roots, snapshots, and stored subtrees can be exposed as copyable tree
handles when you ask for them. This lets applications archive or copy subtrees
without deep-copying every key/value pair, but it also means cycles are a real
footgun: do not store a tree inside itself, inside one of its descendants, or in
any indirect cycle. Cycles may keep reference-counted storage alive forever.

### Sync and Durability

```cpp
// Set sync mode per session
session->set_sync(sal::sync_type::mprotect);  // default is sync_type::none (no sync)

// Or sync the entire database explicitly
db->sync();
```

See [Write Protection & Durability](../internals/write-protection.md) for details on sync modes.

## 512 Independent Roots

PsiTri supports up to 512 independent top-level roots. Each root is a separate tree with its own key space. Writers to different roots don't conflict:

```cpp
auto tx0 = session->start_transaction(0);   // root 0: user data
auto tx1 = session->start_transaction(1);   // root 1: indexes

tx0.upsert("user:alice", "data");
tx1.upsert("idx:age:25:alice", "");

tx0.commit();
tx1.commit();
```

## Multi-Threaded Writers

Multiple threads can write to **different roots** simultaneously with zero contention. The key rule: create the session on the thread that will use it.

```cpp
auto db = psitri::database::open("my_database", psitri::open_mode::create_or_open);

const int num_writers = 4;
std::vector<std::thread> writers;

for (int w = 0; w < num_writers; ++w)
{
   writers.emplace_back([&db, w]()
   {
      // Each thread creates its own session -- do NOT share sessions across threads
      auto session = db->start_write_session();

      // Each writer uses a different root (1, 2, 3, 4) -- no locking between them
      auto tx = session->start_transaction(w + 1);

      for (int i = 0; i < 10000; ++i)
      {
         std::string key = "key:" + std::to_string(i);
         std::string val = "writer_" + std::to_string(w);
         tx.upsert(key, val);
      }

      tx.commit();  // atomic root swap -- only touches root w+1
   });
}

for (auto& t : writers)
   t.join();
```

Writers to the **same root** are serialized by a per-root mutex -- the transaction blocks until the previous writer on that root commits or aborts:

```cpp
// 4 threads writing to the SAME root -- safe but serialized
for (int w = 0; w < num_writers; ++w)
{
   writers.emplace_back([&db, w]()
   {
      auto session = db->start_write_session();

      // start_transaction(1) acquires the per-root mutex for root 1
      // -- blocks until the previous writer commits or aborts
      auto tx = session->start_transaction(1);

      for (int i = 0; i < 1000; ++i)
      {
         std::string key = "w" + std::to_string(w) + ":key:" + std::to_string(i);
         tx.upsert(key, "data");
      }

      tx.commit();  // releases the per-root mutex
   });
}
```

## Multi-Threaded Readers

Reader threads can iterate and query concurrently with writers. Readers never block writers, and writers never block readers. Each reader sees a consistent snapshot.

```cpp
auto db = psitri::database::open("my_database", psitri::open_mode::create_or_open);

std::atomic<bool> writers_done{false};
std::vector<std::thread> threads;

// Writer thread: continuously inserting data
threads.emplace_back([&db, &writers_done]()
{
   auto session = db->start_write_session();
   for (int round = 0; round < 100; ++round)
   {
      auto tx = session->start_transaction(0);
      for (int i = 0; i < 1000; ++i)
      {
         std::string key = "r" + std::to_string(round) + ":k" + std::to_string(i);
         tx.upsert(key, "value");
      }
      tx.commit();
   }
   writers_done.store(true);
});

// Reader threads: scanning the tree while writes are in progress
for (int r = 0; r < 4; ++r)
{
   threads.emplace_back([&db, &writers_done]()
   {
      // Each reader creates its own session on its own thread
      auto session = db->start_read_session();

      while (!writers_done.load())
      {
         // Each snapshot cursor sees a consistent view
         auto cur = session->snapshot_cursor(0);
         cur.seek_begin();
         uint64_t count = 0;
         while (!cur.is_end())
         {
            ++count;
            cur.next();
         }
         // count is always consistent -- never sees a partial transaction
      }
   });
}

for (auto& t : threads)
   t.join();
```

### Key Points

- **Sessions are thread-affine**: Always call `start_write_session()` or `start_read_session()` from the thread that will use it. Never create a session on one thread and pass it to another.
- **Write sessions are long-lived**: A write session is an allocator context with a 32 MB write buffer. Use one per writer thread and reuse it for many transactions. Do not create write sessions repeatedly for small operations.
- **Different roots = zero contention**: Writers to different roots never block each other.
- **Same root = serialized**: Only one writer per root at a time. `start_transaction()` blocks until the root is available.
- **Readers never block**: Readers take atomic snapshots and see a consistent view. They never interfere with writers or other readers.
- **Up to 50 application write sessions**: The allocator has 64 session slots total; 14 are reserved for backend and compaction work.

## Next Steps

- [API Reference](api.md) -- full type and method documentation
- [Architecture Overview](../architecture/overview.md) -- how PsiTri works under the hood
- [Concurrent Writers](../architecture/concurrent-writers.md) -- how zero-contention multi-writer works internally
- [Bank Transaction Benchmark](../benchmarks/bank.md) -- performance comparison with RocksDB, MDBX, and more
