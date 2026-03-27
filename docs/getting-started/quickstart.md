# Quick Start

## Building

```bash
git clone https://github.com/gofractally/arbtrie.git
cd arbtrie
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release -j8
```

### Prerequisites

- C++20 compiler (Clang 15+ or GCC 12+)
- CMake 3.16+
- Ninja (recommended) or Make
- Boost (program_options, for benchmarks)

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

### Create a Database

```cpp
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>

// Create or open a database in the given directory
auto db = psitri::database::create("my_database");
```

The database stores data in memory-mapped files within the specified directory. The `create` factory method uses default configuration; for custom settings, construct with a `runtime_config`.

### Write Data

Sessions are thread-affine -- always create them on the thread that will use them. See [API Reference](api.md) for details.

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

auto val = tx.get<std::string>("user:alice");
if (val) {
    std::cout << "Alice: " << *val << std::endl;
}
```

### Iterate with a Cursor

```cpp
// Create a read-only cursor from a snapshot
auto read_ses = db->start_read_session();
auto cursor = read_ses->create_cursor(0);

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

// Range removal -- O(log n), not O(k)
tx.remove_range("log:2024-01", "log:2024-06");

tx.commit();
```

### Count Keys in a Range

```cpp
auto cursor = read_ses->create_cursor(0);

// O(log n) -- does not scan the keys
uint64_t count = cursor.count_keys("user:", "user:\xFF");
```

### Subtrees

```cpp
auto tx = session->start_transaction(0);

// Create a subtree (independent tree stored as a value)
auto sub = session->create_write_cursor();
sub->upsert("field1", "value1");
sub->upsert("field2", "value2");

// Store the subtree as a value
tx.upsert("my_subtree", sub->root());

// Read back the subtree
auto subtree_cursor = tx.get_subtree_cursor("my_subtree");

tx.commit();
```

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
auto db = psitri::database::create("my_database");

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
auto db = psitri::database::create("my_database");

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
         // Each cursor call takes a snapshot -- sees a consistent view
         auto cur = session->create_cursor(0);
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
- **Different roots = zero contention**: Writers to different roots never block each other.
- **Same root = serialized**: Only one writer per root at a time. `start_transaction()` blocks until the root is available.
- **Readers never block**: Readers take atomic snapshots and see a consistent view. They never interfere with writers or other readers.
- **Up to 64 concurrent sessions**: The database supports up to 64 threads with active sessions simultaneously.

## Next Steps

- [API Reference](api.md) -- full type and method documentation
- [Architecture Overview](../architecture/overview.md) -- how PsiTri works under the hood
- [Concurrent Writers](../architecture/concurrent-writers.md) -- how zero-contention multi-writer works internally
- [Bank Transaction Benchmark](../benchmarks/bank.md) -- performance comparison with RocksDB, MDBX, and more
