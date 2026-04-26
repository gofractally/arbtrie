# PsiTri

A persistent, transactional key-value store built on an adaptive radix trie with sorted B-tree leaves.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-gofractally.github.io%2Farbtrie-purple)](https://gofractally.github.io/arbtrie/)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)]()

## Key Features

- **Node-level copy-on-write** — mutations copy individual nodes (64-byte multiples), not 4 KB pages. ~9x less write amplification than page-based COW databases.
- **1.25 bytes per child pointer** — the lowest per-child overhead of any pointer-based tree structure. See [the analysis](https://gofractally.github.io/arbtrie/architecture/control-blocks/#per-child-pointer-overhead-how-psitri-compares).
- **O(log n) range operations** — count or delete millions of keys without scanning leaves.
- **Zero-cost snapshots** — O(1) via reference counting on the root node.
- **Composable subtrees** — store entire trees as values with O(1) attach/detach.
- **Self-tuning MFU cache** — hot objects are physically relocated to pinned RAM. No configuration needed.
- **Scales beyond RAM** — sequential writes, object-granularity caching, and clean page eviction.
- **Lock-free reads** — MVCC snapshot isolation with wait-free read locks.

## Quick Start

### Prerequisites

- C++20 compiler (Clang 15+ or GCC 12+)
- CMake 3.16+
- Ninja (recommended) or Make

### Build

```bash
git clone https://github.com/gofractally/arbtrie.git
cd arbtrie
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release -j$(nproc)
```

### Run Tests

```bash
cmake --build build/release --target psitri-tests
./build/release/bin/psitri-tests
```

### Run Examples

```bash
cmake --build build/release --target example-basic_crud
./build/release/bin/examples/example-basic_crud
```

### Install

```bash
cmake --install build/release --prefix /usr/local
```

Then in your project's `CMakeLists.txt`:

```cmake
find_package(psitri REQUIRED)
target_link_libraries(myapp PRIVATE psitri::psitri)
```

## Example

```cpp
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>

auto db = psitri::database::open("mydb", psitri::open_mode::create_or_open);
auto ws = db->start_write_session();

// Transactions commit atomically
{
    auto tx = ws->start_transaction(0);
    tx.upsert("hello", "world");
    tx.upsert("count", "42");
    tx.commit();
}

// Point lookups
{
    auto tx = ws->start_transaction(0);
    auto val = tx.get<std::string>("hello");  // "world"
    tx.abort();
}
```

More examples: [examples/](examples/) | [Full documentation](https://gofractally.github.io/arbtrie/)

## SQLite Drop-In Replacement

PsiTri-SQLite replaces SQLite's B-tree storage engine with psitri's DWAL (Distributed Write-Ahead Log). The SQL parser, code generator, and VDBE run unchanged — only the on-disk layer is swapped.

```cmake
# Link against psitri-sqlite instead of system SQLite
target_link_libraries(myapp PRIVATE psitri-sqlite)
```

Your existing `#include <sqlite3.h>` code works unchanged — same `sqlite3_open`, `sqlite3_exec`, `sqlite3_prepare_v2` API:

```c
sqlite3* db;
sqlite3_open("mydb", &db);
sqlite3_exec(db, "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", 0, 0, 0);
sqlite3_exec(db, "INSERT INTO t VALUES(1, 'hello')", 0, 0, 0);
```

Durability is controlled through standard SQLite PRAGMAs:

| PRAGMA | Behavior |
|--------|----------|
| `synchronous=OFF` | No flush — fastest |
| `synchronous=NORMAL` | Write buffer flushed, no fsync |
| `synchronous=FULL` | `fsync()` at commit — survives OS crash |
| `fullfsync=ON` | `F_FULLFSYNC` at commit — survives power loss |

### Performance (TATP Telecom Benchmark)

10,000 subscribers, 10 seconds, single-threaded, WAL journal mode:

| Configuration | TPS | vs System SQLite |
|---|---:|---:|
| PsiTri-SQLite `sync=off` | 970K | **47x faster** |
| System SQLite `sync=off` | 21K | baseline |
| PsiTri-SQLite `sync=full` | 19K | **3.8x faster** |
| System SQLite `sync=full` | 5K | baseline |

```bash
# Run the benchmark yourself
cmake --build build/release --target tatp-bench
./build/release/libraries/psitri-duckdb/tatp-bench --engine sqlite --subscribers 10000 --sync off
./build/release/libraries/psitri-duckdb/tatp-bench --engine sqlite --subscribers 10000 --sync full
```

See [libraries/psitri-sqlite/](libraries/psitri-sqlite/) for architecture details.

## RocksDB Drop-In Replacement

PsiTriRocks provides a RocksDB-compatible API backed by PsiTri. Switch by relinking — no code changes needed:

```cmake
# Before: find_package(RocksDB REQUIRED)
find_package(psitri REQUIRED)
target_link_libraries(myapp PRIVATE psitri::psitrirocks)
```

Your existing `#include <rocksdb/db.h>` code works unchanged. Supports `DB::Open`, `Put`, `Get`, `Delete`, `WriteBatch`, iterators, snapshots, column families, and `DeleteRange`. See the [migration guide](https://gofractally.github.io/arbtrie/getting-started/rocksdb-migration/) for the full compatibility matrix.

## Documentation

**[gofractally.github.io/arbtrie](https://gofractally.github.io/arbtrie/)**

- [Why PsiTri](https://gofractally.github.io/arbtrie/why-psitri/problem/) — the problem with existing databases
- [Design Philosophy](https://gofractally.github.io/arbtrie/why-psitri/design/) — the three novel subsystems
- [Architecture](https://gofractally.github.io/arbtrie/architecture/overview/) — how it works internally
- [Control Blocks](https://gofractally.github.io/arbtrie/architecture/control-blocks/) — the keystone of the design
- [Benchmarks](https://gofractally.github.io/arbtrie/benchmarks/bank/) — performance comparisons

## Project Status

Under active development. The core data structure and allocator are stable and well-tested. The public API may change before 1.0.

## License

[MIT](LICENSE)
