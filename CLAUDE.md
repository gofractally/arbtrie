# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**PsiTri** is a persistent, transactional key-value store built on an adaptive radix trie with sorted B-tree leaves. It uses node-level copy-on-write (mutating individual 64-byte nodes rather than full pages) with MVCC snapshot isolation and an mmap-backed segment allocator.

## Build Commands

**Requires:** CMake 3.16+, Ninja (recommended), C++20 compiler (Clang 15+ preferred, GCC 12+ supported)

```bash
# Release build (recommended)
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
ninja -C build/release -j$(nproc)

# Debug build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -B build/debug
ninja -C build/debug
```

Key CMake options:
- `ENABLE_SANITIZER` — Enable Thread Sanitizer
- `ENABLE_COVERAGE` — Enable code coverage (lcov/genhtml)
- `BUILD_TESTS` — Build test suite (ON by default)
- `ENABLE_NATIVE_ARCH` — Compile with `-march=native` (ON by default)
- `BUILD_ROCKSDB_BENCH` / `BUILD_TIDESDB_BENCH` — Enable comparison benchmarks

## Running Tests

```bash
# Build and run all tests
cmake --build build/release --target psitri-tests sal-tests
./build/release/bin/psitri-tests
./build/release/bin/sal-tests

# Run a single Catch2 test tag
./build/release/bin/psitri-tests "[leaf_node]"

# Run a single named test
./build/release/bin/psitri-tests "write_cursor basic CRUD"

# Smart test runner (caches results in .test_state/, runs fastest first)
bash run_tests.sh
bash run_tests.sh --reset   # Clear cached state and rerun all

# Coverage report (Debug build)
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON
ninja -C build coverage     # Generates HTML in build/coverage/html/
```

Notable test tags: `[leaf_node]`, `[inner_node]`, `[cursor]`, `[trie]`, `[tree_context]`, `[remove]`, `[collapse]`, `[smart_ptr]`, `[subtree]`, `[recovery]`, `[fuzz]`, `[multi_writer]`, `[public-api]`

## Architecture

### Library Structure

| Library | Role |
|---------|------|
| `libraries/psitri/` | Main database — trie engine, transactions, sessions, nodes |
| `libraries/sal/` | Segment Allocator Library — persistent memory management (32 MB segments, ref-counting, COW) |
| `libraries/psitrirocks/` | RocksDB-compatible drop-in replacement API wrapping psitri |
| `libraries/mdbxrocks/` | MDBX backend wrapper (for benchmark comparisons) |
| `libraries/ucc/` | Unsigned character comparison / branch encoding utilities |
| `libraries/hash/` | xxhash utility library |

### Key Architectural Layers (psitri)

1. **Public API** — `database`, `write_session`, `read_session`, `transaction`, `write_cursor` in `include/psitri/`
2. **Tree Operations** — `tree_ops.hpp`, `tree_context` — core COW mutation logic, branch management
3. **Node Types** in `include/psitri/node/`:
   - `inner.hpp` — Trie radix nodes; children stored as compact 1.25 bytes/pointer using cache-line packing (128-byte multiples)
   - `leaf.hpp` — B-tree-style leaves with sorted key-value pairs
   - `value_node.hpp` — Wrapper for subtree references (composable subtrees)
   - `node.hpp` — Base definitions, `node_type` enum, `branch` type
4. **SAL Allocator** — `sal/include/sal/allocator.hpp` — mmap-backed persistent storage, reference counting, garbage collection via clean page eviction

### Design Principles

- **Node-level COW**: Mutations clone only the affected node(s); unchanged nodes are shared by reference
- **MVCC**: Read snapshots are taken by incrementing the root's reference count; readers never block writers
- **Composable subtrees**: Entire subtrees can be stored as values in other trees (`value_node`)
- **SAL segments**: All persistent data lives in 32 MB mmap'd segments managed by the Segment Allocator Library

### RocksDB Compatibility

`libraries/psitrirocks/` provides a drop-in RocksDB API. Include `<rocksdb/db.h>` from that library to use psitri with existing RocksDB code.

## Known Issues

See `BUGS.md` for active known issues with repro steps. Current open issue: 1-object leak in `ptr_alloc` bookkeeping in shared-mode update (seed=1337).

## Benchmarks

Benchmark targets: `psitri-benchmark`, `bank-bench-*`, `psitrirocks-bench`. Configure via CMake variables: `BENCH_NUM_OPERATIONS` (default 100M), `BENCH_NUM_THREADS` (default 8), `BENCH_KEY_SIZE` (default 16), `BENCH_VALUE_SIZE` (default 100).

Scripts `run_bank_matrix.sh` and `run_scale_bench.sh` automate multi-configuration benchmark runs.
