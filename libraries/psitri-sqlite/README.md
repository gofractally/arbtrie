# psitri-sqlite

Drop-in replacement for SQLite's B-tree storage engine using psitri's DWAL (Distributed Write-Ahead Log). SQLite's SQL parser, code generator, and VDBE execute unchanged — only the on-disk storage layer is swapped.

## Architecture

```
 Application
     |
 sqlite3 API  (unchanged)
     |
 VDBE / Code Generator / Parser  (unchanged, from amalgamation)
     |
 btree.h interface  (~50 functions)
     |
 btree_psitri.cpp  ← replaces btree.c
     |
 psitri DWAL  (adaptive radix trie + WAL)
```

**Two translation units, one library:**

| File | Language | Role |
|------|----------|------|
| `src/btree_helpers.c` | C | `#include`s the SQLite amalgamation (with btree.c `#if 0`'d out). Provides C helper functions that need access to the internal `Mem` struct (key serialization, dummy Pager). |
| `src/btree_psitri.cpp` | C++ | Implements all `sqlite3Btree*` symbols as `extern "C"`. Uses psitri DWAL for storage. |

**Key design decisions:**

- **Global singleton per path** — A `PsitriDb` (containing `dwal_database` + metadata) is shared across all connections to the same database path, matching SQLite's shared-cache semantics.
- **DWAL root mapping** — Root 0 = metadata, Root 1 = `sqlite_schema` (hardcoded by SQLite), Roots 2+ = user tables/indexes allocated by `sqlite3BtreeCreateTable`.
- **INTKEY tables** — Rowid encoded as big-endian int64 with sign bit flipped for correct sort order. Value = record blob.
- **BLOBKEY indexes** — Key = raw index key blob. No value.
- **Placement new for C++ members** — SQLite allocates `BtCursor` via `malloc+memset`. C++ members (`std::optional<owned_merge_cursor>`, `std::string`) are constructed via placement new in `sqlite3BtreeCursor` and explicitly destructed in `sqlite3BtreeCloseCursor`.
- **Dummy Pager** — SQLite's VDBE and commit path dereference `Pager*` fields directly. A static zeroed Pager with `noLock=1`, `memDb=1`, `journalMode=OFF` satisfies these code paths without real I/O.

## Sync Modes

`PRAGMA synchronous` maps to psitri sync levels via `sqlite3BtreeSetPagerFlags`:

| PRAGMA | sal::sync_type | Behavior |
|--------|---------------|----------|
| `synchronous=OFF` | `none` | No flush. Data persists at OS discretion. |
| `synchronous=NORMAL` | `msync_async` | Write buffer flushed, no fsync. |
| `synchronous=FULL` | `fsync` | `fsync()` WAL at commit. Survives OS crash. |
| `synchronous=EXTRA` | `fsync` | Same as FULL (+ directory sync in real SQLite). |
| `fullfsync=ON` | `full` | `F_FULLFSYNC` at commit. Survives power loss. |

Sync is applied once per SQLite transaction commit (`sqlite3BtreeCommitPhaseOne`), not per individual INSERT/UPDATE/DELETE.

## Build

Built automatically as part of the main CMake project:

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
ninja -C build/release psitri-sqlite-smoke
./build/release/libraries/psitri-sqlite/psitri-sqlite-smoke
```

LTO is disabled for this library — the amalgamation triggers LLVM LTO bugs on arm64.

## TATP Benchmark Results

10,000 subscribers, 10 seconds, single-threaded, WAL journal mode:

| Configuration | Total TPS | vs System SQLite |
|---|---:|---:|
| Psitri-SQLite sync=off | 970K | **47x faster** |
| System SQLite sync=off | 21K | baseline |
| Psitri-SQLite sync=full | 19K | **3.8x faster** |
| System SQLite sync=full | 5K | baseline |

Run the benchmark:

```bash
ninja -C build/release tatp-bench
./build/release/libraries/psitri-duckdb/tatp-bench --engine sqlite --subscribers 10000 --sync off
./build/release/libraries/psitri-duckdb/tatp-bench --engine sqlite --subscribers 10000 --sync full
```

For comparison against system SQLite (requires system SQLite3 dev package):

```bash
ninja -C build/release tatp-bench-system-sqlite
./build/release/libraries/psitri-duckdb/tatp-bench-system-sqlite --engine sqlite --subscribers 10000 --sync full
```

## SQLite Test Suite Compatibility

We build SQLite's own `testfixture` (TCL test harness) linked against psitri-sqlite and run the core SQL test files:

```bash
# One-time setup: clone SQLite source and generate headers
git clone --depth 1 --branch version-3.51.3 https://github.com/sqlite/sqlite.git external/sqlite-src
cd external/sqlite-src && mkdir -p bld && cd bld && ../configure && make parse.h opcodes.h keywordhash.h

# Build and run
ninja -C build/release psitri-testfixture
bash libraries/psitri-sqlite/tests/run_sqlite_tests.sh
```

**Current results** (58 test files, ~6300 test cases):

| Category | Pass | Fail | Notes |
|----------|------|------|-------|
| Expression/function evaluation | 3494 | 3 | `expr`, `func`, `func2`, `cast`, `coalesce` |
| SELECT queries | 345 | 62 | Row order differences without ORDER BY |
| JOIN operations | 123 | 33 | `join`, `join2`, `join3`, `join5` fully pass |
| INSERT/UPDATE/DELETE | 188 | 285 | `changes()` counter not maintained; index sync issues |
| DDL (ALTER, triggers, views) | 119 | 94 | Multi-table operations need work |
| Type handling | 303 | 75 | `types3` nearly passes |
| **Total** | **~5255** | **~1074** | **83% pass rate** |

**Known failure categories:**

1. **Row ordering** — Queries without `ORDER BY` return rows in trie storage order (key-sorted) rather than btree insertion order. Not a correctness bug — SQL doesn't guarantee order without `ORDER BY`.
2. **`sqlite3_changes()` counter** — Returns 1 instead of actual row count. Our btree stub doesn't increment the VDBE change counter.
3. **Index integrity** — `PRAGMA integrity_check` reports "wrong # of entries in index" for some operations, indicating index/table row count divergence.
4. **Database re-open** — Tests that `forcedelete test.db` and re-create fail because psitri databases are directories, not files. On macOS Sequoia, `com.apple.provenance` extended attributes can prevent cleanup.

Tests that probe btree/pager internals (`test_btree.c`, pager counters, etc.) are expected to fail since those internals are replaced.

## Limitations

- **Single-writer per DWAL root** — Concurrent writes to the same table serialize at the DWAL level.
- **No `sqlite3_backup` API** — `backup.c` accesses Pager internals; backup operations will fail.
- **No shared cache** — `SQLITE_OMIT_SHARED_CACHE=1` is defined.
- **No incremental blob I/O** — `sqlite3BtreePutData` returns `SQLITE_READONLY`.
- **Amalgamation version** — Built against SQLite 3.51.3. Struct layouts in `sqlite3_btree_compat.h` must stay in sync if the amalgamation is updated.

## SQLite Amalgamation Modifications

The amalgamation (`sqlite3/sqlite3.c`) has three `#if 0` regions:

1. **Lines 71395–71414**: btmutex.c preamble (replaced by our mutex stubs)
2. **Lines 72159–72453**: btmutex.c functions (replaced by our mutex stubs)
3. **Lines 72455–84002**: btree.c (replaced by `btree_psitri.cpp`)

`btreeInt.h` (lines 71416–72157) is intentionally **not** excluded — `backup.c` and other amalgamation code reference the struct type declarations.
