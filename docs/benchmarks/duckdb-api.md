# Drop-In: DuckDB Storage

> **Same SQL engine, different storage backend.** This benchmark runs DuckDB
> with PsiTri as the persistent storage layer vs DuckDB's native in-memory engine.

## What's Being Compared

- **PsiTri+DuckDB**: DuckDB with PsiTri storage extension (`libraries/psitri-duckdb/`)
- **DuckDB (native)**: DuckDB's built-in in-memory storage

Both use the DuckDB SQL API via the same `tatp-bench` binary. The `--engine`
flag selects the storage backend.

## How the Extension Works

PsiTri-DuckDB is a DuckDB `StorageExtension` that replaces the default storage
layer. Tables are stored as PsiTri DWAL roots with memcomparable key encoding
for primary keys. The SQL parser, optimizer, and execution engine are unchanged.

```cpp
psitri_duckdb::RegisterPsitriStorage(db);
conn.Query("ATTACH '/path/to/db' AS mydb (TYPE psitri)");
conn.Query("CREATE TABLE mydb.main.users(id INTEGER PRIMARY KEY, name VARCHAR)");
```

Each table maps to a separate DWAL root. Primary keys are encoded as sortable
byte strings so PsiTri's ordered key-value store provides correct SQL ordering.

## TATP Benchmark

The TATP workload (7 transaction types, 4 tables, 80% reads / 20% writes)
tests realistic OLTP performance.

!!! note "Benchmark data needed"
    Run `bench/run_all.sh tatp` to collect data on your machine.

Results will appear in `docs/data/tatp/<date>/<machine>/`.

### What to Expect

PsiTri+DuckDB provides **persistent storage** -- data survives process restart.
Native DuckDB is in-memory only in this configuration. The comparison shows the
cost of persistence: how much throughput do you give up for durable storage?

For read-heavy OLTP workloads, PsiTri's COW trie and MVCC snapshots keep reads
fast even under write load. For write-heavy workloads, the DWAL's ART buffer
absorbs bursts without blocking the SQL engine.

## Current Status

The PsiTri-DuckDB extension is an **early-stage integration** demonstrating
PsiTri as a storage backend for a sophisticated query engine. It supports:

- CREATE TABLE with primary keys and secondary indexes
- INSERT / SELECT / UPDATE / DELETE
- Filter pushdown (PK equality, range filters)
- Transaction isolation via DWAL + MVCC snapshots
- ~20 SQL types (INTEGER, BIGINT, VARCHAR, DATE, TIMESTAMP, UUID, etc.)

Not yet supported: multi-threaded query execution, advanced query optimization,
distributed transactions.

## Reproducing

```bash
# Build
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=clang-20 -DCMAKE_CXX_COMPILER=clang++-20 \
    -B build/release
cmake --build build/release -j16 --target tatp-bench

# PsiTri storage
./build/release/libraries/psitri-duckdb/tatp-bench \
    --engine psitri --subscribers 10000 --duration 10

# DuckDB native (in-memory)
./build/release/libraries/psitri-duckdb/tatp-bench \
    --engine duckdb --subscribers 10000 --duration 10
```
