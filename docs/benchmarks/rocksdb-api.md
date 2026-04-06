# Drop-In: RocksDB API

> **Same API, same workload, different engine.** This benchmark uses identical
> `rocksdb::DB` calls -- only the storage backend changes.

## What's Being Compared

- **PsiTriRocks**: PsiTri's RocksDB-compatible wrapper (`libraries/psitrirocks/`)
- **RocksDB**: Native RocksDB (linked against system or vendored build)

Both use the same `rocks_bench.cpp` source compiled against different libraries.
The benchmark binary is `psitrirocks-bench` (PsiTri) and `rocksdb-bench` (native).

## Workloads

The standard `db_bench` workload set:

| Workload | Description |
|----------|-------------|
| `fillrandom` | Insert N random keys |
| `overwrite` | Overwrite N existing keys (random order) |
| `readrandom` | Point lookups of N random keys |
| `readseq` | Sequential scan of all keys |
| `seekrandom` | Random seeks + short forward scans |
| `deleteseq` | Sequential deletion of all keys |
| `deleterandom` | Random deletion of N keys |

Parameters: 1M keys, batch size 1, 256-byte values.

## Results

!!! note "Benchmark data needed"
    Run `bench/run_all.sh rocksdb_api` to collect data on your machine.
    Requires `BUILD_ROCKSDB_BENCH=ON` in your CMake configuration.

Results will appear in `docs/data/rocksdb_api/<date>/<machine>/`.

## Bank Benchmark (Transactional Workload)

The [bank transaction benchmark](bank.md) provides a higher-level comparison
using the RocksDB API for realistic transactional operations. PsiTriRocks
achieves **95% of native PsiTri performance** and **2.9x faster** than native
RocksDB on TPC-B style workloads:

| Engine | Transfers/sec | Relative |
|--------|--------------|----------|
| PsiTri (native) | 376,691 | 1.00x |
| **PsiTriRocks** | **356,703** | **0.95x** |
| RocksDB | 126,341 | 0.34x |

See the [RocksDB migration guide](../getting-started/rocksdb-migration.md) for
API compatibility details and how to switch.

## Reproducing

```bash
# Build with RocksDB comparison enabled
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=clang-20 -DCMAKE_CXX_COMPILER=clang++-20 \
    -DBUILD_ROCKSDB_BENCH=ON -B build/release
cmake --build build/release -j16

# Run the benchmark suite
bench/run_all.sh rocksdb_api

# Or run the CMake comparison target directly
cmake --build build/release --target bench-compare
```
