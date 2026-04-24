# Silkworm Case Study: PsiTri as MDBX Drop-In Replacement

## Overview

[Silkworm](https://github.com/erigontech/silkworm) is the C++ implementation of the
Ethereum execution client by the Erigon team. It uses MDBX (libmdbx) as its primary
database for storing blockchain state: accounts, storage, code, and block headers.

PsiTri provides `psitrimdbx` — an MDBX-compatible API shim backed by PsiTri's
adaptive radix trie with node-level COW and MVCC. This benchmark compares the two
backends on Silkworm's actual workload patterns.

## Architecture

Silkworm uses MDBX with DUPSORT tables for account and storage data. The `psitrimdbx`
shim maps these to PsiTri's DWAL (Database Write-Ahead Log) layer with composite
key encoding for DUPSORT emulation.

Key mapping:
| Silkworm Table | MDBX Type | psitrimdbx Encoding |
|---------------|-----------|---------------------|
| Account | DUPSORT | `escape(addr) + \x00\x00 + value` |
| Storage | DUPSORT | `escape(addr+slot) + \x00\x00 + value` |
| Code | Regular | Direct key-value |
| Header | Regular | Direct key-value |

## Building

```bash
cmake -B build -DBUILD_SILKWORM_CASE_STUDY=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build --target eth-state-bench
```

## Running

```bash
# Default: 500 blocks x 2000 ops each
./build/bin/eth-state-bench

# Custom configuration
./build/bin/eth-state-bench --blocks 1000 --ops 5000 --dir /tmp/bench_data
```

## Workload

The benchmark simulates Ethereum block processing:
- **Preload**: 100K accounts with storage and code
- **Per block**: Configurable operations with realistic mix
  - 70% reads (account lookups)
  - 20% updates (balance changes)
  - 8% inserts (new accounts)
  - 2% deletes (account removal)
- **Headers**: One header write per block

## Comparing with Native MDBX

To compare against native libmdbx, install it separately and build the benchmark
with the `USE_NATIVE_MDBX` flag (requires libmdbx headers and library):

```bash
cmake -B build -DBUILD_SILKWORM_CASE_STUDY=ON -DUSE_NATIVE_MDBX=ON
```
