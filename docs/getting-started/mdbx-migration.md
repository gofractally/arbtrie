# MDBX Drop-In Migration

PsiTriMDBX provides an MDBX-compatible API backed by PsiTri's DWAL (Durable Write-Ahead Log) layer. Existing applications that use the libmdbx C or C++ API can switch to PsiTri by relinking — no code changes required for standard operations.

## Quick Migration

**Before (native libmdbx):**
```cmake
find_package(mdbx REQUIRED)
target_link_libraries(myapp PRIVATE mdbx::mdbx)
```

**After (PsiTriMDBX):**
```cmake
find_package(psitri REQUIRED)
target_link_libraries(myapp PRIVATE psitri::psitrimdbx)
```

Your application code stays the same:

```cpp
#include <mdbx.h>

MDBX_env* env = nullptr;
mdbx_env_create(&env);
mdbx_env_set_geometry(env, 1024*1024, 1024*1024, 64ULL*1024*1024*1024, -1, -1, -1);
mdbx_env_open(env, "/tmp/mydb", MDBX_CREATE | MDBX_SAFE_NOSYNC, 0664);

// Transactions
MDBX_txn* txn = nullptr;
mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)0, &txn);

MDBX_dbi dbi;
mdbx_dbi_open(txn, nullptr, MDBX_CREATE, &dbi);

// Put / Get
MDBX_val key = {"hello", 5};
MDBX_val val = {"world", 5};
mdbx_put(txn, dbi, &key, &val, MDBX_UPSERT);
mdbx_txn_commit(txn);

// Read
MDBX_txn* ro_txn;
mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)MDBX_TXN_RDONLY, &ro_txn);
MDBX_val result;
mdbx_get(ro_txn, dbi, &key, &result);
mdbx_txn_abort(ro_txn);

// Cursors
MDBX_cursor* cur;
mdbx_cursor_open(txn, dbi, &cur);
MDBX_val k, v;
int rc = mdbx_cursor_get(cur, &k, &v, MDBX_FIRST);
while (rc == MDBX_SUCCESS) {
    // process k, v
    rc = mdbx_cursor_get(cur, &k, &v, MDBX_NEXT);
}
mdbx_cursor_close(cur);

mdbx_env_close(env);
```

The C++ API (`mdbx::env_managed`, `mdbx::txn_managed`, etc.) is also supported:

```cpp
#include <mdbx.h++>

mdbx::env_managed env(
    "/tmp/mydb",
    mdbx::env_managed::create_parameters{},
    mdbx::env::operate_parameters{});

auto txn = env.start_write();
auto map = txn.create_map("accounts", mdbx::key_mode::usual, mdbx::value_mode::single);
txn.upsert(map, mdbx::slice("alice"), mdbx::slice("1000"));
txn.commit();

auto ro = env.start_read();
auto val = ro.get(map, mdbx::slice("alice"));
```

## What's Supported

| API | Status | Notes |
|-----|--------|-------|
| `mdbx_env_create` / `mdbx_env_open` / `mdbx_env_close` | Supported | |
| `mdbx_env_set_geometry` | Accepted | Stored but ignored — PsiTri manages its own sizing |
| `mdbx_env_set_maxdbs` / `mdbx_env_set_maxreaders` | Accepted | |
| `mdbx_txn_begin` / `mdbx_txn_commit` / `mdbx_txn_abort` | Supported | |
| `mdbx_txn_reset` / `mdbx_txn_renew` | Supported | Optimized: session reuse, no destroy/recreate |
| `mdbx_dbi_open` / `mdbx_dbi_close` | Supported | Named DBIs persisted across restarts |
| `mdbx_put` / `mdbx_get` / `mdbx_del` | Supported | |
| `mdbx_cursor_open` / `mdbx_cursor_get` / `mdbx_cursor_put` | Supported | FIRST, LAST, NEXT, PREV, SET, SET_RANGE |
| `mdbx_cursor_on_first` / `mdbx_cursor_on_last` | Supported | |
| `mdbx_drop` | Supported | Both clear (del=0) and delete (del=1) |
| `mdbx_dbi_stat` | Supported | Real entry counting |
| `mdbx_replace` | Supported | |
| `MDBX_DUPSORT` | Supported | Composite key encoding |
| `MDBX_NOOVERWRITE` / `MDBX_UPSERT` | Supported | |
| `mdbx_env_set_userctx` / `mdbx_env_get_userctx` | Supported | |
| C++ API (`mdbx::env_managed`, `mdbx::txn_managed`, etc.) | Supported | |

## What's Not Supported

| Feature | Status |
|---------|--------|
| Nested transactions | Returns `MDBX_ENOSYS` |
| `MDBX_INTEGERKEY` / `MDBX_INTEGERDUP` | Not yet implemented |
| `mdbx_env_copy` / `mdbx_env_copyfd` | Not yet implemented |
| Multi-DBI transaction atomicity | Each DBI commits independently |
| Geometry enforcement | PsiTri self-manages storage; geometry parameters are accepted but ignored |

## PsiTri Extension: Read Modes

PsiTriMDBX exposes a read mode selector that controls how read-only transactions query the three-layer DWAL architecture (RW btree, RO snapshot, trie). This allows applications to choose the optimal tradeoff between read freshness, read throughput, and write contention.

```cpp
#include <mdbx.h>

// After mdbx_env_open:
mdbx_env_set_read_mode(env, PSITRI_READ_MODE_BUFFERED);
```

### Available Modes

| Mode | Constant | Layers Searched | Locking | Behavior |
|------|----------|----------------|---------|----------|
| **Trie** | `PSITRI_READ_MODE_TRIE` | Tri only | None | Reads only data that has been merged into the COW trie. Fastest reads, but may not see recently committed data that is still in the WAL btree layers. |
| **Buffered** | `PSITRI_READ_MODE_BUFFERED` | RO snapshot + Tri | None | Reads from a cached RO btree snapshot plus the COW trie. The snapshot is refreshed automatically when the DWAL generation counter advances (after a writer swaps RW→RO). No lock contention with writers. |
| **Latest** | `PSITRI_READ_MODE_LATEST` | RW + RO + Tri | Shared lock on RW layer | Sees all committed data including writes not yet swapped to the RO layer. Acquires a shared lock on the RW btree, which can contend with the writer's exclusive lock during commits. |

The default mode is **Latest**, which matches native MDBX semantics (readers see the most recently committed data).

### How It Works

PsiTri's DWAL layer maintains three data tiers:

```
  ┌─────────────┐
  │  RW btree   │  ← Active writes land here (exclusive lock on commit)
  ├─────────────┤
  │  RO btree   │  ← Frozen snapshot after RW→RO swap (read-only, no locks)
  ├─────────────┤
  │  PsiTri     │  ← Persistent COW trie (merged from RO by background thread)
  └─────────────┘
```

A point lookup in **Latest** mode searches all three layers top-to-bottom, stopping at the first hit. **Buffered** mode skips the RW layer (zero contention with writers). **Trie** mode reads only the bottom trie layer (fastest, but sees only fully merged data).

The `dwal_read_session` caches per-root snapshots and refreshes them only when the generation counter changes (an atomic load). On the fast path, a read is: one atomic gen check → cached btree map lookup → trie cursor lookup. No mutexes acquired.

### Performance Impact

Benchmark: 100K accounts, 1M transactions, batch=100, one concurrent reader thread.

| Read Mode | Write-Only | Write+Read | Reader | Write Impact |
|-----------|-----------|------------|--------|-------------|
| **Trie** (Tri only) | 452K tx/s | 260K tx/s | **1,535K reads/s** | -42% |
| **Buffered** (RO + Tri) | 419K tx/s | 254K tx/s | 1,166K reads/s | -39% |
| **Latest** (RW + RO + Tri) | 465K tx/s | 373K tx/s | 324K reads/s | -20% |
| Native MDBX (reference) | 38K tx/s | 79K tx/s | 1,277K reads/s | +109% |

Key observations:

- **Trie mode** delivers the fastest reads (1.53M/s — 20% faster than native MDBX) because it reads directly from the COW trie with zero lock contention. The tradeoff is that very recently committed data may not be visible until the background merge thread processes it.
- **Buffered mode** is the closest match to native MDBX's snapshot semantics. At 1.17M reads/s it is within 10% of native MDBX read throughput while writing 11x faster.
- **Latest mode** has the lowest write impact (-20%) because the reader is slower (shared lock contention), so it spends less time competing with the writer. Best for workloads where write throughput matters more than read throughput.

Choose the mode based on your workload:

- **Read-heavy with stale tolerance:** `PSITRI_READ_MODE_TRIE`
- **Read-heavy with fresh data:** `PSITRI_READ_MODE_BUFFERED`
- **Write-heavy with occasional reads:** `PSITRI_READ_MODE_LATEST` (default)

## Performance

In the [bank transaction benchmark](../benchmarks/bank.md), PsiTriMDBX achieves dramatically higher write throughput than native MDBX while maintaining competitive read performance, using the identical MDBX C API:

| Metric | PsiTriMDBX | Native MDBX | Ratio |
|--------|-----------|-------------|-------|
| Write-only | 452K tx/s | 38K tx/s | **12x faster** |
| Write+Read | 260–373K tx/s | 79K tx/s | **3.3–4.7x faster** |
| Reader (trie) | 1,535K reads/s | 1,277K reads/s | **1.2x faster** |
| Reader (buffered) | 1,166K reads/s | 1,277K reads/s | 0.9x |
| Total wall time | 4.9–6.8s | 40.8s | **6–8x faster** |

## DBI Persistence

Named DBIs (created with `mdbx_dbi_open`) are automatically persisted across database restarts. The DBI catalog is stored directly in PsiTri root 0, bypassing the WAL layer to ensure it survives clean-close marking.

On reopen, `mdbx_env_open` restores all previously created named DBIs with their original flags (including `MDBX_DUPSORT`).

## Building

PsiTriMDBX is always built as part of the PsiTri project:

```bash
cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -B build/release
cmake --build build/release
```

The library is available as `libpsitrimdbx.a` and the `psitrimdbx` CMake target.

To run the MDBX compatibility tests:

```bash
./build/release/bin/mdbx-tests
```

To run the bank benchmark comparing PsiTriMDBX against native libmdbx:

```bash
# PsiTriMDBX (all read modes)
./build/release/bin/bank-bench-psitrimdbx --num-accounts 100000 \
    --num-transactions 1000000 --batch-size 100 --read-mode trie

./build/release/bin/bank-bench-psitrimdbx --num-accounts 100000 \
    --num-transactions 1000000 --batch-size 100 --read-mode buffered

./build/release/bin/bank-bench-psitrimdbx --num-accounts 100000 \
    --num-transactions 1000000 --batch-size 100 --read-mode latest

# Native libmdbx (requires libmdbx source in build/_deps)
./build/release/bin/bank-bench-mdbx --num-accounts 100000 \
    --num-transactions 1000000 --batch-size 100
```
