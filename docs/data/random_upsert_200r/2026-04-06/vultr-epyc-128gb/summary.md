# Random Upsert Benchmark — 200 Rounds

## Test Parameters

| Parameter | Value |
|-----------|-------|
| Operation | Random upsert (hashed 64-bit keys) |
| Rounds | 200 (1M ops per round = 200M total) |
| Batch size | 100 ops per commit/WriteBatch |
| Value size | 256 bytes |
| Key size | 8 bytes (big-endian hash of sequence) |
| Concurrent readers | 0 (write-only) |
| Date | 2026-04-06 |

## Machine

| Spec | Value |
|------|-------|
| Host | Vultr VPS |
| CPU | AMD EPYC-Turin, 16 vCPUs |
| RAM | 128 GB |
| OS | Linux 6.17.0-20-generic x86_64 |
| Filesystem | ext4 (NVMe) |

## Engine Configuration

- **PsiTri DWAL**: pinned_cache=256 MB, merge_threads=2, max_rw=100K entries, sync=none
- **RocksDB**: default options (create_if_missing), WriteBatch size=100
- **MDBX**: UTTERLY_NOSYNC, commit_interval=100 ops, map_size=200 GB, MDBX_UPSERT

## Results Summary

| Metric | PsiTri DWAL | RocksDB | MDBX |
|--------|------------|---------|------|
| Rounds completed | **200** | **200** | 20 (MAP_FULL) |
| Total ops | 200M | 200M | 20M |
| Total time | 740s | — | — |
| **Avg first 30 rounds** | **1,588,320/s** | 1,155,254/s | 296,806/s |
| **Avg first 100 rounds** | **1,311,423/s** | 1,000,853/s | — |
| **Avg all 200 rounds** | **1,129,318/s** | 712,824/s | — |
| **Avg last 30 rounds** | **909,379/s** | 390,162/s | — |
| Peak throughput | 2,334,318/s | 1,622,951/s | 471,055/s |
| Min throughput | 24,791/s | 85,161/s | 162,650/s |
| Final DB size | 76.7 GB | 51.5 GB | MAP_FULL at 200 GB |

## PsiTri vs RocksDB Speedup

| Period | PsiTri | RocksDB | Speedup |
|--------|--------|---------|---------|
| First 30 rounds (in-RAM) | 1.59M/s | 1.16M/s | **1.37x** |
| First 100 rounds | 1.31M/s | 1.00M/s | **1.31x** |
| Full 200 rounds | 1.13M/s | 713K/s | **1.58x** |
| Last 30 rounds (beyond-RAM) | 909K/s | 390K/s | **2.33x** |

## Observations

### PsiTri DWAL
- Sustained high throughput throughout the entire run
- Merge stalls cause periodic drops to ~25-80K/s but recover quickly
- Throughput degrades gradually as DB grows beyond RAM (page cache pressure)
- Space amplification: ~76.7 GB for 200M keys x 264B = ~49 GB theoretical (~1.6x)

### RocksDB
- Strong initial throughput with LSM write-ahead-log buffering
- Classic compaction stall pattern: drops to 85-125K/s every 3-5 rounds
- Beyond round 150, throughput collapses to 200-400K/s sustained (compaction-bound)
- Most space-efficient: 51.5 GB (~1.05x theoretical) due to SSTable compaction

### MDBX (libmdbx)
- COW B-tree cannot reclaim pages fast enough with frequent commits (every 100 ops)
- MAP_FULL after only 20M keys despite 200 GB map allocation
- Space amplification is extreme: ~10x with commit_interval=100
- With commit_interval=10K, reaches 31M keys before MAP_FULL
- Not competitive for high-frequency random write workloads

## Files

- `psitri_dwal_200r.log` — Full PsiTri DWAL run (200 rounds)
- `rocksdb_200r.log` — Full RocksDB run (200 rounds, dense random only)
- `rocksdb_30r_with_seq.log` — Earlier RocksDB run with sequential + random (30 rounds each)
- `mdbx_ci100_20r.log` — MDBX with commit_interval=100 (20 rounds before MAP_FULL)
- `mdbx_ci10k_31r.log` — MDBX with commit_interval=10K (31 rounds before MAP_FULL)
- `psitri_dwal.csv` — Per-round PsiTri data for charting
- `rocksdb.csv` — Per-round RocksDB data for charting
- `mdbx.csv` — Per-round MDBX data for charting
