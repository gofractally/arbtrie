# KV Benchmark Results — PsiTri

Raw key-value operation benchmark measuring insert, get, upsert, iterate, lower-bound,
remove, and multi-writer throughput as the dataset grows from 0 to 100M+ keys.

## Configuration

| Parameter | Value |
|-----------|-------|
| Rounds | 100 |
| Items per round | 1,000,000 |
| Batch size | 512 |
| Value size | 8 bytes |
| Key format | 8-byte big-endian (seq), 8-byte hash (rand), variable string (str) |
| Sync mode | none |
| Writer threads | 4 (multiwriter tests) |
| Machine | Apple Silicon, 16GB RAM |

## Summary

| Test | Throughput | Dataset | Notes |
|------|-----------|---------|-------|
| **Seq insert (BE)** | 5.07M → 4.21M/sec | 0→100M | 17% degradation over 100M keys |
| **Random insert** | 2.86M → 823K/sec | 0→100M | Expected: random access pattern |
| **String random insert** | 1.51M → 601K/sec | 0→100M | Variable-length keys, most expensive |
| **Seq get (BE)** | 4,175,219/sec | 300M keys | 100% hit rate |
| **Random get** | 1,352,231/sec | 300M keys | 100% hit rate, dense random |
| **Random get (post-delete)** | 1,402,980/sec | 200M keys | After 100M seq removes |
| **Seq upsert (BE)** | 4.56M → 4.79M/sec | 300M keys | Remarkably flat — no degradation |
| **Iterate** | 75,906,171 keys/sec | 300M keys | Full scan in 3.95s |
| **Lower bound** | 1,492,637/sec | 200M keys | Random range seeks |
| **Seq remove** | 4.65M → 5.48M/sec | 200M→100M | Accelerates as tree shrinks |
| **Random remove** | 1.03M → 2.40M/sec | 100M→0 | Accelerates as tree shrinks |
| **Multi-writer rand (4T)** | 5.49M aggregate | 400M total | 1.37M/writer |
| **Multi-writer seq (4T)** | ~15.9M aggregate | 249M+ total | ~4.0M/writer |

## Degradation Profiles

### Sequential Insert (Big-Endian Keys) — 0 to 100M

Remarkably stable. Tree depth increases cause periodic dips that recover after
internal reorganization. Overall: 5.07M/sec at round 0, still 4.21M/sec at round 99.

```
ops/sec (M)
 5.1 |****
 5.0 | ***********
 4.9 |            *
 4.8 |
 4.7 |             **
 4.6 |               *******          ******          ******          *******
 4.5 |              *       ***      *      **       *      **      *       ****
 4.4 |                         ****        ** **          ** **          *      **
 4.3 |                             *          *              *                   *
 4.2 |                              ***        *              **                  ***
 4.1 |                                 ***      *               *                    *
 4.0 |                                    *      *               *
     +--------+--------+--------+--------+--------+--------+--------+--------+--------+---
     0       10M      20M      30M      40M      50M      60M      70M      80M      100M
                                        total keys
```

**Key observation:** The sawtooth pattern corresponds to tree depth transitions.
PsiTri's internal reorganization recovers throughput after each dip, keeping
performance within a ~20% band across the entire range.

### Dense Random Insert — 0 to 100M

Random access is inherently cache-unfriendly. Initial throughput is high while the
tree fits in cache, then degrades as working set grows beyond L2/L3.

```
ops/sec (M)
 2.9 |*
 2.5 |
 2.2 | *
 2.0 |
 1.8 |  *
 1.6 |   *
 1.4 |    *
 1.2 |     ****
 1.1 |         **********
 1.0 |                   **********
 0.9 |                             ***************
 0.8 |                                            ****************************
 0.7 |
 0.6 |                                                                        ****
     +--------+--------+--------+--------+--------+--------+--------+--------+--------+---
     0       10M      20M      30M      40M      50M      60M      70M      80M      100M
                                        total keys
```

**Key observation:** Throughput stabilizes around 850K-900K/sec from 40-80M keys,
then drops to ~640-800K/sec at 95-100M. This is cache pressure, not algorithmic degradation.

### String Random Insert — 0 to 100M

Variable-length string keys (decimal representation of 64-bit hash) are the most
expensive pattern: longer keys mean more trie depth and more prefix comparisons.

```
ops/sec (K)
1500 |*
1100 | *
1050 |  ***
 980 |     **
 920 |       *
 850 |        *
 810 |         ****     ****
 780 |             *****    *****
 760 |                           **********
 750 |                                     ****
 740 |                                         **********
 720 |                                                   *
 680 |                                                    ****
 660 |                                                        ****
 640 |                                                            ***
 620 |                                                               **
 600 |                                                                 **
     +--------+--------+--------+--------+--------+--------+--------+--------+--------+---
     0       10M      20M      30M      40M      50M      60M      70M      80M      100M
                                        total keys
```

### Sequential Upsert — Flat at 300M Keys

Upsert overwrites existing keys in-place. With 300M keys already in the tree,
throughput is essentially constant — the tree structure is stable and updates
touch the same leaf nodes repeatedly.

```
ops/sec (M)
 4.95|      *              *        *       *       *  *
 4.90|   *   *  *  *      * * *    * *      **  *     *  *       *
 4.85|  *     *         *     *      *                    *     * *
 4.80| *   *    ** * **      * **    **  *** * **  ** * ** ** **
 4.75|                                *          **
 4.70|*      *      *    *        *         *             *    *    *
 4.65|                                            *   *     *    *  **
 4.60|            *                                         *       * *
 4.55|                                                              *  *
 4.50|                                                                  **
     +--------+--------+--------+--------+--------+--------+--------+--------+--------+---
     0       10M      20M      30M      40M      50M      60M      70M      80M      100M
                                     upsert operations
```

**Key observation:** Upsert throughput is remarkably stable at ~4.7M/sec with no
degradation over 100M operations on a 300M-key tree. In-place updates do not grow
the tree, so there is no structural degradation.

### Sequential Remove — 200M down to 100M Keys

Tree shrinks with each round, so removals get faster over time.

```
ops/sec (M)
 5.5 |                                                                        ****************
 5.3 |                                                                   *****
 5.2 |                                                                  *
 5.0 |
 4.9 |       *****                   *****                   *****
 4.8 |      *     ********   *      *     ********   *      *     ********   *
 4.7 |     *              * *      *              * *      *              * *
 4.6 |****                 *  ****                 *  ****
 4.5 |                        *                       *
     +--------+--------+--------+--------+--------+--------+--------+--------+--------+---
    200M     190M     180M     170M     160M     150M     140M     130M     120M     100M
                                     remaining keys
```

**Key observation:** Sawtooth pattern mirrors insert — tree depth transitions cause
brief dips. Final rounds at 5.2-5.5M/sec as the tree thins out.

### Random Remove — 100M down to 0

Same Fisher-Yates shuffle pattern as insert, but in reverse. Starts slow (100M random
removes on a large tree) and accelerates dramatically as the tree shrinks.

```
ops/sec (M)
 2.4 |                                                                                     *
 1.3 |                                                                                   **
 1.2 |                                                                             *******
 1.1 |                                                                       ******
 1.0 |                                                              **********
 0.95|                                                       *******
 0.93|                                                  *****
 0.90|                                           *******
 0.88|                                    *******
 0.87|                              ******
 0.86|                         *****
 0.84|                  *******
 0.83|           *******
 0.83|    *******
 0.83|****
     +--------+--------+--------+--------+--------+--------+--------+--------+--------+---
    100M      90M      80M      70M      60M      50M      40M      30M      20M       0
                                     remaining keys
```

**Key observation:** The curve is the inverse of random insert — performance
improves steadily as cache pressure decreases. The final round (2.4M/sec) approaches
sequential remove speed as the remaining tree fits entirely in cache.

## Multi-Writer (4 Threads)

### Random Keys

Each writer uses salted keys to avoid conflicts. PsiTri's multi-root architecture
gives each writer its own independent tree root, so writers never block each other.

```
total: 400,000,000 inserts across 4 writers in 72.8 sec
  aggregate: 5,494,029 inserts/sec
  per-writer: 1,373,507 inserts/sec
```

Per-writer throughput (1.37M/sec) tracks the single-threaded random insert rate
at the corresponding dataset size, confirming zero contention between writers.

### Sequential Keys

```
  aggregate: ~15,900,000 inserts/sec
  per-writer: ~3,975,000 inserts/sec
```

Near-linear scaling: 4 writers achieve ~3.5x single-thread sequential insert rate.

## Tree Statistics

After 300M keys (100M seq + 100M rand + 100M str_rand):

```
inner_nodes:        5,355
inner_prefix_nodes: 1,048,749
leaf_nodes:         5,327,998
total_keys:         300,000,000
branches:           6,382,101
cache_lines:        1,827,596
max_depth:          12
avg_inner_size:     69 bytes
avg_clines/inner:   1.73
avg_branches/inner: 6.05
```

300 million keys stored in ~5.3M leaf nodes with a maximum tree depth of 12.
Average inner node holds 6 branches in 1.73 cache lines (69 bytes) — compact
enough that the upper levels of the tree stay resident in L2 cache.
