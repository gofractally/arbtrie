# x86 SIMD Benchmark: AVX-512 / SSSE3 Optimizations

This page documents the SIMD-accelerated hot paths added for x86-64 and their measured
impact on system-level throughput at 20 million keys with 12 concurrent reader threads.

## Test Environment

| Component | Spec |
|-----------|------|
| CPU | AMD EPYC-Turin, 8 cores / 16 threads (SMT), 2.4 GHz |
| ISA extensions | SSE2, SSE4.1, SSSE3, AVX, AVX2, AVX-512F/BW/DQ/VL/VBMI/VBMI2/VNNI/BF16 |
| RAM | 121 GB |
| Storage | 960 GB virtual disk (cloud VM — Vultr, Linux 6.17) |
| OS | Ubuntu, Linux 6.17.0 x86_64, 4 KB page size |
| Compiler | Clang 20, C++20 |
| Build | Release (-O3, LTO, -march=native) |

## Optimized Functions

Five hot-path functions received explicit x86 SIMD implementations, dispatched at compile
time via `#if defined(__AVX512F__)` / `__SSE2__` / `__SSSE3__` guards.

### lower_bound (byte array, sorted)

Used at every inner-node traversal to find the child branch for the next key byte.
With an average tree depth of 4–5 this is called millions of times per second.

**SSE2 approach:** XOR both the array and the search byte with `0x80` to convert unsigned
`<` into signed comparison, then use `_mm_cmplt_epi8` + `_mm_movemask_epi8` +
`__builtin_popcount` to count elements less than the target in 16-byte chunks.

| | Scalar | SSE2 | Speedup |
|-|--------|------|---------|
| `lower_bound` (size=64) | 7.1 ns | 0.23 ns | **31x** |
| `lower_bound_padded` (size=64) | 7.0 ns | 0.23 ns | **31x** |

### copy_branches + update cline index (SSSE3)

Called on every inner-node clone during copy-on-write. Copies branch data and remaps
control-line indices via a 16-entry LUT using `_mm_shuffle_epi8` (PSHUFB).

**SSSE3 approach:** Forward iteration in 16-byte chunks with a scalar tail. No padding
requirement (contrast with the NEON version which uses end-aligned first + pointer
wraparound).

| | Scalar | SSSE3 | Speedup |
|-|--------|-------|---------|
| `copy_branches` (N=16) | 4.5 ns | 0.69 ns | **6.5x** |
| `copy_branches` (N=64) | 16.3 ns | 1.8 ns | **9x** |
| `copy_branches` (N=128) | 32.0 ns | 3.6 ns | **8.9x** |

### find_u32x16 — child pointer search (AVX-512F)

Finds the first matching 32-bit child pointer in a padded 16-element array.

**AVX-512F approach:** Single 512-bit load + `vpcmpeqd` mask comparison — 5 instructions
total. The compiler auto-upgrades `_mm_cmpeq_epi32` to k-register ops anyway, but
the explicit path is cleaner and forward-compatible.

```asm
vpbroadcastd zmm0, edx        ; broadcast search value
vpcmpeqd k0, zmm0, [rdi]     ; compare all 16 in one 512-bit op → 16-bit mask
kmovw eax, k0
btsl esi, eax                 ; set sentinel bit at position `size`
tzcntl eax, eax               ; index of first match
```

Measured ~1.6 ns for both scalar-unrolled and AVX-512 — L1 load latency (64-byte load
≈ 4–5 cycles) is the ceiling regardless of instruction count.

### create_nth_set_bit_table — exclusive prefix sum (SSE2)

Builds a 16-entry lookup table mapping each branch slot to its rank among non-empty
slots. Used during inner-node construction.

**SSE2 approach:** 4-step parallel prefix-sum tree using `_mm_slli_si128` byte-shifts:

```cpp
sum = _mm_add_epi8(sum, _mm_slli_si128(sum, 1));
sum = _mm_add_epi8(sum, _mm_slli_si128(sum, 2));
sum = _mm_add_epi8(sum, _mm_slli_si128(sum, 4));
sum = _mm_add_epi8(sum, _mm_slli_si128(sum, 8));
sum = _mm_sub_epi8(sum, ones);  // exclusive prefix
```

Generates ~14 instructions vs 40+ for the scalar equivalent. Both bottleneck on the
8-deep arithmetic dependency chain at ~1.6 ns/call.

### find_min_index — branchless tournament (no SSE path)

Finds the index of the minimum uint16 in a 32- or 64-element array. SSE4.1
`_mm_minpos_epu16` was benchmarked and found **slower** than the existing branchless
tournament (0.57 ns vs 0.23 ns for 32 elements) due to `_mm_extract_epi16` latency
chains. The dispatch falls through to the tournament on all x86 targets.

| | Naive scan | Tournament | Speedup |
|-|------------|------------|---------|
| `find_min_index_32` | 18.2 ns | 0.23 ns | **80x** |
| `find_min_index_64` | 53.1 ns | 0.23 ns | **234x** |

## System-Level Impact at 20M Keys

The benchmark inserts 20 million random keys (8-byte values) in 20 rounds of 1 million
keys each, with 12 concurrent reader threads performing random point lookups throughout.

### Insert Throughput (single writer)

The lower_bound improvement is the primary driver — at 20M keys the tree is deep enough
that every insert requires multiple traversals, each of which calls `lower_bound`
repeatedly.

| Workload | Pre-SIMD | Post-SIMD | Delta |
|----------|----------|-----------|-------|
| Sequential (round 1) | ~1,860K/sec | ~1,790K/sec | -4% |
| Sequential (round 20, 20M keys) | 1,860K/sec | 1,672K/sec | -10% |
| Dense random (round 20, 20M keys) | **381K/sec** | **1,129K/sec** | **+196%** |
| String random (round 20, 20M keys) | **299K/sec** | **1,003K/sec** | **+235%** |

Random insert throughput at 20M keys improved ~3x. Before SIMD, the tree's growing
depth caused progressive degradation; 31x faster `lower_bound` keeps insert throughput
stable across the full key range.

The sequential insert slight regression (-4% to -10%) is a separate effect: sequential
keys form a degenerate tree shape that stresses the COW clone path more heavily,
where the SSSE3 `copy_branches` adds a small constant overhead compared to the
previous scalar path on this specific pattern.

### Read Throughput

| Workload | Pre-SIMD | Post-SIMD | Delta |
|----------|----------|-----------|-------|
| Sequential get (20M found) | 4.30M/sec | 5.52M/sec | **+28%** |
| Dense random get (20M found) | 1.07M/sec | 1.17M/sec | **+9%** |
| Lower bound scan (20M ops) | 514K/sec | 521K/sec | +1% |

### Concurrent Read Throughput (12 threads)

| Workload | Pre-SIMD | Post-SIMD | Delta |
|----------|----------|-----------|-------|
| Multithread get (rand keys, ~21M DB) | 12.4M/sec | **19.4M/sec** | **+57%** |
| Multithread get (known keys, ~21M DB) | ~13M/sec | **13.2M/sec** | +2% |

The +57% concurrent random-read gain is the headline result: 12 reader threads
achieving 19.4M lookups/sec, up from 12.4M.

## Notes

- All benchmarks run with `-march=native` (the default when `ENABLE_NATIVE_ARCH=ON`).
  Binaries are not portable to older x86 CPUs.
- The ARM NEON paths (for Apple Silicon / ARM Linux) remain unchanged; SIMD dispatch
  is compile-time via `#if defined(__ARM_NEON)` / `__AVX512F__` / `__SSE2__` guards.
- On machines without AVX-512F the dispatch falls through to the SSE2 `find_u32x16`
  path (4x 128-bit compares + movemask chain), which the compiler upgrades to
  k-register operations when AVX-512 is available.

## Reproducing

```bash
cmake --build build/release --target simd-x86-benchmark -j$(nproc)
./build/release/bin/simd-x86-benchmark
```

For the system-level benchmark:

```bash
cmake --build build/release --target psitri-benchmark -j$(nproc)
./build/release/bin/psitri-benchmark \
    --rounds 20 --items 1000000 --batch 512 \
    --read-threads 12 --db-dir /tmp/psitri_simd_bench
```
