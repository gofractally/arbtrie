# Branchless Design

## Optimizing for the CPU Pipeline

PsiTri is built on two hardware-level principles. The first -- minimizing [cache misses](control-blocks.md#the-real-bottleneck-cache-misses-not-cpu-cycles) -- drives the control block and 1-byte branch encoding design. The second drives everything on this page: **eliminating branch mispredictions.**

A modern CPU pipeline is 15-20 stages deep. When the CPU encounters a conditional branch (`if/else`), it predicts which path to take and speculatively executes ahead. If the prediction is wrong, the entire pipeline must be flushed and restarted -- a penalty of **10-20 cycles per misprediction**.

For tree operations, branch misprediction is especially costly because the data-dependent comparisons are inherently unpredictable. When searching for a key in a sorted array, each comparison depends on the data -- the CPU has no pattern to learn. A binary search over 58 keys makes ~6 comparisons, each with roughly 50% probability of going either way. That's ~3 mispredictions per leaf lookup, costing 30-60 wasted cycles.

PsiTri is designed from the ground up to eliminate conditional branches on every hot path. The techniques fall into four categories:

1. **SIMD parallel comparison** -- replace serial if/else chains with vector operations
2. **Arithmetic branchless patterns** -- convert boolean results to integers used in arithmetic
3. **Bitmap iteration** -- replace conditional loops with hardware bit-scan instructions
4. **Compiler hints** -- guide prediction for the rare branches that remain

---

## SIMD Lower Bound: Branchless Inner Node Routing

The most critical operation in any tree is routing: given a search byte, find which branch to follow. Traditional B-trees do a binary search over dividers, incurring O(log N) comparisons with unpredictable branches.

PsiTri's inner nodes store sorted 1-byte dividers. The SIMD lower bound compares the search byte against **all dividers simultaneously** in a single instruction:

### ARM NEON Implementation

```
1. vdupq_n_u8(byte)     Broadcast search byte to all 16 lanes
2. vcltq_u8(data, byte)  Compare all 16 dividers at once (0xFF where less, 0x00 where not)
3. vandq_u8(result, 1)   Convert 0xFF → 0x01
4. vaddlvq_u8(masked)    Horizontal sum = count of dividers less than search byte
                          = the branch index
```

Four instructions. No conditional branches. The result is the index of the correct branch. For nodes with more than 16 dividers, the operation repeats in 16-byte chunks.

Compare this to a binary search over the same 16 dividers: 4 comparisons, each with an unpredictable branch, plus the loop control branches. The SIMD version does zero comparisons in the branch-prediction sense -- it's pure arithmetic.

### Scalar Fallback: Branchless Unrolled Sum

When SIMD isn't available, PsiTri uses a branchless scalar technique for small arrays:

```cpp
inline int lowerbound_unroll8(const uint8_t arr[8], uint8_t value) noexcept
{
   return (arr[0] < value) + (arr[1] < value) + (arr[2] < value) + (arr[3] < value) +
          (arr[4] < value) + (arr[5] < value) + (arr[6] < value) + (arr[7] < value);
}
```

Each comparison `arr[i] < value` produces 0 or 1. Summing them gives the lower bound index. The CPU can execute all 8 comparisons in parallel because there are no data dependencies between them. Total latency: ~4 cycles for 8 elements, with zero branch mispredictions.

---

## Branchless Binary Search in Leaves

Leaf nodes store sorted key suffixes. The lookup has two stages: a hash filter to skip non-matching keys, then a branchless binary search for the exact position.

### Stage 1: Hash-Accelerated Key Filtering

Each key in a leaf has a precomputed 1-byte XXH3 hash. Before doing any full key comparison, the lookup scans the hash array using a branchless byte-find algorithm:

```cpp
// Magic number branchless byte search
const uint64_t target = value * 0x0101010101010101ULL;  // Broadcast to all bytes
const uint64_t data = *(const uint64_t*)p;              // Load 8 hashes at once
const uint64_t xor_result = data ^ target;
uint64_t mask = (xor_result - 0x0101010101010101ULL) & ~xor_result;
mask &= 0x8080808080808080ULL;                          // Extract match positions
return __builtin_ctzll(mask) >> 3;                      // First match index
```

The formula `(x - 0x01010101...) & ~x & 0x80808080...` is a classic branchless technique that detects zero bytes in a word. By XORing with the target first, it detects matching bytes. No loops, no conditional branches -- just arithmetic on 8 bytes at once.

Only when a hash matches does the lookup perform a full key comparison. With a 1-byte hash (256 possible values) and ~58 keys per leaf, false positives are rare.

### Stage 2: Branchless Binary Search

The binary search itself uses a technique that eliminates the conditional branch at every step:

```cpp
branch_number lower_bound(key_view key) const noexcept
{
   int pos[2];
   pos[0] = -1;          // left
   pos[1] = num_branches(); // right

   while (pos[1] - pos[0] > 1)
   {
      int middle = (pos[0] + pos[1]) >> 1;
      bool geq = get_key(branch_number(middle)) >= key;
      pos[geq] = middle;  // geq is 0 or 1 -- used as array index
   }
   return branch_number(pos[1]);
}
```

The key line is `pos[geq] = middle`. Instead of:

```cpp
if (geq)
   right = middle;  // branch misprediction ~50% of the time
else
   left = middle;
```

It uses the boolean comparison result (0 or 1) as an **array index**. The compiler emits a conditional move (`CMOV` on x86, `CSEL` on ARM) instead of a conditional branch. Pipeline never stalls. For 58 keys, this saves ~3 mispredictions per lookup, or 30-60 cycles.

---

## SIMD Branch Remapping

When inner nodes are split, merged, or cloned, the 1-byte branch references must be remapped because the set of referenced cachelines changes. PsiTri does this with a NEON table lookup:

```
For each branch byte:
  1. vshrq_n_u8(data, 4)       Extract high nibble (old cacheline index)
  2. vqtbl1q_u8(lut, indices)  Parallel 16-way table lookup → new cacheline index
  3. vshlq_n_u8(result, 4)     Shift back to high nibble
  4. vandq_u8(data, 0x0F)      Extract low nibble (slot within cacheline)
  5. vorrq_u8(high, low)       Combine
```

`vqtbl1q_u8` performs 16 independent table lookups in a single instruction. The entire remapping of 16 branches takes 5 instructions with zero conditional branches. For nodes with more than 16 branches, the loop repeats in 16-byte chunks.

The lookup table itself is built with a branchless NEON parallel prefix sum:

```cpp
uint8x16_t mask = vtstq_u8(input, input);            // Non-zero → 0xFF
uint8x16_t ones = vandq_u8(mask, vdupq_n_u8(1));     // → 0x01
uint8x16_t sum = ones;
sum = vaddq_u8(sum, vextq_u8(zeros, sum, 15));        // Shift-and-add cascade
sum = vaddq_u8(sum, vextq_u8(zeros, sum, 14));        // (parallel prefix sum)
sum = vaddq_u8(sum, vextq_u8(zeros, sum, 12));
sum = vaddq_u8(sum, vextq_u8(zeros, sum, 8));
```

Four shift-and-add operations compute a running total across 16 elements. No loops, no branches.

---

## Bitmap Iteration with Hardware Bit-Scan

PsiTri uses bitmaps extensively -- in the zone allocator's free list, in cacheline reference tracking, in the hierarchical bitmap. Iterating over set bits in a bitmap is traditionally done with a loop that tests each bit. PsiTri uses hardware bit-scan instructions instead:

```cpp
do {
   uint32_t i = __builtin_ctz(bitmap);       // Find first set bit (1 instruction)
   destination[dest++] = source[i];
   bitmap &= (bitmap - 1);                   // Clear lowest set bit (Brian Kernighan's)
} while (--count);
```

`__builtin_ctz` (count trailing zeros) compiles to a single `CTZ`/`TZCNT` instruction. `bitmap &= (bitmap - 1)` clears the lowest set bit using arithmetic -- no conditional branch needed. The loop processes exactly as many iterations as there are set bits, with zero wasted work.

The hierarchical bitmap extends this to multi-level structures. Finding the first set bit across thousands of entries requires only 2-3 `CTZ` operations (one per level), each providing the index into the next level:

```
Level 2:  __builtin_ctzll(level2[0])           → which word in level 1
Level 1:  __builtin_ctzll(level1[l2_index])    → which word in level 0
Level 0:  __builtin_ctzll(level0[l1_index])    → which bit
```

Three instructions to find the first set bit among 262,144 entries.

---

## Branchless Patterns Used Throughout

### Boolean-as-Index (CMOV Pattern)

The most pervasive branchless technique in PsiTri. A boolean comparison result (0 or 1) is used directly as an array index or arithmetic operand:

```cpp
int cmp = (values[i + 1] < values[i]);   // 0 or 1
tournament[j] = tournament[i + cmp];      // Selects without branching
```

This pattern appears in the binary search, tournament-style minimum finding, and inner node initialization.

### Branchless Conditional Increment

```cpp
non_zero_count += (freq_table[i] != 0);  // 0 or 1, no branch
```

### Countdown Loops

```cpp
int count = n / 128;
while (count--) { ... }
```

Counting down toward zero gives the CPU a single, highly predictable termination condition. Counting up requires comparing against a variable bound, which can be harder to predict.

### NEON Find with Branchless Index Selection

Finding a value across 16 elements produces two 64-bit masks (low 8 and high 8 elements). The final index is selected without branching:

```cpp
uint64_t low = (__builtin_ctzll(low64 | (1ULL << 63)) + 1) / 8;
uint64_t high = (__builtin_ctzll(high64 | (1ULL << 63)) + 1) / 8;
return low + (high * (low >> 3));  // Branchless: if low < 8, use low; else 8 + high
```

The expression `low >> 3` is 0 when `low` is 0-7 and 1 when `low` is 8. Multiplying by it selects between the two halves without a conditional.

---

## Compiler Hints for Remaining Branches

Some branches can't be eliminated -- they represent genuinely different control flow paths (node type dispatch, error handling). For these, PsiTri uses `[[likely]]` and `[[unlikely]]` attributes to guide the compiler's code layout and the CPU's branch predictor:

```cpp
if (not _alloc_seg_ptr) [[unlikely]]        // First allocation is rare
   init_active_segment();

if (prior.ref == 0) [[unlikely]]            // Zero ref during CAS move is rare
   return false;

if (remaining_bytes > 0) [[likely]]         // 15/16 probability
   process_partial_chunk();
```

These hints tell the compiler to:

- Place the predicted path inline (no jump instruction on the fast path)
- Place the unlikely path out-of-line (cold code section)
- Help the CPU's static branch predictor on first encounter

---

## Why It Matters: The Combined Effect

Consider a single point lookup traversing 5 levels to find a key among 30M entries:

| Step | Traditional B-tree | PsiTri |
|------|-------------------|--------|
| **Inner node routing** | Binary search: ~4 comparisons x 4 levels, ~8 mispredictions | SIMD lower bound: 0 mispredictions |
| **Leaf key search** | Binary search: ~6 comparisons, ~3 mispredictions | Hash filter + branchless binary search: 0-1 mispredictions |
| **Pointer dereference** | N/A (page-based) | 1-byte branch decode: pure arithmetic, 0 mispredictions |
| **COW check** | Page latch: 1 branch | `ref == 1` check: highly predictable, ~0 mispredictions |
| **Total mispredictions** | **~11** | **~0-1** |
| **Wasted cycles** | **~110-220** | **~0-20** |

At 11 mispredictions per lookup and 10-20 cycles each, a traditional B-tree wastes 110-220 cycles per lookup just on pipeline flushes. PsiTri wastes almost none. Combined with the cache-miss reduction from 1-byte branch encoding and cacheline co-location, the two optimizations are multiplicative: fewer cache misses **and** fewer pipeline stalls on the data that is in cache.

This is why PsiTri matches or exceeds the throughput of in-memory-only data structures despite being a fully persistent, crash-safe, copy-on-write database. The bottleneck is neither CPU cycles nor disk I/O -- it's memory access patterns. PsiTri optimizes for the memory bus and the pipeline simultaneously.
