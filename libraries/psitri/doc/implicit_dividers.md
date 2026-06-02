# Implicit Dividers for wtree2 Inner Nodes

Status: **design / spec** (2026-06-02). No code yet. This is the agreed plan before
we touch `wtree2/insert.hpp` / `node.hpp`.

## 1. The win

A wtree2 inner today stores, for fanout `F`:

```
stems[F-1]  +  children[F]  +  APGM estimator(96B)
```

and a lookup runs the estimator → predicted stem → short correction walk → child.
The data-dependent loads (the predicted stem cacheline, the estimator) are exactly
the cold misses we **cannot prefetch** during descent (see
`project_wtree2_readpath_perf`), and they are most of the remaining find gap.

For **uniform** keys an inner does not need stored dividers at all. The child index
is pure bit math:

```
child = children[(key - base) >> shift]
```

| | btree inner (F=4096) | implicit inner (F=4096) |
|---|---|---|
| storage | stems + children + estimator (~32 KB) | **children only (~16 KB)** |
| route | predict → walk → child | `(key-base)>>shift` → child |
| cold cls touched | estimator + predicted-stem + children (~3) | **1 (children)** |

An implicit inner is the *exact* predictor for uniform data: the APGM estimator
approximates key→position with a piecewise line + correction; for uniform keys that
line is the identity on the high bits, so you keep neither the line nor the stems.

The leaf is unchanged — it keeps **exact stems** (it is the only validator). See §4.

## 2. Existence predicate and the n² trap

> A power-of-2 (binary) representation of an inner exists for spacing `2^s` iff, for
> every adjacent branch pair, a multiple of `2^s` falls strictly between the rightmost
> key of the left branch and the leftmost key of the right branch.

For a boundary pair `a < b`, the **coarsest** separating power of 2 sits at
`s = msb(a ^ b)` — the highest differing bit (literally the radix / Patricia split
bit). Over a node, a single grid works iff `2^s` separates every pair, so the binding
constraint is the **radix-closest adjacent pair**:

```
s_node = min over adjacent pairs of  msb(boundary_i ^ boundary_{i-1})
fanout = 2^(span_bits - s_node)
```

**The trap:** do *not* apply this to individual leaf boundaries. `n` uniform points
have expected minimum gap `≈ R/n²`, so the binding pair is an accidental near-collision
and exact separation of `n` random leaves needs `≈ n²` cells (≈2.5 billion for 50k
leaves). The predicate is satisfiable but demands an absurd grid, because it is asking
implicit dividers to resolve the one place uniform data is *not* smooth: the fine grain.

## 3. Resolution: grid-first, not branches-first

Do not pick branches (count-median dividers) and then fit a grid — that is the framing
that hits n². Instead **choose the grid; let the cells be the branches**:

- Pick spacing `2^s` at a node. A cell holding `≤ Lmax` keys → a leaf; a cell holding
  more → recurse with a finer sub-grid.
- Cells are grid-aligned by construction ⇒ the predicate holds trivially ⇒ no cell ever
  straddles two children ⇒ routing is exact.
- Accidental near-collisions simply land in the **same leaf** and are resolved by its
  exact stems. No redundant child pointers, no min-gap blowup — cells are sized by
  **count**, not by separating individual keys.

Uniform 10M, worked: top `s=24` → 256 cells × ~39k keys → recurse; mid `s=16` → 256
cells × ~150 keys → leaves spanning 2¹⁶; the leaf's stems resolve within-cell
collisions. Depth 3, no redundancy.

The free knob is **balance**: the coarsest spacing whose cells stay `≤ Lmax`:

```
s = span_bits - ceil(log2(N / Lmax))     // uniform
```

with capped fanout + recursion for skew. The exact `msb(a^b)` predicate and the
statistical tolerance test (`get_split_tolerance`) are two faces of one coin: exact
when branches are given and few; statistical when sizing a grid over many keys.

## 4. Why leaves keep exact stems

Leaves are the **only validators**; inners are **pure routers** (classic ART semantics):

- A leaf confirms the key by exact compare and returns the value, or returns *miss*.
- An inner never rejects: a non-existent key still routes to *some* leaf, whose exact
  compare returns miss. Correct, provided each child's subrange equals the cell that
  points to it (guaranteed by grid-first construction).

Leaves store full remaining words as stems; the high bits shared across a leaf are
redundantly present but free in-cacheline and keep exact-match SIMD trivial (no
sub-byte alignment).

## 5. Order robustness

The implicit structure is a **pure function of the settled key set, not insertion
order** — bulk-sort, random insert, and sorted insert of the same set all yield the
same correct grid once all keys are present. Two things make a *naive* implicit split
fail, and both have fixes:

1. **Unknown range** (dense `0..N` collapses into bucket 0 of a full-width grid) →
   **affine dividers**: store `(base, shift)` per node, route `(key-base)>>shift`. Two
   numbers, not F stems. Sized to observed `[min,max]`.
2. **Unknown distribution-so-far** (sorted insert: at the first overflow the inserted
   set is a low prefix, value-midpoint ≠ count-median) → **explicit on the write path,
   implicit on the compacted path**:

| insertion | during build | after compaction |
|---|---|---|
| hashed / random uniform | implicit immediately (every node stays uniform) | implicit |
| sorted / sequential IDs | explicit stems (count-median, robust) | implicit (set uniform in aggregate) |
| clustered / skewed | explicit stems | stays explicit (fails tolerance — correctly) |

Nothing new architecturally: we already have dual-mode leaves (fixed/varlen) and a
compactor that rewrites settled nodes. This is the same pattern one level up.

## 6. Phase 1 — the snap kernel

Choose a power-of-2-**aligned** divider close to the median. Between adjacent keys
`a < b` it is exact and O(1):

```
p = 31 - clz(a ^ b)     // 32-bit; highest differing bit = radix split bit
d = (b >> p) << p        // unique 2^p-aligned value in (a, b]; a < d <= b guaranteed
```

`d` is always a valid separator (left `< d`, right `>= d`, matching the `q==d`-routes-
right convention) and provably the most-aligned value in the gap (a,b agree above bit
`p`). "Close to the median" is a window scan: over candidate gaps
`j ∈ [N/2 − tol, N/2 + tol]`, pick the `j` with the **largest `p_j`**, tie-broken
toward the median. `tol = get_split_tolerance(N)` — how far off the exact median we
reach to land on a coarser grid line. `O(tol)`, branchless.

```
inline int get_split_tolerance(int N) {            // ~3σ of a Binomial(N,½) split
    if (N <= 0) return 0;
    uint32_t gt32  = (uint32_t)(N - 33) >> 31;
    uint32_t gt128 = (uint32_t)(N - 129) >> 31;
    uint32_t gt512 = (uint32_t)(N - 513) >> 31;
    int base = 6 + (N >> 3);
    base += gt32  * ((8  + (N >> 4)) - base);
    base += gt128 * ((12 + (N >> 5)) - base);
    int msb  = 31 - __builtin_clz(N);
    int huge = (3 * (1 << (msb >> 1))) >> 1;        // 1.5·√N
    base += gt512 * (huge - base);
    return base;
}
```

## 6a. Point/slope → point/shift (per-cacheline indexing)

Once dividers are snapped (§6) the APGM predictor can drop from point/**slope** to
point/**shift**: the slope becomes the grid exponent, and prediction is a single shift
instead of a multiply + segment lookup. This is the read-path payoff of Step 1 and it
needs **no new node shape** — stems stay, so correctness never depends on uniformity.

Today an inner lookup touches **3 cold cachelines**: the estimator, the stems line (the
correction scan), and the children line. The key insight: an **exact** prediction has
nothing to confirm — at an inner the stems are only routing dividers (the leaf is the
validator), so an exact index goes **straight to `children[i]`** and the **stems line is
never read at all.** The only reason to touch stems is the correction scan, which an
exact grid does not have.

```
i = (key - base) >> shift;   // base, shift from the fixed header (line 0, already hot)
prefetch children[i];        // the ONLY cold line
descend(children[i]);        // stems never touched, estimator never read
```

So the cold-cl count by mode:

| mode | route | cold cls |
|---|---|---|
| linear (today) | estimator → **stems (scan)** → children | 3 |
| binary, single-segment (`base,shift` in fixed header) | header (hot) → ~~stems~~ → children | **1 (children)** |

**Keeping the stems in storage costs nothing on the read path as long as we never read
them** — they stay resident for rebuild and for the linear fallback, but a binary node's
route skips them entirely. Full implicit-routing speed, no node-shape change, no dropping
data, no flip risk; the binary flag just gates whether the stems line is on the path.

**Graceful degradation.** When a node is not yet aligned the prediction is approximate
(linear): `cl = (key-base) >> (shift + log2(W))` selects the stem cacheline, prefetch it,
and the existing branchless `W`-wide scan finds the stem (child = `cl·W + local`). Exact
vs approximate is the only difference; correctness never depends on it, because the stems
are still present.

This is **Step 1.5**: full read win for uniform nodes (1 cold cl), graceful linear
fallback otherwise, no implicit-routing shape, no flip risk.

## 6b. Per-segment binary/linear flag

§6a assumes the whole node is one clean grid. Real nodes — especially mid-settle under
incremental insert — can be uniform in most of their range and skewed in one corner. So
carry the linear/binary distinction **per APGM segment** rather than per node.

Steal one bit from `recips[i]` (the slope, `slope × 2³²`): its LSB is noise (ULP
2⁻³² → 2⁻³¹). A binary segment needs no slope, only a ~6-bit shift, so the whole field
is free once the flag is set:

```
binary segment:  recips[i] = (shift << 1) | 1     // flag=1, rest = shift exponent
linear segment:  recips[i] = recip,  (recips[i] & 1) == 0
```

(A power-of-2 slope already makes `recip` a power of two, so it's value-detectable — but
the flag carries semantics the value can't: "this range is an **exact** grid, skip the
ε-correction *and the stems read*.")

Segment selection (the 9-key NEON `count_le`) is unchanged; only the in-segment step
branches on the flag:

- **linear** → `intercept + (q − key)·recip/2³²`, ±ε, then the 1-CL stem scan.
- **binary** → `i = intercept + ((q − key) >> shift)`, **exact** → straight to
  `children[i]`, stems skipped.

A node uniform in most segments and skewed in one gets exact stem-skipping routing where
it's uniform and graceful linear fallback for the one irregular segment — no all-or-
nothing decision. As regions settle into a clean grid the build flips their segments
linear→binary one at a time. **This is Step 2 expressed inside the existing predictor** —
no new node shape, no whole-node flip.

Cold-cl note: a per-segment-binary route still reads the estimator (to get the segment's
shift) → 2 cold cls (estimator + children), stems skipped. The estimator-cl *elimination*
(§6a, 1 cold cl) happens only when the **whole node collapses to a single binary
segment** — then promote `(base, shift)` into the fixed header and skip the estimator
region. So: header-binary for fully-uniform (common, hashed keys), per-segment-binary for
the mixed middle.

Build cost is unchanged: the greedy fit tests each sample run for a constant power-of-2
stride (§7) and emits binary (cheaper — no regression) or linear.

## 7. The regularity-of-shift signal

Each leaf snaps to *its own* best `p`. For **settled uniform** data every leaf has the
same key-span → they all snap to the **same shift** → the inner's dividers form a
regular grid. For **transitional / lopsided sorted-insert** data the spans differ → the
`p`s differ → dividers are aligned-but-irregular → not yet recognizable → the node
correctly stays explicit. So the scheme is **self-recognizing and order-independent**:
a node flips to implicit exactly when its children converge to a common shift. Under
sorted insert, lower-left subtrees settle and flip first; recognition propagates upward;
the root stays roomy/explicit until it has the sample to know its grid.

## 8. Phase 2 — the detect / compress kernel

Given the aligned dividers `d_1 < … < d_{F-1}`, mask out the common bits and read off
the grid:

```
span   = d_1 ^ d_{F-1}                       // sorted ⇒ spans all varying bits
base   = d_1 & ~mask(msb(span))              // common high prefix
shift  = min over i of ctz(d_{i+1} - d_i)    // coarsest common grid spacing
cell_i = (d_i - base) >> shift               // residual = grid-cell indices
```

- `cell_i` contiguous → **dense uniform grid** → drop stems, store `(base, shift)`,
  route `(key − base) >> shift`.
- `cell_i` with gaps → **sparse grid with holes** → still implicit; missing cells are
  `kNoChild`. Flip only if `(max_cell + 1)` fits the fanout budget.
- residual irregular / too many holes → stay explicit. Correct.

## 9. Balance vs alignment (the one soft policy)

Snapping off the exact median trades balance for alignment; uncontrolled it compounds.
Guards: (1) the window `tol` caps per-split imbalance; (2) among gaps tied for max `p`,
pick the one nearest the median; (3) cap `p` so one side is never starved. For uniform
data the natural `p` already lands near the median, so the cap rarely binds; for skewed
data it keeps the explicit tree healthy until the compactor decides it will never be
implicit anyway.

## 10. Node descriptor

Replace "prefix_len words" with a **bit cursor + affine grid**:

- explicit inner: unchanged (stems + children + estimator).
- implicit inner: `(word_index, base, shift, k)` + `children[2^k]`, no stems, no
  estimator. Route `((word[word_index] - base) >> shift) & ((1<<k)-1)`. Child cursor =
  parent cursor advanced by `k` (rolling to the next word at the word boundary).

## 11. Phased rollout

**Step 1 — snap only.** Change *only* the divider chosen at split time to the Phase-1
snapped value (§6). Keep everything else exactly as today: dividers stay stored
explicitly, search still uses the APGM estimator + correction walk. We are **not**
routing implicitly yet. Expectation: aligned dividers make the key→position mapping
regular, so the estimator gets more accurate — its correction walk shrinks and the
model reduces to a power-of-2 approximation. Reversible; no new node shape.

Measure: average correction-walk length per inner, and find ns, before vs after the
snap. If the walk collapses toward 0, the regularity is real → Step 1.5.

**Step 1.5 — point/shift predictor (§6a).** Replace the point/slope APGM with
point/shift `(base, shift)` in the header: prediction is a shift, the estimator
cacheline is eliminated, and the predicted-stem cacheline is selected exactly and
prefetched at node entry. Stems stay → order-robust, no flip risk. This captures most
of the find win (3 cold cls → ~1).

**Step 2 — detect + flip.** Add the implicit inner shape (§10) and the detect kernel
(§8). At build/compact, flip inners whose dividers form a clean grid; drop their stems
and estimator; route by bit math. Leaves and the explicit path are untouched.

**Step 3 — compactor / inline flips.** Wire the flip into the compactor (which already
rewrites settled nodes) so sorted-inserted, now-uniform subtrees earn the implicit fast
path; optionally fire inline during insert when a node's children already share a shift.

## 12. Where it plugs into wtree2

- split (`insert.hpp`, `leaf_*_add` / `case2_split` / balanced split): Step 1 snap kernel.
- `bulk_build`: grid-first construction (§3) once Step 2 lands.
- `find_value` (`node.hpp`): implicit-inner route branch (Step 2).
- compactor: explicit→implicit flip (Step 3).
- detect/compress + tolerance: shared helper used by both build and compactor.

## 13. Minimal experiment (read-side proof)

Goal: prove that exact grids let an inner skip the stems line, and measure the find win,
with the smallest reversible diff. **Verification is a one-time creation-time assertion,
never a query-time check** — the binary flag *means* "creation proved this is an exact
grid," so a flagged query routes by shift with zero checking.

Three steps:

1. **`bulk_build`** — partition children **by grid cell** `(key − base) >> S` (not by
   count + relabel, or the boundary and grid point drift and the proof fails). For
   uniform data grid-cells ≈ equal-count, so it's nearly a drop-in. This makes the
   stems an exact grid.
2. **`build_inner`** — one pass that *detects and verifies together*, over **all** stems:
   ```
   shift = ctz(stems[1] - stems[0]);                          // candidate stride
   ok    = all i: (stems[i+1] - stems[i]) == (1u << shift);   // constant-stride proof
   if (ok) { est->segment_count = kBinarySentinel;            // reuse estimator region
             est->segment_keys[0] = base; est->sample_step = shift; }
   else    { /* normal linear apgm_build */ }
   ```
   Must check *all* stems (one off-grid stem routes some key wrong). O(n) once, on wide
   inners only, vectorizable as a delta-equality reduction.
3. **`find_value`** — sentinel set → `i = (q − base) >> shift; child = children[i]`.
   Stems never read, nothing verified.

Readout (build time, not query time): wide-inner **grid candidates vs passed** the
constant-stride proof (the binary fraction), plus find ns. No query-time mismatch counter
— a flagged node cannot mismatch by construction. Low passed-fraction → the snap/partition
isn't producing exact grids; high fraction → the find delta vs the 109/144 baseline is the
result. No header layout change, no new node shape, fully reversible.

## 14. Experimental results (2026-06-02, 10M random u32 / 8-B values)

Implemented as the `grid` bench mode (standalone byte-radix grid builder; production
`bulk_build` untouched) + `detect_grid` + sentinel shift-routing. 261/261 inners detect
as exact grids, miss=0, build 5 ns/key.

| route | find/hot (sorted, compute-bound) | find/rnd (random, latency-bound) |
|---|---|---|
| **grid — shift** | **12 ns** | 116 ns |
| bulk — apgm (9-SIMD + walk) | 20 ns | 122 ns |
| grid — 255-wide linear scan | — | 198 ns |

Findings:
- **The win is a compute win.** Hot (sorted-order, misses hidden by the HW prefetcher):
  grid 12 ns vs apgm 20 ns — **40% faster**, and the grid wins despite being depth-4 (3
  inner hops) vs bulk depth-3 (2 hops); the per-hop saving (one shift vs 9-SIMD select +
  correction walk + multiply) more than pays for the extra hop.
- **In the random/latency-bound regime the gap collapses** (116 vs 122, ~5%): both are
  bottlenecked on the same cold cachelines (line 0 = header+estimator, + children), not
  CPU. On Apple Silicon's 128-B lines the estimator lives in line 0 (loaded with the
  header) and pf2 prefetches the stems line, so apgm's stems read was already mostly
  hidden — skipping it saves compute, not a miss.
- **On 64-B-line hardware the grid should also win the random regime**: there the apgm
  `recips[]`/`intercepts[]` start at byte 64 = a *second* estimator cacheline that the
  grid's `(segment_keys[0], sample_step)` (both in line 0) never reads.
- shift-routing beats the 255-wide linear scan by ~2× (106–113 vs 198) — the headline
  improvement over a non-predicted node.

**Coverage — the snap is required (uniform density ≠ grid dividers):**

| construction | grid inners detected |
|---|---|
| grid builder (snaps dividers to cell boundaries) | 261/261 = **100%** |
| production `bulk_build` u32 (raw first-key dividers) | 0/14 = **0%** |
| production `bulk_build` 24-byte | 0/31 = **0%** |

Production `bulk_build` uses each child's first key as the divider — random values,
never exactly constant-stride — so `detect_grid` (which demands an exact power-of-2
stride) fires on **0%** of natural inners *despite perfectly uniform data*. The benefit
is not free from uniformity: the construction must **snap** each divider onto its grid
boundary (§6; the divider may differ from any real key as long as it separates
left-max < d ≤ right-min). With snapping → ~100%; without → 0%. So the open work to take
this to production is the snap in `bulk_build` + the incremental split, not anything in
the detect/route path (those are done and proven).
