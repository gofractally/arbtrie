# wtree2 — SAL-adapted node structure (rewrite spec)

The general algorithm is `~/libwtree/algo.md` (ART + Patricia path-compression
over u32 words + btree-of-stems within a radix level). This doc is the **SAL
adaptation** — what changes because nodes are addressed by a 32-bit
`ptr_address` through a reference-counted `control_block` rather than by tagged
64-bit pointers, and because the engine is general (untemplated value size) with
COW/MVCC requirements.

Companion docs (still authoritative for their areas):
- `wtree_mvcc.md` — leaf/inner version tables, alt-chains, tombstones.
- `big_values.md` — out-of-line `big` values (≤2 MB immutable blob now).
- `wtree_shared_mode.md` — the COW / shared-mode descent infrastructure (DONE).

The goal of the rewrite: **a smaller, flatter node model than the current
implementation**, with the proven COW/MVCC/lifecycle machinery plugged in
unchanged. The current `wtree` test suite is the conformance spec.

---

## 0. The SAL cost model (why the structure is shaped this way)

A node jump on SAL is `ptr_address → control_block → loc → node`: **two serial
loads** (CB then node), where the in-memory algo's tagged pointer is one. The CB
load is a **per-level tax**. Three consequences drive every design choice:

1. **Depth is the lever, not per-level cost.** The CB tax is structure-agnostic
   (any COW structure pays it) and *per level*, so it compounds the depth gap:
   the deeper tree pays it more times. Measured apples-to-apples (both on SAL,
   both COW): arbtrie (ART/1-byte) 657/334 ns vs wtree 252/163 ns @10M. So:
   **minimize level count (wide fanout), don't chase a cheaper level**
   (depth-2→1 is capped by the CB indirection — low ceiling).

2. **Refcount lives in the CB on purpose.** It's the hottest-churning field
   (every snapshot retain, every shared-mode `retain_off_path`, every release).
   In the node it would dirty+fault a node page on every refcount op → write
   amplification on persistent storage. The dense CB array (16/cl) confines that
   churn off the node pages. This is *why* refcount-in-CB / type-in-header:
   refcount churns and needs 21 bits → CB; type doesn't churn and the CB has no
   spare bits → header.

3. **Metadata split:** anything needed to **prefetch** → the CB (a data-driven
   "prefetch K cachelines" field, set at alloc, read free on CB load). Anything
   needed to **route** → the node header (type, prefix, estimator, versions).
   The CB cline-count *subsumes* the algo's pointer type-tag (it's the actual
   must-read size of this instance — strictly better than a type-category proxy).

---

## 1. Pointer & prefetch model

- Node-to-node references are `sal::ptr_address` (u32), reference-counted via the
  control_block. Top-16 are reserved sentinels (`first_reserved_ptr_address`).
- A **branch is the same width as a stem** (both u32). Branches are rarer than
  in-memory (we don't want a CB per sparse pocket), which motivates the inline
  radix (§3).
- **Prefetch:** the parent, off its estimator's predicted `pos`, fires a
  two-stage prefetch: (a) resolve `slots[pos]` ptr_address → prefetch its CB;
  (b) when the CB lands (loc + cline-count), prefetch the node's K cls. Fire (a)
  as early as the prediction allows — it's only partially hideable. CB
  co-location (`alloc_hint`) and the read-cache keep hot CBs warm.

---

## 2. The unified node (SAL form)

Abstract record (from algo §3), SAL-specialized:

```
Node {
    type/flags                  # in header (not the pointer)
    prefix      : u32[prefix_len]    # Patricia path compression, inline
    terminal    : optional value     # key ending at prefix-end; in header, MVCC-versioned
    estimator   : (BigInner only) APGM piecewise-linear predictor, fixed offset
    stems       : sorted u32[N]       # btree dividers within one u32 word's range
    children    : ptr_address[N+1]    # EXT column (refcounted)
    local_offs  : u16[N+1]?           # OPTIONAL inline-slot column (see §3), iff N < 64
    versions    : (when present) per-node version table + history   # MVCC, see wtree_mvcc.md
}
```

Shapes (algo §3.1), SAL-trimmed:

| Shape      | Slots                         | Fanout      |
|------------|-------------------------------|-------------|
| **Leaf**   | DIRECT (inline ≤64 B) or EXT  | 1 — ~250    |
| **Inner**  | EXT (+ optional LOCAL if N<64)| 1 — ~6000   |

We collapse the algo's SmallInner/BigInner into one **Inner** that carries the
estimator only when N warrants it (Tiny/Medium/Big are search *modes* of one
layout, picked at build time per algo §6, not separate types).

---

## 3. Slots — the SAL simplification

The algo has DIRECT / SUBRN / T234 / EXT / LOCAL with a low-bit pointer tag.
SAL can't bit-tag a 32-bit ptr_address (scarce + reserved range), so:

- **Two parallel columns when inline is enabled** (`N < 64`):
  - `children[]` : u32 `ptr_address` (EXT) — refcounted external child.
  - `local_offs[]` : u16 offset into THIS node addressing an inline record;
    `0` (or a sentinel) = "not local, use the EXT column".
  Cost: **+2 bytes per child for the optionality**, paid only while `N < 64`.
- **Above 64 branches: no inline column** — all children are EXT.
- **Inline only `DIRECT` records ≤ 64 B, and only if the node has space**;
  otherwise externalize. **Drop `SUBRN` and `T234`** (no inline multi-entry
  sub-structures). This avoids radix-in-radix for the pathological "var-length
  keys differing by 2 at every word" case — those pockets externalize and pay a
  slower descent, keeping the structure flat.

**Lifecycle consequence (transfers from `wtree_shared_mode.md` §lifecycle):**
- A **LOCAL/DIRECT** record is **byte-copied when the node clones** — no
  refcount, no retain. The lifecycle walk (`retain_off_path_` / `visit_children`
  / `destroy`) covers **EXT children only** (the `children[]` ptr_address
  column). The `local_offs[]` column is *not* a column of ptr_addresses and is
  excluded by construction. So inlining is both address-cheap (no CB) and
  COW-cheap (no retain).

**Address-space angle:** the 32-bit ptr_address space is a hard ceiling (≤4 B
live nodes) and each node costs a CB. Inlining isn't only a cache win — it
**conserves the scarce address space + CB/refcount overhead**, so lean inline
more aggressively than the in-memory algo would.

---

## 4. Node layout & the CB metadata split

```
control_block (per node):  [ ref:20 | cacheline_offset:41 | active | pf_clines ]
                                                              └ data-driven prefetch hint
node header (line 0..):    [ alloc_hdr ][ type/flags, N, terminal-present ]
                           [ estimator (Inner, fixed offset, ~96 B → spans ~2 cls) ]
                           [ prefix_len, prefix words (inline) ]
                           [ versions (EMPTY when none → no push for common nodes) ]
                           [ stems[] ][ children[] ][ local_offs[]? ]
            tail (cold):   [ history heap (older-version payloads), long-prefix overflow ]
```

Prefetch-shaping rules (from the prefetch discussion):
- **Estimator fixed offset, always needed** → in the cold line-0/1 the CB hint
  prefetches.
- **Versions after the estimator, empty when absent.** Read *late* (only by
  historical-snapshot readers) but must be *fetched early* — the estimator
  read's spatial fetch-ahead naturally drags the adjacent version region in, and
  the CB hint can fire it at `get_ref`. The version region also **shields the
  stems**: it absorbs the estimator's fetch-ahead so HW doesn't over-fetch stems
  (we want exactly the one estimator-predicted stem cl).
- **Stems past the version buffer.** Inner stem access is *targeted* (estimator →
  predicted cl), NOT a scan → do **not** let the HW stream prefetcher engage on
  stems (it would over-fetch). Contrast the **leaf**, whose `count_lt` is a
  linear scan that *wants* streaming. Opposite prefetch regimes per shape.
- **History heap stays tail-ward** — reached only after the version table says
  there's relevant history (a genuine second-level dependent access).

---

## 5. Allocation (bump + lazy GC — aligned with the algo)

SAL alloc is a **bump pointer**; free is a **mark** reclaimed lazily by the
compactor. This makes the algo's "never restructure in place; always
bulk-construct a fresh node, free the old" (§9.1) the *cheap* path — construct-
fresh and SAL agree. Two allocation patterns matched to access shape:

- **Leaf: COW-expand → compact-contract.** A COW gives the leaf a full page of
  free heap so a bulk insert hitting the same leaf doesn't resize per key; the
  compactor contracts to packed later. Transient over-alloc for write
  throughput, exact-fit steady state, net-zero long-term. (Current
  `cow_size`→full / `compact_size`→packed already does this.)
- **Inner: exact-fit, resize on cl-crossing.** Stems grow 16/cl, children
  ~10/cl; realloc when a region crosses a cl = bump + memcpy + mark-old-dead.
  Exact-fit is *preferred* — over-alloc only inflates live bytes the compactor
  must relocate. No coarse size classes by default (only a measured response if
  compactor throughput ever saturates under extreme write rates).
- **Contiguity for free:** sequential bump-alloc clusters a freshly bulk-built
  subtree (and its CBs) in the segment → reinforces `alloc_hint` co-location for
  node bytes *and* CBs → better cold-traversal locality + cheaper compaction.

---

## 6. Descent — what gets simpler vs the current implementation

Replace the current node descent's special-case zoo with the algo's clean core:

**Deleted:**
- The **delta=1 sentinel system**: `kSentinelChildAddr`, create-child-on-demand-
  at-sentinel, `inner_enforce_invariant_at`, `g_routing_inconsistency`, the
  parent-pre-narrows-routing invariant. Out-of-range queries reach the child and
  it rejects (algo §7.3) — one extra prefix-compare on the rare out-of-range
  find, for an entire invariant class gone.
- **K=1 promote** (`leaf_promote_k1`) and the **container-wrap** path — subsumed
  by the plain Patricia prefix + Case 2 split.
- **`upsert_extra`** (heap-allocated prepend/tail multi-way carrier + `new`/
  `delete`) — Case 1 returns `{left, div, right}`; Case 2 returns a fresh
  intermediate via InPlace. Fixed-size value return (à la `branch_set`), no heap.

**Core (algo §5–§7):**
```
descend(node, key, depth):
    prefix-match (the ONLY step that consumes key words)
        → mismatch: Case 2 (Patricia split into fresh intermediate)
    stem-search (btree upper_bound; matching a stem does NOT consume a word)
    dispatch slot: empty | inline_value | key_val_node | child-node
    overflow: Case 1 (horizontal split) or leaf→inner convert (§8)
```

**Transfers unchanged** from `wtree_shared_mode.md` (the hard correctness layer):
`upsert_mode` NTTP + per-step `ref()>1` demotion; fused `apply<shared>`
(alloc-with-op, no eager-clone); `retain_off_path_`; `writable_inner_<mode>`;
`leaf_remove_<mode>` + remove-as-`mode.is_remove()`; `snapshot()`; the
lifecycle hooks. These sit *on top of* the descent and are node-structure-
agnostic.

---

## 7. MVCC & values

- **Values:** per-entry `value_region` (varlen, already built) for inline;
  `big` = one ptr_address to an immutable ≤2 MB blob (`big_values.md`); `tree` =
  one ptr_address to a subtree root. `big`/`tree` are the EXT-retained refs the
  lifecycle walks.
- **Versions:** per-node 15-slot version table + alt-chains + tombstones per
  `wtree_mvcc.md`, laid out per §4 (after the estimator, empty when none, history
  to the tail). `kTombstone` three-way routing folds remove into the descent
  (the current remove-as-mode is the pre-tombstone form).

---

## 8. Implementation phases (test-driven; suite is the conformance spec)

Build in a fresh module so the working `wtree` stays the baseline until swap.

1. **Node record + layout + sizing.** The struct, region offsets (header/prefix/
   stems/children/local_offs), cl-rounded alloc, accessors, static_asserts.
   Test: layout asserts + alloc/free + CB pf_clines round-trip.
2. **Bulk-in-order constructor** (§9.1) — the only materialization primitive.
   Prefix absorption (§9.2). Test: build from sorted streams, verify structure.
3. **find** (prefix-match → stem-search → dispatch), raw-pointer descent. Test:
   the existing find/varlen tests.
4. **insert** (Case 1 / Case 2 / canonicalize / leaf→inner convert §8). Test:
   the upsert/varlen/fuzz tests.
5. **erase** (the algo §11 compaction) — but **as `mode.is_remove()` of the
   shared descent**, reusing the transferred infra. Test: remove/collapse tests.
6. **COW/MVCC integration** — wire `upsert_mode`/`apply<shared>`/
   `retain_off_path_`/lifecycle/`snapshot`. Test: snapshot-isolation + leak.
7. **Inline radix (LOCAL column)** + the prefetch-shaping layout + CB pf_clines
   driving prefetch. Test: perf (bench_wtree_sal ≥ baseline) + leak.
8. **Swap** the new module in; retire the old node code.

Each phase: build + full `wtree-tests` green + (phase 7+) `bench_wtree_sal` no
regression vs the 254/160 @10M baseline.
