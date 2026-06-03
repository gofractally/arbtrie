# psitri node design patterns (CANONICAL — apply to EVERY new node experiment)

These are the patterns refined in legacy psitri (`node/inner*.hpp`, art-based) and
`wtree` (wt1, `wtree/*.hpp`). They are the *default* structure for any SAL-backed
node type (wtree2, wtree3, big_index, …). Do not re-derive them per experiment;
start from here.

> **META-RULE — legacy beats wt1 when they disagree.** Legacy psitri (the art-based
> `node/` + `tree_ops`) is the carefully hand-tuned reference; wt1 (`wtree/`) is a
> later port that took some shortcuts (e.g. `get_ptr` reads, `inner_free_moved`).
> When the two differ, **check legacy and follow it — do NOT assume wt1 is right.**

## HARD RULES (read first — each one cost a debugging session)

- **NO heap allocations on hot paths.** Build/find/insert use SAL alloc + a
  thread_local scratch arena + in-place/remake only. No `std::vector`/`new`/`malloc`/
  `std::function` on the hot path.
- **`ptr_address(0)` is a VALID allocatable address; null is `kNoChild` = `0xFFFFFFFF`.**
  Never use `0` / a default-constructed `ptr_address` as null/empty/absent, and never
  leave a slot uninitialized (it reads as 0 → released as real id 0). Initialize every
  slot to `kNoChild`. (Cost: the `inner_route` + empty-leaf over-release of id 0.)
- **Release by a CAPTURED id, never `staleptr->address()`.** A raw node pointer held
  across an alloc can be relocated by the compactor → its bytes are reclaimed → reading
  `->address()` yields a stale/zeroed id (→ `release(0)`). Capture the id before any
  alloc; release that.
- **The compactor is PROVEN.** A crash / leak / over-release under it is YOUR
  retain/release/realloc/alloc/0-as-null usage, not the compactor. Suspect your code.
- **Benchmark ONLY `build/release` (`-O3 -DNDEBUG`).** If an `assert` fires in a
  "release" run, the build dir is mis-typed (it's Debug, `-O0`). See §G. (Cost: a 2×
  "regression" that was just a debug binary.)
- **A wrong-shape/width memory read must be a COMPILE error, not a runtime miss** —
  achieved by the typed CRTP forms (§B): the u16 form has no u32 `stems()`.

---

## A. Construction & mutation contract

1. **`alloc_size(args…)` mirrors `T(asize, seq, args…)`.** The static `alloc_size`
   computes the byte size for a set of construction args; the ctor takes the *same*
   args (plus the size + seq) and does ALL the work. The ctor asserts
   `assert(asize == alloc_size(args…))`. A node is never half-built.
   (`inner_node::alloc_size(src, op)` ↔ `inner_node(asize, seq, src, op)`.)

2. **Op work-ticket** (`op_insert`, `op_replace_slot`): a small struct with *input*
   fields (filled by the caller) and *plan* fields (filled by `alloc_size`, read
   back by the ctor/apply — compute-once). `alloc_size` returns **0 ⇒ "doesn't fit,
   caller must split"**. The plan caches the chosen `copy_mode` + result capacity.

3. **`apply(op)` mirrors the `(src, op)` ctor**, and BOTH delegate to ONE templated
   splice primitive `splice_X<splice_mode M>(dst, src, …)`:
   - `apply(op)` = in-place edit → `splice_X<in_place>(this, this, …)` (overlapping
     memmove on the same buffer).
   - `T(asize, seq, src, op)` = copy/grow → `splice_X<fresh_copy>(this, &src, …)`
     (non-overlapping copies into fresh bytes).
   The `splice_mode` is a *compile-time* template param → no runtime `src==dst` test
   on the hot path; the edit logic is written ONCE. (`splice_insert<M>`,
   `splice_replace<M>`.)

4. **`apply<mode>(ref, op, hint)` is THE node-mutation dispatcher** (wt1
   `apply.hpp`). Policy lives here, built only on SAL primitives + the (T, Op)
   contract:
   - `sz = T::alloc_size(*ref.obj(), op)`; `sz==0` → return null (caller splits).
   - **unique**: in-place via `ref.modify([&](T* n){ n->apply(op); })` when
     `!modify_will_copy() && op.copy==fast && sz<=size()`; else **relocate SAME id**
     via `session.realloc<T>(ref, sz, *ref.obj(), op)` (grow / COW — keeps the
     ptr_address, so the parent skips a repoint).
   - **shared**: `session.alloc<T>(sz, hint, *ref.obj(), op)` → fresh id (MVCC fork).
   Returns the result address; the caller re-derefs it (never reuses the threaded ref).

5. **`copy_mode {fast, rebuild}`**: fast = active 64-aligned head/tail copy (carries
   the free gap, in-place capable); rebuild = walk live structure dropping dead space
   (stale MVCC entries / freed slots) into a fresh buffer. Cached in the op by
   `alloc_size`.

---

## B. Type system (cleanliness · type safety · correct-by-construction)

6. **3-TIER CRTP: `inner_base` → `inner<Derived>` → `Derived`.** Three layers, on
   purpose — to kill template bloat AND keep type safety:
   - **`inner_base : psitri::node`** (NON-template) — all code that does NOT depend on
     the concrete type lives here and is **compiled ONCE** (no per-Derived
     instantiation → smaller binary, better I-cache).
   - **`inner<Derived> : inner_base`** (CRTP middle) — ONLY the genuinely
     type-dependent code, static-dispatched through `Derived` (zero overhead). Keep
     this layer thin — every function here is instantiated per type.
   - **`Derived : inner<Derived>`** — the concrete data + `static constexpr node_type
     type_id`. For the unified `wtree` engine the `Derived` set is the **routing-tier ×
     stem-width matrix** (`tiny / medium / big × u32 / u16`), NOT a leaf/inner/grid
     split — see §H. Each form exposes ONLY its valid stem accessor (`u16` has no u32
     `stems()`), so a wrong-width read is a compile error.
   **Distinct `Derived` types make "wrong shape" a COMPILE error** (a grid type has no
   `stems()` member → can't be misused). Evidence: libwtree's move from 2-tier to this
   3-tier CRTP gave **+9% perf and −988 B code size**. Default to the non-template
   `inner_base` for anything that doesn't need `Derived`; only push to `inner<Derived>`
   what truly varies by type.

7. **Concepts gate generic code** (`is_inner_node`, `any_inner_node_type`). Template
   algorithms constrain on the concept; the compiler enforces the surface.

8. **Per-type SAL `type_ops` for the cold lifecycle** (`cow_size`/`copy_to`/
   `compact_to`/`destroy`/`visit_children`), keyed by the node's own `type_id` →
   correct-by-construction (the node carries its type; SAL routes to the right ops, no
   manual discriminator). **The hot read/find path uses a `switch(type_id)`** (cheap,
   predictable), NOT the SAL vtable — so dynamic dispatch never touches find.

9. **Pointer-interconvertibility helpers** `as_header(T*)` / `as_T(node*)` (with a
   `type()` assert) for crossing the typed ↔ base boundary explicitly, plus typed
   result builders.

---

## C0. Ownership state-transitions (split / merge — correct-by-construction)

> **The indirection: `ptr_address` → control_block (a shared_ptr) → node memory.** The
> CB is the level of indirection. **`remake` = alloc new node memory via SAL, point the
> CB at it, `record_freed` the old memory.** The parent's `ptr_address` NEVER changes —
> so a remake needs **no parent repoint and no retain/release of the id** (the id and
> its children-ptrs ride along). You may reuse (redirect) a ptr_address only when
> **`ref==1` AND the path is unique**; if the CB is held (snapshot/shared) you must
> `alloc` a NEW id instead. A remake can **resize AND re-type** the node (the CB just
> points at whatever new memory) — leaf→inner, grid→explicit, grow, split-left, all the
> same primitive. **COW is exactly this** (redirect the CB to fresh writable memory on a
> read-only hit); and `modify` should **hook the pre-COW copy to transform-on-write** —
> build the new memory from `src + op` in ONE pass (the (src,op) ctor / `splice`),
> never copy-then-transform. ptr_address release is SEPARATE from memory free: remake
> frees *memory* and keeps the *id*; only a genuinely-discarded node releases its *id*.


**unique vs shared is a property of the DESCENT, not of the node.** The mode is
threaded down from the root (template param): start `unique`, and the instant a node
on the path has `ref>1` flip to `shared` and STAY shared for the whole subtree below.
A node's LOCAL `ref==1` does NOT mean unique — if any ancestor (e.g. the root held by
a snapshot, ref 2) is shared, every node beneath it is *implicitly shared* even though
99% of them have local ref 1. So decide by the threaded mode, never by re-checking the
current node's ref. (wtree2: `insert_rec<unique>` flips to `insert_rec<shared>` on
`nref.ref()>1`; once shared it never flips back.)

A double-release is NOT a race — it's `release(addr)` called too many times on one
ptr_address (the compactor only changes *when* the assert trips). Design the ref
moves so the counts are correct *by construction*, and **minimize retain/release**
(each touches a cold control-block cacheline). `remake` MOVES ownership → skip
retain/release entirely.

- **Unique-mode split**: **no retain, no release.** Ownership just moves. The **left
  half REUSES the old node's ptr_address** (remake in place → returned to the parent,
  which therefore skips a repoint); the right half gets a fresh id. The old node's
  children partition into left/right — already owned, nothing to retain or release.
- **Shared-mode split**: the old node is still held (a snapshot reaches it through the
  shared path), so its id **cannot** be repurposed → BOTH halves get **new** ids, and
  each must **retain** every child it keeps (the old node still references them too).
- **Unique merge / grow / re-prefix**: remake in place (reuse id), ownership moves,
  no retain/release.
- **Shared anything**: build fresh nodes; retain the children they keep; never touch
  the snapshot's old node.
- The legacy brute-force fallback (copy all to the clone, retain-what-you-keep,
  release-everything-from-src) is *correct* but wasteful (max cold-line touches) —
  prefer the move-by-construction transitions above.

## C. Lifecycle (refcount, COW, transfer)

> **Memory model (read this first).** SAL is NOT malloc/free. Under the read lock
> every node is **stable and read-only**; lifetime is governed by a **shared
> refcount** (held directly, or through the live path). `release` **defers** the
> teardown: it posts the node to the compactor's queue and returns — the recursive
> child-release runs on the **compactor, off the write critical path**. Only when
> that queue is full does the caller do the recursive release inline. So: keep
> release cost OFF the hot path (let the compactor drain it), reason about lifetime
> as refcount *accounting* (there is no use-after-free *race* — freed bytes aren't
> clobbered until the compactor reclaims), and treat **use-after-release as a code
> smell** to avoid even though it doesn't crash.


10. **`destroy` / `visit_children` / `retain_children`** release/visit/retain every
    EXT child → refcount-aware RECURSIVE teardown (a snapshot-shared child just
    decrements). A single release at a subtree root tears down the uniquely-owned
    subtree.

11. **`release_node(p)` = `session.release(p->address())` is the ONLY free primitive.**
    The refcount drops; when it hits 0 SAL's deferred `destroy` releases each EXT child
    → recursive, refcount-aware teardown (a snapshot-shared child just decrements). A
    single `release_node` at a subtree root tears down the uniquely-owned subtree.
    To **replace / grow / move-children** a node, **REMAKE IT IN PLACE via `realloc`**
    (keeps the ptr_address; the ctor builds the new bytes from src+op; the old bytes are
    reclaimed via `record_freed_space` WITHOUT running destroy, so children transfer with
    zero retain/release). A split remakes one half in place (reuse id) + `alloc`s the
    other (fresh id). The parent of a remade node skips the repoint (id unchanged).
    For a genuine **clone-transfer** (children copied to ANOTHER node, e.g. flattening
    a sub-inner into its parent — the source has no dangling slot), the proper move is
    `retain_children(src)` then `release_node(src)`: the retain hands src's owning ref
    to the clone, src's deferred destroy gives it back (net zero), src is freed. This
    is the legacy transfer (tree_ops:1386) and is fine — the refcount accounting
    balances regardless of when the deferred destroy drains.
    **ANTI-PATTERN — `free`/`free_moved`/neuter** (zero num_keys so destroy releases
    nothing): it writes the node to skip the destructor — fragile (faults on a
    read-only / sync-finalized node) and *unnecessary*. Use it for NOTHING. To replace
    or grow, remake in place (above). The wtree2 varlen-insert leak was a neuter
    breaking the accounting (trunc neutered+freed without releasing a child its clone
    sibling no longer covered) — NOT a memory race; the fix was to drop the neuter and
    remake/transfer properly. wt1 carried `inner_free_moved`; do NOT bring it forward.

12. **`cow_X(p)` = make writable via `get_ref().modify()`** — in-place when unique &
    writable, else `copy_on_write` to fresh writable bytes, KEEPING the ptr_address
    (CB moves). The mutating dispatch fetches the smart_ref ONCE and threads it BY
    REFERENCE down the recursion. Grow that changes size uses `realloc` (same id).

13. **`smart_ref` is non-owning** (no destructor release; retain/release explicit).
    Under the read lock `record_freed` bytes stay valid (the compactor reclaims them
    later), so a stale ref/ptr left after a relocate does NOT crash — but reading it is
    use-after-release, a code smell: prefer to use the returned/relocated id and not
    re-touch the old pointer.

---

## D. Session & deref

14. **Per-thread session bridge** (`g_session` / `session()`), installed at the top of
    each public op so the node free-functions/methods don't thread a session param.
15. **Reads use `get_ref` (a `smart_ref`) — like LEGACY, not wt1's `get_ptr`.**
    `get_ref` feeds the read-cache + prefetch, which **promotes/groups hot nodes into
    the pinned tier** (the memory grouping that cuts TLB/page misses on descent).
    `get_ptr` is a "lean borrow" whose read-cache promotion is *deferred/skipped* — it
    prefetches but loses the grouping; wt1 used it, legacy did not (`count_keys` /
    descent use `session.get_ref`). `smart_ref` is non-owning, so a read `get_ref`
    does NOT retain — you get the grouping at no refcount cost. Mutating sites use the
    same `get_ref` + `.modify()`.

---

## E. Layout discipline

16. **`static_assert(sizeof(...)==N)` + `offsetof` per field** — pin the header
    layout; any drift is a compile error.
17. **Region accessors computed from header fields** (cl-aligned offsets via
    `round_up_multiple<kCacheLine>`); fixed regions sized for *capacity* (slack for
    in-place growth); the compactor contracts a settled node to exact occupancy
    (`compact_size`/`compact_to`).
18. **Reserved sentinels distinct from real addresses** (`kNoChild`,
    `kSentinelChildAddr`) — SAL hands out addresses from 0 up; never use a
    default-constructed address as "null".

---

## F. Modes

19. **`upsert_mode`** — a rich flag struct (`unique`/`shared`, `insert`/`update`/
    `remove`, `sorted` hint) with `is_*`/`make_*` helpers, threaded at COMPILE TIME so
    the shared branches vanish from the unique fast path. (wtree2's minimal
    `umode {unique, shared}` is the same idea, smaller.)

---

## G. Build & measure discipline

20. **Two persistent build dirs; switch by dir, never reconfigure.** `build/release` =
    Release (`-O3 -DNDEBUG`, asserts OFF) — **all perf numbers come from here**.
    `build/debug` = Debug (`-O0 -g -UNDEBUG`, asserts ON) — correctness only. If an
    `assert` fires in a "release" run, the dir is mis-typed.
21. **Keep tests green every stage** — `--order decl` AND `--rng-seed N` (random order
    surfaces order/reuse-dependent bugs); run the correctness suite in `build/debug`
    (asserts on). Measure perf vs the committed baseline (`doc/baselines/`) at matched
    N, per-op (insert/query ns), not wall-clock under contention.

## H. Node taxonomy & routing (the `wtree` matrix)

22. **ONE discriminator = the SAL `node_type` tag.** No dual shape+sentinel. The tag is
    the **routing-tier × stem-width matrix**; `height` (separate u8) = leaf(0)/inner(>0).

    | tier (routing) | u32 | u16 = `(key−base)>>shift` |
    |---|---|---|
    | Tiny — branchless SIMD linear `count_le` | `tiny` | `tiny16` |
    | Medium — per-cacheline divs index (≥~128 stems) | `medium` | `medium16` |
    | Big — APGM predict + correction walk | `big` | `big16` |

    Leaves use Tiny/Medium; inners use all six. NO grid type (an exact power-of-2 inner
    is just a fast Big/u16 case). Leaf value layout (fixed-stride SoA vs varlen
    `value_region` + LOCAL/EXT slots) is an **auto-detected `fixed_values` flag**,
    orthogonal to the tag — keep the fixed-width auto-detection.

23. **u16 encoding**: stems stored as `(key − base) >> shift`, `shift = min ctz over
    (stem − base)` (largest power-of-2 dividing every delta → lossless). Route:
    `qd = (q − base) >> shift`, then branchless `count_le` over the u16 stems
    (8-lane NEON = 2× a u32 scan, half the stems cache footprint). The **split divider
    rule snaps to a high-ctz value** in a tight window around the median, so a parent's
    stems share a large `shift` → u16-eligible.

24. **Prefix = how keys are CONSUMED (not decoration).** The radix/Patricia descent
    consumes key words: each inner matches its `prefix` words then routes ONE word by
    stem → `depth += prefix_len + 1`; the leaf holds the remaining suffix. The routing
    key at each level is the WORD `key[depth]`, never the whole key. Mis-accounting
    depth/prefix loses or misroutes keys (a recurring bug class).

## wtree (the one, 2026-06) — target conformance / migration notes

- **A1 alloc_size↔ctor**: DONE — `node2::alloc_size(prefix,stems,children)` ↔
  `node2(…, prefix,stems,children)`; `alloc_size(src, op_inner_absorb)` ↔
  `node2(…, src, op)`.
- **A2/A3 op + apply-mirrors-ctor + shared splice + apply<mode> dispatcher**: in
  progress — give the op-ctor an `apply(op)` twin sharing a `splice_absorb<mode>`
  primitive, and an `apply<umode>(nref, op)` dispatcher (in-place / realloc-same-id /
  fork).
- **B6 CRTP + distinct types**: the clean `wtree` rewrite (branch `wtree-rewrite`) does
  this from scratch — `node_base` / `node_form<Derived>` / the **§H matrix** types
  (`tiny/medium/big × u32/u16`), single `node_type` tag + `height`, per-type SAL ops,
  `switch(type_id)` on the hot path. This kills the single-`node2` footguns (untyped
  `stems()` read on the wrong form; `children_off` depending on a sentinel) by
  construction. wtree2 (in `reference/`) is the superseded in-place attempt.
- B8/C/D/E/F: lifecycle (destroy/visit/retain), session bridge (`w2session`/`w2deref`),
  static_asserts, reserved sentinels, and `umode` are already followed.
