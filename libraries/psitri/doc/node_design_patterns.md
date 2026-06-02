# psitri node design patterns (CANONICAL — apply to EVERY new node experiment)

These are the patterns refined in legacy psitri (`node/inner*.hpp`, art-based) and
`wtree` (wt1, `wtree/*.hpp`). They are the *default* structure for any SAL-backed
node type (wtree2, wtree3, big_index, …). Do not re-derive them per experiment;
start from here. References below point at the wt1 implementation.

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

6. **CRTP base + distinct types per variant.** Common machinery lives in
   `node_base<Derived> : psitri::node`; each concrete variant is its own C++ type with
   `static constexpr node_type type_id`. Shared code is static-dispatched through
   `Derived` (zero overhead), variant-specific data/behavior lives on the variant.
   (`inner_node_base<Derived>`; `inner_node`, `inner_prefix_node`.) **Distinct types
   make "wrong shape" a COMPILE error, not a runtime branch** — e.g. a grid type has
   no `stems()` member, so it can't be misused.

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

## C. Lifecycle (refcount, COW, transfer)

10. **`destroy` / `visit_children` / `retain_children`** release/visit/retain every
    EXT child → refcount-aware RECURSIVE teardown (a snapshot-shared child just
    decrements). A single release at a subtree root tears down the uniquely-owned
    subtree.

11. **`free` vs `free_moved`.** `free` = release (deep, destroy releases children).
    `free_moved` = NEUTER first (num_keys=0, child0=null) so destroy releases NOTHING
    — the moved children keep their single owning ref in the destination. Use
    `free_moved` ONLY on a WRITABLE node whose children were transferred. **Do NOT**
    `retain_children + DEFERRED release` a node whose children are shared with a live
    clone (the deferred destroy races later mutations → orphan/leak).

12. **`cow_X(p)` = make writable via `get_ref().modify()`** — in-place when unique &
    writable, else `copy_on_write` to fresh writable bytes, KEEPING the ptr_address
    (CB moves). The mutating dispatch fetches the smart_ref ONCE and threads it BY
    REFERENCE down the recursion. Grow that changes size uses `realloc` (same id).

13. **`smart_ref` is non-owning** (no destructor release; retain/release explicit).
    Under the read lock, `record_freed` bytes stay valid and are never reused, so a
    stale ref left after a relocate is safe until the op unwinds.

---

## D. Session & deref

14. **Per-thread session bridge** (`g_session` / `session()`), installed at the top of
    each public op so the node free-functions/methods don't thread a session param.
15. **Reads use `wderef` = `get_ptr`** (prefetch + read-cache, NO smart_ref, NO
    session retain); only MUTATING sites build a `get_ref().modify()`.

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

## wtree2 conformance (2026-06) — migrating toward the above

- **A1 alloc_size↔ctor**: DONE — `node2::alloc_size(prefix,stems,children)` ↔
  `node2(…, prefix,stems,children)`; `alloc_size(src, op_inner_absorb)` ↔
  `node2(…, src, op)`.
- **A2/A3 op + apply-mirrors-ctor + shared splice + apply<mode> dispatcher**: in
  progress — give the op-ctor an `apply(op)` twin sharing a `splice_absorb<mode>`
  primitive, and an `apply<umode>(nref, op)` dispatcher (in-place / realloc-same-id /
  fork).
- **B6 CRTP + distinct types**: TODO (staged) — `node2_base<Derived>` + distinct
  shape types (leaf_fixed / leaf_varlen / leaf_u16 / inner / inner_grid), per-type SAL
  ops, `switch(type_id|shape)` only on the hot path. This kills the single-`node2`
  footguns (e.g. a grid's `stems()` aliasing children; the construction-order
  dependence of `children_off` on the estimator sentinel) by construction.
- B8/C/D/E/F: lifecycle (destroy/visit/retain), session bridge (`w2session`/`w2deref`),
  static_asserts, reserved sentinels, and `umode` are already followed.
