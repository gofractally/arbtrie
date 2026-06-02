# psitri node design patterns (CANONICAL — apply to EVERY new node experiment)

These are the patterns refined in legacy psitri (`node/inner*.hpp`, art-based) and
`wtree` (wt1, `wtree/*.hpp`). They are the *default* structure for any SAL-backed
node type (wtree2, wtree3, big_index, …). Do not re-derive them per experiment;
start from here.

> **META-RULE — legacy beats wt1 when they disagree.** Legacy psitri (the art-based
> `node/` + `tree_ops`) is the carefully hand-tuned reference; wt1 (`wtree/`) is a
> later port that took some shortcuts (e.g. `get_ptr` reads, `inner_free_moved`).
> When the two differ, **check legacy and follow it — do NOT assume wt1 is right.**

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

## C0. Ownership state-transitions (split / merge — correct-by-construction)

A double-release is NOT a race — it's `release(addr)` called too many times on one
ptr_address (the compactor only changes *when* the assert trips). Design the ref
moves so the counts are correct *by construction*, and **minimize retain/release**
(each touches a cold control-block cacheline). `remake` MOVES ownership → skip
retain/release entirely.

- **Unique split** (`ref==1`): **no retain, no release.** Ownership just moves. The
  **left half REUSES the old node's ptr_address** (remake in place → returned to the
  parent, which therefore skips a repoint); the right half gets a fresh id. The old
  node's children partition into left/right — they're already owned, nothing to
  retain or release.
- **Shared split** (`ref>1`): the old node is still held (snapshot), so its id
  **cannot** be repurposed → BOTH left and right get **new** ids, and each must
  **retain** every child it keeps (the old node still references them too).
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
