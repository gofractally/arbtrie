---
id: transaction-refactor
title: Two-mode transaction API (expect_success / expect_failure) with lazy versioning
status: in-progress
priority: high
area: psitri
agent: ~
branch: mvcc
created: 2026-04-25
depends_on: []
blocks: []
---

## Status (2026-04-26)

**All phases of the original plan landed on the `mvcc` branch:**

First wave:
- Phase 0 — `with_subtree` removed (b432258)
- Phase 1 — stabilization (9beb37b): get_stats fix, dead nodes.hpp deleted, hardcoded test paths replaced with unique temp dirs, tree_context Dense Random tagged `[!benchmark]`
- Phase 2 — OCC dropped entirely (5370aab)
- Phase 3 — `tx_mode::batch` → `expect_success`, `tx_mode::micro` → `expect_failure` (e7ac82c)
- Phase 4a — `value_node` coalesce ctor with `replace_last_tag` (ec2ae62)
- Phase 4b (initial) — coalesce branch wired into `try_mvcc_upsert` / `try_mvcc_remove` (e98fea1).

Second wave (transaction-refactor-finish):
- Phase A — per-txn version threading via `smart_ptr`'s `_ver` (3aa21c3). `make_unique_root` as the explicit state-transition primitive; `give(addr)` preserves `_ver`; tree_context's COW update path uses chain extension on existing value_nodes.
- Phase B — lazy version allocation for `expect_failure` + cursor snapshot version (2f60d13). `ensure_txn_version` plugged into the buffer-flush funnel; cursors inherit the snapshot's version from `smart_ptr.ver()`.
- Phase C — `_has_txn_version` flag distinguishes "txn owns its ver" from "inherited slot ver"; expect_failure aborts that allocated a ver release it cleanly (bbe9aa5).
- Phase D — in-place coalesce: `value_node::try_coalesce_in_place` memcpy fast path for same/smaller updates with matching version (5881120). 1000 hot-key updates allocate ≤ 5 new objects.
- Phase E — type-erased `held_lock` removes the `std::mutex*` hardcode (aa40f88). Multi-root locking now matches whatever LockPolicy the bound write_session uses.
- Phase F — tree_context SIGBUS diagnosed as SAL segment-remap stale-pointer bug (cedfcbe), out of scope for transaction-refactor; documented in BUGS.md with lldb evidence; deferred as a SAL ticket.

**Verification:**
- All `~[!benchmark]` tests pass (587+ cases, 975M assertions).
- Hot-key perf assertion: 1000 same-size updates to one key in one expect_success txn produce ≤ 5 new allocations and ≤ 2 chain entries.
- Lazy-alloc assertion: 1000 expect_failure abort cycles with no writes produce zero global_version delta.
- Abort-release assertion: aborted versions appear in the dead_versions snapshot after the compactor runs; aborted txn pages are reclaimed.

**Out of scope (tracked separately):**
- DWAL `expect_failure` mode (Phase 8 of original plan). DWAL stays eager-WAL-append.
- The tree_context SIGBUS itself — needs a SAL segment-remap-protocol fix.

## Summary

Replace the current `tx_mode::{batch, micro, occ}` enum with a two-mode design
based on what the caller knows about commit likelihood. Apply consistently
across both engines (COW and DWAL). Bake in per-txn versioning + value-node
coalescing + lazy version allocation as natural consequences of the new
contract.

## Background

The current API exposes three modes:

- `batch` — direct COW; each `upsert` mutates the persistent tree (or a
  uniquely-owned branch of it) immediately.
- `micro` — RAM delta buffer (`detail::write_buffer`); writes accumulate in
  memory and are replayed against the tree at commit.
- `occ` — like `micro` plus read-set tracking; commit re-validates each read
  before applying.

Two findings drive this refactor:

1. **OCC is a bust.** Validation overhead exceeds the cost of just taking the
   root lock for in-memory trie writes. The protected work is microseconds;
   read-set bookkeeping is also microseconds; the lock would have been held
   for microseconds. There is nothing to amortize. OCC pays off only when
   the protected work is large relative to validation (long analytical reads,
   network round-trips). For point-update workloads it loses to single-writer
   sequential. Drop it from the public API.

2. **The current names leak implementation.** `batch`/`micro` describe what
   the engine does internally, not what the caller expects. The caller's
   actual question is "is this transaction likely to succeed?" Different
   answer → different optimal strategy.

## Proposed API

```cpp
enum class tx_mode : uint8_t
{
   expect_success,  // Replay, import, known-good txns. Eager write-through.
   expect_failure,  // Speculative txns (block-building, validation). Buffered.
};

auto tx = ses.start_transaction(root, tx_mode::expect_success);
```

OCC drops out of the enum entirely.

### Workload mapping

| Workload | Mode | Why |
|---|---|---|
| Silkworm replay / chain import | `expect_success` | Every tx is known good (already in chain). |
| Blockchain block-building | `expect_failure` | Txns regularly revert (gas, conflicts, signatures). |
| DB recovery replay | `expect_success` | Re-applying durable WAL records. |
| Schema migration | `expect_success` | Caller controls correctness. |
| Speculative validation passes | `expect_failure` | May abort partway. |

## Engine semantics

### `expect_success`

- **Eager write-through**: writes mutate the tree as they arrive.
- **Per-txn version bumped at `start_transaction`**: every op in the txn
  carries the same `ver_num`. One atomic increment per txn, not per op.
- **Coalescing rule**: if the topmost chain entry on a value_node has
  version == this txn's version, **overwrite that entry in place** rather
  than appending a new one. Multiple updates to the same key collapse to
  one chain entry per (key, txn) pair.
- **Unique-root optimization (primary root only)**: if no concurrent reader
  holds the published primary root, the writer can mint a private root via
  one root-level COW at txn-start, after which all mutations are in-place
  under SAL's `can_modify` (session-owned, post-first-write-pos) fast path.
  **Not applicable to subtrees** — see the subtree caveat below.
- **Abort path**: a failed version goes through the same release lifecycle
  as any superseded version. `dead_versions.add_dead_version(ver_num)`
  registers it; the GC sweeps pages tagged with that version and reclaims
  the version control block. The only difference from a normal supersede is
  that no live reader could ever have held this version (it was never
  published), so the dead-set is immediately reclaimable rather than
  pending reader drain. **Not "leaked" — properly released through the
  same machinery.**
- **Commit path**: one `swap_root_ver` to publish the txn's version.

### `expect_failure`

- **Lazy/deferred write**: all mutations land in `detail::write_buffer`. Tree
  is not touched until commit or a buffer-bypass path.
- **Lazy version allocation**: no version is bumped at `start_transaction`.
  No `global_version.fetch_add`. No `alloc_custom_cb`. The txn is genuinely
  free until it does something tree-touching.
- **First tree touch triggers `ensure_version()`**:
  - `commit()` replaying a non-empty buffer
  - `remove_range(lo, hi)` whose persistent count exceeds the
    tombstone threshold (today: 256 keys) — buffer flushes then the
    range removal hits the tree directly
  - any future op that bypasses the buffer for size or correctness reasons
- **Abort path**: drop the buffer. If `ensure_version()` was never called,
  the txn registered nothing — no global state changed. If it was called,
  same path as `expect_success` abort: register the version dead.
- **Reads do not bump**: a txn that did `get`s and aborted is genuinely free.

### Subtree caveat (applies to both modes)

A subtree opened mid-transaction is **never uniquely owned**, even when its
root smart_ptr's local refcount looks like 1. The reason: the subtree's root
address is held by the parent's value_node, which is reachable through every
old snapshot of the parent. Any reader that opened the parent at an earlier
generation can walk down to the subtree through their parent snapshot. So
the subtree's effective reachability is "the parent's reachability set" —
which by definition includes pre-existing readers we don't control.

Mutating subtree nodes in place (via the unique-root fast path) would
therefore corrupt those readers' views. Concretely: a reader holding an old
parent root walks down → reaches the subtree's value_node → dereferences the
subtree's root pointer → reads the subtree contents. If we mutated those
nodes in place under "we own this," the reader sees torn or wrong data.

There's a second, independent reason: **abort rollback**. The unique-root
optimization assumes mutations are durable in the sense that "we won't
change our mind." For the primary root, abort just drops the writer's
private new root — the published one is unaffected. For a subtree, the
mutations might be rolled back by a parent-frame abort, and if they were
done in place on shared nodes, there is no original state to restore.

Therefore:

- **Subtree mutations always go through the versioned/COW path.** No
  in-place fast path. The MVCC stripe-lock fast path is fine — it tags
  each mutation with the parent txn's version and chains entries on
  value_nodes, so abort can release the version and the GC reclaims tagged
  pages exactly as for the primary tree.
- **Opening a subtree does not COW the subtree root.** The subtree's tree_id
  is preserved as-is until something actually mutates it; the first
  structural mutation triggers a normal COW like any shared node.
- **Subtree mutations participate in the parent's version.** Multi-level
  subtrees inherit `_txn_version` from the top-level txn, so coalescing
  works across the whole transaction tree (one chain entry per (key,
  parent-txn) regardless of depth).

### What both modes share

- The MVCC fast path (`try_mvcc_upsert`/`mvcc_upsert`) — both modes apply
  buffered/eager writes through the same per-key version-chain mutator.
- The dead_versions GC.
- The `_change_sets` vector / `tree_handle` / multi-root machinery.
- Frame stack for sub-transactions.

## DWAL parity

DWAL transactions are eager-WAL-append by construction. Today there is no
buffer in front of the WAL; every `upsert` produces a WAL record immediately.
This is the "expect_success" pattern.

DWAL would also benefit from `expect_failure`:

- **Block-building over DWAL** wants the same speculative semantics.
  Buffering writes in RAM and only emitting WAL records on commit avoids
  WAL pollution from aborted txns.
- **WAL bytes are not free** — they take space, the merge thread has to
  process them, recovery has to scan them. Aborted txns appending to the
  WAL is pure waste.
- **The forced-flush path** for "big change" ops (large remove_range, etc.)
  applies symmetrically: when the cost of buffering exceeds the cost of
  going through the WAL directly, flush and proceed.

Concretely, on DWAL:

- `expect_success`: WAL append per op (today's behavior).
- `expect_failure`: in-RAM buffer; commit replays the buffer into a single
  group-WAL record (or chained records) and fsyncs once. Aborts produce
  zero WAL bytes. Mid-transaction "big" ops force the buffer to flush
  through the WAL machinery before proceeding.

This also simplifies version semantics on DWAL: the WAL records carry the
txn's version, lazily allocated at first WAL emission. Aborted txns burn
no version. Committed txns get exactly one.

## Per-txn versioning details (cross-engine)

Today: each `upsert` calls `global_version.fetch_add(1)` and allocates a
new ver control block (`alloc_custom_cb`). 100 updates → 100 atomic
increments + 100 CB allocations + 100 chain entries.

After: one bump per committing txn, one CB allocation per committing txn,
≤1 chain entry per (key, txn) pair.

### Value-node coalesce ctor

A new `value_node` constructor variant alongside the existing append:

```cpp
// Existing: copy + append
value_node(uint32_t asize, ptr_address_seq seq,
           const value_node* src, uint64_t version, value_view new_val);

// New: copy-all-but-last + append (replaces top entry without growing chain)
value_node(uint32_t asize, ptr_address_seq seq,
           const value_node* src, uint64_t version, value_view new_val,
           replace_last_tag);
```

In `try_mvcc_upsert` value_node case:

```cpp
auto last_ver = vref->latest_version();
if (last_ver == _txn_version) {
   // Coalesce: overwrite the top entry
   if (new_val fits in src's existing offset slot)
      memcpy bytes in place
   else
      mvcc_realloc<value_node>(vref, vref.obj(), version, new_val,
                               replace_last_tag{});
} else {
   // Existing path: mvcc_realloc with append
   mvcc_realloc<value_node>(vref, vref.obj(), version, new_val);
}
```

### Cases by size

| New value vs. old slot | Strategy |
|---|---|
| Same size | memcpy bytes in existing offset. Free. |
| Smaller | memcpy, leftover bytes become slack until compaction. Free. |
| Larger | `mvcc_realloc` + replace-last (one alloc, no chain growth). |

### Net effect

| Pattern | Today | After refactor |
|---|---|---|
| Insert K | 1 leaf-insert | 1 leaf-insert |
| Insert K, update K | 1 insert + 1 inline→VN promote | 1 insert; second update mutates leaf bytes in place (Case A skipped) |
| 2 updates to K (committed) | 2 chain entries, 2 reallocs | 1 chain entry, ≤1 realloc |
| 100 updates to K | 100 chain entries, 100 reallocs | 1 chain entry, ≤1 realloc |

## Three-axis API model

The reshaped public surface:

1. **Engine** — `psitri::database` (COW) vs `psitri::dwal::dwal_database`
   (WAL). Durability/sharing model.
2. **Commit expectation** — `tx_mode::expect_success` vs
   `tx_mode::expect_failure`. Eager vs deferred. Applies to both engines.
3. **Concurrency** — implicit (auto-detect via current reader count) or
   explicit via a flag/method. Determines whether the writer can use the
   unique-root in-place fast path or must go through MVCC.

OCC is gone. The three axes are orthogonal; current code collapses 2 and 3
into the `tx_mode` enum and bolts OCC on as a third value.

## Implementation outline

Order matters — earlier steps unblock later ones.

1. **Drop OCC code paths.**
   - Remove `tx_mode::occ`, `read_set`, `lb_entry`, `read_entry`.
   - Remove `transaction::_occ_commit_func`, the OCC ctor, `track_read`,
     `track_bound`, `do_bound`'s tracking branch.
   - Remove `basic_write_session::occ_commit`.
   - Remove `[occ]` test cases (or repurpose them to validate the new
     concurrency check).

2. **Rename `tx_mode::batch` → `expect_success`, `tx_mode::micro` →
   `expect_failure`.** Mechanical. Update all call sites and test fixtures.

3. **Per-txn version threading.**
   - `tree_context` carries `_txn_version`.
   - `try_mvcc_upsert`/`mvcc_upsert`/`mvcc_remove` drop their `version`
     parameter; pull from `_txn_version`.
   - `start_transaction(expect_success)` allocates ver_num up front.

4. **Value-node coalesce ctor + `try_mvcc_upsert` coalesce branch.**
   See above.

5. **Lazy version allocation for `expect_failure`.**
   - `_txn_version = 0` sentinel until first tree touch.
   - `ensure_version()` helper. Plug into `commit()` (when buffer non-empty)
     and `merge_buffer_to_persistent()` (the only buffer-flush callsite).

6. **Abort path: release the failed version like any superseded version.**
   - `transaction::abort()` calls `dead_versions.add_dead_version(_txn_version)`
     when the version was actually allocated.
   - Release the version control block (`_allocator_session->release(_ver_cb)`)
     — same call the commit path makes when superseding the old ver. The CB
     refcount falls to zero and SAL reclaims it.
   - Pages tagged with the released version are swept by the GC sweep on
     its next pass — same path as committed-but-superseded versions, except
     the dead-set is immediately reclaimable since no live reader could
     have held an unpublished version.
   - The mechanism is identical to a successful txn whose commit was
     immediately superseded by another commit. Abort is just "supersede
     by nothing."

7. **Unique-root optimization (separate ticket, primary root only).**
   - Detect zero readers on the published primary root at
     `start_transaction`.
   - If zero, do one root-level COW at start; subsequent primary-tree
     mutations stay in-place via `can_modify`.
   - Falls back to MVCC fast path if reader detected mid-txn (rare).
   - **Explicitly does not apply to subtrees** — see the subtree caveat
     section. `tree_handle::open_subtree` always uses the versioned MVCC
     path. Trying to extend the optimization to subtrees would require
     proving exclusivity through the parent's reachability graph (i.e.,
     "no snapshot of any parent generation reaches this subtree"), which
     is not tractable without traversing the live_range_map.

8. **DWAL `expect_failure` mode (separate ticket).**
   - Add buffer in front of WAL append.
   - Commit emits a single grouped WAL record from the buffer.
   - Mid-tx flush on "big" ops.

## What needs verification

- **Today's `batch` mode root sharing.** Does `init_primary_cs(get_root(0))`
  inherit a shared (refcount > 1) root from the published `top_root`,
  forcing per-mutation COW until depth fills with session-owned nodes? Or
  does the SAL `can_modify` per-segment-by-session check already short-circuit
  this? Read of `tree_ops::copy_on_write` + segment session ownership flow.

- **Aborted-version release symmetry.** Confirm that
  `dead_versions.add_dead_version(_txn_version)` plus releasing the
  version control block is sufficient to reclaim all pages tagged with that
  version. Today's release path is exercised by commit-then-supersede; abort
  uses the same primitives but skips the publish step, and we want to verify
  no GC code path implicitly assumes "this version was once published."

- **DWAL buffer integration.** DWAL today applies WAL records to the trie via
  the merge thread. Adding a buffer in front of WAL means the buffer's
  contents are not durable until commit-flush. Recovery semantics: only
  committed (WAL-flushed) data is recoverable. Verify this matches caller
  expectations — block-building wants this exact semantic.

## Open questions

- Should `concurrency` be exposed as a third tx-start parameter, or kept
  implicit via auto-detection of reader count? Implicit-with-override
  feels right; manual override is escape-hatch.

- For DWAL, does `expect_failure` change the commit-side group-fsync story?
  Today many writers can group-commit through the same WAL flush. With
  per-writer buffers, the commit point becomes "buffer→WAL emit + group
  fsync" — one extra layer but otherwise unchanged.

- Does `tree_handle::open_subtree` need explicit mode propagation, or does
  the parent txn's mode dictate? Probably the latter — sub-trees inherit the
  parent's strategy. The `with_subtree` API was just removed
  (.issues redundancy with `tree_handle`); confirm that doesn't reintroduce
  itself here.

## Why this is worth doing

- **Removes a known-bad mode** (OCC) that confuses callers and contributes
  no value.
- **Aligns API names with caller intent** — fewer "what's the difference
  between batch and micro?" questions.
- **Enables real perf wins**:
  - Per-txn versioning + coalesce: hot-key updates collapse from O(updates)
    chain entries to O(1).
  - Lazy versioning: aborted speculative txns don't pollute the version
    counter or burn CB allocations.
  - Unique-root: replay workloads skip COW entirely on the structural ops.
- **Symmetry across engines** — same mental model for COW and DWAL callers.
- **Cleaner GC contract** — version space is "generations that exist or
  existed in the tree," not "txns that started."
