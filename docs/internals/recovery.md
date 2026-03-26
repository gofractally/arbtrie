# Recovery

PsiTri has no write-ahead log. Recovery is built into the data layout itself.

## Why Control Blocks Can't Survive a Crash

Control blocks track two things: where each object lives (location) and how many references point to it (ref count). Both become untrustworthy after a crash:

- **Reference counts include in-flight stack references.** Every `smart_ptr` on the stack holds a reference. When the process crashes, those destructors never run -- ref counts are left permanently inflated.
- **Control blocks are pinned to RAM to avoid SSD wear.** They change on every retain, release, and relocation -- far too frequently to flush to disk. They are volatile by design.

On clean shutdown, a `clean_shutdown` flag is set so control blocks can be reloaded as-is. On crash, they must be rebuilt from segments.

## Recovery Modes

The caller tells the database what kind of failure occurred via `recovery_mode`:

| Mode | When to use | What it does |
|------|-------------|-------------|
| `none` | Clean shutdown detected | No recovery needed |
| `deferred_cleanup` | App crash, fast restart | Mark ref counts stale, defer leak reclamation |
| `app_crash` | App crash, full cleanup | Reset ref counts, reclaim leaked memory |
| `power_loss` | OS or hardware crash | Validate segments, rebuild control blocks and roots |
| `full_verify` | Suspected corruption | Deep checksum verification of all objects |

If no mode is specified and the clean_shutdown flag is unset, the database defaults to `deferred_cleanup` -- the tree is consistent, so the only cost is leaked objects occupying space until the user explicitly runs recovery.

## How Recovery Works

The segments -- append-only, ordered by allocation sequence -- are the durable source of truth.

### Root Pointer Sources

1. **The roots file** -- memory-mapped, synced at each commit. Primary source of root pointers.
2. **Sync headers** -- 64-byte records at segment sync boundaries with backward chain, timestamp, XXH3 checksum, and optional root info. Fallback when the roots file is corrupt.

### App-Crash Recovery (Common Case)

**Phase 1: Rebuild locations.** Clear all control block metadata. Sort segments newest-to-oldest by `_provider_sequence`. Scan each segment's objects sequentially. For each object ID:

- If unseen: record its location, set ref count to 1
- If already mapped from a newer segment: skip (newer copy wins)
- If in the same segment: update (later offset = newer)

**Phase 2: Retain reachable nodes.** Starting from all valid roots (from roots file or sync headers), recursively traverse using `visit_branches()`. Each reachable node gets `retain()`, bumping ref count to 2+.

**Phase 3: Release unreachable.** Decrement every ref count by 1. Objects that drop to 0 are freed. Reachable objects settle to their correct ref counts.

### Power-Loss Recovery

Adds segment validation before the three phases above: validate sync header checksums to identify the last trustworthy sync boundary. Data beyond the last valid sync header may be torn; data before it is guaranteed consistent.

## Design Considerations

!!! note "These are design notes, not fully implemented features"

### Determining Most Recent Copy

Given two segments with a copy of a node with the same ID, recovery must correctly identify which one is most recent:

- Within a segment, later offset = newer
- Each segment tracks the session that allocated it
- Segment timestamps provide ordering between sessions
- A 24-bit allocation sequence number in the node header disambiguates across overlapping segments

### Worst Case Scenario

1. Thread A uses ID1 in Segment 1
2. Compactor moves it to Segment 2
3. Thread B re-allocs ID1 after A releases it in Segment 3

Three copies of the same ID in three segments. The allocation sequence number provides a total order.

### Segment Header

Each segment stores:

- Time the segment was popped from the allocation queue
- Time the segment was finalized after being filled
- Size-weighted average time of the data within
- Session number that allocated it
- Source segment info for compacted data
