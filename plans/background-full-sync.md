# Background Full Sync Design

## Overview

A new sync mode (`background_full`) that provides zero-latency writes with
configurable-interval background durability. Writers never block on disk I/O.
A background thread periodically fsyncs all finalized segments. Segments cannot
be recycled until confirmed durable, giving automatic multi-version snapshots.

## Motivation

- **SSD write amplification**: Sequential append-only writes to new segments.
  No segment reuse means no rewriting sectors. Recycling only starts after
  fsync confirms durability.
- **Zero write latency**: Main thread only does mprotect (software crash safety).
  All disk I/O is batched in the background thread.
- **Automatic snapshots**: Every committed root has a complete tree behind it
  because old segments persist until recycling. Each fsync boundary is a
  guaranteed-consistent recovery point.
- **Simple recovery**: Last completed fsync = the only valid snapshot on disk.
  Pinned RAM prevents the OS from lazily flushing pages, so on-disk state is
  entirely under our control.

## Sync Type Hierarchy

```
none        = 0   // data persists on OS schedule / process exit
mprotect    = 1   // mprotect(PROT_READ) — software crash safety
msync_async = 2   // non-blocking hint to flush to disk
msync_sync  = 3   // block until written to disk (default)
fsync       = 4   // fsync the file descriptor
full        = 5   // F_FULLFSYNC (macOS) — flush drive write cache
```

The proposed `background_full` mode behaves like `mprotect` for the writer
thread, with a background thread that periodically does `full` sync.

## Segment Durability State Machine

```
active --> finalized --> sync_pending --> sync_confirmed --> recyclable
  ^                                                            |
  +-------------------------- free <---------------------------+
```

New per-segment metadata:
```cpp
enum class sync_state : uint8_t {
   active,          // session is writing to it
   finalized,       // no more writes, queued for background sync
   sync_pending,    // background thread is fsyncing it
   sync_confirmed,  // on physical media, safe to recycle
   free             // available for reuse
};
```

## Writer Flow (main thread)

Identical to `mprotect` mode — zero disk I/O:

1. Allocate objects in active segment
2. On commit: `segment::sync()` writes sync header with root_info,
   advances `_first_writable_page`, calls `mprotect(PROT_READ)`
3. `allocator::sync()` syncs the roots file (memory-mapped, mprotected)
4. Returns immediately

## Background Sync Thread

New thread alongside existing compactor and segment_provider:

```
while (running) {
    seg_num = durability_queue.pop();  // finalized, unsynced segments
    if (none) { wait(sync_interval); continue; }

    seg.sync_state = sync_pending;

    msync(seg->data, segment_size, MS_SYNC);
    #if __APPLE__
    fcntl(fd, F_FULLFSYNC);       // flush drive write cache
    #else
    fdatasync(fd);
    #endif

    seg.sync_state = sync_confirmed;
    confirmed_seq.fetch_add(1);
}
```

The `sync_interval` is configurable in `runtime_config` — the tradeoff between
"how much work can I lose on power failure" and "disk I/O bandwidth consumed."

## Compactor Recycling Gate

The compactor copies live objects from old segments to new ones. One constraint:

```cpp
// Before recycling source segment:
if (source.sync_state != sync_confirmed)
    return;  // defer — old version must survive
if (dest.sync_state != sync_confirmed)
    return;  // defer — new copy not yet durable
free_segment(source);
```

**Critical safety invariant**: never free the old copy until the new copy is
confirmed on physical media. This guarantees that at any point, every logical
object has at least one durable copy on disk (after the first fsync).

## Recovery Model

Because segments are pinned in RAM (`mlock`), the OS does not lazily flush
pages to disk. The only time data reaches physical media is when the background
thread explicitly calls fsync. Therefore:

- **Last completed fsync = the only valid on-disk snapshot**
- Everything committed after the last fsync is lost on power failure
- No partial-flush ambiguity — the on-disk state is entirely under our control
- Recovery is simple: find the most recent `sync_confirmed` sync header,
  that root and everything reachable from it is guaranteed consistent

### Recovery algorithm

1. Scan segments, validate sync header checksums
2. `sync_confirmed` segments are fully trusted
3. `sync_pending`/`finalized` segments: validate checksums, use if valid
4. Rebuild roots from newest valid sync headers
5. Fall back to roots file if sync headers unavailable
6. Walk roots to retain reachable objects, free the rest

## Backpressure

Under heavy write load, segments accumulate faster than background sync:

```cpp
if (pending_sync_count > high_water_mark) {
    // Block writer until background thread catches up
    // Or: switch to synchronous msync for this segment
}
```

Configurable via `runtime_config::background_sync_high_water_mark`.

## Configuration

```cpp
struct runtime_config {
    sync_type sync_mode = sync_type::msync_sync;  // existing

    // New fields for background_full mode:
    uint32_t background_sync_interval_ms = 1000;  // fsync every N ms
    uint32_t background_sync_high_water  = 16;    // max unsynced segments
};
```

## Files to Modify

| File | Change |
|------|--------|
| `config.hpp` | Add `background_full` to sync_type enum, add config fields |
| `segment.hpp` | Add `sync_state` field to segment footer |
| `segment_thread.cpp` | New background sync thread |
| `segment_provider` | Gate free_segments on sync_confirmed |
| `compactor` | Check source AND dest sync_confirmed before recycling |
| `segment_impl.hpp` | For background_full: mprotect only, enqueue to durability queue |
| `allocator.hpp` | Add durability queue, confirmed_seq counter |

## Key Properties

- **Zero write latency** — main thread never touches disk
- **Full durability** — background thread ensures everything reaches media
- **No data loss from compaction** — old segments survive until new confirmed
- **Simple recovery** — last fsync boundary is ground truth
- **Minimal SSD wear** — sequential appends, no overwrites until recycled
- **Configurable durability window** — interval controls max data loss
