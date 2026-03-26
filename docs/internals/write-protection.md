# Write Protection & Durability

PsiTri uses hardware memory protection to prevent corruption of committed data and provides configurable durability levels.

## Hardware-Enforced Write Protection

All newly written data is marked read-only via `mprotect(PROT_READ)` **before** the atomic root pointer swap that makes it visible. By the time any reader can reach the data, it is hardware-protected against writes.

Any subsequent access from a bug, buffer overrun, or dangling pointer triggers an immediate CPU fault (SIGSEGV) rather than silent corruption.

```
Segment layout:
+------------------------------+---------------------+
|  Committed (PROT_READ)       |  Active (PROT_R|W)  |
|  mprotect'd after sync       |  New allocations     |
|  Stray write = SIGSEGV       |  only here           |
+------------------------------+---------------------+
page 0            _first_writable_page          _alloc_pos
```

### Multi-Layer Defense

| Layer | Mechanism | Effect |
|-------|-----------|--------|
| OS / MMU | mprotect(PROT_READ) | Stray writes fault immediately |
| Segment | `_first_writable_page` boundary | Tracks mutable vs immutable regions |
| Session | `can_modify()` ownership check | Only the allocating session can write |
| Object | `smart_ptr::modify()` COW | Read-only objects copied to writable segments |

Legitimate modifications go through `smart_ptr::modify()`, which checks `can_modify()`. If the object is in a read-only region, it automatically triggers copy-on-write to a writable segment.

## OS Paging Benefits

Read-only pages are never dirty, so the OS can evict them instantly under memory pressure -- no write-back required. Since the vast majority of a PsiTri database is committed (read-only) data, the OS can efficiently page cold portions in and out with zero write amplification.

## Transaction Model

```
start transaction
    modify_lock(index).lock()  -- blocks other writers to same root

commit transaction
    set_root() -- grabs root change mutex
    modify_lock(index).unlock()
    advance _first_writable_page on all segments used in transaction

abort transaction
    modify_lock(index).unlock()
```

## Compactor Safety

The compactor must ensure no one is modifying a segment it is compacting. A segment can only be modified by the thread that allocated it, and only before it is marked read-only (which happens on commit).

### Race Resolution

When the compactor and a session both try to update the same node's location:

**Session wins**: Session writes new location, compactor detects and abandons its copy.

**Compactor wins**: Compactor updates location, session overwrites with its own new location. Both mark free space correctly in their respective old segments.

## Sync Modes

| Level | Mechanism | App crash | Power loss | Write latency |
|-------|-----------|-----------|------------|---------------|
| `none` | OS writes when convenient | Maybe | No | Zero |
| `mprotect` | Hardware write-protect pages | Yes | No | ~microseconds |
| `msync_async` | Hint to OS: flush soon | Yes | Probably | ~microseconds |
| `msync_sync` | Block until OS buffers written | Yes | Mostly | ~milliseconds |
| `fsync` | Flush OS buffers to drive | Yes | Yes* | ~milliseconds |
| `full` | F_FULLFSYNC / flush drive cache | Yes | Yes | ~10s of ms |

*`fsync` sends data to the drive, but the drive's write cache may not have committed it to physical media.

## Types of Failure

| Failure | Impact | Recovery |
|---------|--------|----------|
| Program crash | Read-only memory safe; RW state (ID DB) may be inconsistent | Rebuild control blocks, reclaim leaks |
| Power failure | Only data synced to disk is safe | Validate sync boundaries, rebuild from segments |
| OS crash | Similar to power failure | Same as power loss recovery |
| RAM corruption | Detected by checksums at compaction time | Reopen with `full_verify` recovery mode |
| Disk corruption | Detected by checksums at compaction time | Compactor halts writes, triggers `corruption_error` |

## Passive Corruption Detection

The compactor validates per-object checksums (16-bit XXH3) as it copies. If a mismatch is detected:

1. Sets atomic `_corruption_detected` flag
2. Halts compaction
3. Every subsequent `start_transaction()` checks this flag and throws `corruption_error`
4. Reads continue working, but all writes are halted until the database is reopened with appropriate recovery
