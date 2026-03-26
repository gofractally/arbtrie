# Segment Allocator (SAL)

SAL provides persistent, memory-mapped allocation with reference counting, copy-on-write, and background compaction. It is the foundation that PsiTri builds on.

## Addressing Model

Every allocated object has a permanent 32-bit ID (`ptr_address`) that indexes into an array of atomic 64-bit control blocks:

```
ptr_address (32-bit permanent ID)
     |
     v  array lookup (O(1))
control_block (64-bit atomic)
  +------------------------------------------------------+
  | ref_count (21 bits) | location (41 bits) | flags (2b) |
  +------------------------------------------------------+
     |
     v  direct memory access
Object Data (at physical location in memory-mapped segment)
```

The location field addresses 64-byte cache lines. The 41-bit field can theoretically address 128 TB, but the configured maximum database size is **32 TB**.

## Object Header (alloc_header, 12 bytes)

```
+---------------------------------------------------------------+
| checksum (2B)   ptr_addr_seq (6B)   size:25 | type:7 (4B)     |
+---------------------------------------------------------------+
```

- **checksum**: XXH3 hash for corruption detection
- **ptr_addr_seq**: 2-byte sequence number + 4-byte `ptr_address`, used for crash recovery ordering when the same address appears in multiple segments
- **size**: Object size in bytes (64-byte aligned), max 32 MB
- **type**: User-defined type ID (7 bits, 127 types)

## Copy-on-Write (COW)

**Copy-on-write** means shared data is never modified in place -- a copy is made before any mutation, preserving the original for concurrent readers and crash recovery.

```
write to object
     |
     v
ref_count == 1? ---yes---> modify in-place (unique path)
     |
     no
     |
     v
allocate new object
copy data + apply modification
update control_block location
release old reference
```

The unique vs shared decision propagates through the tree: if a parent node has `ref == 1`, all modifications down to the leaf can be in-place. If any ancestor is shared (`ref > 1`), the entire path must be copied.

## Background Threads

### Segment Provider

Pre-allocates segments and manages pinned vs unpinned pools. Writers never block on file I/O, mmap, or system calls during allocation -- they grab pre-initialized segments from a ready queue.

```
Writer A:  [segment 7 ........]  <-- bump allocate here
Writer B:  [segment 12 ......]  <-- bump allocate here, independently
Writer C:  [segment 3 ........]  <-- about to grab next from ready queue

Background provider: [...] -> [seg 15] -> [seg 16] -> [seg 17]
```

### Compactor

The compactor runs continuously in the background:

1. **Defragments segments** by moving live objects to new segments
2. **Promotes hot data** to pinned (mlocked) segments based on MFU tracking
3. **Validates checksums** as it copies, detecting corruption passively
4. **Drains release queues** from writer sessions, batching free-list updates

### Read Bit Decay

Periodically clears MFU access bits on a rolling window, ensuring the cache tracks frequency rather than recency. Also manages mlock/munlock of segments and proactive file extension.

## MVCC Read Isolation

```
Thread A (writer)            Thread B (reader)
     |                            |
     |                       read_lock rl = session.lock()
     |                            |
  modify node --COW-->       sees old version via
  (new location)              original location
     |                            |
     |                       ~read_lock()
     |                            |
     |                       next lock sees new version
```

- Read locks are **wait-free** (atomic counter only, no mutex)
- Up to 64 concurrent readers
- Compactor respects active read locks before relocating objects

## Concurrent Writers

Multiple writer sessions can operate simultaneously on independent trees with zero contention. See [Concurrent Writers](concurrent-writers.md) for the full design.

## Sync Modes

| Mode | Durability | Performance |
|------|-----------|-------------|
| `none` | Process crash safe only | Fastest |
| `mprotect` | + Write protection on committed data | Fast |
| `msync_async` | + OS flush initiated | Moderate |
| `msync_sync` | + Block until OS writes | Slower |
| `fsync` | + Block until drive acknowledges | Slow |
| `full` | + F_FULLFSYNC (flush drive cache) | Slowest, safest |

## Scaling Limits

| Limit | Value | Constraint |
|-------|-------|-----------|
| Configured max database | 32 TB | 41-bit field supports up to 128 TB; configured limit is 32 TB |
| Object IDs | ~4 billion | 32-bit `ptr_address` space |
| Descendant counter | ~550 billion | 39-bit `_descendents` per inner node |
| Concurrent snapshots | ~2 million | 21-bit reference count |

Which limit you hit first depends on your data profile:

| Avg Key | Avg Value | Inlined? | Limiting Factor | Max Keys |
|--------:|----------:|----------|----------------|--------:|
| 8 B | 8 B | Yes | 4B address space | ~230B |
| 32 B | 32 B | Yes | 4B address space | ~230B |
| 32 B | 256 B | No | 4B address space | ~4B |
| 32 B | 4 KB | No | 4B address space | ~4B |
| 32 B | 1 MB | No | 32 TB storage | ~33M |
