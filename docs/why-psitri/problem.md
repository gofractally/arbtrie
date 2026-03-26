# The Problem

Every persistent key-value store makes the same set of tradeoffs:

- **B-trees** (LMDB, BoltDB, SQLite, InnoDB) operate on fixed-size pages (4KB-16KB). To support snapshots and crash safety, they use **copy-on-write** (COW) -- instead of modifying data in place, they copy it first so the original remains intact. But the unit of copying is an entire page, so changing a single byte means copying 4KB-16KB. Write amplification is proportional to page size, not data size.
- **LSM trees** (RocksDB, LevelDB, Cassandra) batch writes for throughput but pay for it with read amplification, compaction stalls, and space amplification from tombstones.
- **Adaptive radix trees** (ART) achieve optimal depth and cache-line efficiency, but only in memory. No persistent ART implementation exists that matches B-tree durability.

PsiTri eliminates these tradeoffs by combining three novel subsystems:

1. **A radix/B-tree hybrid with sorted leaf nodes and 67-byte copy-on-write**
2. **A relocatable object allocator with lock-free O(1) compaction moves**
3. **A self-tuning physical data layout that sorts objects by access frequency**

---

## Comparison with Alternatives

| System          | Insert/sec (persistent) | Depth (30M keys) | Persistent | Copy-on-Write | Node Size |
|-----------------|-------------------------|-------------------|------------|-----|-----------|
| **PsiTri**      | **1.1-3.7M**            | **5**             | Yes        | Yes | 67B avg   |
| LMDB            | 0.3-0.8M                | 3-4               | Yes        | Yes | 4KB pages |
| RocksDB         | 0.5-1.5M                | N/A (LSM)         | Yes        | No  | variable  |
| ART (in-memory) | 5-10M                   | 5-8               | No         | No  | 52-2048B  |
| SQLite          | 0.1-0.5M                | 3-4               | Yes        | No  | 4KB pages |

## Where Existing Approaches Fall Short

### B-Trees: Page-Level Write Amplification

B-trees are the workhorse of persistent storage. But their unit of work is a **page** -- typically 4KB. When you change one key, copy-on-write forces copying 4KB at every level of the tree. For a 4-level B-tree, that's 16KB of writes for a single byte change.

This is a fundamental consequence of how B-trees organize data: keys are stored in sorted order within pages, and pages are the unit of I/O, locking, and copy-on-write. You can't copy-on-write part of a page -- the whole thing must be duplicated.

### LSM Trees: Compaction Tax

LSM trees (RocksDB, LevelDB, Cassandra) solve the write amplification problem by batching writes into sorted runs. But they pay for it elsewhere:

- **Read amplification**: Lookups must check the memtable, immutable memtables, and multiple SSTable levels
- **Compaction stalls**: Background compaction can cause latency spikes when it falls behind write throughput
- **Space amplification**: Tombstones and duplicate keys across levels consume extra storage
- **Tuning complexity**: Level sizes, compaction triggers, and bloom filter configurations require careful tuning per workload

### In-Memory Tries: Not Persistent

Adaptive radix tries (ART) achieve excellent cache-line efficiency and O(key_length) lookups. But existing implementations are memory-only. No production ART supports:

- Crash-safe persistence
- Copy-on-write for MVCC
- Background compaction for space reclamation
- Concurrent readers with snapshot isolation

## PsiTri's Design Point

PsiTri occupies a region of the design space that was previously empty:

| Capability                   | B-tree            | LSM-tree                | In-memory ART | PsiTri                                  |
|------------------------------|-------------------|-------------------------|---------------|------------------------------------------|
| Persistent + crash-safe      | Yes               | Yes                     | No            | Yes                                      |
| Copy-on-write granularity    | 4KB page          | N/A                     | N/A           | 67 bytes                                 |
| Online compaction            | No (offline copy) | Background (file-level) | N/A           | Background (object-level)                |
| Object relocation cost       | Cannot            | File rewrite            | N/A           | memcpy + 1 atomic CAS                    |
| Cache management             | OS or buffer pool | OS or buffer pool       | N/A           | Self-tuning MFU with physical relocation |
| Range count                  | O(k)              | O(k)                    | O(k)          | O(log n)                                 |
| Range delete                 | O(k)              | O(k) tombstones         | O(k)          | O(log n)                                 |
| Write amplification          | High (page-level) | High (compaction)       | N/A           | Minimal (node-level, tunable)            |
| Composable hierarchical data | Flattened + joins | Embedded blobs          | N/A           | Native subtrees with O(1) operations     |
