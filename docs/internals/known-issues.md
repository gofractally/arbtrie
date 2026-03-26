# Known Issues

!!! warning "PsiTri is under active development"
    The following are known issues and limitations. They are tracked here for transparency.

## File Size Management

PsiTri's copy-on-write architecture means dead copies accumulate between compaction passes. The background compactor continuously reclaims space, but the file size can be 2-4x the reachable data size during sustained write workloads.

Use `compact_and_truncate()` after bulk operations to reclaim disk space:

```cpp
db->compact_and_truncate();
```

## Snapshot Pressure on Fan-Out

Long-lived snapshots compete with live nodes for cacheline slots in inner nodes. Under heavy snapshot pressure, inner node fan-out can degrade from 256 to as low as 16 branches per node. The compactor can relocate object data but cannot reassign `ptr_address` slots to new cacheline positions.

Fan-out recovers when snapshots are released. See [Scaling Limits](../architecture/sal.md#scaling-limits) for analysis.

## Recovery Time

Recovery time is proportional to database size (must scan segments + walk reachable tree), not proportional to uncommitted work. For very large databases, recovery after a crash may take significant time. The `deferred_cleanup` mode provides fast restart by deferring the expensive leak reclamation.

## Multi-Writer Concurrency

Multi-writer support is in progress. Currently, concurrent writes to the **same root index** are serialized via mutex. Writes to **different root indices** (out of 512 available) are fully concurrent.
