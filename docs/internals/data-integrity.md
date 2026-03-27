# Data Integrity & Verification

PsiTri implements five independent checksum levels that protect data from the segment layer up through individual key-value pairs. Together they detect corruption at any granularity -- from a single flipped bit in a value to a torn segment write -- and pinpoint exactly which keys are affected.

Most databases offer one or two integrity mechanisms. PsiTri's layered approach means that a failure at one level doesn't prevent verification at others: a corrupt inner node still has individually checksummed children, and a failed segment sync checksum doesn't invalidate the object-level checksums within it.

## Zero Performance Cost, Zero Wasted Space

A natural concern with five checksum levels is overhead. In PsiTri, the cost is effectively zero:

- **Key hashes (1 byte each) are not overhead -- they accelerate lookups.** The hash array is the first thing consulted during a leaf search. It filters non-matching keys before any full key comparison, turning an O(n) scan into a hash-first probe. The integrity check is a free side effect of a performance optimization.
- **Value checksums (1 byte each) fit in existing padding.** The `value_data` header is 2 bytes (checksum + size), and these bytes would otherwise be alignment padding or wasted in the variable-length layout.
- **Object checksums (2 bytes) are computed once on allocation.** Copy-on-write means every mutation creates a new object -- the checksum is computed at creation time and never recomputed. There is no "update in place" path where the checksum must be recalculated.
- **Segment sync checksums are computed during sync** -- an operation that is already dominated by the `msync`/`fsync` call. The XXH3 hash over the committed byte range is negligible compared to the I/O wait.

Traditional databases face a harder tradeoff: page checksums must be recomputed on every page write, and verifying them on every page read adds measurable overhead (PostgreSQL's `data_checksums` is a non-trivial cost). PsiTri's copy-on-write model eliminates this tension entirely -- checksums are write-once and read-only.

## The Five Checksum Levels

### Level 1: Segment Sync Checksums

**Scope:** Byte range between two sync points within a segment.

Every time a session syncs, a `sync_header` is appended to the segment. It records the byte range since the previous sync header and stores an XXH3-64 checksum over that entire range. This creates a backward-linked chain of checksummed regions covering all data written to the segment.

```
Segment layout:
┌─────────────────┬──────────────┬─────────────────┬──────────────┬────────┐
│  objects A-D    │ sync_header₁ │  objects E-H    │ sync_header₂ │  ...   │
│                 │ cksum(A..D)  │                 │ cksum(E..H)  │        │
└─────────────────┴──────────────┴─────────────────┴──────────────┴────────┘
```

Sync checksums are configurable -- when `checksum_commits` is enabled, every sync computes `XXH3_64bits(data + start, end - start)`. When disabled, the checksum field is zero, and the verification pass reports these ranges as "not checksummed" rather than failed.

**What it catches:** Torn writes from power loss, filesystem corruption, partial page flushes, storage media errors that corrupt a range of bytes.

**Granularity:** Coarse -- a failure implicates all objects in the range, though individual object checksums (Level 2) can narrow it down.

### Level 2: Object Checksums

**Scope:** Individual allocated object (node, value, or subtree).

Every `alloc_header` has a 16-bit checksum field computed as `XXH3_64bits(data + &_address, _size - sizeof(_checksum))` -- covering everything in the object except the checksum field itself. This is updated on every copy-on-write allocation.

```
alloc_header layout (12 bytes):
┌──────────┬────────────────────────┬──────────────┐
│ checksum │ ptr_addr_seq (6 bytes) │ size | type  │
│ (2 bytes)│ sequence + address     │ (4 bytes)    │
└──────────┴────────────────────────┴──────────────┘
          ↑ checksum covers this range →→→→→→→→→→→ ... end of object
```

A checksum of zero means "not set" -- the verification pass counts these as unknown rather than failed, since some allocation paths may skip checksumming for performance.

**What it catches:** Any corruption of a single node's data -- flipped bits in branch pointers, corrupted key suffixes in leaves, damaged inner node dividers.

**Granularity:** Per-object. A failed checksum on an inner node makes all keys in that subtree suspect. A failed checksum on a leaf makes all keys in that leaf suspect.

### Level 3: Key Hashes

**Scope:** Individual key within a leaf node.

Every key stored in a leaf has a precomputed 1-byte XXH3 hash stored in the `key_hashs[]` array. These hashes serve double duty: they accelerate lookups (hash-first filtering before full key comparison) and detect per-key corruption.

```cpp
uint8_t calc_key_hash(key_view key) const noexcept {
    return XXH3_64bits(key.data(), key.size());
}
```

**What it catches:** Corruption isolated to a single key's data within an otherwise intact leaf node. Bit rot in key storage that wouldn't be caught by the object checksum if the checksum itself is also corrupt.

**Granularity:** Per-key. The verification report identifies the exact key (hex-encoded) and its position within the leaf.

### Level 4: Value Checksums

**Scope:** Individual inline value within a leaf node.

Inline values (stored directly in the leaf rather than in a separate value_node) carry a 1-byte XXH3 checksum over the value's size and data:

```cpp
void set(value_view value) {
    _size = value.size();
    std::memcpy(_data, value.data(), _size);
    _checksum = XXH3_64bits((char*)&_size, sizeof(_size) + _size);
}
```

Values stored in separate `value_node` objects (large values, subtrees) are covered by the object checksum (Level 2) instead.

**What it catches:** Corruption of a specific value while the key and leaf structure remain intact.

**Granularity:** Per-value. Reports the exact key whose value is corrupt.

### Level 5: Tree Structure Verification

**Scope:** Every child pointer in the entire tree.

The verification walk resolves every branch pointer from root to leaf. A pointer that fails to resolve to a valid object is a **dangling pointer** -- evidence of a structural break in the tree.

The walk also maintains a visited set to handle shared nodes in the DAG (PsiTri uses structural sharing via copy-on-write, so multiple paths may reference the same node). Each node is checksummed once on first visit.

**What it catches:** Dangling pointers from incomplete copy-on-write operations, orphaned subtrees, corruption in branch pointer encoding, cycles introduced by pointer corruption.

**Granularity:** Per-branch. Reports the key prefix leading to the dangling pointer and which root index it belongs to.

## Verification Algorithm

Verification runs in two passes:

**Pass 1: Segment Scan.** Walk every segment's sync_header chain. For each header with a non-zero checksum, recompute and compare. This is a sequential scan that doesn't require tree knowledge.

**Pass 2: Tree Walk.** Starting from all 512 root slots, recursively descend through every inner node, prefix node, and leaf. At each node:

1. Resolve the address through the control block to get the physical location
2. Check the visited set (skip shared nodes already verified)
3. Verify the object checksum (Level 2)
4. Dispatch by node type:
    - **Inner nodes**: recurse into each branch, accumulating the key prefix
    - **Leaves**: verify each key hash (Level 3) and value checksum (Level 4); recurse into value_node/subtree addresses
    - **Value nodes**: recurse into subtree children if applicable

The accumulated key prefix at each level provides failure context -- when an inner node's checksum fails, the report identifies the affected key range (e.g., "all keys with prefix `0x3f7a...`") rather than just a raw address.

### Output

```
$ psitri-tool verify /path/to/db

-- Segment Sync Checksums --
  Checked               42
  Passed                38
  Failed                 0
  Not checksummed        4

-- Object Checksums --
  Reachable objects  125,432
  Passed             120,100
  Failed                   0
  Not checksummed      5,332

-- Key Hashes --
  Keys checked        98,200
  Passed              98,200
  Failed                   0

-- Value Checksums --
  Values checked      98,200
  Passed              98,200
  Failed                   0

-- Tree Structure --
  Roots checked         1 / 512
  Nodes visited     125,432
  Reachable size    560.2 MB
  Dangling pointers       0

  Database integrity verified.
```

Each failure is reported with enough context for targeted repair: the address and location of the corrupt node, the key prefix or full key affected, the root index, and the failure type.

## Comparison with Other Databases

### How Databases Protect Data

There are four fundamental strategies for data integrity, and every database uses some combination:

| Strategy | Description | Databases |
|----------|-------------|-----------|
| **Page checksums** | Checksum per fixed-size page (4-16 KB) | PostgreSQL, SQLite, MDBX, LMDB |
| **Block checksums** | Checksum per variable-size block | RocksDB (SST blocks), LevelDB |
| **WAL checksums** | Checksum per log record/batch | PostgreSQL, SQLite, RocksDB, MySQL/InnoDB |
| **Object checksums** | Checksum per logical object | PsiTri |

PsiTri is unique in combining object-level checksums with fine-grained per-key and per-value verification. Most databases stop at the page or block level.

---

### RocksDB

RocksDB is an LSM-tree that protects data at the block and file level.

| Layer | Mechanism | Granularity |
|-------|-----------|-------------|
| WAL | CRC32 per log record | Per batch |
| SST data blocks | CRC32 or XXH3 per block (4-64 KB) | Per block |
| SST metadata | CRC32 on index/filter blocks | Per block |
| SST file | Optional file checksum (XXH3 or CRC32c) | Per file |
| Manifest | CRC32 per version edit record | Per record |

**Verification:** `ldb checksmdump` / `ldb verify_checksum` walks SST blocks and verifies each block checksum. `BackupEngine::VerifyBackup()` verifies file-level checksums.

**Key limitations:**

- **No per-key checksums.** If a block checksum passes but a single key within that block has a bit flip, RocksDB cannot detect it. Block checksums cover 4-64 KB at a time -- corruption that spans exactly the right bytes to produce a valid CRC32 collision would be undetected.
- **No structural verification.** RocksDB doesn't walk the LSM tree structure to verify that all referenced SST files exist and are readable. The manifest tracks file references, but there is no single command that verifies end-to-end reachability from the manifest through every key.
- **CRC32 weakness.** Many RocksDB checksum paths still use CRC32 (32-bit), which has a collision probability of ~1 in 4 billion. PsiTri uses XXH3-64 for segment sync checksums (1 in ~18 quintillion collision probability). Object checksums store the lower 16 bits of an XXH3-64 hash (~1 in 65,536 collision probability) -- weaker than CRC32 per-object, but objects are additionally protected by per-key and per-value checksums. The 8-bit per-key/per-value hashes serve primarily as fast lookup filters with corruption detection as a secondary benefit.
- **Compaction can propagate corruption.** If a corrupt block has a valid checksum (due to collision or the corruption happening after checksumming), compaction will read and rewrite the corrupt data into new SST files, preserving the corruption with a fresh valid checksum. PsiTri's copy-on-write model has the same theoretical risk, but the additional per-key and per-value checksums provide a second line of defense.

---

### MDBX (and LMDB)

MDBX is a B+tree with copy-on-write pages and single-writer / multi-reader concurrency. LMDB is its predecessor with a similar design.

| Layer | Mechanism | Granularity |
|-------|-----------|-------------|
| Pages | None by default (MDBX); LMDB has no page checksums | Per page (4 KB) |
| Meta pages | CRC32 on two meta pages | Per database |
| Geometry | Double-buffered meta pages for atomic commit | 2 copies |

**Verification:** `mdbx_chk` walks the B+tree and verifies page structure (parent-child consistency, key ordering, free list consistency). MDBX added optional page checksums in recent versions (`MDBX_PAGECHECK`).

**Key limitations:**

- **No checksums on data pages by default.** MDBX historically relied on the filesystem and hardware for data integrity. The `MDBX_PAGECHECK` option adds page-level checksums but is not enabled by default and has a performance cost.
- **Page-level granularity only.** Even with page checksums, a failure implicates the entire 4 KB page. There's no way to narrow corruption to a specific key or value within the page.
- **No value-level checksums.** If a value is silently corrupted (bit rot) but the page checksum still passes -- or page checksums are disabled -- MDBX cannot detect it.
- **Single-writer means simpler corruption modes.** MDBX's single-writer design eliminates many concurrent corruption scenarios, but it cannot detect silent storage corruption (bit rot) without page checksums enabled.
- **No segment/range-level checksums.** MDBX doesn't have anything equivalent to PsiTri's sync checksums. The meta page CRC32 verifies the database root pointers, but there's no intermediate layer between "is the root valid" and "is this specific page valid."

---

### TidesDB

TidesDB is a newer LSM-tree engine designed for large-scale time-series and general key-value workloads.

| Layer | Mechanism | Granularity |
|-------|-----------|-------------|
| SST files | CRC32 per data block | Per block |
| WAL | CRC32 per entry | Per entry |
| Bloom filters | Per-SST Bloom filter | Per file |

**Key limitations:**

- **Similar to RocksDB's model** with block-level CRC32 checksums. No per-key or per-value verification.
- **Younger codebase** with less battle-tested verification tooling compared to RocksDB or MDBX.
- **No offline structural verification** equivalent to PsiTri's full tree walk or MDBX's `mdbx_chk`.

---

### Broader Industry Comparison

#### SQLite

SQLite offers optional per-page checksums via the `checksum` VFS shim and its `PRAGMA integrity_check`:

- **`PRAGMA integrity_check`**: walks every table and index B-tree, verifying page structure, key ordering, and free list consistency. Does not verify data checksums -- it verifies structural invariants.
- **Checksum VFS**: adds a CRC32 per page when enabled. Not on by default.
- **WAL checksums**: CRC32 per WAL frame, verified on recovery.

SQLite's `integrity_check` is analogous to PsiTri's Level 5 (tree structure verification) but lacks the per-key/per-value checksum layers.

#### PostgreSQL

PostgreSQL has the most mature integrity infrastructure of any open-source relational database:

- **`data_checksums`** (init option): CRC32 per 8 KB data page, verified on every page read. Must be enabled at cluster creation time.
- **WAL**: CRC32 per WAL record.
- **`pg_verify_checksums`** / `pg_checksums`: offline tool to verify all page checksums.
- **`amcheck` extension**: walks B-tree indexes verifying structural invariants (parent-child consistency, key ordering, sibling links).

PostgreSQL's page checksums are verified on every read (not just during verification), which catches corruption early but adds overhead. PsiTri's object checksums can also be verified on read paths, but the dedicated `verify` command performs the exhaustive offline check.

#### BoltDB / bbolt

BoltDB (used by etcd) is a B+tree with copy-on-write, similar in spirit to MDBX:

- **No page checksums.** Relies entirely on the filesystem.
- **`bbolt check`**: walks the B+tree verifying structural consistency (reachability, key ordering, page type correctness).
- **`bbolt surgery`**: tools for inspecting and repairing damaged databases.

BoltDB demonstrates the risk of relying on filesystem integrity alone -- etcd has experienced data corruption incidents that page checksums would have caught earlier.

#### ZFS / Btrfs (Filesystem Level)

Copy-on-write filesystems provide their own integrity layer below the database:

- **Per-block checksums** (SHA-256 or Fletcher-4 in ZFS) verified on every read.
- **Merkle tree** structure: parent blocks checksum their children, creating a root-to-leaf verification chain.
- **Automatic scrubbing**: background process that reads and re-verifies all data.
- **Self-healing**: with redundancy (mirror/RAIDZ), corrupt blocks can be transparently repaired.

When running on ZFS, the filesystem provides the equivalent of PsiTri's Levels 1 and 2 (range and object checksums). However, database-level checksums remain valuable because they catch corruption that happens *above* the filesystem -- bugs in the database engine, memory corruption before writing, or application-level data corruption.

---

### Summary Comparison

| Feature | PsiTri | RocksDB | MDBX | SQLite | PostgreSQL |
|---------|--------|---------|------|--------|------------|
| **Segment/range checksums** | XXH3-64 per sync range | -- | -- | -- | -- |
| **Object/page checksums** | 16-bit (XXH3-64 truncated) per object | CRC32 per block | Optional per page | Optional per page | CRC32 per 8KB page |
| **Per-key checksums** | 8-bit (XXH3-64 truncated) per key | -- | -- | -- | -- |
| **Per-value checksums** | 8-bit (XXH3-64 truncated) per value | -- | -- | -- | -- |
| **WAL checksums** | N/A (no WAL) | CRC32 per record | -- | CRC32 per frame | CRC32 per record |
| **Structural verification** | Full tree walk | Partial (SST-level) | `mdbx_chk` tree walk | `integrity_check` | `amcheck` B-tree walk |
| **Failure localization** | Per-key + hex prefix | Per-block | Per-page | Per-page | Per-page |
| **Checksum levels** | 5 | 2-3 | 1-2 | 2-3 | 3 |
| **Implicit redundancy** | COW prior copies in compacted segments | Multiple SST levels (lost on compaction) | None (in-place COW overwrites) | None (in-place update) | None (in-place update) |
| **Performance cost** | Zero (checksums are write-once or dual-purpose) | Low (CRC32 per block write) | Optional (page checksums add overhead) | Optional | Moderate (`data_checksums` on every read) |
| **Offline verify tool** | `psitri-tool verify` | `ldb verify_checksum` | `mdbx_chk` | `PRAGMA integrity_check` | `pg_checksums` + `amcheck` |

### Why Five Levels Matter

#### The End-to-End Argument

The theoretical foundation for layered checksums is the **end-to-end argument** (Saltzer, Reed, Clark, 1984): reliability mechanisms at lower layers of a system can reduce but never eliminate the need for checks at the endpoints. A page-level checksum is a "middle layer" check -- it validates that the storage layer delivered what was written, but it cannot detect corruption that happened *before* the write (software bugs, memory errors) or *between* fields within a valid page.

Per-key and per-value checksums are **end-to-end checks** from the application's perspective. They are computed at the moment data enters the database and verified at the moment it is read back or audited. Every layer of the stack between those two points -- the trie engine, the allocator, the memory-mapped I/O, the filesystem, the storage controller -- is covered.

#### Physical vs. Logical Corruption

The critical distinction that page-level checksums miss:

- **Physical corruption**: bits flip on storage media. Page/block checksums catch this reliably.
- **Logical corruption**: the database engine has a bug and writes structurally valid but semantically wrong data. The page checksum passes because the engine wrote a "correct" page -- it just contains the wrong data.

Studies from CERN, Google, and Facebook have found that **logical corruption is more common than physical corruption** in modern systems with ECC memory and enterprise storage. Per-key checksums are the only defense against this class of failure.

#### Real-World Incidents

The argument for fine-grained checksums is not theoretical. Multiple production databases have suffered corruption that page-level checksums failed to catch:

**etcd / BoltDB.** BoltDB's page-level CRC32 checksums did not catch intra-page corruption caused by bugs in B-tree rebalancing logic. Pages were structurally valid, but individual key-value pairs within them were corrupted -- keys pointing to wrong values, or values silently truncated. This led to the bbolt fork adding additional structural consistency checks beyond page checksums.

**CockroachDB.** After real production corruption incidents where RocksDB's block-level checksums were insufficient to catch bugs in the MVCC transaction layer, CockroachDB added per-key MVCC checksums on top of the storage engine. Block checksums validated the physical blocks; the per-key checksums caught logical corruption from higher layers.

**TiKV / TiDB.** TiKV added per-key-value checksum support explicitly because RocksDB's block checksums don't protect against bugs in TiKV's transaction layer that might write wrong values for correct keys.

**RocksDB.** Facebook documented multiple incidents where compaction produced corrupt SST files with valid block checksums but violated key ordering or wrong key-value associations. These are exactly the class of bugs that per-KV checksums catch but block checksums cannot.

**PostgreSQL.** Heap corruption caused by multixact bugs produced pages that were structurally valid (checksums passed) but contained wrong visibility information. Core developers have acknowledged on pgsql-hackers that page-level checksums are a "minimum viable" solution.

**MySQL / InnoDB.** Percona documented client incidents where InnoDB pages were logically corrupted despite checksums being enabled -- approximately 0.1% of hosted MySQL instances experienced at least one corrupted page per year, some undetected by page checksums because they were logical corruptions from InnoDB bugs.

#### Industry Trend

The databases that have added per-key/value checksums -- FoundationDB (from inception), CockroachDB, TiKV -- all did so after real production incidents proved that page-level checksums were insufficient. PsiTri builds this protection in from day one rather than retrofitting it after a production failure.

#### Failure Modes by Level

| Failure | Level 1 (Segment) | Level 2 (Object) | Level 3 (Key) | Level 4 (Value) | Level 5 (Structure) |
|---------|:-:|:-:|:-:|:-:|:-:|
| Torn write from power loss | **catches** | catches | -- | -- | -- |
| Bit rot on storage media | catches | **catches** | catches | catches | -- |
| Engine bug writes wrong value | -- | -- | -- | **catches** | -- |
| Engine bug corrupts node structure | -- | catches | -- | -- | **catches** |
| Dangling pointer from incomplete COW | -- | -- | -- | -- | **catches** |
| Memory error before write | -- | -- | **catches** | **catches** | -- |

Each level catches failures that other levels cannot. Segment checksums catch range-level tears that individual object checksums straddle. Object checksums catch per-node corruption that segment checksums average over. Key and value checksums catch per-field corruption within otherwise valid nodes -- including logical corruption from software bugs, which is the most common class of corruption in modern systems.

## Copy-on-Write as Implicit Redundancy

PsiTri's copy-on-write design provides an often-overlooked integrity benefit: **old versions of objects survive in segments that have been compacted but not yet recycled.**

When a node is modified, the new version is written to a fresh location and the old version's segment is marked for compaction. But the old data remains physically present in the segment until that segment is fully recycled -- which may not happen for a significant period, especially under moderate write loads. This means:

- **Recently modified data has at least two physical copies**: the current version in the active segment and the prior version in the compacted segment.
- **Frequently modified (hot) data may have many copies** across multiple generations of compacted segments.
- **If the current version is corrupt, a verification pass can scan compacted segments** for prior copies of the same object (matched by `ptr_address`). The most recent uncorrupted copy can be promoted back to the live tree.

This is not RAID-style redundancy -- it's a probabilistic, temporal redundancy that falls out naturally from COW + deferred segment recycling. The probability of recovery depends on how recently the data was modified and how aggressively the compactor recycles segments. But for the most common corruption scenario -- damage to hot, recently-written data -- the odds of finding an intact prior copy are meaningful.

No other database in the comparison set offers this property. In-place-update databases (MDBX, PostgreSQL, SQLite) destroy the old version on write. LSM-trees (RocksDB, TidesDB) do maintain multiple versions across levels, but compaction merges and discards old versions -- there is no guarantee that a prior copy survives independently. PsiTri's segment-based compaction preserves complete, independently-checksummed copies until the segment is recycled.

## Repair Strategies

The verification result captures enough context per failure to drive targeted repair without re-scanning the database. Future `psitri-tool repair` modes include:

| Mode | What it fixes | Data loss |
|------|--------------|-----------|
| **`--repair=hashes`** | Recompute key hashes and value checksums from intact data | None |
| **`--repair=prune`** | Remove dangling pointers and corrupt nodes | Keys under pruned subtrees |
| **`--repair=rebuild`** | Harvest readable keys from corrupt subtrees, rebuild | Only unreadable leaves |
| **`--repair=recover`** | Scan compacted segments for prior copies of corrupt objects | None if prior copy found |
| **`--repair=salvage`** | Full segment scan for any readable data, rebuild tree | Only truly unreadable data |

Each failure record includes the node address, physical location, key prefix, root index, and failure type -- everything needed for surgical repair of individual nodes without walking the entire tree again.
