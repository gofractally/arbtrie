# Data Integrity & Verification

PsiTri implements five independent checksum levels that protect data from the segment layer up through individual key-value pairs. Together they detect corruption at any granularity -- from a single flipped bit in a value to a torn segment write -- and pinpoint exactly which keys are affected.

Most databases offer one or two integrity mechanisms. PsiTri's layered approach means that a failure at one level doesn't prevent verification at others: a corrupt inner node still has individually checksummed children, and a failed segment sync checksum doesn't invalidate the object-level checksums within it.

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
- **CRC32 weakness.** Many RocksDB checksum paths still use CRC32 (32-bit), which has a collision probability of ~1 in 4 billion. PsiTri uses XXH3-64 for segment and object checksums (1 in ~18 quintillion), and XXH3-derived 8-bit hashes for per-key/per-value checks where the primary function is fast filtering with corruption detection as a secondary benefit.
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
| **Segment/range checksums** | XXH3-64 | -- | -- | -- | -- |
| **Object/page checksums** | XXH3-16 per object | CRC32 per block | Optional per page | Optional per page | CRC32 per 8KB page |
| **Per-key checksums** | XXH3-8 per key | -- | -- | -- | -- |
| **Per-value checksums** | XXH3-8 per value | -- | -- | -- | -- |
| **WAL checksums** | N/A (no WAL) | CRC32 per record | -- | CRC32 per frame | CRC32 per record |
| **Structural verification** | Full tree walk | Partial (SST-level) | `mdbx_chk` tree walk | `integrity_check` | `amcheck` B-tree walk |
| **Failure localization** | Per-key + hex prefix | Per-block | Per-page | Per-page | Per-page |
| **Checksum levels** | 5 | 2-3 | 1-2 | 2-3 | 3 |
| **Offline verify tool** | `psitri-tool verify` | `ldb verify_checksum` | `mdbx_chk` | `PRAGMA integrity_check` | `pg_checksums` + `amcheck` |

### Why Five Levels Matter

The value of layered checksums becomes clear when you consider failure modes:

1. **A single value is silently corrupted** (bit rot in storage). Page-level databases can't pinpoint which key is affected -- they report a bad page containing potentially hundreds of keys. PsiTri's Level 4 identifies the exact key and value.

2. **An inner node's branch pointer is corrupted**, but the children are fine. Page-level databases must either trust the whole page or discard it. PsiTri detects the dangling pointer (Level 5), reports the affected key prefix, and the children may still be individually verified and salvaged.

3. **A power loss tears a write across segment boundaries.** PsiTri's sync checksums (Level 1) identify exactly which byte ranges are suspect, and the object checksums (Level 2) within those ranges can distinguish which specific objects were damaged.

4. **Corruption happens above the filesystem** -- a memory error corrupts data before it's written, or a bug writes garbage to a value. Filesystem-level checksums (ZFS) won't catch this because the "correct" checksum was computed over the already-corrupt data. PsiTri's per-key and per-value checksums, computed at insertion time, detect this on the next verification pass.

The general principle: **each checksum level catches failures that the levels above and below it cannot.** Segment checksums catch range-level tears that individual object checksums straddle. Object checksums catch per-node corruption that segment checksums average over. Key and value checksums catch per-field corruption within otherwise valid nodes.

## Repair Strategies

The verification result captures enough context per failure to drive targeted repair without re-scanning the database. Future `psitri-tool repair` modes include:

| Mode | What it fixes | Data loss |
|------|--------------|-----------|
| **`--repair=hashes`** | Recompute key hashes and value checksums from intact data | None |
| **`--repair=prune`** | Remove dangling pointers and corrupt nodes | Keys under pruned subtrees |
| **`--repair=rebuild`** | Harvest readable keys from corrupt subtrees, rebuild | Only unreadable leaves |
| **`--repair=salvage`** | Full segment scan for any readable data, rebuild tree | Only truly unreadable data |

Each failure record includes the node address, physical location, key prefix, root index, and failure type -- everything needed for surgical repair of individual nodes without walking the entire tree again.
