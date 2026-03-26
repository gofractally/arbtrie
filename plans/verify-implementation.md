# Verify Command: Full Integrity Verification

## Goal

Offline-only command that verifies all checksum levels and identifies affected keys when corruption is found.

## Five Checksum Levels

| Level | Field | Zero means | Scope |
|-------|-------|-----------|-------|
| Segment sync | `sync_header._sync_checksum` | unknown (configurable) | Byte range between two sync points |
| Object | `alloc_header._checksum` | unknown (configurable) | Individual allocated object |
| Key hash | `leaf_node._key_hashs[]` | valid hash (always checked) | Per key in leaf |
| Value checksum | `leaf_node::value_data._checksum` | valid hash (always checked) | Per inline value in leaf |
| Tree structure | child pointer resolution | N/A | Every child pointer resolves to valid node |

## Failure Impact Tracing

When a checksum fails, we need to report which keys are affected:

- **Inner node object checksum fails** → ALL keys under that subtree are suspect. Report the key prefix accumulated to that point. Do NOT descend further (structure is untrusted).
- **Leaf node object checksum fails** → ALL keys in that leaf are suspect. Report each key.
- **Specific key hash fails** → Report that specific key.
- **Specific value checksum fails** → Report that specific key.
- **Segment sync checksum fails** → Report affected byte range and segment number. Cross-reference with tree walk to identify which objects/keys fall in that range.
- **Dangling pointer** → child address doesn't resolve. Report key prefix + branch byte that leads to the missing child.

## Algorithm

### Pass 1: Segment Sync Checksum Verification

Walk each segment's sync_header chain (via `_last_aheader_pos` / `prev_aheader_pos()`):
- For each sync_header with `sync_checksum() != 0`:
  - Recompute `XXH3_64bits(seg->data + start_checksum_pos, checksum_offset - start_checksum_pos)`
  - Compare with stored `sync_checksum()`
  - Record failures: `{segment_number, sync_header_pos, start_checksum_pos, end_pos}`

### Pass 2: Tree Walk with Key Path Tracking

Recursive walk from all roots, accumulating a key prefix (`std::string`) as we descend:

```
verify_node(address, key_prefix, results):
  1. Resolve address → control_block → location → alloc_header*
     - If address doesn't resolve → record dangling pointer failure with key_prefix
     - Return (don't descend)

  2. Check visited set (prevent cycles in DAGs with shared nodes)
     - If already visited, skip (checksums already checked on first visit)

  3. Verify object checksum: alloc_header::verify_checksum()
     - checksum == 0 → increment unknown count
     - checksum != 0 && mismatch → record failure {address, node_type, key_prefix}
     - If object checksum FAILS on inner/leaf node → mark all descendant keys affected
       but still attempt to descend (structure may still be navigable)

  4. Dispatch by node type:

     inner_node:
       - For each branch b (0..num_branches-1):
         - child_prefix = key_prefix + division_byte(b)
         - child_addr = get_branch(b)
         - verify_node(child_addr, child_prefix, results)

     inner_prefix_node:
       - For each branch b:
         - child_prefix = key_prefix + prefix() + division_byte(b)
         - child_addr = get_branch(b)
         - verify_node(child_addr, child_prefix, results)

     leaf_node:
       - For each branch b (0..num_branches-1):
         - full_key = get_key(b)  // leaf stores complete remaining key suffix
         - Verify key hash: calc_key_hash(full_key) vs key_hashs()[b]
           - Mismatch → record {full_key, "key_hash"}
         - Verify value checksum (if inline): value_data::is_valid()
           - Mismatch → record {full_key, "value_checksum"}
         - If value is address type (value_node or subtree):
           - verify_node(value_addr, key_prefix + full_key, results)

     value_node:
       - If is_subtree: verify_node(child_addr, key_prefix, results)
       - No separate value checksum (covered by object checksum)
```

### Key Prefix Reconstruction

At each inner node level, we need the divider byte that was used to route to the child. The divisions array partitions the byte space:
- Branch 0: bytes [0, divisions[0])
- Branch i: bytes [divisions[i-1], divisions[i])
- Branch N-1: bytes [divisions[N-2], 255]

For reporting purposes, we store the first byte of the range as the prefix contribution. This gives enough context to identify the subtree.

Actually — the key byte at this level IS the division byte. When descending, the byte that routed us to branch `b` is:
- `b == 0` → byte range starts at 0
- `b > 0` → byte is `divisions[b-1]`

For reporting affected keys, we report the prefix as a hex string, which identifies the subtree.

## Output Structure

```
verify_result {
    // Counters
    struct { uint64_t passed, failed, unknown; } segment_checksums;
    struct { uint64_t passed, failed, unknown; } object_checksums;
    struct { uint64_t passed, failed; } key_checksums;     // no unknown
    struct { uint64_t passed, failed; } value_checksums;   // no unknown

    uint64_t nodes_visited;
    uint64_t reachable_bytes;
    uint64_t dangling_pointers;
    uint32_t roots_checked;

    // Detailed failures
    struct segment_failure {
        uint32_t segment;
        uint32_t sync_pos;
        uint32_t range_start;
        uint32_t range_end;
    };

    struct node_failure {
        ptr_address address;
        header_type node_type;
        std::string key_prefix_hex;   // hex-encoded prefix leading to this node
        std::string failure_type;     // "object_checksum", "dangling_pointer"
    };

    struct key_failure {
        std::string key_hex;          // hex-encoded full key
        std::string failure_type;     // "key_hash", "value_checksum"
        uint32_t    root_index;
    };

    std::vector<segment_failure> segment_failures;
    std::vector<node_failure>    node_failures;
    std::vector<key_failure>     key_failures;
};
```

## Files to Modify

1. **`libraries/sal/include/sal/verify.hpp`** (NEW) — `verify_result` struct and `verify_options`
2. **`libraries/sal/include/sal/allocator.hpp`** — Add `verify_result verify()` method
3. **`libraries/sal/src/allocator.cpp`** — Implement segment sync checksum pass
4. **`libraries/psitri/include/psitri/database.hpp`** — Add `verify_result verify()` forwarding
5. **`libraries/psitri/src/database.cpp`** — Implement tree walk verify (needs psitri node knowledge)
6. **`programs/psitri_tool.cpp`** — Add `verify` command with formatted output

## Design Decision: Where Does the Tree Walk Live?

The segment sync checksum pass is pure SAL — no node knowledge needed. Lives in `allocator.cpp`.

The tree walk with key/value verification needs psitri node types (inner_node, leaf_node, value_node). Two options:

**Option A**: Put tree walk in `database.cpp` (psitri layer), call allocator for segment pass.
**Option B**: Use vtable dispatch in SAL (like reachable_size), add verify_node virtual.

Option A is simpler — verify is already an offline/exclusive operation, no need for the abstraction. The allocator provides `verify_segments()` for pass 1, and `database::verify()` does pass 2 using node-level knowledge.

## CLI Output Format

```
$ psitri-tool verify /path/to/db

── Segment Sync Checksums ──
  Checked               42
  Passed                38
  Failed                 0
  Not checksummed        4

── Object Checksums ──
  Reachable objects  125,432
  Passed             120,100
  Failed                   0
  Not checksummed      5,332

── Key Hashes ──
  Keys checked        98,200
  Passed              98,200
  Failed                   0

── Value Checksums ──
  Values checked      98,200
  Passed              98,200
  Failed                   0

── Tree Structure ──
  Roots checked         1 / 512
  Nodes visited     125,432
  Reachable size    560.2 MB
  Dangling pointers       0

✓ Database integrity verified.
```

On failure:
```
── Failures ──
  FAIL: object checksum at address 0x1a3f (inner_prefix_node)
        Affected keys: all keys with prefix 0x0a3f2b...
  FAIL: key hash mismatch at key 0x0a3f2b4e...
        Root: 0
  FAIL: value checksum mismatch at key 0x0a3f2b4e...
        Root: 0
  FAIL: segment 17 sync checksum at pos 32768-65472

✗ 4 integrity failures found.
```

## Repair Context: What the Verify Result Enables

The verify result captures enough information to offer targeted repair options
rather than "database is broken, start over." Each failure type has a repair path:

### Repair Strategies by Failure Type

| Failure | Repair Strategy | Data Loss |
|---------|----------------|-----------|
| **Key hash mismatch** | Recompute hash from key data, patch in place | None — key data likely intact, hash is derived |
| **Value checksum mismatch** | Option A: recompute checksum (if data intact). Option B: null out value, preserve key. Option C: delete key entirely | None / value only / key+value |
| **Inner node object checksum** | Rebuild node from children (children may be intact). Walk children to harvest surviving keys, re-insert into a repaired subtree | Depends on child integrity |
| **Leaf node object checksum** | Attempt to read individual keys/values (some may survive). Re-insert surviving entries into a new leaf | Partial — only unreadable entries lost |
| **Dangling pointer** | Prune the branch from parent node. Keys in that subtree are lost unless recoverable from segment scan | Subtree below dangling pointer |
| **Segment sync checksum** | Objects in that range are suspect but may pass individual object checksums. Only objects that ALSO fail object checksum need repair | None if object checksums pass |
| **Value node object checksum** | Null out the value for the parent key, or delete the key | Value only / key+value |

### Key Design Principle

The failure records contain enough to drive repair without re-scanning:

- **`node_failure.address`** — can locate and patch/rebuild the specific node
- **`node_failure.key_prefix_hex`** — identifies the affected subtree for targeted rebuild
- **`key_failure.key_hex`** — exact key for surgical value repair or deletion
- **`key_failure.root_index`** — which root tree contains the failure
- **`segment_failure`** range — can cross-reference with object locations

### Repair Modes (future `psitri-tool repair`)

1. **`--repair=hashes`** — Recompute all key hashes and value checksums. Zero data loss.
   Fixes: key hash mismatches, value checksum mismatches where data is intact.

2. **`--repair=prune`** — Remove corrupt nodes and dangling pointers. Preserves rest of tree.
   Fixes: dangling pointers, unreadable inner/leaf nodes.
   Loss: keys under pruned subtrees.

3. **`--repair=rebuild`** — Harvest all readable keys from corrupt subtrees, rebuild.
   Fixes: corrupt inner nodes where leaf children survive.
   Loss: only keys in unreadable leaf nodes.

4. **`--repair=salvage`** — Full segment scan for any readable key/value pairs, rebuild tree.
   Last resort: walks all segments (not just reachable tree), recovers anything readable.
   Loss: only truly unreadable data.

### Information Needed for Each Repair Path

For repair to work without re-scanning, verify must capture:

```
node_failure {
    ptr_address address;          // locate the node
    location    loc;              // segment + offset for in-place patching
    header_type node_type;        // dispatch repair strategy
    uint32_t    root_index;       // which root tree
    std::string key_prefix_hex;   // affected key range
    std::string failure_type;     // "object_checksum", "dangling_pointer"
    bool        children_reachable; // true if we successfully descended past this node
};

key_failure {
    std::string key_hex;          // exact key
    uint32_t    root_index;       // which root tree
    ptr_address leaf_address;     // the leaf containing this key
    location    leaf_loc;         // segment + offset of leaf
    uint16_t    branch_index;     // position within leaf
    std::string failure_type;     // "key_hash", "value_checksum"
};
```

This gives a future `repair` command everything it needs to surgically fix
individual nodes without walking the entire tree again.
