# Multi-Root DWAL Transactions

## Motivation

PsiTri supports 512 independent top-level roots, each functioning as an
independent key-value namespace (analogous to tables). The DWAL layer provides
buffered writes with WAL durability, making micro-transactions fast by avoiding
per-transaction COW overhead.

Today, each DWAL transaction operates on exactly one root. This means there is
no way to atomically commit writes across multiple roots. Any database that
wants table-level write parallelism with cross-table transactions needs this.

## Design Overview

A new `dwal::transaction` class coordinates one `dwal_transaction` per
participating root. Roots are declared upfront (write set + read set) and
locked in sorted order to prevent deadlocks. Read-only roots take shared locks;
write roots take exclusive locks. Commit is atomic across all write roots via
linked WAL entries with a shared transaction ID.

## API

```cpp
namespace psitri::dwal {

class transaction {
public:
    /// Per-root handle. Exposes mutations (write roots only) and reads (all roots).
    class root_handle {
    public:
        // Mutations — assert writable
        void upsert(std::string_view key, std::string_view value);
        void upsert_subtree(std::string_view key, sal::ptr_address addr);
        bool remove(std::string_view key);
        void remove_range(std::string_view low, std::string_view high);

        // Reads — always allowed (RW → RO → Tri layered lookup)
        dwal_transaction::lookup_result get(std::string_view key) const;
        owned_merge_cursor cursor() const;

        bool     writable() const noexcept;
        uint32_t index() const noexcept;
    };

    // Access a root that was declared at construction time.
    // Asserts/throws if root_index was not in the initial write or read set.
    root_handle& root(uint32_t root_index);

    // Convenience: operate on a root directly.
    void upsert(uint32_t root, std::string_view key, std::string_view value);
    bool remove(uint32_t root, std::string_view key);
    dwal_transaction::lookup_result get(uint32_t root, std::string_view key);

    // Transaction control
    void commit();
    void abort();

    bool is_committed() const noexcept;
    bool is_aborted() const noexcept;
};

// On dwal_database:
transaction start_transaction(std::initializer_list<uint32_t> write_roots,
                              std::initializer_list<uint32_t> read_roots = {});

transaction start_transaction(uint32_t root_index);  // convenience, single write root
```

### Usage

```cpp
auto tx = db.start_transaction({0, 1},  // write roots
                               {3});    // read roots

auto& users  = tx.root(0);  // writable
auto& orders = tx.root(1);  // writable
auto& config = tx.root(3);  // read-only

users.upsert("user:42", user_data);
orders.upsert("order:100", order_data);

auto setting = config.get("setting:x");  // shared lock, doesn't block other readers

tx.commit();  // atomic across roots 0 and 1; root 3 just releases shared lock
```

## Locking

### Deadlock Prevention

Roots are locked at transaction construction time in ascending root index order.
This is a total order — no two transactions can deadlock regardless of which
roots they access.

Write roots acquire exclusive locks (`tx_mutex.lock()`).
Read roots acquire shared locks (`tx_mutex.lock_shared()`).

A `std::shared_mutex tx_mutex` is added to `dwal_root`.

### Lock Release

All locks are released on commit or abort (including destructor-abort).
Lock release order does not matter for correctness.

## WAL Format Changes

### Entry Header (v2)

The WAL entry header is extended from 14 bytes to 25 bytes:

```
Offset  Size   Field
[0..4)   u32   entry_size
[4..12)  u64   sequence
[12..14) u16   op_count
[14..15) u8    entry_flags        — NEW: bit 0 = multi_tx_commit
[15..23) u64   multi_tx_id        — NEW: 0 = single-root entry
[23..25) u16   multi_participant_count — NEW: 0 = single-root entry
[25..N-8)      operations[]
[N-8..N) u64   xxh3_64 hash
```

The `wal_version` remains 1. The new fields occupy space that was previously
implicit (the operations started immediately after op_count). Readers detect
v2 entries by checking `entry_size >= 25 + 8` (v1 entries have header size 14).

**Alternative (simpler):** Bump `wal_version` to 2. Old readers reject new
files; new readers handle both versions. Since WAL files are ephemeral (deleted
after merge), version coexistence is not a concern.

### Entry Flags

```
bit 0: multi_tx_commit — this entry is the final participant in a multi-root tx
bits 1-7: reserved (0)
```

### Single-Root Entries (Backward Compatible)

Single-root transactions write `entry_flags = 0`, `multi_tx_id = 0`,
`multi_participant_count = 0`. The commit is implicit (every single-root entry
is self-committed). This is the fast path — no behavioral change from today.

### Multi-Root Entries

All participants in a multi-root transaction share the same `multi_tx_id`
(monotonically increasing, generated by `dwal_database::next_multi_tx_id()`).
Each entry carries `multi_participant_count` = total number of write roots.
The last entry written (highest root index) sets `multi_tx_commit = 1`.

## Commit Protocol

### Single-Root (Fast Path)

Unchanged from today:

1. `wal_writer::commit_entry()` — write ops + hash to WAL buffer
2. Discard undo log
3. Optional swap check

### Multi-Root

```
for each write root (sorted ascending):
    wal_writer::commit_entry_multi(tx_id, participant_count, is_last)
    → writes entry with multi_tx_id, participant_count
    → last entry sets multi_tx_commit flag
    undo_log::discard()

for each write root:
    check should_swap() → try_swap_rw_to_ro()

release all locks (write + read)
```

### Abort

```
for each write root (reverse order):
    dwal_transaction::abort()
    → replay undo log, discard WAL entry

release all locks (write + read)
```

Read-only roots: just release the shared lock. No undo, no WAL.

## Recovery

### Algorithm

Recovery already iterates all `root-N/` WAL directories. The extension:

```
Phase 0: Scan all RW WAL files, collect multi-tx metadata.
         Build: map<multi_tx_id → {participant_count, entries_found, commit_seen}>

Phase 1: Replay RO WALs into Tri (unchanged — RO WALs never contain multi-tx
         entries because swap only happens after commit).

Phase 2: Replay RW WALs into RW btrees.
         For each entry:
           if multi_tx_id == 0:
             → single-root: replay as today
           else:
             → check multi_tx_index:
               if commit_seen AND entries_found == participant_count:
                 → replay (complete multi-tx)
               else:
                 → skip (incomplete multi-tx, effectively aborted)

Phase 2 can be done in a single pass if the multi-tx index is built first
(Phase 0), then entries are filtered during replay.
```

### Crash Scenarios

| Crash point | WAL state | Recovery action |
|---|---|---|
| Before any commit_entry_multi | No multi-tx entries | Nothing to do |
| After some but not all entries written | Partial entries, no COMMIT flag | Discard all entries for this tx_id |
| After last entry (with COMMIT) written | All entries present | Replay all |
| During flush_wal (partial fsync) | Some entries on disk, some not | entries_found < participant_count → discard |

### fsync Ordering

WAL writes go to the OS page cache without fsync. The application calls
`flush_wal()` periodically for durability. On power loss, unfsynced entries may
be lost — this is the same durability model as single-root transactions.

For kernel crashes (no power loss), all page-cache writes survive. Since all
`commit_entry_multi()` calls happen in the same thread before any lock is
released, the kernel sees them in order.

The `flush_wal()` function fsyncs all root WAL files. After it returns, either
all multi-tx entries are durable or none are (approximately — the kernel's
dirty page writeback may flush some files before the explicit fsync, but this
only matters for power loss, which already has the "unfsynced = maybe lost"
contract).

## Per-Root Independence

The swap/merge pipeline is unaffected. Each root swaps and merges independently:

- `try_swap_rw_to_ro()` — per-root, called after commit for each write root
- Merge pool — drains each root's RO btree into Tri independently
- WAL rotation — per-root, triggered by swap

A multi-root transaction that writes to roots 0, 1, and 5 may trigger swaps on
any subset of those roots. The merge pipeline doesn't need cross-root coordination.

## What This Design Does Not Include

- **Cross-root read snapshots**: Readers see each root independently. A reader
  might see the committed state of root 0 but a pre-commit state of root 1 if
  reading between individual root swaps. Consistent cross-root read snapshots
  would require a global generation counter + epoch-based retention.

- **Sub-transactions (savepoints)**: The existing `dwal_transaction::sub_transaction()`
  still works per-root. A multi-root savepoint would push undo frames on all
  participating roots. Not included in the initial implementation.

- **Direct-mode fallback**: Large multi-root transactions that should bypass
  DWAL buffering are not yet supported.

## Files Changed

| File | Change |
|---|---|
| `include/psitri/dwal/wal_format.hpp` | New entry header size, flags, multi-tx fields |
| `include/psitri/dwal/wal_writer.hpp` | Add `commit_entry_multi()` |
| `src/dwal/wal_writer.cpp` | Implement `commit_entry_multi()` |
| `include/psitri/dwal/wal_reader.hpp` | Expose multi-tx fields on `wal_entry` |
| `src/dwal/wal_reader.cpp` | Parse new header fields |
| `include/psitri/dwal/dwal_root.hpp` | Add `root_mode` enum, `tx_mutex` |
| `include/psitri/dwal/dwal_transaction.hpp` | Add `root_mode` param, `commit_multi()` |
| `src/dwal/dwal_transaction.cpp` | Implement read-only mode, `commit_multi()` |
| **NEW** `include/psitri/dwal/transaction.hpp` | `transaction` + `root_handle` classes |
| **NEW** `src/dwal/transaction.cpp` | Implementation |
| `include/psitri/dwal/dwal_database.hpp` | New `start_transaction()` overloads, `next_multi_tx_id()` |
| `src/dwal/dwal_database.cpp` | Implement new API, update recovery |
| `CMakeLists.txt` | Add `src/dwal/transaction.cpp` |
| `tests/dwal_tests.cpp` | Multi-root transaction tests |
