#pragma once
#include <psitri/dwal/dwal_read_session.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/epoch_lock.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/merge_pool.hpp>
#include <psitri/dwal/transaction.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>

namespace psitri
{
   class database;
   class write_session;
   class read_session;
}  // namespace psitri

namespace psitri::dwal
{
   /// Configuration for the DWAL layer.
   struct dwal_config
   {
      /// Maximum RW btree size (entries) before triggering a swap.
      uint32_t max_rw_entries = 100'000;

      /// Maximum WAL file size (bytes) before triggering a swap.
      uint64_t max_wal_bytes = 64 * 1024 * 1024;  // 64 MB

      /// Idle flush interval — RW btrees untouched for this long are swapped.
      /// Zero disables time-based flushing.
      std::chrono::milliseconds idle_flush_interval{1000};

      /// Number of merge threads in the pool.
      uint32_t merge_threads = 2;
   };

   /// DWAL database — wraps a PsiTri database with buffered write-ahead logging.
   ///
   /// Provides the same logical API as psitri::database but routes writes through
   /// a per-root RW btree backed by a WAL file. Under low write pressure, the
   /// btree is small and drains quickly; under high pressure, it batches writes
   /// for amortized COW cost.
   ///
   /// Each root (0-511) has independent state: its own btree, WAL file, undo log,
   /// mutex, and RO slot. Operations on different roots never contend.
   class dwal_database
   {
     public:
      /// Create a DWAL database wrapping an existing PsiTri database.
      /// WAL files are stored in wal_dir (defaults to db_dir/wal/).
      dwal_database(std::shared_ptr<psitri::database> db,
                    std::filesystem::path              wal_dir,
                    dwal_config                        cfg = {});

      ~dwal_database();

      dwal_database(const dwal_database&)            = delete;
      dwal_database& operator=(const dwal_database&) = delete;

      // ── Transactions ──────────────────────────────────────────────

      /// Start a buffered write transaction on a root (legacy single-root API).
      /// Acquires the per-root exclusive lock.
      dwal_transaction start_write_transaction(uint32_t         root_index,
                                               transaction_mode mode = transaction_mode::buffered);

      /// Start a multi-root transaction. Write roots get exclusive locks;
      /// read roots get shared locks. Locks acquired in sorted index order.
      transaction start_transaction(std::initializer_list<uint32_t> write_roots,
                                    std::initializer_list<uint32_t> read_roots = {});

      /// Convenience: single write root transaction.
      transaction start_transaction(uint32_t root_index);

      /// Generate a monotonically increasing multi-transaction ID.
      uint64_t next_multi_tx_id() noexcept;

      // ── Read Access ───────────────────────────────────────────────

      /// Create a read session with cached snapshots.
      /// One per reader thread. The session caches DWAL snapshots and PsiTri
      /// cursors, refreshing only when the generation changes (after a swap).
      dwal_read_session start_read_session() { return dwal_read_session(*this); }

      /// Single-shot layered lookup of RO + Tri only (no caching — acquires mutex per call).
      /// Does NOT see uncommitted RW data. Prefer start_read_session() for repeated reads.
      dwal_transaction::lookup_result get(uint32_t         root_index,
                                          std::string_view key,
                                          read_mode        mode = read_mode::persistent);

      /// Full layered lookup: RW → RO → Tri.
      /// Sees uncommitted writes in the RW btree. Intended for same-thread
      /// read-after-write (e.g. RocksDB Get() after Put()).
      dwal_transaction::lookup_result get_latest(uint32_t root_index, std::string_view key);

      /// Create a merge cursor over the DWAL layers for iteration.
      /// Locking is handled internally based on the read mode:
      ///   - latest:    RW + RO + Tri (writer-thread only — RW is not locked)
      ///   - buffered:  RO + Tri (acquires buffered_mutex internally)
      ///   - persistent: Tri only (no DWAL locks)
      /// The returned cursor owns shared_ptr copies of the layer snapshots,
      /// so callers do not need to hold any locks during iteration.
      owned_merge_cursor create_cursor(uint32_t root_index, read_mode mode);

      // ── Flush & Swap ──────────────────────────────────────────────

      /// Try to swap the RW btree to RO for a specific root.
      /// Only succeeds if the merge thread has completed (merge_complete == true).
      /// The caller must hold the exclusive rw_mutex lock.
      void try_swap_rw_to_ro(uint32_t root_index);

      /// Legacy entry point — delegates to try_swap_rw_to_ro.
      void swap_rw_to_ro(uint32_t root_index);

      /// Flush all dirty WAL files to disk (F_FULLFSYNC).
      void flush_wal();

      /// Flush all dirty WAL files with explicit sync level.
      void flush_wal(sal::sync_type sync);

      /// Flush a specific root's WAL to disk (F_FULLFSYNC).
      void flush_wal(uint32_t root_index);

      /// Flush a specific root's WAL with explicit sync level.
      void flush_wal(uint32_t root_index, sal::sync_type sync);

      // ── Accessors ─────────────────────────────────────────────────

      std::shared_ptr<psitri::database>& underlying_db() noexcept { return _db; }
      const dwal_config&                 config() const noexcept { return _cfg; }
      dwal_root&                         root(uint32_t index) { return *_roots[index]; }

      /// Access the epoch registry (for session lock allocation).
      epoch_registry& epochs() noexcept { return _epochs; }

      /// Check if a swap should be triggered for a root after a commit.
      bool should_swap(uint32_t root_index) const;

      /// Lazily initialize a root's DWAL state (public for transaction).
      dwal_root& ensure_root_public(uint32_t index) { return ensure_root(index); }

      /// Ensure WAL directory and files exist for a root (public for transaction).
      void ensure_wal_public(uint32_t root_index) { ensure_wal(root_index); }

      /// Tri-layer point lookup — reads directly from the PsiTri COW tree.
      /// Used by dwal_transaction::get() as the final fallback layer.
      /// Thread-safe: uses a thread-local read session internally.
      dwal_transaction::lookup_result tri_get(uint32_t root_index, std::string_view key);

      /// Clear thread-local caches (read sessions, cursors) for the calling thread.
      /// Must be called on each thread that used tri_get() or create_cursor()
      /// before the database is destroyed, to avoid dangling shared_ptr references.
      void clear_thread_local_caches();

      /// Replay any WAL files found on disk to recover from a crash.
      /// Called automatically by the constructor. Safe to call on clean startup
      /// (no WAL files = no-op).
      void recover();

     private:

      /// Replay a single WAL file's entries into the PsiTri COW tree.
      void replay_wal_to_tri(uint32_t root_index, const std::filesystem::path& wal_path);

      /// Replay a single WAL file's entries into a root's RW btree layer.
      /// @deprecated Superseded by inline replay in recover() which handles multi-tx filtering.
      ///             Kept temporarily for existing tests. Remove when tests are migrated.
      void replay_wal_to_rw(uint32_t root_index, const std::filesystem::path& wal_path);

      /// Ensure WAL directory and files exist for a root.
      void ensure_wal(uint32_t root_index);

      std::shared_ptr<psitri::database> _db;
      std::filesystem::path             _wal_dir;
      dwal_config                       _cfg;

      /// Per-root DWAL state. Only roots that are actively used get initialized.
      /// Using unique_ptr to avoid huge array of dwal_root (which has mutex + atomics).
      static constexpr uint32_t  max_roots = 512;
      std::unique_ptr<dwal_root> _roots[max_roots];

      /// Lazily initialize a root's DWAL state.
      dwal_root& ensure_root(uint32_t index);

      /// Monotonically increasing counter for multi-root transaction IDs.
      std::atomic<uint64_t> _next_multi_tx_id{1};

      /// Epoch registry for RO pool reclamation.
      epoch_registry _epochs;

      /// Merge thread pool — drains RO btrees into PsiTri.
      std::unique_ptr<merge_pool> _merge_pool;
   };

}  // namespace psitri::dwal
