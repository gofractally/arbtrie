#pragma once
#include <psitri/dwal/dwal_read_session.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/epoch_lock.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/merge_pool.hpp>
#include <psitri/dwal/transaction.hpp>
#include <psitri/fwd.hpp>
#include <psitri/lock_policy.hpp>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>

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

      /// Maximum time between COW snapshot publications.
      /// When non-zero, the writer publishes prev_root on commit if this
      /// interval has elapsed since the last publication, even if no reader
      /// requested it. This bounds staleness for fresh-mode readers.
      /// Zero (default) disables time-based freshness.
      std::chrono::milliseconds max_freshness_delay{0};

      /// Number of merge threads in the pool.
      uint32_t merge_threads = 2;

      /// Maximum RW arena capacity before the writer blocks waiting for merge.
      /// The ART arena is a bump allocator with uint32_t offsets (4 GB max)
      /// that grows by doubling.  When capacity reaches this limit and the
      /// merge thread hasn't finished, the writer yields until merge completes.
      /// Default: 1 GB — the next doubling goes to 2 GB (safe), and the arena
      /// would need to fill the full 2 GB before attempting a 4 GB growth.
      /// Set to 0 to disable (not recommended).
      uint64_t max_rw_arena_bytes = 1ULL * 1024 * 1024 * 1024;
   };

   namespace detail
   {
      /// Thread-local caches used by dwal_database::tri_get and create_cursor.
      /// Non-template storage — holds type-erased pointers so a single TU
      /// can serve every policy instantiation.
      struct tl_cache_storage
      {
         // Used by create_cursor (Tri layer)
         std::shared_ptr<void> cursor_session;
         std::weak_ptr<void>   cursor_db;

         // Used by tri_get
         std::shared_ptr<void> tri_session;
         std::weak_ptr<void>   tri_db;
         psitri::cursor*       tri_cursor = nullptr;

         void reset()
         {
            cursor_session.reset();
            cursor_db.reset();
            delete tri_cursor;
            tri_cursor = nullptr;
            tri_session.reset();
            tri_db.reset();
         }
      };

      inline bool same_owner(const std::shared_ptr<void>& lhs,
                             const std::shared_ptr<void>& rhs)
      {
         return lhs && rhs && !lhs.owner_before(rhs) && !rhs.owner_before(lhs);
      }

      tl_cache_storage& thread_local_cache();
   }  // namespace detail

   /// DWAL database — wraps a PsiTri database with buffered write-ahead logging.
   ///
   /// Provides the same logical API as psitri::database but routes writes through
   /// a per-root RW btree backed by a WAL file. Under low write pressure, the
   /// btree is small and drains quickly; under high pressure, it batches writes
   /// for amortized COW cost.
   ///
   /// Each root (0-511) has independent state: its own btree, WAL file, undo log,
   /// mutex, and RO slot. Operations on different roots never contend.
   template <class LockPolicy = std_lock_policy>
   class basic_dwal_database
   {
     public:
      using lock_policy_type      = LockPolicy;
      using database_type         = basic_database<LockPolicy>;
      using read_session_type     = basic_read_session<LockPolicy>;
      using write_session_type    = basic_write_session<LockPolicy>;
      using dwal_root_type        = basic_dwal_root<LockPolicy>;
      using epoch_registry_type   = basic_epoch_registry<LockPolicy>;
      using merge_pool_type       = basic_merge_pool<LockPolicy>;
      using dwal_transaction_type = basic_dwal_transaction<LockPolicy>;
      using transaction_type      = basic_transaction<LockPolicy>;
      using read_session_dwal_type = basic_dwal_read_session<LockPolicy>;
      using lookup_result         = typename dwal_transaction_type::lookup_result;

      /// Create a DWAL database wrapping an existing PsiTri database.
      /// WAL files are stored in wal_dir (defaults to db_dir/wal/).
      basic_dwal_database(std::shared_ptr<database_type> db,
                          std::filesystem::path          wal_dir,
                          dwal_config                    cfg = {});

      ~basic_dwal_database();

      basic_dwal_database(const basic_dwal_database&)            = delete;
      basic_dwal_database& operator=(const basic_dwal_database&) = delete;

      // ── Transactions ──────────────────────────────────────────────

      dwal_transaction_type start_write_transaction(
          uint32_t         root_index,
          transaction_mode mode = transaction_mode::buffered);

      transaction_type start_transaction(std::initializer_list<uint32_t> write_roots,
                                         std::initializer_list<uint32_t> read_roots = {});

      transaction_type start_transaction(uint32_t root_index);

      uint64_t next_multi_tx_id() noexcept;

      // ── Read Access ───────────────────────────────────────────────

      read_session_dwal_type start_read_session() { return read_session_dwal_type(*this); }

      lookup_result get(uint32_t root_index, std::string_view key,
                        read_mode mode = read_mode::trie);

      lookup_result get_latest(uint32_t root_index, std::string_view key);

      owned_merge_cursor create_cursor(uint32_t root_index, read_mode mode,
                                       bool skip_rw_lock = false);

      // ── Flush & Swap ──────────────────────────────────────────────

      void try_swap_rw_to_ro(uint32_t root_index);
      void swap_rw_to_ro(uint32_t root_index);
      void flush_wal();
      void flush_wal(sal::sync_type sync);
      void flush_wal(uint32_t root_index);
      void flush_wal(uint32_t root_index, sal::sync_type sync);

      // ── Accessors ─────────────────────────────────────────────────

      std::shared_ptr<database_type>& underlying_db() noexcept { return _db; }
      const dwal_config&              config() const noexcept { return _cfg; }
      dwal_root_type&                 root(uint32_t index) { return ensure_root(index); }

      epoch_registry_type& epochs() noexcept { return _epochs; }

      bool should_swap(uint32_t root_index) const;
      bool should_backpressure(uint32_t root_index) const;

      void request_shutdown();

      dwal_root_type& ensure_root_public(uint32_t index) { return ensure_root(index); }

      void clear_thread_local_cache();

      void ensure_wal_public(uint32_t root_index) { ensure_wal(root_index); }

      lookup_result tri_get(uint32_t root_index, std::string_view key);

      psitri::cursor create_tri_cursor(uint32_t root_index);

      void clear_thread_local_caches();

      /// Replay any WAL files found on disk to recover from a crash.
      void recover();

     private:
      void replay_wal_to_tri(uint32_t root_index, const std::filesystem::path& wal_path);
      void replay_wal_to_rw(uint32_t root_index, const std::filesystem::path& wal_path);
      void ensure_wal(uint32_t root_index);

      std::shared_ptr<database_type> _db;
      std::filesystem::path          _wal_dir;
      dwal_config                    _cfg;
      std::unique_ptr<wal_status_mapping> _wal_status;

      static constexpr uint32_t       max_roots = 512;
      std::unique_ptr<dwal_root_type> _roots[max_roots];

      dwal_root_type& ensure_root(uint32_t index);

      std::atomic<uint64_t> _next_multi_tx_id{1};

      epoch_registry_type _epochs;

      std::unique_ptr<merge_pool_type> _merge_pool;
   };

   using dwal_database = basic_dwal_database<std_lock_policy>;

}  // namespace psitri::dwal
