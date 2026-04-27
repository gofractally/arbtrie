#pragma once
#include <chrono>
#include <filesystem>
#include <hash/xxhash.h>
#include <memory>
#include <mutex>
#include <psitri/live_range_map.hpp>
#include <psitri/lock_policy.hpp>
#include <psitri/write_session.hpp>
#include <sal/allocator.hpp>
#include <sal/config.hpp>
#include <sal/mapping.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <sal/verify.hpp>
#include <thread>

namespace psitri
{
   using runtime_config = sal::runtime_config;
   using recovery_mode  = sal::recovery_mode;

   template <class LockPolicy>
   class basic_write_session;

   template <class LockPolicy>
   class basic_read_session;

   static constexpr uint32_t num_top_roots = 512;

   /// Controls how database::open() handles existing vs. new databases.
   enum class open_mode
   {
      /// Open an existing database or create a new one.
      create_or_open,

      /// Create a new database. Fails if the database already exists.
      create_only,

      /// Open an existing database. Fails if the database does not exist.
      open_existing,

      /// Open an existing database in read-only mode.
      /// Not yet implemented — reserved for future use.
      read_only,
   };

   namespace detail
   {
      class database_state;

      /// One-time registration of psitri node vtables with SAL.  Idempotent,
      /// safe to call multiple times and from multiple threads.
      void register_node_types();

      /// Walk the tree from all top-level roots and populate a verify_result.
      /// Non-template so it can live in a .cpp file and keep the tree-walk
      /// logic out of the header.
      sal::verify_result verify_all_roots(sal::allocator& alloc);
   }  // namespace detail

   /**
    * @brief High-level database statistics for monitoring and diagnostics.
    *
    * Returned by database::get_stats(). All fields are a point-in-time snapshot;
    * values may change between the call and when you read them.
    */
   struct database_stats
   {
      /** @name Storage */
      ///@{
      uint64_t total_segments         = 0;  ///< Number of 32 MB segments in the data file.
      uint64_t total_live_bytes       = 0;  ///< Bytes occupied by live (reachable) objects.
      uint64_t total_live_objects     = 0;  ///< Number of live objects across all segments.
      uint64_t total_free_bytes       = 0;  ///< Bytes reclaimable by compaction.
      uint64_t database_file_bytes    = 0;  ///< Total data file size on disk (segments x 32 MB).
      ///@}

      /** @name Cache */
      ///@{
      uint32_t pinned_segments        = 0;  ///< Segments currently mlock'd in RAM.
      uint64_t pinned_bytes           = 0;  ///< Total bytes in pinned segments (pinned_segments x 32 MB).
      uint32_t cache_difficulty       = 0;  ///< Current MFU promotion difficulty (self-tuning).
      uint64_t total_promoted_bytes   = 0;  ///< Cumulative bytes promoted to pinned cache.
      ///@}

      /** @name Sessions */
      ///@{
      uint32_t active_sessions        = 0;  ///< Number of active allocator sessions (read + write).
      int64_t  pending_releases       = 0;  ///< Objects queued for deferred deallocation by the compactor.
      ///@}

      /** @name Recycling */
      ///@{
      uint64_t recycled_queue_depth   = 0;  ///< Segments waiting for read locks to release before reuse.
      uint64_t recycled_queue_capacity= 0;  ///< Maximum capacity of the recycled-segments queue.
      ///@}

      /**
       * @brief Format the stats as a human-readable multi-line string.
       */
      std::string to_string() const
      {
         auto fmt_bytes = [](uint64_t b) -> std::string {
            char buf[64];
            if (b >= 1024ull * 1024 * 1024)
               snprintf(buf, sizeof(buf), "%.2f GB", b / (1024.0 * 1024.0 * 1024.0));
            else if (b >= 1024ull * 1024)
               snprintf(buf, sizeof(buf), "%.2f MB", b / (1024.0 * 1024.0));
            else if (b >= 1024ull)
               snprintf(buf, sizeof(buf), "%.2f KB", b / 1024.0);
            else
               snprintf(buf, sizeof(buf), "%llu B", (unsigned long long)b);
            return buf;
         };

         std::string s;
         s += "Storage:\n";
         s += "  segments:        " + std::to_string(total_segments) + "\n";
         s += "  live objects:    " + std::to_string(total_live_objects) + "\n";
         s += "  live data:       " + fmt_bytes(total_live_bytes) + "\n";
         s += "  free space:      " + fmt_bytes(total_free_bytes) + "\n";
         s += "  file size:       " + fmt_bytes(database_file_bytes) + "\n";
         s += "Cache:\n";
         s += "  pinned segments: " + std::to_string(pinned_segments) + "\n";
         s += "  pinned memory:   " + fmt_bytes(pinned_bytes) + "\n";
         s += "  difficulty:      " + std::to_string(cache_difficulty) + "\n";
         s += "  promoted:        " + fmt_bytes(total_promoted_bytes) + "\n";
         s += "Sessions:\n";
         s += "  active:          " + std::to_string(active_sessions) + "\n";
         s += "  pending releases:" + std::to_string(pending_releases) + "\n";
         return s;
      }

      friend std::ostream& operator<<(std::ostream& os, const database_stats& s)
      {
         return os << s.to_string();
      }
   };

   /**
    * @brief The main entry point for creating and managing a PsiTri database.
    *
    * A database manages the on-disk storage, background threads (compactor,
    * segment provider, read-bit decay), and up to 512 independent top-level
    * roots. All reads and writes go through sessions obtained from this class.
    *
    * @tparam LockPolicy  Policy type whose `mutex_type` alias selects the
    *                    mutex used for internal synchronization. Defaults to
    *                    `std_lock_policy` (std::mutex). Fiber-based callers
    *                    should supply a policy whose mutex yields instead of
    *                    blocking the OS thread on contention.
    *
    * Typical usage (default policy):
    * @code
    *   auto db = psitri::database::open("mydb", psitri::open_mode::create_or_open);
    *   auto ws = db->start_write_session();
    *   auto tx = ws->start_transaction(0);
    *   tx.upsert("key", "value");
    *   tx.commit();
    * @endcode
    */
   template <class LockPolicy = std_lock_policy>
   class basic_database : public std::enable_shared_from_this<basic_database<LockPolicy>>
   {
     public:
      using lock_policy_type = LockPolicy;
      using mutex_type       = typename LockPolicy::mutex_type;
      using write_session_type = basic_write_session<LockPolicy>;
      using read_session_type  = basic_read_session<LockPolicy>;

      /** @name Construction & Lifecycle */
      ///@{

      /**
       * @brief Open or create a database.
       *
       * @param dir   Directory containing (or to contain) the database files.
       * @param mode  How to handle existing vs. new databases.
       * @param cfg   Runtime configuration (cache budget, sync mode, etc.).
       * @return A shared_ptr to the database.
       */
      static std::shared_ptr<basic_database> open(
          std::filesystem::path dir,
          open_mode             mode     = open_mode::create_or_open,
          const runtime_config& cfg      = {},
          recovery_mode         recovery = recovery_mode::none);

      /**
       * @brief Create-only convenience helper. Fails if the database already exists.
       *
       * Equivalent to database::open(dir, open_mode::create_only, cfg).
       * Prefer database::open() in user-facing examples so the open mode is
       * visible at the call site.
       */
      static std::shared_ptr<basic_database> create(std::filesystem::path dir,
                                                    const runtime_config& = {});

      ~basic_database();

      ///@}

      /// @cond INTERNAL
      /**
       * @brief Low-level constructor. Prefer database::open() for normal use.
       */
      basic_database(const std::filesystem::path& dir,
                     const runtime_config&        cfg,
                     recovery_mode                mode = recovery_mode::none);
      /// @endcond

      /** @name Sessions */
      ///@{

      std::shared_ptr<write_session_type> start_write_session();
      std::shared_ptr<read_session_type>  start_read_session();

      ///@}

      /** @name Configuration */
      ///@{

      void sync()
      {
         std::lock_guard<mutex_type> lock(_sync_mutex);
         _allocator.sync(_cfg.sync_mode);
      }

      void set_runtime_config(const runtime_config& cfg)
      {
         _cfg = cfg;
         _allocator.set_runtime_config(cfg);
      }

      ///@}

      /** @name Statistics */
      ///@{

      database_stats get_stats() const
      {
         auto d = _allocator.dump();
         database_stats s;
         s.total_segments       = d.total_segments;
         s.total_live_bytes     = d.total_read_bytes;
         // total_read_nodes from the segment dump only counts objects in
         // their last-known segment location; objects relocated by the
         // compactor are double-counted/missed there. Pull the live count
         // from the control-block allocator instead.
         s.total_live_objects   = _allocator.total_allocated_objects();
         s.total_free_bytes     = d.total_free_space;
         s.database_file_bytes  = d.total_segments * sal::segment_size;
         s.pinned_segments      = d.mlocked_segments_count;
         s.pinned_bytes         = uint64_t(d.mlocked_segments_count) * sal::segment_size;
         s.cache_difficulty     = d.cache_difficulty;
         s.total_promoted_bytes = d.total_promoted_bytes;
         s.active_sessions      = d.active_sessions;
         s.pending_releases     = d.free_release_count;
         s.recycled_queue_depth    = d.recycled_queue_depth;
         s.recycled_queue_capacity = d.recycled_queue_capacity;
         return s;
      }

      ///@}

      /** @name MVCC Configuration */
      ///@{

      /// Set the epoch interval (number of global versions per epoch).
      /// Smaller values cause more frequent COW maintenance passes;
      /// larger values allow more MVCC fast-path writes before cleanup.
      void set_epoch_interval(uint64_t interval);

      /// Current epoch number.
      uint64_t current_epoch() const;

      ///@}

      /** @name Compaction & Maintenance */
      ///@{

      bool wait_for_compactor(std::chrono::milliseconds timeout = std::chrono::milliseconds(10000))
      {
         auto deadline = std::chrono::steady_clock::now() + timeout;
         while (std::chrono::steady_clock::now() < deadline)
         {
            if (_allocator.total_pending_releases() == 0)
            {
               std::this_thread::sleep_for(std::chrono::milliseconds(50));
               if (_allocator.total_pending_releases() == 0)
               {
                  // Publish any accumulated dead versions so defrag/COW can see them
                  _dead_versions.flush_pending();
                  _dead_versions.publish_snapshot();
                  return true;
               }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
         }
         return false;
      }

      void compact_and_truncate()
      {
         wait_for_compactor();
         _allocator.truncate_free_tail();
      }

      void defrag();

      ///@}

      /** @name Recovery */
      ///@{

      bool ref_counts_stale() const;

      void reclaim_leaked_memory();

      sal::verify_result verify() { return detail::verify_all_roots(_allocator); }

      void recover() { _allocator.recover(); }

      void reset_reference_counts() { _allocator.reset_reference_counts(); }

      ///@}

      /** @name Low-Level Diagnostics */
      ///@{

      sal::seg_alloc_dump dump() const { return _allocator.dump(); }

      uint64_t reachable_size() { return _allocator.reachable_size(); }

      auto audit_freed_space() { return _allocator.audit_freed_space(); }

      ///@}

      /** @name MVCC Version Reclamation */
      ///@{

      /// Access the dead-version range map (for diagnostics or testing).
      live_range_map& dead_versions() noexcept { return _dead_versions; }
      const live_range_map& dead_versions() const noexcept { return _dead_versions; }

      ///@}

      /// @cond INTERNAL
      mutex_type& modify_lock(int index) { return _modify_lock[index]; }

      /// Internal accessor used by layered components (e.g. DWAL) to
      /// perform ref-count manipulation on addresses without going
      /// through a thread-affine session. `sal::allocator::retain` is
      /// atomic; `sal::allocator::release` forwards to the calling
      /// thread's thread-local session, so both can be called from any
      /// thread safely.
      sal::allocator& underlying_allocator() noexcept { return _allocator; }
      /// @endcond

     private:
      template <class> friend class basic_read_session;
      template <class> friend class basic_write_session;

      std::filesystem::path _dir;
      runtime_config        _cfg;

      mutable mutex_type _sync_mutex;
      /// Lightweight lock for root slot version-CB swaps (fast-path MVCC).
      /// Does NOT protect tree structure — only the ver field in the root slot.
      mutable mutex_type _root_ver_mutex[num_top_roots];
      /// Full per-root mutex for COW mutations that change the root.
      mutable mutex_type _modify_lock[num_top_roots];

      mutex_type& root_ver_lock(int index) { return _root_ver_mutex[index]; }

      // ── Stripe locks for fine-grained MVCC concurrency ──────────
      static constexpr uint32_t num_stripe_locks = 1024;

      struct alignas(64) stripe_lock
      {
         mutex_type m;
      };
      stripe_lock _stripes[num_stripe_locks];

      uint32_t stripe_index(sal::ptr_address adr) const noexcept
      {
         uint32_t v = *adr;
         return XXH3_64bits(&v, sizeof(v)) & (num_stripe_locks - 1);
      }

      mutex_type& stripe_mutex(sal::ptr_address adr) { return _stripes[stripe_index(adr)].m; }

      void           init_allocator_shared_ownership();
      void           recover_global_version_from_roots();
      std::once_flag _alloc_shared_init;
      sal::allocator _allocator;
      sal::mapping            _dbfile;
      detail::database_state* _dbm;
      live_range_map          _dead_versions;
   };

   /// Default-policy alias preserved for existing consumers.
   using database     = basic_database<std_lock_policy>;
   using database_ptr = std::shared_ptr<database>;

}  // namespace psitri
