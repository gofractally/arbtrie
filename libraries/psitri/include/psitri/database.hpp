#pragma once
#include <chrono>
#include <filesystem>
#include <mutex>
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
   class write_session;
   class read_session;

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
   }

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
    * Typical usage:
    * @code
    *   auto db = psitri::database::create("mydb");
    *   auto ws = db->start_write_session();
    *   auto tx = ws->start_transaction(0);
    *   tx.upsert("key", "value");
    *   tx.commit();
    * @endcode
    */
   class database : public std::enable_shared_from_this<database>
   {
     public:
      /** @name Construction & Lifecycle */
      ///@{

      /**
       * @brief Open or create a database.
       *
       * This is the primary entry point for obtaining a database instance.
       *
       * @param dir   Directory containing (or to contain) the database files.
       * @param mode  How to handle existing vs. new databases.
       * @param cfg   Runtime configuration (cache budget, sync mode, etc.).
       * @return A shared_ptr to the database.
       *
       * @throws std::runtime_error if mode is create_only and the database exists.
       * @throws std::runtime_error if mode is open_existing and the database does not exist.
       * @throws std::runtime_error if mode is read_only (not yet implemented).
       */
      static std::shared_ptr<database> open(std::filesystem::path dir,
                                            open_mode             mode     = open_mode::create_or_open,
                                            const runtime_config& cfg      = {},
                                            recovery_mode         recovery = recovery_mode::none);

      /**
       * @brief Create a new database. Fails if the database already exists.
       * @deprecated Use database::open(dir, open_mode::create_only) instead.
       */
      static std::shared_ptr<database> create(std::filesystem::path dir,
                                              const runtime_config& = {});

      ~database();

      ///@}

      /// @cond INTERNAL
      /**
       * @brief Low-level constructor. Prefer database::open() for normal use.
       * @param dir  Directory containing the database files.
       * @param cfg  Runtime configuration.
       * @param mode Recovery mode to apply on open.
       */
      database(const std::filesystem::path& dir,
               const runtime_config&       cfg,
               recovery_mode               mode = recovery_mode::none);
      /// @endcond

      /** @name Sessions */
      ///@{

      /**
       * @brief Create a write session for the calling thread.
       *
       * The returned session is backed by the calling thread's allocator_session
       * and must only be used from the thread that created it. For multi-writer
       * patterns, call start_write_session() from each writer thread rather than
       * creating sessions on a coordinator thread and distributing them.
       *
       * @return A shared_ptr to the new write session.
       */
      std::shared_ptr<write_session> start_write_session();

      /**
       * @brief Create a read session for the calling thread.
       *
       * Same thread-affinity rule as start_write_session(): create the session
       * on the thread that will use it.
       *
       * @return A shared_ptr to the new read session.
       */
      std::shared_ptr<read_session> start_read_session();

      ///@}

      /** @name Configuration */
      ///@{

      /**
       * @brief Flush all pending writes to the configured sync level.
       */
      void sync();

      /**
       * @brief Update runtime configuration (cache budget, sync mode, etc.).
       * @param cfg The new configuration to apply.
       */
      void set_runtime_config(const runtime_config& cfg);

      ///@}

      /** @name Statistics */
      ///@{

      /**
       * @brief Return a snapshot of database statistics.
       *
       * Gathers storage, cache, and session metrics into a database_stats
       * object that can be inspected programmatically or printed.
       *
       * @code
       *   auto stats = db->get_stats();
       *   std::cout << stats;                           // human-readable
       *   if (stats.total_free_bytes > 1024*1024*100)   // programmatic check
       *       db->compact_and_truncate();
       * @endcode
       */
      database_stats get_stats() const
      {
         auto d = _allocator.dump();
         database_stats s;
         s.total_segments       = d.total_segments;
         s.total_live_bytes     = d.total_read_bytes;
         s.total_live_objects   = d.total_read_nodes;
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

      /** @name Compaction & Maintenance */
      ///@{

      /**
       * @brief Block until the compactor has drained all pending releases.
       * @param timeout Maximum time to wait.
       * @return true if drained, false if timed out.
       */
      bool wait_for_compactor(std::chrono::milliseconds timeout = std::chrono::milliseconds(10000))
      {
         auto deadline = std::chrono::steady_clock::now() + timeout;
         while (std::chrono::steady_clock::now() < deadline)
         {
            if (_allocator.total_pending_releases() == 0)
            {
               std::this_thread::sleep_for(std::chrono::milliseconds(50));
               if (_allocator.total_pending_releases() == 0)
                  return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
         }
         return false;
      }

      /**
       * @brief Wait for compaction to complete, then truncate trailing free
       *        segments from the data file to reclaim disk space.
       */
      void compact_and_truncate()
      {
         wait_for_compactor();
         _allocator.truncate_free_tail();
      }

      /**
       * @brief Create a defragmented copy of the database, then swap it in.
       *
       * The old database files are preserved as dir.old until verification passes.
       */
      void defrag();

      ///@}

      /** @name Recovery */
      ///@{

      /**
       * @brief Check whether reference counts are stale from a deferred_cleanup recovery.
       *
       * Leaked memory is not reclaimed until reclaim_leaked_memory() is called.
       *
       * @return true if ref counts need rebuilding.
       */
      bool ref_counts_stale() const;

      /**
       * @brief Reclaim leaked memory from a prior deferred_cleanup recovery.
       *
       * This is the expensive O(live objects) walk that deferred_cleanup skips.
       * No-op if ref counts are not stale.
       */
      void reclaim_leaked_memory();

      /**
       * @brief Full offline integrity verification.
       *
       * Checks segment checksums, object checksums, key hashes, value checksums,
       * and tree structure. Returns detailed results including per-failure context
       * for targeted repair.
       */
      sal::verify_result verify();

      /**
       * @brief Full recovery: rebuild control blocks from segments and reclaim leaked memory.
       */
      void recover() { _allocator.recover(); }

      /**
       * @brief Lightweight recovery: reset reference counts and reclaim leaked memory.
       */
      void reset_reference_counts() { _allocator.reset_reference_counts(); }

      ///@}

      /** @name Low-Level Diagnostics
       *  These methods expose SAL allocator internals. They are intended for
       *  debugging, tooling (psitri-tool), and advanced monitoring — not for
       *  normal application use. Prefer get_stats() for production monitoring.
       */
      ///@{

      /**
       * @brief Return the raw SAL allocator dump with per-segment detail.
       *
       * Contains detailed per-segment info (freed bytes, pinned state, age,
       * object counts) and internal histograms. Use get_stats() instead for
       * a clean summary.
       */
      sal::seg_alloc_dump dump() const { return _allocator.dump(); }

      /**
       * @brief Return the total size in bytes of all reachable (live) objects.
       *
       * Walks the entire object graph. Expensive for large databases.
       */
      uint64_t reachable_size() { return _allocator.reachable_size(); }

      /**
       * @brief Audit freed space accounting across all segments.
       *
       * Compares the allocator's tracked free space against a full segment
       * scan. For debugging allocator accounting bugs.
       */
      auto audit_freed_space() { return _allocator.audit_freed_space(); }

      ///@}

     private:
      friend class read_session;
      friend class write_session;

      std::filesystem::path _dir;
      runtime_config        _cfg;

      mutable std::mutex _sync_mutex;
      mutable std::mutex _root_change_mutex[num_top_roots];
      mutable std::mutex _modify_lock[num_top_roots];

      std::mutex& modify_lock(int index) { return _modify_lock[index]; }

      void            init_allocator_shared_ownership();
      std::once_flag  _alloc_shared_init;
      sal::allocator  _allocator;
      sal::mapping            _dbfile;
      detail::database_state* _dbm;
   };
   using database_ptr = std::shared_ptr<database>;
}  // namespace psitri
