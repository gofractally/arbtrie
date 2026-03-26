#pragma once
#include <chrono>
#include <filesystem>
#include <mutex>
#include <psitri/write_session.hpp>
#include <sal/allocator.hpp>
#include <sal/config.hpp>
#include <sal/mapping.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <thread>

namespace psitri
{
   using runtime_config = sal::runtime_config;
   using recovery_mode  = sal::recovery_mode;
   class write_session;
   class read_session;

   static constexpr uint32_t num_top_roots = 512;
   namespace detail
   {
      class database_state;
   }

   class database : public std::enable_shared_from_this<database>
   {
     public:
      database(const std::filesystem::path& dir,
               const runtime_config&       cfg,
               recovery_mode               mode = recovery_mode::none);
      ~database();

      static std::shared_ptr<database> create(std::filesystem::path dir,
                                              const runtime_config& = {});

      void sync();
      void set_runtime_config(const runtime_config& cfg);

      sal::seg_alloc_dump dump() const { return _allocator.dump(); }
      void print_stats(std::ostream& os = std::cout) const { dump().print(os); }
      uint64_t reachable_size() { return _allocator.reachable_size(); }
      auto audit_freed_space() { return _allocator.audit_freed_space(); }

      /// Block until the compactor has drained all pending releases across all sessions.
      /// Returns true if drained, false if timed out.
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

      /// Wait for compaction to complete, then truncate trailing free segments
      /// from the data file to reclaim disk space.
      void compact_and_truncate()
      {
         wait_for_compactor();
         _allocator.truncate_free_tail();
      }

      /// Create a defragmented copy of the database, then swap it in.
      /// The old database files are preserved as dir.old until verification passes.
      void defrag();

      /// True if ref counts are stale from a deferred_cleanup recovery.
      /// Leaked memory is not reclaimed until reclaim_leaked_memory() is called.
      bool ref_counts_stale() const;

      /// Reclaim leaked memory from a prior deferred_cleanup recovery.
      /// This is the expensive O(live objects) walk that deferred_cleanup skips.
      /// No-op if ref counts are not stale.
      void reclaim_leaked_memory();

      /// Full recovery: rebuild control blocks from segments and reclaim leaked memory
      void recover() { _allocator.recover(); }

      /// Lightweight recovery: reset reference counts and reclaim leaked memory
      void reset_reference_counts() { _allocator.reset_reference_counts(); }

      /// Creates a new write session backed by the calling thread's
      /// allocator_session.  The returned session must only be used
      /// from the thread that created it -- the underlying allocator
      /// session is thread_local and not safe to share across threads.
      ///
      /// For multi-writer patterns, call start_write_session() from
      /// each writer thread rather than creating sessions on a
      /// coordinator thread and distributing them.
      std::shared_ptr<write_session> start_write_session();

      /// Creates a new read session backed by the calling thread's
      /// allocator_session.  Same thread-affinity rule as
      /// start_write_session(): create the session on the thread
      /// that will use it.
      std::shared_ptr<read_session>  start_read_session();

     private:
      friend class read_session;
      friend class write_session;

      std::filesystem::path _dir;
      runtime_config        _cfg;

      mutable std::mutex _sync_mutex;
      mutable std::mutex _root_change_mutex[num_top_roots];
      mutable std::mutex _modify_lock[num_top_roots];

      std::mutex& modify_lock(int index) { return _modify_lock[index]; }

      sal::allocator          _allocator;
      sal::mapping            _dbfile;
      detail::database_state* _dbm;
   };
   using database_ptr = std::shared_ptr<database>;
}  // namespace psitri