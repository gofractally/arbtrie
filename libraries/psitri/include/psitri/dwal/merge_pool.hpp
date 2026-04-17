#pragma once
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/dwal/epoch_lock.hpp>
#include <psitri/fwd.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace psitri::dwal
{
   /// A bounded thread pool that drains RO btrees into PsiTri.
   ///
   /// Each pool thread owns a PsiTri write session (sessions are root-independent
   /// and reused across roots). When a writer swaps RW→RO, it signals the pool.
   /// A pool thread wakes, picks up the root, and drains all entries from the
   /// RO btree into PsiTri via a transaction.
   ///
   /// The queue mutex and condition_variable are OS-thread primitives: the
   /// pool runs its own background std::threads and never yields a fiber
   /// while holding them. LockPolicy therefore only parameterizes the types
   /// that cross into user threads (dwal_root, epoch_registry, database).
   template <class LockPolicy = std_lock_policy>
   class basic_merge_pool
   {
     public:
      using database_type       = basic_database<LockPolicy>;
      using write_session_type  = basic_write_session<LockPolicy>;
      using dwal_root_type      = basic_dwal_root<LockPolicy>;
      using epoch_registry_type = basic_epoch_registry<LockPolicy>;

      basic_merge_pool(std::shared_ptr<database_type> db,
                       uint32_t                       num_threads,
                       epoch_registry_type&           epochs,
                       std::filesystem::path          wal_dir            = {},
                       uint64_t                       target_arena_bytes = 0);

      ~basic_merge_pool();

      basic_merge_pool(const basic_merge_pool&)            = delete;
      basic_merge_pool& operator=(const basic_merge_pool&) = delete;

      /// Signal that a root's RO btree needs draining.
      void signal(uint32_t root_index, dwal_root_type& root);

      /// Shut down the pool — waits for all in-flight merges to complete.
      void shutdown();

      /// Signal stop without blocking — safe to call from a signal handler.
      /// Sets the shutdown flag and notifies the queue CV so workers will
      /// check _shutdown on their next iteration.
      void request_stop();

      /// Reclaim old RO pools. With shared_ptr this is automatic.
      void try_reclaim();

     private:
      struct merge_request
      {
         uint32_t        root_index;
         dwal_root_type* root;
      };

      void worker_loop(uint32_t thread_index);
      void drain_ro_btree(uint32_t thread_index, uint32_t root_index, dwal_root_type& root);

      std::shared_ptr<database_type> _db;
      epoch_registry_type&           _epochs;
      std::filesystem::path          _wal_dir;
      uint64_t                       _target_arena_bytes = 0;

      // Worker threads and their write sessions.
      std::vector<std::thread>                         _threads;
      std::vector<std::shared_ptr<write_session_type>> _sessions;

      // Work queue — std::mutex is intentional: the merge pool owns its own
      // OS threads and does not participate in any fiber scheduler.
      std::mutex                _queue_mu;
      std::condition_variable   _queue_cv;
      std::queue<merge_request> _queue;
      std::atomic<bool>         _shutdown{false};
   };

   using merge_pool = basic_merge_pool<std_lock_policy>;

}  // namespace psitri::dwal
