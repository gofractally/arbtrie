#pragma once
#include <psitri/dwal/epoch_lock.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace psitri
{
   class database;
   class write_session;
}  // namespace psitri

namespace psitri::dwal
{
   struct dwal_root;

   /// A bounded thread pool that drains RO btrees into PsiTri.
   ///
   /// Each pool thread owns a PsiTri write session (sessions are root-independent
   /// and reused across roots). When a writer swaps RW→RO, it signals the pool.
   /// A pool thread wakes, picks up the root, and drains all entries from the
   /// RO btree into PsiTri via a transaction.
   ///
   /// After merge completes, the pool nulls the buffered shared_ptr and sets
   /// merge_complete=true. The shared_ptr ref count handles lifetime — the last
   /// reader holding a copy will free the btree_layer.
   class merge_pool
   {
     public:
      merge_pool(std::shared_ptr<psitri::database> db,
                 uint32_t                           num_threads,
                 epoch_registry&                    epochs,
                 std::filesystem::path              wal_dir              = {},
                 uint64_t                           target_arena_bytes   = 0);

      ~merge_pool();

      merge_pool(const merge_pool&)            = delete;
      merge_pool& operator=(const merge_pool&) = delete;

      /// Signal that a root's RO btree needs draining.
      void signal(uint32_t root_index, dwal_root& root);

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
         uint32_t   root_index;
         dwal_root* root;
      };

      void worker_loop(uint32_t thread_index);
      void drain_ro_btree(uint32_t thread_index, uint32_t root_index, dwal_root& root);

      std::shared_ptr<psitri::database> _db;
      epoch_registry&                   _epochs;
      std::filesystem::path             _wal_dir;
      uint64_t                          _target_arena_bytes = 0;

      // Worker threads and their write sessions.
      std::vector<std::thread>                            _threads;
      std::vector<std::shared_ptr<psitri::write_session>> _sessions;

      // Work queue.
      std::mutex                _queue_mu;
      std::condition_variable   _queue_cv;
      std::queue<merge_request> _queue;
      std::atomic<bool>         _shutdown{false};
   };

}  // namespace psitri::dwal
