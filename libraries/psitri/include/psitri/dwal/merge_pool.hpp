#pragma once
#include <psitri/dwal/epoch_lock.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
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
   /// After merge completes, the RO btree is marked for reclamation. The pool
   /// is freed once all readers have released (epoch-based reclamation via
   /// epoch_registry).
   class merge_pool
   {
     public:
      /// Construct a merge pool with the given number of threads.
      /// Each thread gets its own write session from `db`.
      merge_pool(std::shared_ptr<psitri::database> db,
                 uint32_t                           num_threads,
                 epoch_registry&                    epochs);

      ~merge_pool();

      merge_pool(const merge_pool&)            = delete;
      merge_pool& operator=(const merge_pool&) = delete;

      /// Signal that a root's RO btree needs draining.
      /// Called by the writer after swap_rw_to_ro().
      void signal(uint32_t root_index, dwal_root& root);

      /// Shut down the pool — waits for all in-flight merges to complete.
      void shutdown();

      /// Try to reclaim old RO pools that are no longer referenced by readers.
      /// Called periodically by merge threads after completing a drain.
      void try_reclaim();

     private:
      struct merge_request
      {
         uint32_t   root_index;
         dwal_root* root;
      };

      /// Pool of old RO btree layers waiting for epoch-based reclamation.
      struct pending_pool
      {
         std::unique_ptr<struct btree_layer> layer;
         uint32_t                            generation;
      };

      void worker_loop(uint32_t thread_index);
      void drain_ro_btree(uint32_t thread_index, uint32_t root_index, dwal_root& root);

      std::shared_ptr<psitri::database>            _db;
      epoch_registry&                              _epochs;

      // Worker threads and their write sessions.
      std::vector<std::thread>                         _threads;
      std::vector<std::shared_ptr<psitri::write_session>> _sessions;

      // Work queue.
      std::mutex              _queue_mu;
      std::condition_variable _queue_cv;
      std::queue<merge_request> _queue;
      std::atomic<bool>       _shutdown{false};

      // Pending reclamation.
      std::mutex                  _reclaim_mu;
      std::vector<pending_pool>   _pending_reclaim;
   };

}  // namespace psitri::dwal
