#include <psitri/dwal/merge_pool.hpp>

#include <psitri/database.hpp>
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/write_session_impl.hpp>

namespace psitri::dwal
{
   merge_pool::merge_pool(std::shared_ptr<psitri::database> db,
                          uint32_t                           num_threads,
                          epoch_registry&                    epochs,
                          std::filesystem::path              wal_dir)
       : _db(std::move(db)), _epochs(epochs), _wal_dir(std::move(wal_dir))
   {
      // Sessions are created lazily on each worker thread (not here) because
      // allocator_sessions are thread-local — a session created on the main
      // thread cannot be used on a worker thread.
      _sessions.resize(num_threads);

      _threads.reserve(num_threads);
      for (uint32_t i = 0; i < num_threads; ++i)
         _threads.emplace_back(&merge_pool::worker_loop, this, i);
   }

   merge_pool::~merge_pool() { shutdown(); }

   void merge_pool::signal(uint32_t root_index, dwal_root& root)
   {
      {
         std::lock_guard lk(_queue_mu);
         _queue.push({root_index, &root});
      }
      _queue_cv.notify_one();
   }

   void merge_pool::shutdown()
   {
      if (_shutdown.exchange(true))
         return;

      _queue_cv.notify_all();
      for (auto& t : _threads)
      {
         if (t.joinable())
            t.join();
      }
      _threads.clear();
      _sessions.clear();
   }

   void merge_pool::worker_loop(uint32_t thread_index)
   {
      // Create the write session on this worker thread so the allocator_session
      // is bound to the correct thread-local storage.  The session must also be
      // destroyed on this thread (allocator_session has thread affinity).
      _sessions[thread_index] = _db->start_write_session();

      while (!_shutdown.load(std::memory_order_relaxed))
      {
         merge_request req;
         {
            std::unique_lock lk(_queue_mu);
            _queue_cv.wait(lk, [this]
                           { return !_queue.empty() || _shutdown.load(std::memory_order_relaxed); });
            if (_shutdown.load(std::memory_order_relaxed) && _queue.empty())
               break;
            req = _queue.front();
            _queue.pop();
         }

         drain_ro_btree(thread_index, req.root_index, *req.root);
         try_reclaim();
      }

      // Destroy session on this thread to respect allocator_session thread affinity.
      _sessions[thread_index].reset();
   }

   void merge_pool::drain_ro_btree(uint32_t thread_index, uint32_t root_index, dwal_root& root)
   {
      // Grab a shared_ptr to the RO btree — keeps it alive during merge.
      std::shared_ptr<btree_layer> ro;
      {
         std::shared_lock lk(root.buffered_mutex);
         ro = root.buffered_ptr;
      }
      if (!ro)
         return;

      auto& ws = *_sessions[thread_index];
      auto  tx = ws.start_transaction(root_index);

      // Drain all entries from the RO btree into PsiTri.
      for (auto it = ro->map.begin(); it != ro->map.end(); ++it)
      {
         auto key = it.key();
         auto& val = it.value();
         if (val.is_tombstone())
         {
            tx.remove(key);
         }
         else if (val.is_subtree())
         {
            auto subtree = ws.make_ptr(val.subtree_root, /*retain=*/true);
            if (subtree)
               tx.upsert(key, std::move(subtree));
         }
         else
         {
            tx.upsert(key, val.data);
         }
      }

      // Apply range tombstones.
      for (const auto& range : ro->tombstones.ranges())
         tx.remove_range(range.low, range.high);

      tx.commit();

      // Update the DWAL root's tri_root to reflect the new PsiTri root.
      auto new_root = ws.get_root(root_index);
      if (new_root)
         root.tri_root.store(static_cast<uint32_t>(new_root.address()), std::memory_order_release);

      // Null the buffered shared_ptr — last reader holding a copy will free it.
      {
         std::unique_lock lk(root.buffered_mutex);
         root.buffered_ptr.reset();
      }

      // Drop our local reference.
      uint32_t gen = ro->generation;
      ro.reset();

      // Queue for epoch-based reclamation (in case readers still hold copies).
      // Note: with shared_ptr this is handled automatically, but we keep the
      // epoch tracking for generation ordering.

      // Delete the RO WAL file — its data is now in PsiTri.
      if (!_wal_dir.empty())
      {
         auto ro_wal = _wal_dir / ("root-" + std::to_string(root_index)) / "wal-ro.dwal";
         std::error_code ec;
         std::filesystem::remove(ro_wal, ec);
      }

      // Signal the writer that it can swap again.
      root.merge_complete.store(true, std::memory_order_release);
   }

   void merge_pool::try_reclaim()
   {
      // With shared_ptr-based RO pools, reclamation is automatic.
      // The last shared_ptr holder (reader or merge thread) frees the btree_layer.
      // We keep this method for future epoch-based optimizations.
   }

}  // namespace psitri::dwal
