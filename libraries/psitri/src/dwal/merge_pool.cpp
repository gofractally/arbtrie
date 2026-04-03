#include <psitri/dwal/merge_pool.hpp>

#include <psitri/database.hpp>
#include <psitri/dwal/btree_layer.hpp>
#include <psitri/dwal/dwal_root.hpp>
#include <psitri/write_session_impl.hpp>

namespace psitri::dwal
{
   merge_pool::merge_pool(std::shared_ptr<psitri::database> db,
                          uint32_t                           num_threads,
                          epoch_registry&                    epochs)
       : _db(std::move(db)), _epochs(epochs)
   {
      _sessions.resize(num_threads);
      for (uint32_t i = 0; i < num_threads; ++i)
         _sessions[i] = _db->start_write_session();

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
      while (!_shutdown.load(std::memory_order_relaxed))
      {
         merge_request req;
         {
            std::unique_lock lk(_queue_mu);
            _queue_cv.wait(lk, [this]
                           { return !_queue.empty() || _shutdown.load(std::memory_order_relaxed); });
            if (_shutdown.load(std::memory_order_relaxed) && _queue.empty())
               return;
            req = _queue.front();
            _queue.pop();
         }

         drain_ro_btree(thread_index, req.root_index, *req.root);
         try_reclaim();
      }
   }

   void merge_pool::drain_ro_btree(uint32_t thread_index, uint32_t root_index, dwal_root& root)
   {
      auto* ro = root.ro_ptr.load(std::memory_order_acquire);
      if (!ro)
         return;

      auto& ws = *_sessions[thread_index];
      auto  tx = ws.start_transaction(root_index);

      // Drain all entries from the RO btree into PsiTri.
      for (auto& [key, val] : ro->map)
      {
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

      // Clear the RO slot — take ownership for epoch-based reclamation.
      uint32_t gen = ro->generation;
      root.ro_ptr.store(nullptr, std::memory_order_release);

      {
         std::lock_guard lk(_reclaim_mu);
         _pending_reclaim.push_back({std::unique_ptr<btree_layer>(ro), gen});
      }

      // Signal the writer that the RO slot is free (backpressure release).
      {
         std::lock_guard lk(root.merge_mutex);
         root.merge_active.store(false, std::memory_order_relaxed);
      }
      root.merge_cv.notify_one();
   }

   void merge_pool::try_reclaim()
   {
      uint32_t min_gen = _epochs.min_pinned();

      std::lock_guard lk(_reclaim_mu);
      auto            it = _pending_reclaim.begin();
      while (it != _pending_reclaim.end())
      {
         if (min_gen > it->generation)
            it = _pending_reclaim.erase(it);
         else
            ++it;
      }
   }

}  // namespace psitri::dwal
