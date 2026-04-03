#include <psitri/dwal/dwal_database.hpp>

#include <cassert>
#include <stdexcept>

namespace psitri::dwal
{
   dwal_database::dwal_database(std::shared_ptr<psitri::database> db,
                                std::filesystem::path              wal_dir,
                                dwal_config                        cfg)
       : _db(std::move(db)), _wal_dir(std::move(wal_dir)), _cfg(cfg)
   {
      std::filesystem::create_directories(_wal_dir);

      if (_cfg.merge_threads > 0)
         _merge_pool = std::make_unique<merge_pool>(_db, _cfg.merge_threads, _epochs);
   }

   dwal_database::~dwal_database()
   {
      // Shut down merge pool first — waits for in-flight merges.
      if (_merge_pool)
         _merge_pool->shutdown();

      // Close all WAL files cleanly.
      for (uint32_t i = 0; i < max_roots; ++i)
      {
         if (_roots[i] && _roots[i]->wal)
            _roots[i]->wal->close();
      }
   }

   dwal_root& dwal_database::ensure_root(uint32_t index)
   {
      assert(index < max_roots);
      if (!_roots[index])
         _roots[index] = std::make_unique<dwal_root>();
      return *_roots[index];
   }

   void dwal_database::ensure_wal(uint32_t root_index)
   {
      auto& root = ensure_root(root_index);
      if (!root.wal)
      {
         auto dir = _wal_dir / ("root-" + std::to_string(root_index));
         std::filesystem::create_directories(dir);
         root.wal = std::make_unique<wal_writer>(
             dir / "wal-rw.dwal", static_cast<uint16_t>(root_index), root.next_wal_seq);
      }
   }

   dwal_transaction dwal_database::start_write_transaction(uint32_t         root_index,
                                                           transaction_mode mode)
   {
      assert(root_index < max_roots);

      if (mode == transaction_mode::direct)
      {
         // TODO: Flush RW btree, wait for merge, then delegate to PsiTri.
         // For now, throw — direct mode requires merge thread pool (Task #12).
         throw std::runtime_error("dwal: direct transaction mode not yet implemented");
      }

      auto& root = ensure_root(root_index);
      ensure_wal(root_index);

      // Acquire exclusive lock for the full transaction duration.
      root.rw_mutex.lock();

      return dwal_transaction(root, root.wal.get(), root_index, this);
   }

   dwal_transaction::lookup_result dwal_database::get(uint32_t         root_index,
                                                      std::string_view key,
                                                      read_mode        mode)
   {
      if (mode == read_mode::persistent)
      {
         // Bypass DWAL entirely — read directly from PsiTri.
         // The caller should use the underlying PsiTri session for this.
         return {false, {}};
      }

      assert(root_index < max_roots);
      if (!_roots[root_index])
         return {false, {}};

      auto& root = *_roots[root_index];

      if (mode == read_mode::latest)
      {
         // Acquire shared lock on RW btree.
         root.rw_mutex.lock_shared();

         // Check RW btree.
         auto it = root.rw_layer->map.find(key);
         if (it != root.rw_layer->map.end())
         {
            auto result = it->second.is_tombstone()
                              ? dwal_transaction::lookup_result{false, {}}
                              : dwal_transaction::lookup_result{true, it->second};
            root.rw_mutex.unlock_shared();
            return result;
         }
         if (root.rw_layer->tombstones.is_deleted(key))
         {
            root.rw_mutex.unlock_shared();
            return {false, {}};
         }
         root.rw_mutex.unlock_shared();
      }

      // Check RO btree (read_mode::latest or read_mode::buffered).
      auto* ro = root.ro_ptr.load(std::memory_order_acquire);
      if (ro)
      {
         auto it = ro->map.find(key);
         if (it != ro->map.end())
         {
            if (it->second.is_tombstone())
               return {false, {}};
            return {true, it->second};
         }
         if (ro->tombstones.is_deleted(key))
            return {false, {}};
      }

      // Fall through to PsiTri — caller handles.
      return {false, {}};
   }

   bool dwal_database::should_swap(uint32_t root_index) const
   {
      auto& root = *_roots[root_index];
      if (root.rw_layer->size() >= _cfg.max_rw_entries)
         return true;
      if (root.wal && root.wal->file_size() >= _cfg.max_wal_bytes)
         return true;
      return false;
   }

   void dwal_database::swap_rw_to_ro(uint32_t root_index)
   {
      assert(root_index < max_roots && _roots[root_index]);
      auto& root = *_roots[root_index];

      // Wait for previous merge to complete (backpressure).
      {
         std::unique_lock lock(root.merge_mutex);
         root.merge_cv.wait(lock, [&] { return root.ro_ptr.load(std::memory_order_relaxed) == nullptr; });
      }

      // Capture the current PsiTri root as the base root for this RO btree.
      uint32_t base = root.tri_root.load(std::memory_order_acquire);

      // Freeze the current RW btree as the new RO btree.
      auto* old_rw = root.rw_layer.release();
      root.ro_base_root.store(base, std::memory_order_relaxed);
      root.ro_ptr.store(old_rw, std::memory_order_release);

      // Allocate a fresh RW btree.
      root.rw_layer = std::make_unique<btree_layer>();

      // Increment generation.
      root.generation.fetch_add(1, std::memory_order_release);

      // Rotate WAL file.
      if (root.wal)
      {
         root.next_wal_seq = root.wal->next_sequence();
         root.wal->close();
         root.wal.reset();

         auto dir    = _wal_dir / ("root-" + std::to_string(root_index));
         auto rw_wal = dir / "wal-rw.dwal";
         auto ro_wal = dir / "wal-ro.dwal";

         // Rename current WAL to RO WAL.
         if (std::filesystem::exists(rw_wal))
            std::filesystem::rename(rw_wal, ro_wal);

         // Open fresh WAL for new RW btree.
         root.wal = std::make_unique<wal_writer>(
             rw_wal, static_cast<uint16_t>(root_index), root.next_wal_seq);
      }

      // Broadcast the new generation to all session locks.
      _epochs.broadcast_all(root.generation.load(std::memory_order_relaxed));

      // Signal the merge pool to drain the RO btree.
      if (_merge_pool)
         _merge_pool->signal(root_index, root);
   }

   void dwal_database::flush_wal()
   {
      for (uint32_t i = 0; i < max_roots; ++i)
      {
         if (_roots[i] && _roots[i]->wal)
            _roots[i]->wal->flush();
      }
   }

   void dwal_database::flush_wal(uint32_t root_index)
   {
      assert(root_index < max_roots);
      if (_roots[root_index] && _roots[root_index]->wal)
         _roots[root_index]->wal->flush();
   }

}  // namespace psitri::dwal
