#include <psitri/dwal/dwal_database.hpp>

#include <psitri/cursor.hpp>
#include <psitri/database.hpp>
#include <psitri/read_session.hpp>
#include <psitri/read_session_impl.hpp>

#include <cassert>
#include <shared_mutex>
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
      if (_merge_pool)
         _merge_pool->shutdown();

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
         throw std::runtime_error("dwal: direct transaction mode not yet implemented");

      auto& root = ensure_root(root_index);
      ensure_wal(root_index);

      return dwal_transaction(root, root.wal.get(), root_index, this);
   }

   dwal_transaction::lookup_result dwal_database::get(uint32_t         root_index,
                                                      std::string_view key,
                                                      read_mode        mode)
   {
      if (mode == read_mode::persistent)
         return {false, {}};

      assert(root_index < max_roots);
      if (!_roots[root_index])
         return {false, {}};

      auto& root = *_roots[root_index];

      // Latest and buffered both read from the frozen buffered_ptr.
      // Fast path: if generation is 0, no swap has ever happened — skip.
      if (root.generation.load(std::memory_order_acquire) == 0)
         return {false, {}};

      std::shared_ptr<btree_layer> ro;
      {
         std::shared_lock lk(root.buffered_mutex);
         ro = root.buffered_ptr;
      }
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

   void dwal_database::try_swap_rw_to_ro(uint32_t root_index)
   {
      assert(root_index < max_roots && _roots[root_index]);
      auto& root = *_roots[root_index];

      if (!root.merge_complete.load(std::memory_order_acquire))
         return;

      // Freeze current RW as the new buffered RO (exclusive lock — writers only).
      {
         std::unique_lock lk(root.buffered_mutex);
         root.buffered_ptr = std::move(root.rw_layer);
      }

      // Allocate a fresh RW btree.
      root.rw_layer = std::make_shared<btree_layer>();

      // Capture the current PsiTri root as the base for this RO btree.
      uint32_t base = root.tri_root.load(std::memory_order_acquire);
      root.ro_base_root.store(base, std::memory_order_relaxed);

      root.merge_complete.store(false, std::memory_order_release);

      // Increment generation — signals readers that a new snapshot is available.
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

         if (std::filesystem::exists(rw_wal))
            std::filesystem::rename(rw_wal, ro_wal);

         root.wal = std::make_unique<wal_writer>(
             rw_wal, static_cast<uint16_t>(root_index), root.next_wal_seq);
      }

      _epochs.broadcast_all(root.generation.load(std::memory_order_relaxed));

      if (_merge_pool)
         _merge_pool->signal(root_index, root);
   }

   void dwal_database::swap_rw_to_ro(uint32_t root_index)
   {
      try_swap_rw_to_ro(root_index);
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

   dwal_transaction::lookup_result dwal_database::get_latest(uint32_t         root_index,
                                                              std::string_view key)
   {
      assert(root_index < max_roots);

      if (_roots[root_index])
      {
         auto& root = *_roots[root_index];

         // Layer 1: RW btree (uncommitted writes)
         if (root.rw_layer)
         {
            auto it = root.rw_layer->map.find(key);
            if (it != root.rw_layer->map.end())
            {
               if (it->second.is_tombstone())
                  return {false, {}};
               return {true, it->second};
            }
            if (root.rw_layer->tombstones.is_deleted(key))
               return {false, {}};
         }

         // Layer 2: RO btree (frozen snapshot)
         {
            std::shared_ptr<btree_layer> ro;
            {
               std::shared_lock lk(root.buffered_mutex);
               ro = root.buffered_ptr;
            }
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
         }
      }

      // Layer 3: PsiTri COW tree
      return tri_get(root_index, key);
   }

   dwal_transaction::lookup_result dwal_database::tri_get(uint32_t         root_index,
                                                         std::string_view key)
   {
      // Thread-local read session + cursor for Tri-layer lookups.
      // Each calling thread gets its own session (safe for concurrent readers/writers).
      thread_local std::shared_ptr<psitri::read_session> tl_session;
      thread_local psitri::cursor*                       tl_cursor = nullptr;
      thread_local psitri::database*                     tl_db     = nullptr;

      // Re-create session if the database changed (shouldn't happen, but safe).
      if (tl_db != _db.get())
      {
         delete tl_cursor;
         tl_cursor = nullptr;
         tl_session = _db->start_read_session();
         tl_db      = _db.get();
      }

      if (!tl_cursor)
      {
         auto root = tl_session->create_cursor(root_index);
         tl_cursor = new psitri::cursor(std::move(root));
      }
      else
      {
         tl_cursor->refresh(root_index);
      }

      std::string buf;
      if (tl_cursor->get(key_view(key.data(), key.size()), &buf) >= 0)
         return dwal_transaction::lookup_result::make_owned(std::move(buf));

      return {false, {}};
   }

}  // namespace psitri::dwal
