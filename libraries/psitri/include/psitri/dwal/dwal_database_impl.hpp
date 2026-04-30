#pragma once
#include <psitri/cursor.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction_impl.hpp>
#include <psitri/dwal/merge_pool_impl.hpp>
#include <psitri/dwal/transaction_impl.hpp>
#include <psitri/dwal/wal_reader.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session_impl.hpp>

#include <cassert>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace psitri::dwal
{
   template <class LockPolicy>
   basic_dwal_database<LockPolicy>::basic_dwal_database(std::shared_ptr<database_type> db,
                                                        std::filesystem::path          wal_dir,
                                                        dwal_config                    cfg)
       : _db(std::move(db)), _wal_dir(std::move(wal_dir)), _cfg(cfg)
   {
      std::filesystem::create_directories(_wal_dir);
      _wal_status = std::make_unique<wal_status_mapping>(_wal_dir);

      recover();

      if (_cfg.merge_threads > 0)
         _merge_pool = std::make_unique<merge_pool_type>(
             _db, _cfg.merge_threads, _epochs, _wal_dir, _cfg.max_rw_arena_bytes);
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::recover()
   {
      std::error_code ec;

      struct root_wal_info
      {
         uint32_t              root_index;
         std::filesystem::path ro_wal;
         std::filesystem::path rw_wal;
      };
      std::vector<root_wal_info> roots;

      for (auto& entry : std::filesystem::directory_iterator(_wal_dir, ec))
      {
         if (!entry.is_directory())
            continue;

         auto dirname = entry.path().filename().string();
         if (dirname.substr(0, 5) != "root-")
            continue;

         uint32_t root_index = 0;
         try
         {
            root_index = static_cast<uint32_t>(std::stoul(dirname.substr(5)));
         }
         catch (...)
         {
            continue;
         }

         if (root_index >= max_roots)
            continue;

         roots.push_back(
             {root_index, entry.path() / "wal-ro.dwal", entry.path() / "wal-rw.dwal"});
      }

      // Phase 1: Replay RO WALs into PsiTri (frozen data that wasn't merged).
      for (auto& ri : roots)
      {
         if (std::filesystem::exists(ri.ro_wal, ec))
         {
            replay_wal_to_tri(ri.root_index, ri.ro_wal);
            std::filesystem::remove(ri.ro_wal, ec);
         }
      }

      // Phase 2a: Scan all RW WALs to build multi-tx index.
      struct multi_tx_info
      {
         uint16_t participant_count = 0;
         uint16_t entries_found     = 0;
         bool     commit_seen       = false;
      };
      std::unordered_map<uint64_t, multi_tx_info> multi_tx_index;

      for (auto& ri : roots)
      {
         if (!std::filesystem::exists(ri.rw_wal, ec))
            continue;

         wal_reader reader;
         if (!reader.open(ri.rw_wal) || reader.was_clean_close())
            continue;

         wal_entry entry;
         while (reader.next(entry))
         {
            if (entry.is_multi_tx())
            {
               auto& info             = multi_tx_index[entry.multi_tx_id];
               info.participant_count = entry.multi_participant_count;
               info.entries_found++;
               if (entry.is_multi_tx_commit())
                  info.commit_seen = true;
            }
         }
      }

      std::unordered_set<uint64_t> committed_multi_txs;
      for (auto& [tx_id, info] : multi_tx_index)
      {
         if (info.commit_seen && info.entries_found == info.participant_count)
            committed_multi_txs.insert(tx_id);
      }

      // Phase 2b: Replay RW WALs into RW btrees, filtering multi-tx entries.
      for (auto& ri : roots)
      {
         if (!std::filesystem::exists(ri.rw_wal, ec))
            continue;

         wal_reader reader;
         if (!reader.open(ri.rw_wal) || reader.was_clean_close())
         {
            std::filesystem::remove(ri.rw_wal, ec);
            continue;
         }

         auto& root = ensure_root(ri.root_index);

         wal_entry entry;
         while (reader.next(entry))
         {
            if (entry.is_multi_tx() &&
                committed_multi_txs.find(entry.multi_tx_id) == committed_multi_txs.end())
               continue;

            for (auto& op : entry.ops)
            {
               switch (op.type)
               {
                  case wal_op_type::upsert_data:
                     root.rw_layer->store_data(op.key, op.value);
                     break;
                  case wal_op_type::upsert_subtree:
                     root.rw_layer->store_subtree(op.key, op.subtree);
                     break;
                  case wal_op_type::remove:
                     root.rw_layer->store_tombstone(op.key);
                     break;
                  case wal_op_type::remove_range:
                  {
                     std::vector<std::string> keys_to_erase;
                     auto it = root.rw_layer->map.lower_bound(op.range_low);
                     for (; it != root.rw_layer->map.end() && it.key() < op.range_high; ++it)
                        keys_to_erase.emplace_back(it.key());
                     for (auto& k : keys_to_erase)
                        root.rw_layer->erase(k);
                     root.rw_layer->tombstones.add(std::string(op.range_low),
                                                   std::string(op.range_high));
                     break;
                  }
               }
            }
         }

         root.next_wal_seq = reader.end_sequence();

         root.cow.set_root(root.rw_layer->map.snapshot_root());

         std::filesystem::remove(ri.rw_wal, ec);
      }
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::replay_wal_to_tri(
       uint32_t root_index, const std::filesystem::path& wal_path)
   {
      wal_reader reader;
      if (!reader.open(wal_path))
         return;

      auto ws = _db->start_write_session();
      auto tx = ws->start_transaction(root_index);

      uint64_t count = reader.replay_all(
          [&](const wal_entry& entry)
          {
             for (auto& op : entry.ops)
             {
                switch (op.type)
                {
                   case wal_op_type::upsert_data:
                      tx.upsert(op.key, op.value);
                      break;
                   case wal_op_type::upsert_subtree:
                   {
                      auto subtree = ws->make_ptr(op.subtree, /*retain=*/true);
                      if (subtree)
                         tx.upsert(op.key, std::move(subtree));
                      break;
                   }
                   case wal_op_type::remove:
                      tx.remove(op.key);
                      break;
                   case wal_op_type::remove_range:
                      tx.remove_range_any(op.range_low, op.range_high);
                      break;
                }
             }
          });

      if (count > 0)
         tx.commit();
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::replay_wal_to_rw(
       uint32_t root_index, const std::filesystem::path& wal_path)
   {
      wal_reader reader;
      if (!reader.open(wal_path))
         return;

      auto& root = ensure_root(root_index);

      uint64_t count = reader.replay_all(
          [&](const wal_entry& entry)
          {
             for (auto& op : entry.ops)
             {
                switch (op.type)
                {
                   case wal_op_type::upsert_data:
                      root.rw_layer->store_data(op.key, op.value);
                      break;
                   case wal_op_type::upsert_subtree:
                      root.rw_layer->store_subtree(op.key, op.subtree);
                      break;
                   case wal_op_type::remove:
                      root.rw_layer->store_tombstone(op.key);
                      break;
                   case wal_op_type::remove_range:
                   {
                      std::vector<std::string> keys_to_erase;
                      auto it = root.rw_layer->map.lower_bound(op.range_low);
                      for (; it != root.rw_layer->map.end() && it.key() < op.range_high; ++it)
                         keys_to_erase.emplace_back(it.key());
                      for (auto& k : keys_to_erase)
                         root.rw_layer->erase(k);
                      root.rw_layer->tombstones.add(std::string(op.range_low),
                                                    std::string(op.range_high));
                      break;
                   }
                }
             }
          });

      root.next_wal_seq = reader.end_sequence();
   }

   template <class LockPolicy>
   basic_dwal_database<LockPolicy>::~basic_dwal_database()
   {
      if (_merge_pool)
         _merge_pool->shutdown();

      clear_thread_local_cache();

      // Flush any remaining RW/RO data into the PsiTri trie before closing.
      {
         std::shared_ptr<write_session_type> ws;
         try
         {
            ws = _db->start_write_session();
         }
         catch (...)
         {
         }

         for (uint32_t i = 0; i < max_roots; ++i)
         {
            if (!_roots[i])
               continue;
            auto& root = *_roots[i];

            bool flushed = false;
            if (ws && (root.rw_layer || root.buffered_ptr))
            {
               try
               {
                  auto flush_layer = [&](btree_layer& layer)
                  {
                     auto tx = ws->start_transaction(i);
                     for (auto it = layer.map.begin(); it != layer.map.end(); ++it)
                     {
                        auto& val = it.value();
                        if (val.is_tombstone())
                           tx.remove(it.key());
                        else if (val.is_subtree())
                        {
                           auto subtree = ws->make_ptr(val.subtree, true);
                           if (subtree)
                              tx.upsert(it.key(), std::move(subtree));
                        }
                        else
                           tx.upsert(it.key(), val.data);
                     }
                     for (const auto& range : layer.tombstones.ranges())
                        tx.remove_range_any(range.low, range.high);
                     tx.commit();
                  };

                  std::shared_ptr<btree_layer> ro;
                  {
                     std::lock_guard lk(root.buffered_mutex);
                     ro = root.buffered_ptr;
                  }
                  if (ro && !ro->map.empty())
                     flush_layer(*ro);

                  if (root.rw_layer && !root.rw_layer->map.empty())
                     flush_layer(*root.rw_layer);

                  // Ensure each layer is bound to the shared allocator.
                  // `alloc->release` forwards to the calling thread's
                  // thread-local session, so the layer's destructor will
                  // run correctly regardless of which thread owns the
                  // last shared_ptr reference.
                  if (ro)
                     ro->set_allocator(&_db->underlying_allocator());
                  if (root.rw_layer)
                     root.rw_layer->set_allocator(&_db->underlying_allocator());

                  flushed = true;
               }
               catch (...)
               {
               }
            }
            else
            {
               flushed = true;
            }

            if (root.wal)
            {
               if (flushed)
                  root.wal->close();
            }
         }
      }
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::clear_thread_local_cache()
   {
      detail::thread_local_cache().reset();
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::clear_thread_local_caches()
   {
      detail::thread_local_cache().reset();
   }

   template <class LockPolicy>
   typename basic_dwal_database<LockPolicy>::dwal_root_type&
   basic_dwal_database<LockPolicy>::ensure_root(uint32_t index)
   {
      assert(index < max_roots);
      if (!_roots[index])
      {
         _roots[index] = std::make_unique<dwal_root_type>();
      }
      _roots[index]->status = _wal_status ? _wal_status->root(index) : nullptr;
      if (_roots[index]->status)
      {
         auto* status = _roots[index]->status;
         status->active.store(1, std::memory_order_relaxed);
         status->generation.store(_roots[index]->generation.load(std::memory_order_relaxed),
                                  std::memory_order_relaxed);
         status->merge_complete.store(
            _roots[index]->merge_complete.load(std::memory_order_relaxed) ? 1 : 0,
            std::memory_order_relaxed);
         status->tri_root.store(_roots[index]->tri_root.load(std::memory_order_relaxed),
                                std::memory_order_relaxed);
         status->ro_base_root.store(
            _roots[index]->ro_base_root.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
         status->throttle_sleep_ns.store(
            _roots[index]->throttle_sleep_ns.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
         status->arena_at_merge_complete.store(
            _roots[index]->arena_at_merge_complete.load(std::memory_order_relaxed),
            std::memory_order_relaxed);
         if (_roots[index]->rw_layer)
         {
            status->rw_layer_entries.store(_roots[index]->rw_layer->size(),
                                           std::memory_order_relaxed);
            status->rw_arena_bytes.store(_roots[index]->rw_layer->map.arena_capacity(),
                                         std::memory_order_relaxed);
         }
      }
      // Bind the allocator so the btree_layer can retain/release subtree
      // addresses. `sal::allocator::retain` is atomic and `release`
      // forwards to the calling thread's thread-local session, so the
      // same allocator pointer is safe from any thread (writer, merge
      // worker, or the database-destructor flush thread).
      if (_roots[index]->rw_layer && !_roots[index]->rw_layer->alloc)
         _roots[index]->rw_layer->set_allocator(&_db->underlying_allocator());
      return *_roots[index];
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::ensure_wal(uint32_t root_index)
   {
      auto& root = ensure_root(root_index);
      if (!root.wal)
      {
         auto dir = _wal_dir / ("root-" + std::to_string(root_index));
         std::filesystem::create_directories(dir);
         root.wal = std::make_unique<wal_writer>(
             dir / "wal-rw.dwal", static_cast<uint16_t>(root_index), root.next_wal_seq,
             root.status);
      }
   }

   template <class LockPolicy>
   typename basic_dwal_database<LockPolicy>::dwal_transaction_type
   basic_dwal_database<LockPolicy>::start_write_transaction(uint32_t         root_index,
                                                            transaction_mode mode)
   {
      assert(root_index < max_roots);

      if (mode == transaction_mode::direct)
         throw std::runtime_error("dwal: direct transaction mode not yet implemented");

      auto& root = ensure_root(root_index);

      if (_cfg.max_rw_arena_bytes > 0 &&
          root.rw_layer->map.arena_capacity() >= _cfg.max_rw_arena_bytes)
      {
         root.merge_complete.wait(false, std::memory_order_acquire);
         try_swap_rw_to_ro(root_index);
      }

      ensure_wal(root_index);

      return dwal_transaction_type(root, root.wal.get(), root_index, this);
   }

   template <class LockPolicy>
   typename basic_dwal_database<LockPolicy>::transaction_type
   basic_dwal_database<LockPolicy>::start_transaction(
       std::initializer_list<uint32_t> write_roots,
       std::initializer_list<uint32_t> read_roots)
   {
      return transaction_type(*this, write_roots, read_roots);
   }

   template <class LockPolicy>
   typename basic_dwal_database<LockPolicy>::transaction_type
   basic_dwal_database<LockPolicy>::start_transaction(uint32_t root_index)
   {
      return transaction_type(*this, {root_index});
   }

   template <class LockPolicy>
   uint64_t basic_dwal_database<LockPolicy>::next_multi_tx_id() noexcept
   {
      return _next_multi_tx_id.fetch_add(1, std::memory_order_relaxed);
   }

   template <class LockPolicy>
   typename basic_dwal_database<LockPolicy>::lookup_result
   basic_dwal_database<LockPolicy>::get(uint32_t         root_index,
                                        std::string_view key,
                                        read_mode        mode)
   {
      if (mode == read_mode::trie)
         return {false, {}};

      assert(root_index < max_roots);
      if (!_roots[root_index])
         return {false, {}};

      auto& root = *_roots[root_index];

      if (root.generation.load(std::memory_order_acquire) == 0)
         return {false, {}};

      std::shared_ptr<btree_layer> ro;
      {
         std::lock_guard lk(root.buffered_mutex);
         ro = root.buffered_ptr;
      }
      if (ro)
      {
         auto* v = ro->map.get(key);
         if (v)
         {
            if (v->is_tombstone())
               return {false, {}};
            return {true, *v};
         }
         if (ro->tombstones.is_deleted(key))
            return {false, {}};
      }

      return {false, {}};
   }

   template <class LockPolicy>
   bool basic_dwal_database<LockPolicy>::should_swap(uint32_t root_index) const
   {
      auto& root = *_roots[root_index];
      if (root.rw_layer->size() >= _cfg.max_rw_entries)
         return true;
      if (root.wal && root.wal->file_size() >= _cfg.max_wal_bytes)
         return true;
      if (_cfg.max_freshness_delay.count() > 0 && !root.rw_layer->empty())
      {
         auto elapsed = std::chrono::steady_clock::now() - root.last_snapshot_time;
         if (elapsed >= _cfg.max_freshness_delay)
            return true;
      }
      return false;
   }

   template <class LockPolicy>
   bool basic_dwal_database<LockPolicy>::should_backpressure(uint32_t root_index) const
   {
      if (_cfg.max_rw_arena_bytes == 0)
         return false;
      auto& root = *_roots[root_index];
      return root.rw_layer->map.arena_capacity() >= _cfg.max_rw_arena_bytes;
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::try_swap_rw_to_ro(uint32_t root_index)
   {
      assert(root_index < max_roots && _roots[root_index]);
      auto& root = *_roots[root_index];

      if (!root.merge_complete.load(std::memory_order_acquire))
         return;

      {
         {
            std::lock_guard lk(root.buffered_mutex);
            root.buffered_ptr = std::move(root.rw_layer);
         }
         root.rw_layer = std::make_shared<btree_layer>();
         root.rw_layer->set_allocator(&_db->underlying_allocator());

         root.cow.reset();
      }

      root.last_snapshot_time = std::chrono::steady_clock::now();

      uint32_t base = root.tri_root.load(std::memory_order_acquire);
      root.ro_base_root.store(base, std::memory_order_relaxed);

      root.merge_complete.store(false, std::memory_order_release);

      root.generation.fetch_add(1, std::memory_order_release);
      if (root.status)
      {
         root.status->swaps.fetch_add(1, std::memory_order_relaxed);
         root.status->generation.store(root.generation.load(std::memory_order_relaxed),
                                       std::memory_order_relaxed);
         root.status->merge_complete.store(0, std::memory_order_relaxed);
         root.status->ro_base_root.store(base, std::memory_order_relaxed);
         root.status->rw_layer_entries.store(root.rw_layer ? root.rw_layer->size() : 0,
                                             std::memory_order_relaxed);
         root.status->rw_arena_bytes.store(
            root.rw_layer ? root.rw_layer->map.arena_capacity() : 0,
            std::memory_order_relaxed);
         std::lock_guard lk(root.buffered_mutex);
         root.status->ro_layer_entries.store(root.buffered_ptr ? root.buffered_ptr->size() : 0,
                                             std::memory_order_relaxed);
         root.status->ro_arena_bytes.store(
            root.buffered_ptr ? root.buffered_ptr->map.arena_capacity() : 0,
            std::memory_order_relaxed);
      }

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
         if (root.status)
         {
            std::error_code ec;
            auto ro_wal_bytes = std::filesystem::file_size(ro_wal, ec);
            root.status->ro_wal_file_bytes.store(ec ? 0 : ro_wal_bytes,
                                                 std::memory_order_relaxed);
         }

         root.wal = std::make_unique<wal_writer>(
             rw_wal, static_cast<uint16_t>(root_index), root.next_wal_seq,
             root.status);
      }

      _epochs.broadcast_all(root.generation.load(std::memory_order_relaxed));

      if (_merge_pool)
      {
         if (root.status)
            root.status->merge_requests.fetch_add(1, std::memory_order_relaxed);
         _merge_pool->signal(root_index, root);
      }
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::swap_rw_to_ro(uint32_t root_index)
   {
      try_swap_rw_to_ro(root_index);
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::flush_wal()
   {
      flush_wal(sal::sync_type::full);
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::flush_wal(sal::sync_type sync)
   {
      for (uint32_t i = 0; i < max_roots; ++i)
      {
         if (_roots[i] && _roots[i]->wal)
            _roots[i]->wal->flush(sync);
      }
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::flush_wal(uint32_t root_index)
   {
      flush_wal(root_index, sal::sync_type::full);
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::flush_wal(uint32_t root_index, sal::sync_type sync)
   {
      assert(root_index < max_roots);
      if (_roots[root_index] && _roots[root_index]->wal)
         _roots[root_index]->wal->flush(sync);
   }

   template <class LockPolicy>
   typename basic_dwal_database<LockPolicy>::lookup_result
   basic_dwal_database<LockPolicy>::get_latest(uint32_t root_index, std::string_view key)
   {
      assert(root_index < max_roots);

      if (_roots[root_index])
      {
         auto& root = *_roots[root_index];

         {
            art::offset_t head = root.cow.begin_read_latest();

            if (head != art::null_offset && root.rw_layer)
            {
               auto& arena = root.rw_layer->map.get_arena();
               auto* v     = art::get<btree_value>(arena, head, key);
               if (v)
               {
                  root.cow.end_read_latest();
                  if (v->is_tombstone())
                     return {false, {}};
                  return {true, *v};
               }
               if (root.rw_layer->tombstones.is_deleted(key))
               {
                  root.cow.end_read_latest();
                  return {false, {}};
               }
            }

            root.cow.end_read_latest();
         }

         {
            std::shared_ptr<btree_layer> ro;
            {
               std::lock_guard lk(root.buffered_mutex);
               ro = root.buffered_ptr;
            }
            if (ro)
            {
               auto* v = ro->map.get(key);
               if (v)
               {
                  if (v->is_tombstone())
                     return {false, {}};
                  return {true, *v};
               }
               if (ro->tombstones.is_deleted(key))
                  return {false, {}};
            }
         }
      }

      return tri_get(root_index, key);
   }

   template <class LockPolicy>
   owned_merge_cursor basic_dwal_database<LockPolicy>::create_cursor(uint32_t  root_index,
                                                                     read_mode mode,
                                                                     bool      skip_rw_lock)
   {
      assert(root_index < max_roots);

      std::shared_ptr<btree_layer> rw, ro;

      if (_roots[root_index])
      {
         auto& root = *_roots[root_index];

         if (mode == read_mode::latest || mode == read_mode::fresh)
            rw = root.rw_layer;

         if (mode != read_mode::trie)
         {
            std::lock_guard lk(root.buffered_mutex);
            ro = root.buffered_ptr;
         }
      }

      auto& tlc      = detail::thread_local_cache();
      auto  db_owner = std::static_pointer_cast<void>(_db);
      if (!detail::same_owner(tlc.cursor_db.lock(), db_owner))
      {
         auto rs            = _db->start_read_session();
         tlc.cursor_session = std::static_pointer_cast<void>(rs);
         tlc.cursor_db      = db_owner;
      }
      auto session =
          std::static_pointer_cast<read_session_type>(tlc.cursor_session);

      std::optional<psitri::cursor> tri_cursor;
      tri_cursor.emplace(session->create_cursor(root_index));

      return owned_merge_cursor(std::move(rw), std::move(ro), std::move(tri_cursor));
   }

   template <class LockPolicy>
   psitri::cursor basic_dwal_database<LockPolicy>::create_tri_cursor(uint32_t root_index)
   {
      auto& tlc      = detail::thread_local_cache();
      auto  db_owner = std::static_pointer_cast<void>(_db);
      if (!detail::same_owner(tlc.cursor_db.lock(), db_owner))
      {
         auto rs            = _db->start_read_session();
         tlc.cursor_session = std::static_pointer_cast<void>(rs);
         tlc.cursor_db      = db_owner;
      }
      auto session =
          std::static_pointer_cast<read_session_type>(tlc.cursor_session);
      return session->create_cursor(root_index);
   }

   template <class LockPolicy>
   typename basic_dwal_database<LockPolicy>::lookup_result
   basic_dwal_database<LockPolicy>::tri_get(uint32_t root_index, std::string_view key)
   {
      auto& tlc      = detail::thread_local_cache();
      auto  db_owner = std::static_pointer_cast<void>(_db);

      if (!detail::same_owner(tlc.tri_db.lock(), db_owner))
      {
         delete tlc.tri_cursor;
         tlc.tri_cursor  = nullptr;
         auto rs         = _db->start_read_session();
         tlc.tri_session = std::static_pointer_cast<void>(rs);
         tlc.tri_db      = db_owner;
      }

      auto session =
          std::static_pointer_cast<read_session_type>(tlc.tri_session);

      if (!tlc.tri_cursor)
      {
         auto root      = session->create_cursor(root_index);
         tlc.tri_cursor = new psitri::cursor(std::move(root));
      }
      else
      {
         tlc.tri_cursor->refresh(root_index);
      }

      std::string buf;
      if (tlc.tri_cursor->get(key_view(key.data(), key.size()), &buf) >= 0)
         return lookup_result::make_owned(std::move(buf));

      return {false, {}};
   }

   template <class LockPolicy>
   void basic_dwal_database<LockPolicy>::request_shutdown()
   {
      if (_merge_pool)
      {
         _merge_pool->request_stop();
      }

      for (uint32_t i = 0; i < max_roots; ++i)
      {
         if (_roots[i])
         {
            _roots[i]->merge_complete.store(true, std::memory_order_release);
            _roots[i]->merge_complete.notify_all();
         }
      }
   }

}  // namespace psitri::dwal
