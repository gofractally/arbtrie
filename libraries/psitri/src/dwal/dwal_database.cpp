#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/transaction.hpp>

#include <psitri/cursor.hpp>
#include <psitri/database.hpp>
#include <psitri/dwal/wal_reader.hpp>
#include <psitri/read_session.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/write_session.hpp>
#include <psitri/write_session_impl.hpp>

#include <cassert>
#include <shared_mutex>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace psitri::dwal
{
   // Thread-local cache for PsiTri read sessions used by create_cursor and tri_get.
   // Extracted to namespace scope so clear_thread_local_cache() can reset them.
   struct tl_cache
   {
      // Used by create_cursor (Tri layer)
      std::shared_ptr<psitri::read_session> cursor_session;
      psitri::database*                     cursor_db = nullptr;

      // Used by tri_get
      std::shared_ptr<psitri::read_session> tri_session;
      psitri::cursor*                       tri_cursor = nullptr;
      psitri::database*                     tri_db     = nullptr;

      void reset()
      {
         cursor_session.reset();
         cursor_db = nullptr;
         delete tri_cursor;
         tri_cursor = nullptr;
         tri_session.reset();
         tri_db = nullptr;
      }
   };
   static thread_local tl_cache s_tl_cache;

   dwal_database::dwal_database(std::shared_ptr<psitri::database> db,
                                std::filesystem::path              wal_dir,
                                dwal_config                        cfg)
       : _db(std::move(db)), _wal_dir(std::move(wal_dir)), _cfg(cfg)
   {
      std::filesystem::create_directories(_wal_dir);

      recover();

      if (_cfg.merge_threads > 0)
         _merge_pool = std::make_unique<merge_pool>(_db, _cfg.merge_threads, _epochs, _wal_dir,
                                                    _cfg.max_rw_arena_bytes);
   }

   void dwal_database::recover()
   {
      std::error_code ec;

      // Collect root directories and their WAL paths.
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
      // RO WALs never contain multi-tx entries (swap only happens after commit).
      for (auto& ri : roots)
      {
         if (std::filesystem::exists(ri.ro_wal, ec))
         {
            replay_wal_to_tri(ri.root_index, ri.ro_wal);
            std::filesystem::remove(ri.ro_wal, ec);
         }
      }

      // Phase 2a: Scan all RW WALs to build multi-tx index.
      // For each multi_tx_id, track how many participants we found and
      // whether the commit flag was seen.
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
               auto& info = multi_tx_index[entry.multi_tx_id];
               info.participant_count = entry.multi_participant_count;
               info.entries_found++;
               if (entry.is_multi_tx_commit())
                  info.commit_seen = true;
            }
         }
      }

      // Determine which multi-tx IDs are complete (safe to replay).
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
            // Skip incomplete multi-root transactions.
            if (entry.is_multi_tx() &&
                committed_multi_txs.find(entry.multi_tx_id) == committed_multi_txs.end())
               continue;

            // Replay this entry's ops into the RW btree.
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
                        root.rw_layer->map.erase(k);
                     root.rw_layer->tombstones.add(std::string(op.range_low),
                                                   std::string(op.range_high));
                     break;
                  }
               }
            }
         }

         root.next_wal_seq = reader.end_sequence();

         // Delete the old RW WAL — a fresh one will be created on first write.
         std::filesystem::remove(ri.rw_wal, ec);
      }
   }

   void dwal_database::replay_wal_to_tri(uint32_t                       root_index,
                                          const std::filesystem::path&   wal_path)
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
                      tx.remove_range(op.range_low, op.range_high);
                      break;
                }
             }
          });

      if (count > 0)
         tx.commit();
   }

   void dwal_database::replay_wal_to_rw(uint32_t                       root_index,
                                          const std::filesystem::path&   wal_path)
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
                      // Erase buffered keys in range, then add range tombstone.
                      std::vector<std::string> keys_to_erase;
                      auto it = root.rw_layer->map.lower_bound(op.range_low);
                      for (; it != root.rw_layer->map.end() && it.key() < op.range_high; ++it)
                         keys_to_erase.emplace_back(it.key());
                      for (auto& k : keys_to_erase)
                         root.rw_layer->map.erase(k);
                      root.rw_layer->tombstones.add(std::string(op.range_low),
                                                    std::string(op.range_high));
                      break;
                   }
                }
             }
          });

      // Set the WAL sequence so the next WAL file starts at the right point.
      root.next_wal_seq = reader.end_sequence();
   }

   dwal_database::~dwal_database()
   {
      if (_merge_pool)
         _merge_pool->shutdown();

      // Clear thread-local caches on the destroying thread to release
      // shared_ptr references to the underlying database.
      clear_thread_local_cache();

      // Flush any remaining RW/RO data into the PsiTri trie before closing.
      // Without this, a clean close marks the WAL as clean_close and recovery
      // skips replay — losing any data that wasn't merged yet.
      {
         std::shared_ptr<psitri::write_session> ws;
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
                           auto subtree = ws->make_ptr(val.subtree_root, true);
                           if (subtree)
                              tx.upsert(it.key(), std::move(subtree));
                        }
                        else
                           tx.upsert(it.key(), val.data);
                     }
                     for (const auto& range : layer.tombstones.ranges())
                        tx.remove_range(range.low, range.high);
                     tx.commit();
                  };

                  // Drain RO layer.
                  std::shared_ptr<btree_layer> ro;
                  {
                     std::shared_lock lk(root.buffered_mutex);
                     ro = root.buffered_ptr;
                  }
                  if (ro && !ro->map.empty())
                     flush_layer(*ro);

                  // Drain RW layer.
                  if (root.rw_layer && !root.rw_layer->map.empty())
                     flush_layer(*root.rw_layer);

                  flushed = true;
               }
               catch (...)
               {
               }
            }
            else
            {
               // No data to flush — safe to mark clean.
               flushed = true;
            }

            if (root.wal)
            {
               if (flushed)
                  root.wal->close();  // Sets clean_close flag.
               // If !flushed, WAL left without clean_close — recovery will replay.
            }
         }
      }
   }

   void dwal_database::clear_thread_local_cache()
   {
      s_tl_cache.reset();
   }

   void dwal_database::clear_thread_local_caches()
   {
      s_tl_cache.reset();
   }

   dwal_root& dwal_database::ensure_root(uint32_t index)
   {
      assert(index < max_roots);
      if (!_roots[index])
      {
         _roots[index] = std::make_unique<dwal_root>();
         _roots[index]->enable_rw_locking = _cfg.enable_rw_locking;
      }
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

      // Backpressure: if the RW arena capacity is approaching the uint32_t
      // offset limit, wait for the merge to finish so we can swap to a
      // fresh tree.  We check capacity (not bytes_used) because the arena
      // doubles on growth — once capacity reaches the limit, the next
      // doubling would overflow uint32_t.
      if (_cfg.max_rw_arena_bytes > 0 &&
          root.rw_layer->map.arena_capacity() >= _cfg.max_rw_arena_bytes)
      {
         root.merge_complete.wait(false, std::memory_order_acquire);
         try_swap_rw_to_ro(root_index);
      }

      ensure_wal(root_index);

      return dwal_transaction(root, root.wal.get(), root_index, this);
   }

   transaction dwal_database::start_transaction(std::initializer_list<uint32_t> write_roots,
                                                std::initializer_list<uint32_t> read_roots)
   {
      return transaction(*this, write_roots, read_roots);
   }

   transaction dwal_database::start_transaction(uint32_t root_index)
   {
      return transaction(*this, {root_index});
   }

   uint64_t dwal_database::next_multi_tx_id() noexcept
   {
      return _next_multi_tx_id.fetch_add(1, std::memory_order_relaxed);
   }

   dwal_transaction::lookup_result dwal_database::get(uint32_t         root_index,
                                                      std::string_view key,
                                                      read_mode        mode)
   {
      if (mode == read_mode::trie)
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

   bool dwal_database::should_swap(uint32_t root_index) const
   {
      auto& root = *_roots[root_index];
      if (root.rw_layer->size() >= _cfg.max_rw_entries)
         return true;
      if (root.wal && root.wal->file_size() >= _cfg.max_wal_bytes)
         return true;
      // Time-based flush: swap if max_flush_delay has elapsed and
      // the RW layer has data worth swapping.
      if (_cfg.max_flush_delay.count() > 0 && !root.rw_layer->empty())
      {
         auto elapsed = std::chrono::steady_clock::now() - root.last_swap_time;
         if (elapsed >= _cfg.max_flush_delay)
            return true;
      }
      return false;
   }

   bool dwal_database::should_backpressure(uint32_t root_index) const
   {
      if (_cfg.max_rw_arena_bytes == 0)
         return false;
      auto& root = *_roots[root_index];
      return root.rw_layer->map.arena_capacity() >= _cfg.max_rw_arena_bytes;
   }

   void dwal_database::try_swap_rw_to_ro(uint32_t root_index)
   {
      assert(root_index < max_roots && _roots[root_index]);
      auto& root = *_roots[root_index];

      if (!root.merge_complete.load(std::memory_order_acquire))
         return;

      // Freeze current RW as the new buffered RO.
      // Caller must ensure no concurrent mutation of rw_layer.
      // When called from dwal_transaction::commit(), the writer has
      // finished mutations and released any locks.
      {
         std::unique_lock rw_lk(root.rw_mutex, std::defer_lock);
         if (root.enable_rw_locking)
            rw_lk.lock();
         {
            std::unique_lock lk(root.buffered_mutex);
            root.buffered_ptr = std::move(root.rw_layer);
         }
         root.rw_layer = std::make_shared<btree_layer>();
      }

      root.last_swap_time = std::chrono::steady_clock::now();

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
      flush_wal(sal::sync_type::full);
   }

   void dwal_database::flush_wal(sal::sync_type sync)
   {
      for (uint32_t i = 0; i < max_roots; ++i)
      {
         if (_roots[i] && _roots[i]->wal)
            _roots[i]->wal->flush(sync);
      }
   }

   void dwal_database::flush_wal(uint32_t root_index)
   {
      flush_wal(root_index, sal::sync_type::full);
   }

   void dwal_database::flush_wal(uint32_t root_index, sal::sync_type sync)
   {
      assert(root_index < max_roots);
      if (_roots[root_index] && _roots[root_index]->wal)
         _roots[root_index]->wal->flush(sync);
   }

   dwal_transaction::lookup_result dwal_database::get_latest(uint32_t         root_index,
                                                              std::string_view key)
   {
      assert(root_index < max_roots);

      if (_roots[root_index])
      {
         auto& root = *_roots[root_index];

         // Layer 1: RW btree — shared lock allows concurrent readers
         // when rw_locking is enabled.
         {
            std::shared_lock lk(root.rw_mutex, std::defer_lock);
            if (root.enable_rw_locking)
               lk.lock();
            if (root.rw_layer)
            {
               auto* v = root.rw_layer->map.get(key);
               if (v)
               {
                  if (v->is_tombstone())
                     return {false, {}};
                  return {true, *v};
               }
               if (root.rw_layer->tombstones.is_deleted(key))
                  return {false, {}};
            }
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

      // Layer 3: PsiTri COW tree
      return tri_get(root_index, key);
   }

   owned_merge_cursor dwal_database::create_cursor(uint32_t root_index, read_mode mode,
                                                    bool skip_rw_lock)
   {
      assert(root_index < max_roots);

      // Fresh mode: if there's swappable data and the merge slot is free,
      // signal the writer/merge thread to swap RW→RO and wait briefly.
      // Falls back to buffered in all other cases (RW empty, RO already
      // has data, merge in progress, or timeout).
      if (mode == read_mode::fresh && _roots[root_index])
      {
         auto& root = *_roots[root_index];
         // Wait only if there's data in RW to swap and the merge slot is free
         bool need_wait = !root.rw_layer->empty()
                       && root.merge_complete.load(std::memory_order_acquire);

         if (need_wait)
         {
            auto gen_before = root.generation.load(std::memory_order_acquire);
            root.readers_want_swap.store(true, std::memory_order_release);
            auto timeout = _cfg.max_flush_delay.count() > 0
               ? _cfg.max_flush_delay
               : std::chrono::milliseconds(10);
            std::shared_lock lk(root.swap_mutex);
            root.swap_cv.wait_for(lk, timeout, [&] {
               // A swap happened if the generation counter advanced
               return root.generation.load(std::memory_order_acquire) != gen_before;
            });
         }

         mode = read_mode::buffered;
      }

      std::shared_ptr<btree_layer> rw, ro;

      if (_roots[root_index])
      {
         auto& root = *_roots[root_index];

         // RW layer: included for latest mode.
         if (mode == read_mode::latest)
         {
            std::shared_lock lk(root.rw_mutex, std::defer_lock);
            if (root.enable_rw_locking && !skip_rw_lock)
               lk.lock();
            rw = root.rw_layer;
         }

         // RO layer: included for latest and buffered modes
         if (mode != read_mode::trie)
         {
            std::shared_lock lk(root.buffered_mutex);
            ro = root.buffered_ptr;
         }
      }

      // Tri layer: always included
      auto& tlc = s_tl_cache;
      if (tlc.cursor_db != _db.get())
      {
         tlc.cursor_session = _db->start_read_session();
         tlc.cursor_db      = _db.get();
      }

      std::optional<psitri::cursor> tri_cursor;
      tri_cursor.emplace(tlc.cursor_session->create_cursor(root_index));

      return owned_merge_cursor(std::move(rw), std::move(ro), std::move(tri_cursor));
   }

   psitri::cursor dwal_database::create_tri_cursor(uint32_t root_index)
   {
      auto& tlc = s_tl_cache;
      if (tlc.cursor_db != _db.get())
      {
         tlc.cursor_session = _db->start_read_session();
         tlc.cursor_db      = _db.get();
      }
      return tlc.cursor_session->create_cursor(root_index);
   }

   dwal_transaction::lookup_result dwal_database::tri_get(uint32_t         root_index,
                                                         std::string_view key)
   {
      // Thread-local read session + cursor for Tri-layer lookups.
      // Each calling thread gets its own session (safe for concurrent readers/writers).
      auto& tlc = s_tl_cache;

      // Re-create session if the database changed (shouldn't happen, but safe).
      if (tlc.tri_db != _db.get())
      {
         delete tlc.tri_cursor;
         tlc.tri_cursor = nullptr;
         tlc.tri_session = _db->start_read_session();
         tlc.tri_db      = _db.get();
      }

      if (!tlc.tri_cursor)
      {
         auto root = tlc.tri_session->create_cursor(root_index);
         tlc.tri_cursor = new psitri::cursor(std::move(root));
      }
      else
      {
         tlc.tri_cursor->refresh(root_index);
      }

      std::string buf;
      if (tlc.tri_cursor->get(key_view(key.data(), key.size()), &buf) >= 0)
         return dwal_transaction::lookup_result::make_owned(std::move(buf));

      return {false, {}};
   }

   void dwal_database::request_shutdown()
   {
      // Signal the merge pool to abort in-flight merges (non-blocking —
      // only sets the shutdown flag and notifies the queue CV).
      if (_merge_pool)
      {
         _merge_pool->request_stop();
      }

      // Wake any writer blocked on merge backpressure.
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
