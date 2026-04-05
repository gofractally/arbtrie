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
   dwal_database::dwal_database(std::shared_ptr<psitri::database> db,
                                std::filesystem::path              wal_dir,
                                dwal_config                        cfg)
       : _db(std::move(db)), _wal_dir(std::move(wal_dir)), _cfg(cfg)
   {
      std::filesystem::create_directories(_wal_dir);

      recover();

      if (_cfg.merge_threads > 0)
         _merge_pool = std::make_unique<merge_pool>(_db, _cfg.merge_threads, _epochs, _wal_dir);
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

         // Layer 1: RW btree (writer-private — only safe from writer thread)
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

   owned_merge_cursor dwal_database::create_cursor(uint32_t root_index, read_mode mode)
   {
      assert(root_index < max_roots);

      std::shared_ptr<btree_layer> rw, ro;

      if (_roots[root_index])
      {
         auto& root = *_roots[root_index];

         // RW layer: only included for latest mode (writer-thread only, no lock needed)
         if (mode == read_mode::latest)
            rw = root.rw_layer;

         // RO layer: included for latest and buffered modes
         if (mode != read_mode::persistent)
         {
            std::shared_lock lk(root.buffered_mutex);
            ro = root.buffered_ptr;
         }
      }

      // Tri layer: always included
      thread_local std::shared_ptr<psitri::read_session> tl_session;
      thread_local psitri::database*                     tl_db = nullptr;

      if (tl_db != _db.get())
      {
         tl_session = _db->start_read_session();
         tl_db      = _db.get();
      }

      std::optional<psitri::cursor> tri_cursor;
      tri_cursor.emplace(tl_session->create_cursor(root_index));

      return owned_merge_cursor(std::move(rw), std::move(ro), std::move(tri_cursor));
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
