#pragma once
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>

#include <cassert>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace psitri::dwal
{
   template <class LockPolicy>
   basic_dwal_transaction<LockPolicy>::basic_dwal_transaction(dwal_root_type& root,
                                                              wal_writer*     wal,
                                                              uint32_t        root_index,
                                                              database_type*  db,
                                                              bool            nested,
                                                              root_mode       mode)
       : _root(&root),
         _wal(wal),
         _db(db),
         _root_index(root_index),
         _nested(nested),
         _mode(mode)
   {
      if (_mode == root_mode::read_write)
      {
         // COWART: begin write — sets writer_active, bumps cow_seq if readers active
         if (!_nested)
         {
            uint32_t old_seq = _root->cow.load_state().cow_seq();
            uint32_t new_seq = _root->cow.begin_write();
            if (new_seq != old_seq)
               _root->rw_layer->map.bump_cow_seq();
         }

         _undo.push_frame();

         if (!_nested && _wal)
            _wal->begin_entry();
      }
   }

   template <class LockPolicy>
   basic_dwal_transaction<LockPolicy>::~basic_dwal_transaction()
   {
      if (!_committed && !_aborted && _root)
         abort();
   }

   template <class LockPolicy>
   basic_dwal_transaction<LockPolicy>::basic_dwal_transaction(
       basic_dwal_transaction&& other) noexcept
       : _root(std::exchange(other._root, nullptr)),
         _wal(std::exchange(other._wal, nullptr)),
         _db(std::exchange(other._db, nullptr)),
         _root_index(other._root_index),
         _committed(other._committed),
         _aborted(other._aborted),
         _nested(other._nested),
         _mode(other._mode),
         _undo(std::move(other._undo))
   {
      other._committed = true;  // prevent double-abort in moved-from dtor
   }

   // ── Mutations ────────────────────────────────────────────────────

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::upsert(std::string_view key, std::string_view value)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      record_undo_for_upsert(key);
      _root->rw_layer->store_data(key, value);

      if (!_nested && _wal)
         _wal->add_upsert_data(key, value);
   }

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::upsert_subtree(std::string_view key,
                                                           sal::tree_id     tid)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      record_undo_for_upsert(key);
      _root->rw_layer->store_subtree(key, tid);

      if (!_nested && _wal)
         _wal->add_upsert_subtree(key, tid);
   }

   template <class LockPolicy>
   typename basic_dwal_transaction<LockPolicy>::remove_result_type
   basic_dwal_transaction<LockPolicy>::remove(std::string_view key)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      auto& layer = *_root->rw_layer;
      auto* v     = layer.map.get(key);
      bool  in_rw = v && !v->is_tombstone();

      record_undo_for_remove(key);

      _root->rw_layer->store_tombstone(key);

      if (!_nested && _wal)
         _wal->add_remove(key);

      if (in_rw)
         return remove_result_type(true);

      return remove_result_type(this, std::string(key));
   }

   template <class LockPolicy>
   bool basic_dwal_transaction<LockPolicy>::exists_in_lower_layers(std::string_view key) const
   {
      auto ro = ro_get(key);
      if (ro.found)
         return true;
      if (ro.value.is_tombstone())
         return false;

      if (_db)
      {
         auto tri = _db->tri_get(_root_index, key);
         return tri.found;
      }
      return false;
   }

   template <class LockPolicy>
   void basic_remove_result<LockPolicy>::resolve()
   {
      _resolved = true;
      _existed  = _tx && _tx->exists_in_lower_layers(_key);
   }

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::remove_range(std::string_view low,
                                                         std::string_view high)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      auto& layer = *_root->rw_layer;

      std::vector<undo_entry::range_data::buffered_entry> buffered;
      {
         auto it = layer.map.lower_bound(low);
         for (; it != layer.map.end() && it.key() < high; ++it)
         {
            if (!it.value().is_tombstone())
               buffered.push_back({std::string_view(it.key()), it.value()});
         }
      }

      {
         std::vector<std::string> keys_to_erase;
         auto it = layer.map.lower_bound(low);
         for (; it != layer.map.end() && it.key() < high; ++it)
            keys_to_erase.emplace_back(it.key());
         for (auto& k : keys_to_erase)
            layer.erase(k);
      }

      layer.tombstones.add(std::string(low), std::string(high));

      _undo.record_erase_range(low, high, std::move(buffered));

      if (!_nested && _wal)
         _wal->add_remove_range(low, high);
   }

   // ── Point Reads ──────────────────────────────────────────────────

   template <class LockPolicy>
   typename basic_dwal_transaction<LockPolicy>::lookup_result
   basic_dwal_transaction<LockPolicy>::get(std::string_view key) const
   {
      assert(_root);

      // Layer 1: RW btree
      {
         auto* v = _root->rw_layer->map.get(key);
         if (v)
         {
            if (v->is_tombstone())
               return {false, {}};
            return {true, *v};
         }
         if (_root->rw_layer->tombstones.is_deleted(key))
            return {false, {}};
      }

      // Layer 2: RO btree (if active)
      {
         auto result = ro_get(key);
         if (result.found || result.value.is_tombstone())
            return result;
      }

      // Layer 3: PsiTri COW tree (via dwal_database)
      if (_db)
         return _db->tri_get(_root_index, key);

      return {false, {}};
   }

   // ── Cursor ────────────────────────────────────────────────────────

   template <class LockPolicy>
   owned_merge_cursor basic_dwal_transaction<LockPolicy>::create_cursor() const
   {
      assert(_root && !_committed && !_aborted);

      std::shared_ptr<btree_layer> rw = _root->rw_layer;

      std::shared_ptr<btree_layer> ro;
      {
         std::lock_guard lk(_root->buffered_mutex);
         ro = _root->buffered_ptr;
      }

      std::optional<psitri::cursor> tri;
      if (_db)
         tri.emplace(_db->create_tri_cursor(_root_index));

      return owned_merge_cursor(std::move(rw), std::move(ro), std::move(tri));
   }

   template <class LockPolicy>
   typename basic_dwal_transaction<LockPolicy>::lookup_result
   basic_dwal_transaction<LockPolicy>::ro_get(std::string_view key) const
   {
      std::shared_ptr<btree_layer> ro;
      {
         std::lock_guard lk(_root->buffered_mutex);
         ro = _root->buffered_ptr;
      }
      if (!ro)
         return {false, {}};

      auto* v = ro->map.get(key);
      if (v)
      {
         if (v->is_tombstone())
            return {false, btree_value::make_tombstone()};
         return {true, *v};
      }
      if (ro->tombstones.is_deleted(key))
         return {false, btree_value::make_tombstone()};

      return {false, {}};
   }

   // ── Transaction Control ──────────────────────────────────────────

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::commit()
   {
      assert(_root && !_committed && !_aborted);
      _committed = true;

      if (_mode == root_mode::read_only)
         return;

      if (!_nested)
      {
         if (_wal)
            _wal->commit_entry();

         _undo.discard();

         {
            auto     new_root = _root->rw_layer->map.snapshot_root();
            uint32_t cur_seq  = _root->cow.load_state().cow_seq();

            if (_db)
            {
               auto delay = _db->config().max_freshness_delay;
               if (delay.count() > 0)
               {
                  auto now = std::chrono::steady_clock::now();
                  if (now - _root->last_snapshot_time >= delay)
                  {
                     _root->cow.force_publish(new_root, cur_seq);
                     _root->rw_layer->map.bump_cow_seq();
                     cur_seq = _root->cow.load_state().cow_seq();
                  }
               }
            }

            bool notified = _root->cow.end_write(new_root, cur_seq);
            if (notified)
               _root->rw_layer->map.bump_cow_seq();
         }

         if (_root->status)
         {
            _root->status->generation.store(
               _root->generation.load(std::memory_order_relaxed),
               std::memory_order_relaxed);
            _root->status->merge_complete.store(
               _root->merge_complete.load(std::memory_order_relaxed) ? 1 : 0,
               std::memory_order_relaxed);
            _root->status->rw_layer_entries.store(_root->rw_layer->size(),
                                                  std::memory_order_relaxed);
            _root->status->rw_arena_bytes.store(
               _root->rw_layer->map.arena_capacity(),
               std::memory_order_relaxed);
            _root->status->throttle_sleep_ns.store(
               _root->throttle_sleep_ns.load(std::memory_order_relaxed),
               std::memory_order_relaxed);
         }

         if (_db && _root->merge_complete.load(std::memory_order_acquire) &&
             _db->should_swap(_root_index))
         {
            _db->try_swap_rw_to_ro(_root_index);
         }

         if (uint32_t sleep_ns = _root->throttle_sleep_ns.load(std::memory_order_relaxed))
         {
            uint32_t arena_cap = _root->rw_layer->map.arena_capacity();
            if (arena_cap >= (1u << 20) * 16)
               std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
         }
      }
      else
      {
         _undo.pop_frame();
      }
   }

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::commit_multi(uint64_t tx_id,
                                                         uint16_t participants,
                                                         bool     is_commit)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);
      _committed = true;

      if (_wal)
         _wal->commit_entry_multi(tx_id, participants, is_commit);

      _undo.discard();

      {
         auto     new_root = _root->rw_layer->map.snapshot_root();
         uint32_t cur_seq  = _root->cow.load_state().cow_seq();
         bool     notified = _root->cow.end_write(new_root, cur_seq);
         if (notified)
            _root->rw_layer->map.bump_cow_seq();
      }
      if (_root->status)
      {
         _root->status->rw_layer_entries.store(_root->rw_layer->size(),
                                               std::memory_order_relaxed);
         _root->status->rw_arena_bytes.store(_root->rw_layer->map.arena_capacity(),
                                             std::memory_order_relaxed);
      }
   }

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::abort()
   {
      assert(_root && !_committed && !_aborted);
      _aborted = true;

      if (_mode == root_mode::read_only)
         return;

      if (!_nested)
      {
         if (_wal && _wal->entry_in_progress())
            _wal->discard_entry();

         _undo.replay_all(
             [this](const undo_entry& entry)
             {
                auto& layer = *_root->rw_layer;
                switch (entry.type)
                {
                   case undo_entry::kind::insert:
                   case undo_entry::kind::overwrite_cow:
                   case undo_entry::kind::erase_cow:
                      // Route through layer.erase so that any subtree ref
                      // held by the slot is released.
                      layer.erase(entry.key);
                      break;

                   case undo_entry::kind::overwrite_buffered:
                   case undo_entry::kind::erase_buffered:
                   {
                      btree_value restored = entry.old_value;
                      if (restored.is_data() && !restored.data.empty())
                         restored.data = layer.store_string(restored.data);
                      // Route through the layer wrappers so the refcount
                      // contract for subtree values is maintained. For
                      // subtree restoration the address recorded in the
                      // undo log was not separately retained, so restoring
                      // it assumes the caller kept the allocation alive —
                      // matching the pre-existing contract on these paths.
                      if (restored.is_subtree())
                         layer.store_subtree(entry.key, restored.subtree);
                      else if (restored.is_tombstone())
                         layer.store_tombstone(entry.key);
                      else
                         layer.store_data(entry.key, restored.data);
                      break;
                   }

                   case undo_entry::kind::erase_range:
                      if (entry.range)
                      {
                         layer.tombstones.remove(entry.range->low, entry.range->high);
                         for (const auto& be : entry.range->buffered_keys)
                         {
                            btree_value restored = be.old_value;
                            if (restored.is_data() && !restored.data.empty())
                               restored.data = layer.store_string(restored.data);
                            if (restored.is_subtree())
                               layer.store_subtree(be.key, restored.subtree);
                            else if (restored.is_tombstone())
                               layer.store_tombstone(be.key);
                            else
                               layer.store_data(be.key, restored.data);
                         }
                      }
                      break;
                }
             });

         {
            auto     restored_root = _root->rw_layer->map.snapshot_root();
            uint32_t cur_seq       = _root->cow.load_state().cow_seq();
            bool     notified      = _root->cow.end_write(restored_root, cur_seq);
            if (notified)
               _root->rw_layer->map.bump_cow_seq();
         }
      }
      else
      {
         _undo.replay_current_frame(
             [this](const undo_entry& entry)
             {
                auto& layer = *_root->rw_layer;
                switch (entry.type)
                {
                   case undo_entry::kind::insert:
                   case undo_entry::kind::overwrite_cow:
                   case undo_entry::kind::erase_cow:
                      // Route through layer.erase so that any subtree ref
                      // held by the slot is released.
                      layer.erase(entry.key);
                      break;

                   case undo_entry::kind::overwrite_buffered:
                   case undo_entry::kind::erase_buffered:
                   {
                      btree_value restored = entry.old_value;
                      if (restored.is_data() && !restored.data.empty())
                         restored.data = layer.store_string(restored.data);
                      // Route through the layer wrappers so the refcount
                      // contract for subtree values is maintained. For
                      // subtree restoration the address recorded in the
                      // undo log was not separately retained, so restoring
                      // it assumes the caller kept the allocation alive —
                      // matching the pre-existing contract on these paths.
                      if (restored.is_subtree())
                         layer.store_subtree(entry.key, restored.subtree);
                      else if (restored.is_tombstone())
                         layer.store_tombstone(entry.key);
                      else
                         layer.store_data(entry.key, restored.data);
                      break;
                   }

                   case undo_entry::kind::erase_range:
                      if (entry.range)
                      {
                         layer.tombstones.remove(entry.range->low, entry.range->high);
                         for (const auto& be : entry.range->buffered_keys)
                         {
                            btree_value restored = be.old_value;
                            if (restored.is_data() && !restored.data.empty())
                               restored.data = layer.store_string(restored.data);
                            if (restored.is_subtree())
                               layer.store_subtree(be.key, restored.subtree);
                            else if (restored.is_tombstone())
                               layer.store_tombstone(be.key);
                            else
                               layer.store_data(be.key, restored.data);
                         }
                      }
                      break;
                }
             });
      }
   }

   template <class LockPolicy>
   basic_dwal_transaction<LockPolicy>
   basic_dwal_transaction<LockPolicy>::sub_transaction()
   {
      assert(_root && !_committed && !_aborted);
      return basic_dwal_transaction(*_root, _wal, _root_index, _db, /*nested=*/true);
   }

   // ── Undo Recording ───────────────────────────────────────────────

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::record_undo_for_upsert(std::string_view key)
   {
      auto& layer = *_root->rw_layer;
      auto* v     = layer.map.get(key);

      if (v)
      {
         _undo.record_overwrite_buffered(key, *v);
      }
      else
      {
         auto ro_result = ro_get(key);
         if (ro_result.found)
         {
            _undo.record_overwrite_cow(key);
         }
         else
         {
            _undo.record_insert(key);
         }
      }
   }

   template <class LockPolicy>
   void basic_dwal_transaction<LockPolicy>::record_undo_for_remove(std::string_view key)
   {
      auto& layer = *_root->rw_layer;
      auto* v     = layer.map.get(key);

      if (v && !v->is_tombstone())
      {
         _undo.record_erase_buffered(key, *v);
      }
      else
      {
         _undo.record_erase_cow(key);
      }
   }

}  // namespace psitri::dwal
