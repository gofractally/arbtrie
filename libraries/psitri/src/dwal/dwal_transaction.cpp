#include <psitri/dwal/dwal_transaction.hpp>

#include <psitri/dwal/dwal_database.hpp>

#include <cassert>
#include <utility>

namespace psitri::dwal
{
   dwal_transaction::dwal_transaction(dwal_root&     root,
                                      wal_writer*    wal,
                                      uint32_t       root_index,
                                      dwal_database* db,
                                      bool           nested)
       : _root(&root),
         _wal(wal),
         _db(db),
         _root_index(root_index),
         _nested(nested),
         _owns_lock(false)  // no longer used — single writer, no mutex
   {
      _undo.push_frame();

      // For outer transactions, begin a WAL entry.
      if (!_nested && _wal)
         _wal->begin_entry();
   }

   dwal_transaction::~dwal_transaction()
   {
      if (!_committed && !_aborted && _root)
         abort();
   }

   dwal_transaction::dwal_transaction(dwal_transaction&& other) noexcept
       : _root(std::exchange(other._root, nullptr)),
         _wal(std::exchange(other._wal, nullptr)),
         _db(std::exchange(other._db, nullptr)),
         _root_index(other._root_index),
         _committed(other._committed),
         _aborted(other._aborted),
         _nested(other._nested),
         _owns_lock(other._owns_lock)
         , _undo(std::move(other._undo))
   {
      other._owns_lock = false;
      other._committed = true;  // prevent double-abort in moved-from dtor
   }

   // ── Mutations ────────────────────────────────────────────────────

   void dwal_transaction::upsert(std::string_view key, std::string_view value)
   {
      assert(_root && !_committed && !_aborted);

      record_undo_for_upsert(key);
      _root->rw_layer->store_data(key, value);

      // Record in WAL (only for outermost; nested commits merge into parent).
      if (!_nested && _wal)
         _wal->add_upsert_data(key, value);
   }

   void dwal_transaction::upsert_subtree(std::string_view key, sal::ptr_address addr)
   {
      assert(_root && !_committed && !_aborted);

      record_undo_for_upsert(key);
      _root->rw_layer->store_subtree(key, addr);

      if (!_nested && _wal)
         _wal->add_upsert_subtree(key, addr);
   }

   bool dwal_transaction::remove(std::string_view key)
   {
      assert(_root && !_committed && !_aborted);

      record_undo_for_remove(key);

      // Split any range tombstone covering this key (if we're re-inserting
      // a tombstone for a specific key, the range must be split).
      // Actually for remove, we ADD a tombstone, so no split needed.

      _root->rw_layer->store_tombstone(key);

      if (!_nested && _wal)
         _wal->add_remove(key);

      return true;  // We don't know if PsiTri had it; conservatively return true.
   }

   void dwal_transaction::remove_range(std::string_view low, std::string_view high)
   {
      assert(_root && !_committed && !_aborted);

      auto& layer = *_root->rw_layer;

      // Collect buffered entries in [low, high) for undo.
      std::vector<undo_entry::range_data::buffered_entry> buffered;
      {
         auto it  = layer.map.lower_bound(low);
         auto end = layer.map.lower_bound(high);
         for (; it != end; ++it)
         {
            if (!it->second.is_tombstone())
               buffered.push_back({it->first, it->second});
         }
      }

      // Erase buffered entries in range from the map.
      {
         auto it  = layer.map.lower_bound(low);
         auto end = layer.map.lower_bound(high);
         layer.map.erase(it, end);
      }

      // Add range tombstone.
      layer.tombstones.add(std::string(low), std::string(high));

      // Record undo.
      _undo.record_erase_range(low, high, std::move(buffered));

      if (!_nested && _wal)
         _wal->add_remove_range(low, high);
   }

   // ── Point Reads ──────────────────────────────────────────────────

   dwal_transaction::lookup_result dwal_transaction::get(std::string_view key) const
   {
      assert(_root);

      // Layer 1: RW btree
      {
         auto it = _root->rw_layer->map.find(key);
         if (it != _root->rw_layer->map.end())
         {
            if (it->second.is_tombstone())
               return {false, {}};
            return {true, it->second};
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

      // Layer 3: PsiTri — the caller must handle this externally,
      // since we don't have direct access to the PsiTri cursor here.
      // Return not-found to indicate "check PsiTri".
      return {false, {}};
   }

   dwal_transaction::lookup_result dwal_transaction::ro_get(std::string_view key) const
   {
      std::shared_ptr<btree_layer> ro;
      {
         std::shared_lock lk(_root->buffered_mutex);
         ro = _root->buffered_ptr;
      }
      if (!ro)
         return {false, {}};

      auto it = ro->map.find(key);
      if (it != ro->map.end())
      {
         if (it->second.is_tombstone())
            return {false, btree_value::make_tombstone()};
         return {true, it->second};
      }
      if (ro->tombstones.is_deleted(key))
         return {false, btree_value::make_tombstone()};

      return {false, {}};
   }

   // ── Transaction Control ──────────────────────────────────────────

   void dwal_transaction::commit()
   {
      assert(_root && !_committed && !_aborted);
      _committed = true;

      if (!_nested)
      {
         // Write WAL entry for the outermost transaction.
         if (_wal)
            _wal->commit_entry();

         // Release old subtree refs from overwritten/erased entries.
         // (In a real implementation, these would call allocator::release.)
         // For now, collect them for the caller to handle.
         // _undo.release_old_subtrees([](sal::ptr_address addr) { ... });

         _undo.discard();

         // If the merge thread has finished and the RW btree is large enough, swap.
         if (_db && _root->merge_complete.load(std::memory_order_acquire)
             && _db->should_swap(_root_index))
            _db->try_swap_rw_to_ro(_root_index);
      }
      else
      {
         // Inner commit: pop the undo frame (entries merge into parent).
         _undo.pop_frame();
      }
   }

   void dwal_transaction::abort()
   {
      assert(_root && !_committed && !_aborted);
      _aborted = true;

      if (!_nested)
      {
         // Discard the in-progress WAL entry.
         if (_wal && _wal->entry_in_progress())
            _wal->discard_entry();

         // Replay all undo entries to restore the btree.
         _undo.replay_all(
             [this](const undo_entry& entry)
             {
                auto& layer = *_root->rw_layer;
                switch (entry.type)
                {
                   case undo_entry::kind::insert:
                   case undo_entry::kind::overwrite_cow:
                   case undo_entry::kind::erase_cow:
                      layer.map.erase(entry.key);
                      break;

                   case undo_entry::kind::overwrite_buffered:
                   case undo_entry::kind::erase_buffered:
                      layer.map.insert_or_assign(
                          layer.store_string(entry.key), entry.old_value);
                      break;

                   case undo_entry::kind::erase_range:
                      if (entry.range)
                      {
                         layer.tombstones.remove(entry.range->low, entry.range->high);
                         for (const auto& be : entry.range->buffered_keys)
                         {
                            layer.map.insert_or_assign(
                                layer.store_string(be.key), be.old_value);
                         }
                      }
                      break;
                }
             });
      }
      else
      {
         // Inner abort: replay current frame only.
         _undo.replay_current_frame(
             [this](const undo_entry& entry)
             {
                auto& layer = *_root->rw_layer;
                switch (entry.type)
                {
                   case undo_entry::kind::insert:
                   case undo_entry::kind::overwrite_cow:
                   case undo_entry::kind::erase_cow:
                      layer.map.erase(entry.key);
                      break;

                   case undo_entry::kind::overwrite_buffered:
                   case undo_entry::kind::erase_buffered:
                      layer.map.insert_or_assign(
                          layer.store_string(entry.key), entry.old_value);
                      break;

                   case undo_entry::kind::erase_range:
                      if (entry.range)
                      {
                         layer.tombstones.remove(entry.range->low, entry.range->high);
                         for (const auto& be : entry.range->buffered_keys)
                         {
                            layer.map.insert_or_assign(
                                layer.store_string(be.key), be.old_value);
                         }
                      }
                      break;
                }
             });
      }
   }

   dwal_transaction dwal_transaction::sub_transaction()
   {
      assert(_root && !_committed && !_aborted);
      return dwal_transaction(*_root, _wal, _root_index, _db, /*nested=*/true);
   }

   // ── Undo Recording ───────────────────────────────────────────────

   void dwal_transaction::record_undo_for_upsert(std::string_view key)
   {
      auto& layer = *_root->rw_layer;
      auto  it    = layer.map.find(key);

      if (it != layer.map.end())
      {
         // Key exists in RW btree — overwrite_buffered.
         _undo.record_overwrite_buffered(key, it->second);
      }
      else
      {
         // Check RO + PsiTri (approximation: check RO only here).
         auto ro_result = ro_get(key);
         if (ro_result.found)
         {
            // Key exists in lower layers — overwrite_cow.
            _undo.record_overwrite_cow(key);
         }
         else
         {
            // Key doesn't exist anywhere (or we assume it doesn't
            // without checking PsiTri — conservative: treat as insert).
            // TODO: Check PsiTri for exact classification.
            _undo.record_insert(key);
         }
      }
   }

   void dwal_transaction::record_undo_for_remove(std::string_view key)
   {
      auto& layer = *_root->rw_layer;
      auto  it    = layer.map.find(key);

      if (it != layer.map.end() && !it->second.is_tombstone())
      {
         // Key exists in RW btree — erase_buffered.
         _undo.record_erase_buffered(key, it->second);
      }
      else
      {
         // Key exists in lower layers (or nowhere) — erase_cow.
         _undo.record_erase_cow(key);
      }
   }

}  // namespace psitri::dwal
