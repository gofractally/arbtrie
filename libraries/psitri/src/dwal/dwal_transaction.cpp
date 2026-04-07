#include <psitri/dwal/dwal_transaction.hpp>

#include <psitri/dwal/dwal_database.hpp>

#include <cassert>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace psitri::dwal
{
   dwal_transaction::dwal_transaction(dwal_root&     root,
                                      wal_writer*    wal,
                                      uint32_t       root_index,
                                      dwal_database* db,
                                      bool           nested,
                                      root_mode      mode)
       : _root(&root),
         _wal(wal),
         _db(db),
         _root_index(root_index),
         _nested(nested),
         _owns_lock(false),
         _mode(mode)
   {
      if (_mode == root_mode::read_write)
      {
         // Outer write transactions hold the RW mutex exclusively so
         // concurrent readers (get_latest, latest-mode cursors) never
         // see a torn, partially-applied transaction.
         if (!_nested && _root->enable_rw_locking)
         {
            _root->rw_mutex.lock();
            _owns_lock = true;
         }

         // COWART: set writer_active via CAS (readers may be concurrently
         // incrementing reader_count via fetch_add).
         if (!_nested)
         {
            auto& cow = _root->cow;
            uint64_t expected = cow.root_and_flags.load(std::memory_order_acquire);
            for (;;)
            {
               auto f = cowart_flags{expected};

               uint32_t new_seq = f.cow_seq();
               if (f.reader_count() > 0)
               {
                  // Readers are traversing head — must COW to protect them.
                  new_seq = (f.cow_seq() + 1) & cowart_flags::cow_seq_mask;
                  // Publish head → prev for fresh readers
                  cow.prev_root.store(f.root_offset(), std::memory_order_release);
               }

               uint64_t desired = cowart_flags::make(
                   f.root_offset(), /*wa=*/true, /*rw=*/false,
                   f.reader_count(), new_seq);

               if (cow.root_and_flags.compare_exchange_weak(
                       expected, desired, std::memory_order_acq_rel))
               {
                  // Bump art_map's cow_seq to match if readers forced COW
                  if (new_seq != f.cow_seq())
                  {
                     _root->rw_layer->map.bump_cow_seq();
                  }
                  break;
               }
               // CAS failed — reader_count changed, retry with updated expected
            }
         }

         _undo.push_frame();

         if (!_nested && _wal)
            _wal->begin_entry();
      }
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
         _owns_lock(other._owns_lock),
         _mode(other._mode)
         , _undo(std::move(other._undo))
   {
      other._owns_lock = false;
      other._committed = true;  // prevent double-abort in moved-from dtor
   }

   // ── Mutations ────────────────────────────────────────────────────

   void dwal_transaction::upsert(std::string_view key, std::string_view value)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      record_undo_for_upsert(key);
      _root->rw_layer->store_data(key, value);

      // Record in WAL (only for outermost; nested commits merge into parent).
      if (!_nested && _wal)
         _wal->add_upsert_data(key, value);
   }

   void dwal_transaction::upsert_subtree(std::string_view key, sal::ptr_address addr)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      record_undo_for_upsert(key);
      _root->rw_layer->store_subtree(key, addr);

      if (!_nested && _wal)
         _wal->add_upsert_subtree(key, addr);
   }

   remove_result dwal_transaction::remove(std::string_view key)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      // Check RW layer — this also determines the undo entry type.
      auto& layer = *_root->rw_layer;
      auto* v     = layer.map.get(key);
      bool  in_rw = v && !v->is_tombstone();

      record_undo_for_remove(key);

      _root->rw_layer->store_tombstone(key);

      if (!_nested && _wal)
         _wal->add_remove(key);

      if (in_rw)
         return remove_result(true);  // Known immediately.

      // Defer RO + Tri check until the caller inspects the result.
      return remove_result(this, std::string(key));
   }

   bool dwal_transaction::exists_in_lower_layers(std::string_view key) const
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

   void remove_result::resolve()
   {
      _resolved = true;
      _existed  = _tx && _tx->exists_in_lower_layers(_key);
   }

   void dwal_transaction::remove_range(std::string_view low, std::string_view high)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);

      auto& layer = *_root->rw_layer;

      // Collect buffered entries in [low, high) for undo.
      std::vector<undo_entry::range_data::buffered_entry> buffered;
      {
         auto it = layer.map.lower_bound(low);
         for (; it != layer.map.end() && it.key() < high; ++it)
         {
            if (!it.value().is_tombstone())
               buffered.push_back({std::string_view(it.key()), it.value()});
         }
      }

      // Collect keys to erase, then erase them (can't erase while iterating).
      {
         std::vector<std::string> keys_to_erase;
         auto it = layer.map.lower_bound(low);
         for (; it != layer.map.end() && it.key() < high; ++it)
            keys_to_erase.emplace_back(it.key());
         for (auto& k : keys_to_erase)
            layer.map.erase(k);
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

   owned_merge_cursor dwal_transaction::create_cursor() const
   {
      assert(_root && !_committed && !_aborted);

      // RW: use the live layer directly (same thread owns it, no lock needed)
      std::shared_ptr<btree_layer> rw = _root->rw_layer;

      // RO: snapshot under buffered_mutex (brief)
      std::shared_ptr<btree_layer> ro;
      {
         std::shared_lock lk(_root->buffered_mutex);
         ro = _root->buffered_ptr;
      }

      // Tri: create a cursor via dwal_database's thread-local read session
      std::optional<psitri::cursor> tri;
      if (_db)
         tri.emplace(_db->create_tri_cursor(_root_index));

      return owned_merge_cursor(std::move(rw), std::move(ro), std::move(tri));
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

   void dwal_transaction::commit()
   {
      assert(_root && !_committed && !_aborted);
      _committed = true;

      // Read-only transactions have no undo/WAL state.
      if (_mode == root_mode::read_only)
         return;

      if (!_nested)
      {
         // Write WAL entry for the outermost transaction.
         if (_wal)
            _wal->commit_entry();

         _undo.discard();

         // ── COWART commit ────────────────────────────────────────
         // Update head root, handle reader coordination via CAS.
         {
            auto& cow = _root->cow;
            auto new_root = _root->rw_layer->map.snapshot_root();
            uint64_t expected = cow.root_and_flags.load(std::memory_order_acquire);
            bool     did_notify = false;

            for (;;)
            {
               auto f = cowart_flags{expected};
               bool has_readers = f.reader_waiting() || f.reader_count() > 0;
               uint32_t new_seq = f.cow_seq();

               if (has_readers)
               {
                  // Readers are active or waiting — protect this root.
                  // Bump cow_seq so next tx COWs shared nodes.
                  new_seq = (f.cow_seq() + 1) & cowart_flags::cow_seq_mask;
                  cow.prev_root.store(new_root, std::memory_order_release);
                  _root->rw_layer->map.bump_cow_seq();
               }

               uint64_t desired = cowart_flags::make(
                   new_root, /*wa=*/false, /*rw=*/false,
                   f.reader_count(), new_seq);

               if (cow.root_and_flags.compare_exchange_weak(
                       expected, desired, std::memory_order_acq_rel))
               {
                  did_notify = has_readers;
                  break;
               }
            }

            if (did_notify)
               cow.writer_done_cv.notify_all();
         }

         // Release the RW mutex before swap — readers can now see
         // the committed state. Swap may re-acquire briefly.
         if (_owns_lock)
         {
            _root->rw_mutex.unlock();
            _owns_lock = false;
         }

         // Swap RW→RO if: buffer is full, time elapsed, OR readers want fresh data.
         if (_db && _root->merge_complete.load(std::memory_order_acquire)
             && (_db->should_swap(_root_index)
                 || _root->readers_want_swap.load(std::memory_order_relaxed)))
         {
            _db->try_swap_rw_to_ro(_root_index);
            if (_root->readers_want_swap.exchange(false, std::memory_order_relaxed))
               _root->swap_cv.notify_all();
         }

         // Adaptive throttle: if the merge thread set a non-zero sleep,
         // apply it to smooth out write pressure so the merge can keep up.
         // Only throttle when the arena is past the low-water mark (25% of
         // capacity) to avoid penalizing small/fast transactions.
         if (uint32_t sleep_ns = _root->throttle_sleep_ns.load(std::memory_order_relaxed))
         {
            uint32_t arena_cap = _root->rw_layer->map.arena_capacity();
            // Low-water mark: don't throttle when arena is small
            if (arena_cap >= (1u << 20) * 16)  // 16 MB
               std::this_thread::sleep_for(std::chrono::nanoseconds(sleep_ns));
         }
      }
      else
      {
         // Inner commit: pop the undo frame (entries merge into parent).
         _undo.pop_frame();
      }
   }

   void dwal_transaction::commit_multi(uint64_t tx_id, uint16_t participants, bool is_commit)
   {
      assert(_root && !_committed && !_aborted && _mode == root_mode::read_write);
      _committed = true;

      if (_wal)
         _wal->commit_entry_multi(tx_id, participants, is_commit);

      _undo.discard();

      // COWART: same as commit — update head, handle reader coordination
      {
         auto& cow = _root->cow;
         auto new_root = _root->rw_layer->map.snapshot_root();
         uint64_t expected = cow.root_and_flags.load(std::memory_order_acquire);
         bool     did_notify = false;

         for (;;)
         {
            auto f = cowart_flags{expected};
            bool has_readers = f.reader_waiting() || f.reader_count() > 0;
            uint32_t new_seq = f.cow_seq();

            if (has_readers)
            {
               new_seq = (f.cow_seq() + 1) & cowart_flags::cow_seq_mask;
               cow.prev_root.store(new_root, std::memory_order_release);
               _root->rw_layer->map.bump_cow_seq();
            }

            uint64_t desired = cowart_flags::make(
                new_root, /*wa=*/false, /*rw=*/false,
                f.reader_count(), new_seq);

            if (cow.root_and_flags.compare_exchange_weak(
                    expected, desired, std::memory_order_acq_rel))
            {
               did_notify = has_readers;
               break;
            }
         }

         if (did_notify)
            cow.writer_done_cv.notify_all();
      }

      if (_owns_lock)
      {
         _root->rw_mutex.unlock();
         _owns_lock = false;
      }
   }

   void dwal_transaction::abort()
   {
      assert(_root && !_committed && !_aborted);
      _aborted = true;

      // Read-only transactions have no undo/WAL state.
      if (_mode == root_mode::read_only)
         return;

      if (!_nested)
      {
         // Discard the in-progress WAL entry.
         if (_wal && _wal->entry_in_progress())
            _wal->discard_entry();

         // Replay all undo entries FIRST — restores the btree to pre-tx state.
         // This must happen before clearing writer_active, because readers
         // will see the root as soon as writer_active is cleared.
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
                   {
                      btree_value restored = entry.old_value;
                      if (restored.is_data() && !restored.data.empty())
                         restored.data = layer.store_string(restored.data);
                      layer.map.upsert(entry.key, restored);
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
                            layer.map.upsert(be.key, restored);
                         }
                      }
                      break;
                }
             });

         // COWART: now clear writer_active via CAS (same protocol as commit).
         // Root is restored to pre-tx state by undo replay above.
         {
            auto& cow = _root->cow;
            auto restored_root = _root->rw_layer->map.snapshot_root();
            uint64_t expected = cow.root_and_flags.load(std::memory_order_acquire);
            bool     did_notify = false;

            for (;;)
            {
               auto f = cowart_flags{expected};
               bool has_readers = f.reader_waiting() || f.reader_count() > 0;
               uint32_t new_seq = f.cow_seq();

               if (has_readers)
               {
                  new_seq = (f.cow_seq() + 1) & cowart_flags::cow_seq_mask;
                  cow.prev_root.store(restored_root, std::memory_order_release);
                  _root->rw_layer->map.bump_cow_seq();
               }

               uint64_t desired = cowart_flags::make(
                   restored_root, /*wa=*/false, /*rw=*/false,
                   f.reader_count(), new_seq);

               if (cow.root_and_flags.compare_exchange_weak(
                       expected, desired, std::memory_order_acq_rel))
               {
                  did_notify = has_readers;
                  break;
               }
            }

            if (did_notify)
               cow.writer_done_cv.notify_all();
         }

         if (_owns_lock)
         {
            _root->rw_mutex.unlock();
            _owns_lock = false;
         }
      }
      else
      {
         // Inner abort: replay current frame only.
         // Same pool-copy treatment as outer abort — the undo arena may
         // outlive this frame, but inner frames share the same arena, so
         // the data is still alive. However, for consistency and safety
         // (the arena is freed on ~undo_log), copy into the pool.
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
                   {
                      btree_value restored = entry.old_value;
                      if (restored.is_data() && !restored.data.empty())
                         restored.data = layer.store_string(restored.data);
                      layer.map.upsert(entry.key, restored);
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
                            layer.map.upsert(be.key, restored);
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
      auto* v     = layer.map.get(key);

      if (v)
      {
         // Key exists in RW btree — overwrite_buffered.
         _undo.record_overwrite_buffered(key, *v);
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
            _undo.record_insert(key);
         }
      }
   }

   void dwal_transaction::record_undo_for_remove(std::string_view key)
   {
      auto& layer = *_root->rw_layer;
      auto* v     = layer.map.get(key);

      if (v && !v->is_tombstone())
      {
         // Key exists in RW btree — erase_buffered.
         _undo.record_erase_buffered(key, *v);
      }
      else
      {
         // Key exists in lower layers (or nowhere) — erase_cow.
         _undo.record_erase_cow(key);
      }
   }

}  // namespace psitri::dwal
