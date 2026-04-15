#pragma once
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session.hpp>
#include <sal/allocator_session_impl.hpp>

namespace psitri
{

   inline write_session::write_session(database& db) : read_session(db) {}

   inline write_cursor_ptr write_session::create_write_cursor()
   {
      return std::make_shared<write_cursor>(_allocator_session);
   }

   inline write_cursor_ptr write_session::create_write_cursor(sal::smart_ptr<sal::alloc_header> root)
   {
      return std::make_shared<write_cursor>(std::move(root));
   }

   inline sal::smart_ptr<sal::alloc_header> write_session::get_root(uint32_t root_index)
   {
      return _allocator_session->get_root<>(sal::root_object_number(root_index));
   }

   inline void write_session::set_root(uint32_t                          root_index,
                                        sal::smart_ptr<sal::alloc_header> root,
                                        sal::sync_type                    sync)
   {
      _allocator_session->set_root(sal::root_object_number(root_index), std::move(root), sync);
   }

   inline uint64_t write_session::get_total_allocated_objects() const
   {
      return _allocator_session->get_total_allocated_objects();
   }

   inline uint64_t write_session::get_pending_release_count() const
   {
      return _allocator_session->get_pending_release_count();
   }

   inline transaction write_session::start_transaction(uint32_t root_index, tx_mode mode)
   {
      if (mode == tx_mode::occ)
      {
         // OCC: snapshot root without locking; per-key validation at commit time
         auto root    = get_root(root_index);
         auto session = _allocator_session;
         auto* self   = this;

         return transaction(
             session, std::move(root),
             // occ_commit: lock, validate read set per-key, re-base writes, publish
             [self, root_index](const detail::write_buffer* buffer,
                                const read_set&             reads)
             {
                auto& lock = self->_db->modify_lock(root_index);
                std::lock_guard guard(lock);

                // Get current tree state
                auto current_root = self->get_root(root_index);

                // Per-key validation: check each read against the current tree
                if (!reads.empty())
                {
                   cursor c(current_root);
                   for (const auto& entry : reads.entries)
                   {
                      auto info = c.get_key_info(entry.key);
                      if (info.leaf_addr != entry.leaf_addr || info.version != entry.version)
                         throw occ_conflict{};
                   }
                }

                // Apply buffered writes to the current tree (not the snapshot)
                if (buffer && !buffer->empty())
                {
                   write_cursor wc(std::move(current_root));
                   auto it  = buffer->begin();
                   auto end = buffer->end();
                   for (; it != end; ++it)
                   {
                      auto& e = it.value();
                      auto  k = it.key();
                      if (e.is_data())
                         wc.upsert_sorted(k, e.value());
                      else if (e.type == detail::buffer_entry::tombstone)
                         wc.remove(k);
                   }
                   current_root = wc.root();
                }

                // Allocate version and publish
                if (current_root)
                {
                   auto ver_num =
                       self->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;
                   auto ver_adr = self->_allocator_session->alloc_custom_cb(ver_num);
                   current_root.set_ver(ver_adr);
                }
                else
                {
                   current_root.release();
                }
                self->set_root(root_index, std::move(current_root), self->_sync);
             },
             // rollback: no lock held, nothing to release
             []() {});
      }

      auto& lock = _db->modify_lock(root_index);
      lock.lock();

      auto root    = get_root(root_index);
      auto session = _allocator_session;
      auto db      = _db;

      // Capture this session for commit/rollback
      auto* self = this;

      return transaction(
          session, std::move(root),
          // commit: allocate version, save root with version, unlock
          [self, root_index, &lock](sal::smart_ptr<sal::alloc_header> new_root)
          {
             if (new_root)
             {
                auto ver_num =
                    self->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;
                auto ver_adr = self->_allocator_session->alloc_custom_cb(ver_num);
                new_root.set_ver(ver_adr);
             }
             else
             {
                // Null root: release any stale ver CB inherited from the snapshot
                new_root.release();
             }
             self->set_root(root_index, std::move(new_root), self->_sync);
             lock.unlock();
          },
          // rollback: just unlock
          [&lock]() { lock.unlock(); },
          mode);
   }

   /// Fast-path version-CB swap: allocate new version CB, update root slot ver,
   /// release old version CB.  Protected by a lightweight per-root ver lock
   /// (not the full modify_lock).  The root address is unchanged — only the
   /// version metadata in the root slot is swapped.
   inline void write_session::swap_root_ver(uint32_t root_index, uint64_t ver_num)
   {
      auto ver_adr = _allocator_session->alloc_custom_cb(ver_num);

      std::lock_guard guard(_db->root_ver_lock(root_index));

      auto root    = get_root(root_index);
      auto old_ver = root.ver();
      root.set_ver(ver_adr);
      set_root(root_index, std::move(root), _sync);

      if (old_ver != sal::null_ptr_address)
         _allocator_session->release(old_ver);
   }

   inline uint64_t write_session::mvcc_upsert(uint32_t root_index, key_view key, value_view value)
   {
      auto ver_num = _db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;

      // Fast path: stripe lock on target node (value_node or leaf).
      // Handles Cases A (inline promotion), B (value_node append), C (new key).
      // Returns false only on leaf overflow (needs split → COW fallback).
      {
         tree_context ctx(get_root(root_index));
         ctx.set_dead_versions(_db->dead_versions().load_snapshot());
         ctx.set_current_epoch(_db->_dbm->current_epoch());

         auto target = ctx.mvcc_find_target(key);
         if (target != sal::null_ptr_address)
         {
            bool ok;
            {
               std::lock_guard guard(_db->stripe_mutex(target));
               ok = ctx.try_mvcc_upsert(key, value_type(value), ver_num);
            }
            if (ok)
            {
               swap_root_ver(root_index, ver_num);
               return ver_num;
            }
            // Overflow — fall through to slow path
         }
      }

      // Slow path: COW fallback (split, prefix mismatch, empty tree).
      auto& lock = _db->modify_lock(root_index);
      std::lock_guard guard(lock);

      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(_db->dead_versions().load_snapshot());
      ctx.set_current_epoch(_db->_dbm->current_epoch());
      ctx.mvcc_upsert(key, value_type(value), ver_num);

      auto new_root = ctx.take_root();
      auto old_ver  = new_root.ver();
      auto ver_adr  = _allocator_session->alloc_custom_cb(ver_num);
      new_root.set_ver(ver_adr);

      set_root(root_index, std::move(new_root), _sync);

      if (old_ver != sal::null_ptr_address)
         _allocator_session->release(old_ver);

      return ver_num;
   }

   inline uint64_t write_session::mvcc_remove(uint32_t root_index, key_view key)
   {
      auto ver_num = _db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;

      // Fast path: stripe lock on target node (value_node or leaf).
      {
         tree_context ctx(get_root(root_index));
         ctx.set_dead_versions(_db->dead_versions().load_snapshot());
         ctx.set_current_epoch(_db->_dbm->current_epoch());

         auto target = ctx.mvcc_find_target(key);
         if (target != sal::null_ptr_address)
         {
            bool ok;
            {
               std::lock_guard guard(_db->stripe_mutex(target));
               ok = ctx.try_mvcc_remove(key, ver_num);
            }
            if (ok)
            {
               swap_root_ver(root_index, ver_num);
               return ver_num;
            }
         }
      }

      // Slow path: COW fallback.
      auto& lock = _db->modify_lock(root_index);
      std::lock_guard guard(lock);

      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(_db->dead_versions().load_snapshot());
      ctx.set_current_epoch(_db->_dbm->current_epoch());
      ctx.mvcc_remove(key, ver_num);

      auto new_root = ctx.take_root();
      auto old_ver  = new_root.ver();
      auto ver_adr  = _allocator_session->alloc_custom_cb(ver_num);
      new_root.set_ver(ver_adr);

      set_root(root_index, std::move(new_root), _sync);

      if (old_ver != sal::null_ptr_address)
         _allocator_session->release(old_ver);

      return ver_num;
   }

   inline uint64_t write_session::defrag_tree(uint32_t root_index)
   {
      auto& lock = _db->modify_lock(root_index);
      std::lock_guard guard(lock);

      auto root = get_root(root_index);
      if (!root)
         return 0;

      tree_context ctx(std::move(root));
      ctx.set_dead_versions(_db->dead_versions().load_snapshot());
      uint64_t cleaned = ctx.defrag();

      auto new_root = ctx.take_root();
      set_root(root_index, std::move(new_root), _sync);

      return cleaned;
   }

   inline void write_session::dump_live_objects() const
   {
      _allocator_session->for_each_live_object(
          [](sal::ptr_address adr, uint32_t ref, const sal::alloc_header* obj)
          {
             const char* type_name = "unknown";
             switch ((int)obj->type())
             {
                case (int)node_type::inner: type_name = "inner"; break;
                case (int)node_type::inner_prefix: type_name = "inner_prefix"; break;
                case (int)node_type::leaf: type_name = "leaf"; break;
                case (int)node_type::value: type_name = "value_node"; break;
             }
             std::cerr << "  LIVE addr=" << *adr
                       << " ref=" << ref
                       << " type=" << type_name
                       << " size=" << obj->size()
                       << std::endl;
          });
   }

}  // namespace psitri
