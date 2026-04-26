#pragma once
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session.hpp>
#include <sal/allocator_session_impl.hpp>

namespace psitri
{

   template <class LockPolicy>
   inline basic_write_session<LockPolicy>::basic_write_session(database_type& db)
       : basic_read_session<LockPolicy>(db)
   {
   }

   template <class LockPolicy>
   inline typename LockPolicy::mutex_type&
   basic_write_session<LockPolicy>::root_modify_lock(uint32_t root_index)
   {
      return this->_db->modify_lock(root_index);
   }

   template <class LockPolicy>
   inline write_cursor_ptr basic_write_session<LockPolicy>::create_write_cursor()
   {
      return std::make_shared<write_cursor>(this->_allocator_session);
   }

   template <class LockPolicy>
   inline write_cursor_ptr basic_write_session<LockPolicy>::create_write_cursor(
       sal::smart_ptr<sal::alloc_header> root)
   {
      return std::make_shared<write_cursor>(std::move(root));
   }

   template <class LockPolicy>
   inline sal::smart_ptr<sal::alloc_header>
   basic_write_session<LockPolicy>::get_root(uint32_t root_index)
   {
      return this->_allocator_session->template get_root<>(
          sal::root_object_number(root_index));
   }

   template <class LockPolicy>
   inline void basic_write_session<LockPolicy>::set_root(
       uint32_t                          root_index,
       sal::smart_ptr<sal::alloc_header> root,
       sal::sync_type                    sync)
   {
      this->_allocator_session->set_root(
          sal::root_object_number(root_index), std::move(root), sync);
   }

   template <class LockPolicy>
   inline uint64_t basic_write_session<LockPolicy>::get_total_allocated_objects() const
   {
      return this->_allocator_session->get_total_allocated_objects();
   }

   template <class LockPolicy>
   inline uint64_t basic_write_session<LockPolicy>::get_pending_release_count() const
   {
      return this->_allocator_session->get_pending_release_count();
   }

   template <class LockPolicy>
   inline void basic_write_session<LockPolicy>::publish_root(
       uint32_t root_index, sal::smart_ptr<sal::alloc_header> new_root)
   {
      if (new_root)
      {
         auto ver_num =
             this->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;
         auto ver_adr = this->_allocator_session->alloc_custom_cb(ver_num);
         new_root.set_ver(ver_adr);
      }
      else
      {
         new_root.release();
      }
      set_root(root_index, std::move(new_root), _sync);
   }

   template <class LockPolicy>
   inline tree_context basic_write_session<LockPolicy>::make_tree_context(uint32_t root_index)
   {
      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(this->_db->dead_versions().load_snapshot());
      ctx.set_current_epoch(this->_db->_dbm->current_epoch());
      return ctx;
   }

   template <class LockPolicy>
   inline transaction basic_write_session<LockPolicy>::start_transaction(
       uint32_t root_index, tx_mode mode)
   {
      auto  session = this->_allocator_session;
      auto* self    = this;

      auto& lock = this->_db->modify_lock(root_index);
      lock.lock();

      auto root = get_root(root_index);
      auto tx   = transaction(
          session, std::move(root),
          [self, root_index, &lock](sal::smart_ptr<sal::alloc_header> new_root)
          {
             self->publish_root(root_index, std::move(new_root));
             lock.unlock();
          },
          [&lock]() { lock.unlock(); },
          mode);
      tx._ws                 = self;
      tx.cs_at(0).root_index = root_index;
      tx._max_held_root      = root_index;
      return tx;
   }

   /// Fast-path version-CB swap: allocate new version CB, update root slot ver,
   /// release old version CB.  Protected by a lightweight per-root ver lock
   /// (not the full modify_lock).  The root address is unchanged — only the
   /// version metadata in the root slot is swapped.
   template <class LockPolicy>
   inline void basic_write_session<LockPolicy>::swap_root_ver(uint32_t root_index, uint64_t ver_num)
   {
      auto ver_adr = this->_allocator_session->alloc_custom_cb(ver_num);

      std::lock_guard<typename LockPolicy::mutex_type> guard(this->_db->root_ver_lock(root_index));

      auto root    = get_root(root_index);
      auto old_ver = root.ver();
      root.set_ver(ver_adr);
      set_root(root_index, std::move(root), _sync);

      if (old_ver != sal::null_ptr_address)
         this->_allocator_session->release(old_ver);
   }

   template <class LockPolicy>
   inline uint64_t basic_write_session<LockPolicy>::mvcc_upsert(
       uint32_t root_index, key_view key, value_view value)
   {
      auto ver_num = this->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;

      // Fast path: stripe lock on target node (value_node or leaf).
      // Handles Cases A (inline promotion), B (value_node append), C (new key).
      // Returns false only on leaf overflow (needs split → COW fallback).
      {
         auto ctx = make_tree_context(root_index);

         auto target = ctx.mvcc_find_target(key);
         if (target != sal::null_ptr_address)
         {
            bool ok;
            {
               std::lock_guard<typename LockPolicy::mutex_type> guard(this->_db->stripe_mutex(target));
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
      auto& lock = this->_db->modify_lock(root_index);
      std::lock_guard<typename LockPolicy::mutex_type> guard(lock);

      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(this->_db->dead_versions().load_snapshot());
      ctx.set_current_epoch(this->_db->_dbm->current_epoch());
      ctx.mvcc_upsert(key, value_type(value), ver_num);

      auto new_root = ctx.take_root();
      auto old_ver  = new_root.ver();
      auto ver_adr  = this->_allocator_session->alloc_custom_cb(ver_num);
      new_root.set_ver(ver_adr);

      set_root(root_index, std::move(new_root), _sync);

      if (old_ver != sal::null_ptr_address)
         this->_allocator_session->release(old_ver);

      return ver_num;
   }

   template <class LockPolicy>
   inline uint64_t basic_write_session<LockPolicy>::mvcc_remove(uint32_t root_index, key_view key)
   {
      auto ver_num = this->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;

      // Fast path: stripe lock on target node (value_node or leaf).
      {
         auto ctx = make_tree_context(root_index);

         auto target = ctx.mvcc_find_target(key);
         if (target != sal::null_ptr_address)
         {
            bool ok;
            {
               std::lock_guard<typename LockPolicy::mutex_type> guard(this->_db->stripe_mutex(target));
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
      auto& lock = this->_db->modify_lock(root_index);
      std::lock_guard<typename LockPolicy::mutex_type> guard(lock);

      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(this->_db->dead_versions().load_snapshot());
      ctx.set_current_epoch(this->_db->_dbm->current_epoch());
      ctx.mvcc_remove(key, ver_num);

      auto new_root = ctx.take_root();
      auto old_ver  = new_root.ver();
      auto ver_adr  = this->_allocator_session->alloc_custom_cb(ver_num);
      new_root.set_ver(ver_adr);

      set_root(root_index, std::move(new_root), _sync);

      if (old_ver != sal::null_ptr_address)
         this->_allocator_session->release(old_ver);

      return ver_num;
   }

   template <class LockPolicy>
   inline uint64_t basic_write_session<LockPolicy>::defrag_tree(uint32_t root_index)
   {
      auto& lock = this->_db->modify_lock(root_index);
      std::lock_guard<typename LockPolicy::mutex_type> guard(lock);

      auto root = get_root(root_index);
      if (!root)
         return 0;

      tree_context ctx(std::move(root));
      ctx.set_dead_versions(this->_db->dead_versions().load_snapshot());
      uint64_t cleaned = ctx.defrag();

      auto new_root = ctx.take_root();
      set_root(root_index, std::move(new_root), _sync);

      return cleaned;
   }

   template <class LockPolicy>
   inline void basic_write_session<LockPolicy>::dump_live_objects() const
   {
      this->_allocator_session->for_each_live_object(
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

   // ═════════════════════════════════════════════════════════════════════
   // transaction::open_root — multi-root support
   // ═════════════════════════════════════════════════════════════════════

   // ═════════════════════════════════════════════════════════════════════
   // transaction multi-root support (needs write_session access)
   // ═════════════════════════════════════════════════════════════════════

   inline tree_handle transaction::open_root(uint32_t root_index)
   {
      assert(_ws && "open_root requires a transaction created via start_transaction");
      assert(_mode == tx_mode::batch && "multi-root only supported in batch mode");
      assert(root_index > _max_held_root && "roots must be opened in ascending index order");

      auto& lock = _ws->root_modify_lock(root_index);
      lock.lock();

      auto root = _ws->get_root(root_index);

      change_set cs;
      if (root)
         cs.cursor.emplace(std::move(root));
      else
         cs.cursor.emplace(_session);
      cs.root_index = root_index;

      uint32_t idx = static_cast<uint32_t>(_change_sets.size());
      _change_sets.push_back(std::move(cs));
      _held_locks.push_back({root_index, idx, &lock});
      _max_held_root = root_index;

      return tree_handle(*this, idx);
   }

   inline void transaction::commit_additional_roots()
   {
      for (auto& hl : _held_locks)
      {
         auto& cs = cs_at(hl.cs_index);
         if (cs.buffer && !cs.buffer->empty())
            merge_buffer_to_persistent(cs);
         _ws->publish_root(hl.root_index, cs.cursor->root());
         hl.lock->unlock();
      }
      _held_locks.clear();
   }

   inline void transaction::abort_additional_roots() noexcept
   {
      for (auto it = _held_locks.rbegin(); it != _held_locks.rend(); ++it)
         it->lock->unlock();
      _held_locks.clear();
   }

}  // namespace psitri
