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
   inline typename LockPolicy::mutex_type& basic_write_session<LockPolicy>::root_modify_lock(
       uint32_t root_index)
   {
      return this->_db->modify_lock(root_index);
   }

   template <class LockPolicy>
	   inline write_cursor_ptr basic_write_session<LockPolicy>::create_write_cursor()
	   {
	      return std::make_shared<write_cursor>(this->_allocator_session,
	                                            this->_db->_dbm->current_epoch_base());
	   }

   template <class LockPolicy>
   inline write_cursor_ptr basic_write_session<LockPolicy>::create_write_cursor(
       sal::smart_ptr<sal::alloc_header> root)
	   {
	      return std::make_shared<write_cursor>(std::move(root), this->_db->_dbm->current_epoch_base());
	   }

   template <class LockPolicy>
   inline tree basic_write_session<LockPolicy>::get_root(uint32_t root_index)
   {
      return tree(
          this->_allocator_session->template get_root<>(sal::root_object_number(root_index)));
   }

   template <class LockPolicy>
   inline tree basic_write_session<LockPolicy>::create_temporary_tree()
   {
      return tree(this->_allocator_session);
   }

   template <class LockPolicy>
   inline write_transaction basic_write_session<LockPolicy>::start_write_transaction(tree    base,
                                                                                     tx_mode mode)
   {
      auto root            = std::move(base).take_root();
      bool has_txn_version = false;
      if (mode == tx_mode::expect_success)
      {
         root            = make_unique_root(std::move(root));
         has_txn_version = true;
      }

	      auto tx             = transaction(this->_allocator_session, std::move(root), {}, {}, mode,
	                                        this->_db->_dbm->current_epoch_base());
      tx._ws              = this;
      tx._has_txn_version = has_txn_version;
      return write_transaction(std::move(tx));
   }

   template <class LockPolicy>
   inline void basic_write_session<LockPolicy>::set_root(uint32_t       root_index,
                                                         tree           root,
                                                         sal::sync_type sync)
   {
      this->_allocator_session->set_root(sal::root_object_number(root_index), root.take_root(),
                                         sync);
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
       uint32_t                          root_index,
       sal::smart_ptr<sal::alloc_header> new_root)
   {
      if (!new_root)
         new_root.release();
      set_root(root_index, std::move(new_root), _sync);
   }

   template <class LockPolicy>
   inline tree_context basic_write_session<LockPolicy>::make_tree_context(uint32_t root_index)
   {
      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(this->_db->dead_versions().load_snapshot());
      ctx.set_epoch_base(this->_db->_dbm->current_epoch_base());
      return ctx;
   }

   template <class LockPolicy>
   inline sal::smart_ptr<sal::alloc_header> basic_write_session<LockPolicy>::make_unique_root(
       sal::smart_ptr<sal::alloc_header> root)
   {
      auto session   = this->_allocator_session;
      auto ver_num   = this->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;
      auto ver_adr   = session->alloc_custom_cb(ver_num);
      auto prior_ver = root.ver();
      root.set_ver(ver_adr);
      if (prior_ver != sal::null_ptr_address)
         session->release(prior_ver);
      return root;
   }

   template <class LockPolicy>
   inline transaction basic_write_session<LockPolicy>::start_transaction(uint32_t root_index,
                                                                         tx_mode  mode)
   {
      auto* self = this;

      auto& lock = this->_db->modify_lock(root_index);
      lock.lock();

      // Single state transition: get the published root, and (for
      // expect_success) convert it to a uniquely-owned root by attaching
      // a fresh version. From this point forward the working root carries
      // the txn's version through every COW operation; no other code path
      // needs to allocate or move the version.
      //
      // expect_failure defers this transition until the first tree touch
      // (commit replay or forced flush), so a txn that never writes burns
      // no global_version and no ver CB.
      auto root            = get_root(root_index);
      bool has_txn_version = false;
      if (mode == tx_mode::expect_success)
      {
         root            = tree(make_unique_root(std::move(root)));
         has_txn_version = true;
      }

      auto tx = transaction(
          this->_allocator_session, std::move(root),
          [self, root_index, &lock](sal::smart_ptr<sal::alloc_header> new_root)
          {
             // The new_root carries the txn's ver from make_unique_root.
             // For an empty-tree commit (txn removed all keys), the ver
             // has nothing to tag — drop it before publishing so the slot
             // ends up at {null, null} instead of {null, vestigial_ver}.
             if (!new_root)
                new_root.release();
             self->set_root(root_index, std::move(new_root), self->_sync);
             lock.unlock();
          },
	          [&lock]() { lock.unlock(); }, mode, this->_db->_dbm->current_epoch_base());
      tx._ws                 = self;
      tx.cs_at(0).root_index = root_index;
      tx._max_held_root      = root_index;
      tx._has_txn_version    = has_txn_version;
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
   inline uint64_t basic_write_session<LockPolicy>::upsert(uint32_t   root_index,
                                                           key_view   key,
                                                           value_view value)
   {
      auto op_scope = this->_allocator_session->record_operation(
          sal::mapped_memory::session_operation::txn_upsert);
      auto ver_num = this->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;
      this->_db->maybe_publish_dead_versions_for_epoch(this->_db->_dbm->current_epoch_base());

      // Fast path: stripe lock on target node (value_node or leaf).
      // Handles existing-key cases (inline promotion and value_node append)
      // plus same-leaf new-key inserts tagged by branch creation version.
      {
         auto ctx = make_tree_context(root_index);
         ctx.set_root_version(ver_num);

         auto target = ctx.find_versioned_write_target(key);
         if (target != sal::null_ptr_address)
         {
            bool ok;
            {
               std::lock_guard<typename LockPolicy::mutex_type> guard(
                   this->_db->stripe_mutex(target));
               ok = ctx.try_upsert_at_version(key, value_type(value), ver_num);
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
      auto&                                            lock = this->_db->modify_lock(root_index);
      std::lock_guard<typename LockPolicy::mutex_type> guard(lock);

      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(this->_db->dead_versions().load_snapshot());
      ctx.set_epoch_base(this->_db->_dbm->current_epoch_base());
      ctx.set_root_version(ver_num);
      ctx.upsert_at_version(key, value_type(value), ver_num);

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
   inline uint64_t basic_write_session<LockPolicy>::remove(uint32_t root_index, key_view key)
   {
      auto op_scope = this->_allocator_session->record_operation(
          sal::mapped_memory::session_operation::txn_remove);
      auto ver_num = this->_db->_dbm->global_version.fetch_add(1, std::memory_order_relaxed) + 1;
      this->_db->maybe_publish_dead_versions_for_epoch(this->_db->_dbm->current_epoch_base());

      // Fast path: stripe lock on target node (value_node or leaf).
      {
         auto ctx = make_tree_context(root_index);
         ctx.set_root_version(ver_num);

         auto target = ctx.find_versioned_write_target(key);
         if (target != sal::null_ptr_address)
         {
            bool ok;
            {
               std::lock_guard<typename LockPolicy::mutex_type> guard(
                   this->_db->stripe_mutex(target));
               ok = ctx.try_remove_at_version(key, ver_num);
            }
            if (ok)
            {
               swap_root_ver(root_index, ver_num);
               return ver_num;
            }
         }
      }

      // Slow path: COW fallback.
      auto&                                            lock = this->_db->modify_lock(root_index);
      std::lock_guard<typename LockPolicy::mutex_type> guard(lock);

      tree_context ctx(get_root(root_index));
      ctx.set_dead_versions(this->_db->dead_versions().load_snapshot());
      ctx.set_epoch_base(this->_db->_dbm->current_epoch_base());
      ctx.set_root_version(ver_num);
      ctx.remove_at_version(key, ver_num);

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
      auto&                                            lock = this->_db->modify_lock(root_index);
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
                case (int)node_type::inner:
                   type_name = "inner";
                   break;
                case (int)node_type::inner_prefix:
                   type_name = "inner_prefix";
                   break;
                case (int)node_type::leaf:
                   type_name = "leaf";
                   break;
                case (int)node_type::value:
                   type_name = "value_node";
                   break;
             }
             std::cerr << "  LIVE addr=" << *adr << " ref=" << ref << " type=" << type_name
                       << " size=" << obj->size() << std::endl;
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
      assert(_mode == tx_mode::expect_success && "multi-root only supported in batch mode");
      assert(root_index > _max_held_root && "roots must be opened in ascending index order");

      auto& lock = _ws->root_modify_lock(root_index);
      lock.lock();

      auto root = _ws->make_unique_root(_ws->get_root(root_index));

      change_set cs;
	      cs.cursor.emplace(std::move(root), _ws->_db->current_epoch_base());
      cs.root_index = root_index;

      uint32_t idx = static_cast<uint32_t>(_change_sets.size());
      _change_sets.push_back(std::move(cs));
      // The transaction class is non-templated; the type-erased unlock_fn
      // captures the actual mutex type at the fill site. The unqualified
      // `psitri::transaction` is bound to write_session, which is
      // bound to std_lock_policy, so we know the mutex type concretely.
      using mutex_t = typename write_session::lock_policy_type::mutex_type;
      _held_locks.push_back({
          .root_index = root_index,
          .cs_index   = idx,
          .lock       = &lock,
          .unlock_fn  = [](void* p) { static_cast<mutex_t*>(p)->unlock(); },
      });
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
         if (cs.dirty)
            _ws->publish_root(hl.root_index, cs.cursor->take_root());
         hl.unlock_fn(hl.lock);
      }
      _held_locks.clear();
   }

   inline void transaction::abort_additional_roots() noexcept
   {
      for (auto it = _held_locks.rbegin(); it != _held_locks.rend(); ++it)
         it->unlock_fn(it->lock);
      _held_locks.clear();
   }

   inline void transaction::materialize_txn_version()
   {
      if (_has_txn_version)
         return;
      auto& cs       = cs_at(_primary_index);
      auto  root     = cs.cursor->take_root();
      auto  with_ver = _ws->make_unique_root(std::move(root));
	      cs.cursor.emplace(std::move(with_ver), _ws->_db->current_epoch_base());
      _has_txn_version = true;
   }

}  // namespace psitri
