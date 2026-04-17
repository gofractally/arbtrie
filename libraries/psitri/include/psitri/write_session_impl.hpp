#pragma once
#include <psitri/database.hpp>
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
   inline transaction basic_write_session<LockPolicy>::start_transaction(
       uint32_t root_index, tx_mode mode)
   {
      auto& lock = this->_db->modify_lock(root_index);
      lock.lock();

      auto root    = get_root(root_index);
      auto session = this->_allocator_session;

      // Capture this session for commit/rollback
      auto* self = this;

      return transaction(
          session, std::move(root),
          // commit: save root and unlock
          [self, root_index, &lock](sal::smart_ptr<sal::alloc_header> new_root)
          {
             self->set_root(root_index, std::move(new_root), self->_sync);
             lock.unlock();
          },
          // rollback: just unlock
          [&lock]() { lock.unlock(); },
          mode);
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

}  // namespace psitri
