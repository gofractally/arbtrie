#pragma once
#include <psitri/database.hpp>
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

   inline transaction write_session::start_transaction(uint32_t root_index)
   {
      auto& lock = _db->modify_lock(root_index);
      lock.lock();

      auto root    = get_root(root_index);
      auto session = _allocator_session;
      auto db      = _db;

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
          [&lock]() { lock.unlock(); });
   }

}  // namespace psitri
