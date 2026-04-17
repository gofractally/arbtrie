#pragma once
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/read_session.hpp>
#include <sal/allocator_session_impl.hpp>

namespace psitri
{

   template <class LockPolicy>
   inline basic_read_session<LockPolicy>::basic_read_session(database_type& db)
       : _db(db.shared_from_this()),
         _allocator_session(db._allocator.get_session())
   {
   }

   template <class LockPolicy>
   inline sal::smart_ptr<sal::alloc_header>
   basic_read_session<LockPolicy>::get_root(uint32_t root_index)
   {
      return _allocator_session->template get_root<>(sal::root_object_number(root_index));
   }

   template <class LockPolicy>
   inline cursor basic_read_session<LockPolicy>::create_cursor(uint32_t root_index)
   {
      auto root = get_root(root_index);
      auto ver  = root.ver();
      if (ver != sal::null_ptr_address)
      {
         uint64_t version = _allocator_session->read_custom_cb(ver);
         return cursor(std::move(root), version);
      }
      return cursor(std::move(root));
   }

}  // namespace psitri
