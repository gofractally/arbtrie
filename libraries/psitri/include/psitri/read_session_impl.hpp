#pragma once
#include <psitri/database.hpp>
#include <psitri/read_session.hpp>
#include <sal/allocator_session_impl.hpp>

namespace psitri
{

   inline read_session::read_session(database& db)
       : _db(db.shared_from_this()), _allocator_session(db._allocator.get_session())
   {
   }

   inline sal::smart_ptr<sal::alloc_header> read_session::get_root(uint32_t root_index)
   {
      return _allocator_session->get_root<>(sal::root_object_number(root_index));
   }

   inline cursor read_session::create_cursor(uint32_t root_index)
   {
      return cursor(get_root(root_index));
   }

}  // namespace psitri
