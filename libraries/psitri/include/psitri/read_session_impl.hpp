#pragma once
#include <psitri/database.hpp>
#include <psitri/read_session.hpp>

namespace psitri
{

   inline read_session::read_session(database& db)
   {
      _db                = db.shared_from_this();
      _allocator_session = db._allocator.start_session();
   }

   inline std::shared_ptr<read_cursor> read_session::create_read_cursor(sal::smart_ref root)
   {
      return std::make_shared<read_cursor>(*this, root);
   }

}  // namespace psitri
