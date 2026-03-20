#pragma once
#include <psitri/database.hpp>
#include <psitri/write_session.hpp>

namespace psitri
{

   inline write_session::write_session(database& db) : read_session(db) {}

   inline std::shared_ptr<write_cursor> write_session::create_write_cursor(sal::smart_ref root)
   {
      return std::make_shared<write_cursor>(*this, root);
   }

}  // namespace psitri
