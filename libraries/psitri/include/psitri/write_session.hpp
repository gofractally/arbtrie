#pragma once
#include <psitri/read_session.hpp>
//#include <psitri/write_cursor.hpp>

namespace psitri
{
   class write_cursor;
   class write_session;
   using write_session_ptr = std::shared_ptr<write_session>;
   using write_cursor_ptr  = std::shared_ptr<write_cursor>;

   class write_session : public read_session
   {
     public:
      using ptr = std::shared_ptr<write_session>;
      write_session(database& db);
      ~write_session();

      //      write_cursor_ptr create_write_cursor(sal::smart_ref root = sal::smart_ref());

     private:
   };

}  // namespace psitri
