#pragma once
#include <memory>
#include <psitri/node/node.hpp>
#include <sal/allocator.hpp>
#include <sal/smart_ptr.hpp>

namespace psitri
{
   class database;
   class read_cursor;
   /**
    * @class read_session
    * provides a read-only interface to the database for one logical thread,
    * which may be shared by multiple threads provided they all access the 
    * read_session through a mutex or other synchronization mechanism.
    * 
    * In principle, there should be one long-lived read_session per logical thread.
    */
   class read_session : public std::enable_shared_from_this<read_session>
   {
     public:
      using ptr = std::shared_ptr<read_session>;
      ~read_session();
      read_session(database& db);

      std::shared_ptr<read_cursor> create_read_cursor(sal::smart_ptr<node> root);

     protected:
      friend class read_cursor;
      friend class write_cursor;

      std::shared_ptr<database>  _db;
      sal::allocator_session_ptr _allocator_session;

      friend class database;

     private:
   };

}  // namespace psitri
