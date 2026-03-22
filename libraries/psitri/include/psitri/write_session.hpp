#pragma once
#include <psitri/read_session.hpp>
#include <psitri/write_cursor.hpp>

namespace psitri
{
   class transaction;
   class write_session;
   using write_session_ptr = std::shared_ptr<write_session>;

   class write_session : public read_session
   {
     public:
      using ptr = std::shared_ptr<write_session>;
      write_session(database& db);
      ~write_session();

      /// Create a write cursor on an empty (transient) tree
      write_cursor_ptr create_write_cursor();

      /// Create a write cursor on an existing root
      write_cursor_ptr create_write_cursor(sal::smart_ptr<sal::alloc_header> root);

      /// Get the root at the given top-level index
      sal::smart_ptr<sal::alloc_header> get_root(uint32_t root_index);

      /// Atomically set the root at the given top-level index
      void set_root(uint32_t              root_index,
                    sal::smart_ptr<sal::alloc_header> root,
                    sal::sync_type        sync = sal::sync_type::none);

      /// Set the sync mode used when transactions commit
      void set_sync(sal::sync_type sync) { _sync = sync; }

      /// Get the current sync mode
      sal::sync_type get_sync() const { return _sync; }

      /// Start a transaction on the given top-level root index
      transaction start_transaction(uint32_t root_index);

     private:
      friend class transaction;
      sal::sync_type _sync = sal::sync_type::none;
   };

}  // namespace psitri
