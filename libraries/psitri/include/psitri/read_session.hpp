#pragma once
#include <memory>
#include <psitri/cursor.hpp>
#include <psitri/lock_policy.hpp>
#include <psitri/node/node.hpp>
#include <sal/allocator.hpp>
#include <sal/smart_ptr.hpp>

namespace psitri
{
   template <class LockPolicy>
   class basic_database;

   class read_cursor;

   /**
    * @brief A read-only view of the database for one thread.
    *
    * A basic_read_session provides snapshot-isolated reads without blocking
    * writers. Each call to get_root() or create_cursor() captures the current
    * state of the tree at that instant — subsequent writes by other sessions
    * do not affect already-obtained roots or cursors (copy-on-write isolation).
    *
    * @tparam LockPolicy  Must match the policy used by the owning database.
    *
    * @note **Thread affinity:** A read_session is backed by a thread-local
    *       allocator_session and must only be used from the thread that created
    *       it. Create one session per reader thread.
    */
   template <class LockPolicy = std_lock_policy>
   class basic_read_session
       : public std::enable_shared_from_this<basic_read_session<LockPolicy>>
   {
     public:
      using lock_policy_type = LockPolicy;
      using database_type    = basic_database<LockPolicy>;
      using ptr              = std::shared_ptr<basic_read_session>;

      ~basic_read_session() = default;
      basic_read_session(database_type& db);

      /** @name Tree Access */
      ///@{

      sal::smart_ptr<sal::alloc_header> get_root(uint32_t root_index);

      cursor create_cursor(uint32_t root_index);

      /// @brief Access the underlying allocator session.
      sal::allocator_session_ptr allocator_session() const { return _allocator_session; }

      ///@}

     protected:
      friend class read_cursor;
      friend class write_cursor;
      template <class> friend class basic_write_session;
      template <class> friend class basic_database;

      std::shared_ptr<database_type> _db;
      sal::allocator_session_ptr     _allocator_session;
   };

   /// Default-policy alias preserved for existing consumers.
   using read_session = basic_read_session<std_lock_policy>;

}  // namespace psitri
