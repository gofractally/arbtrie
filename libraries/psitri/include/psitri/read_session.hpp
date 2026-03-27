#pragma once
#include <memory>
#include <psitri/cursor.hpp>
#include <psitri/node/node.hpp>
#include <sal/allocator.hpp>
#include <sal/smart_ptr.hpp>

namespace psitri
{
   class database;
   class read_cursor;

   /**
    * @brief A read-only view of the database for one thread.
    *
    * A read_session provides snapshot-isolated reads without blocking writers.
    * Each call to get_root() or create_cursor() captures the current state of
    * the tree at that instant — subsequent writes by other sessions do not
    * affect already-obtained roots or cursors (copy-on-write isolation).
    *
    * @note **Thread affinity:** A read_session is backed by a thread-local
    *       allocator_session and must only be used from the thread that created
    *       it. Create one session per reader thread.
    *
    * @note **Lifetime:** The session holds a shared_ptr to the database,
    *       keeping it alive. Roots and cursors obtained from the session hold
    *       reference-counted pointers into the tree, preventing the compactor
    *       from relocating or freeing those nodes until they are released.
    *
    * Typical usage:
    * @code
    *   auto rs = db->start_read_session();
    *   auto cursor = rs->create_cursor(0);      // snapshot of root 0
    *   cursor.seek_begin();
    *   while (!cursor.is_end()) {
    *       auto val = cursor.value<std::string>();
    *       cursor.next();
    *   }
    * @endcode
    */
   class read_session : public std::enable_shared_from_this<read_session>
   {
     public:
      using ptr = std::shared_ptr<read_session>;
      ~read_session();
      read_session(database& db);

      /** @name Tree Access */
      ///@{

      /**
       * @brief Get the root of the tree at the given index as a snapshot.
       *
       * The returned smart_ptr holds a reference count on the root node,
       * creating a lightweight snapshot. The snapshot is valid until the
       * smart_ptr is destroyed, regardless of concurrent writes.
       *
       * @param root_index Index into the 512 top-level roots (0-511).
       * @return A reference-counted pointer to the root node, or a null
       *         smart_ptr if the root has never been set.
       */
      sal::smart_ptr<sal::alloc_header> get_root(uint32_t root_index);

      /**
       * @brief Create a read-only cursor positioned before the first key.
       *
       * The cursor captures a snapshot of the tree at root_index. Use
       * seek_begin(), lower_bound(), or seek() to position it, then
       * iterate with next()/prev().
       *
       * @param root_index Index into the 512 top-level roots (0-511).
       * @return A cursor for iterating the snapshot. If the root has never
       *         been set, the cursor is empty (seek_begin() returns false).
       */
      cursor create_cursor(uint32_t root_index);

      ///@}

     protected:
      friend class read_cursor;
      friend class write_cursor;
      friend class write_session;

      std::shared_ptr<database>  _db;
      sal::allocator_session_ptr _allocator_session;

      friend class database;
   };

}  // namespace psitri
