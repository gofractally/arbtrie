#pragma once
#include <arbtrie/read_lock.hpp>
#include <arbtrie/seg_alloc_session.hpp>
#include <arbtrie/seg_allocator.hpp>

namespace arbtrie
{
   class object_ref;

   // copy E to R*
   /**
    * Acquires a read lock for the session by incrementing a nested lock counter.
    * 
    * When the nested lock counter transitions from 0 to 1, the session_rlock's
    * lock() method is called to establish the read lock.
    */
   inline void seg_alloc_session::retain_read_lock()
   {
      if (++_nested_read_lock != 1)
         return;

      _session_rlock.lock();
   }

   /**
    * Releases a read lock for the session by decrementing the nested lock counter.
    * 
    * When the nested lock counter transitions from 1 to 0, the session_rlock's
    * unlock() method is called to release the read lock.
    */
   inline void seg_alloc_session::release_read_lock()
   {
      // Only release the lock if this is the last nested lock
      if (not --_nested_read_lock)
         _session_rlock.unlock();

      assert(_nested_read_lock >= 0);
   }

   /**
    * Locks the session by acquiring a read lock.
    * 
    * This method creates a read_lock object that will automatically manage the
    * session lock. When the read_lock goes out of scope, it will release the
    * read lock.
    * 
    * @return A read_lock object that manages the session lock
    */
   inline read_lock seg_alloc_session::lock()
   {
      return read_lock(*this);
   }

   /**
    * Free an object in a segment
    * 
    * @param segment The segment number containing the object
    * @param object_size The size of the object to free
    */
   inline void seg_alloc_session::free_object(segment_number segment, uint32_t object_size)
   {
      _sega.free_object(segment, object_size);
   }

   /**
    * Get the last synced position for a segment
    * 
    * @param segment The segment number
    * @return The last synced position in the segment
    */
   inline size_t seg_alloc_session::get_last_sync_position(segment_number segment) const
   {
      return _sega.get_last_sync_position(segment);
   }

   /**
    * Check if a node location has been synced to disk.
    * 
    * @param loc The node location to check
    * @return true if the location is synced, false otherwise
    */
   inline bool seg_alloc_session::is_synced(node_location loc) const
   {
      return _sega.is_synced(loc);
   }

   /**
    * Get the cache difficulty value which is used for determining read bit updates
    * 
    * @return The current cache difficulty value
    */
   inline uint32_t seg_alloc_session::get_cache_difficulty() const
   {
      return _sega.get_cache_difficulty();
   }
}  // namespace arbtrie
