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
      if (not--_nested_read_lock)
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
   template <typename T>
   inline void seg_alloc_session::record_freed_space(segment_number seg, T* obj)
   {
      _sega.record_freed_space(seg, obj);
   }

   /**
    * Get the last synced position for a segment
    * 
    * @param segment The segment number
    * @return The last synced position in the segment
   inline size_t seg_alloc_session::get_last_sync_position(segment_number segment) const
   {
      return _sega.get_last_sync_position(segment);
   }
    */

   /**
    * Check if a node location has been synced to disk.
    * 
    * @param loc The node location to check
    * @return true if the location is synced, false otherwise
   inline bool seg_alloc_session::is_synced(node_location loc) const
   {
      return _sega.is_synced(loc);
   }
    */

   /**
    * Check if a node location is read-only
    * 
    * @param loc The node location to check
    * @return true if the location is read-only, false otherwise
    */
   inline bool seg_alloc_session::is_read_only(node_location loc) const
   {
      return _sega.is_read_only(loc);
   }

   /**
    * 
    */
   inline bool seg_alloc_session::can_modify(node_location loc) const
   {
      return _sega.can_modify(_session_num, loc);
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

   /**
    * Check if an object should be cached based on its size and difficulty threshold
    * 
    * @param size The size of the object in bytes
    * @return true if the object should be cached, false otherwise
    */
   inline bool seg_alloc_session::should_cache(uint32_t size)
   {
      return _sega._mapped_state->_cache_difficulty_state.should_cache(get_random(), size);
   }

   /**
    * Generate a random number for cache decisions
    * 
    * @return A random 32-bit number
    */
   inline uint32_t seg_alloc_session::get_random()
   {
      return _session_rng.next();
   }

}  // namespace arbtrie
