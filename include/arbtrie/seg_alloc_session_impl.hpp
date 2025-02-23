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
    * The session lock uses a 64-bit atomic value where:
    * - Upper 32 bits: View of end position from compactor thread
    * - Lower 32 bits: Current locked end position
    *
    * When unlocked, the lower 32 bits are set to max (all 1s).
    * When locked, the lower 32 bits <= the upper 32 bits.
    *
    * This method:
    * 1. Increments nested lock counter
    *    - prevents recursively locking a second time if already locked
    * 2. If first lock, updates the atomic lock value by:
    *    - Loading current value (as published by compactor)
    *    - Verifying unlocked state (lower bits are max, in debug mode)
    *    - Setting lower bits to match upper bits (lock the end)
    *    - Preserving upper bits for compactor view 
    *        - in case the compactor updated the upper bits 
    *          between load and sub
    */
   inline void seg_alloc_session::retain_read_lock()
   {
      if (++_nested_read_lock != 1)
         return;

      uint64_t cur = _session_lock_ptr.load(std::memory_order_acquire);

      //cur.locked_end = cur.view_of_end;
      uint32_t view_of_end = cur >> 32;
      uint32_t cur_end     = uint32_t(cur);
      // it should be unlocked which signaled by max
      assert(cur_end == uint32_t(-1));
      auto diff = cur_end - view_of_end;

      // an atomic sub should leave the higher-order bits in place where the view
      // from the compactor is being updated.
      _session_lock_ptr.fetch_sub(diff, std::memory_order_release);
   }

   // R* goes to inifinity and beyond
   inline void seg_alloc_session::release_read_lock()
   {
      // set it to max uint32_t
      if (not --_nested_read_lock)
         _session_lock_ptr.fetch_or(uint32_t(-1));
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
}  // namespace arbtrie
