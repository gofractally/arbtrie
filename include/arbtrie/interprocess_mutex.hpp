#pragma once

#include <arbtrie/debug.hpp>
#include <atomic>
#include <cassert>
#include <cstdint>

namespace arbtrie
{

   /**
    * A mutex implementation for interprocess synchronization.
    * Compatible with std::unique_lock.
    * 
    * Uses atomic operations and kernel-assisted waiting to provide
    * efficient synchronization between processes in shared memory.
    * 
    * This implementation uses a FIFO (First-In-First-Out) ticket approach
    * with a bitmask to track waiting threads, ensuring fairness even
    * under high contention.
    * 
    * (on linux std::atomic<>::wait is implemented using futexes),
    * other platforms may or may not work interprocess, but this is
    * still effective for within a process.
    */
   class interprocess_mutex
   {
     private:
      static constexpr uint64_t MAX_WAITERS = 64;
      static constexpr uint64_t TICKET_MASK = MAX_WAITERS - 1;

      std::atomic<uint32_t> _ticket_counter{0};   // Next ticket to assign
      std::atomic<uint32_t> _serving{0};          // Current ticket being served
      std::atomic<uint64_t> _waiters_bitmask{0};  // Bitmask of waiting threads

     public:
      interprocess_mutex()  = default;
      ~interprocess_mutex() = default;

      // Non-copyable and non-movable
      interprocess_mutex(const interprocess_mutex&)            = delete;
      interprocess_mutex& operator=(const interprocess_mutex&) = delete;

      void lock()
      {
         // Get a unique ticket
         uint32_t my_ticket = _ticket_counter.fetch_add(1, std::memory_order_relaxed);
         uint32_t my_slot   = my_ticket & TICKET_MASK;

         while (true)
         {
            uint32_t current_serving = _serving.load(std::memory_order_acquire);
            if (current_serving == my_ticket)
               break;  // Lock acquired

            // Mark myself as waiting
            _waiters_bitmask.fetch_add(1ULL << my_slot, std::memory_order_acquire);

            // Wait for my turn
            _serving.wait(current_serving, std::memory_order_relaxed);

            if (current_serving != _serving.load(std::memory_order_acquire))
            {
               // A change happened
               current_serving = _serving.load(std::memory_order_acquire);
               if (current_serving == my_ticket)
                  break;  // Lock acquired
            }
         }
      }

      bool try_lock()
      {
         uint32_t current_serving = _serving.load(std::memory_order_acquire);
         uint32_t next_ticket     = _ticket_counter.load(std::memory_order_relaxed);

         if (current_serving == next_ticket)
         {
            bool success = _ticket_counter.compare_exchange_strong(next_ticket, next_ticket + 1,
                                                                   std::memory_order_acquire);

            return success;
         }

         return false;
      }

      void unlock()
      {
         uint32_t current_serving = _serving.fetch_add(1, std::memory_order_release);
         uint32_t next_serving    = current_serving + 1;

         // Clear the bit for the next ticket
         uint32_t next_slot = next_serving & TICKET_MASK;
         // Use fetch_sub to atomically clear the bit
         uint32_t old_mask =
             _waiters_bitmask.fetch_sub(1ULL << next_slot, std::memory_order_release);

         // Check if there are waiters in the bitmask
         if (old_mask != 0)
            _serving.notify_all();
      }
   };

}  // namespace arbtrie