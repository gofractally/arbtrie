#pragma once

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
    * This mutex will make the first waiter wait until all other
    * subsequent waiters have also waited and completed their wait,
    * it is not a "fair" mutex, but is suitable for quickly protecting
    * small operations where the expectation is minimal contention.
    * 
    * (on linux std::atomic<>::wait is implemented using futexes),
    * other platforms may or may not work interprocess, but this is
    * still effective for within a process.
    */
   class interprocess_mutex
   {
     private:
      // State representation:
      // If _state == 0: Unlocked
      // If _state == 1: Locked with no waiters
      // If _state > 1: Locked with (_state - 1) waiters
      std::atomic<uint32_t> _state{0};

      static constexpr uint32_t unlocked = 0;
      static constexpr uint32_t locked   = 1;

     public:
      interprocess_mutex()  = default;
      ~interprocess_mutex() = default;

      // Non-copyable and non-movable
      interprocess_mutex(const interprocess_mutex&)            = delete;
      interprocess_mutex& operator=(const interprocess_mutex&) = delete;

      void lock()
      {
         // Get ticket and increment counter in one atomic operation
         uint32_t ticket = _state.fetch_add(1, std::memory_order_acquire);

         // If ticket was 0, we got the lock immediately (was unlocked)
         if (ticket == unlocked)
            return;

         const uint32_t wait_on = ticket + 1;

         // Otherwise wait until the count drops to our ticket value
         while (true)
         {
            _state.wait(wait_on, std::memory_order_relaxed);

            // Check if it's our turn
            if (_state.load(std::memory_order_acquire) == ticket)
               break;
         }
         // Now we hold the lock
      }

      bool try_lock()
      {
         // Must use compare_exchange to ensure atomic check and update
         uint32_t expected = unlocked;
         return _state.compare_exchange_strong(expected, locked, std::memory_order_acquire);
      }

      void unlock()
      {
         // Simply decrement the count by 1
         uint32_t prev = _state.fetch_sub(1, std::memory_order_release);

         // Wake up waiters if there were any
         if (prev > 1)
            _state.notify_all();
      }
   };

}  // namespace arbtrie