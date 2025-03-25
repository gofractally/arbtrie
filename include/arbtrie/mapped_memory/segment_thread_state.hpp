#pragma once
#include <atomic>

namespace arbtrie
{
   namespace mapped_memory
   {
      /**
       * Shared state for segment threads that is stored in mapped memory
       * for inter-process coordination
       * 
       * used by @ref segment_thread to track thread state
       */
      struct segment_thread_state
      {
         // Flag indicating if the thread is currently running
         // Used to prevent multiple processes from running duplicate threads
         // and to detect unclean shutdowns
         std::atomic<bool> running{false};

         // Process ID of the process running the thread
         // Helps with debugging and determining if the process crashed
         std::atomic<int> pid{0};

         // When the thread was started
         std::atomic<int64_t> start_time_ms{0};

         // Last time the thread reported being alive (heartbeat)
         std::atomic<int64_t> last_alive_time_ms{0};
      };
   }  // namespace mapped_memory
}  // namespace arbtrie