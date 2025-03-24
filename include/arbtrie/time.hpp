#pragma once

#include <atomic>
#include <chrono>
#include <thread>

namespace arbtrie
{
   /**
    * time_manager - Singleton class that maintains a background thread 
    * for updating the current time at regular intervals.
    * 
    * This reduces the overhead of frequent chrono calls by having a single
    * background thread update a shared atomic value that other threads read,
    * this is useful when we don't need the full precision of a high resolution
    * clock, but still want a monotonic clock that is consistent across threads.
    */
   class time_manager
   {
     private:
      std::atomic<uint64_t> _current_time_ms{0};
      std::thread           _time_thread;
      std::atomic<bool>     _running{true};

      // Singleton instance
      static time_manager& instance()
      {
         static time_manager manager;
         return manager;
      }

      // Private constructor - starts the background thread
      time_manager()
      {
         // Initialize with current time
         _current_time_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now().time_since_epoch())
                                    .count());

         // Start background thread
         _time_thread = std::thread(
             [this]()
             {
// Set thread name
#ifdef __APPLE__
                pthread_setname_np("time_updater");
#else
                pthread_setname_np(pthread_self(), "time_updater");
#endif

                while (_running.load(std::memory_order_relaxed))
                {
                   // Update time
                   _current_time_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              std::chrono::steady_clock::now().time_since_epoch())
                                              .count(),
                                          std::memory_order_relaxed);

                   // Sleep for 1 microsecond
                   std::this_thread::sleep_for(std::chrono::microseconds(10));
                }
             });

         // Register shutdown handler
         std::atexit([]() { time_manager::shutdown(); });
      }

     public:
      ~time_manager() { shutdown(); }

      // Get current time (milliseconds)
      static uint64_t getCurrentTimeMs()
      {
         return instance()._current_time_ms.load(std::memory_order_relaxed);
      }

      // Explicitly shutdown the time manager
      static void shutdown()
      {
         auto& manager = instance();
         if (manager._running.exchange(false) && manager._time_thread.joinable())
         {
            manager._time_thread.join();
         }
      }

      // Prevent copying
      time_manager(const time_manager&)            = delete;
      time_manager& operator=(const time_manager&) = delete;
   };

}  // namespace arbtrie
