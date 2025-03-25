#include <pthread.h>  // For pthread_setname_np
#include <string.h>   // For strncpy
#include <arbtrie/segment_thread.hpp>

namespace arbtrie
{
   bool segment_thread::start()
   {
      // Check if another process might already be running the thread
      bool expected        = false;
      bool already_running = !_thread_state->running.compare_exchange_strong(
          expected, true, std::memory_order_acquire, std::memory_order_relaxed);

      if (already_running)
      {
         // Another process likely has this thread running
         int pid = _thread_state->pid.load(std::memory_order_acquire);

         // Check if the process with this PID is still running
         bool process_running = false;
         if (pid > 0)
         {
            // Use kill with signal 0 to test if process exists and is accessible
            int result      = kill(pid, 0);
            process_running = (result == 0);

            // If process is not running but the running flag is still set, we have a stale entry
            if (!process_running && errno == ESRCH)
            {
               ARBTRIE_WARN(_thread_name, " thread was running in process PID: ", pid,
                            " (process no longer exists) - taking over");
               _thread_state->running.store(false, std::memory_order_release);
               // Fall through to restart the thread
            }
            else if (process_running)
            {
               // Process exists, but let's also check if it's making progress
               // Get the current last_alive_time
               int64_t last_alive =
                   _thread_state->last_alive_time_ms.load(std::memory_order_relaxed);

               // Wait a short time to check for progress
               std::this_thread::sleep_for(_progress_check_interval_ms);

               // Check again to see if the last_alive_time has been updated
               int64_t new_last_alive =
                   _thread_state->last_alive_time_ms.load(std::memory_order_relaxed);

               if (last_alive == new_last_alive)
               {
                  // No progress made - thread may be stuck
                  int64_t elapsed_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                                             std::chrono::system_clock::now().time_since_epoch())
                                             .count() -
                                         last_alive;

                  ARBTRIE_WARN(_thread_name, " thread in process PID: ", pid,
                               " is not making progress (no heartbeat for ", elapsed_time,
                               "ms) - taking over");
                  _thread_state->running.store(false, std::memory_order_release);
                  // Fall through to restart the thread
               }
               else
               {
                  // The other process is making progress, let it continue
                  ARBTRIE_INFO(_thread_name, " thread is already running in process PID: ", pid,
                               " (and making progress)");
                  return false;
               }
            }
         }
         else
         {
            // No PID set, but running flag is set - inconsistent state
            ARBTRIE_WARN(_thread_name, " thread has running=true but no PID - taking over");
            _thread_state->running.store(false, std::memory_order_release);
            // Fall through to restart the thread
         }
      }

      // At this point, we either weren't running or we determined we should take over
      if (_thread.joinable())
      {
         // Make sure we don't have a local thread already running
         stop();
      }

      // Clear the stop flag since we're starting
      _stop.store(false, std::memory_order_relaxed);

      // Record start time
      int64_t current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
      _thread_state->start_time_ms.store(current_time_ms, std::memory_order_release);
      _thread_state->last_alive_time_ms.store(current_time_ms, std::memory_order_release);

      // Store current PID
      _thread_state->pid.store(getpid(), std::memory_order_release);

      // Make sure running is set (should be already from above, but just to be safe)
      _thread_state->running.store(true, std::memory_order_release);

      // Launch the thread
      _thread = std::thread(
          [this]()
          {
             try
             {
                // Set OS-level thread name (truncated for OS compatibility)
                char truncated_name[16] = {0};
                strncpy(truncated_name, _thread_name.c_str(), sizeof(truncated_name) - 1);
                thread_name(truncated_name);

                // Set thread-local name for debug macros (using full name)
                arbtrie::thread_name(_thread_name.c_str());

                try
                {
                   // Call the actual thread work
                   _work(*this);
                }
                catch (const std::exception& e)
                {
                   ARBTRIE_ERROR("Exception in ", _thread_name, " thread: ", e.what());
                }
                catch (...)
                {
                   ARBTRIE_ERROR("Unknown exception in ", _thread_name, " thread");
                }
             }
             catch (...)
             {
                // Ensure we handle any errors in setup
                ARBTRIE_ERROR("Unknown exception in thread setup: ", _thread_name);
             }

             // Clear thread state on exit, regardless of how we exited
             _thread_state->running.store(false, std::memory_order_release);
             _thread_state->pid.store(0, std::memory_order_release);
          });

      ARBTRIE_INFO("Started ", _thread_name, " thread");
      return true;
   }

   bool segment_thread::is_running() const
   {
      return _thread.joinable() && !_stop.load();
   }

   void segment_thread::stop()
   {
      if (_thread.joinable())
      {
         // Set stop flag to signal thread to exit
         _stop.store(true, std::memory_order_release);

         // Notify any waiting condition variables
         _cv.notify_all();

         // Wait for thread to finish
         _thread.join();

         // Clear thread state
         _thread_state->running.store(false, std::memory_order_release);
         _thread_state->pid.store(0, std::memory_order_release);

         ARBTRIE_INFO("Stopped ", _thread_name, " thread");
      }
   }

   bool segment_thread::yield(std::chrono::milliseconds time_ms)
   {
      try
      {
         // Special case for 1ms sleep
         if (time_ms.count() <= 10)
         {
            std::this_thread::sleep_for(time_ms);
            return !_stop.load(std::memory_order_relaxed);
         }

         // Check if yield is being called infrequently
         auto now = std::chrono::steady_clock::now();
         auto time_since_last_yield =
             std::chrono::duration_cast<std::chrono::milliseconds>(now - _last_yield_time);

         // Only warn if the interval is reasonable but still exceeds the threshold
         // Extremely large values likely indicate this is the first call after startup
         // or the system clock has been adjusted
         constexpr auto max_reasonable_interval = std::chrono::milliseconds(60000);  // 1 minute
         if (time_since_last_yield > _heartbeat_warning_threshold &&
             time_since_last_yield < max_reasonable_interval)
         {
            ARBTRIE_WARN("Thread '", _thread_name, "' yield() call interval (",
                         time_since_last_yield.count(), "ms) exceeds heartbeat interval (",
                         _heartbeat_warning_threshold.count(), "ms)");
         }

         // Update last yield time
         _last_yield_time = now;

         // Update heartbeat timestamp initially
         int64_t current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count();
         _thread_state->last_alive_time_ms.store(current_time_ms, std::memory_order_release);

         // If sleep time requested, sleep using condition variable
         if (time_ms.count() > 0)
         {
            // Define a heartbeat interval - update heartbeat every second at most
            const std::chrono::milliseconds heartbeat_interval(1000);

            // Calculate the end time
            auto end_time = std::chrono::steady_clock::now() + time_ms;

            std::unique_lock<std::mutex> lock(_mutex);

            // Keep waiting until we reach the total wait time or the stop flag is set
            while (std::chrono::steady_clock::now() < end_time)
            {
               // Calculate the remaining time to wait
               auto remaining_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                   end_time - std::chrono::steady_clock::now());

               // Wait for the minimum of heartbeat_interval and remaining_time
               auto wait_time = std::min(heartbeat_interval, remaining_time);

               // Exit early if stop flag is set during wait
               if (_cv.wait_for(lock, wait_time,
                                [this]() { return _stop.load(std::memory_order_relaxed); }))
               {
                  // Stop flag was set, exit immediately
                  return false;
               }

               // Update heartbeat timestamp after each wait segment
               current_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count();
               _thread_state->last_alive_time_ms.store(current_time_ms, std::memory_order_release);

               // Check if we've reached the end time
               if (std::chrono::steady_clock::now() >= end_time)
                  break;
            }
         }

         // Check if thread should exit
         return !_stop.load(std::memory_order_relaxed);
      }
      catch (const std::exception& e)
      {
         ARBTRIE_ERROR("Exception in ", _thread_name, " thread yield: ", e.what());
         return false;  // Signal thread to exit on error
      }
      catch (...)
      {
         ARBTRIE_ERROR("Unknown exception in ", _thread_name, " thread yield");
         return false;  // Signal thread to exit on error
      }
   }

   void segment_thread::thread_name(const char* name)
   {
#if defined(__APPLE__)
      pthread_setname_np(name);
#elif defined(__linux__)
      pthread_setname_np(pthread_self(), name);
#endif
   }

   void segment_thread::set_current_thread_name(const char* name)
   {
      // Truncate name if needed as most platforms have a 16 character limit
      char truncated_name[16] = {0};
      strncpy(truncated_name, name, sizeof(truncated_name) - 1);

      // Only set the OS-level thread name - this does NOT set the thread_name
      // used by debug macros. For that, call arbtrie::thread_name() directly
      // from within the thread.
      thread_name(truncated_name);
   }
}  // namespace arbtrie