#pragma once

#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#include <arbtrie/debug.hpp>
#include <arbtrie/mapped_memory.hpp>
namespace arbtrie
{

   /**
    * A thread class that manages thread lifecycle with cross-process awareness.
    * 
    * This class handles:
    * 1. Thread creation and cleanup
    * 2. Checking for stale threads from crashed processes
    * 3. Coordinating which process owns a thread
    * 4. Progress tracking via heartbeats
    * 5. Clean shutdown
    */
   class segment_thread
   {
     public:
      /**
       * Callback function type for thread work
       * 
       * @param thread Reference to the segment_thread that's running the function
       */
      using thread_func = std::function<void(segment_thread&)>;

      /**
       * Constructor
       * 
       * @param thread_state Pointer to shared thread state in mapped memory
       * @param thread_name Name of the thread for logging and debugging
       * @param work Function to execute in the thread
       * @param progress_check_interval_ms How often to check for progress in takeover decisions (default 5000ms)
       */
      segment_thread(
          mapped_memory::segment_thread_state* thread_state,
          std::string                          thread_name,
          thread_func                          work,
          std::chrono::milliseconds progress_check_interval_ms = std::chrono::milliseconds(5000))
          : _thread_state(thread_state),
            _thread_name(std::move(thread_name)),
            _work(std::move(work)),
            _progress_check_interval_ms(progress_check_interval_ms),
            _last_yield_time(std::chrono::steady_clock::now())
      {
      }

      /**
       * Destructor - ensures thread is stopped and joined
       */
      ~segment_thread() { stop(); }

      // Disable copying
      segment_thread(const segment_thread&)            = delete;
      segment_thread& operator=(const segment_thread&) = delete;

      // Disable moving (std::atomic cannot be moved)
      segment_thread(segment_thread&&)            = delete;
      segment_thread& operator=(segment_thread&&) = delete;

      /**
       * Start the thread if it's not already running in another process,
       * or if the other process appears to be dead or not making progress.
       * 
       * @return true if this process started the thread, false if another process has it
       */
      bool start();

      /**
       * Check if the thread is running and this process owns it
       */
      bool is_running() const;

      /**
       * Stop the thread if it's running and this process owns it
       */
      void stop();

      /**
       * Get the condition variable that can be used to wake up the thread
       */
      std::condition_variable& condition_variable() { return _cv; }

      /**
       * Get the mutex associated with the condition variable
       */
      std::mutex& mutex() { return _mutex; }

      /**
       * Get the stop flag for use by the thread function
       */
      const std::atomic<bool>& get_stop_flag() const { return _stop; }

      /**
       * Get a function that returns true when the thread should exit
       */
      std::function<bool()> get_should_exit_func() const;

      /**
       * Get a function that updates the last_alive_time
       */
      std::function<void()> get_heartbeat_func() const;

      /**
       * Yields execution, updates the heartbeat timestamp, and checks if the thread should exit.
       * 
       * @param time_ms Time to sleep in milliseconds (0 = no sleep, just check status)
       * @return true if the thread should continue execution, false if it should exit
       */
      bool yield(std::chrono::milliseconds time_ms = std::chrono::milliseconds(0));

     private:
      // Helper function to set thread name in OS
      static void thread_name(const char* name);

      // Helper function to set thread name for current thread
      static void set_current_thread_name(const char* name);

      // Pointer to shared thread state in mapped memory
      mapped_memory::segment_thread_state* _thread_state;

      // Thread name for logging and debugging
      std::string _thread_name;

      // Work function to execute in the thread
      thread_func _work;

      // Thread object
      std::thread _thread;

      // Stop signal
      std::atomic<bool> _stop{false};

      // Condition variable for signaling the thread
      std::condition_variable _cv;

      // Mutex for the condition variable
      std::mutex _mutex;

      // How often to check for progress when deciding to take over a thread
      std::chrono::milliseconds _progress_check_interval_ms;

      // Last time yield was called (for tracking yield frequency)
      std::chrono::steady_clock::time_point _last_yield_time;

      // Threshold for warning about infrequent yields
      std::chrono::milliseconds _heartbeat_warning_threshold{2000};
   };

}  // namespace arbtrie