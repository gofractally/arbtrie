#include <arbtrie/interprocess_mutex.hpp>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <thread>
#include <vector>

using namespace arbtrie;
using namespace std::chrono_literals;

// Test helper to execute with timeout to avoid hanging the test suite
template <typename Func>
bool run_with_timeout(Func func, std::chrono::milliseconds timeout = 1000ms)
{
   std::atomic<bool> completed = false;

   std::thread test_thread(
       [&func, &completed]()
       {
          arbtrie::set_current_thread_name("timeout-thread");
          func();
          completed = true;
       });

   // Allow the function to run for the timeout period
   auto start = std::chrono::steady_clock::now();
   while (!completed && (std::chrono::steady_clock::now() - start) < timeout)
   {
      std::this_thread::sleep_for(10ms);
   }

   if (!completed)
   {
      // If the operation didn't complete, we need to terminate the thread
      // This isn't ideal but necessary to avoid hanging the test suite
      test_thread.detach();  // Let the thread continue in the background
      return false;
   }
   else
   {
      test_thread.join();
      return true;
   }
}

TEST_CASE("interprocess_mutex basic functionality", "[mutex]")
{
   SECTION("Basic lock/unlock")
   {
      interprocess_mutex mutex;

      bool success = run_with_timeout(
          [&mutex]()
          {
             mutex.lock();
             mutex.unlock();
          });

      REQUIRE(success);
   }

   SECTION("try_lock")
   {
      interprocess_mutex mutex;

      bool success = run_with_timeout(
          [&mutex]()
          {
             REQUIRE(mutex.try_lock());        // First try_lock should succeed
             REQUIRE_FALSE(mutex.try_lock());  // Second should fail
             mutex.unlock();
             REQUIRE(mutex.try_lock());  // After unlock, try_lock should succeed again
             mutex.unlock();
          });

      REQUIRE(success);
   }
}

TEST_CASE("interprocess_mutex with multiple threads", "[mutex][concurrent]")
{
   SECTION("Two threads contending")
   {
      interprocess_mutex mutex;
      std::atomic<int>   shared_counter = 0;
      std::atomic<int>   lock_acquired  = 0;

      auto thread_func = [&]()
      {
         for (int i = 0; i < 100; i++)
         {
            mutex.lock();
            lock_acquired++;
            shared_counter++;
            mutex.unlock();
         }
      };

      bool success = run_with_timeout(
          [&]()
          {
             std::thread t1(
                 [&thread_func]()
                 {
                    arbtrie::set_current_thread_name("contend-t1");
                    thread_func();
                 });
             std::thread t2(
                 [&thread_func]()
                 {
                    arbtrie::set_current_thread_name("contend-t2");
                    thread_func();
                 });
             t1.join();
             t2.join();
          },
          5000ms);

      REQUIRE(success);
      REQUIRE(shared_counter == 200);
      REQUIRE(lock_acquired == 200);
   }

   SECTION("Multiple threads with mixed lock/try_lock")
   {
      interprocess_mutex mutex;
      std::atomic<int>   shared_counter = 0;

      auto thread_func = [&](int id)
      {
         for (int i = 0; i < 100; i++)
         {
            // Mix locking strategies based on thread ID
            bool locked = (id % 2 == 0) ? mutex.try_lock()
                                        : [&]()
            {
               mutex.lock();
               return true;
            }();

            if (locked)
            {
               shared_counter++;
               mutex.unlock();
            }
            else
            {
               // If try_lock failed, wait a bit and try with regular lock
               std::this_thread::sleep_for(1ms);
               mutex.lock();
               shared_counter++;
               mutex.unlock();
            }
         }
      };

      bool success = run_with_timeout(
          [&]()
          {
             std::vector<std::thread> threads;
             for (int i = 0; i < 4; i++)
             {
                threads.emplace_back(
                    [&thread_func, i]()
                    {
                       arbtrie::set_current_thread_name(("mixed-t" + std::to_string(i)).c_str());
                       thread_func(i);
                    });
             }

             for (auto& t : threads)
             {
                t.join();
             }
          },
          5000ms);

      REQUIRE(success);
      REQUIRE(shared_counter == 400);
   }
}

TEST_CASE("interprocess_mutex issue identification", "[mutex][issue]")
{
   SECTION("Identify infinite loop issue")
   {
      interprocess_mutex mutex;

      // The issue is likely related to the wait condition in the lock() method.
      // Let's try to reproduce the condition that causes infinite loop:

      bool success = run_with_timeout(
          [&mutex]()
          {
             // First thread locks the mutex
             mutex.lock();

             // Start another thread that will get blocked waiting
             std::thread t1(
                 [&mutex]()
                 {
                    arbtrie::set_current_thread_name("issue-wait-t");
                    mutex.lock();
                    mutex.unlock();
                 });

             // Let the second thread enter waiting state
             std::this_thread::sleep_for(50ms);

             // Unlock the mutex - this should wake up the waiting thread
             mutex.unlock();

             // Wait for the second thread
             t1.join();
          },
          2000ms);

      REQUIRE(success);
   }

   SECTION("Possible fix: simplified state machine")
   {
      // Note: This is a test for the suggested fix
      // The actual fix should be implemented in interprocess_mutex.hpp

      // Create a new mutex instance with a simpler locking protocol
      // 1. State transitions: 0 (unlocked) <-> 1 (locked)
      // 2. No waiting counter, just notify_all on unlock

      interprocess_mutex mutex;
      std::atomic<int>   counter = 0;

      bool success = run_with_timeout(
          [&]()
          {
             // Start multiple threads that lock and increment
             std::vector<std::thread> threads;
             for (int i = 0; i < 10; i++)
             {
                threads.emplace_back(
                    [&]()
                    {
                       arbtrie::set_current_thread_name("simpl-fix-t");
                       for (int j = 0; j < 10; j++)
                       {
                          mutex.lock();
                          counter++;
                          mutex.unlock();
                       }
                    });
             }

             // Join all threads
             for (auto& t : threads)
             {
                t.join();
             }
          },
          5000ms);

      REQUIRE(success);
      REQUIRE(counter == 100);
   }
}

TEST_CASE("interprocess_mutex wait loop issue", "[mutex][issue][detailed]")
{
   SECTION("Test the wait loop logic")
   {
      interprocess_mutex mutex;

      bool success = run_with_timeout(
          [&mutex]()
          {
             // Lock the mutex
             mutex.lock();

             // Launch 3 threads that will all wait
             std::vector<std::thread> threads;
             std::atomic<int>         counter = 0;

             for (int i = 0; i < 3; i++)
             {
                threads.emplace_back(
                    [&mutex, &counter, i]()
                    {
                       arbtrie::set_current_thread_name(("wait-t" + std::to_string(i)).c_str());
                       mutex.lock();
                       counter++;
                       mutex.unlock();
                    });
             }

             // Give time for threads to wait
             std::this_thread::sleep_for(100ms);

             // Unlock - this should allow exactly one thread to proceed
             mutex.unlock();

             // Wait for all threads to finish
             for (auto& t : threads)
             {
                t.join();
             }

             REQUIRE(counter == 3);
          },
          5000ms);

      REQUIRE(success);
   }
}

TEST_CASE("interprocess_mutex stress test", "[mutex][stress]")
{
   // This is a stress test to catch any potential deadlocks or race conditions
   SECTION("Stress test with many threads")
   {
      interprocess_mutex mutex;
      std::atomic<int>   counter     = 0;
      const int          num_threads = 20;
      const int          iterations  = 1000;

      bool success = run_with_timeout(
          [&]()
          {
             std::vector<std::thread> threads;

             for (int i = 0; i < num_threads; i++)
             {
                threads.emplace_back(
                    [&]()
                    {
                       arbtrie::set_current_thread_name("stress-t");
                       for (int j = 0; j < iterations; j++)
                       {
                          // Mix lock and try_lock for better test coverage
                          if (j % 10 == 0)
                          {
                             if (mutex.try_lock())
                             {
                                counter++;
                                mutex.unlock();
                             }
                             else
                             {
                                // If try_lock fails, use regular lock
                                mutex.lock();
                                counter++;
                                mutex.unlock();
                             }
                          }
                          else
                          {
                             mutex.lock();
                             counter++;
                             mutex.unlock();
                          }
                       }
                    });
             }

             for (auto& t : threads)
             {
                t.join();
             }
          },
          30000ms);  // 30 second timeout for this stress test

      REQUIRE(success);
      REQUIRE(counter == num_threads * iterations);
   }
}