#include <catch2/catch_test_macros.hpp>
#include <sal/debug.hpp>
#include <sal/shared_ptr.hpp>
#include <sal/shared_ptr_alloc.hpp>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

TEST_CASE("shared_ptr_alloc basic operations", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   SECTION("Construction and destruction")
   {
      // Make sure we can construct and destruct the allocator
      REQUIRE_NOTHROW(sal::shared_ptr_alloc(temp_path));
   }

   SECTION("Basic allocation and freeing")
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Get a region for allocation
      auto region = alloc.next_region();

      // Allocate a shared_ptr
      auto allocation = alloc.alloc(region);

      // Check that we got a valid allocation
      REQUIRE(allocation.ptr != nullptr);

      // Should be able to access the shared_ptr
      REQUIRE(allocation.ptr->use_count() == 0);

      // Free the allocation
      REQUIRE_NOTHROW(alloc.free(allocation.address));
   }

   SECTION("Multiple allocations and frees")
   {
      sal::shared_ptr_alloc alloc(temp_path);
      auto                  region = alloc.next_region();

      // Allocate a smaller number of shared_ptrs to avoid potential issues
      constexpr size_t              num_allocs = 10;
      std::vector<sal::ptr_address> addresses;

      for (size_t i = 0; i < num_allocs; ++i)
      {
         auto allocation = alloc.alloc(region);
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.address);
      }

      // Free them in random order
      std::random_device rd;
      std::mt19937       g(rd());
      std::shuffle(addresses.begin(), addresses.end(), g);

      for (const auto& addr : addresses)
      {
         REQUIRE_NOTHROW(alloc.free(addr));
      }
   }

   SECTION("Region selection")
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Get multiple regions
      auto region1 = alloc.next_region();
      auto region2 = alloc.next_region();

      // They should be different
      REQUIRE(*region1 != *region2);

      // Should be able to allocate in different regions
      auto alloc1 = alloc.alloc(region1);
      auto alloc2 = alloc.alloc(region2);

      // The allocations should have different regions
      REQUIRE(*alloc1.address.region != *alloc2.address.region);

      // Free them
      REQUIRE_NOTHROW(alloc.free(alloc1.address));
      REQUIRE_NOTHROW(alloc.free(alloc2.address));
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc persistence", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_persist_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   std::vector<sal::ptr_address> addresses;

   // First allocator instance
   {
      sal::shared_ptr_alloc alloc(temp_path);
      auto                  region = alloc.next_region();

      // Allocate a small number of pointers
      for (int i = 0; i < 5; ++i)
      {
         auto allocation = alloc.alloc(region);

         // Store the allocation address
         addresses.push_back(allocation.address);
      }
   }

   // Second allocator instance should be able to access the same pointers
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Check each pointer can be accessed and freed
      for (const auto& addr : addresses)
      {
         // Should be able to access the pointer without throwing
         REQUIRE_NOTHROW(alloc.get(addr));

         // Free the pointer
         REQUIRE_NOTHROW(alloc.free(addr));
      }
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc exhaustive allocation and free", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_exhaust_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   sal::shared_ptr_alloc alloc(temp_path);
   auto                  region = alloc.next_region();

   // Vector to hold our permanent allocations (A pointers)
   std::vector<sal::ptr_address> permanent_addrs;

   // The number of iterations to run (2^16-1)
   constexpr uint32_t iterations = (1 << 16) - 2;

   sal::ptr_address::index_type last_index(1);
   // Perform the alloc A, alloc B, free B pattern
   for (uint32_t i = 0; i < iterations; ++i)
   {
      // Allocate pointer A and keep it
      auto allocation_a = alloc.alloc(region);
      REQUIRE(allocation_a.ptr != nullptr);
      permanent_addrs.push_back(allocation_a.address);
      assert(last_index == allocation_a.address.index);
      // Allocate pointer B
      auto allocation_b = alloc.alloc(region);
      REQUIRE(allocation_b.ptr != nullptr);
      last_index = allocation_b.address.index;

      // Free pointer B immediately
      REQUIRE_NOTHROW(alloc.free(allocation_b.address));

      // Every 1000 iterations, print progress
      if (i % 1000 == 0)
      {
         std::cout << "Completed " << i << " iterations out of " << iterations << "    \r";
      }
   }
   permanent_addrs.push_back(alloc.alloc(region).address);
   // Try one final allocation - should fail since we've used all available pointers
   REQUIRE_THROWS_AS(alloc.alloc(region), std::runtime_error);

   // Now test the reverse pattern: free one, alloc one, free the allocated one
   // This tests if we can properly reuse just-freed slots and then free them again

   // Iterate through all permanent addresses
   for (size_t i = 0; i < permanent_addrs.size(); ++i)
   {
      // Free one pointer from our original allocations
      REQUIRE_NOTHROW(alloc.free(permanent_addrs[i]));

      // Allocate one pointer (should succeed since we freed one)
      auto new_alloc = alloc.alloc(region);
      REQUIRE(new_alloc.ptr != nullptr);

      // Free the pointer we just allocated
      REQUIRE_NOTHROW(alloc.free(new_alloc.address));

      // Every 1000 iterations, print progress
      if (i % 1000 == 0)
      {
         std::cout << "Reverse pattern: Completed " << i << " iterations" << "    \r";
      }
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc multithreaded test", "[sal][shared_ptr_alloc][multithreaded]")
{
   // Set main thread name
   sal::set_current_thread_name("TestMain");

   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_mt_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create allocator and single region for all threads
   sal::shared_ptr_alloc alloc(temp_path);
   auto                  region = alloc.next_region();

   // Number of threads to use
   constexpr int num_threads = 16;

   // Log the start of the test
   SAL_INFO("Starting multithreaded test with {} threads", num_threads);

   // Number of operations per thread
   constexpr int ops_per_thread = 2000;

   // Thread synchronization primitives
   std::mutex              mutex;
   std::condition_variable cv;
   bool                    start_flag = false;
   std::atomic<int>        threads_ready(0);
   std::atomic<int>        threads_done(0);

   // No need to collect addresses from threads, each thread will handle its own cleanup

   // Create and launch threads
   std::vector<std::thread> threads;

   for (int t = 0; t < num_threads; ++t)
   {
      threads.emplace_back(
          [t, &alloc, region, &mutex, &cv, &start_flag, &threads_ready, &threads_done]()
          {
             // Set a thread name (8 chars or less)
             std::string thread_name = "spaPt" + std::to_string(t);
             sal::set_current_thread_name(thread_name.c_str());

             // Log the thread start
             SAL_INFO("Thread {} starting allocation test", t);

             std::vector<sal::ptr_address>    local_addresses;
             std::random_device               rd;
             std::mt19937                     gen(rd());
             std::uniform_real_distribution<> dis(0.0, 1.0);

             // Signal that thread is ready
             {
                std::unique_lock<std::mutex> lock(mutex);
                threads_ready++;
                if (threads_ready == num_threads)
                   cv.notify_all();
                else
                   // Wait for all threads to be ready
                   cv.wait(lock, [&start_flag] { return start_flag; });
             }

             // Main thread work
             for (int i = 0; i < ops_per_thread; ++i)
             {
                // Log progress every 1000 operations
                if (i > 0 && i % 1000 == 0)
                {
                   SAL_INFO("Thread {} completed {} operations", t, i);
                }

                // Always allocate one pointer
                auto allocation = alloc.alloc(region);
                local_addresses.push_back(allocation.address);

                // 30% chance to free a random previously allocated pointer
                if (dis(gen) < 0.4 && !local_addresses.empty())
                {
                   // Pick a random address to free
                   size_t idx = static_cast<size_t>(dis(gen) * local_addresses.size());
                   if (idx >= local_addresses.size())
                      idx = local_addresses.size() - 1;

                   alloc.free(local_addresses[idx]);

                   // Remove from our local tracking
                   local_addresses.erase(local_addresses.begin() + idx);
                }
             }

             // Log completion
             SAL_INFO("Thread {} finished with {} remaining pointers", t, local_addresses.size());

             // Clean up our own pointers
             SAL_INFO("Thread {} freeing its {} remaining pointers", t, local_addresses.size());
             for (const auto& addr : local_addresses)
             {
                // Free each pointer this thread still owns
                alloc.free(addr);
             }

             // Signal that we're done
             threads_done++;
          });
   }

   // Start all threads simultaneously
   {
      std::unique_lock<std::mutex> lock(mutex);
      // Wait for all threads to be ready
      cv.wait(lock, [&threads_ready, num_threads] { return threads_ready == num_threads; });
      start_flag = true;
      cv.notify_all();
   }

   // Wait for all threads to complete their work
   for (auto& t : threads)
   {
      t.join();
   }

   REQUIRE(threads_done == num_threads);
   SAL_INFO("All threads completed, test successful");

   // No need to free pointers here, each thread cleaned up its own pointers

   // Clean up
   fs::remove_all(temp_path);

   SAL_INFO("Multithreaded test completed successfully");
}