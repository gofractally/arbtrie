#include <catch2/catch_test_macros.hpp>
#include <sal/debug.hpp>
#include <sal/shared_ptr.hpp>
#include <sal/shared_ptr_alloc.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
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
      auto region = alloc.get_new_region();

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
      auto                  region = alloc.get_new_region();

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
      auto region1 = alloc.get_new_region();
      auto region2 = alloc.get_new_region();

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
      auto                  region = alloc.get_new_region();

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
   auto                  region = alloc.get_new_region();

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
      //   assert(last_index == allocation_a.address.index);
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
   auto                  region = alloc.get_new_region();

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

TEST_CASE("shared_ptr_alloc reset_all_refs", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_reset_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create a vector to track our allocations
   std::vector<sal::ptr_address> addresses;
   std::vector<sal::shared_ptr*> pointers;

   // Create the allocator and a region for allocations
   sal::shared_ptr_alloc alloc(temp_path);
   auto                  region = alloc.get_new_region();

   SECTION("Reset ref counts to 1 for active pointers")
   {
      // Allocate several pointers and set them to different states
      for (int i = 0; i < 10; ++i)
      {
         auto allocation = alloc.alloc(region);
         addresses.push_back(allocation.address);
         pointers.push_back(allocation.ptr);

         // Set the location to a valid value (non-zero cacheline)
         allocation.ptr->reset(sal::location::from_cacheline(100 + i), 0);

         // For each pointer, set a different reference count
         // - Some with 0 refs (should be freed by reset_all_refs)
         // - Some with 1 ref (should remain at 1)
         // - Some with >1 refs (should be reset to 1)
         if (i % 3 == 0)
         {
            // Set to 0 refs - these should be freed
            allocation.ptr->set_ref(0);
            REQUIRE(allocation.ptr->use_count() == 0);
         }
         else if (i % 3 == 1)
         {
            // Set to 1 ref - these should remain at 1
            allocation.ptr->set_ref(1);
            REQUIRE(allocation.ptr->use_count() == 1);
         }
         else
         {
            // Set to >1 refs - these should be reset to 1
            allocation.ptr->set_ref(5);
            REQUIRE(allocation.ptr->use_count() == 5);
         }
      }

      // Now call reset_all_refs
      alloc.reset_all_refs();

      // Verify the results for each pointer
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i % 3 == 0)
         {
            // These should have been freed, try to re-allocate them
            auto allocation = alloc.alloc(region);

            // In a FIFO system, we'd expect to get the freed pointers back in order
            // but this might be implementation dependent, so we'll just verify we can allocate
            REQUIRE(allocation.ptr != nullptr);

            // Clean up
            alloc.free(allocation.address);
         }
         else
         {
            // For both 1-ref and >1-ref cases, they should now be exactly 1
            // We need to access the pointers again to see the updated reference count
            sal::shared_ptr& ptr = alloc.get(addresses[i]);
            REQUIRE(ptr.use_count() == 1);
         }
      }
   }

   SECTION("reset_all_refs should handle wrap-around case")
   {
      // Allocate enough pointers to wrap around the first page
      // We'll need at least 512 pointers to trigger wrapping
      std::vector<sal::ptr_address> wrap_addresses;
      std::vector<sal::shared_ptr*> wrap_pointers;

      // First fill up one page
      for (int i = 0; i < 512; ++i)
      {
         auto allocation = alloc.alloc(region);

         // Set location and ref count for every pointer
         allocation.ptr->reset(sal::location::from_cacheline(100 + i), 3);

         wrap_addresses.push_back(allocation.address);
         wrap_pointers.push_back(allocation.ptr);
      }

      // Now free every other pointer to create a sparse pattern
      for (size_t i = 0; i < wrap_addresses.size(); i += 2)
      {
         alloc.free(wrap_addresses[i]);
      }

      // Set various ref counts for the remaining pointers
      for (size_t i = 1; i < wrap_addresses.size(); i += 2)
      {
         int ref_count = (i % 6 == 1) ? 0 : ((i % 6 == 3) ? 1 : 5);
         wrap_pointers[i]->set_ref(ref_count);
         // Verify the reference count was set correctly
         REQUIRE(wrap_pointers[i]->use_count() == ref_count);
      }

      // Call reset_all_refs
      alloc.reset_all_refs();

      // Verify the results
      for (size_t i = 1; i < wrap_addresses.size(); i += 2)
      {
         if (i % 6 == 1)
         {
            // These had 0 refs and should be freed - skip them
            continue;
         }
         else
         {
            // First check directly via the pointer
            int direct_ref_count = wrap_pointers[i]->use_count();

            // Then check via alloc.get()
            sal::shared_ptr& ptr           = alloc.get(wrap_addresses[i]);
            int              get_ref_count = ptr.use_count();

            // They should match and be 1
            REQUIRE(direct_ref_count == 1);
            REQUIRE(get_ref_count == 1);
         }
      }
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc release_unreachable", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_release_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   SECTION("Release unreferenced and unreachable pointers")
   {
      // Create a vector to track our allocations
      std::vector<sal::ptr_address> addresses;
      std::vector<sal::shared_ptr*> pointers;

      // Create the allocator and a region for allocations
      sal::shared_ptr_alloc alloc(temp_path);
      auto                  region = alloc.get_new_region();

      // Allocate several pointers and set them to different states
      for (int i = 0; i < 10; ++i)
      {
         auto allocation = alloc.alloc(region);
         addresses.push_back(allocation.address);
         pointers.push_back(allocation.ptr);

         // Set the location to a valid value (non-zero cacheline)
         allocation.ptr->reset(sal::location::from_cacheline(100 + i), 0);

         // For each pointer, set a different reference count:
         // - Some with 0 refs (should be freed immediately)
         // - Some with 1 ref (should be freed after release)
         // - Some with 2 refs (should be kept, but decremented to 1)
         // - Some with >2 refs (should be kept, but decremented to >1)
         if (i % 4 == 0)
         {
            // Set to 0 refs - these should be freed immediately
            allocation.ptr->set_ref(0);
            REQUIRE(allocation.ptr->use_count() == 0);
         }
         else if (i % 4 == 1)
         {
            // Set to 1 ref - these should be freed after release
            allocation.ptr->set_ref(1);
            REQUIRE(allocation.ptr->use_count() == 1);
         }
         else if (i % 4 == 2)
         {
            // Set to 2 refs - these should be decremented to 1
            allocation.ptr->set_ref(2);
            REQUIRE(allocation.ptr->use_count() == 2);
         }
         else
         {
            // Set to >2 refs - these should be decremented but still >1
            allocation.ptr->set_ref(5);
            REQUIRE(allocation.ptr->use_count() == 5);
         }
      }

      // Now call release_unreachable
      alloc.release_unreachable();

      // Verify the results
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i % 4 == 0 || i % 4 == 1)
         {
            // These had 0 or 1 refs and should have been freed
            // Try to re-allocate to confirm
            auto allocation = alloc.alloc(region);

            // We're not guaranteed to get the same pointers back in the same order
            // But we should be able to allocate a new pointer
            REQUIRE(allocation.ptr != nullptr);

            // Clean up
            alloc.free(allocation.address);
         }
         else if (i % 4 == 2)
         {
            // These had 2 refs, should now have 1
            sal::shared_ptr& ptr = alloc.get(addresses[i]);
            REQUIRE(ptr.use_count() == 1);
         }
         else
         {
            // These had 5 refs, should now have 4
            sal::shared_ptr& ptr = alloc.get(addresses[i]);
            REQUIRE(ptr.use_count() == 4);
         }
      }
   }

   SECTION("Combination of reset_all_refs and release_unreachable")
   {
      // Create the allocator and a region for allocations
      sal::shared_ptr_alloc alloc(temp_path);
      auto                  region = alloc.get_new_region();

      std::vector<sal::ptr_address> addresses;
      std::vector<sal::shared_ptr*> pointers;

      // Create a "reachable" tree-like structure of nodes with different ref counts

      // The root node will have ref=3 initially
      auto root = alloc.alloc(region);
      root.ptr->reset(sal::location::from_cacheline(200), 3);
      addresses.push_back(root.address);
      pointers.push_back(root.ptr);

      // Create some child nodes with ref>1 (simulating shared references)
      for (int i = 0; i < 3; ++i)
      {
         auto child = alloc.alloc(region);
         child.ptr->reset(sal::location::from_cacheline(300 + i), 2);
         addresses.push_back(child.address);
         pointers.push_back(child.ptr);
      }

      // Create some nodes with ref=1 (these would be freed by release_unreachable)
      for (int i = 0; i < 3; ++i)
      {
         auto node = alloc.alloc(region);
         node.ptr->reset(sal::location::from_cacheline(400 + i), 1);
         addresses.push_back(node.address);
         pointers.push_back(node.ptr);
      }

      // Create some nodes with ref=0 (these would be freed immediately)
      for (int i = 0; i < 3; ++i)
      {
         auto node = alloc.alloc(region);
         node.ptr->reset(sal::location::from_cacheline(500 + i), 0);
         addresses.push_back(node.address);
         pointers.push_back(node.ptr);
      }

      // First reset all refs (sets all refs>1 to 1)
      alloc.reset_all_refs();

      // Verify refs were reset
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i < 7)  // The first 7 pointers had ref counts > 0
         {
            sal::shared_ptr& ptr = alloc.get(addresses[i]);
            if (pointers[i]->use_count() > 0)
               REQUIRE(ptr.use_count() == 1);
         }
      }

      // Now simulate retaining the reachable nodes (root and its children)
      // by incrementing their reference counts
      for (size_t i = 0; i < 4; ++i)  // Root + 3 children
      {
         sal::shared_ptr& ptr = alloc.get(addresses[i]);
         REQUIRE(ptr.retain());  // Increment to 2
      }

      // Now release_unreachable should free nodes with ref=1 and decrement others
      alloc.release_unreachable();

      // Verify the results
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i < 4)  // Reachable nodes (root + children)
         {
            // These had ref=2, should now have ref=1
            sal::shared_ptr& ptr = alloc.get(addresses[i]);
            REQUIRE(ptr.use_count() == 1);
         }
         else
         {
            // These had ref=1 or ref=0, should be freed
            // Attempt to allocate to verify
            auto allocation = alloc.alloc(region);
            REQUIRE(allocation.ptr != nullptr);
            alloc.free(allocation.address);
         }
      }
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc try_get method", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_try_get_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create the allocator and a region for allocations
   sal::shared_ptr_alloc alloc(temp_path);
   auto                  region = alloc.get_new_region();

   SECTION("try_get with valid addresses")
   {
      // Allocate some pointers
      std::vector<sal::ptr_address> addresses;
      for (int i = 0; i < 5; ++i)
      {
         auto allocation = alloc.alloc(region);
         addresses.push_back(allocation.address);

         // Set some data to verify later
         allocation.ptr->reset(sal::location::from_cacheline(100 + i), i + 1);
      }

      // Verify try_get returns non-null pointers for all valid addresses
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         sal::shared_ptr* ptr = alloc.try_get(addresses[i]);
         REQUIRE(ptr != nullptr);
         REQUIRE(ptr->use_count() == i + 1);
         REQUIRE(ptr->loc().cacheline() == 100 + i);
      }

      // Clean up
      for (const auto& addr : addresses)
      {
         alloc.free(addr);
      }
   }

   SECTION("try_get with freed addresses")
   {
      // Allocate and then free some pointers
      std::vector<sal::ptr_address> addresses;
      for (int i = 0; i < 5; ++i)
      {
         auto allocation = alloc.alloc(region);
         addresses.push_back(allocation.address);
         alloc.free(allocation.address);
      }

      // try_get should return nullptr for freed addresses
      for (const auto& addr : addresses)
      {
         REQUIRE(alloc.try_get(addr) == nullptr);
      }
   }

   SECTION("try_get with non-existent addresses")
   {
      // Create some addresses pointing to non-existent locations

      // Address in a valid region but with a page that hasn't been allocated
      sal::ptr_address addr1(region,
                             sal::ptr_address::index_type(65000));  // Far beyond any allocated page
      REQUIRE(alloc.try_get(addr1) == nullptr);

      // Address in a non-existent region
      sal::ptr_address addr2(sal::ptr_address::region_type(65000), sal::ptr_address::index_type(0));
      REQUIRE(alloc.try_get(addr2) == nullptr);

      // Address with an out-of-bounds index value
      sal::ptr_address addr3(region, sal::ptr_address::index_type(UINT16_MAX));
      REQUIRE(alloc.try_get(addr3) == nullptr);
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc used count", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_used_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   sal::shared_ptr_alloc alloc(temp_path);

   // Initially there should be no used pointers
   REQUIRE(alloc.used() == 0);

   SECTION("Count increases with allocations")
   {
      // Create some pointers in the first region
      auto                          region1 = alloc.get_new_region();
      std::vector<sal::ptr_address> addresses1;

      // Allocate 10 pointers in first region
      for (int i = 0; i < 10; ++i)
      {
         auto allocation = alloc.alloc(region1);
         addresses1.push_back(allocation.address);
      }

      // Should have 10 used pointers
      REQUIRE(alloc.used() == 10);

      // Create another region with different pointers
      auto                          region2 = alloc.get_new_region();
      std::vector<sal::ptr_address> addresses2;

      // Allocate 5 pointers in second region
      for (int i = 0; i < 5; ++i)
      {
         auto allocation = alloc.alloc(region2);
         addresses2.push_back(allocation.address);
      }

      // Should have 15 used pointers
      REQUIRE(alloc.used() == 15);

      // Free some pointers from the first region
      for (int i = 0; i < 3; ++i)
      {
         alloc.free(addresses1[i]);
      }

      // Should have 12 used pointers
      REQUIRE(alloc.used() == 12);

      // Free all remaining pointers
      for (size_t i = 3; i < addresses1.size(); ++i)
      {
         alloc.free(addresses1[i]);
      }

      for (auto& addr : addresses2)
      {
         alloc.free(addr);
      }

      // Should be back to 0
      REQUIRE(alloc.used() == 0);
   }

   SECTION("Handles pointers across many regions")
   {
      std::vector<sal::ptr_address::region_type> regions;
      std::vector<sal::ptr_address>              all_addresses;

      // Create 8 regions to ensure we span multiple 64-bit entries in region_use_counts
      // (since each 64-bit entry contains 4 region counts)
      for (int r = 0; r < 8; ++r)
      {
         regions.push_back(alloc.get_new_region());

         // Allocate a different number of pointers in each region
         int count = r + 1;
         for (int i = 0; i < count; ++i)
         {
            auto allocation = alloc.alloc(regions.back());
            all_addresses.push_back(allocation.address);
         }
      }

      // Total should be sum of 1+2+3+4+5+6+7+8 = 36
      REQUIRE(alloc.used() == 36);

      // Free all pointers
      for (auto& addr : all_addresses)
      {
         alloc.free(addr);
      }

      // Should be back to 0
      REQUIRE(alloc.used() == 0);
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc region statistics", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_region_stats_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create the allocator
   sal::shared_ptr_alloc alloc(temp_path);

   SECTION("Empty allocator should return default stats")
   {
      auto stats = alloc.region_stats();
      REQUIRE(stats.count == 0);
      REQUIRE(stats.min == 0);
      REQUIRE(stats.max == 0);
      REQUIRE(stats.mean == 0.0);
      REQUIRE(stats.stddev == 0.0);
   }

   SECTION("Statistics with varied region usage")
   {
      // Create multiple regions with different counts of allocations
      std::vector<sal::ptr_address::region_type> regions;
      std::vector<std::vector<sal::ptr_address>> region_addresses;

      // Create 5 regions with different usage patterns
      // Region 0: 5 pointers
      // Region 1: 10 pointers
      // Region 2: 15 pointers
      // Region 3: 20 pointers
      // Region 4: 25 pointers
      const int num_regions = 5;
      region_addresses.resize(num_regions);

      for (int r = 0; r < num_regions; ++r)
      {
         auto region = alloc.get_new_region();
         regions.push_back(region);

         // Allocate different number of pointers in each region
         int count = (r + 1) * 5;  // 5, 10, 15, 20, 25
         for (int i = 0; i < count; ++i)
         {
            auto allocation = alloc.alloc(region);
            region_addresses[r].push_back(allocation.address);
         }
      }

      // Check statistics
      auto stats = alloc.region_stats();

      // We should have 5 regions with allocations
      REQUIRE(stats.count == 5);

      // Min should be 5, max should be 25
      REQUIRE(stats.min == 5);
      REQUIRE(stats.max == 25);

      // Mean should be (5+10+15+20+25)/5 = 15
      REQUIRE(std::abs(stats.mean - 15.0) < 0.001);

      // Stddev calculation: sqrt(sum((x-mean)^2)/n)
      // (5-15)^2 + (10-15)^2 + (15-15)^2 + (20-15)^2 + (25-15)^2 = 100+25+0+25+100 = 250
      // sqrt(250/5) = sqrt(50) ≈ 7.07
      REQUIRE(std::abs(stats.stddev - 7.07) < 0.01);

      // Free pointers in one region and check updated stats
      for (auto& addr : region_addresses[2])  // Free all pointers in region 2 (15 pointers)
      {
         alloc.free(addr);
      }

      auto updated_stats = alloc.region_stats();

      // Now we should have 4 regions with allocations
      REQUIRE(updated_stats.count == 4);

      // Min should still be 5, max should still be 25
      REQUIRE(updated_stats.min == 5);
      REQUIRE(updated_stats.max == 25);

      // Mean should be (5+10+20+25)/4 = 15
      REQUIRE(std::abs(updated_stats.mean - 15.0) < 0.001);

      // New stddev: sqrt(sum((x-mean)^2)/n)
      // (5-15)^2 + (10-15)^2 + (20-15)^2 + (25-15)^2 = 100+25+25+100 = 250
      // sqrt(250/4) = sqrt(62.5) ≈ 7.91
      REQUIRE(std::abs(updated_stats.stddev - 7.91) < 0.01);
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc clear_active_bits", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_clear_active_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create the allocator
   sal::shared_ptr_alloc alloc(temp_path);

   SECTION("Clear active bits in a specific range of regions")
   {
      // Create multiple regions with pointers having different states
      std::vector<sal::ptr_address::region_type> regions;
      std::vector<std::vector<sal::ptr_address>> region_addresses;
      std::vector<std::vector<sal::shared_ptr*>> region_pointers;

      // Set up 5 regions with 10 pointers each
      const int num_regions     = 5;
      const int ptrs_per_region = 10;

      regions.resize(num_regions);
      region_addresses.resize(num_regions);
      region_pointers.resize(num_regions);

      // Allocate pointers in each region
      for (int r = 0; r < num_regions; ++r)
      {
         // Get a new region for allocation
         regions[r] = alloc.get_new_region();

         // Allocate some pointers in this region
         for (int p = 0; p < ptrs_per_region; ++p)
         {
            auto allocation = alloc.alloc(regions[r]);
            region_addresses[r].push_back(allocation.address);
            region_pointers[r].push_back(allocation.ptr);

            // Set a valid location
            allocation.ptr->reset(sal::location::from_cacheline(100 + p), 1);

            // Set pending_cache bit on every other pointer
            if (p % 2 == 0)
            {
               // Call try_inc_activity() in a loop until it returns true twice
               // First call sets the active bit, second call sets the pending_cache bit
               bool active_set  = false;
               bool pending_set = false;

               while (!pending_set)
               {
                  bool result = allocation.ptr->try_inc_activity();
                  if (result)
                  {
                     if (!active_set)
                        active_set = true;
                     else
                        pending_set = true;
                  }
               }

               REQUIRE(allocation.ptr->pending_cache() == true);
            }
         }
      }

      // Count how many pointers have the pending_cache bit set initially
      int initial_pending_count = 0;
      for (int r = 0; r < num_regions; ++r)
      {
         for (int p = 0; p < ptrs_per_region; ++p)
         {
            if (region_pointers[r][p]->pending_cache())
               initial_pending_count++;
         }
      }

      // Should be half of all pointers
      REQUIRE(initial_pending_count == num_regions * ptrs_per_region / 2);

      // Now clear active bits for regions 1-3 (indices 1, 2)
      alloc.clear_active_bits(regions[1], 2);

      // Verify bits were cleared only in the specified regions
      int cleared_region_pending_count = 0;
      int other_region_pending_count   = 0;

      for (int r = 0; r < num_regions; ++r)
      {
         int region_pending_count = 0;
         for (int p = 0; p < ptrs_per_region; ++p)
         {
            if (region_pointers[r][p]->pending_cache())
               region_pending_count++;
         }

         if (r == 1 || r == 2)
         {
            // These regions should have no pending_cache bits set
            cleared_region_pending_count += region_pending_count;
         }
         else
         {
            // Other regions should still have pending_cache bits
            other_region_pending_count += region_pending_count;
         }
      }

      // The cleared regions should have no pending_cache bits
      REQUIRE(cleared_region_pending_count == 0);

      // The other regions should still have their pending_cache bits
      REQUIRE(other_region_pending_count == (num_regions - 2) * ptrs_per_region / 2);

      // Clear all regions
      alloc.clear_active_bits(regions[0], num_regions);

      // Verify all bits were cleared
      int final_pending_count = 0;
      for (int r = 0; r < num_regions; ++r)
      {
         for (int p = 0; p < ptrs_per_region; ++p)
         {
            if (region_pointers[r][p]->pending_cache())
               final_pending_count++;
         }
      }

      // All pending_cache bits should now be cleared
      REQUIRE(final_pending_count == 0);
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc get_or_alloc method", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_get_or_alloc_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create the allocator
   sal::shared_ptr_alloc alloc(temp_path);

   SECTION("Get existing pointers or allocate new ones")
   {
      // Get a new region
      auto region = alloc.get_new_region();

      // 1. First create some pointers through normal allocation
      std::vector<sal::ptr_address> addresses;

      // Allocate 5 pointers
      for (int i = 0; i < 5; ++i)
      {
         auto allocation = alloc.alloc(region);

         // Set a reference count and location
         allocation.ptr->reset(sal::location::from_cacheline(100 + i), i + 1);

         addresses.push_back(allocation.address);
      }

      // 2. Test get_or_alloc on existing pointers
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         // Should return the existing pointer
         sal::shared_ptr& ptr = alloc.get_or_alloc(addresses[i]);

         // Verify it has the values we set
         REQUIRE(ptr.use_count() == i + 1);
         REQUIRE(ptr.loc().cacheline() == 100 + i);
      }

      // 3. Create some specific addresses that don't exist yet
      for (int i = 0; i < 5; ++i)
      {
         // Create an address with a high index to ensure it doesn't exist yet
         auto             custom_index = 10000 + i;
         sal::ptr_address new_addr(region, sal::ptr_address::index_type(custom_index));

         // The first call should allocate the pointer
         sal::shared_ptr& new_ptr = alloc.get_or_alloc(new_addr);

         // Set some values to verify later
         new_ptr.reset(sal::location::from_cacheline(200 + i), 10 + i);

         // The second call should return the same pointer
         sal::shared_ptr& existing_ptr = alloc.get_or_alloc(new_addr);

         // Verify it's the same pointer with the same values
         REQUIRE(existing_ptr.use_count() == 10 + i);
         REQUIRE(existing_ptr.loc().cacheline() == 200 + i);

         // Also check with try_get to ensure it's registered properly
         sal::shared_ptr* ptr = alloc.try_get(new_addr);
         REQUIRE(ptr != nullptr);
         REQUIRE(ptr->use_count() == 10 + i);
      }

      // 4. Test with a completely new region
      auto             new_region = alloc.get_new_region();
      sal::ptr_address region_addr(new_region, sal::ptr_address::index_type(42));

      // Should create a new pointer
      sal::shared_ptr& region_ptr = alloc.get_or_alloc(region_addr);

      // Set some data to verify
      region_ptr.reset(sal::location::from_cacheline(500), 5);

      // Verify it exists and has the correct data
      REQUIRE(alloc.try_get(region_addr) != nullptr);
      REQUIRE(alloc.try_get(region_addr)->use_count() == 5);
      REQUIRE(alloc.try_get(region_addr)->loc().cacheline() == 500);
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("ptr_address memory layout and to_int", "[sal][ptr_address]")
{
   SECTION("Memory layout matches to_int() representation")
   {
      // Test various region/index combinations
      for (uint16_t r = 0; r < 0xFFFF; r += 0x1111)
      {
         for (uint16_t i = 0; i < 0xFFFF; i += 0x1111)
         {
            // Create address using explicit constructor
            sal::ptr_address addr{sal::ptr_address::region_type(r),
                                  sal::ptr_address::index_type(i)};

            // Manually compute expected integer representation
            uint32_t expected = (static_cast<uint32_t>(r) << 16) | i;

            // Test to_int() matches expected layout
            REQUIRE(addr.to_int() == expected);

            // Direct memory comparison between ptr_address and uint32_t
            REQUIRE(memcmp(&addr, &expected, sizeof(uint32_t)) == 0);

            // Test round-trip conversion using static method
            auto roundtrip = sal::ptr_address::from_int(expected);
            REQUIRE(*roundtrip.region == r);
            REQUIRE(*roundtrip.index == i);

            // Test explicit uint32_t constructor
            sal::ptr_address from_int(expected);
            REQUIRE(*from_int.region == r);
            REQUIRE(*from_int.index == i);
            REQUIRE(from_int.to_int() == expected);
         }
      }
   }
}

TEST_CASE("shared_ptr_alloc handles large initial allocation", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_large_initial_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   SECTION("Large batch allocation from scratch")
   {
      // Create a fresh allocator (nblocks = 0)
      sal::shared_ptr_alloc alloc(temp_path);
      auto                  region = alloc.get_new_region();

      // Force allocation of a large number of pages at once
      std::vector<sal::ptr_address> addresses;

      // Number that requires multiple pages (4000 pointers would require ~7-8 pages)
      // Each page holds 512 pointers, so we need enough to trigger multiple page allocations
      constexpr size_t large_allocation_count = 4000;

      // Reserve space to avoid vector reallocations
      addresses.reserve(large_allocation_count);

      // Allocate all pointers in a tight loop - this forces allocating many pages at once
      std::cout << "Starting large allocation test - allocating " << large_allocation_count
                << " pointers..." << std::endl;
      for (size_t i = 0; i < large_allocation_count; ++i)
      {
         if (i % 500 == 0)
         {
            std::cout << "Allocated " << i << " pointers..." << std::endl;
         }

         auto allocation = alloc.alloc(region);
         addresses.push_back(allocation.address);

         // Initialize with some data to verify
         allocation.ptr->reset(sal::location::from_cacheline(i % 1000), 1);
      }
      std::cout << "Finished allocating " << large_allocation_count << " pointers" << std::endl;

      // Verify all allocations are valid
      std::cout << "Verifying allocations..." << std::endl;
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i % 500 == 0)
         {
            std::cout << "Verified " << i << " pointers..." << std::endl;
         }

         sal::shared_ptr& ptr = alloc.get(addresses[i]);
         REQUIRE(ptr.use_count() == 1);
         REQUIRE(ptr.loc().cacheline() == i % 1000);
      }
      std::cout << "All allocations verified successfully" << std::endl;

      // Free all allocations
      std::cout << "Freeing allocations..." << std::endl;
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i % 500 == 0)
         {
            std::cout << "Freed " << i << " pointers..." << std::endl;
         }

         alloc.free(addresses[i]);
      }
      std::cout << "All allocations freed successfully" << std::endl;
   }

   // Clean up
   fs::remove_all(temp_path);
}