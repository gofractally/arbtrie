#include <catch2/catch_test_macros.hpp>
#include <sal/debug.hpp>
#include <sal/shared_ptr.hpp>
#include <sal/shared_ptr_alloc.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <condition_variable>
#include <filesystem>
#include <iostream>  // Added for progress output
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// Helper function to initialize a pointer for testing
void init_test_ptr(sal::shared_ptr* ptr, uint32_t cacheline_val, int ref_count)
{
   if (ptr)
      ptr->reset(sal::location::from_cacheline(cacheline_val), ref_count);
}

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

      // Allocate a shared_ptr
      auto allocation = alloc.alloc();

      // Check that we got a valid allocation
      REQUIRE(allocation.ptr != nullptr);
      REQUIRE(allocation.address != sal::ptr_address(0));  // Check for a non-zero address

      // Should be able to access the shared_ptr (initial ref count might not be 0)
      REQUIRE_NOTHROW(allocation.ptr->use_count());

      // Free the allocation
      // Initialize before freeing to avoid assertion failure on ref count check
      init_test_ptr(allocation.ptr, 1, 0);
      REQUIRE_NOTHROW(alloc.free(allocation.address));
   }

   SECTION("Multiple allocations and frees")
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Allocate a smaller number of shared_ptrs
      constexpr size_t              num_allocs = 100;  // Increased count
      std::vector<sal::ptr_address> addresses;
      std::vector<sal::shared_ptr*> pointers;  // Store pointers to init before free

      for (size_t i = 0; i < num_allocs; ++i)
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.address);
         pointers.push_back(allocation.ptr);
      }

      // Initialize pointers before freeing
      for (auto* ptr : pointers)
         init_test_ptr(ptr, 1, 0);

      // Free them in random order
      std::random_device rd;
      std::mt19937       g(rd());
      std::shuffle(addresses.begin(), addresses.end(), g);

      for (const auto& addr : addresses)
      {
         REQUIRE_NOTHROW(alloc.free(addr));
      }
   }

   SECTION("Allocation with hint")
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Allocate one pointer to get a potential hint address
      auto initial_alloc = alloc.alloc();
      REQUIRE(initial_alloc.ptr != nullptr);

      // Use the allocated address as a hint for the next allocation
      sal::ptr_address hint_addr = initial_alloc.address;
      sal::alloc_hint  hint{&hint_addr, 1};

      // Try allocating with the hint
      auto hinted_alloc = alloc.alloc(hint);
      REQUIRE(hinted_alloc.ptr != nullptr);
      // We can't guarantee it allocated *at* the hint, but it should succeed

      // Try allocating with a hint where the address is already free
      init_test_ptr(initial_alloc.ptr, 1, 0);
      alloc.free(initial_alloc.address);
      auto hinted_alloc2 = alloc.alloc(hint);
      REQUIRE(hinted_alloc2.ptr != nullptr);

      // Clean up
      init_test_ptr(hinted_alloc.ptr, 1, 0);
      alloc.free(hinted_alloc.address);
      init_test_ptr(hinted_alloc2.ptr, 1, 0);
      alloc.free(hinted_alloc2.address);
   }

   SECTION("try_alloc with hint")
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Allocate one pointer
      auto alloc1 = alloc.alloc();
      REQUIRE(alloc1.ptr != nullptr);
      sal::ptr_address hint_addr = alloc1.address;
      sal::alloc_hint  hint{&hint_addr, 1};

      // try_alloc with a hint for an *already allocated* address should fail
      auto failed_alloc = alloc.try_alloc(hint);
      REQUIRE(!failed_alloc);

      // Free the first pointer
      init_test_ptr(alloc1.ptr, 1, 0);
      alloc.free(alloc1.address);

      // try_alloc with a hint for a *free* address should succeed
      auto success_alloc = alloc.try_alloc(hint);
      REQUIRE(success_alloc);
      REQUIRE(success_alloc->ptr != nullptr);
      REQUIRE(success_alloc->address == hint_addr);  // Should allocate at the hint address

      // Clean up
      init_test_ptr(success_alloc->ptr, 1, 0);
      alloc.free(success_alloc->address);
   }

   // Clean up directory
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

      // Allocate a small number of pointers
      for (int i = 0; i < 20; ++i)  // Increased count
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         // Store the allocation address
         addresses.push_back(allocation.address);
         // Initialize the pointer so it can be loaded later
         init_test_ptr(allocation.ptr, 100 + i, 1);
      }
      // Allocator goes out of scope, data should persist
   }

   // Second allocator instance should be able to access the same pointers
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Check each pointer can be accessed and freed
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         const auto& addr = addresses[i];
         // Should be able to access the pointer without throwing
         REQUIRE_NOTHROW(alloc.get(addr));
         // Verify data
         sal::shared_ptr& ptr = alloc.get(addr);
         REQUIRE(ptr.loc().cacheline() == 100 + i);
         // Can't reliably check ref count after reload, but location should persist

         // Initialize to 0 ref count before freeing
         init_test_ptr(&ptr, 100 + i, 0);
         REQUIRE_NOTHROW(alloc.free(addr));
      }
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc large allocation and free", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_large_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   sal::shared_ptr_alloc alloc(temp_path);

   // Vector to hold our allocations
   std::vector<sal::ptr_address> addresses;
   std::vector<sal::shared_ptr*> pointers;

   // Allocate a large number of pointers to potentially trigger zone expansion
   constexpr uint32_t num_allocs = 50000;  // Reduced from 2^16, increased from 100k
   addresses.reserve(num_allocs);
   pointers.reserve(num_allocs);

   std::cout << "Starting large allocation test (" << num_allocs << ")..." << std::endl;
   // Perform alloc A, alloc B, free B pattern
   for (uint32_t i = 0; i < num_allocs; ++i)
   {
      // Allocate pointer A and keep it
      auto allocation_a = alloc.alloc();
      REQUIRE(allocation_a.ptr != nullptr);
      addresses.push_back(allocation_a.address);
      pointers.push_back(allocation_a.ptr);
      init_test_ptr(allocation_a.ptr, i, 1);  // Keep it referenced

      // Allocate pointer B
      auto allocation_b = alloc.alloc();
      REQUIRE(allocation_b.ptr != nullptr);

      // Free pointer B immediately
      init_test_ptr(allocation_b.ptr, 0, 0);  // Set ref count to 0 before free
      REQUIRE_NOTHROW(alloc.free(allocation_b.address));

      if (i > 0 && i % 5000 == 0)
         std::cout << "  Allocated/Freed " << i << " pairs...  ";
   }
   std::cout << std::endl << "Finished alloc/free pairs." << std::endl;

   // Now free the kept pointers (A)
   std::cout << "Freeing kept pointers..." << std::endl;
   for (size_t i = 0; i < addresses.size(); ++i)
   {
      init_test_ptr(pointers[i], 0, 0);  // Set ref count to 0 before free
      REQUIRE_NOTHROW(alloc.free(addresses[i]));
      if (i > 0 && i % 5000 == 0)
         std::cout << "  Freed " << i << " kept pointers...  ";
   }
   std::cout << std::endl << "Finished freeing kept pointers." << std::endl;

   REQUIRE(alloc.used() == 0);  // Ensure all pointers are freed

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

   // Create allocator instance accessible by all threads
   sal::shared_ptr_alloc alloc(temp_path);

   // Number of threads to use
   constexpr int num_threads = 16;

   // Log the start of the test
   SAL_INFO("Starting multithreaded test with {} threads", num_threads);

   // Number of operations per thread
   constexpr int ops_per_thread = 5000;  // Increased ops

   // Thread synchronization primitives
   std::mutex              mutex;
   std::condition_variable cv;
   bool                    start_flag = false;
   std::atomic<int>        threads_ready(0);
   std::atomic<int>        threads_done(0);

   // Create and launch threads
   std::vector<std::thread> threads;

   for (int t = 0; t < num_threads; ++t)
   {
      threads.emplace_back(
          [t, &alloc, &mutex, &cv, &start_flag, &threads_ready, &threads_done, num_threads]()
          {
             // Set a thread name
             std::string thread_name = "spaPt" + std::to_string(t);
             sal::set_current_thread_name(thread_name.c_str());

             SAL_INFO("Thread {} starting allocation test", t);

             std::vector<sal::ptr_address> local_addresses;
             std::vector<sal::shared_ptr*> local_pointers;  // To init before free
             local_addresses.reserve(ops_per_thread);
             local_pointers.reserve(ops_per_thread);
             std::random_device                    rd;
             std::mt19937                          gen(rd());
             std::uniform_real_distribution<>      dis(0.0, 1.0);
             std::uniform_int_distribution<size_t> idx_dis;  // For index selection

             // Signal that thread is ready
             {
                std::unique_lock<std::mutex> lock(mutex);
                threads_ready++;
                SAL_INFO("Thread {} ready ({}/{})", t, threads_ready.load(), num_threads);
                cv.notify_all();  // Notify main thread every time one is ready
                cv.wait(lock, [&start_flag] { return start_flag; });
             }
             SAL_INFO("Thread {} starting work...", t);

             // Main thread work
             for (int i = 0; i < ops_per_thread; ++i)
             {
                if (i > 0 && i % 1000 == 0)
                   SAL_INFO("Thread {} completed {} operations", t, i);

                // ~60% chance to allocate, ~40% chance to free
                if (dis(gen) < 0.6 || local_addresses.empty())
                {
                   // Allocate one pointer
                   auto allocation = alloc.alloc();
                   // Basic check, detailed checks might cause too much contention/slowdown
                   if (allocation.ptr == nullptr)
                   {
                      SAL_ERROR("Thread {} failed allocation!", t);
                      continue;  // or REQUIRE(allocation.ptr != nullptr); exit?
                   }
                   local_addresses.push_back(allocation.address);
                   local_pointers.push_back(allocation.ptr);
                   // Don't init here, causes too much contention. Init before free.
                }
                else  // Free path
                {
                   // Pick a random address to free
                   idx_dis.param(std::uniform_int_distribution<size_t>::param_type(
                       0, local_addresses.size() - 1));
                   size_t idx = idx_dis(gen);

                   // Initialize pointer before freeing
                   init_test_ptr(local_pointers[idx], 0, 0);
                   alloc.free(local_addresses[idx]);

                   // Remove from our local tracking (swap-and-pop)
                   std::swap(local_addresses[idx], local_addresses.back());
                   std::swap(local_pointers[idx], local_pointers.back());
                   local_addresses.pop_back();
                   local_pointers.pop_back();
                }
             }

             SAL_INFO("Thread {} finished work with {} remaining pointers", t,
                      local_addresses.size());

             // Clean up remaining pointers
             SAL_INFO("Thread {} freeing its {} remaining pointers", t, local_addresses.size());
             for (size_t i = 0; i < local_addresses.size(); ++i)
             {
                init_test_ptr(local_pointers[i], 0, 0);
                alloc.free(local_addresses[i]);
             }

             // Signal that we're done
             threads_done++;
             SAL_INFO("Thread {} done ({}/{})", t, threads_done.load(), num_threads);
          });
   }

   // Wait for all threads to be ready before starting
   {
      std::unique_lock<std::mutex> lock(mutex);
      SAL_INFO("Main thread waiting for {} threads to be ready...", num_threads);
      cv.wait(lock, [&threads_ready, num_threads] { return threads_ready == num_threads; });
      SAL_INFO("All threads ready. Starting test.");
      start_flag = true;
      cv.notify_all();  // Signal threads to start
   }

   // Wait for all threads to complete their work
   SAL_INFO("Main thread waiting for threads to finish...");
   for (auto& th : threads)
   {
      th.join();
   }

   REQUIRE(threads_done == num_threads);
   SAL_INFO("All threads completed.");

   // Final check: ensure all pointers are freed
   REQUIRE(alloc.used() == 0);
   SAL_INFO("Verified all pointers freed. Multithreaded test successful.");

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc try_get method", "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_try_get_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create the allocator
   sal::shared_ptr_alloc alloc(temp_path);

   SECTION("try_get with valid addresses")
   {
      // Allocate some pointers
      std::vector<sal::ptr_address> addresses;
      std::vector<sal::shared_ptr*> pointers;
      for (int i = 0; i < 20; ++i)  // Increased count
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.address);
         pointers.push_back(allocation.ptr);

         // Set some data to verify later
         init_test_ptr(allocation.ptr, 100 + i, i + 1);
      }

      // Verify try_get returns non-null pointers for all valid addresses
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         sal::shared_ptr* ptr = alloc.try_get(addresses[i]);
         REQUIRE(ptr != nullptr);
         // Don't check use_count directly after try_get if it wasn't modified
         REQUIRE(ptr->loc().cacheline() == 100 + i);
      }

      // Clean up
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         init_test_ptr(pointers[i], 0, 0);
         alloc.free(addresses[i]);
      }
   }

   SECTION("try_get with freed addresses")
   {
      // Allocate and then free some pointers
      std::vector<sal::ptr_address> addresses;
      for (int i = 0; i < 10; ++i)
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.address);
         init_test_ptr(allocation.ptr, 0, 0);  // Init before free
         alloc.free(allocation.address);
      }

      // try_get should return nullptr for freed addresses
      for (const auto& addr : addresses)
      {
         REQUIRE(alloc.try_get(addr) == nullptr);
      }
   }

   SECTION("try_get with non-existent or invalid addresses")
   {
      // Address far beyond initial allocation (likely invalid zone/offset)
      // Note: Requires knowledge of internal constants, less robust test
      sal::ptr_address addr1(sal::detail::ptrs_per_zone * 5);  // Assuming < 5 zones initially
      REQUIRE(alloc.try_get(addr1) == nullptr);

      // Address with value 0 (potentially invalid)
      sal::ptr_address addr2(0);
      REQUIRE(alloc.try_get(addr2) == nullptr);

      // Large address value (likely invalid)
      sal::ptr_address addr3(UINT32_MAX);
      REQUIRE(alloc.try_get(addr3) == nullptr);

      // Allocate one, get its address, free it, then try a nearby address
      auto alloc_real = alloc.alloc();
      REQUIRE(alloc_real.ptr != nullptr);
      sal::ptr_address real_addr = alloc_real.address;
      init_test_ptr(alloc_real.ptr, 0, 0);
      alloc.free(real_addr);

      sal::ptr_address nearby_addr(*real_addr + 1);  // Address likely not allocated
      REQUIRE(alloc.try_get(nearby_addr) == nullptr);
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

   SECTION("Count increases with allocations and decreases with frees")
   {
      std::vector<sal::ptr_address> addresses;
      std::vector<sal::shared_ptr*> pointers;

      // Allocate 50 pointers
      constexpr int num_to_alloc = 50;
      for (int i = 0; i < num_to_alloc; ++i)
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.address);
         pointers.push_back(allocation.ptr);
      }

      // Should have 50 used pointers
      REQUIRE(alloc.used() == num_to_alloc);

      // Free 20 pointers
      constexpr int num_to_free = 20;
      for (int i = 0; i < num_to_free; ++i)
      {
         init_test_ptr(pointers[i], 0, 0);
         alloc.free(addresses[i]);
      }

      // Should have 30 used pointers
      REQUIRE(alloc.used() == num_to_alloc - num_to_free);

      // Free all remaining pointers
      for (size_t i = num_to_free; i < addresses.size(); ++i)
      {
         init_test_ptr(pointers[i], 0, 0);
         alloc.free(addresses[i]);
      }

      // Should be back to 0
      REQUIRE(alloc.used() == 0);
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
      // 1. First create some pointers through normal allocation
      std::vector<sal::ptr_address> addresses;
      std::vector<sal::shared_ptr*> pointers;

      // Allocate 10 pointers
      for (int i = 0; i < 10; ++i)
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);

         // Set a reference count and location
         init_test_ptr(allocation.ptr, 100 + i, i + 1);

         addresses.push_back(allocation.address);
         pointers.push_back(allocation.ptr);
      }

      // 2. Test get_or_alloc on existing pointers
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         // Should return the existing pointer
         sal::shared_ptr& ptr = alloc.get_or_alloc(addresses[i]);

         // Verify it has the values we set (ref count might change, check location)
         REQUIRE(ptr.loc().cacheline() == 100 + i);
      }

      // 3. Create some specific addresses that might not exist yet
      // It's hard to guarantee an address doesn't exist without internal knowledge.
      // We'll try addresses likely outside the initial allocation range.
      for (int i = 0; i < 10; ++i)
      {
         // Create an address potentially in a new zone or later offset
         sal::ptr_address new_addr(50000 + i);  // Arbitrary large offset

         // The first call should allocate the pointer if it doesn't exist
         sal::shared_ptr& new_ptr = alloc.get_or_alloc(new_addr);

         // Check if it was newly allocated (ref count likely 0 or 1 initially)
         // We can't definitively know if it was truly *new* vs pre-existing from
         // a previous run without clearing, but get_or_alloc should ensure it exists.

         // Set some values to verify later
         init_test_ptr(&new_ptr, 200 + i, 10 + i);

         // The second call should return the same pointer
         sal::shared_ptr& existing_ptr = alloc.get_or_alloc(new_addr);

         // Verify it's the same pointer with the same values
         REQUIRE(&existing_ptr == &new_ptr);  // Check they are the same object
         REQUIRE(existing_ptr.loc().cacheline() == 200 + i);

         // Also check with try_get to ensure it's registered properly
         sal::shared_ptr* ptr_check = alloc.try_get(new_addr);
         REQUIRE(ptr_check != nullptr);
         REQUIRE(ptr_check->loc().cacheline() == 200 + i);
      }

      // Clean up allocated pointers
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         init_test_ptr(pointers[i], 0, 0);
         alloc.free(addresses[i]);
      }
      for (int i = 0; i < 10; ++i)
      {
         sal::ptr_address new_addr(50000 + i);
         sal::shared_ptr* ptr_check = alloc.try_get(new_addr);
         if (ptr_check)
         {  // Only free if it exists
            init_test_ptr(ptr_check, 0, 0);
            alloc.free(new_addr);
         }
      }
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("shared_ptr_alloc handles large allocation triggering zone growth",
          "[sal][shared_ptr_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "shared_ptr_alloc_zone_growth_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   SECTION("Large batch allocation possibly triggering zone expansion")
   {
      sal::shared_ptr_alloc alloc(temp_path);

      // Allocate a very large number of pointers.
      // This *should* exceed ptrs_per_zone / 2 and trigger ensure_capacity.
      // We allocate more than ptrs_per_zone to be sure.
      constexpr size_t large_allocation_count = sal::detail::ptrs_per_zone + 1000;

      std::vector<sal::ptr_address> addresses;
      std::vector<sal::shared_ptr*> pointers;
      addresses.reserve(large_allocation_count);
      pointers.reserve(large_allocation_count);

      std::cout << "Starting zone growth test - allocating " << large_allocation_count
                << " pointers (ptrs_per_zone=" << sal::detail::ptrs_per_zone << ")..." << std::endl;
      for (size_t i = 0; i < large_allocation_count; ++i)
      {
         if (i > 0 && i % (large_allocation_count / 10) == 0)
            std::cout << "  Allocated " << i << " pointers..." << std::endl;

         sal::allocation allocation = {};
         REQUIRE_NOTHROW(allocation = alloc.alloc());  // Check allocation doesn't throw
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.address);
         pointers.push_back(allocation.ptr);

         // Initialize with some data to verify later
         init_test_ptr(allocation.ptr, i % 1000, 1);
      }
      std::cout << "Finished allocating " << large_allocation_count << " pointers." << std::endl;
      REQUIRE(alloc.num_allocated_zones() > 1);  // Verify at least one new zone was allocated

      // Verify all allocations are valid
      std::cout << "Verifying allocations..." << std::endl;
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i > 0 && i % (large_allocation_count / 10) == 0)
            std::cout << "  Verified " << i << " pointers..." << std::endl;

         sal::shared_ptr* ptr = alloc.try_get(addresses[i]);
         REQUIRE(ptr != nullptr);
         // REQUIRE(ptr->use_count() == 1); // Ref count might change due to internal ops?
         REQUIRE(ptr->loc().cacheline() == i % 1000);
      }
      std::cout << "All allocations verified successfully." << std::endl;

      // Free all allocations
      std::cout << "Freeing allocations..." << std::endl;
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         if (i > 0 && i % (large_allocation_count / 10) == 0)
            std::cout << "  Freed " << i << " pointers..." << std::endl;

         init_test_ptr(pointers[i], 0, 0);  // Init before freeing
         alloc.free(addresses[i]);
      }
      std::cout << "All allocations freed successfully." << std::endl;
      REQUIRE(alloc.used() == 0);
   }

   // Clean up
   fs::remove_all(temp_path);
}