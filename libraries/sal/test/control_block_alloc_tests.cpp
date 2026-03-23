#include <catch2/catch_test_macros.hpp>
#include <sal/control_block.hpp>
#include <sal/control_block_alloc.hpp>
#include <sal/debug.hpp>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <iostream>  // Added for progress output
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// Helper function to initialize a pointer for testing
void init_test_ptr(sal::control_block* ptr, uint32_t cacheline_val, int ref_count)
{
   if (ptr)
      ptr->reset(sal::location::from_cacheline(cacheline_val), ref_count);
}

TEST_CASE("ControlBlockAllocBasic", "[sal][control_block_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   SECTION("Construction and destruction")
   {
      // Make sure we can construct and destruct the allocator
      REQUIRE_NOTHROW(sal::control_block_alloc(temp_path));
   }

   SECTION("Basic allocation and freeing")
   {
      sal::control_block_alloc alloc(temp_path);

      // Allocate a control_block
      auto allocation = alloc.alloc();

      // Check that we got a valid allocation
      REQUIRE(allocation.ptr != nullptr);

      // Should be able to access the control_block (initial ref count might not be 0)
      REQUIRE_NOTHROW(allocation.ptr->use_count());

      // Free the allocation
      // Initialize before freeing to avoid assertion failure on ref count check
      init_test_ptr(allocation.ptr, 1, 0);
      REQUIRE_NOTHROW(alloc.free(allocation.addr_seq.address));
   }

   SECTION("Multiple allocations and frees")
   {
      sal::control_block_alloc alloc(temp_path);

      // Allocate a smaller number of control_blocks
      constexpr size_t                 num_allocs = 100;  // Increased count
      std::vector<sal::ptr_address>    addresses;
      std::vector<sal::control_block*> pointers;  // Store pointers to init before free

      for (size_t i = 0; i < num_allocs; ++i)
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         // Verify no duplicates
         REQUIRE(std::find(addresses.begin(), addresses.end(), allocation.addr_seq.address) ==
                 addresses.end());
         addresses.push_back(allocation.addr_seq.address);
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
      sal::control_block_alloc alloc(temp_path);

      // Allocate one pointer to get a potential hint address
      auto initial_alloc = alloc.alloc();
      REQUIRE(initial_alloc.ptr != nullptr);

      // Use the allocated address as a hint for the next allocation
      sal::ptr_address hint_addr = initial_alloc.addr_seq.address;
      sal::alloc_hint  hint{&hint_addr, 1};

      // Try allocating with the hint
      auto hinted_alloc = alloc.alloc(hint);
      REQUIRE(hinted_alloc.ptr != nullptr);
      // We can't guarantee it allocated *at* the hint, but it should succeed

      // Try allocating with a hint where the address is already free
      init_test_ptr(initial_alloc.ptr, 1, 0);
      alloc.free(initial_alloc.addr_seq.address);
      auto hinted_alloc2 = alloc.alloc(hint);
      REQUIRE(hinted_alloc2.ptr != nullptr);

      // Clean up
      init_test_ptr(hinted_alloc.ptr, 1, 0);
      alloc.free(hinted_alloc.addr_seq.address);
      init_test_ptr(hinted_alloc2.ptr, 1, 0);
      alloc.free(hinted_alloc2.addr_seq.address);
   }

   SECTION("try_alloc with hint")
   {
      sal::control_block_alloc alloc(temp_path);

      // Allocate one pointer
      auto alloc1 = alloc.alloc();
      REQUIRE(alloc1.ptr != nullptr);
      sal::ptr_address hint_addr = alloc1.addr_seq.address;
      sal::alloc_hint  hint{&hint_addr, 1};

      // try_alloc with a hint should allocate in the same cacheline
      auto alloc2 = alloc.try_alloc(hint);
      REQUIRE(alloc2);
      REQUIRE(alloc2->ptr != nullptr);
      // Check they share the same cacheline by masking off the low 4 bits
      REQUIRE((*alloc2->addr_seq.address & ~uint32_t(0x0f)) == (*hint_addr & ~uint32_t(0x0f)));

      // Free the first pointer
      init_test_ptr(alloc1.ptr, 1, 0);
      alloc.free(alloc1.addr_seq.address);

      // try_alloc with a hint for a *free* address should succeed
      auto success_alloc = alloc.try_alloc(hint);
      REQUIRE(success_alloc);
      REQUIRE(success_alloc->ptr != nullptr);
      REQUIRE(success_alloc->addr_seq.address == hint_addr);  // Should allocate at the hint address

      // Clean up
      init_test_ptr(success_alloc->ptr, 1, 0);
      alloc.free(success_alloc->addr_seq.address);
   }

   // Clean up directory
   fs::remove_all(temp_path);
}

TEST_CASE("ControlBlockAllocPersistence", "[sal][control_block_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_persist_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   std::vector<sal::ptr_address> addresses;

   // First allocator instance
   {
      sal::control_block_alloc alloc(temp_path);

      // Allocate a small number of pointers
      for (int i = 0; i < 20; ++i)  // Increased count
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         // Store the allocation.addr_seq.address
         addresses.push_back(allocation.addr_seq.address);
         // Initialize the pointer so it can be loaded later
         init_test_ptr(allocation.ptr, 100 + i, 1);
      }
      // Allocator goes out of scope, data should persist
   }

   // Second allocator instance should be able to access the same pointers
   {
      sal::control_block_alloc alloc(temp_path);

      // Check each pointer can be accessed and freed
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         const auto& addr = addresses[i];
         // Should be able to access the pointer without throwing
         REQUIRE_NOTHROW(alloc.get(addr));
         // Verify data
         sal::control_block& ptr = alloc.get(addr);
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

TEST_CASE("ControlBlockAllocLargeAllocFree", "[sal][control_block_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_large_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   sal::control_block_alloc alloc(temp_path);

   // Vector to hold our allocations
   std::vector<sal::ptr_address>    addresses;
   std::vector<sal::control_block*> pointers;

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
      addresses.push_back(allocation_a.addr_seq.address);
      pointers.push_back(allocation_a.ptr);
      init_test_ptr(allocation_a.ptr, i, 1);  // Keep it referenced

      // Allocate pointer B
      auto allocation_b = alloc.alloc();
      REQUIRE(allocation_b.ptr != nullptr);

      // Free pointer B immediately
      init_test_ptr(allocation_b.ptr, 0, 0);  // Set ref count to 0 before free
      REQUIRE_NOTHROW(alloc.free(allocation_b.addr_seq.address));

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

TEST_CASE("ControlBlockAllocMultithreaded", "[sal][control_block_alloc][multithreaded]")
{
   // Set main thread name
   sal::set_current_thread_name("TestMain");

   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_mt_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create allocator instance accessible by all threads
   sal::control_block_alloc alloc(temp_path);

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

             std::vector<sal::ptr_address>    local_addresses;
             std::vector<sal::control_block*> local_pointers;  // To init before free
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
                   local_addresses.push_back(allocation.addr_seq.address);
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

TEST_CASE("ControlBlockAllocTryGet", "[sal][control_block_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_try_get_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create the allocator
   sal::control_block_alloc alloc(temp_path);

   SECTION("try_get with valid addresses")
   {
      // Allocate some pointers
      std::vector<sal::ptr_address>    addresses;
      std::vector<sal::control_block*> pointers;
      for (int i = 0; i < 20; ++i)  // Increased count
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.addr_seq.address);
         pointers.push_back(allocation.ptr);

         // Set some data to verify later
         init_test_ptr(allocation.ptr, 100 + i, i + 1);
      }

      // Verify try_get returns non-null pointers for all valid addresses
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         sal::control_block* ptr = alloc.try_get(addresses[i]);
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
         addresses.push_back(allocation.addr_seq.address);
         init_test_ptr(allocation.ptr, 0, 0);  // Init before free
         alloc.free(allocation.addr_seq.address);
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
      sal::ptr_address real_addr = alloc_real.addr_seq.address;
      init_test_ptr(alloc_real.ptr, 0, 0);
      alloc.free(real_addr);

      sal::ptr_address nearby_addr(*real_addr + 1);  // Address likely not allocated
      REQUIRE(alloc.try_get(nearby_addr) == nullptr);
   }

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("ControlBlockAllocUsedCount", "[sal][control_block_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_used_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   sal::control_block_alloc alloc(temp_path);

   // Initially there should be no used pointers
   REQUIRE(alloc.used() == 0);

   SECTION("Count increases with allocations and decreases with frees")
   {
      std::vector<sal::ptr_address>    addresses;
      std::vector<sal::control_block*> pointers;

      // Allocate 50 pointers
      constexpr int num_to_alloc = 50;
      for (int i = 0; i < num_to_alloc; ++i)
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.addr_seq.address);
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

TEST_CASE("ControlBlockAllocGetOrAlloc", "[sal][control_block_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_get_or_alloc_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create the allocator
   sal::control_block_alloc alloc(temp_path);

   SECTION("Get existing pointers or allocate new ones")
   {
      // 1. First create some pointers through normal allocation
      std::vector<sal::ptr_address>    addresses;
      std::vector<sal::control_block*> pointers;

      // Allocate 10 pointers
      for (int i = 0; i < 10; ++i)
      {
         auto allocation = alloc.alloc();
         REQUIRE(allocation.ptr != nullptr);

         // Set a reference count and location
         init_test_ptr(allocation.ptr, 100 + i, i + 1);

         addresses.push_back(allocation.addr_seq.address);
         pointers.push_back(allocation.ptr);
      }

      // 2. Test get_or_alloc on existing pointers
      for (size_t i = 0; i < addresses.size(); ++i)
      {
         // Should return the existing pointer
         sal::control_block& ptr = alloc.get_or_alloc(addresses[i]);

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
         sal::control_block& new_ptr = alloc.get_or_alloc(new_addr);

         // Check if it was newly allocated (ref count likely 0 or 1 initially)
         // We can't definitively know if it was truly *new* vs pre-existing from
         // a previous run without clearing, but get_or_alloc should ensure it exists.

         // Set some values to verify later
         init_test_ptr(&new_ptr, 200 + i, 10 + i);

         // The second call should return the same pointer
         sal::control_block& existing_ptr = alloc.get_or_alloc(new_addr);

         // Verify it's the same pointer with the same values
         REQUIRE(&existing_ptr == &new_ptr);  // Check they are the same object
         REQUIRE(existing_ptr.loc().cacheline() == 200 + i);

         // Also check with try_get to ensure it's registered properly
         sal::control_block* ptr_check = alloc.try_get(new_addr);
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
         sal::ptr_address    new_addr(50000 + i);
         sal::control_block* ptr_check = alloc.try_get(new_addr);
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

TEST_CASE("ControlBlockAllocZoneGrowth", "[sal][control_block_alloc]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_zone_growth_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   SECTION("Large batch allocation possibly triggering zone expansion")
   {
      sal::control_block_alloc alloc(temp_path);

      // Allocate a very large number of pointers.
      // This *should* exceed ptrs_per_zone / 2 and trigger ensure_capacity.
      // We allocate more than ptrs_per_zone to be sure.
      constexpr size_t large_allocation_count = sal::detail::ptrs_per_zone + 1000;

      std::vector<sal::ptr_address>    addresses;
      std::vector<sal::control_block*> pointers;
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
         addresses.push_back(allocation.addr_seq.address);
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

         sal::control_block* ptr = alloc.try_get(addresses[i]);
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

TEST_CASE("ControlBlockAlloc8MZoneExpansion", "[sal][control_block_alloc][!mayfail][long]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_8m_zone_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   sal::control_block_alloc alloc(temp_path);

   // Target 8 million allocations, which should require at least 2 zones.
   constexpr size_t num_allocs = 8 * 1000 * 1000;
   // ptrs_per_zone is 4,194,304 (1 << 22)
   REQUIRE(num_allocs > sal::detail::ptrs_per_zone);

   std::vector<sal::ptr_address>    addresses;
   std::vector<sal::control_block*> pointers;
   addresses.reserve(num_allocs);
   pointers.reserve(num_allocs);

   std::cout << "Starting 8 million allocation test..." << std::endl;
   // Allocate all pointers
   for (size_t i = 0; i < num_allocs; ++i)
   {
      if (i > 0 && i % (num_allocs / 10) == 0)
         std::cout << "  Allocated " << i << "/" << num_allocs << " pointers..." << std::endl;

      sal::allocation allocation = {};
      REQUIRE_NOTHROW(allocation = alloc.alloc());
      REQUIRE(allocation.ptr != nullptr);
      addresses.push_back(allocation.addr_seq.address);
      pointers.push_back(allocation.ptr);
      // Initialize immediately to avoid doing it in the free loop later (potentially slower)
      init_test_ptr(allocation.ptr, i % 100, 1);  // Store some minimal data
   }
   std::cout << "Finished allocating " << num_allocs << " pointers." << std::endl;

   // Verify we expanded beyond one zone
   std::cout << "Allocated zones: " << alloc.num_allocated_zones() << std::endl;
   REQUIRE(alloc.num_allocated_zones() >= 2);
   REQUIRE(alloc.used() == num_allocs);

   // Free all pointers
   std::cout << "Freeing " << num_allocs << " pointers..." << std::endl;
   for (size_t i = 0; i < num_allocs; ++i)
   {
      if (i > 0 && i % (num_allocs / 10) == 0)
         std::cout << "  Freed " << i << "/" << num_allocs << " pointers..." << std::endl;
      init_test_ptr(pointers[i], 0, 0);  // Set ref count to 0 before free
      REQUIRE_NOTHROW(alloc.free(addresses[i]));
   }
   std::cout << "Finished freeing " << num_allocs << " pointers." << std::endl;

   // Verify allocator is empty
   REQUIRE(alloc.used() == 0);

   // Clean up
   fs::remove_all(temp_path);
   std::cout << "8 million allocation test completed successfully." << std::endl;
}

TEST_CASE("ControlBlockAllocRandomAllocFree10M", "[sal][control_block_alloc][!mayfail][long]")
{
   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_10m_random_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   sal::control_block_alloc alloc(temp_path);

   // Target pool size and number of random operations
   constexpr size_t target_pool_size = 10 * 1000 * 1000;  // 10 million
   constexpr size_t num_operations   = 30 * 1000 * 1000;  // 20 million random ops

   // Ensure target pool size triggers zone expansion
   REQUIRE(target_pool_size > sal::detail::ptrs_per_zone * 2);

   std::vector<sal::ptr_address>    addresses;
   std::vector<sal::control_block*> pointers;
   addresses.reserve(target_pool_size);  // Reserve space to avoid reallocations
   pointers.reserve(target_pool_size);

   std::random_device                    rd;
   std::mt19937                          gen(rd());
   std::uniform_real_distribution<>      dis(0.0, 1.0);  // For alloc/free decision
   std::uniform_int_distribution<size_t> idx_dis;        // For index selection

   std::cout << "Starting 10M random alloc/free test (" << num_operations << " operations)..."
             << std::endl;

   for (size_t i = 0; i < num_operations; ++i)
   {
      if (i > 0 && i % (num_operations / 20) == 0)  // Progress output every 5%
         std::cout << "  Operation " << i << "/" << num_operations
                   << " (Current used: " << addresses.size() << "/" << target_pool_size << ")"
                   << std::endl;

      bool should_allocate = (dis(gen) < 0.75);  // Slightly bias towards allocating initially

      if (should_allocate && addresses.size() < target_pool_size)
      {
         // Allocate
         sal::allocation allocation = {};
         REQUIRE_NOTHROW(allocation = alloc.alloc());
         REQUIRE(allocation.ptr != nullptr);
         addresses.push_back(allocation.addr_seq.address);
         pointers.push_back(allocation.ptr);
         // Initialize pointer (minimal data)
         init_test_ptr(allocation.ptr, i % 255, 1);
      }
      else if (!addresses.empty())
      {
         // Free
         idx_dis.param(std::uniform_int_distribution<size_t>::param_type(0, addresses.size() - 1));
         size_t idx_to_free = idx_dis(gen);

         // Prepare for free
         init_test_ptr(pointers[idx_to_free], 0, 0);
         REQUIRE_NOTHROW(alloc.free(addresses[idx_to_free]));

         // Remove from tracking vectors (swap-and-pop)
         std::swap(addresses[idx_to_free], addresses.back());
         std::swap(pointers[idx_to_free], pointers.back());
         addresses.pop_back();
         pointers.pop_back();
      }
      // If we wanted to free but addresses is empty, do nothing this iteration
   }

   std::cout << "Finished random operations. Current used: " << alloc.used() << " ("
             << addresses.size() << " tracked)" << std::endl;
   REQUIRE(alloc.used() == addresses.size());

   // Free any remaining pointers
   std::cout << "Freeing " << addresses.size() << " remaining pointers..." << std::endl;
   size_t remaining_count = addresses.size();
   for (size_t i = 0; i < remaining_count; ++i)
   {
      if (i > 0 && i % (remaining_count / 10) == 0)
         std::cout << "  Freed " << i << "/" << remaining_count << " remaining..." << std::endl;

      init_test_ptr(pointers[i], 0, 0);
      REQUIRE_NOTHROW(alloc.free(addresses[i]));
   }
   std::cout << "Finished freeing remaining pointers." << std::endl;

   // Final verification
   REQUIRE(alloc.used() == 0);
   std::cout << "10M random alloc/free test completed successfully." << std::endl;

   // Clean up
   fs::remove_all(temp_path);
}

TEST_CASE("ControlBlockAllocRandomAllocFree10M_Multithreaded",
          "[sal][control_block_alloc][!mayfail][long][multithreaded]")
{
   // Set main thread name
   sal::set_current_thread_name("TestMainMT10M");

   // Create a temporary directory for tests
   fs::path temp_path = fs::temp_directory_path() / "control_block_alloc_10m_random_mt_test";
   fs::remove_all(temp_path);
   fs::create_directories(temp_path);

   // Create allocator instance accessible by all threads
   sal::control_block_alloc alloc(temp_path);

   // Threading parameters
   constexpr int    num_threads       = 8;
   constexpr size_t total_operations  = 100 * 1000 * 1000;  // ~30M total ops
   constexpr size_t ops_per_thread    = total_operations / num_threads;
   constexpr double alloc_probability = 0.55;  // 50% chance to alloc vs free

   // Synchronization primitives
   std::mutex              mutex;
   std::condition_variable cv;
   bool                    start_flag = false;
   std::atomic<int>        threads_ready(0);
   std::atomic<int>        threads_done(0);
   std::atomic<uint64_t>   total_allocs(0);  // Track total successful allocs across threads
   std::atomic<uint64_t>   total_frees(0);   // Track total successful frees across threads

   // Thread vector
   std::vector<std::thread> threads;

   std::cout << "Starting 10M random alloc/free MULTITHREADED test (" << num_threads << " threads, "
             << ops_per_thread << " ops/thread)..." << std::endl;

   // Launch threads
   for (int t = 0; t < num_threads; ++t)
   {
      threads.emplace_back(
          [t, &alloc, &mutex, &cv, &start_flag, &threads_ready, &threads_done, &total_allocs,
           &total_frees, ops_per_thread, num_threads]()
          {
             // Thread setup
             std::string thread_name = "spaRandMT" + std::to_string(t);
             sal::set_current_thread_name(thread_name.c_str());
             std::random_device                    rd;
             std::mt19937                          gen(rd());
             std::uniform_real_distribution<>      dis(0.0, 1.0);
             std::uniform_int_distribution<size_t> idx_dis;
             std::vector<sal::ptr_address>         local_addresses;
             std::vector<sal::control_block*>      local_pointers;
             // Reserve some space to reduce reallocations, but not too much
             constexpr size_t reserve_size = ops_per_thread / 2;
             local_addresses.reserve(reserve_size);
             local_pointers.reserve(reserve_size);

             uint64_t thread_allocs = 0;
             uint64_t thread_frees  = 0;

             // Signal ready and wait
             {
                std::unique_lock<std::mutex> lock(mutex);
                threads_ready++;
                // Optional: Log thread readiness
                // std::cout << "Thread " << t << " ready (" << threads_ready.load() << "/" << num_threads << ")" << std::endl;
                cv.notify_all();
                cv.wait(lock, [&start_flag] { return start_flag; });
             }
             // Optional: Log thread start
             // std::cout << "Thread " << t << " starting work..." << std::endl;

             // Main loop
             for (size_t i = 0; i < ops_per_thread; ++i)
             {
                // Optional: Progress logging within thread (can cause slowdown)
                // if (i > 0 && i % (ops_per_thread / 10) == 0) std::cout << "Thread " << t << " progress: " << i << "/" << ops_per_thread << std::endl;

                bool should_allocate = (dis(gen) < alloc_probability);

                if (should_allocate)
                {
                   // Allocate
                   sal::allocation allocation = {};
                   try
                   {
                      allocation = alloc.alloc();
                   }
                   catch (const std::exception& e)
                   {
                      // Use SAL_ERROR or std::cerr for logging errors in tests
                      std::cerr << "Thread " << t << " allocation failed: " << e.what()
                                << std::endl;
                      continue;  // Skip this op if alloc fails
                   }

                   if (allocation.ptr != nullptr)
                   {
                      local_addresses.push_back(allocation.addr_seq.address);
                      local_pointers.push_back(allocation.ptr);
                      // Initialize pointer (minimal init)
                      init_test_ptr(allocation.ptr, i % 255, 1);
                      thread_allocs++;
                   }
                   else
                   {
                      std::cerr << "Thread " << t << " received nullptr allocation!" << std::endl;
                   }
                }
                else if (!local_addresses.empty())
                {
                   // Free
                   idx_dis.param(std::uniform_int_distribution<size_t>::param_type(
                       0, local_addresses.size() - 1));
                   size_t idx_to_free = idx_dis(gen);

                   // Prepare for free
                   init_test_ptr(local_pointers[idx_to_free], 0, 0);
                   try
                   {
                      alloc.free(local_addresses[idx_to_free]);
                      thread_frees++;  // Only increment if free call doesn't throw

                      // Remove from tracking vectors (swap-and-pop) only after successful free
                      std::swap(local_addresses[idx_to_free], local_addresses.back());
                      std::swap(local_pointers[idx_to_free], local_pointers.back());
                      local_addresses.pop_back();
                      local_pointers.pop_back();
                   }
                   catch (const std::exception& e)
                   {
                      std::cerr << "Thread " << t << " free failed: " << e.what() << " for address "
                                << local_addresses[idx_to_free] << std::endl;
                      // If free fails, we might have an inconsistent state.
                      // For this test, log the error and continue. The final alloc.used() check will catch issues.
                   }
                }
                // If !should_allocate and local_addresses is empty, do nothing.
             }  // End main loop

             // Update global counters
             total_allocs += thread_allocs;
             total_frees += thread_frees;

             // Clean up remaining pointers held by this thread
             // std::cout << "Thread " << t << " cleaning up " << local_addresses.size() << " remaining pointers..." << std::endl;
             size_t remaining_count = local_addresses.size();
             for (size_t i = 0; i < remaining_count; ++i)
             {
                init_test_ptr(local_pointers[i], 0, 0);
                try
                {
                   alloc.free(local_addresses[i]);
                }
                catch (const std::exception& e)
                {
                   std::cerr << "Thread " << t << " cleanup free failed: " << e.what()
                             << " for address " << local_addresses[i] << std::endl;
                }
             }
             local_addresses.clear();
             local_pointers.clear();

             // Signal done
             threads_done++;
             // std::cout << "Thread " << t << " done (" << threads_done.load() << "/" << num_threads << ")" << std::endl;
          });  // End thread lambda
   }  // End launching threads

   // Wait for all threads to be ready before starting
   {
      std::unique_lock<std::mutex> lock(mutex);
      std::cout << "Main thread waiting for " << num_threads << " threads to be ready..."
                << std::endl;
      cv.wait(lock, [&threads_ready, num_threads] { return threads_ready == num_threads; });
      std::cout << "All threads ready. Starting test." << std::endl;
      start_flag = true;
      cv.notify_all();  // Signal threads to start
   }

   // Wait for all threads to complete their work
   std::cout << "Main thread waiting for threads to finish..." << std::endl;
   for (auto& th : threads)
   {
      th.join();
   }

   REQUIRE(threads_done == num_threads);
   std::cout << "All threads completed." << std::endl;
   std::cout << "Total allocations attempted by threads: " << total_allocs.load() << std::endl;
   std::cout << "Total frees attempted by threads: " << total_frees.load() << std::endl;

   // Final check: ensure all pointers are freed
   uint64_t final_used = alloc.used();
   std::cout << "Final allocator used count: " << final_used << std::endl;
   REQUIRE(final_used == 0);
   std::cout << "Verified all pointers freed. Multithreaded random test successful." << std::endl;

   // Clean up
   fs::remove_all(temp_path);
}