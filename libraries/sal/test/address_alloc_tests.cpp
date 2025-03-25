#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <thread>
#include <vector>

#include <sal/address_alloc.hpp>
#include <sal/debug.hpp>

using namespace sal;

TEST_CASE("address_alloc basic operations", "[address_alloc]")
{
   // Create a temporary directory for the test
   std::filesystem::path test_dir = std::filesystem::temp_directory_path() / "address_alloc_test";

   // Clean up any existing test directory before each section
   if (std::filesystem::exists(test_dir))
      std::filesystem::remove_all(test_dir);
   std::filesystem::create_directories(test_dir);

   SECTION("Create and destroy allocator")
   {
      address_alloc alloc(test_dir);
      REQUIRE(alloc.count() == 0);
   }

   SECTION("Allocate and free addresses")
   {
      address_alloc alloc(test_dir);

      // Get a new region
      auto region = alloc.get_new_region();
      REQUIRE(region == 0);

      // Allocate an address
      auto allocation = alloc.get_new_address(region);
      REQUIRE(allocation.addr.region == region);
      REQUIRE(allocation.addr.index == 0);
      REQUIRE(alloc.count() == 1);

      // Free the address
      alloc.free_address(allocation.addr);
      REQUIRE(alloc.count() == 0);
   }

   SECTION("Free Free Alloc pattern")
   {
      address_alloc alloc(test_dir);
      auto          region = alloc.get_new_region();

      // First, allocate a large batch of addresses
      std::vector<address> addresses;
      int                  initial_count = 513;  // Start with a large number
      addresses.reserve(initial_count);

      for (int i = 0; i < initial_count; ++i)
      {
         auto allocation = alloc.get_new_address(region);
         addresses.push_back(allocation.addr);
      }

      REQUIRE(alloc.count() == initial_count);
      SAL_WARN("Initial allocation complete. Count: {}", alloc.count());

      // Now perform the free-free-alloc-free pattern until we reach zero
      while (alloc.count() > 0)
      {
         // If we have at least 3 addresses left, proceed with the pattern
         if (addresses.size() >= 2)
         {
            // Free two addresses
            auto addr1 = addresses.back();
            addresses.pop_back();
            alloc.free_address(addr1);

            auto addr2 = addresses.back();
            addresses.pop_back();
            alloc.free_address(addr2);

            // Allocate one new address
            auto new_allocation = alloc.get_new_address(region);
            addresses.push_back(new_allocation.addr);

            SAL_WARN("After pattern iteration. Count: {}", alloc.count());
         }
         else
         {
            // Not enough addresses for the pattern, just free the remaining ones
            while (!addresses.empty())
            {
               auto addr = addresses.back();
               addresses.pop_back();
               alloc.free_address(addr);
            }
         }
      }

      // Verify all addresses have been freed
      REQUIRE(alloc.count() == 0);
      REQUIRE(addresses.empty());
      exit(1);
   }

   SECTION("Alloc then Free")
   {
      address_alloc alloc(test_dir);
      auto          region = alloc.get_new_region();

      // Store allocated addresses
      std::vector<address> addresses;
      int                  test_count = 1024;
      addresses.reserve(test_count);

      for (int i = 0; i < test_count; ++i)
      {
         auto a1 = alloc.get_new_address(region);
         auto a2 = alloc.get_new_address(region);
         SAL_WARN("a1: {} a2: {} count: {}", a1.addr, a2.addr, alloc.count());
         alloc.free_address(a2.addr);
         auto a3 = alloc.get_new_address(region);
         SAL_WARN("a3: {} count: {}", a3.addr, alloc.count());
      }
      REQUIRE(alloc.count() == 2 * test_count);
   }

   SECTION("Multiple regions")
   {
      address_alloc alloc(test_dir);

      // Get multiple regions
      auto region1 = alloc.get_new_region();
      auto region2 = alloc.get_new_region();
      REQUIRE(region1 == 0);
      REQUIRE(region2 == 1);

      // Allocate addresses in both regions
      auto alloc1 = alloc.get_new_address(region1);
      auto alloc2 = alloc.get_new_address(region2);
      REQUIRE(alloc1.addr.region == region1);
      REQUIRE(alloc2.addr.region == region2);
      REQUIRE(alloc.count() == 2);

      // Free addresses
      alloc.free_address(alloc1.addr);
      REQUIRE(alloc.count() == 1);
      alloc.free_address(alloc2.addr);
      REQUIRE(alloc.count() == 0);
   }
   SECTION("Multi-Region Single Thread allocation test")
   {
      address_alloc alloc(test_dir);
      auto          region = alloc.get_new_region();

      // Store allocated addresses
      for (int i = 0; i < 3; ++i)
      {
         std::vector<address> addresses;
         int                  test_count = 1 << 18;  // 512 slots per page
         addresses.reserve(test_count);

         for (int i = 0; i < test_count; ++i)
         {
            auto region = alloc.get_new_region();
            region &= 0xff;
            auto allocation = alloc.get_new_address(region);
            addresses.push_back(allocation.addr);
            //         REQUIRE(allocation.addr.index == i);
         }

         // Free all addresses in reverse order
         while (!addresses.empty())
         {
            auto addr = addresses.back();
            //auto expected_count = addresses.size() - 1;
            alloc.free_address(addr);
            addresses.pop_back();
         }
         REQUIRE(0 == alloc.count());
         SAL_WARN(" ROUND TO BEGINS");
      }
   }

   SECTION("Single thread allocation test")
   {
      address_alloc alloc(test_dir);
      auto          region = alloc.get_new_region();

      // Store allocated addresses
      std::vector<address> addresses;
      int                  test_count = 1600;  // 512 slots per page
      addresses.reserve(test_count);

      for (int i = 0; i < test_count; ++i)
      {
         auto allocation = alloc.get_new_address(region);
         addresses.push_back(allocation.addr);
         REQUIRE(allocation.addr.index == i);
      }

      // Free all addresses in reverse order
      while (!addresses.empty())
      {
         auto addr           = addresses.back();
         auto expected_count = addresses.size() - 1;
         alloc.free_address(addr);
         addresses.pop_back();
         auto actual_count = alloc.count();
         REQUIRE(actual_count == expected_count);
      }
   }

   SECTION("Thread safety")
   {
      address_alloc alloc(test_dir);
      auto          region = alloc.get_new_region();

      constexpr size_t                  num_threads       = 4;
      constexpr size_t                  allocs_per_thread = 400;
      std::vector<std::thread>          threads;
      std::vector<std::vector<address>> addresses(num_threads);

      // Create threads that allocate addresses
      for (size_t i = 0; i < num_threads; ++i)
      {
         threads.emplace_back(
             [&alloc, region, i, &addresses]()
             {
                // Set descriptive thread name (max 15 chars)
                sal::set_current_thread_name(("alloc-" + std::to_string(i)).c_str());
                for (size_t j = 0; j < allocs_per_thread; ++j)
                {
                   auto allocation = alloc.get_new_address(region);
                   addresses[i].push_back(allocation.addr);
                }
             });
      }

      // Wait for all threads to finish allocating
      for (auto& thread : threads)
         thread.join();

      // Verify count
      REQUIRE(alloc.count() == num_threads * allocs_per_thread);

      // Free all addresses
      threads.clear();
      for (size_t i = 0; i < num_threads; ++i)
      {
         threads.emplace_back(
             [&alloc, i, &addresses]()
             {
                // Set descriptive thread name (max 15 chars)
                sal::set_current_thread_name(("free-" + std::to_string(i)).c_str());
                for (const auto& addr : addresses[i])
                   alloc.free_address(addr);
             });
      }

      // Wait for all threads to finish freeing
      for (auto& thread : threads)
         thread.join();

      // Verify all addresses are freed
      REQUIRE(alloc.count() == 0);
   }

   // Cleanup
   std::filesystem::remove_all(test_dir);
}