#include <atomic>
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include <sal/address_alloc.hpp>
#include <sal/debug.hpp>

using namespace sal;
using namespace std::chrono;

// Global configuration for command-line arguments
struct GlobalConfig
{
   int num_threads = 1;  // Default to single-threaded
} g_config;

struct ThreadStats
{
   uint64_t allocations = 0;
   uint64_t frees       = 0;
};

struct TestConfig
{
   // Test duration in seconds
   int duration = 10;

   // Number of regions to use
   int num_regions = 16;

   // Allocation to free ratio (0.7 means 70% allocations, 30% frees)
   double alloc_ratio = 0.7;

   // Maximum addresses per thread to hold before forcing frees
   int max_addresses_per_thread = 10000;

   // Print progress update every X milliseconds
   int progress_interval_ms = 1000;

   // Microseconds to sleep between operations (to reduce contention)
   int operation_delay_us = 10;

   // Maximum number of retries before reporting a failure
   int max_retries = 0;  // No retries - abort on first failure
};

// Add a thread-safe counter for operation failures
struct OperationStats
{
   std::atomic<uint64_t> allocation_attempts{0};
   std::atomic<uint64_t> allocation_failures{0};
   std::atomic<uint64_t> free_attempts{0};
   std::atomic<uint64_t> free_failures{0};
   std::atomic<uint64_t> exceptions{0};
};

// Custom main function to handle command-line arguments
int main(int argc, char* argv[])
{
   // Manual parsing of command line arguments
   for (int i = 1; i < argc; ++i)
   {
      std::string arg = argv[i];
      if (arg == "-t" || arg == "--threads")
      {
         if (i + 1 < argc)
         {
            g_config.num_threads = std::stoi(argv[++i]);
         }
      }
   }

   std::cout << "Running test with " << g_config.num_threads << " thread(s)" << std::endl;

   // Create a Catch session and run tests
   return Catch::Session().run(argc, argv);
}

TEST_CASE("address_alloc performance stress test", "[address_alloc][performance]")
{
   // Create a temporary directory for the test
   std::filesystem::path test_dir =
       std::filesystem::temp_directory_path() / "address_alloc_perf_test";

   // Clean up any existing test directory
   if (std::filesystem::exists(test_dir))
      std::filesystem::remove_all(test_dir);
   std::filesystem::create_directories(test_dir);

   // Configuration
   TestConfig config;

   // Shared allocator
   address_alloc alloc(test_dir);

   // Validate initial state
   std::string initial_invariant_errors = alloc.validate_invariant();
   if (!initial_invariant_errors.empty())
   {
      SAL_ERROR("Initial state invariant violations: \n{}", initial_invariant_errors);
      std::cerr
          << "CRITICAL ERROR: Bitmap hierarchy invariants violated in initial state! Aborting test."
          << std::endl;
      abort();
   }
   std::cout << "Initial state validation: All bitmap invariants satisfied." << std::endl;

   // Create regions
   std::vector<region_id> regions;
   for (int i = 0; i < config.num_regions; ++i)
   {
      regions.push_back(alloc.get_new_region());
   }

   // Atomic counters for tracking operations
   std::atomic<uint64_t> total_allocations(0);
   std::atomic<uint64_t> total_frees(0);
   std::atomic<bool>     should_stop(false);

   // Add operation stats
   OperationStats op_stats;

   // Function for worker threads
   auto worker_thread = [&](int thread_id)
   {
      // Thread-local random number generator
      std::random_device               rd;
      std::mt19937                     gen(rd());
      std::uniform_int_distribution<>  region_dist(0, regions.size() - 1);
      std::uniform_real_distribution<> op_dist(0.0, 1.0);

      // Thread-local storage for allocated addresses
      std::vector<address> allocated_addresses;
      allocated_addresses.reserve(config.max_addresses_per_thread);

      // Thread statistics
      ThreadStats stats;

      try
      {
         while (!should_stop.load(std::memory_order_relaxed))
         {
            // Decide whether to allocate or free
            bool should_allocate = true;

            if (!allocated_addresses.empty())
            {
               if (allocated_addresses.size() >= config.max_addresses_per_thread)
               {
                  // Force free if we've hit the maximum
                  should_allocate = false;
               }
               else
               {
                  // Randomly decide based on ratio
                  should_allocate = op_dist(gen) < config.alloc_ratio;
               }
            }

            if (should_allocate)
            {
               // Select a random region
               region_id region = regions[region_dist(gen)];

               // Track the allocation attempt
               op_stats.allocation_attempts.fetch_add(1, std::memory_order_relaxed);

               // Try to allocate without retries
               try
               {
                  // Allocate a new address
                  auto allocation = alloc.get_new_address(region);

                  // Validate invariants immediately after allocation
                  std::string invariant_errors = alloc.validate_invariant();
                  if (!invariant_errors.empty())
                  {
                     SAL_ERROR(
                         "Invariant violations after allocation for region {}, thread {}: \n{}",
                         region, thread_id, invariant_errors);
                     std::cerr << "CRITICAL ERROR: Bitmap hierarchy invariants violated after "
                                  "allocation! Aborting test."
                               << std::endl;
                     abort();
                  }

                  allocated_addresses.push_back(allocation.addr);

                  // Update statistics
                  stats.allocations++;
                  total_allocations.fetch_add(1, std::memory_order_relaxed);
               }
               catch (const std::exception& e)
               {
                  SAL_ERROR("Thread {} allocation failed: {}", thread_id, e.what());
                  op_stats.allocation_failures.fetch_add(1, std::memory_order_relaxed);
                  std::cerr << "CRITICAL ERROR: Allocation should not fail! Aborting test."
                            << std::endl;
                  abort();  // Abort on failure as requested
               }
            }
            else if (!allocated_addresses.empty())
            {
               // Select a random address to free
               std::uniform_int_distribution<size_t> addr_dist(0, allocated_addresses.size() - 1);
               size_t                                idx          = addr_dist(gen);
               address                               addr_to_free = allocated_addresses[idx];

               // Track the free attempt
               op_stats.free_attempts.fetch_add(1, std::memory_order_relaxed);

               // Try to free without retries
               try
               {
                  // Free the address
                  address addr_copy = addr_to_free;  // Keep a copy for error reporting
                  alloc.free_address(addr_to_free);

                  // Validate invariants immediately after free
                  std::string invariant_errors = alloc.validate_invariant();
                  if (!invariant_errors.empty())
                  {
                     SAL_ERROR("Invariant violations after freeing address {}, thread {}: \n{}",
                               addr_copy, thread_id, invariant_errors);
                     std::cerr << "CRITICAL ERROR: Bitmap hierarchy invariants violated after "
                                  "free! Aborting test."
                               << std::endl;
                     abort();
                  }

                  // Remove from our tracking (swap with last element and pop)
                  allocated_addresses[idx] = allocated_addresses.back();
                  allocated_addresses.pop_back();

                  // Update statistics
                  stats.frees++;
                  total_frees.fetch_add(1, std::memory_order_relaxed);
               }
               catch (const std::exception& e)
               {
                  SAL_ERROR("Thread {} free failed: {}", thread_id, e.what());
                  op_stats.free_failures.fetch_add(1, std::memory_order_relaxed);
                  std::cerr << "CRITICAL ERROR: Free operation should not fail! Aborting test."
                            << std::endl;
                  abort();  // Abort on failure as requested
               }
            }

            // Add a small delay between operations to reduce contention
            if (config.operation_delay_us > 0)
            {
               std::this_thread::sleep_for(std::chrono::microseconds(config.operation_delay_us));
            }
         }
      }
      catch (const std::exception& e)
      {
         SAL_ERROR("Thread {} encountered exception: {}", thread_id, e.what());
         op_stats.exceptions.fetch_add(1, std::memory_order_relaxed);
         std::cerr << "CRITICAL ERROR: Unexpected exception! Aborting test." << std::endl;
         abort();  // Abort on any exception as well
      }

      // Free all remaining addresses
      for (const auto& addr : allocated_addresses)
      {
         try
         {
            alloc.free_address(addr);
            stats.frees++;
            total_frees.fetch_add(1, std::memory_order_relaxed);
         }
         catch (const std::exception& e)
         {
            SAL_ERROR("Thread {} failed to free address {}: {}", thread_id, addr, e.what());
            std::cerr << "CRITICAL ERROR: Final cleanup free should not fail! Aborting test."
                      << std::endl;
            abort();  // Abort on failure during cleanup
         }
      }

      SAL_WARN("Thread {} completed: {} allocations, {} frees", thread_id, stats.allocations,
               stats.frees);
   };

   // Start timing
   auto       start_time = high_resolution_clock::now();
   const auto end_time   = start_time + seconds(config.duration);

   // Spawn worker threads - use the specified thread count
   std::vector<std::thread> threads;
   for (int i = 0; i < g_config.num_threads; ++i)
   {
      threads.emplace_back(worker_thread, i);
   }

   // Progress reporting thread
   std::thread reporter(
       [&]()
       {
          uint64_t last_allocs         = 0;
          uint64_t last_frees          = 0;
          uint64_t last_alloc_attempts = 0;
          uint64_t last_alloc_failures = 0;
          uint64_t last_free_attempts  = 0;
          uint64_t last_free_failures  = 0;
          auto     last_time           = high_resolution_clock::now();

          while (!should_stop.load(std::memory_order_relaxed))
          {
             std::this_thread::sleep_for(milliseconds(config.progress_interval_ms));

             auto now     = high_resolution_clock::now();
             auto elapsed = duration_cast<milliseconds>(now - last_time).count() / 1000.0;

             uint64_t current_allocs = total_allocations.load(std::memory_order_relaxed);
             uint64_t current_frees  = total_frees.load(std::memory_order_relaxed);

             uint64_t current_alloc_attempts =
                 op_stats.allocation_attempts.load(std::memory_order_relaxed);
             uint64_t current_alloc_failures =
                 op_stats.allocation_failures.load(std::memory_order_relaxed);
             uint64_t current_free_attempts =
                 op_stats.free_attempts.load(std::memory_order_relaxed);
             uint64_t current_free_failures =
                 op_stats.free_failures.load(std::memory_order_relaxed);
             uint64_t current_exceptions = op_stats.exceptions.load(std::memory_order_relaxed);

             uint64_t alloc_diff          = current_allocs - last_allocs;
             uint64_t free_diff           = current_frees - last_frees;
             uint64_t alloc_attempts_diff = current_alloc_attempts - last_alloc_attempts;
             uint64_t alloc_failures_diff = current_alloc_failures - last_alloc_failures;
             uint64_t free_attempts_diff  = current_free_attempts - last_free_attempts;
             uint64_t free_failures_diff  = current_free_failures - last_free_failures;

             double allocs_per_sec = alloc_diff / elapsed;
             double frees_per_sec  = free_diff / elapsed;
             double alloc_failure_rate =
                 alloc_attempts_diff > 0 ? (double)alloc_failures_diff / alloc_attempts_diff * 100.0
                                         : 0.0;
             double free_failure_rate =
                 free_attempts_diff > 0 ? (double)free_failures_diff / free_attempts_diff * 100.0
                                        : 0.0;

             // Print progress and check invariants periodically
             std::cout << "[" << g_config.num_threads << " thread(s)] Progress: " << current_allocs
                       << " allocs (" << allocs_per_sec << "/sec), " << current_frees << " frees ("
                       << frees_per_sec << "/sec), "
                       << "Failure rates: alloc=" << alloc_failure_rate
                       << "%, free=" << free_failure_rate << "%, "
                       << "Exceptions: " << current_exceptions << ", "
                       << "Active addresses: " << (current_allocs - current_frees) << ", "
                       << "count(): " << alloc.count() << std::endl;

             // Periodically validate invariants
             std::string periodic_invariant_errors = alloc.validate_invariant();
             if (!periodic_invariant_errors.empty())
             {
                SAL_ERROR("Periodic invariant check found violations: \n{}",
                          periodic_invariant_errors);
                std::cerr << "CRITICAL ERROR: Bitmap hierarchy invariants violated during periodic "
                             "check! Aborting test."
                          << std::endl;
                abort();
             }

             last_allocs         = current_allocs;
             last_frees          = current_frees;
             last_alloc_attempts = current_alloc_attempts;
             last_alloc_failures = current_alloc_failures;
             last_free_attempts  = current_free_attempts;
             last_free_failures  = current_free_failures;
             last_time           = now;
          }
       });

   // Run for the specified duration
   std::this_thread::sleep_for(seconds(config.duration));

   // Signal threads to stop
   should_stop.store(true, std::memory_order_relaxed);

   // Wait for all threads to finish
   for (auto& thread : threads)
   {
      thread.join();
   }

   reporter.join();

   // Calculate final stats
   auto     end            = high_resolution_clock::now();
   double   total_duration = duration_cast<milliseconds>(end - start_time).count() / 1000.0;
   uint64_t final_allocs   = total_allocations.load(std::memory_order_relaxed);
   uint64_t final_frees    = total_frees.load(std::memory_order_relaxed);

   uint64_t final_alloc_attempts = op_stats.allocation_attempts.load(std::memory_order_relaxed);
   uint64_t final_alloc_failures = op_stats.allocation_failures.load(std::memory_order_relaxed);
   uint64_t final_free_attempts  = op_stats.free_attempts.load(std::memory_order_relaxed);
   uint64_t final_free_failures  = op_stats.free_failures.load(std::memory_order_relaxed);
   uint64_t final_exceptions     = op_stats.exceptions.load(std::memory_order_relaxed);

   double allocs_per_sec    = final_allocs / total_duration;
   double frees_per_sec     = final_frees / total_duration;
   double total_ops_per_sec = (final_allocs + final_frees) / total_duration;

   double alloc_success_rate =
       final_alloc_attempts > 0 ? (double)final_allocs / final_alloc_attempts * 100.0 : 0.0;
   double free_success_rate =
       final_free_attempts > 0 ? (double)final_frees / final_free_attempts * 100.0 : 0.0;

   std::cout << "============ Performance Results (" << g_config.num_threads
             << " thread(s)) ============" << std::endl;
   std::cout << "Total duration: " << total_duration << " seconds" << std::endl;
   std::cout << "Total allocations: " << final_allocs << " (success rate: " << alloc_success_rate
             << "%)" << std::endl;
   std::cout << "Total allocation attempts: " << final_alloc_attempts
             << " (failures: " << final_alloc_failures << ")" << std::endl;
   std::cout << "Total frees: " << final_frees << " (success rate: " << free_success_rate << "%)"
             << std::endl;
   std::cout << "Total free attempts: " << final_free_attempts
             << " (failures: " << final_free_failures << ")" << std::endl;
   std::cout << "Total exceptions: " << final_exceptions << std::endl;
   std::cout << "Allocations/second: " << allocs_per_sec << std::endl;
   std::cout << "Frees/second: " << frees_per_sec << std::endl;
   std::cout << "Total operations/second: " << total_ops_per_sec << std::endl;
   std::cout << "Remaining addresses (should be 0): " << alloc.count() << std::endl;
   std::cout << "=============================================" << std::endl;

   // Validate final state
   std::string final_invariant_errors = alloc.validate_invariant();
   if (!final_invariant_errors.empty())
   {
      SAL_ERROR("Final state invariant violations: \n{}", final_invariant_errors);
      std::cerr
          << "CRITICAL ERROR: Bitmap hierarchy invariants violated in final state! Aborting test."
          << std::endl;
      abort();
   }
   std::cout << "Final state validation: All bitmap invariants satisfied." << std::endl;

   // Verify that all addresses have been freed
   REQUIRE(alloc.count() == 0);

   // Clean up
   std::filesystem::remove_all(test_dir);
}