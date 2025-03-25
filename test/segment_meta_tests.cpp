#include <arbtrie/mapped_memory.hpp>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <vector>

using namespace arbtrie::mapped_memory;

TEST_CASE("segment_meta basic operations", "[segment_meta]")
{
   // Create a segment_meta instance for testing
   segment_meta meta;

   SECTION("Initial state")
   {
      // Initially all values should be 0/false
      REQUIRE(meta.get_freed_space() == 0);
      REQUIRE(meta.get_vage() == 0);
      REQUIRE_FALSE(meta.is_read_only());
      REQUIRE_FALSE(meta.is_pinned());
   }

   SECTION("set_pinned operations")
   {
      // Set the pinned bit
      meta.set_pinned(true);
      REQUIRE(meta.is_pinned());

      // Clear the pinned bit
      meta.set_pinned(false);
      REQUIRE_FALSE(meta.is_pinned());
   }

   SECTION("add_freed_space operations")
   {
      // Add space in increments
      meta.add_freed_space(1024);
      REQUIRE(meta.get_freed_space() == 1024);

      meta.add_freed_space(2048);
      REQUIRE(meta.get_freed_space() == 3072);
   }

   SECTION("prepare_for_reuse operation")
   {
      // Set up some state first
      meta.add_freed_space(1024);
      meta.set_pinned(true);
      meta.prepare_for_compaction(12345);

      // Verify state before resetting
      REQUIRE(meta.get_freed_space() == 1024);
      REQUIRE(meta.is_read_only());
      REQUIRE(meta.is_pinned());
      REQUIRE(meta.get_vage() == 12345);

      // Reset with prepare_for_reuse
      meta.prepare_for_reuse();

      // Verify freed_space is reset and read_only flag is cleared
      REQUIRE(meta.get_freed_space() == 0);
      REQUIRE_FALSE(meta.is_read_only());

      // The pinned flag should remain unchanged
      REQUIRE(meta.is_pinned());

      // vage is not reset by prepare_for_reuse
      REQUIRE(meta.get_vage() == 12345);
   }

   SECTION("prepare_for_compaction operation")
   {
      // Set up some initial state
      meta.add_freed_space(1024);
      REQUIRE_FALSE(meta.is_read_only());

      // Prepare for compaction with a specific vage
      uint64_t test_vage = 98765;
      meta.prepare_for_compaction(test_vage);

      // Verify vage is set and read_only flag is set
      REQUIRE(meta.get_vage() == test_vage);
      REQUIRE(meta.is_read_only());

      // Freed space should remain unchanged
      REQUIRE(meta.get_freed_space() == 1024);
   }
}

TEST_CASE("segment_meta flags operations", "[segment_meta][flags]")
{
   segment_meta meta;

   SECTION("read_only flag operations")
   {
      REQUIRE_FALSE(meta.is_read_only());

      // Set read_only via prepare_for_compaction
      meta.prepare_for_compaction(1000);
      REQUIRE(meta.is_read_only());

      // Clear read_only via prepare_for_reuse
      meta.prepare_for_reuse();
      REQUIRE_FALSE(meta.is_read_only());
   }

   SECTION("pinned flag operations")
   {
      REQUIRE_FALSE(meta.is_pinned());

      // Set pinned flag
      meta.set_pinned(true);
      REQUIRE(meta.is_pinned());

      // Clear pinned flag
      meta.set_pinned(false);
      REQUIRE_FALSE(meta.is_pinned());

      // Set pinned flag again
      meta.set_pinned(true);
      REQUIRE(meta.is_pinned());
   }

   SECTION("flag interaction - prepare_for_reuse preserves pinned")
   {
      // Set up both flags
      meta.set_pinned(true);
      meta.prepare_for_compaction(1000);

      REQUIRE(meta.is_pinned());
      REQUIRE(meta.is_read_only());

      // prepare_for_reuse should clear read_only but preserve pinned
      meta.prepare_for_reuse();

      REQUIRE(meta.is_pinned());
      REQUIRE_FALSE(meta.is_read_only());
   }

   SECTION("flag interaction - prepare_for_compaction preserves pinned")
   {
      // Set pinned flag
      meta.set_pinned(true);
      REQUIRE(meta.is_pinned());
      REQUIRE_FALSE(meta.is_read_only());

      // prepare_for_compaction should set read_only but preserve pinned
      meta.prepare_for_compaction(2000);

      REQUIRE(meta.is_pinned());
      REQUIRE(meta.is_read_only());
   }
}

TEST_CASE("segment_meta concurrent operations", "[segment_meta][concurrent]")
{
   segment_meta meta;

   SECTION("Concurrent add_freed_space")
   {
      std::vector<std::thread> threads;
      constexpr int            num_threads = 10;
      constexpr int            iterations  = 1000;
      constexpr int            increment   = 128;

      for (int i = 0; i < num_threads; i++)
      {
         threads.emplace_back(
             [&meta, iterations, increment]()
             {
                for (int j = 0; j < iterations; j++)
                {
                   meta.add_freed_space(increment);
                }
             });
      }

      for (auto& thread : threads)
      {
         thread.join();
      }

      REQUIRE(meta.get_freed_space() == num_threads * iterations * increment);
   }

   SECTION("Concurrent flag operations")
   {
      std::atomic<bool> keep_running{true};
      std::atomic<int>  iterations{0};

      // Thread that sets and unsets the pinned flag
      std::thread pinning_thread(
          [&]()
          {
             while (keep_running.load(std::memory_order_relaxed))
             {
                meta.set_pinned(true);
                meta.set_pinned(false);
                iterations++;
             }
          });

      // Thread that sets and unsets the read_only flag
      std::thread readonly_thread(
          [&]()
          {
             uint64_t vage = 1000;
             while (keep_running.load(std::memory_order_relaxed))
             {
                meta.prepare_for_compaction(vage++);
                meta.prepare_for_reuse();
             }
          });

      // Let threads run for a short time
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

      // Stop the threads
      keep_running.store(false, std::memory_order_relaxed);
      pinning_thread.join();
      readonly_thread.join();

      // Report iterations - this is just informational
      INFO("Completed " << iterations << " iterations of flag operations");

      // No need for specific assertions here, as we're mainly testing that
      // concurrent operations don't crash or cause data races
   }

   SECTION("Pinned bit integrity during concurrent operations")
   {
      std::atomic<bool> keep_running{true};
      std::atomic<bool> error_detected{false};

      // Set pinned bit initially
      meta.set_pinned(true);

      // Thread that constantly verifies the pinned state
      std::thread checker_thread(
          [&]()
          {
             while (keep_running.load(std::memory_order_relaxed))
             {
                if (!meta.is_pinned())
                {
                   error_detected.store(true, std::memory_order_relaxed);
                   break;
                }
             }
          });

      // Thread that performs other operations but shouldn't affect pinned state
      std::thread worker_thread(
          [&]()
          {
             for (int i = 0; i < 10000 && !error_detected; i++)
             {
                meta.add_freed_space(8);
                meta.prepare_for_compaction(i);
                meta.prepare_for_reuse();
             }
          });

      worker_thread.join();
      keep_running.store(false, std::memory_order_relaxed);
      checker_thread.join();

      REQUIRE_FALSE(error_detected);
      REQUIRE(meta.is_pinned());
   }
}

TEST_CASE("segment_meta size and alignment", "[segment_meta][size]")
{
   // Check that segment_meta has reasonable size for cache line alignment
   INFO("segment_meta size: " << sizeof(segment_meta) << " bytes");

   // This should pass on most architectures with 64-byte cache lines
   REQUIRE(sizeof(segment_meta) <= 128);

   // Check alignment of internal atomic members
   // We can't directly test this, but we can document expected behavior
   INFO("segment_meta should have properly aligned atomic members");
}