#include <arbtrie/mapped_memory.hpp>
#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <vector>

using namespace arbtrie::mapped_memory;

TEST_CASE("segment_meta preserves is_pinned bit", "[segment_meta][pinned]")
{
   // Create a segment_meta instance for testing
   segment_meta meta;

   SECTION("Basic set_pinned operations")
   {
      // Initially pinned bit is not set
      REQUIRE_FALSE(meta.data().is_pinned);

      // Set the pinned bit
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);
   }

   SECTION("free_object should preserve is_pinned bit")
   {
      // Set up initial state with pinned bit set
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);

      // Call free_object and verify pinned bit is still set
      meta.free_object(1024);
      REQUIRE(meta.data().is_pinned);

      // Call free_object multiple times
      for (int i = 0; i < 10; i++)
      {
         meta.free_object(1024);
         REQUIRE(meta.data().is_pinned);
      }
   }

   SECTION("free should preserve is_pinned bit")
   {
      // Set up initial state with pinned bit set
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);

      // Call free and verify pinned bit is still set
      meta.free(1024);
      REQUIRE(meta.data().is_pinned);

      // Call free multiple times
      for (int i = 0; i < 10; i++)
      {
         meta.free(1024);
         REQUIRE(meta.data().is_pinned);
      }
   }

   SECTION("finalize_segment should preserve is_pinned bit")
   {
      // Set up initial state with both alloc and pinned bits set
      meta.set_pinned(true);
      meta.set_alloc_state(true);

      // Verify alloc and pinned bits are set
      REQUIRE(meta.data().is_alloc);
      REQUIRE(meta.data().is_pinned);

      // Call finalize_segment and verify pinned bit is still set
      // but alloc bit should be cleared
      meta.finalize_segment(1024, 100);
      REQUIRE_FALSE(meta.data().is_alloc);
      REQUIRE(meta.data().is_pinned);
   }

   SECTION("set_alloc_state should preserve is_pinned bit")
   {
      // Set up initial state with pinned bit set
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);

      // Call set_alloc_state and verify pinned bit is still set
      meta.set_alloc_state(true);
      REQUIRE(meta.data().is_pinned);
      REQUIRE(meta.data().is_alloc);

      // Toggle alloc state
      meta.set_alloc_state(false);
      REQUIRE(meta.data().is_pinned);
      REQUIRE_FALSE(meta.data().is_alloc);
   }

   SECTION("start_alloc_segment should preserve is_pinned bit")
   {
      // Set up initial state with pinned bit set
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);

      // Call start_alloc_segment and verify pinned bit is still set
      meta.start_alloc_segment();
      REQUIRE(meta.data().is_pinned);
      REQUIRE(meta.data().is_alloc);
   }

   SECTION("set_last_sync_pos should preserve is_pinned bit")
   {
      // Set up initial state with pinned bit set
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);

      // Call set_last_sync_pos and verify pinned bit is still set
      meta.set_last_sync_pos(4096);
      REQUIRE(meta.data().is_pinned);
   }

   SECTION("clear method should reset is_pinned bit")
   {
      // Set up initial state with pinned bit set
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);

      // Clear should reset all bits
      meta.clear();
      REQUIRE_FALSE(meta.data().is_pinned);
   }

   SECTION("Combined operations should preserve is_pinned bit")
   {
      // Set up initial state with pinned bit set
      meta.set_pinned(true);

      // Verify pinned bit is set
      REQUIRE(meta.data().is_pinned);

      // Perform a series of operations
      meta.start_alloc_segment();
      REQUIRE(meta.data().is_pinned);

      meta.free(1024);
      REQUIRE(meta.data().is_pinned);

      meta.set_last_sync_pos(4096);
      REQUIRE(meta.data().is_pinned);

      meta.free_object(512);
      REQUIRE(meta.data().is_pinned);

      meta.finalize_segment(2048, 200);
      REQUIRE(meta.data().is_pinned);
   }
}

// Bug: finalize_segment is not preserving the is_pinned bit
TEST_CASE("Fix for finalize_segment bug", "[segment_meta][bug][pinned]")
{
   segment_meta meta;

   // Set up initial state with both alloc and pinned bits set
   meta.set_pinned(true);
   meta.set_alloc_state(true);

   // Verify correct initial state
   REQUIRE(meta.data().is_alloc);
   REQUIRE(meta.data().is_pinned);

   // Call finalize_segment
   meta.finalize_segment(1024, 100);

   // Current behavior: finalize_segment may clear the pinned bit
   // Expected behavior: finalize_segment should preserve the pinned bit
   INFO("finalize_segment should preserve the is_pinned bit");
   CHECK(meta.data().is_pinned);

   // Recommendation: The finalize_segment method should be fixed to preserve the is_pinned bit
   // by changing:
   // auto updated = state_data(expected).free(size).set_alloc(false).to_int();
   // to:
   // auto updated = state_data(expected).free(size).set_alloc(false).set_pinned(state_data(expected).is_pinned).to_int();
}

// Bug: free_object/free operations might not handle concurrent pinned bit updates correctly
TEST_CASE("Concurrent pinned bit operations", "[segment_meta][concurrent][pinned]")
{
   segment_meta      meta;
   std::atomic<bool> keep_running{true};
   std::atomic<int>  iterations{0};

   // Set pinned bit initially
   meta.set_pinned(true);

   // Thread that constantly checks if pinned bit is set
   std::thread checker(
       [&]()
       {
          while (keep_running.load(std::memory_order_relaxed))
          {
             if (!meta.data().is_pinned)
             {
                // If we ever find the pinned bit cleared, report it
                FAIL("Pinned bit was unexpectedly cleared");
                keep_running.store(false, std::memory_order_relaxed);
                return;
             }
             iterations++;
          }
       });

   // Perform operations that might affect the pinned bit
   for (int i = 0; i < 1000; i++)
   {
      meta.free(8);
      meta.free_object(8);
      meta.set_alloc_state(true);
      meta.set_alloc_state(false);
      meta.set_last_sync_pos(i * 10);
   }

   // Stop and join the checker thread
   keep_running.store(false, std::memory_order_relaxed);
   checker.join();

   // Report how many iterations the checker performed
   INFO("Checker performed " << iterations << " iterations");

   // Verify pinned bit is still set
   REQUIRE(meta.data().is_pinned);
}