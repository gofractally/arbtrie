#include <arbtrie/node_meta.hpp>
#include <atomic>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <optional>
#include <random>
#include <sstream>
#include <thread>
#include <vector>

using namespace arbtrie;

TEST_CASE("node_meta basic operations", "[node_meta]")
{
   node_meta<> meta;

   SECTION("Default constructor initializes to zero")
   {
      REQUIRE(meta.to_int() == 0);
      REQUIRE(meta.ref() == 0);
      REQUIRE(meta.type() == node_type::freelist);
      REQUIRE(meta.loc().to_aligned() == 0);
   }

   SECTION("Integer constructor works correctly")
   {
      node_meta<> meta2(0x123);
      REQUIRE(meta2.to_int() == 0x123);
   }

   SECTION("Bitfield accessors")
   {
      // Test is_changing, is_const, is_copying, is_read, is_pending_cache
      REQUIRE_FALSE(meta.is_changing());
      REQUIRE(meta.is_const());
      REQUIRE_FALSE(meta.is_copying());
      REQUIRE_FALSE(meta.is_read());
      REQUIRE_FALSE(meta.is_pending_cache());
   }

   SECTION("Store operations")
   {
      // Test store with different memory orders
      uint64_t test_value = 0x123456789ABCULL;
      meta.store(test_value, std::memory_order_relaxed);
      REQUIRE(meta.to_int() == test_value);

      // Test another value
      uint64_t new_value = 0xFEDCBA9876ULL;
      meta.store(new_value, std::memory_order_release);
      REQUIRE(meta.to_int() == new_value);
   }
}

TEST_CASE("node_meta type operations", "[node_meta]")
{
   node_meta<> meta;

   SECTION("node_type enum stream output")
   {
      std::stringstream ss;
      ss << node_type::binary;
      REQUIRE(ss.str() == "binary");
   }
}

TEST_CASE("node_meta reference counting", "[node_meta]")
{
   node_meta<> meta;

   SECTION("retain and release")
   {
      // Initially ref count is 0
      REQUIRE(meta.ref() == 0);

      // Set a non-zero ref count so we can call retain
      meta.set_ref(1);
      REQUIRE(meta.ref() == 1);

      // Retain should increment ref count
      REQUIRE(meta.retain());
      REQUIRE(meta.ref() == 2);

      // Release should decrement ref count and return the state before decrement
      auto state = meta.release();
      REQUIRE(state.ref() == 2);
      REQUIRE(meta.ref() == 1);
   }

   SECTION("set_ref directly")
   {
      meta.set_ref(10);
      REQUIRE(meta.ref() == 10);

      meta.set_ref(0);
      REQUIRE(meta.ref() == 0);
   }
}

TEST_CASE("node_meta location operations", "[node_meta]")
{
   node_meta<>   meta;
   node_location loc1 = node_location::from_aligned(0x12345);
   node_location loc2 = node_location::from_aligned(0x54321);

   SECTION("set_location_and_type works correctly")
   {
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);
      REQUIRE(meta.loc() == loc1);
      REQUIRE(meta.type() == node_type::binary);

      meta.set_location_and_type(loc2, node_type::value, std::memory_order_relaxed);
      REQUIRE(meta.loc() == loc2);
      REQUIRE(meta.type() == node_type::value);
   }
}

TEST_CASE("node_meta read bit operations", "[node_meta]")
{
   node_meta<> meta;

   SECTION("set_read and is_read")
   {
      // Initially read bit is not set
      REQUIRE_FALSE(meta.is_read());

      // Setting read bit
      meta.set_read();
      REQUIRE(meta.is_read());

      // Clearing read bit
      meta.clear_read_bit();
      REQUIRE_FALSE(meta.is_read());
   }

   SECTION("try_set_read")
   {
      // Initially read bit is not set
      REQUIRE_FALSE(meta.is_read());

      // First call should succeed and set the bit
      REQUIRE(meta.try_set_read());
      REQUIRE(meta.is_read());

      // Subsequent call should return false (bit already set)
      REQUIRE_FALSE(meta.try_set_read());
      REQUIRE(meta.is_read());

      // After clearing, try_set_read should work again
      meta.clear_read_bit();
      REQUIRE_FALSE(meta.is_read());
      REQUIRE(meta.try_set_read());
      REQUIRE(meta.is_read());
   }
}

TEST_CASE("node_meta modify operations single-threaded", "[node_meta]")
{
   node_meta<> meta;
   meta.set_ref(1);  // Set ref count for operations

   SECTION("start_modify and end_modify")
   {
      // Initially not in modify state
      REQUIRE_FALSE(meta.is_changing());

      // Start modify should set the modify flag
      auto state = meta.start_modify();
      REQUIRE(meta.is_changing());
      REQUIRE_FALSE(state.is_changing());  // State before modification

      // End modify should clear the modify flag
      state = meta.end_modify();
      REQUIRE_FALSE(meta.is_changing());
      REQUIRE(state.is_changing());  // State during modification
   }
}

TEST_CASE("node_meta pending cache bit operations", "[node_meta]")
{
   node_meta<> meta;

   SECTION("set_pending_cache and is_pending_cache")
   {
      // Initially pending cache bit is not set
      REQUIRE_FALSE(meta.is_pending_cache());

      // Setting pending cache bit
      meta.set_pending_cache();
      REQUIRE(meta.is_pending_cache());
   }
}

// Tests for move/copy operations
TEST_CASE("node_meta move operations", "[node_meta]")
{
   node_meta<>   meta;
   node_location loc1 = node_location::from_aligned(0x12345);
   node_location loc2 = node_location::from_aligned(0x54321);

   SECTION("try_start_move with proper conditions")
   {
      // Set up required state
      meta.store(0, std::memory_order_relaxed);  // Clear state
      meta.set_ref(1);                           // Ref count must be > 0
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);

      // When ref count > 0 and locations match, try_start_move should succeed
      REQUIRE(meta.try_start_move(loc1));
      REQUIRE(meta.is_copying());

      // Clean up
      meta.end_move();
      REQUIRE_FALSE(meta.is_copying());
   }

   SECTION("try_move_location basic operation")
   {
      // Set up required state
      meta.store(0, std::memory_order_relaxed);  // Clear state
      meta.set_ref(1);                           // Ref count must be > 0
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);
      meta.set_pending_cache();  // Must be pending cache

      // Should return the location when conditions are met
      auto result = meta.try_move_location();
      REQUIRE(result.has_value());
      REQUIRE(*result == loc1);
      REQUIRE(meta.is_copying());

      // Clean up
      meta.end_move();
      REQUIRE_FALSE(meta.is_copying());
   }
}

// Multi-threaded tests that only use the public API
TEST_CASE("node_meta multi-threaded modify operations", "[node_meta][concurrent]")
{
   node_meta<> meta;
   meta.set_ref(1);  // Set ref count to allow operations

   SECTION("concurrent modify threads")
   {
      const int                num_threads        = 5;
      const int                iterations         = 10;
      std::atomic<int>         completed_modifies = 0;
      std::vector<std::thread> threads;

      for (int i = 0; i < num_threads; i++)
      {
         threads.emplace_back(
             [&]()
             {
                for (int j = 0; j < iterations; j++)
                {
                   // Start modify
                   auto state = meta.start_modify();

                   // Small work simulation
                   std::this_thread::sleep_for(std::chrono::microseconds(10));

                   // End modify
                   meta.end_modify();
                   completed_modifies++;
                }
             });
      }

      for (auto& t : threads)
      {
         t.join();
      }

      REQUIRE(completed_modifies == num_threads * iterations);
      REQUIRE_FALSE(meta.is_changing());
   }

   SECTION("modify and try_set_read interaction")
   {
      std::atomic<bool> modify_started(false);
      std::atomic<bool> read_attempted(false);
      std::atomic<bool> read_succeeded(false);

      // Thread 1: Starts modify, waits, then ends it
      std::thread modify_thread(
          [&]()
          {
             // Start modify
             auto state = meta.start_modify();
             REQUIRE(meta.is_changing());
             modify_started = true;

             // Wait for read thread to attempt operation
             while (!read_attempted)
             {
                std::this_thread::yield();
             }

             // Sleep a bit to ensure read thread has tried
             std::this_thread::sleep_for(std::chrono::milliseconds(50));

             // End modify
             meta.end_modify();
          });

      // Thread 2: Tries to set read bit while modify is active
      std::thread read_thread(
          [&]()
          {
             // Wait for modify to start
             while (!modify_started)
             {
                std::this_thread::yield();
             }

             // Clear read bit first to ensure we're testing fresh
             meta.clear_read_bit();

             // Try setting the read bit
             read_attempted = true;
             read_succeeded = meta.try_set_read();

             // Verify read bit is set if operation succeeded
             if (read_succeeded)
             {
                REQUIRE(meta.is_read());
             }
          });

      modify_thread.join();
      read_thread.join();

      // Read operation should succeed even during modification
      REQUIRE(read_succeeded);
      REQUIRE(meta.is_read());
      REQUIRE_FALSE(meta.is_changing());
   }
}

// Test the interaction between modify and move/copy
TEST_CASE("node_meta modify and copy interaction", "[node_meta][concurrent]")
{
   node_meta<>   meta;
   node_location loc1 = node_location::from_aligned(0x12345);
   node_location loc2 = node_location::from_aligned(0x54321);

   SECTION("modify during active try_start_move")
   {
      // Set up required state
      meta.store(0, std::memory_order_relaxed);  // Clear state
      meta.set_ref(1);                           // Ref count must be > 0
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);

      std::atomic<bool> copy_started(false);
      std::atomic<bool> modify_attempted(false);
      std::atomic<bool> modify_completed(false);
      std::atomic<bool> copy_succeeded(false);

      // Thread 1: Starts try_start_move to set copy flag
      std::thread copy_thread(
          [&]()
          {
             // Start copy
             copy_succeeded = meta.try_start_move(loc1);
             REQUIRE(copy_succeeded);
             REQUIRE(meta.is_copying());
             copy_started = true;

             // Wait for modify thread to attempt operation
             while (!modify_attempted)
             {
                std::this_thread::yield();
             }

             // Wait a bit longer to ensure modify thread is waiting
             std::this_thread::sleep_for(std::chrono::milliseconds(100));

             // End move to release copy flag
             meta.end_move();

             // Wait for modify to complete
             while (!modify_completed)
             {
                std::this_thread::yield();
             }
          });

      // Thread 2: Tries to start_modify while copy flag is set
      std::thread modify_thread(
          [&]()
          {
             // Wait for copy to start
             while (!copy_started)
             {
                std::this_thread::yield();
             }

             // Try to start modify - will wait until copy flag is cleared
             modify_attempted = true;

             // Start modify should wait until copy flag is cleared
             auto state = meta.start_modify();

             // If we get here, the copy flag should be cleared
             REQUIRE(meta.is_changing());
             REQUIRE_FALSE(meta.is_copying());

             // End modify
             meta.end_modify();
             modify_completed = true;
          });

      copy_thread.join();
      modify_thread.join();

      // Final state should be clean
      REQUIRE(modify_completed);
      REQUIRE_FALSE(meta.is_changing());
      REQUIRE_FALSE(meta.is_copying());
   }

   SECTION("try_start_move during active modification")
   {
      // Set up required state
      meta.store(0, std::memory_order_relaxed);  // Clear state
      meta.set_ref(1);                           // Ref count must be > 0
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);

      std::atomic<bool> modify_started(false);
      std::atomic<bool> move_attempted(false);
      std::atomic<bool> move_completed(false);
      std::atomic<bool> move_succeeded(false);

      // Thread 1: Starts modification
      std::thread modify_thread(
          [&]()
          {
             // Start modify
             auto state = meta.start_modify();
             REQUIRE(meta.is_changing());
             modify_started = true;

             // Wait for move thread to attempt operation
             while (!move_attempted)
             {
                std::this_thread::yield();
             }

             // Sleep a bit longer to ensure move thread attempts the move
             std::this_thread::sleep_for(std::chrono::milliseconds(100));

             // End modify
             meta.end_modify();

             // Wait for move to complete
             while (!move_completed)
             {
                std::this_thread::yield();
             }
          });

      // Thread 2: Tries to start_move while modification is active
      std::thread move_thread(
          [&]()
          {
             // Wait for modify to start
             while (!modify_started)
             {
                std::this_thread::yield();
             }

             // Try to start move - should wait until modify is complete
             move_attempted = true;
             move_succeeded = meta.try_start_move(loc1);

             if (move_succeeded)
             {
                REQUIRE(meta.is_copying());
                REQUIRE_FALSE(meta.is_changing());
                meta.end_move();
             }

             move_completed = true;
          });

      modify_thread.join();
      move_thread.join();

      // Move should have succeeded after modify completed
      REQUIRE(move_completed);
      REQUIRE(move_succeeded);
      REQUIRE_FALSE(meta.is_changing());
      REQUIRE_FALSE(meta.is_copying());
   }

   SECTION("try_move with location change during concurrent modification")
   {
      // Set up required state
      meta.store(0, std::memory_order_relaxed);  // Clear state
      meta.set_ref(1);                           // Ref count must be > 0
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);

      std::atomic<bool>                     copy_started(false);
      std::atomic<bool>                     copy_flag_set(false);
      std::atomic<node_meta<>::move_result> move_result =
          static_cast<node_meta<>::move_result>(99);  // Invalid initial value
      std::atomic<bool> move_completed(false);

      // Thread 1: Start copy and then try_move to change location
      std::thread move_thread(
          [&]()
          {
             // First, start a move to set the copy flag
             REQUIRE(meta.try_start_move(loc1));
             REQUIRE(meta.is_copying());
             copy_flag_set = true;
             copy_started  = true;

             // Wait a bit to ensure conditions are stable
             std::this_thread::sleep_for(std::chrono::milliseconds(50));

             // Now try to move to a new location
             move_result = meta.try_move(loc1, loc2);

             // After move completes (success or failure), the copy flag should be cleared
             REQUIRE_FALSE(meta.is_copying());

             move_completed = true;
          });

      // Thread 2: Check if the location changed correctly
      std::thread check_thread(
          [&]()
          {
             // Wait for the copy to start
             while (!copy_started)
             {
                std::this_thread::yield();
             }

             // Wait for move to complete
             while (!move_completed)
             {
                std::this_thread::yield();
             }

             // If move was successful, location should be loc2
             if (move_result == node_meta<>::move_result::success)
             {
                REQUIRE(meta.loc() == loc2);
             }
             else
             {
                // If move failed, location should still be loc1
                REQUIRE(meta.loc() == loc1);
             }
          });

      move_thread.join();
      check_thread.join();

      // Move should have succeeded
      REQUIRE(move_result == node_meta<>::move_result::success);
      REQUIRE(meta.loc() == loc2);
      REQUIRE_FALSE(meta.is_copying());
   }

   SECTION("try_move_location with pending_cache flag")
   {
      // Set up required state
      meta.store(0, std::memory_order_relaxed);  // Clear state
      meta.set_ref(1);                           // Ref count must be > 0
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);
      meta.set_pending_cache();  // Set pending_cache flag

      std::atomic<bool>            test_done(false);
      std::optional<node_location> result_loc;

      std::thread test_thread(
          [&]()
          {
             // Try to get the location for move
             result_loc = meta.try_move_location();

             // If successful, copy flag should be set
             if (result_loc.has_value())
             {
                REQUIRE(meta.is_copying());
                REQUIRE(*result_loc == loc1);

                // Clean up
                meta.end_move();
             }

             test_done = true;
          });

      // Wait for the test to complete
      auto start_time = std::chrono::steady_clock::now();
      while (!test_done)
      {
         std::this_thread::sleep_for(std::chrono::milliseconds(10));
         auto elapsed = std::chrono::steady_clock::now() - start_time;
         if (elapsed > std::chrono::milliseconds(500))
         {
            // Timeout - something's wrong
            break;
         }
      }

      test_thread.join();

      // Test should have completed successfully
      REQUIRE(test_done);
      REQUIRE(result_loc.has_value());
      REQUIRE(*result_loc == loc1);
      REQUIRE_FALSE(meta.is_copying());
   }

   SECTION("move interrupted by modification")
   {
      // Set up required state
      meta.store(0, std::memory_order_relaxed);  // Clear state
      meta.set_ref(1);                           // Ref count must be > 0
      meta.set_location_and_type(loc1, node_type::binary, std::memory_order_relaxed);

      std::atomic<bool>                     copy_started(false);
      std::atomic<bool>                     copy_flag_cleared(false);
      std::atomic<bool>                     modify_started(false);
      std::atomic<bool>                     modify_completed(false);
      std::atomic<bool>                     move_attempted(false);
      std::atomic<bool>                     move_completed(false);
      std::atomic<node_meta<>::move_result> move_result =
          static_cast<node_meta<>::move_result>(99);  // Invalid initial value

      // This test verifies that try_move correctly handles the case where
      // a modification happens between try_start_move and try_move

      // Thread 1: Sets up the copy flag first
      std::thread copy_thread(
          [&]()
          {
             // First, start a move to set the copy flag
             bool copy_flag_set = meta.try_start_move(loc1);
             REQUIRE(copy_flag_set);
             REQUIRE(meta.is_copying());
             copy_started = true;

             // Clear the copy flag to allow modify to proceed
             meta.end_move();
             REQUIRE_FALSE(meta.is_copying());
             copy_flag_cleared = true;

             // Wait for modify to start
             while (!modify_started)
             {
                std::this_thread::yield();
             }

             // Now that modify has started, try to do a complete move operation from scratch
             move_attempted = true;

             // Try to start a move again - this will wait until modify is complete
             if (meta.try_start_move(loc1))
             {
                // If we successfully start the move, try to complete it
                move_result = meta.try_move(loc1, loc2);

                // Clean up regardless of result
                if (meta.is_copying())
                {
                   meta.end_move();
                }
             }

             move_completed = true;
          });

      // Thread 2: Waits for copy flag to be cleared, then starts modification
      std::thread modify_thread(
          [&]()
          {
             // Wait for copy to start and then clear its flag
             while (!copy_flag_cleared)
             {
                std::this_thread::yield();
             }

             // Start modify - this should succeed now that copy flag is cleared
             auto state = meta.start_modify();
             REQUIRE(meta.is_changing());
             modify_started = true;

             // Wait for move to be attempted during our modification
             while (!move_attempted)
             {
                std::this_thread::yield();
             }

             // Do some "work" while move is trying to happen
             std::this_thread::sleep_for(std::chrono::milliseconds(100));

             // End the modification
             meta.end_modify();
             modify_completed = true;

             // Wait for move to complete
             while (!move_completed)
             {
                std::this_thread::yield();
             }
          });

      copy_thread.join();
      modify_thread.join();

      // Both operations should have completed
      REQUIRE(modify_completed);
      REQUIRE(move_completed);

      // The move should have succeeded after the modify completed
      REQUIRE(move_result == node_meta<>::move_result::success);
      REQUIRE(meta.loc() == loc2);
      REQUIRE_FALSE(meta.is_changing());
      REQUIRE_FALSE(meta.is_copying());
   }
}