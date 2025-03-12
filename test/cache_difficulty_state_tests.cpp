#include <arbtrie/mapped_memory.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <thread>

namespace arbtrie::test
{
   using namespace arbtrie::mapped_memory;
   using namespace std::chrono;

   TEST_CASE("cache_difficulty_state construction and initial values", "[cache_difficulty]")
   {
      cache_difficulty_state state;

      // Verify initial values (1 in 1024 probability)
      REQUIRE(state._cache_difficulty.load() == (uint32_t(-1) - (uint32_t(-1) / 1024)));
      REQUIRE(state._bytes_promoted_since_last_difficulty_update == 0);
      REQUIRE(state._cache_frequency_window ==
              std::chrono::milliseconds(60000));  // Default is 60 seconds
   }

   TEST_CASE("should_cache basic functionality", "[cache_difficulty]")
   {
      cache_difficulty_state state;

      // For default difficulty, test the basic functionality
      // Test with a range of values to ensure correct behavior
      bool all_rejected = true;
      bool any_accepted = false;

      // Test with minimum difficulty - should accept most values
      state._cache_difficulty.store(1, std::memory_order_relaxed);  // Minimum difficulty

      // Test multiple random values to ensure some are accepted
      for (uint32_t i = 0; i < 10; i++)
      {
         if (state.should_cache(i, 64))
         {
            any_accepted = true;
         }
      }
      REQUIRE(any_accepted ==
              true);  // At least some values should be accepted with minimum difficulty

      // Test with maximum difficulty - should reject all values
      state._cache_difficulty.store(UINT32_MAX, std::memory_order_relaxed);  // Maximum difficulty

      // Check if all values are rejected
      for (uint32_t i = 0; i < 10; i++)
      {
         if (state.should_cache(i, 64))
         {
            all_rejected = false;
         }
      }
      REQUIRE(all_rejected == true);  // All values should be rejected with maximum difficulty

      // Objects larger than max_cacheable_object_size should never be cached
      REQUIRE(state.should_cache(UINT32_MAX, max_cacheable_object_size + 1) == false);
   }

   TEST_CASE("compactor_promote_bytes basic functionality", "[cache_difficulty]")
   {
      cache_difficulty_state state;

      // Verify total_promoted_bytes starts at 0
      REQUIRE(state.total_promoted_bytes.load() == 0);

      // Test 1: Verify adding bytes works
      uint64_t       initial_bytes = state._bytes_promoted_since_last_difficulty_update;
      const uint64_t test_bytes    = 1024;
      state.compactor_promote_bytes(test_bytes);

      // Verify total_promoted_bytes is incremented
      REQUIRE(state.total_promoted_bytes.load() == test_bytes);

      // Test 2: Add a larger amount of bytes to trigger difficulty update
      const uint64_t test_cache_size = 64 * 1024;  // 64KB
      state._total_cache_size        = test_cache_size;
      state._cache_frequency_window  = std::chrono::milliseconds(1000);  // 1 second

      // Set a threshold we know will trigger
      uint64_t target_bytes = state._total_cache_size / 16;
      uint64_t large_amount = target_bytes * 2;  // Ensure it will trigger

      // Add the large amount
      state.compactor_promote_bytes(large_amount);

      // Verify total_promoted_bytes is accumulated correctly
      REQUIRE(state.total_promoted_bytes.load() == test_bytes + large_amount);
   }

   TEST_CASE("total promoted bytes tracking", "[cache_difficulty]")
   {
      cache_difficulty_state state;

      // Initial value should be 0
      REQUIRE(state.total_promoted_bytes.load() == 0);

      // Manually update total_promoted_bytes
      const uint64_t bytes_to_add = 1024;
      state.total_promoted_bytes.fetch_add(bytes_to_add, std::memory_order_relaxed);

      // Total should be updated
      REQUIRE(state.total_promoted_bytes.load() == bytes_to_add);
   }

   TEST_CASE("cache difficulty ranges are maintained", "[cache_difficulty]")
   {
      cache_difficulty_state state;

      // Use shorter window for testing
      state._cache_frequency_window = std::chrono::milliseconds(1000);

      // Set difficulty to nearly maximum
      state._cache_difficulty.store(UINT32_MAX - 10, std::memory_order_relaxed);

      // Update difficulty with arbitrary values
      state.compactor_update_difficulty(system_clock::now());

      // Ensure it doesn't exceed maximum value
      REQUIRE(state.get_cache_difficulty() <= UINT32_MAX);

      // Now test the minimum bound

      // Set difficulty to nearly minimum
      state._cache_difficulty.store(10, std::memory_order_relaxed);

      // Update difficulty with arbitrary values
      state.compactor_update_difficulty(system_clock::now());

      // Ensure it doesn't go below minimum value
      REQUIRE(state.get_cache_difficulty() >= 1);
   }

   TEST_CASE("cache difficulty adjustment with gap-based logic", "[cache_difficulty]")
   {
      cache_difficulty_state state;

      // Use shorter window for testing
      state._cache_frequency_window = std::chrono::milliseconds(1000);

      // Set a known cache size for predictable calculations
      state._total_cache_size = 1024 * 1024;  // 1MB for testing

      // Define the targets based on the implementation
      uint64_t target_bytes = state._total_cache_size / 16;  // 1/16th of cache
      int64_t  target_time_interval =
          state._cache_frequency_window.count() / 16;  // 1/16th of time window

      // Test Case 1: Bytes trigger before time trigger (should increase difficulty by reducing gap by 20%)
      {
         // Start with a known value with a significant gap
         const uint32_t initial_difficulty = 2000000000;  // 2 billion, significant gap from max
         state._cache_difficulty.store(initial_difficulty, std::memory_order_relaxed);

         // Set up for bytes-trigger condition
         auto now           = system_clock::now();
         auto recent        = now - milliseconds(target_time_interval / 2);  // Half the target time
         state._last_update = recent;

         // Set bytes to be just over target
         state._bytes_promoted_since_last_difficulty_update = target_bytes + 1;

         // Update difficulty
         state.compactor_update_difficulty(now);

         // Check if difficulty was increased correctly by reducing gap by 12.5% (7/8)
         uint64_t max_uint32  = uint32_t(-1);
         uint64_t initial_gap = max_uint32 - initial_difficulty;
         uint64_t expected_new_gap =
             (initial_gap * 7) / 8;  // Apply 7/8 reduction (same as in code)
         uint32_t expected = static_cast<uint32_t>(max_uint32 - expected_new_gap);

         REQUIRE(state.get_cache_difficulty() == expected);
      }

      // Test Case 2: Time trigger before bytes trigger (should decrease difficulty by increasing gap)
      {
         // Start with a known value
         const uint32_t initial_difficulty = 2000000000;  // 2 billion
         state._cache_difficulty.store(initial_difficulty, std::memory_order_relaxed);

         // Set up for time-trigger condition
         auto now  = system_clock::now();
         auto past = now - milliseconds(target_time_interval * 2);  // Double the target time
         state._last_update = past;

         // Set bytes to be below target
         state._bytes_promoted_since_last_difficulty_update = target_bytes / 2;

         // Update difficulty
         state.compactor_update_difficulty(now);

         // Check if difficulty was decreased correctly by increasing gap by 12.5% (9/8)
         uint64_t max_uint32       = uint32_t(-1);
         uint64_t initial_gap      = max_uint32 - initial_difficulty;
         uint64_t expected_new_gap = (initial_gap * 9) / 8;  // Apply 9/8 increase (same as in code)
         uint32_t expected         = static_cast<uint32_t>(max_uint32 - expected_new_gap);

         REQUIRE(state.get_cache_difficulty() == expected);
      }

      // Test Case 3: Ensure minimum difficulty is maintained
      {
         // Start with a very low value
         state._cache_difficulty.store(2, std::memory_order_relaxed);

         // Set up for time-trigger condition (which decreases difficulty)
         auto now           = system_clock::now();
         auto past          = now - milliseconds(target_time_interval * 2);
         state._last_update = past;
         state._bytes_promoted_since_last_difficulty_update = 1;  // Very low value

         // Update difficulty
         state.compactor_update_difficulty(now);

         // Check minimum enforced
         REQUIRE(state.get_cache_difficulty() >= 1);
      }

      // Test Case 4: Ensure maximum difficulty is maintained
      {
         // Start with a very high value
         state._cache_difficulty.store(UINT32_MAX - 1, std::memory_order_relaxed);

         // Set up for bytes-trigger condition (which increases difficulty)
         auto now           = system_clock::now();
         auto recent        = now - milliseconds(1);  // Very short time
         state._last_update = recent;
         state._bytes_promoted_since_last_difficulty_update = target_bytes * 2;  // High value

         // Update difficulty
         state.compactor_update_difficulty(now);

         // Check maximum enforced
         REQUIRE(state.get_cache_difficulty() <= UINT32_MAX);
      }
   }
}  // namespace arbtrie::test