#define CATCH_CONFIG_ABORT_AFTER_FAILURE
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/spmc_buffer.hpp>
#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <thread>
#include <vector>

using namespace arbtrie;

TEST_CASE("SPMC Circular Buffer", "[spmc_circular_buffer]")
{
   SECTION("Basic Operations")
   {
      ARBTRIE_WARN("Basic Operations Start");
      spmc_buffer<int> buf;

      // Test initial state
      REQUIRE(buf.usage() == 0);
      REQUIRE(buf.free_space() == 64);
      REQUIRE(buf.get_available_bitmap() == 0);

      // Test single push/consume
      REQUIRE(buf.push(42) >= 0);
      REQUIRE(buf.usage() == 1);
      auto result = buf.pop();
      REQUIRE(result.has_value());
      REQUIRE(*result == 42);
      REQUIRE(buf.usage() == 0);
      ARBTRIE_WARN("Basic Operations End");
   }

   SECTION("Non-blocking Push/Pop Scenarios")
   {
      ARBTRIE_WARN("Non-blocking Push/Pop Scenarios Start");
      spmc_buffer<int> buf;

      // Single item push/pop
      SECTION("Single item")
      {
         ARBTRIE_WARN("Single item Start");
         REQUIRE(buf.push(1) >= 0);
         REQUIRE(buf.usage() == 1);
         auto result = buf.pop();
         REQUIRE(result.has_value());
         REQUIRE(*result == 1);
         REQUIRE(buf.usage() == 0);
         ARBTRIE_WARN("Single item End");
      }

      // Two items push/pop
      SECTION("Two items")
      {
         ARBTRIE_WARN("Two items Start");
         REQUIRE(buf.push(1) >= 0);
         REQUIRE(buf.push(2) >= 0);
         REQUIRE(buf.usage() == 2);
         auto result1 = buf.pop();
         REQUIRE(result1.has_value());
         REQUIRE(*result1 == 1);
         REQUIRE(buf.usage() == 1);
         auto result2 = buf.pop();
         REQUIRE(result2.has_value());
         REQUIRE(*result2 == 2);
         REQUIRE(buf.usage() == 0);
         ARBTRIE_WARN("Two items End");
      }

      // Fill up to just below high water mark
      SECTION("Fill to high water - 1")
      {
         ARBTRIE_WARN("Fill to high water - 1 Start");
         uint64_t         high_water = buf.get_high_water_mark();
         std::vector<int> values;

         // Push items up to high water - 1
         for (int i = 0; i < high_water - 1; ++i)
         {
            REQUIRE(buf.push(i) >= 0);
            values.push_back(i);
         }
         REQUIRE(buf.usage() == high_water - 1);

         // Pop all items and verify values
         for (int expected : values)
         {
            auto result = buf.pop();
            REQUIRE(result.has_value());
            REQUIRE(*result == expected);
         }
         REQUIRE(buf.usage() == 0);
         ARBTRIE_WARN("Fill to high water - 1 End");
      }

      // Verify we can still push after draining
      SECTION("Push after drain")
      {
         ARBTRIE_WARN("Push after drain Start");
         uint64_t high_water = buf.get_high_water_mark();

         // Fill up to high water - 1
         for (int i = 0; i < high_water - 1; ++i)
         {
            REQUIRE(buf.push(i) >= 0);
         }

         // Drain completely
         for (int i = 0; i < high_water - 1; ++i)
         {
            auto result = buf.pop();
            REQUIRE(result.has_value());
            REQUIRE(*result == i);
         }

         // Verify can push again
         REQUIRE(buf.push(100) >= 0);
         auto result = buf.pop();
         REQUIRE(result.has_value());
         REQUIRE(*result == 100);
         ARBTRIE_WARN("Push after drain End");
      }

      ARBTRIE_WARN("Non-blocking Push/Pop Scenarios End");
   }

   SECTION("Blocking Push/Pop Scenarios")
   {
      ARBTRIE_WARN("Blocking Push/Pop Scenarios Start");
      spmc_buffer<int> buf;

      SECTION("Basic push/pop_wait")
      {
         ARBTRIE_WARN("Basic push/pop_wait Start");
         // Single push/pop_wait
         REQUIRE(buf.can_push());
         REQUIRE(buf.push(42) >= 0);
         REQUIRE(buf.usage() == 1);
         auto result = buf.pop_wait();
         REQUIRE(result == 42);
         REQUIRE(buf.usage() == 0);
         ARBTRIE_WARN("Basic push/pop_wait End");
      }

      SECTION("Push to high water")
      {
         ARBTRIE_WARN("Push to high water Start");
         uint64_t high_water = buf.get_high_water_mark();

         // Fill up to high water exactly
         for (int i = 0; i < high_water; ++i)
         {
            REQUIRE(buf.can_push());
            REQUIRE(buf.push(i) >= 0);
         }
         REQUIRE(buf.usage() == high_water);

         // Drain halfway
         for (int i = 0; i < high_water / 2; ++i)
         {
            auto result = buf.pop_wait();
            REQUIRE(result == i);
         }

         // Should be able to push more now
         for (int i = 0; i < high_water / 2; ++i)
         {
            REQUIRE(buf.can_push());
            REQUIRE(buf.push(i + 100) >= 0);
         }

         // Drain the rest
         for (int i = high_water / 2; i < high_water; ++i)
         {
            auto result = buf.pop_wait();
            REQUIRE(result == i);
         }
         for (int i = 0; i < high_water / 2; ++i)
         {
            auto result = buf.pop_wait();
            REQUIRE(result == i + 100);
         }
         ARBTRIE_WARN("Push to high water End");
      }

      ARBTRIE_WARN("Blocking Push/Pop Scenarios End");
   }

   SECTION("Water Mark Adjustments")
   {
      ARBTRIE_WARN("Water Mark Adjustments Start");
      spmc_buffer<int> buf;

      uint64_t original_high = buf.get_high_water_mark();
      uint64_t original_low  = buf.get_low_water_mark();
      uint64_t original_gap  = buf.get_min_water_gap();

      REQUIRE(original_high > original_low);
      REQUIRE(original_high - original_low >= original_gap);

      // Test setting min gap
      REQUIRE(buf.set_min_water_gap(original_gap + 2));
      REQUIRE(buf.get_min_water_gap() == original_gap + 2);

      // Verify high water mark adjusts if needed
      REQUIRE(buf.get_high_water_mark() - buf.get_low_water_mark() >= buf.get_min_water_gap());

      // Test invalid gap (too large)
      REQUIRE_FALSE(buf.set_min_water_gap(buf.capacity()));

      // Test invalid gap (zero)
      REQUIRE_FALSE(buf.set_min_water_gap(0));

      ARBTRIE_WARN("Water Mark Adjustments End");
   }

   SECTION("Bitmap state tracking")
   {
      ARBTRIE_WARN("Bitmap state tracking Start");
      spmc_buffer<int> buf;

      // Initially all bits in bitmap should be 0
      REQUIRE(buf.get_available_bitmap() == 0);

      // Set one bit
      int64_t idx = buf.push(42);
      REQUIRE(idx >= 0);

      // Verify the corresponding bit is set
      REQUIRE((buf.get_available_bitmap() & (1ULL << idx)) != 0);

      // Set another bit
      int64_t idx2 = buf.push(43);
      REQUIRE(idx2 >= 0);
      REQUIRE(idx2 != idx);

      // Verify both bits are set
      REQUIRE((buf.get_available_bitmap() & (1ULL << idx)) != 0);
      REQUIRE((buf.get_available_bitmap() & (1ULL << idx2)) != 0);

      // Consume one item
      auto result = buf.pop();
      REQUIRE(result.has_value());
      REQUIRE(*result == 42);

      // Verify first bit is now cleared, second still set
      REQUIRE((buf.get_available_bitmap() & (1ULL << idx)) == 0);
      REQUIRE((buf.get_available_bitmap() & (1ULL << idx2)) != 0);

      // Consume second item
      result = buf.pop();
      REQUIRE(result.has_value());
      REQUIRE(*result == 43);

      // Verify both bits now cleared
      REQUIRE(buf.get_available_bitmap() == 0);

      ARBTRIE_WARN("Bitmap state tracking End");
   }

   SECTION("Consumption Tracking")
   {
      ARBTRIE_WARN("Consumption Tracking Start");
      spmc_buffer<int> buf;

      // Push an item and check consumption
      int64_t idx = buf.push(42);
      REQUIRE(idx >= 0);
      REQUIRE_FALSE(buf.check_consumption(idx));

      // Consume and verify consumption
      auto result = buf.pop();
      REQUIRE(result.has_value());
      REQUIRE(*result == 42);
      REQUIRE(buf.check_consumption(idx));

      // Push multiple items
      std::vector<int64_t> indices;
      for (int i = 0; i < 5; ++i)
      {
         indices.push_back(buf.push(100 + i));
         REQUIRE(indices.back() >= 0);
      }

      // Check consumption bitmap
      uint64_t to_check = 0;
      for (auto index : indices)
      {
         to_check |= (1ULL << index);
      }
      REQUIRE(buf.get_consumed_bitmap(to_check) == 0);

      // Consume some items
      for (int i = 0; i < 3; ++i)
      {
         result = buf.pop();
         REQUIRE(result.has_value());
         REQUIRE(*result == 100 + i);
      }

      // Verify consumption bitmap shows first 3 consumed, last 2 not
      uint64_t expected_consumed = 0;
      for (int i = 0; i < 3; ++i)
      {
         expected_consumed |= (1ULL << indices[i]);
      }
      REQUIRE(buf.get_consumed_bitmap(to_check) == expected_consumed);

      ARBTRIE_WARN("Consumption Tracking End");
   }

   SECTION("push_front LIFO behavior")
   {
      ARBTRIE_WARN("push_front LIFO behavior Start");
      spmc_buffer<int> buf;

      // Push multiple items in FIFO order
      REQUIRE(buf.push(100) >= 0);
      REQUIRE(buf.push(200) >= 0);
      REQUIRE(buf.push(300) >= 0);

      // Push to front - this should be the next item popped
      int64_t front_idx = buf.push_front(50);
      REQUIRE(front_idx >= 0);

      // Verify LIFO behavior for the front-pushed item
      auto result1 = buf.pop();
      REQUIRE(result1.has_value());
      REQUIRE(*result1 == 50);  // Front item should come first

      // Then verify remaining items come in FIFO order
      auto result2 = buf.pop();
      REQUIRE(result2.has_value());
      REQUIRE(*result2 == 100);

      auto result3 = buf.pop();
      REQUIRE(result3.has_value());
      REQUIRE(*result3 == 200);

      auto result4 = buf.pop();
      REQUIRE(result4.has_value());
      REQUIRE(*result4 == 300);

      ARBTRIE_WARN("push_front LIFO behavior End");
   }

   SECTION("try_swap functionality")
   {
      ARBTRIE_WARN("try_swap functionality Start");
      spmc_buffer<int> buf;

      // Push some data
      int64_t idx = buf.push(42);
      REQUIRE(idx >= 0);

      // Verify swappable bitmap shows this slot is available for swapping
      REQUIRE(buf.get_swappable_bitmap() & (1ULL << idx));

      // Try to swap with a slot that has data
      auto swapped = buf.try_swap(idx, 99);
      REQUIRE(swapped.has_value());
      REQUIRE(*swapped == 42);

      // Verify the slot now contains the new value
      auto result = buf.pop();
      REQUIRE(result.has_value());
      REQUIRE(*result == 99);

      // After popping, the slot is not swappable (pending ack)
      REQUIRE_FALSE(buf.get_swappable_bitmap() & (1ULL << idx));

      // Acknowledge the pop
      auto acked = buf.pop_ack();
      REQUIRE(acked.has_value());

      // After ack, the slot is no longer in use, so not swappable
      REQUIRE_FALSE(buf.get_swappable_bitmap() & (1ULL << idx));

      // But it is pushable
      REQUIRE(buf.get_pushable_bitmap() & (1ULL << idx));

      // Try to swap with an empty slot - should fail and return nullopt
      auto empty_swap = buf.try_swap(idx, 100);
      REQUIRE_FALSE(empty_swap.has_value());

      // But the value should still have been written
      auto new_result = buf.pop();
      REQUIRE(new_result.has_value());
      REQUIRE(*new_result == 100);

      ARBTRIE_WARN("try_swap functionality End");
   }

   SECTION("Bitmap accessors")
   {
      ARBTRIE_WARN("Bitmap accessors Start");
      spmc_buffer<int> buf;

      // Initially all slots are pushable and none are swappable
      REQUIRE(buf.get_pushable_bitmap() == 0xFFFFFFFFFFFFFFFF);
      REQUIRE(buf.get_swappable_bitmap() == 0);

      // Push to some slots
      int64_t idx1 = buf.push(1);
      int64_t idx2 = buf.push(2);
      int64_t idx3 = buf.push(3);

      // Slots with data are not pushable
      REQUIRE_FALSE(buf.get_pushable_bitmap() & (1ULL << idx1));
      REQUIRE_FALSE(buf.get_pushable_bitmap() & (1ULL << idx2));

      // But they are swappable
      REQUIRE(buf.get_swappable_bitmap() & (1ULL << idx1));
      REQUIRE(buf.get_swappable_bitmap() & (1ULL << idx2));

      // Pop an item
      auto popped = buf.pop();
      REQUIRE(popped.has_value());

      // After pop, the slot is still not pushable (pending ack)
      REQUIRE_FALSE(buf.get_pushable_bitmap() & (1ULL << idx1));

      // And not swappable either
      REQUIRE_FALSE(buf.get_swappable_bitmap() & (1ULL << idx1));

      // Acknowledge the pop
      auto acked = buf.pop_ack();
      REQUIRE(acked.has_value());

      // Now the slot is pushable again
      REQUIRE(buf.get_pushable_bitmap() & (1ULL << idx1));

      // But still not swappable
      REQUIRE_FALSE(buf.get_swappable_bitmap() & (1ULL << idx1));

      ARBTRIE_WARN("Bitmap accessors End");
   }

   SECTION("pending_ack_count tracking")
   {
      ARBTRIE_WARN("pending_ack_count tracking Start");
      spmc_buffer<int> buf;

      // Initially no pending acks
      REQUIRE(buf.pending_ack_count() == 0);

      // Push some items
      for (int i = 0; i < 5; i++)
      {
         REQUIRE(buf.push(i) >= 0);
      }

      // Still no pending acks before consumption
      REQUIRE(buf.pending_ack_count() == 0);

      // Consume 3 items
      for (int i = 0; i < 3; i++)
      {
         auto result = buf.pop();
         REQUIRE(result.has_value());
         REQUIRE(*result == i);
      }

      // Now we should have 3 pending acks
      REQUIRE(buf.pending_ack_count() == 3);

      // Verify usage reflects both available and pending ack slots
      REQUIRE(buf.usage() == 5);  // 2 available + 3 pending ack

      // Verify free space takes into account both available and pending ack slots
      REQUIRE(buf.free_space() == buf.capacity() - 5);

      // Acknowledge 2 consumed items
      for (int i = 0; i < 2; i++)
      {
         auto acked = buf.pop_ack();
         REQUIRE(acked.has_value());
      }

      // Now we should have 1 pending ack
      REQUIRE(buf.pending_ack_count() == 1);

      // Usage reflects 2 available + 1 pending ack
      REQUIRE(buf.usage() == 3);

      // Free space is now increased
      REQUIRE(buf.free_space() == buf.capacity() - 3);

      // Acknowledge the last consumed item
      auto last_ack = buf.pop_ack();
      REQUIRE(last_ack.has_value());

      // No more pending acks
      REQUIRE(buf.pending_ack_count() == 0);

      // Usage reflects only available items
      REQUIRE(buf.usage() == 2);

      ARBTRIE_WARN("pending_ack_count tracking End");
   }

   SECTION("pop_back LIFO behavior")
   {
      ARBTRIE_WARN("pop_back LIFO behavior Start");
      spmc_buffer<int> buf;

      // Push multiple items with regular push (FIFO)
      REQUIRE(buf.push(10) >= 0);
      REQUIRE(buf.push(20) >= 0);
      REQUIRE(buf.push(30) >= 0);
      REQUIRE(buf.push(40) >= 0);

      // Pop from the back (LIFO order from consumer perspective)
      auto result1 = buf.pop_back();
      REQUIRE(result1.has_value());
      REQUIRE(*result1 == 40);  // Last pushed item comes out first

      auto result2 = buf.pop_back();
      REQUIRE(result2.has_value());
      REQUIRE(*result2 == 30);  // Second-to-last pushed item

      auto result3 = buf.pop_back();
      REQUIRE(result3.has_value());
      REQUIRE(*result3 == 20);

      auto result4 = buf.pop_back();
      REQUIRE(result4.has_value());
      REQUIRE(*result4 == 10);  // First pushed item comes out last

      // Buffer should now be empty
      auto result5 = buf.pop_back();
      REQUIRE_FALSE(result5.has_value());

      ARBTRIE_WARN("pop_back LIFO behavior End");
   }

   SECTION("Mixed pop and pop_back")
   {
      ARBTRIE_WARN("Mixed pop and pop_back Start");
      spmc_buffer<int> buf;

      // Push 5 items
      for (int i = 0; i < 5; i++)
      {
         REQUIRE(buf.push(i) >= 0);
      }

      // Alternating pop from front and back
      auto front1 = buf.pop();       // Take from front (0)
      auto back1  = buf.pop_back();  // Take from back (4)
      auto front2 = buf.pop();       // Take from front (1)
      auto back2  = buf.pop_back();  // Take from back (3)
      auto last   = buf.pop();       // Take last item (2)

      REQUIRE(front1.has_value());
      REQUIRE(*front1 == 0);

      REQUIRE(back1.has_value());
      REQUIRE(*back1 == 4);

      REQUIRE(front2.has_value());
      REQUIRE(*front2 == 1);

      REQUIRE(back2.has_value());
      REQUIRE(*back2 == 3);

      REQUIRE(last.has_value());
      REQUIRE(*last == 2);

      // Buffer should now be empty
      REQUIRE_FALSE(buf.pop().has_value());
      REQUIRE_FALSE(buf.pop_back().has_value());

      ARBTRIE_WARN("Mixed pop and pop_back End");
   }

   SECTION("push_front with pop_back")
   {
      ARBTRIE_WARN("push_front with pop_back Start");
      spmc_buffer<int> buf;

      // Push items with push_front (LIFO on producer side)
      for (int i = 0; i < 5; i++)
      {
         REQUIRE(buf.push_front(i) >= 0);
      }

      // Pop from back (LIFO on consumer side)
      // Since we pushed with push_front, this should give us the original order
      for (int i = 0; i < 5; i++)
      {
         auto result = buf.pop_back();
         REQUIRE(result.has_value());
         REQUIRE(*result == i);
      }

      ARBTRIE_WARN("push_front with pop_back End");
   }

   SECTION("pop_back_wait behavior")
   {
      ARBTRIE_WARN("pop_back_wait behavior Start");
      spmc_buffer<int> buf;

      // Push some data
      REQUIRE(buf.push(42) >= 0);

      // Test pop_back_wait
      auto val = buf.pop_back_wait();
      REQUIRE(val == 42);

      // Test with multiple values
      REQUIRE(buf.push(10) >= 0);
      REQUIRE(buf.push(20) >= 0);
      REQUIRE(buf.push(30) >= 0);

      // pop_back_wait should return values in reverse order
      REQUIRE(buf.pop_back_wait() == 30);
      REQUIRE(buf.pop_back_wait() == 20);
      REQUIRE(buf.pop_back_wait() == 10);

      // Test concurrent push/pop_back_wait with a thread
      std::atomic<bool> thread_done{false};
      std::thread       consumer(
          [&]()
          {
             auto val1 = buf.pop_back_wait();
             auto val2 = buf.pop_back_wait();
             REQUIRE(val1 == 100);
             REQUIRE(val2 == 200);
             thread_done.store(true);
          });

      // Sleep briefly to ensure consumer is waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      // Push values that the consumer is waiting for
      REQUIRE(buf.push(100) >= 0);
      REQUIRE(buf.push(200) >= 0);

      // Wait for consumer thread to complete
      consumer.join();
      REQUIRE(thread_done.load());

      ARBTRIE_WARN("pop_back_wait behavior End");
   }

   SECTION("pop with skip_ack feature")
   {
      ARBTRIE_WARN("pop with skip_ack feature Start");
      spmc_buffer<int> buf;

      // Push some items
      for (int i = 0; i < 8; i++)
      {
         REQUIRE(buf.push(i) >= 0);
      }

      // Normal pop (with acknowledgment) - using default template parameter
      auto result1 = buf.pop();  // Default is ack_mode::require_ack
      REQUIRE(result1.has_value());
      REQUIRE(*result1 == 0);

      // There should be one pending acknowledgment
      REQUIRE(buf.pending_ack_count() == 1);

      // Different ways to specify skipping acknowledgment:

      // 1. Using the full enum path
      auto result2 = buf.pop<spmc_buffer<int>::ack_mode::skip_ack>();
      REQUIRE(result2.has_value());
      REQUIRE(*result2 == 1);

      // 2. Using the convenient static constant
      auto result3 = buf.pop<spmc_buffer<int>::skip_ack>();
      REQUIRE(result3.has_value());
      REQUIRE(*result3 == 2);

      // 3. Using the most readable helper method
      auto result4 = buf.pop_without_ack();
      REQUIRE(result4.has_value());
      REQUIRE(*result4 == 3);

      // Still only one pending acknowledgment (from the first pop)
      REQUIRE(buf.pending_ack_count() == 1);

      // Different pop variants without acknowledgment
      auto result5 = buf.pop_back_without_ack();  // LIFO pop without ack
      REQUIRE(result5.has_value());
      REQUIRE(*result5 == 7);

      auto result6 = buf.pop_wait_without_ack();  // Blocking FIFO pop without ack
      REQUIRE(result6 == 4);

      auto result7 = buf.pop_back_wait_without_ack();  // Blocking LIFO pop without ack
      REQUIRE(result7 == 6);

      // Still only one pending acknowledgment
      REQUIRE(buf.pending_ack_count() == 1);

      // The producer acknowledges the first pop
      auto ack_result = buf.pop_ack();
      REQUIRE(ack_result.has_value());
      REQUIRE(*ack_result == 0);

      // No more pending acknowledgments
      REQUIRE(buf.pending_ack_count() == 0);

      // Only item 5 remains
      auto result8 = buf.pop();
      REQUIRE(result8.has_value());
      REQUIRE(*result8 == 5);
      REQUIRE(buf.pending_ack_count() == 1);

      ARBTRIE_WARN("pop with skip_ack feature End");
   }

   SECTION("Priority Handling")
   {
      ARBTRIE_WARN("Priority Handling Start");
      spmc_buffer<int> buf;

      // Low priority (push)
      REQUIRE(buf.push(100) >= 0);
      REQUIRE(buf.push(101) >= 0);
      REQUIRE(buf.push(102) >= 0);

      // High priority (push_front)
      REQUIRE(buf.push_front(10) >= 0);
      REQUIRE(buf.push_front(11) >= 0);

      // More low priority
      REQUIRE(buf.push(103) >= 0);
      REQUIRE(buf.push(104) >= 0);

      // Another high priority
      REQUIRE(buf.push_front(12) >= 0);

      // Test pop (which prefers high priority)
      auto high1 = buf.pop();
      REQUIRE(high1.has_value());
      REQUIRE(*high1 == 12);  // Should get newest high priority item (LIFO for high)

      auto high2 = buf.pop();
      REQUIRE(high2.has_value());
      REQUIRE(*high2 == 11);

      auto high3 = buf.pop();
      REQUIRE(high3.has_value());
      REQUIRE(*high3 == 10);

      // No more high priority items
      auto high4 = buf.pop();
      REQUIRE(high4.has_value());
      REQUIRE(*high4 == 100);  // Now gets low priority items

      // Test pop_back (which gets low priority)
      auto low1 = buf.pop_back();
      REQUIRE(low1.has_value());
      REQUIRE(*low1 == 101);  // Should get next low priority item (FIFO for low)

      auto low2 = buf.pop_back();
      REQUIRE(low2.has_value());
      REQUIRE(*low2 == 102);

      // Test pop when no high priority items left
      auto prefer_high = buf.pop();
      REQUIRE(prefer_high.has_value());
      REQUIRE(*prefer_high == 103);  // Gets next low priority item

      // Reset buffer for more tests
      buf.reset();  // Reset the buffer to its initial state

      // Push mix of priorities
      REQUIRE(buf.push(200) >= 0);       // Low
      REQUIRE(buf.push_front(20) >= 0);  // High
      REQUIRE(buf.push(201) >= 0);       // Low
      REQUIRE(buf.push_front(21) >= 0);  // High

      // Test pop (gets high priority)
      auto prefer_high1 = buf.pop();
      REQUIRE(prefer_high1.has_value());
      REQUIRE(*prefer_high1 == 21);  // Should get high priority

      // Test pop_back (gets low priority)
      auto prefer_low1 = buf.pop_back();
      REQUIRE(prefer_low1.has_value());
      REQUIRE(*prefer_low1 == 200);  // Should get low priority

      // Test with skip_ack feature
      auto high_no_ack = buf.pop_without_ack();
      REQUIRE(high_no_ack.has_value());
      REQUIRE(*high_no_ack == 20);
      REQUIRE(buf.pending_ack_count() == 0);  // No pending ack

      auto low_no_ack = buf.pop_back_without_ack();
      REQUIRE(low_no_ack.has_value());
      REQUIRE(*low_no_ack == 201);
      REQUIRE(buf.pending_ack_count() == 0);  // No pending ack

      ARBTRIE_WARN("Priority Handling End");
   }
}

TEST_CASE("SPMC Circular Buffer Concurrent", "[spmc_circular_buffer][concurrent]")
{
   SECTION("Single Producer Multiple Consumers")
   {
      constexpr int     NUM_ITEMS     = 10000;
      constexpr int     NUM_CONSUMERS = 4;
      spmc_buffer<int>  buf;
      std::atomic<int>  consumed{0};
      std::atomic<bool> stop_consumers{false};
      std::vector<int>  items_to_push;

      // Prepare items to push
      for (int i = 0; i < NUM_ITEMS; ++i)
      {
         items_to_push.push_back(i);
      }

      // Start consumer threads
      std::vector<std::thread> consumers;
      for (int c = 0; c < NUM_CONSUMERS; ++c)
      {
         consumers.emplace_back(
             [&buf, &consumed, &stop_consumers]()
             {
                while (!stop_consumers.load())
                {
                   auto item = buf.pop();
                   if (item.has_value())
                   {
                      consumed.fetch_add(1, std::memory_order_relaxed);
                   }
                   else
                   {
                      // Give producer a chance
                      std::this_thread::yield();
                   }
                }
             });
      }

      // Producer pushes all items
      for (int value : items_to_push)
      {
         while (buf.push(value) < 0)
         {
            // Buffer full, wait a bit
            std::this_thread::yield();
         }
      }

      // Wait for all items to be consumed
      while (consumed.load() < NUM_ITEMS)
      {
         std::this_thread::yield();
      }

      // Stop consumers and join threads
      stop_consumers.store(true);
      for (auto& consumer : consumers)
      {
         consumer.join();
      }

      // Verify all items were consumed
      REQUIRE(consumed.load() == NUM_ITEMS);
   }
}