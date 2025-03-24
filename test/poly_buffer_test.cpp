#include <arbtrie/poly_buffer.hpp>
#include <catch2/catch_test_macros.hpp>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace arbtrie;

TEST_CASE("poly_buffer basic operations", "[poly_buffer]")
{
   poly_buffer<int> buffer;

   SECTION("initial state")
   {
      REQUIRE(buffer.usage() == 0);
      REQUIRE(buffer.capacity() == 32);
      REQUIRE_FALSE(buffer.try_pop().has_value());
   }

   SECTION("single push/pop")
   {
      REQUIRE(buffer.try_push(42));
      REQUIRE(buffer.usage() == 1);

      auto result = buffer.try_pop();
      REQUIRE(result.has_value());
      REQUIRE(*result == 42);
      REQUIRE(buffer.usage() == 0);
   }

   SECTION("multiple push until full")
   {
      // Fill buffer
      for (int i = 0; i < 32; i++)
      {
         REQUIRE(buffer.try_push(i));
      }

      // Buffer should be full
      REQUIRE_FALSE(buffer.try_push(999));
      REQUIRE(buffer.usage() == 32);

      // Verify values
      for (int i = 0; i < 32; i++)
      {
         auto result = buffer.try_pop();
         REQUIRE(result.has_value());
         REQUIRE(*result == i);
      }
   }
}

TEST_CASE("poly_buffer concurrent operations", "[poly_buffer]")
{
   poly_buffer<int> buffer;
   constexpr int    num_items     = 100000;  // Large number to ensure good distribution
   constexpr int    num_consumers = 4;

   SECTION("single producer multiple consumers with uniqueness check")
   {
      std::atomic<int> total_consumed{0};

      // Thread-safe sets to track values consumed by each consumer
      std::vector<std::unordered_set<int>> consumed_values(num_consumers);
      std::vector<std::mutex>              set_mutexes(num_consumers);

      // Start consumer threads
      std::vector<std::thread> consumers;
      for (int i = 0; i < num_consumers; i++)
      {
         consumers.emplace_back(
             [&buffer, &total_consumed, &consumed_values, &set_mutexes, i]
             {
                while (total_consumed.load(std::memory_order_relaxed) < num_items)
                {
                   if (auto val = buffer.try_pop())
                   {
                      // Safely add the value to this consumer's set
                      std::lock_guard<std::mutex> lock(set_mutexes[i]);
                      consumed_values[i].insert(*val);
                      total_consumed.fetch_add(1, std::memory_order_relaxed);
                   }
                }
             });
      }

      // Producer thread
      std::thread producer(
          [&buffer]
          {
             for (int i = 0; i < num_items; i++)
             {
                buffer.push(i);
             }
          });

      producer.join();
      for (auto& consumer : consumers)
      {
         consumer.join();
      }

      // Verify all items were consumed
      REQUIRE(total_consumed == num_items);
      REQUIRE(buffer.usage() == 0);

      // Combine all consumed values into one set to check for duplicates
      std::unordered_set<int> all_consumed;
      int                     total_unique = 0;

      for (int i = 0; i < num_consumers; i++)
      {
         // Each value should appear exactly once across all consumers
         for (int val : consumed_values[i])
         {
            REQUIRE(all_consumed.insert(val).second);  // Should always insert successfully
            total_unique++;
         }
      }

      // Verify we got all unique values from 0 to num_items-1
      REQUIRE(total_unique == num_items);
      for (int i = 0; i < num_items; i++)
      {
         REQUIRE(all_consumed.contains(i));
      }
   }
}

TEST_CASE("poly_buffer blocking operations", "[poly_buffer]")
{
   poly_buffer<int> buffer;

   SECTION("blocking push when full")
   {
      // Fill buffer
      for (int i = 0; i < 32; i++)
      {
         REQUIRE(buffer.try_push(i));
      }

      // Start thread that will push one more item
      bool        push_completed = false;
      std::thread pusher(
          [&buffer, &push_completed]
          {
             buffer.push(999);  // This should block
             push_completed = true;
          });

      // Give the pusher thread time to block
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      REQUIRE_FALSE(push_completed);  // Should still be blocked

      // Pop one item to make space
      auto val = buffer.try_pop();
      REQUIRE(val.has_value());

      // Wait for pusher to complete
      pusher.join();
      REQUIRE(push_completed);
      REQUIRE(buffer.usage() == 32);  // Buffer should be full again
   }

   SECTION("blocking pop when empty")
   {
      bool pop_completed = false;
      int  popped_value  = 0;

      // Start thread that will pop
      std::thread popper(
          [&buffer, &pop_completed, &popped_value]
          {
             popped_value  = buffer.pop();  // This should block
             pop_completed = true;
          });

      // Give the popper thread time to block
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      REQUIRE_FALSE(pop_completed);  // Should still be blocked

      // Push a value
      buffer.push(42);

      // Wait for popper to complete
      popper.join();
      REQUIRE(pop_completed);
      REQUIRE(popped_value == 42);
      REQUIRE(buffer.usage() == 0);
   }
}

TEST_CASE("poly_buffer edge cases", "[poly_buffer]")
{
   poly_buffer<int> buffer;

   SECTION("rapid push/pop cycles")
   {
      for (int i = 0; i < 1000; i++)
      {
         REQUIRE(buffer.try_push(i));
         auto val = buffer.try_pop();
         REQUIRE(val.has_value());
         REQUIRE(*val == i);
      }
   }

   SECTION("alternating full/empty cycles")
   {
      for (int cycle = 0; cycle < 10; cycle++)
      {
         // Fill
         for (int i = 0; i < 32; i++)
         {
            REQUIRE(buffer.try_push(i));
         }
         REQUIRE(buffer.usage() == 32);

         // Empty
         for (int i = 0; i < 32; i++)
         {
            auto val = buffer.try_pop();
            REQUIRE(val.has_value());
            REQUIRE(*val == i);
         }
         REQUIRE(buffer.usage() == 0);
      }
   }
}