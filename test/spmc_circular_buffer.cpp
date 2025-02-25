#define CATCH_CONFIG_ABORT_AFTER_FAILURE
#include <arbtrie/circular_buffer.hpp>
#include <arbtrie/debug.hpp>
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
      spmc_circular_buffer<int> buf;

      // Test initial state
      REQUIRE(buf.usage() == 0);
      REQUIRE(buf.free_space() == 64);
      REQUIRE(buf.get_available_bitmap() == 0);

      // Test single push/consume
      int value;
      REQUIRE(buf.push(42));
      REQUIRE(buf.usage() == 1);
      REQUIRE(buf.try_consume(value));
      REQUIRE(value == 42);
      REQUIRE(buf.usage() == 0);
      ARBTRIE_WARN("Basic Operations End");
   }

   SECTION("Non-blocking Push/Pop Scenarios")
   {
      ARBTRIE_WARN("Non-blocking Push/Pop Scenarios Start");
      spmc_circular_buffer<int> buf;
      int                       value;

      // Single item push/pop
      SECTION("Single item")
      {
         ARBTRIE_WARN("Single item Start");
         REQUIRE(buf.push(1));
         REQUIRE(buf.usage() == 1);
         REQUIRE(buf.try_consume(value));
         REQUIRE(value == 1);
         REQUIRE(buf.usage() == 0);
         ARBTRIE_WARN("Single item End");
      }

      // Two items push/pop
      SECTION("Two items")
      {
         ARBTRIE_WARN("Two items Start");
         REQUIRE(buf.push(1));
         REQUIRE(buf.push(2));
         REQUIRE(buf.usage() == 2);
         REQUIRE(buf.try_consume(value));
         REQUIRE(value == 1);
         REQUIRE(buf.usage() == 1);
         REQUIRE(buf.try_consume(value));
         REQUIRE(value == 2);
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
            REQUIRE(buf.push(i));
            values.push_back(i);
         }
         REQUIRE(buf.usage() == high_water - 1);

         // Pop all items and verify values
         for (int expected : values)
         {
            REQUIRE(buf.try_consume(value));
            REQUIRE(value == expected);
         }
         REQUIRE(buf.usage() == 0);
         ARBTRIE_WARN("Fill to high water - 1 End");
      }

      // Verify we can still push after draining
      SECTION("Push after drain")
      {
         ARBTRIE_WARN("Push after drain Start");
         uint64_t high_water = buf.get_high_water_mark();

         // Fill and drain twice to ensure no state issues
         for (int round = 0; round < 2; ++round)
         {
            // Fill to high water - 1
            for (int i = 0; i < high_water - 1; ++i)
            {
               REQUIRE(buf.push(i));
            }
            REQUIRE(buf.usage() == high_water - 1);

            // Drain completely
            while (buf.try_consume(value))
            {
            }
            REQUIRE(buf.usage() == 0);
         }
         ARBTRIE_WARN("Push after drain End");
      }
      ARBTRIE_WARN("Non-blocking Push/Pop Scenarios End");
   }

   SECTION("Water Marks")
   {
      ARBTRIE_WARN("Water Marks Start");
      spmc_circular_buffer<int> buf;

      // Test initial water marks
      REQUIRE(buf.get_high_water_mark() == 48);  // 75% of 64
      REQUIRE(buf.get_low_water_mark() == 16);   // 25% of 64
      REQUIRE(buf.get_min_water_gap() == 8);

      // Test setting min water gap
      REQUIRE(buf.set_min_water_gap(12));
      REQUIRE(buf.get_min_water_gap() == 12);

      // Test invalid gap values
      REQUIRE_FALSE(buf.set_min_water_gap(0));
      REQUIRE_FALSE(buf.set_min_water_gap(65));
      ARBTRIE_WARN("Water Marks End");
   }

   SECTION("Buffer Full")
   {
      ARBTRIE_WARN("Buffer Full Start");
      spmc_circular_buffer<int> buf;
      std::atomic<bool>         producer_done{false};
      std::atomic<bool>         consumer_done{false};

      // Start consumer thread first
      std::thread consumer(
          [&]()
          {
             int value;
             int items_consumed       = 0;
             int consecutive_failures = 0;

             // Keep consuming while producer is running or there's data
             while (!producer_done || buf.usage() > 0)
             {
                if (buf.try_consume(value))
                {
                   items_consumed++;
                   consecutive_failures = 0;
                }
                else
                {
                   consecutive_failures++;
                   if (consecutive_failures > 1000)
                   {
                      // If we've failed many times in a row, sleep to reduce contention
                      std::this_thread::sleep_for(std::chrono::microseconds(100));
                      consecutive_failures = 0;
                   }
                   else
                   {
                      // Otherwise just yield to let other threads run
                      std::this_thread::yield();
                   }
                }
             }
             consumer_done = true;
          });

      // Producer thread
      std::thread producer(
          [&]()
          {
             // Try to fill buffer beyond high water mark
             int i;
             for (i = 0; i < buf.get_high_water_mark() * 2; ++i)
             {
                REQUIRE(buf.push(i));
             }
             producer_done = true;
          });

      // Wait for both threads to finish
      producer.join();
      consumer.join();

      REQUIRE(producer_done);
      REQUIRE(consumer_done);
      REQUIRE(buf.usage() == 0);
      ARBTRIE_WARN("Buffer Full End");
   }

   SECTION("Multiple Consumers")
   {
      ARBTRIE_WARN("Multiple Consumers Start");
      spmc_circular_buffer<int> buf;
      std::atomic<int>          total_consumed{0};
      constexpr int             items_per_consumer = 1000;
      constexpr int             num_consumers      = 4;

      // Producer thread
      std::thread producer(
          [&]()
          {
             for (int i = 0; i < items_per_consumer * num_consumers; ++i)
             {
                while (!buf.push(i))
                {
                   std::this_thread::yield();
                }
             }
          });

      // Consumer threads
      std::vector<std::thread> consumers;
      for (int i = 0; i < num_consumers; ++i)
      {
         consumers.emplace_back(
             [&]()
             {
                int consumed = 0;
                int value;
                while (consumed < items_per_consumer)
                {
                   if (buf.try_consume(value))
                   {
                      consumed++;
                      total_consumed.fetch_add(1, std::memory_order_relaxed);
                   }
                   else
                   {
                      std::this_thread::yield();
                   }
                }
             });
      }

      producer.join();
      for (auto& consumer : consumers)
      {
         consumer.join();
      }

      REQUIRE(total_consumed == items_per_consumer * num_consumers);
      REQUIRE(buf.usage() == 0);
      ARBTRIE_WARN("Multiple Consumers End");
   }

   SECTION("Blocking Consume")
   {
      ARBTRIE_WARN("Blocking Consume Start");
      spmc_circular_buffer<int> buf;
      bool                      consumer_finished = false;

      // Consumer thread using blocking consume
      std::thread consumer(
          [&]()
          {
             int value;
             buf.consume(value);  // This should block until data is available
             REQUIRE(value == 42);
             consumer_finished = true;
          });

      // Give consumer time to start waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      // Producer pushes data
      REQUIRE(buf.push(42));

      consumer.join();
      REQUIRE(consumer_finished);
      ARBTRIE_WARN("Blocking Consume End");
   }

   /*
   SECTION("Adaptive Water Marks")
   {
      ARBTRIE_WARN("Adaptive Water Marks Start");
      spmc_circular_buffer<int> buf;
      const uint64_t            initial_high = buf.get_high_water_mark();

      // Fill buffer to force producer waiting
      for (int i = 0; i < 64; ++i)
      {
         buf.push(i);
      }

      // Consumer thread that waits before consuming
      std::thread consumer(
          [&]()
          {
             std::this_thread::sleep_for(std::chrono::milliseconds(100));
             int value;
             for (int i = 0; i < 32; ++i)
             {
                REQUIRE(buf.try_consume(value));
             }
          });

      // Producer thread that should experience waiting
      std::thread producer(
          [&]()
          {
             for (int i = 0; i < 16; ++i)
             {
                buf.push(i);  // This should trigger water mark adjustment
             }
          });

      producer.join();
      consumer.join();

      // High water mark should have been adjusted down due to producer waiting
      REQUIRE(buf.get_high_water_mark() < initial_high);
      ARBTRIE_WARN("Adaptive Water Marks End");
   }

   SECTION("Min Water Gap Adjustment")
   {
      ARBTRIE_WARN("Min Water Gap Adjustment Start");
      spmc_circular_buffer<int> buf;
      const uint64_t            initial_high = buf.get_high_water_mark();
      const uint64_t            initial_low  = buf.get_low_water_mark();

      // Set a larger minimum gap
      REQUIRE(buf.set_min_water_gap(20));

      // High water mark should have increased to maintain the gap
      REQUIRE(buf.get_high_water_mark() - buf.get_low_water_mark() >= 20);

      // Fill buffer to test the new water marks
      std::atomic<bool> producer_waiting{false};
      std::thread       producer(
          [&]()
          {
             for (int i = 0; i < 64; ++i)
             {
                if (!buf.push(i))
                {
                   producer_waiting = true;
                   break;
                }
             }
          });

      // Wait for producer to potentially block
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      // Verify producer is waiting at the new high water mark
      if (producer_waiting)
      {
         REQUIRE(buf.usage() >= buf.get_high_water_mark());
      }

      // Cleanup
      producer.join();

      // Consume all items
      int value;
      while (buf.try_consume(value))
      {
      }
      ARBTRIE_WARN("Min Water Gap Adjustment End");
   }
   */

   SECTION("Bit Operations")
   {
      ARBTRIE_WARN("Bit Operations Start");
      spmc_circular_buffer<int> buf;

      // Test bitmap operations
      REQUIRE(buf.push(1));
      uint64_t bitmap = buf.get_available_bitmap();
      REQUIRE(bitmap != 0);
      REQUIRE(__builtin_popcountll(bitmap) == 1);  // Should have exactly one bit set

      int value;
      REQUIRE(buf.try_consume(value));
      REQUIRE(buf.get_available_bitmap() == 0);  // All bits should be cleared
      ARBTRIE_WARN("Bit Operations End");
   }

   SECTION("Producer Consumer Synchronization")
   {
      ARBTRIE_WARN("Producer Consumer Synchronization Start");
      spmc_circular_buffer<int> buf;
      std::atomic<bool>         first_producer_done{false};
      std::atomic<bool>         second_producer_done{false};
      std::atomic<bool>         consumer_done{false};
      std::atomic<int>          items_produced{0};
      std::atomic<int>          items_consumed{0};

      // Start producer thread - it will push until blocked
      std::thread producer(
          [&]()
          {
             int count = 0;
             while (count < buf.get_high_water_mark())
             {
                if (!buf.push(count))
                {
                   break;  // Buffer full
                }
                items_produced.fetch_add(1, std::memory_order_relaxed);
                count++;
             }
             ARBTRIE_WARN("First producer done, produced=", items_produced.load());
             first_producer_done = true;
          });

      // Wait for buffer to fill to high water mark
      auto start = std::chrono::steady_clock::now();
      while (buf.usage() < buf.get_high_water_mark())
      {
         if (std::chrono::steady_clock::now() - start > std::chrono::seconds(1))
         {
            FAIL("Timeout waiting for buffer to fill to high water mark");
         }
         std::this_thread::yield();
      }

      // Verify producer is blocked
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      REQUIRE(buf.usage() >= buf.get_high_water_mark());
      REQUIRE_FALSE(first_producer_done);

      // Start consumer thread
      std::thread consumer(
          [&]()
          {
             int  value;
             bool keep_running = true;
             while (keep_running)
             {
                if (buf.try_consume(value))
                {
                   items_consumed.fetch_add(1, std::memory_order_relaxed);
                }
                else if (first_producer_done && second_producer_done &&
                         items_consumed.load() == items_produced.load())
                {
                   // Only exit if both producers are done AND we've consumed everything
                   keep_running = false;
                }
                else
                {
                   std::this_thread::yield();
                }
             }
             consumer_done = true;
          });

      // Wait for buffer to drain to low water mark
      start = std::chrono::steady_clock::now();
      while (buf.usage() > buf.get_low_water_mark())
      {
         if (std::chrono::steady_clock::now() - start > std::chrono::seconds(1))
         {
            ARBTRIE_WARN("Timeout waiting for buffer to drain to low water mark\n",
                         "usage=", buf.usage(), " low_water=", buf.get_low_water_mark());
            FAIL("Timeout waiting for buffer to drain to low water mark");
         }
         std::this_thread::yield();
      }
      ARBTRIE_WARN("Consumer drained to low water mark");

      // Wait for first producer to complete
      start = std::chrono::steady_clock::now();
      while (!first_producer_done)
      {
         if (std::chrono::steady_clock::now() - start > std::chrono::seconds(5))
         {
            FAIL("Timeout waiting for first producer to complete");
         }
         std::this_thread::yield();
      }
      producer.join();

      // Start a new producer thread that should push once without blocking
      std::thread second_producer(
          [&]()
          {
             REQUIRE(buf.push(999));  // Should succeed without blocking
             items_produced.fetch_add(1, std::memory_order_relaxed);
             ARBTRIE_WARN("Second producer done, total produced=", items_produced.load());
             second_producer_done = true;
          });

      // Wait for consumer to complete and verify final state
      start = std::chrono::steady_clock::now();
      while (!consumer_done || items_consumed.load() != items_produced.load())
      {
         if (std::chrono::steady_clock::now() - start > std::chrono::seconds(5))
         {
            ARBTRIE_WARN("Timeout waiting for consumer to complete\n",
                         "consumer_done=", consumer_done.load(),
                         " items_consumed=", items_consumed.load(),
                         " items_produced=", items_produced.load());
            FAIL("Timeout waiting for consumer to complete");
         }
         std::this_thread::yield();
      }

      second_producer.join();
      consumer.join();

      // Verify final state
      REQUIRE(buf.usage() == 0);
      REQUIRE(items_consumed.load() == items_produced.load());
      ARBTRIE_WARN("Producer Consumer Synchronization End");
   }
   SECTION("Producer Consumer Pattern")
   {
      ARBTRIE_WARN("Producer Consumer Pattern Start");
      spmc_circular_buffer<int> buf;
      std::atomic<bool>         stop{false};
      std::atomic<int>          produced{0};
      std::atomic<int>          consumed{0};

      // Producer thread
      std::thread producer(
          [&]()
          {
             int count = 0;
             while (!stop && count < 10000)
             {
                if (buf.push(count))
                {
                   produced.fetch_add(1, std::memory_order_relaxed);
                   count++;
                }
                else
                {
                   std::this_thread::yield();
                }
             }
          });

      // Multiple consumer threads
      constexpr int            num_consumers = 3;
      std::vector<std::thread> consumers;
      for (int i = 0; i < num_consumers; ++i)
      {
         consumers.emplace_back(
             [&]()
             {
                int value;
                while (!stop || buf.usage() > 0)
                {
                   if (buf.try_consume(value))
                   {
                      consumed.fetch_add(1, std::memory_order_relaxed);
                   }
                   else
                   {
                      std::this_thread::yield();
                   }
                }
             });
      }

      // Let the test run for a short time
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      stop = true;

      producer.join();
      for (auto& consumer : consumers)
      {
         consumer.join();
      }

      REQUIRE(produced.load() == consumed.load());
      REQUIRE(buf.usage() == 0);
      ARBTRIE_WARN("Producer Consumer Pattern End");
   }
}