#include <arbtrie/padded_atomic.hpp>
#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <vector>

using namespace arbtrie;

TEST_CASE("padded_atomic<uint64_t> basic operations", "[atomic]")
{
   padded_atomic<uint64_t> atom{0};

   SECTION("load and store")
   {
      atom.store(0x123456789ABCDEF0ULL);
      REQUIRE(atom.load() == 0x123456789ABCDEF0ULL);
   }

   SECTION("fetch_add")
   {
      atom.store(10);
      REQUIRE(atom.fetch_add(5) == 10);
      REQUIRE(atom.load() == 15);
   }
}

TEST_CASE("padded_atomic<uint64_t> bit manipulation", "[atomic]")
{
   padded_atomic<uint64_t> atom{0};

   SECTION("set_high_bits")
   {
      // Set high bits to a value
      atom.store(0x00000000FFFFFFFFULL);
      atom.set_high_bits(0x12345678);
      // Pre-compute the expected value instead of using | inside REQUIRE
      const uint64_t expected1 = 0x1234567800000000ULL | 0xFFFFFFFFULL;
      REQUIRE(atom.load() == expected1);

      // Change high bits while preserving low bits
      atom.store(0xAAAAAAAA55555555ULL);
      atom.set_high_bits(0xBBBBBBBB);
      REQUIRE(atom.load() == 0xBBBBBBBB55555555ULL);

      // Set high bits to 0
      atom.set_high_bits(0);
      REQUIRE(atom.load() == 0x0000000055555555ULL);

      // Set high bits to max value
      atom.set_high_bits(0xFFFFFFFF);
      REQUIRE(atom.load() == 0xFFFFFFFF55555555ULL);
   }

   SECTION("set_low_bits")
   {
      // Set low bits to a value
      atom.store(0xFFFFFFFF00000000ULL);
      atom.set_low_bits(0x12345678);
      REQUIRE(atom.load() == 0xFFFFFFFF12345678ULL);

      // Change low bits while preserving high bits
      atom.store(0x5555555566666666ULL);
      atom.set_low_bits(0xAAAAAAAA);
      REQUIRE(atom.load() == 0x55555555AAAAAAAAULL);

      // Set low bits to 0
      atom.set_low_bits(0);
      REQUIRE(atom.load() == 0x5555555500000000ULL);

      // Set low bits to max value (this is the case that was failing)
      atom.store(0x5555555500000000ULL);
      atom.set_low_bits(0xFFFFFFFF);
      REQUIRE(atom.load() == 0x55555555FFFFFFFFULL);
   }
}

// This test simulates the actual usage pattern in session_rlock
TEST_CASE("padded_atomic<uint64_t> simulated session_rlock", "[atomic][simulation]")
{
   padded_atomic<uint64_t> lock_ptr{
       static_cast<uint64_t>(-1)};  // Initialized like in session_rlock

   SECTION("basic lock/unlock cycle")
   {
      // Initially it should be all 1s
      REQUIRE(lock_ptr.load() == static_cast<uint64_t>(-1));
      REQUIRE(lock_ptr.load() == 0xFFFFFFFFFFFFFFFFULL);

      // Set high bits to a value (simulating update())
      uint32_t high_value = 0x12345678;
      lock_ptr.set_high_bits(high_value);

      // Verify high bits are set correctly
      REQUIRE((lock_ptr.load() >> 32) == high_value);
      REQUIRE((lock_ptr.load() & 0xFFFFFFFFULL) == 0xFFFFFFFFULL);

      // Lock by copying high bits to low bits (simulating lock())
      uint32_t high_bits = lock_ptr.load() >> 32;
      lock_ptr.set_low_bits(high_bits);

      // Verify the low bits now match the high bits
      REQUIRE((lock_ptr.load() & 0xFFFFFFFFULL) == high_value);
      REQUIRE((lock_ptr.load() >> 32) == high_value);

      // Unlock by setting low bits to all 1s (simulating unlock())
      lock_ptr.set_low_bits(0xFFFFFFFF);

      // Verify high bits unchanged and low bits all 1s
      REQUIRE((lock_ptr.load() >> 32) == high_value);
      REQUIRE((lock_ptr.load() & 0xFFFFFFFFULL) == 0xFFFFFFFFULL);
   }
}

// This test verifies concurrent access with two threads
TEST_CASE("padded_atomic<uint64_t> concurrent access", "[atomic][concurrent]")
{
   padded_atomic<uint64_t> atom{0};

   // Initialize to a known state
   atom.store(0x1111111122222222ULL);

   std::vector<std::thread> threads;

   // Thread 1 repeatedly modifies high bits
   threads.emplace_back(
       [&]()
       {
          for (int i = 0; i < 10000; i++)
          {
             atom.set_high_bits(0x33333333);
             atom.set_high_bits(0x44444444);
             atom.set_high_bits(0x55555555);
             atom.set_high_bits(0x11111111);
          }
       });

   // Thread 2 repeatedly modifies low bits
   threads.emplace_back(
       [&]()
       {
          for (int i = 0; i < 10000; i++)
          {
             atom.set_low_bits(0x66666666);
             atom.set_low_bits(0x77777777);
             atom.set_low_bits(0x88888888);
             atom.set_low_bits(0x22222222);
          }
       });

   // Join threads
   for (auto& t : threads)
   {
      t.join();
   }

   // Final value should have high bits from thread 1 and low bits from thread 2
   uint64_t final_value = atom.load();
   uint32_t final_high  = (final_value >> 32);
   uint32_t final_low   = (final_value & 0xFFFFFFFFULL);

   // The final values should be one of the values set by each respective thread
   bool valid_high = (final_high == 0x11111111 || final_high == 0x33333333 ||
                      final_high == 0x44444444 || final_high == 0x55555555);

   bool valid_low = (final_low == 0x22222222 || final_low == 0x66666666 ||
                     final_low == 0x77777777 || final_low == 0x88888888);

   REQUIRE(valid_high);
   REQUIRE(valid_low);
}