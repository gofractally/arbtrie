#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <iostream>
#include <random>

#include <sal/min_index.hpp>

// For aligned memory allocation
template <typename T>
T* aligned_alloc(size_t count, size_t alignment = 64)
{
#if defined(_MSC_VER)
   void* ptr = _aligned_malloc(count * sizeof(T), alignment);
#else
   void* ptr = nullptr;
   if (posix_memalign(&ptr, alignment, count * sizeof(T)) != 0)
      return nullptr;
#endif
   return static_cast<T*>(ptr);
}

// Free aligned memory
template <typename T>
void aligned_free(T* ptr)
{
#if defined(_MSC_VER)
   _aligned_free(ptr);
#else
   free(ptr);
#endif
}

// Simple reference implementation for validation
uint32_t find_min_index_reference(const uint16_t* values, size_t count)
{
   uint16_t min_val = values[0];
   uint32_t min_idx = 0;

   for (size_t i = 1; i < count; i++)
   {
      if (values[i] < min_val)
      {
         min_val = values[i];
         min_idx = i;
      }
   }

   return min_idx;
}

TEST_CASE("Find minimum index in 32-element arrays", "[sal][min_index_32]")
{
   // Allocate aligned memory for test arrays
   uint16_t* values = aligned_alloc<uint16_t>(32, 128);

   SECTION("Known minimum at specific positions")
   {
      // Fill with increasing values
      for (int i = 0; i < 32; i++)
      {
         values[i] = 1000 + i;
      }

      SECTION("Minimum at beginning")
      {
         values[0] = 500;
         REQUIRE(sal::find_min_index_32(values) == 0);
      }

      SECTION("Minimum in middle")
      {
         values[15] = 500;
         REQUIRE(sal::find_min_index_32(values) == 15);
      }

      SECTION("Minimum at end")
      {
         values[31] = 500;
         REQUIRE(sal::find_min_index_32(values) == 31);
      }
   }

   SECTION("With random values")
   {
      std::random_device                      rd;
      std::mt19937                            gen(rd());
      std::uniform_int_distribution<uint16_t> dist(1, 65535);

      // Test multiple random arrays
      for (int test = 0; test < 10; test++)
      {
         // Fill with random values
         for (int i = 0; i < 32; i++)
         {
            values[i] = dist(gen);
         }

         // Find the expected minimum index using reference implementation
         uint32_t expected_idx = find_min_index_reference(values, 32);
         uint16_t expected_min = values[expected_idx];

         // Find using our implementation
         uint32_t actual_idx = sal::find_min_index_32(values);
         uint16_t actual_min = values[actual_idx];

         INFO("Test " << test << ": values[" << expected_idx << "] = " << expected_min
                      << ", values[" << actual_idx << "] = " << actual_min);

         // The found minimum value should be the same
         REQUIRE(actual_min == expected_min);
      }
   }

   SECTION("With duplicate minimum values")
   {
      // Fill with same value
      for (int i = 0; i < 32; i++)
      {
         values[i] = 1000;
      }

      // Place multiple minimum values
      values[5]  = 500;
      values[25] = 500;

      // The implementation should return one of the minimum indices
      uint32_t result = sal::find_min_index_32(values);
      REQUIRE(((result == 5) || (result == 25)));
   }

   // Clean up
   aligned_free(values);
}

TEST_CASE("Find minimum index in 64-element arrays", "[sal][min_index_64]")
{
   // Allocate aligned memory for test arrays
   uint16_t* values = aligned_alloc<uint16_t>(64, 128);

   SECTION("Known minimum at specific positions")
   {
      // Fill with increasing values
      for (int i = 0; i < 64; i++)
      {
         values[i] = 2000 + i;
      }

      SECTION("Minimum at beginning")
      {
         values[0] = 500;
         REQUIRE(sal::find_min_index_64(values) == 0);
      }

      SECTION("Minimum in first half")
      {
         values[15] = 500;
         REQUIRE(sal::find_min_index_64(values) == 15);
      }

      SECTION("Minimum in second half")
      {
         values[48] = 500;
         REQUIRE(sal::find_min_index_64(values) == 48);
      }

      SECTION("Minimum at end")
      {
         values[63] = 500;
         REQUIRE(sal::find_min_index_64(values) == 63);
      }
   }

   SECTION("With random values")
   {
      std::random_device                      rd;
      std::mt19937                            gen(rd());
      std::uniform_int_distribution<uint16_t> dist(1, 65535);

      // Test multiple random arrays
      for (int test = 0; test < 10; test++)
      {
         // Fill with random values
         for (int i = 0; i < 64; i++)
         {
            values[i] = dist(gen);
         }

         // Find the expected minimum index using reference implementation
         uint32_t expected_idx = find_min_index_reference(values, 64);
         uint16_t expected_min = values[expected_idx];

         // Find using our implementation
         uint32_t actual_idx = sal::find_min_index_64(values);
         uint16_t actual_min = values[actual_idx];

         INFO("Test " << test << ": values[" << expected_idx << "] = " << expected_min
                      << ", values[" << actual_idx << "] = " << actual_min);

         // The found minimum value should be the same
         REQUIRE(actual_min == expected_min);
      }
   }

   SECTION("With duplicate minimum values")
   {
      // Fill with same value
      for (int i = 0; i < 64; i++)
      {
         values[i] = 1000;
      }

      // Place multiple minimum values
      values[10] = 500;
      values[50] = 500;

      // The implementation should return one of the minimum indices
      uint32_t result = sal::find_min_index_64(values);
      REQUIRE(((result == 10) || (result == 50)));
   }

   SECTION("Edge case: all values equal")
   {
      // Fill with same value
      for (int i = 0; i < 64; i++)
      {
         values[i] = 1000;
      }

      // The implementation should return some index (typically the first)
      uint32_t result = sal::find_min_index_64(values);
      REQUIRE(result >= 0);
      REQUIRE(result < 64);
   }

   // Clean up
   aligned_free(values);
}