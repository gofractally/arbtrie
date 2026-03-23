#include <algorithm>
#include <arbtrie/find_byte.hpp>
#include <array>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

using namespace arbtrie;

int lower_bound_simple(const uint8_t* arr, uint8_t value, size_t size)
{
   auto sl  = arr;
   auto slp = sl;
   auto sle = slp + size;

   while (slp != sle && uint8_t(*slp) < value)
      ++slp;
   return slp - sl;
}

TEST_CASE("lower_bound_scalar vs std::lower_bound", "[find_byte]")
{
   SECTION("Empty array")
   {
      uint8_t arr[0] = {};
      REQUIRE(lower_bound_scalar(arr, 0, 5) == 0);
      REQUIRE(lower_bound(arr, 0, 5) == 0);
   }

   SECTION("Single element array")
   {
      uint8_t arr[1] = {10};
      // Value less than the element
      REQUIRE(lower_bound_scalar(arr, 5, 1) == 0);
      REQUIRE(lower_bound_neon(arr, 5, 1) == 0);
      REQUIRE(lower_bound(arr, 1, 5) == 0);

      // Value equal to the element
      REQUIRE(lower_bound_scalar(arr, 1, 10) == 0);
      REQUIRE(lower_bound_neon(arr, 1, 10) == 0);
      REQUIRE(lower_bound(arr, 1, 10) == 0);

      // Value greater than the element
      REQUIRE(lower_bound_scalar(arr, 1, 15) == 1);
      REQUIRE(lower_bound_neon(arr, 1, 15) == 1);
      REQUIRE(lower_bound(arr, 1, 15) == 1);
   }

   SECTION("Small arrays (< 8 elements)")
   {
      uint8_t      arr[] = {10, 20, 30, 40, 50, 60, 70};
      const size_t size  = sizeof(arr) / sizeof(arr[0]);

      for (uint8_t value = 0; value <= 80; value += 5)
      {
         auto std_result     = std::lower_bound(arr, arr + size, value) - arr;
         auto scalar_result  = lower_bound_scalar(arr, size, value);
         auto arbtrie_result = lower_bound(arr, size, value);

         REQUIRE(scalar_result == std_result);
         REQUIRE(arbtrie_result == std_result);
      }
   }

   SECTION("Medium arrays (8-16 elements)")
   {
      uint8_t      arr[] = {5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115, 125, 135, 145, 155};
      const size_t size  = sizeof(arr) / sizeof(arr[0]);

      for (uint8_t value = 0; value <= 160; value += 10)
      {
         auto std_result     = std::lower_bound(arr, arr + size, value) - arr;
         auto scalar_result  = lower_bound_scalar(arr, size, value);
         auto neon_result    = lower_bound_neon(arr, size, value);
         auto arbtrie_result = lower_bound(arr, size, value);

         REQUIRE(scalar_result == std_result);
         REQUIRE(neon_result == std_result);
         REQUIRE(arbtrie_result == std_result);
      }
   }

   SECTION("Large arrays (> 16 elements)")
   {
      std::vector<uint8_t> vec;
      for (uint8_t i = 0; i < 200; i += 2)
      {
         vec.push_back(i);
      }

      for (uint8_t value = 0; value <= 200; value += 5)
      {
         auto std_result     = std::lower_bound(vec.begin(), vec.end(), value) - vec.begin();
         auto scalar_result  = lower_bound_scalar(vec.data(), vec.size(), value);
         auto arbtrie_result = lower_bound(vec.data(), vec.size(), value);

         REQUIRE(scalar_result == std_result);
         REQUIRE(arbtrie_result == std_result);
      }
   }

   SECTION("Very large arrays (up to 256 bytes)")
   {
      std::vector<uint8_t> vec;
      // Create a sorted array with all possible uint8_t values (0-255)
      for (int i = 0; i < 256; i++)
      {
         vec.push_back(static_cast<uint8_t>(i));
      }

      // Test with values at various positions
      for (int value : {0, 1, 63, 64, 127, 128, 191, 192, 254, 255})
      {
         auto std_result     = std::lower_bound(vec.begin(), vec.end(), value) - vec.begin();
         auto scalar_result  = lower_bound_scalar(vec.data(), vec.size(), value);
         auto arbtrie_result = lower_bound(vec.data(), vec.size(), value);

         REQUIRE(scalar_result == std_result);
         REQUIRE(arbtrie_result == std_result);
      }
   }

   SECTION("Random arrays of varying sizes")
   {
      std::random_device rd;
      std::mt19937       gen(rd());

      // Test several array sizes
      for (size_t size : {7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256})
      {
         // Create sorted array of random values
         std::vector<uint8_t> vec;
         for (size_t i = 0; i < size; ++i)
         {
            vec.push_back(gen() % 256);
         }
         std::sort(vec.begin(), vec.end());

         // Test with various search values
         for (int i = 0; i < 10; ++i)
         {
            uint8_t value = gen() % 256;

            auto std_result     = std::lower_bound(vec.begin(), vec.end(), value) - vec.begin();
            auto scalar_result  = lower_bound_scalar(vec.data(), vec.size(), value);
            auto arbtrie_result = lower_bound(vec.data(), vec.size(), value);

            REQUIRE(scalar_result == std_result);
            REQUIRE(arbtrie_result == std_result);
         }
      }
   }

   SECTION("Value not found (beyond all elements)")
   {
      uint8_t      arr[] = {10, 20, 30, 40, 50};
      const size_t size  = sizeof(arr) / sizeof(arr[0]);

      // Value greater than all elements should return size
      REQUIRE(lower_bound_scalar(arr, size, 255) == size);
      assert(lower_bound_neon(arr, size, 255) == size);
      REQUIRE(lower_bound_neon(arr, size, 255) == size);
      REQUIRE(lower_bound(arr, size, 255) == size);
   }
}

// Helper function to generate a sorted array of random uint8_t values
std::vector<uint8_t> generate_sorted_random_array(size_t size, std::mt19937& gen)
{
   std::vector<uint8_t> result;
   result.reserve(size);

   for (size_t i = 0; i < size; ++i)
   {
      result.push_back(gen() % 256);
   }

   std::sort(result.begin(), result.end());
   return result;
}

TEST_CASE("Benchmark lower_bound implementations", "[benchmark][find_byte]")
{
   std::random_device rd;
   std::mt19937       gen(rd());

   // Define array sizes to test - include more non-power-of-2 sizes
   const std::array<size_t, 13> sizes = {3, 7, 8, 11, 16, 23, 32, 47, 64, 97, 128, 192, 256};

   if constexpr (false)
      for (size_t size : sizes)
      {
         // Create data arrays with specific size
         auto data = generate_sorted_random_array(size, gen);

         // Create a set of random search values to use in benchmarks
         constexpr size_t                       num_search_values = 100;
         std::array<uint8_t, num_search_values> search_values;
         for (auto& val : search_values)
         {
            val = gen() % 256;
         }

         // For better accuracy, we'll run multiple searches in each benchmark
         auto run_searches = [&](auto search_fn)
         {
            size_t sum = 0;
            for (int rep = 0; rep < 1000; ++rep)
            {
               for (uint8_t val : search_values)
               {
                  sum += search_fn(data.data(), val, data.size());
               }
            }
            return sum;  // Return to prevent optimization
         };

         // Benchmark std::lower_bound
         BENCHMARK("std::lower_bound (size " + std::to_string(size) + ")")
         {
            return run_searches([](const uint8_t* arr, uint8_t val, size_t size)
                                { return std::lower_bound(arr, arr + size, val) - arr; });
         };

         // Benchmark lower_bound_scalar
         BENCHMARK("lower_bound_scalar (size " + std::to_string(size) + ")")
         {
            return run_searches([](const uint8_t* arr, uint8_t val, size_t size)
                                { return lower_bound_scalar(arr, size, val); });
         };

         // Benchmark arbtrie's lower_bound
         BENCHMARK("lower_bound_neon (size " + std::to_string(size) + ")")
         {
            return run_searches([](const uint8_t* arr, uint8_t val, size_t size)
                                { return lower_bound_neon(arr, size, val); });
         };
      }

   // Also include a manual micro-benchmark that runs 100k iterations
   SECTION("Manual micro-benchmark (100k iterations)")
   {
      std::cout << "\nRunning manual micro-benchmark with 100k iterations per array size...\n";
      std::cout << "-------------------------------------------------------------------------------"
                   "--------------------\n";
#if defined(__ARM_NEON)
      std::cout << " Size |   std::lower_bound   |  lower_bound_simple  |  lower_bound_scalar  |  "
                   "lower_bound_neon   | Ratio \n";
      std::cout << "------+----------------------+----------------------+----------------------+---"
                   "------------------+-------\n";
#else
      std::cout << " Size |   std::lower_bound   |  lower_bound_simple  |  lower_bound_scalar  | "
                   "Ratio \n";
      std::cout << "------+----------------------+----------------------+----------------------+---"
                   "----\n";
#endif

      // ANSI color codes
      const char* green = "\033[32m";
      const char* red   = "\033[31m";
      const char* reset = "\033[0m";

      constexpr int num_iterations = 1000000;

      for (size_t size : sizes)
      {
         // Generate random data for this size
         auto data = generate_sorted_random_array(size, gen);

         // Random search value (consistent for all algorithms)
         uint8_t search_value = gen() % 256;

         // Time std::lower_bound - use total time
         auto            start_std  = std::chrono::high_resolution_clock::now();
         volatile size_t result_std = 0;
         for (int i = 0; i < num_iterations; ++i)
         {
            result_std = std::lower_bound(data.begin(), data.end(), search_value) - data.begin();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto us_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Time lower_bound_simple - use total time
         auto            start_simple  = std::chrono::high_resolution_clock::now();
         volatile size_t result_simple = 0;
         for (int i = 0; i < num_iterations; ++i)
         {
            result_simple = lower_bound_simple(data.data(), search_value, data.size());
         }
         auto end_simple = std::chrono::high_resolution_clock::now();
         auto us_simple =
             std::chrono::duration_cast<std::chrono::microseconds>(end_simple - start_simple)
                 .count();

         // Time lower_bound_scalar - use total time
         auto            start_scalar  = std::chrono::high_resolution_clock::now();
         volatile size_t result_scalar = 0;
         for (int i = 0; i < num_iterations; ++i)
         {
            result_scalar = lower_bound_scalar(data.data(), data.size(), search_value);
         }
         auto end_scalar = std::chrono::high_resolution_clock::now();
         auto us_scalar =
             std::chrono::duration_cast<std::chrono::microseconds>(end_scalar - start_scalar)
                 .count();

#if defined(__ARM_NEON)
         // Time lower_bound_neon - use total time
         auto            start_neon  = std::chrono::high_resolution_clock::now();
         volatile size_t result_neon = 0;
         for (int i = 0; i < num_iterations; ++i)
         {
            result_neon = lower_bound_neon(data.data(), data.size(), search_value);
         }
         auto end_neon = std::chrono::high_resolution_clock::now();
         auto us_neon =
             std::chrono::duration_cast<std::chrono::microseconds>(end_neon - start_neon).count();
#endif

         // Calculate ratio for scalar (avoid division by zero)
         double ratio_simple =
             us_std > 0 && us_simple > 0 ? static_cast<double>(us_std) / us_simple : 0.0;
         double ratio_scalar =
             us_std > 0 && us_scalar > 0 ? static_cast<double>(us_std) / us_scalar : 0.0;

#if defined(__ARM_NEON)
         // Calculate ratio for neon (avoid division by zero)
         double ratio_neon =
             us_std > 0 && us_neon > 0 ? static_cast<double>(us_std) / us_neon : 0.0;
#endif

         // Format times with appropriate units (microseconds)
         std::cout << std::setw(5) << size << " | ";
         std::cout << std::setw(8) << us_std << " µs | ";
         std::cout << std::setw(8) << us_simple << " µs | ";
         std::cout << std::setw(8) << us_scalar << " µs | ";

#if defined(__ARM_NEON)
         std::cout << std::setw(8) << us_neon << " µs | ";
#endif

         // Show the speed comparison with color for simple
         std::cout << "simple: ";
         if (ratio_simple > 1.0)
            std::cout << green << std::fixed << std::setprecision(2) << ratio_simple << "x"
                      << reset;
         else if (ratio_simple > 0.0)
            std::cout << red << std::fixed << std::setprecision(2) << ratio_simple << "x" << reset;
         else
            std::cout << "n/a";

         std::cout << ", scalar: ";
         if (ratio_scalar > 1.0)
            std::cout << green << std::fixed << std::setprecision(2) << ratio_scalar << "x"
                      << reset;
         else if (ratio_scalar > 0.0)
            std::cout << red << std::fixed << std::setprecision(2) << ratio_scalar << "x" << reset;
         else
            std::cout << "n/a";

#if defined(__ARM_NEON)
         // Show the speed comparison with color for neon
         std::cout << ", neon: ";
         if (ratio_neon > 1.0)
            std::cout << green << std::fixed << std::setprecision(2) << ratio_neon << "x" << reset;
         else if (ratio_neon > 0.0)
            std::cout << red << std::fixed << std::setprecision(2) << ratio_neon << "x" << reset;
         else
            std::cout << "n/a";
#endif

         std::cout << std::endl;
      }
      std::cout
          << "----------------------------------------------------------------------------------\n";
   }
}