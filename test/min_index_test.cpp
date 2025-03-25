#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

// Include the header file that contains our min_index implementations
#include "min_index/min_index.hpp"

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

// Scalar implementation for validation
int scalar_min_index_32(uint16_t* counters, int start)
{
   int      min_idx = start;
   uint16_t min_val = counters[start];

   for (int i = start + 1; i < start + 32; i++)
   {
      if (counters[i] < min_val)
      {
         min_val = counters[i];
         min_idx = i;
      }
   }

   return min_idx;
}

// Scalar implementation for validation
int scalar_min_index_64(uint16_t* counters, int start)
{
   int      min_idx = start;
   uint16_t min_val = counters[start];

   for (int i = start + 1; i < start + 64; i++)
   {
      if (counters[i] < min_val)
      {
         min_val = counters[i];
         min_idx = i;
      }
   }

   return min_idx;
}

int main(int argc, char* argv[])
{
   // Constants for array sizes and value ranges
   const size_t   ARRAY_SIZE      = 65536;
   const uint16_t SMALL_MAX_VALUE = 1023;  // Small values: 0-1023
   const uint16_t LARGE_MIN_VALUE = 1024;  // Large values: 1024-65535
   const uint16_t LARGE_MAX_VALUE = 65535;
   const int      NUM_ITERATIONS  = 1000;  // Number of benchmark iterations

   // Allocate aligned memory for test arrays
   uint16_t* small_values = aligned_alloc<uint16_t>(ARRAY_SIZE, 128);
   uint16_t* large_values = aligned_alloc<uint16_t>(ARRAY_SIZE, 128);

   // Initialize random number generator
   std::random_device                      rd;
   std::mt19937                            gen(rd());
   std::uniform_int_distribution<uint16_t> small_dist(0, SMALL_MAX_VALUE);
   std::uniform_int_distribution<uint16_t> large_dist(LARGE_MIN_VALUE, LARGE_MAX_VALUE);
   std::uniform_int_distribution<size_t>   pos_dist(0, ARRAY_SIZE - 64);

   // Fill arrays with random values
   for (size_t i = 0; i < ARRAY_SIZE; i++)
   {
      small_values[i] = small_dist(gen);
      large_values[i] = large_dist(gen);
   }

   // Helper function for validation and benchmarking
   auto test_algorithm = [&](const std::string& name, std::string value_type, uint16_t* values,
                             int (*func_32)(uint16_t*, int), int (*func_64)(uint16_t*, int),
                             int (*ref_32)(uint16_t*, int), int (*ref_64)(uint16_t*, int))
   {
      std::cout << "==== Testing " << name << " with " << value_type << " values ====" << std::endl;

      // Validate 32-bit version
      bool correct_32 = true;
      for (int i = 0; i < 100; i++)
      {
         size_t pos = (pos_dist(gen) / 64) * 64;  // Ensure 64-byte alignment

         int      ref_idx = ref_32(values, pos);
         uint16_t ref_min = values[ref_idx];

         int      test_idx = func_32(values, pos);
         uint16_t test_min = values[test_idx];

         if (test_min != ref_min)
         {
            correct_32 = false;
            std::cout << "Validation failed for 32-bit: Expected min " << ref_min << " at index "
                      << ref_idx << ", got " << test_min << " at index " << test_idx << std::endl;
            break;
         }
      }

      // Validate 64-bit version
      bool correct_64 = true;
      for (int i = 0; i < 100; i++)
      {
         size_t pos = (pos_dist(gen) / 64) * 64;  // Ensure 64-byte alignment

         int      ref_idx = ref_64(values, pos);
         uint16_t ref_min = values[ref_idx];

         int      test_idx = func_64(values, pos);
         uint16_t test_min = values[test_idx];

         if (test_min != ref_min)
         {
            correct_64 = false;
            std::cout << "Validation failed for 64-bit: Expected min " << ref_min << " at index "
                      << ref_idx << ", got " << test_min << " at index " << test_idx << std::endl;
            break;
         }
      }

      // Benchmark 32-bit version
      std::vector<size_t> positions(NUM_ITERATIONS);
      for (int i = 0; i < NUM_ITERATIONS; i++)
      {
         positions[i] = (pos_dist(gen) / 64) * 64;  // Ensure 64-byte alignment
      }

      auto start_time = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < NUM_ITERATIONS; i++)
      {
         volatile int result = func_32(values, positions[i]);
      }
      auto end_time = std::chrono::high_resolution_clock::now();
      auto duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
      double time_32_ns = static_cast<double>(duration) / NUM_ITERATIONS;

      // Benchmark 64-bit version
      start_time = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < NUM_ITERATIONS; i++)
      {
         volatile int result = func_64(values, positions[i]);
      }
      end_time = std::chrono::high_resolution_clock::now();
      duration =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
      double time_64_ns = static_cast<double>(duration) / NUM_ITERATIONS;

      // Print results
      std::cout << std::left << std::setw(15) << "Version" << std::setw(15) << "Time (ns)"
                << "Validation" << std::endl;
      std::cout << std::string(45, '-') << std::endl;

      std::cout << std::left << std::setw(15) << "32-bit" << std::setw(15) << std::fixed
                << std::setprecision(2) << time_32_ns << (correct_32 ? "PASSED" : "FAILED")
                << std::endl;

      std::cout << std::left << std::setw(15) << "64-bit" << std::setw(15) << std::fixed
                << std::setprecision(2) << time_64_ns << (correct_64 ? "PASSED" : "FAILED")
                << std::endl;

      std::cout << std::endl;
   };

   // Test NEON v15 with small values
   test_algorithm("NEON v15", "small", small_values, find_approx_min_index_neon_v15_32,
                  find_approx_min_index_neon_v15_64, scalar_min_index_32, scalar_min_index_64);

   // Test NEON v15 with large values
   test_algorithm("NEON v15", "large", large_values, find_approx_min_index_neon_v15_32,
                  find_approx_min_index_neon_v15_64, scalar_min_index_32, scalar_min_index_64);

   // Clean up
   aligned_free(small_values);
   aligned_free(large_values);

   return 0;
}