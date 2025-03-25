#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// Include the common header
#include "min_index/min_index.hpp"

// Define colors for output
#define RED_COLOR "\033[1;31m"
#define GREEN_COLOR "\033[1;32m"
#define ORANGE_COLOR "\033[1;33m"
#define BLUE_COLOR "\033[1;34m"
#define PURPLE_COLOR "\033[1;35m"
#define CYAN_COLOR "\033[1;36m"
#define GRAY_COLOR "\033[1;37m"
#define RESET_COLOR "\033[0m"

// For testing purposes, we'll use a 64-byte aligned allocator
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

// Structure to hold benchmark results
struct AlgorithmResult
{
   std::string name;
   double      time_32_ns;
   double      time_64_ns;
   bool        correct_32;
   bool        correct_64;
   bool        supported_32;
   bool        supported_64;
};

// Forward declarations for various algorithm wrappers
int run_scalar_32(uint16_t* counters, int start);
int run_scalar_64(uint16_t* counters, int start);
int run_tournament_32(uint16_t* counters, int start);
int run_tournament_64(uint16_t* counters, int start);
int run_neon_v11_32(uint16_t* counters, int start);
int run_neon_v11_64(uint16_t* counters, int start);
int run_neon_v11a_32(uint16_t* counters, int start);
int run_neon_v11b_32(uint16_t* counters, int start);
int run_neon_v13_32(uint16_t* counters, int start);
int run_neon_v13_64(uint16_t* counters, int start);
int run_neon_v13a_32(uint16_t* counters, int start);
int run_neon_v14_32(uint16_t* counters, int start);
int run_neon_v14_64(uint16_t* counters, int start);
int run_neon_v15_32(uint16_t* counters, int start);
int run_neon_v15_64(uint16_t* counters, int start);
int run_sse41_64(uint16_t* counters, int start);
int run_global_32(uint16_t* counters, int start);
int run_global_64(uint16_t* counters, int start);

// Main benchmark function
void run_benchmarks(int num_iterations)
{
   const size_t   ARRAY_SIZE      = 65536;  // 2^16
   const uint16_t MIN_VALUE       = 1024;   // 2^10
   const uint16_t MAX_VALUE       = 1023;   // 2^10-1
   const uint16_t LARGE_MAX_VALUE = 65535;  // 2^16-1

   // Allocate aligned memory for small values (0 to 2^10-1)
   uint16_t* counters = aligned_alloc<uint16_t>(ARRAY_SIZE, 64);

   // Allocate aligned memory for large values (2^10 to 2^16-1) - only for v11 testing
   uint16_t* large_counters = aligned_alloc<uint16_t>(ARRAY_SIZE, 64);

   // Setup random number generators
   std::random_device                      rd;
   std::mt19937                            gen(rd());
   std::uniform_int_distribution<uint16_t> small_value_dist(0, MAX_VALUE);
   std::uniform_int_distribution<uint16_t> large_value_dist(MIN_VALUE, LARGE_MAX_VALUE);
   std::uniform_int_distribution<size_t>   pos_dist(0, ARRAY_SIZE - 64);

   // Fill arrays with random values
   for (size_t i = 0; i < ARRAY_SIZE; i++)
   {
      counters[i]       = small_value_dist(gen);
      large_counters[i] = large_value_dist(gen);
   }

   // Container for results
   std::vector<AlgorithmResult> results;

   // Lambda for validating algorithm correctness without timing
   auto validate_algorithm =
       [&](const std::string& name, std::function<int(uint16_t*, int)> func_32,
           std::function<int(uint16_t*, int)> func_64, uint16_t* test_array, bool supported_32,
           bool supported_64, bool allow_approximation = false)
   {
      bool correct_32 = true;
      bool correct_64 = true;

      // Test 32-byte version if supported
      if (supported_32)
      {
         for (int i = 0; i < 100; i++)  // Fewer iterations for validation only
         {
            // Pick random 64-byte aligned position
            size_t pos = pos_dist(gen);
            pos        = (pos / 64) * 64;  // Ensure 64-byte alignment

            // Run scalar version for validation
            int      scalar_idx = run_scalar_32(test_array, pos);
            uint16_t scalar_min = test_array[scalar_idx];

            // Run test algorithm
            int      test_idx = func_32(test_array, pos);
            uint16_t test_min = test_array[test_idx];

            // Validate result
            bool is_valid = (test_min == scalar_min);

            // For approximate min testing
            if (!is_valid && allow_approximation && test_min >= 1024)
            {
               // For values >= 1024, check if it's a "close" approximation
               is_valid = (test_min <= scalar_min * 1.1);  // Allow 10% error
            }

            if (!is_valid)
            {
               correct_32            = false;
               std::string test_type = allow_approximation ? "approximate min" : "exact min";
               std::cout << "Validation failed for " << name << " (32, " << test_type << "): "
                         << "Expected min " << scalar_min << " at index " << scalar_idx << ", got "
                         << test_min << " at index " << test_idx << std::endl;
               break;
            }
         }
      }

      // Test 64-byte version if supported
      if (supported_64)
      {
         for (int i = 0; i < 100; i++)  // Fewer iterations for validation only
         {
            // Pick random 64-byte aligned position
            size_t pos = pos_dist(gen);
            pos        = (pos / 64) * 64;  // Ensure 64-byte alignment

            // Run scalar version for validation
            int      scalar_idx = run_scalar_64(test_array, pos);
            uint16_t scalar_min = test_array[scalar_idx];

            // Run test algorithm
            int      test_idx = func_64(test_array, pos);
            uint16_t test_min = test_array[test_idx];

            // Validate result
            bool is_valid = (test_min == scalar_min);

            // For approximate min testing
            if (!is_valid && allow_approximation && test_min >= 1024)
            {
               // For values >= 1024, check if it's a "close" approximation
               is_valid = (test_min <= scalar_min * 1.1);  // Allow 10% error
            }

            if (!is_valid)
            {
               correct_64            = false;
               std::string test_type = allow_approximation ? "approximate min" : "exact min";
               std::cout << "Validation failed for " << name << " (64, " << test_type << "): "
                         << "Expected min " << scalar_min << " at index " << scalar_idx << ", got "
                         << test_min << " at index " << test_idx << std::endl;
               break;
            }
         }
      }

      return std::make_pair(correct_32, correct_64);
   };

   // Lambda for measuring algorithm performance without validation overhead
   auto benchmark_algorithm =
       [&](const std::string& name, std::function<int(uint16_t*, int)> func_32,
           std::function<int(uint16_t*, int)> func_64, uint16_t* test_array, bool supported_32,
           bool supported_64, bool correct_32, bool correct_64)
   {
      double time_32_ns = 0;
      double time_64_ns = 0;

      // Test 32-byte version if supported and correct
      if (supported_32 && correct_32)
      {
         // Prepare positions array to avoid random generation overhead during timing
         std::vector<size_t> positions(num_iterations);
         for (int i = 0; i < num_iterations; i++)
         {
            positions[i] = (pos_dist(gen) / 64) * 64;  // Ensure 64-byte alignment
         }

         // Only time the algorithm execution, not validation
         auto start_time = std::chrono::high_resolution_clock::now();
         for (int i = 0; i < num_iterations; i++)
         {
            // Use pre-generated position
            size_t pos = positions[i];

            // Only run test algorithm, no validation inside timing
            volatile int test_idx = func_32(test_array, pos);
            // volatile prevents the compiler from optimizing out the calculation
         }
         auto end_time = std::chrono::high_resolution_clock::now();
         auto duration =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
         time_32_ns = static_cast<double>(duration) / num_iterations;
      }

      // Test 64-byte version if supported and correct
      if (supported_64 && correct_64)
      {
         // Prepare positions array to avoid random generation overhead during timing
         std::vector<size_t> positions(num_iterations);
         for (int i = 0; i < num_iterations; i++)
         {
            positions[i] = (pos_dist(gen) / 64) * 64;  // Ensure 64-byte alignment
         }

         // Only time the algorithm execution, not validation
         auto start_time = std::chrono::high_resolution_clock::now();
         for (int i = 0; i < num_iterations; i++)
         {
            // Use pre-generated position
            size_t pos = positions[i];

            // Only run test algorithm, no validation inside timing
            volatile int test_idx = func_64(test_array, pos);
            // volatile prevents the compiler from optimizing out the calculation
         }
         auto end_time = std::chrono::high_resolution_clock::now();
         auto duration =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
         time_64_ns = static_cast<double>(duration) / num_iterations;
      }

      // Store results
      results.push_back(
          {name, time_32_ns, time_64_ns, correct_32, correct_64, supported_32, supported_64});
   };

   // First validate all algorithms with small values
   std::cout << "Validating algorithms with small values (0-1023)..." << std::endl;

   // Track validation results
   std::map<std::string, std::pair<bool, bool>> validation_results;

   // Scalar implementations (always correct by definition)
   validation_results["Scalar"] = std::make_pair(true, true);

   // Tournament implementations
   validation_results["Tournament"] =
       validate_algorithm("Tournament", run_tournament_32, run_tournament_64, counters, true, true);

   // NEON implementations (if available)
#ifdef __ARM_NEON
   // Remove v11 family from small values test
   validation_results["NEON v13"] =
       validate_algorithm("NEON v13", run_neon_v13_32, run_neon_v13_64, counters, true, true);
   validation_results["NEON v13a"] =
       validate_algorithm("NEON v13a", run_neon_v13a_32, nullptr, counters, true, false);
   validation_results["NEON v14"] =
       validate_algorithm("NEON v14", run_neon_v14_32, run_neon_v14_64, counters, true, true);
   validation_results["NEON v15"] =
       validate_algorithm("NEON v15", run_neon_v15_32, run_neon_v15_64, counters, true, true);
#endif

   // SSE4.1 implementations (if available)
#ifdef HAS_SSE41
   validation_results["SSE4.1"] =
       validate_algorithm("SSE4.1", nullptr, run_sse41_64, counters, false, true);
#endif

   // Global implementations (should select best available)
   validation_results["Global"] =
       validate_algorithm("Global", run_global_32, run_global_64, counters, true, true);

   std::cout << "Validation complete.\n" << std::endl;

   // Now benchmark all algorithms that passed validation
   // Scalar implementations (baseline)
   benchmark_algorithm("Scalar", run_scalar_32, run_scalar_64, counters, true, true,
                       validation_results["Scalar"].first, validation_results["Scalar"].second);

   // Tournament implementations
   benchmark_algorithm("Tournament", run_tournament_32, run_tournament_64, counters, true, true,
                       validation_results["Tournament"].first,
                       validation_results["Tournament"].second);

   // NEON implementations (if available)
#ifdef __ARM_NEON
   // Remove v11 family from small values test
   benchmark_algorithm("NEON v13", run_neon_v13_32, run_neon_v13_64, counters, true, true,
                       validation_results["NEON v13"].first, validation_results["NEON v13"].second);
   benchmark_algorithm("NEON v13a", run_neon_v13a_32, nullptr, counters, true, false,
                       validation_results["NEON v13a"].first,
                       validation_results["NEON v13a"].second);
   benchmark_algorithm("NEON v14", run_neon_v14_32, run_neon_v14_64, counters, true, true,
                       validation_results["NEON v14"].first, validation_results["NEON v14"].second);
   benchmark_algorithm("NEON v15", run_neon_v15_32, run_neon_v15_64, counters, true, true,
                       validation_results["NEON v15"].first, validation_results["NEON v15"].second);
#endif

   // SSE4.1 implementations (if available)
#ifdef HAS_SSE41
   benchmark_algorithm("SSE4.1", nullptr, run_sse41_64, counters, false, true,
                       validation_results["SSE4.1"].first, validation_results["SSE4.1"].second);
#endif

   // Global implementations (should select best available)
   benchmark_algorithm("Global", run_global_32, run_global_64, counters, true, true,
                       validation_results["Global"].first, validation_results["Global"].second);

   // Find best implementations for coloring
   double      best_time_32 = std::numeric_limits<double>::max();
   double      best_time_64 = std::numeric_limits<double>::max();
   std::string best_algo_32;
   std::string best_algo_64;

   // Track tournament times for speedup comparison
   double tournament_time_32 = 0.0;
   double tournament_time_64 = 0.0;

   for (const auto& result : results)
   {
      if (result.supported_32 && result.correct_32 && result.time_32_ns < best_time_32)
      {
         best_time_32 = result.time_32_ns;
         best_algo_32 = result.name;
      }

      if (result.supported_64 && result.correct_64 && result.time_64_ns < best_time_64)
      {
         best_time_64 = result.time_64_ns;
         best_algo_64 = result.name;
      }

      // Save tournament times for comparison
      if (result.name == "Tournament")
      {
         tournament_time_32 = result.time_32_ns;
         tournament_time_64 = result.time_64_ns;
      }
   }

   // Output results table
   std::cout
       << "\n==================================================================================="
       << std::endl;
   std::cout << "Min Index Algorithms Performance Comparison (Values 0-1023)" << std::endl;
   std::cout
       << "==================================================================================="
       << std::endl;
   std::cout << std::left << std::setw(15) << "Algorithm" << std::setw(15) << "32-byte (ns)"
             << std::setw(15) << "64-byte (ns)" << std::setw(15) << "32-byte (x)" << std::setw(15)
             << "64-byte (x)"
             << "Validation" << std::endl;
   std::cout
       << "==================================================================================="
       << std::endl;

   for (const auto& result : results)
   {
      // Algorithm name
      std::cout << std::left << std::setw(15) << result.name;

      // 32-byte timing (with highlighting for best)
      if (result.supported_32)
      {
         if (result.name == best_algo_32)
         {
            std::cout << GREEN_COLOR;
         }
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << result.time_32_ns;
         if (result.name == best_algo_32)
         {
            std::cout << RESET_COLOR;
         }
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // 64-byte timing (with highlighting for best)
      if (result.supported_64)
      {
         if (result.name == best_algo_64)
         {
            std::cout << GREEN_COLOR;
         }
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << result.time_64_ns;
         if (result.name == best_algo_64)
         {
            std::cout << RESET_COLOR;
         }
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // 32-byte speedup compared to tournament
      if (result.supported_32 && tournament_time_32 > 0)
      {
         double speedup = tournament_time_32 / result.time_32_ns;
         if (speedup > 1.0 && result.name != "Tournament")
         {
            std::cout << GREEN_COLOR;
         }
         else if (speedup < 1.0 && result.name != "Tournament")
         {
            std::cout << RED_COLOR;
         }
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << speedup;
         if ((speedup > 1.0 || speedup < 1.0) && result.name != "Tournament")
         {
            std::cout << RESET_COLOR;
         }
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // 64-byte speedup compared to tournament
      if (result.supported_64 && tournament_time_64 > 0)
      {
         double speedup = tournament_time_64 / result.time_64_ns;
         if (speedup > 1.0 && result.name != "Tournament")
         {
            std::cout << GREEN_COLOR;
         }
         else if (speedup < 1.0 && result.name != "Tournament")
         {
            std::cout << RED_COLOR;
         }
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << speedup;
         if ((speedup > 1.0 || speedup < 1.0) && result.name != "Tournament")
         {
            std::cout << RESET_COLOR;
         }
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // Validation results
      bool pass_32 = !result.supported_32 || result.correct_32;
      bool pass_64 = !result.supported_64 || result.correct_64;

      if (pass_32 && pass_64)
      {
         std::cout << GREEN_COLOR << "PASS" << RESET_COLOR;
      }
      else
      {
         std::cout << RED_COLOR << "FAIL";
         if (!pass_32)
            std::cout << " (32-byte)";
         if (!pass_64)
            std::cout << " (64-byte)";
         std::cout << RESET_COLOR;
      }

      std::cout << std::endl;
   }

   // Clear results for v11 large value testing
   results.clear();

   // Now test only the v11 family with large values (1024-65535)
   std::cout
       << "\n==================================================================================="
       << std::endl;
   std::cout << "V11 with Large Values (1024-65535) - Testing Approximate Min Finding" << std::endl;
   std::cout
       << "==================================================================================="
       << std::endl;

   // Validate v11 family with large values first (allowing approximation)
   std::cout << "Validating v11 algorithms with large values (1024-65535)..." << std::endl;

   // Track validation results for approximate min finding
   std::map<std::string, std::pair<bool, bool>> v11_validation_results;

   // Scalar (always correct by definition)
   v11_validation_results["Scalar"] = std::make_pair(true, true);

   // Tournament (also correct by definition for validation purposes)
   v11_validation_results["Tournament"] = std::make_pair(true, true);

   // Test v11 family
#ifdef __ARM_NEON
   v11_validation_results["NEON v11"] = validate_algorithm(
       "NEON v11", run_neon_v11_32, run_neon_v11_64, large_counters, true, true, true);
   v11_validation_results["NEON v11a"] = validate_algorithm("NEON v11a", run_neon_v11a_32, nullptr,
                                                            large_counters, true, false, true);
   v11_validation_results["NEON v11b"] = validate_algorithm("NEON v11b", run_neon_v11b_32, nullptr,
                                                            large_counters, true, false, true);
   v11_validation_results["NEON v14"]  = validate_algorithm(
       "NEON v14", run_neon_v14_32, run_neon_v14_64, large_counters, true, true, true);
   v11_validation_results["NEON v15"] = validate_algorithm(
       "NEON v15", run_neon_v15_32, run_neon_v15_64, large_counters, true, true, true);
#endif

   std::cout << "Validation complete.\n" << std::endl;

   // Benchmark v11 with large values
   benchmark_algorithm("Scalar", run_scalar_32, run_scalar_64, large_counters, true, true,
                       v11_validation_results["Scalar"].first,
                       v11_validation_results["Scalar"].second);

   // Add Tournament benchmark for comparison
   benchmark_algorithm("Tournament", run_tournament_32, run_tournament_64, large_counters, true,
                       true, v11_validation_results["Tournament"].first,
                       v11_validation_results["Tournament"].second);

#ifdef __ARM_NEON
   benchmark_algorithm("NEON v11", run_neon_v11_32, run_neon_v11_64, large_counters, true, true,
                       v11_validation_results["NEON v11"].first,
                       v11_validation_results["NEON v11"].second);
   benchmark_algorithm("NEON v11a", run_neon_v11a_32, nullptr, large_counters, true, false,
                       v11_validation_results["NEON v11a"].first,
                       v11_validation_results["NEON v11a"].second);
   benchmark_algorithm("NEON v11b", run_neon_v11b_32, nullptr, large_counters, true, false,
                       v11_validation_results["NEON v11b"].first,
                       v11_validation_results["NEON v11b"].second);
   benchmark_algorithm("NEON v14", run_neon_v14_32, run_neon_v14_64, large_counters, true, true,
                       v11_validation_results["NEON v14"].first,
                       v11_validation_results["NEON v14"].second);
   benchmark_algorithm("NEON v15", run_neon_v15_32, run_neon_v15_64, large_counters, true, true,
                       v11_validation_results["NEON v15"].first,
                       v11_validation_results["NEON v15"].second);
#endif

   // Get tournament performance as baseline instead of scalar
   double tournament_time_32_large = 0;
   double tournament_time_64_large = 0;
   for (const auto& result : results)
   {
      if (result.name == "Tournament")
      {
         tournament_time_32_large = result.time_32_ns;
         tournament_time_64_large = result.time_64_ns;
         break;
      }
   }

   // Output results table for v11 with large values
   std::cout << std::left << std::setw(15) << "Algorithm" << std::setw(15) << "32-byte (ns)"
             << std::setw(15) << "64-byte (ns)" << std::setw(15) << "32-byte (x)" << std::setw(15)
             << "64-byte (x)"
             << "Validation" << std::endl;
   std::cout
       << "==================================================================================="
       << std::endl;

   for (const auto& result : results)
   {
      // Algorithm name
      std::cout << std::left << std::setw(15) << result.name;

      // 32-byte timing
      if (result.supported_32)
      {
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << result.time_32_ns;
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // 64-byte timing
      if (result.supported_64)
      {
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << result.time_64_ns;
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // 32-byte performance vs tournament
      if (result.supported_32 && tournament_time_32_large > 0 && result.name != "Tournament")
      {
         double speedup = tournament_time_32_large / result.time_32_ns;
         if (speedup > 1.0)
         {
            std::cout << GREEN_COLOR;
         }
         else if (speedup < 1.0)
         {
            std::cout << RED_COLOR;
         }
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << speedup;
         if (speedup != 1.0)
         {
            std::cout << RESET_COLOR;
         }
      }
      else if (result.name == "Tournament")
      {
         // Tournament vs itself is always 1.0x
         std::cout << std::setw(15) << "1.00";
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // 64-byte performance vs tournament
      if (result.supported_64 && tournament_time_64_large > 0 && result.name != "Tournament")
      {
         double speedup = tournament_time_64_large / result.time_64_ns;
         if (speedup > 1.0)
         {
            std::cout << GREEN_COLOR;
         }
         else if (speedup < 1.0)
         {
            std::cout << RED_COLOR;
         }
         std::cout << std::setw(15) << std::fixed << std::setprecision(2) << speedup;
         if (speedup != 1.0)
         {
            std::cout << RESET_COLOR;
         }
      }
      else if (result.name == "Tournament" && result.supported_64)
      {
         // Tournament vs itself is always 1.0x
         std::cout << std::setw(15) << "1.00";
      }
      else
      {
         std::cout << std::setw(15) << "N/A";
      }

      // Validation results
      bool pass_32 = !result.supported_32 || result.correct_32;
      bool pass_64 = !result.supported_64 || result.correct_64;

      if (pass_32 && pass_64)
      {
         std::cout << GREEN_COLOR << "PASS" << RESET_COLOR;
      }
      else
      {
         std::cout << RED_COLOR << "FAIL";
         if (!pass_32)
            std::cout << " (32-byte)";
         if (!pass_64)
            std::cout << " (64-byte)";
         std::cout << RESET_COLOR;
      }

      std::cout << std::endl;
   }

   // Free allocated memory
   aligned_free(counters);
   aligned_free(large_counters);
}

// Actually just wraps the functions from the header
int run_scalar_32(uint16_t* counters, int start)
{
   return find_approx_min_index_scalar_32(counters, start);
}

int run_scalar_64(uint16_t* counters, int start)
{
   return find_approx_min_index_scalar_64(counters, start);
}

int run_tournament_32(uint16_t* counters, int start)
{
   return find_approx_min_index_tournament_32(counters, start);
}

int run_tournament_64(uint16_t* counters, int start)
{
   return find_approx_min_index_tournament_64(counters, start);
}

int run_neon_v11_32(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v11_32(counters, start);
}

int run_neon_v11_64(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v11_64(counters, start);
}

int run_neon_v11a_32(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v11a_32(counters, start);
}

int run_neon_v11b_32(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v11b_32(counters, start);
}

int run_neon_v13_32(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v13_32(counters, start);
}

int run_neon_v13_64(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v13_64(counters, start);
}

int run_neon_v13a_32(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v13a_32(counters, start);
}

int run_neon_v14_32(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v14_32(counters, start);
}

int run_neon_v14_64(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v14_64(counters, start);
}

int run_neon_v15_32(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v15_32(counters, start);
}

int run_neon_v15_64(uint16_t* counters, int start)
{
   return find_approx_min_index_neon_v15_64(counters, start);
}

int run_sse41_64(uint16_t* counters, int start)
{
   // Fallback to scalar implementation since SSE4.1 version isn't implemented yet
   return find_approx_min_index_scalar_64(counters, start);
}

int run_global_32(uint16_t* counters, int start)
{
   return find_approx_min_index_32(counters, start);
}

int run_global_64(uint16_t* counters, int start)
{
   return find_approx_min_index_64(counters, start);
}

int main(int argc, char* argv[])
{
   int num_iterations = 10000;

   if (argc > 1)
   {
      num_iterations = std::atoi(argv[1]);
   }

   std::cout << "Running benchmark with " << num_iterations << " iterations..." << std::endl;
   run_benchmarks(num_iterations);

   return 0;
}