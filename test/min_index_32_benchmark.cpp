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

// For testing purposes, we'll use a 16-byte aligned allocator
template <typename T>
T* aligned_alloc(size_t count, size_t alignment = 16)
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

// Use a function type for the callback
using ResultCallback = std::function<void(const char*, double, double, bool)>;

// Benchmark function for 32-value implementations
void benchmark_32(int num_iterations, int data_size, const ResultCallback& save_run_results)
{
   std::random_device rd;
   std::mt19937       gen(rd());
   // Limit random values to be less than 2^10 (1024) to work with the v13 implementation
   std::uniform_int_distribution<> dist(1, 1023);  // Avoid 0 and use values < 2^10

   uint16_t* counters = aligned_alloc<uint16_t>(data_size, 16);

   // Fill with random data
   for (int i = 0; i < data_size; i++)
      counters[i] = dist(gen);

   // Define start position
   int start = 0;

   // Validate all implementations against the scalar implementation
   int      scalar_min_idx   = find_approx_min_index_scalar_32(counters, start);
   uint16_t scalar_min_value = counters[scalar_min_idx];

   auto validate_result = [&](const char* name, auto func) -> bool
   {
      int      min_idx   = func(counters, start);
      uint16_t min_value = counters[min_idx];

      // Consider any index with the same minimum value as correct
      bool is_correct = (min_value == scalar_min_value);

      if (!is_correct)
      {
         std::cout << ORANGE_COLOR << "WARNING: " << name << " found different minimum: "
                   << "0x" << std::hex << min_value << ", "
                   << "reference found: 0x" << scalar_min_value << std::dec << RESET_COLOR
                   << std::endl;
      }

      return is_correct;
   };

   struct BenchmarkResult
   {
      std::string name;
      double      time_ns;
      double      speedup_vs_scalar;
      bool        correct_min;
   };

   std::vector<BenchmarkResult> results;

   auto benchmark_impl = [&](const char* name, auto func) -> double
   {
      bool correct_min = validate_result(name, func);

      // Warm-up run
      for (int i = 0; i < 1000; i++)
         volatile int result = func(counters, start);

      // Actual benchmark
      auto start_time = std::chrono::high_resolution_clock::now();

      for (int i = 0; i < num_iterations; i++)
      {
         volatile int result = func(counters, start);
      }

      auto end_time = std::chrono::high_resolution_clock::now();

      // Calculate time in nanoseconds for better precision
      auto duration_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();

      // Calculate time per iteration in nanoseconds
      double time_per_call_ns = static_cast<double>(duration_ns) / num_iterations;

      results.push_back({name, time_per_call_ns, 0.0, correct_min});
      return time_per_call_ns;
   };

   // Benchmark scalar implementation first to use as baseline
   double scalar_time = benchmark_impl("scalar_32", find_approx_min_index_scalar_32);

   // Add tournament implementation benchmark
   benchmark_impl("tournament_32", find_approx_min_index_tournament_32);

   // Now benchmark all other implementations
#if defined(__ARM_NEON)
   benchmark_impl("neon_v11_32", find_approx_min_index_neon_v11_32);
   benchmark_impl("neon_v11b_32", find_approx_min_index_neon_v11b_32);
   benchmark_impl("neon_v13_32", find_approx_min_index_neon_v13_32);
#endif

   // Benchmark the global implementation
   benchmark_impl("global_32", find_approx_min_index_32);

   // Benchmark v11a implementation
   benchmark_impl("neon_v11a_32", find_approx_min_index_neon_v11a_32);

   // Benchmark v13a implementation
   benchmark_impl("neon_v13a_32", find_approx_min_index_neon_v13a_32);

   // Calculate speedups
   for (auto& result : results)
      result.speedup_vs_scalar = scalar_time / result.time_ns;

   // Free memory
   aligned_free(counters);

   // Save results for this run
   for (const auto& result : results)
   {
      save_run_results(result.name.c_str(), result.time_ns, result.speedup_vs_scalar,
                       result.correct_min);
   }
}

int main()
{
   const int NUM_RUNS       = 5;         // Number of benchmark runs
   const int NUM_ITERATIONS = 10000000;  // 10M iterations for more reliable timing

   std::cout << CYAN_COLOR << "Running " << NUM_RUNS << " benchmarks with " << NUM_ITERATIONS
             << " iterations each..." << RESET_COLOR << std::endl;

   // Store the best result for each implementation
   struct BenchmarkResult
   {
      std::string name;
      double      time_ns;
      double      speedup_vs_scalar;
      bool        correct_min;
   };

   std::vector<BenchmarkResult>  best_results;
   std::map<std::string, double> best_times;
   double                        best_scalar_time = std::numeric_limits<double>::max();

   // Run multiple benchmarks and track best results
   for (int run = 0; run < NUM_RUNS; run++)
   {
      std::cout << "." << std::flush;  // Simple progress indicator

      // Create a fresh vector for this run's results
      std::vector<BenchmarkResult> run_results;

      // Temporary lambda to capture this run's results
      auto save_run_results =
          [&run_results](const char* name, double time_ns, double speedup, bool correct)
      { run_results.push_back({name, time_ns, speedup, correct}); };

      // Run the benchmark with the temporary capture
      benchmark_32(NUM_ITERATIONS, 32, save_run_results);

      // Process this run's results
      double scalar_time = 0;
      for (const auto& result : run_results)
      {
         if (result.name == "scalar_32")
         {
            scalar_time = result.time_ns;
            if (scalar_time < best_scalar_time)
            {
               best_scalar_time = scalar_time;
            }
            break;
         }
      }

      // Update best times for each implementation
      for (const auto& result : run_results)
      {
         // Store best time for each implementation
         if (best_times.find(result.name) == best_times.end() ||
             result.time_ns < best_times[result.name])
         {
            best_times[result.name] = result.time_ns;
         }
      }
   }

   std::cout << " Done!" << std::endl;

   // Create final result set with best times and recalculated speedups
   std::vector<BenchmarkResult> final_results;
   for (const auto& [name, time] : best_times)
   {
      double speedup = best_scalar_time / time;
      // Assume all implementations are correct (validated during runs)
      final_results.push_back({name, time, speedup, true});
   }

   // Sort results by speedup (descending)
   std::sort(final_results.begin(), final_results.end(),
             [](const BenchmarkResult& a, const BenchmarkResult& b)
             { return a.speedup_vs_scalar > b.speedup_vs_scalar; });

   // Print the final results
   std::cout << "\n"
             << CYAN_COLOR << "Best Results Across " << NUM_RUNS << " Runs (" << NUM_ITERATIONS
             << " iterations each):" << RESET_COLOR << std::endl;

   std::cout << std::left << std::setw(20) << "Implementation" << std::right << std::setw(15)
             << "Best Time (ns)" << std::setw(15) << "Speedup vs Scalar" << std::endl;

   std::cout << "--------------------------------------------------------" << std::endl;

   for (const auto& result : final_results)
   {
      // Highlight the winner in green
      if (&result == &final_results[0])
         std::cout << GREEN_COLOR;

      std::cout << std::left << std::setw(20) << result.name << std::right << std::fixed
                << std::setprecision(2) << std::setw(15) << result.time_ns << std::setprecision(3)
                << std::setw(15) << result.speedup_vs_scalar;

      // Reset color
      if (&result == &final_results[0])
         std::cout << RESET_COLOR;

      std::cout << std::endl;
   }

   return 0;
}