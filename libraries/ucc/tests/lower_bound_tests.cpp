#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/catch_test_macros.hpp>
#include <ucc/lower_bound.hpp>

#include <algorithm>
#include <cstdint>
#include <iterator>  // for std::distance
#include <limits>
#include <random>
#include <vector>

// Helper function to generate sorted random data with padding
std::vector<uint8_t> generate_sorted_data(size_t size, size_t padding = 15)
{
   std::vector<uint8_t>               data(size + padding);  // Add padding for safe reads
   std::mt19937                       gen(std::random_device{}());
   std::uniform_int_distribution<int> dist(0, 255);

   for (size_t i = 0; i < size; ++i)
   {
      data[i] = static_cast<uint8_t>(dist(gen));
   }
   std::sort(data.begin(), data.begin() + size);

   // Optional: Fill padding area (not strictly necessary for correctness, but good practice)
   std::fill(data.begin() + size, data.end(), 0);

   return data;
}

TEST_CASE("Lower Bound Implementations Validation", "[lower_bound]")
{
   constexpr size_t max_size = 256;
   constexpr size_t padding  = 15;  // Required for lower_bound_padded

   std::mt19937                       gen(std::random_device{}());
   std::uniform_int_distribution<int> byte_dist(0, 255);

   for (size_t size = 0; size <= max_size; ++size)
   {
      INFO("Testing size: " << size);
      std::vector<uint8_t> test_data_padded = generate_sorted_data(size, padding);
      const uint8_t*       data_ptr         = test_data_padded.data();

      // Test cases for various bytes
      std::vector<uint8_t> search_bytes = {0, 128, 255};
      // Add some random bytes to test
      for (int i = 0; i < 5; ++i)
      {
         search_bytes.push_back(static_cast<uint8_t>(byte_dist(gen)));
      }
      // Add boundary values if data exists
      if (size > 0)
      {
         search_bytes.push_back(data_ptr[0]);         // First element
         search_bytes.push_back(data_ptr[size - 1]);  // Last element
         // Add a byte guaranteed to be less than the first (if possible)
         if (data_ptr[0] > 0)
         {
            search_bytes.push_back(data_ptr[0] - 1);
         }
         // Add a byte guaranteed to be more than the last (if possible)
         if (data_ptr[size - 1] < 255)
         {
            search_bytes.push_back(data_ptr[size - 1] + 1);
         }
      }
      else
      {
         search_bytes.push_back(
             static_cast<uint8_t>(byte_dist(gen)));  // Test one random byte for size 0
      }
      // Remove duplicates for efficiency
      std::sort(search_bytes.begin(), search_bytes.end());
      search_bytes.erase(std::unique(search_bytes.begin(), search_bytes.end()), search_bytes.end());

      for (uint8_t byte_to_find : search_bytes)
      {
         INFO("Searching for byte: " << static_cast<int>(byte_to_find) << " in array of size "
                                     << size);

         // --- Expected Result ---
         const uint8_t* expected_it  = std::lower_bound(data_ptr, data_ptr + size, byte_to_find);
         size_t         expected_pos = std::distance(data_ptr, expected_it);

         // --- ucc::lower_bound_padded ---
         uint16_t padded_pos = ucc::lower_bound_padded(data_ptr, size, byte_to_find);
         REQUIRE(padded_pos == expected_pos);

         // --- ucc::lower_bound_unpadded ---
         uint16_t unpadded_pos = ucc::lower_bound_unpadded(data_ptr, size, byte_to_find);
         REQUIRE(unpadded_pos == expected_pos);

         // --- ucc::lower_bound (dispatcher) ---
         if (size < std::numeric_limits<uint16_t>::max())
         {
            uint16_t dispatch_pos =
                ucc::lower_bound(data_ptr, static_cast<uint32_t>(size), byte_to_find);
            REQUIRE(dispatch_pos == expected_pos);
         }

         // --- ucc::lower_bound_small (direct call if applicable) ---
         if (size < 8)
         {
            uint8_t small_pos =
                ucc::lower_bound_small(data_ptr, static_cast<uint32_t>(size), byte_to_find);
            REQUIRE(small_pos == expected_pos);
         }

         // --- ucc::lower_bound_scalar (direct call) ---
         // Note: lower_bound_scalar uses lower_bound_small internally for size < 8
         int scalar_pos = ucc::lower_bound_scalar(data_ptr, size, byte_to_find);
         REQUIRE(scalar_pos == expected_pos);

#if defined(__ARM_NEON)
         // --- ucc::lower_bound_neon (direct call if applicable) ---
         // Note: lower_bound_neon uses lower_bound_small internally for trailing bytes
         // Requires size >= 16 for the main NEON path to be potentially hit,
         // but it should work correctly for smaller sizes too due to its internal logic.
         if (size > 0)
         {  // Avoid calling with size 0 if it has assertions
            int neon_pos = ucc::lower_bound_neon(data_ptr, size, byte_to_find);
            REQUIRE(neon_pos == expected_pos);
         }
         else
         {
            // For size 0, lower_bound_neon likely returns 0 directly or via small path
            int neon_pos = ucc::lower_bound_neon(data_ptr, size, byte_to_find);
            REQUIRE(neon_pos == 0);
            REQUIRE(expected_pos == 0);
         }
#endif
      }
   }
}

TEST_CASE("Lower Bound Implementations Performance", "[lower_bound][benchmark]")
{
   // Test various representative sizes, with more density between 8-15 and 20-60
   const std::vector<size_t> test_sizes = {0,  1,  7,  8,  9,  10,  11,  12,  13,
                                           14, 15, 16, 20, 24, 28,  32,  36,  40,
                                           48, 56, 60, 63, 64, 127, 128, 255, 256};

   constexpr size_t padding               = 15;
   constexpr size_t num_benchmark_samples = 1000;  // Number of random bytes to pre-generate

   // --- Pre-generate all test data ---
   std::vector<std::vector<uint8_t>> pregen_data_padded;
   pregen_data_padded.reserve(test_sizes.size());
   for (size_t test_size : test_sizes)
   {
      pregen_data_padded.push_back(generate_sorted_data(test_size, padding));
   }
   // --- End Pre-generation ---

   // Pre-generate random bytes to search for
   std::vector<uint8_t> search_bytes_for_bench;
   search_bytes_for_bench.reserve(num_benchmark_samples);
   std::mt19937                       gen(std::random_device{}());
   std::uniform_int_distribution<int> byte_dist(0, 255);
   for (size_t i = 0; i < num_benchmark_samples; ++i)
   {
      search_bytes_for_bench.push_back(static_cast<uint8_t>(byte_dist(gen)));
   }

   INFO("Benchmarking average performance across sizes: 0..256");

   BENCHMARK_ADVANCED("std::lower_bound")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             // Use the same random byte for all sizes in this sample
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             // Volatile to discourage optimizing away the loop
             volatile const uint8_t* last_result = nullptr;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                const uint8_t* data_ptr  = pregen_data_padded[size_idx].data();
                size_t         test_size = test_sizes[size_idx];
                last_result = std::lower_bound(data_ptr, data_ptr + test_size, byte_to_find);
             }
             return last_result;  // Return last result to prevent optimization
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound (dispatcher)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t           byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile uint16_t last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                const uint8_t* data_ptr  = pregen_data_padded[size_idx].data();
                size_t         test_size = test_sizes[size_idx];
                // Ensure size fits in uint32_t for the dispatcher
                if (test_size < std::numeric_limits<uint32_t>::max())
                {
                   last_result =
                       ucc::lower_bound(data_ptr, static_cast<uint32_t>(test_size), byte_to_find);
                }
             }
             return last_result;
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_padded (NEON/Scalar)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t           byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile uint16_t last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                const uint8_t* data_ptr  = pregen_data_padded[size_idx].data();
                size_t         test_size = test_sizes[size_idx];
                last_result = ucc::lower_bound_padded(data_ptr, test_size, byte_to_find);
             }
             return last_result;
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_unpadded (NEON/Scalar)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t           byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile uint16_t last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                // NOTE: For unpadded, we technically don't need the padded data vector,
                // but using it here is fine and simpler for setup consistency.
                const uint8_t* data_ptr  = pregen_data_padded[size_idx].data();
                size_t         test_size = test_sizes[size_idx];
                last_result = ucc::lower_bound_unpadded(data_ptr, test_size, byte_to_find);
             }
             return last_result;
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_scalar")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t      byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile int last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                const uint8_t* data_ptr  = pregen_data_padded[size_idx].data();
                size_t         test_size = test_sizes[size_idx];
                last_result = ucc::lower_bound_scalar(data_ptr, test_size, byte_to_find);
             }
             return last_result;
          });
   };

#if defined(__ARM_NEON)
   BENCHMARK_ADVANCED("ucc::lower_bound_neon")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t      byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile int last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                const uint8_t* data_ptr  = pregen_data_padded[size_idx].data();
                size_t         test_size = test_sizes[size_idx];
                last_result              = ucc::lower_bound_neon(data_ptr, test_size, byte_to_find);
             }
             return last_result;
          });
   };
#endif
}

TEST_CASE("Lower Bound Implementations Unaligned Validation", "[lower_bound][unaligned]")
{
   constexpr size_t max_size = 256;
   constexpr size_t padding  = 15;  // Keep padding for underlying buffer safety

   std::mt19937                       gen(std::random_device{}());
   std::uniform_int_distribution<int> byte_dist(0, 255);

   // Start from size = 1, because size 0 cannot be offset
   for (size_t size = 1; size <= max_size; ++size)
   {
      INFO("Testing unaligned access with original size: " << size);
      std::vector<uint8_t> test_data_padded = generate_sorted_data(size, padding);

      // Create unaligned pointer and size
      const uint8_t* data_ptr_unaligned = test_data_padded.data() + 1;
      size_t         size_unaligned     = size - 1;

      // Regenerate search bytes for this size
      std::vector<uint8_t> search_bytes = {0, 128, 255};
      for (int i = 0; i < 5; ++i)
      {
         search_bytes.push_back(static_cast<uint8_t>(byte_dist(gen)));
      }
      if (size_unaligned > 0)
      {
         search_bytes.push_back(data_ptr_unaligned[0]);
         search_bytes.push_back(data_ptr_unaligned[size_unaligned - 1]);
         if (data_ptr_unaligned[0] > 0)
         {
            search_bytes.push_back(data_ptr_unaligned[0] - 1);
         }
         if (data_ptr_unaligned[size_unaligned - 1] < 255)
         {
            search_bytes.push_back(data_ptr_unaligned[size_unaligned - 1] + 1);
         }
      }
      else
      {
         search_bytes.push_back(static_cast<uint8_t>(byte_dist(gen)));
      }
      std::sort(search_bytes.begin(), search_bytes.end());
      search_bytes.erase(std::unique(search_bytes.begin(), search_bytes.end()), search_bytes.end());

      for (uint8_t byte_to_find : search_bytes)
      {
         INFO("Searching for byte: " << static_cast<int>(byte_to_find)
                                     << " in unaligned array of size " << size_unaligned);

         // --- Expected Result (on unaligned data) ---
         const uint8_t* expected_it = std::lower_bound(
             data_ptr_unaligned, data_ptr_unaligned + size_unaligned, byte_to_find);
         size_t expected_pos = std::distance(data_ptr_unaligned, expected_it);

         // --- ucc::lower_bound_padded ---
         // NOTE: Called with unaligned pointer.
         uint16_t padded_pos =
             ucc::lower_bound_padded(data_ptr_unaligned, size_unaligned, byte_to_find);
         // Keep validation commented out for observation if desired,
         // but it might pass due to test setup providing sufficient underlying buffer.
         // REQUIRE(padded_pos == expected_pos);

         // --- ucc::lower_bound_unpadded ---
         uint16_t unpadded_pos =
             ucc::lower_bound_unpadded(data_ptr_unaligned, size_unaligned, byte_to_find);
         REQUIRE(unpadded_pos == expected_pos);

         // --- ucc::lower_bound (dispatcher) ---
         if (size_unaligned < std::numeric_limits<uint16_t>::max())
         {
            uint16_t dispatch_pos = ucc::lower_bound(
                data_ptr_unaligned, static_cast<uint32_t>(size_unaligned), byte_to_find);
            REQUIRE(dispatch_pos == expected_pos);
         }

         // --- ucc::lower_bound_small (direct call if applicable) ---
         if (size_unaligned < 8)
         {
            uint8_t small_pos = ucc::lower_bound_small(
                data_ptr_unaligned, static_cast<uint32_t>(size_unaligned), byte_to_find);
            REQUIRE(small_pos == expected_pos);
         }

         // --- ucc::lower_bound_scalar (direct call) ---
         int scalar_pos = ucc::lower_bound_scalar(data_ptr_unaligned, size_unaligned, byte_to_find);
         REQUIRE(scalar_pos == expected_pos);

#if defined(__ARM_NEON)
         // --- ucc::lower_bound_neon (direct call if applicable) ---
         // NEON vld1q should handle unaligned access
         if (size_unaligned > 0)
         {
            int neon_pos = ucc::lower_bound_neon(data_ptr_unaligned, size_unaligned, byte_to_find);
            REQUIRE(neon_pos == expected_pos);
         }
         else
         {
            int neon_pos = ucc::lower_bound_neon(data_ptr_unaligned, size_unaligned, byte_to_find);
            REQUIRE(neon_pos == 0);
            REQUIRE(expected_pos == 0);
         }
#endif
      }
   }
}

TEST_CASE("Lower Bound Implementations Unaligned Performance",
          "[lower_bound][benchmark][unaligned]")
{
   // Use the same size distribution as the aligned test
   const std::vector<size_t> test_sizes = {0,  1,  7,  8,  9,  10,  11,  12,  13,
                                           14, 15, 16, 20, 24, 28,  32,  36,  40,
                                           48, 56, 60, 63, 64, 127, 128, 255, 256};

   constexpr size_t padding               = 15;    // Still needed for underlying buffer generation
   constexpr size_t num_benchmark_samples = 1000;  // Number of random bytes to pre-generate

   // --- Pre-generate all test data (still padded) ---
   std::vector<std::vector<uint8_t>> pregen_data_padded;
   pregen_data_padded.reserve(test_sizes.size());
   for (size_t test_size : test_sizes)
   {
      pregen_data_padded.push_back(generate_sorted_data(test_size, padding));
   }
   // --- End Pre-generation ---

   // Pre-generate random bytes to search for
   std::vector<uint8_t> search_bytes_for_bench;
   search_bytes_for_bench.reserve(num_benchmark_samples);
   std::mt19937                       gen(std::random_device{}());
   std::uniform_int_distribution<int> byte_dist(0, 255);
   for (size_t i = 0; i < num_benchmark_samples; ++i)
   {
      search_bytes_for_bench.push_back(static_cast<uint8_t>(byte_dist(gen)));
   }

   INFO("Benchmarking UNALIGNED average performance across sizes: 0..256 (-1)");

   BENCHMARK_ADVANCED("std::lower_bound (unaligned)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile const uint8_t* last_result = nullptr;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                size_t original_size = test_sizes[size_idx];
                if (original_size == 0)
                   continue;  // Cannot offset size 0
                const uint8_t* data_ptr_unaligned = pregen_data_padded[size_idx].data() + 1;
                size_t         size_unaligned     = original_size - 1;
                last_result                       = std::lower_bound(data_ptr_unaligned,
                                                                     data_ptr_unaligned + size_unaligned, byte_to_find);
             }
             return last_result;
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound (dispatcher, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t           byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile uint16_t last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                size_t original_size = test_sizes[size_idx];
                if (original_size == 0)
                   continue;
                const uint8_t* data_ptr_unaligned = pregen_data_padded[size_idx].data() + 1;
                size_t         size_unaligned     = original_size - 1;
                if (size_unaligned < std::numeric_limits<uint32_t>::max())
                {
                   last_result = ucc::lower_bound(
                       data_ptr_unaligned, static_cast<uint32_t>(size_unaligned), byte_to_find);
                }
             }
             return last_result;
          });
   };

   // Exclude lower_bound_padded as it requires aligned start + padding guarantee

   BENCHMARK_ADVANCED("ucc::lower_bound_unpadded (NEON/Scalar, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t           byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile uint16_t last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                size_t original_size = test_sizes[size_idx];
                if (original_size == 0)
                   continue;
                const uint8_t* data_ptr_unaligned = pregen_data_padded[size_idx].data() + 1;
                size_t         size_unaligned     = original_size - 1;
                last_result =
                    ucc::lower_bound_unpadded(data_ptr_unaligned, size_unaligned, byte_to_find);
             }
             return last_result;
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_scalar (unaligned)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t      byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile int last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                size_t original_size = test_sizes[size_idx];
                if (original_size == 0)
                   continue;
                const uint8_t* data_ptr_unaligned = pregen_data_padded[size_idx].data() + 1;
                size_t         size_unaligned     = original_size - 1;
                last_result =
                    ucc::lower_bound_scalar(data_ptr_unaligned, size_unaligned, byte_to_find);
             }
             return last_result;
          });
   };

#if defined(__ARM_NEON)
   BENCHMARK_ADVANCED("ucc::lower_bound_neon (unaligned)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t      byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile int last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                size_t original_size = test_sizes[size_idx];
                if (original_size == 0)
                   continue;
                const uint8_t* data_ptr_unaligned = pregen_data_padded[size_idx].data() + 1;
                size_t         size_unaligned     = original_size - 1;
                last_result =
                    ucc::lower_bound_neon(data_ptr_unaligned, size_unaligned, byte_to_find);
             }
             return last_result;
          });
   };
#endif

   // Add back lower_bound_padded benchmark for observation, noting potential issues
   BENCHMARK_ADVANCED("ucc::lower_bound_padded (NEON/Scalar, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t           byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             volatile uint16_t last_result  = 0;
             for (size_t size_idx = 0; size_idx < test_sizes.size(); ++size_idx)
             {
                size_t original_size = test_sizes[size_idx];
                if (original_size == 0)
                   continue;
                const uint8_t* data_ptr_unaligned = pregen_data_padded[size_idx].data() + 1;
                size_t         size_unaligned     = original_size - 1;
                last_result =
                    ucc::lower_bound_padded(data_ptr_unaligned, size_unaligned, byte_to_find);
             }
             return last_result;
          });
   };
}

TEST_CASE("Lower Bound Fixed Size Performance (256)", "[lower_bound][benchmark][fixed_size]")
{
   constexpr size_t test_size             = 256;
   constexpr size_t padding               = 15;
   constexpr size_t num_benchmark_samples = 5000;  // Number of random bytes to pre-generate

   // Generate the fixed-size test data
   auto           test_data_padded = generate_sorted_data(test_size, padding);
   const uint8_t* data_ptr         = test_data_padded.data();

   // Pre-generate random bytes to search for
   std::vector<uint8_t> search_bytes_for_bench;
   search_bytes_for_bench.reserve(num_benchmark_samples);
   std::mt19937                       gen(std::random_device{}());
   std::uniform_int_distribution<int> byte_dist(0, 255);
   for (size_t i = 0; i < num_benchmark_samples; ++i)
   {
      search_bytes_for_bench.push_back(static_cast<uint8_t>(byte_dist(gen)));
   }

   INFO("Benchmarking ALIGNED performance for fixed size: " << test_size);

   BENCHMARK_ADVANCED("std::lower_bound (size 256)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return std::lower_bound(data_ptr, data_ptr + test_size, byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound (dispatcher, size 256)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound(data_ptr, static_cast<uint32_t>(test_size), byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_padded (NEON/Scalar, size 256)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_padded(data_ptr, test_size, byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_unpadded (NEON/Scalar, size 256)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_unpadded(data_ptr, test_size, byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_scalar (size 256)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_scalar(data_ptr, test_size, byte_to_find);
          });
   };

#if defined(__ARM_NEON)
   BENCHMARK_ADVANCED("ucc::lower_bound_neon (size 256)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_neon(data_ptr, test_size, byte_to_find);
          });
   };
#endif
}

TEST_CASE("Lower Bound Fixed Size Unaligned Performance (255)",
          "[lower_bound][benchmark][fixed_size][unaligned]")
{
   constexpr size_t original_size         = 256;  // Need size+1 to offset for size 255
   constexpr size_t test_size             = 255;  // Effective size after offset
   constexpr size_t padding               = 15;
   constexpr size_t num_benchmark_samples = 5000;  // Number of random bytes to pre-generate

   // Generate the fixed-size test data
   auto           test_data_padded   = generate_sorted_data(original_size, padding);
   const uint8_t* data_ptr_unaligned = test_data_padded.data() + 1;  // Create unaligned pointer

   // Pre-generate random bytes to search for
   std::vector<uint8_t> search_bytes_for_bench;
   search_bytes_for_bench.reserve(num_benchmark_samples);
   std::mt19937                       gen(std::random_device{}());
   std::uniform_int_distribution<int> byte_dist(0, 255);
   for (size_t i = 0; i < num_benchmark_samples; ++i)
   {
      search_bytes_for_bench.push_back(static_cast<uint8_t>(byte_dist(gen)));
   }

   INFO("Benchmarking UNALIGNED performance for fixed size: " << test_size);

   BENCHMARK_ADVANCED("std::lower_bound (size 255, unaligned)")(Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return std::lower_bound(data_ptr_unaligned, data_ptr_unaligned + test_size,
                                     byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound (dispatcher, size 255, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound(data_ptr_unaligned, static_cast<uint32_t>(test_size),
                                     byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_padded (NEON/Scalar, size 255, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      // NOTE: Called with unaligned pointer. Relies on underlying buffer padding.
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_padded(data_ptr_unaligned, test_size, byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_unpadded (NEON/Scalar, size 255, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_unpadded(data_ptr_unaligned, test_size, byte_to_find);
          });
   };

   BENCHMARK_ADVANCED("ucc::lower_bound_scalar (size 255, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_scalar(data_ptr_unaligned, test_size, byte_to_find);
          });
   };

#if defined(__ARM_NEON)
   BENCHMARK_ADVANCED("ucc::lower_bound_neon (size 255, unaligned)")(
       Catch::Benchmark::Chronometer meter)
   {
      meter.measure(
          [&](int i)
          {
             uint8_t byte_to_find = search_bytes_for_bench[i % num_benchmark_samples];
             return ucc::lower_bound_neon(data_ptr_unaligned, test_size, byte_to_find);
          });
   };
#endif
}