#include <catch2/benchmark/catch_benchmark.hpp>  // Added for benchmarking
#include <catch2/catch_test_macros.hpp>
#include <psitri/node/inner_node_util.hpp>
#include <psitri/node/node.hpp>  // For branch definition

#include <popcntintrin.h>  // std::popcount
#include <algorithm>       // std::equal
#include <cstring>         // memcpy
#include <iostream>        // Added for printing
#include <numeric>         // std::iota
#include <random>          // Re-added for runtime data generation
#include <string>          // Added for benchmark naming
#include <vector>

// Helper to compare std::array
template <typename T, std::size_t N>
bool compare_arrays(const std::array<T, N>& arr1, const std::array<T, N>& arr2)
{
   return std::equal(arr1.begin(), arr1.end(), arr2.begin());
}

using sal::ptr_address;

TEST_CASE("InnerNodeUtilTests", "[inner_node_util]")
{
   using namespace psitri;

   SECTION("create_cline_freq_table")
   {
      branch branches[5];
      branches[0].set_line_index(1, 0);  // line 1
      branches[1].set_line_index(3, 1);  // line 3
      branches[2].set_line_index(1, 2);  // line 1
      branches[3].set_line_index(7, 3);  // line 7
      branches[4].set_line_index(1, 4);  // line 1

      cline_freq_table result = create_cline_freq_table(branches, branches + 5);

      std::array<uint8_t, 16> expected_freq = {};
      expected_freq[1]                      = 3;
      expected_freq[3]                      = 1;
      expected_freq[7]                      = 1;

      uint16_t expected_bitmap = (1 << 1) | (1 << 3) | (1 << 7);  // 0b10001010 = 138

      REQUIRE(compare_arrays(result.freq_table, expected_freq));
      REQUIRE(result.clines_referenced == expected_bitmap);
      REQUIRE(result.needed_clines() == 8);      // 16 - clz(138) = 16 - 8 = 8
      REQUIRE(result.compressed_clines() == 3);  // popcount(138)
   }

   SECTION("create_nth_set_bit_table_scalar")
   {
      std::array<uint8_t, 16> freq1 = {0, 5, 0, 8, 2, 0, 0, 1, 9, 0, 4, 0, 0, 7, 0, 3};
      // Non-zero indices:         1,    3, 4,    7, 8,   10,   13,   15
      std::array<uint8_t, 16> exp1 = {0, 0, 1, 1, 2, 3, 3, 3, 4, 5, 5, 6, 6, 6, 7, 7};
      REQUIRE(compare_arrays(create_nth_set_bit_table_scalar(freq1), exp1));

      std::array<uint8_t, 16> freq2 = {};  // All zeros
      std::array<uint8_t, 16> exp2  = {};
      REQUIRE(compare_arrays(create_nth_set_bit_table_scalar(freq2), exp2));

      std::array<uint8_t, 16> freq3;  // All non-zeros (value 1)
      freq3.fill(1);
      std::array<uint8_t, 16> exp3;
      std::iota(exp3.begin(), exp3.end(), 0);  // 0, 1, 2, ..., 15
      REQUIRE(compare_arrays(create_nth_set_bit_table_scalar(freq3), exp3));
   }

#ifdef __ARM_NEON
   SECTION("create_nth_set_bit_table_neon")
   {
      std::array<uint8_t, 16> freq1 = {0, 5, 0, 8, 2, 0, 0, 1, 9, 0, 4, 0, 0, 7, 0, 3};
      std::array<uint8_t, 16> exp1  = {0, 0, 1, 1, 2, 3, 3, 3, 4, 5, 5, 6, 6, 6, 7, 7};

      // Print arrays for debugging
      std::cout << "Expected (exp1):  ";
      for (uint8_t val : exp1)
      {
         std::cout << (int)val << " ";
      }
      std::cout << std::endl;

      auto actual1 = create_nth_set_bit_table_neon(freq1);
      std::cout << "Actual (neon):    ";
      for (uint8_t val : actual1)
      {
         std::cout << (int)val << " ";
      }
      std::cout << std::endl;

      REQUIRE(compare_arrays(actual1, exp1));

      std::array<uint8_t, 16> freq2 = {};  // All zeros
      std::array<uint8_t, 16> exp2  = {};
      REQUIRE(compare_arrays(create_nth_set_bit_table_neon(freq2), exp2));

      std::array<uint8_t, 16> freq3;  // All non-zeros (value 1)
      freq3.fill(1);
      std::array<uint8_t, 16> exp3;
      std::iota(exp3.begin(), exp3.end(), 0);  // 0, 1, 2, ..., 15
      REQUIRE(compare_arrays(create_nth_set_bit_table_neon(freq3), exp3));

      // Also check consistency with scalar version
      REQUIRE(compare_arrays(create_nth_set_bit_table_neon(freq1),
                             create_nth_set_bit_table_scalar(freq1)));
      REQUIRE(compare_arrays(create_nth_set_bit_table_neon(freq2),
                             create_nth_set_bit_table_scalar(freq2)));
      REQUIRE(compare_arrays(create_nth_set_bit_table_neon(freq3),
                             create_nth_set_bit_table_scalar(freq3)));
   }
#endif

   SECTION("copy_branches_and_update_cline_index_scalar")
   {
      std::array<uint8_t, 16> lut = {0, 1, 1, 2, 1, 2, 2, 3,
                                     1, 2, 2, 3, 2, 3, 3, 4};  // Example LUT (popcount)
      const size_t            N   = 20;

      std::vector<uint8_t> input_data = {
          0x01, 0x1A, 0x23, 0x3F, 0x45, 0x55, 0x6C, 0x70,  //  8 bytes
          0x88, 0x91, 0xA9, 0xB4, 0xC2, 0xD7, 0xE0, 0xFF,  // 16 bytes
          0x05, 0x3B, 0x7E, 0xF0                           // 20 bytes
      };
      std::vector<uint8_t> output_data(N);
      std::vector<uint8_t> expected_output = {0x01, 0x1A, 0x13, 0x2F, 0x15, 0x25, 0x2C,
                                              0x30, 0x18, 0x21, 0x29, 0x34, 0x22, 0x37,
                                              0x30, 0x4F, 0x05, 0x2B, 0x3E, 0x40};

      copy_branches_and_update_cline_index_scalar(input_data.data(), output_data.data(), N, lut);
      REQUIRE(output_data == expected_output);

      // Test N=2
      const size_t         N2          = 2;
      std::vector<uint8_t> input_data2 = {0x5A, 0xB3};
      std::vector<uint8_t> output_data2(N2);
      std::vector<uint8_t> expected_output2 = {0x2A, 0x33};  // lut[5]=2, lut[B]=3
      copy_branches_and_update_cline_index_scalar(input_data2.data(), output_data2.data(), N2, lut);
      REQUIRE(output_data2 == expected_output2);
   }

#ifdef __ARM_NEON
   SECTION("copy_branches_and_update_cline_index_neon")
   {
      std::array<uint8_t, 16> lut     = {0, 1, 1, 2, 1, 2, 2, 3,
                                         1, 2, 2, 3, 2, 3, 3, 4};  // Example LUT (popcount)
      const size_t            padding = 16;  // Padding needed for safe over-read/write

      // Test N=20
      const size_t         N           = 20;
      const size_t         buffer_size = padding + N + padding;
      std::vector<uint8_t> input_buffer(buffer_size);
      std::vector<uint8_t> output_buffer(buffer_size);
      const uint8_t        initial_data[N] = {
          0x01, 0x1A, 0x23, 0x3F, 0x45, 0x55, 0x6C, 0x70,  //  8 bytes
          0x88, 0x91, 0xA9, 0xB4, 0xC2, 0xD7, 0xE0, 0xFF,  // 16 bytes
          0x05, 0x3B, 0x7E, 0xF0                           // 20 bytes
      };
      memcpy(input_buffer.data() + padding, initial_data, N);
      const uint8_t* input_data_ptr  = input_buffer.data() + padding;
      uint8_t*       output_data_ptr = output_buffer.data() + padding;

      std::vector<uint8_t> expected_output = {0x01, 0x1A, 0x13, 0x2F, 0x15, 0x25, 0x2C,
                                              0x30, 0x18, 0x21, 0x29, 0x34, 0x22, 0x37,
                                              0x30, 0x4F, 0x05, 0x2B, 0x3E, 0x40};

      copy_branches_and_update_cline_index_neon(input_data_ptr, output_data_ptr, N, lut);
      // Compare only the N logical bytes
      REQUIRE(std::equal(output_data_ptr, output_data_ptr + N, expected_output.begin()));

      // Test N=16
      const size_t N16 = 16;
      memcpy(input_buffer.data() + padding, initial_data, N16);
      output_buffer.assign(buffer_size, 0);  // Clear output
      copy_branches_and_update_cline_index_neon(input_data_ptr, output_data_ptr, N16, lut);
      REQUIRE(std::equal(output_data_ptr, output_data_ptr + N16, expected_output.begin()));

      // Test N=2
      const size_t  N2                = 2;
      const uint8_t initial_data2[N2] = {0x5A, 0xB3};
      memcpy(input_buffer.data() + padding, initial_data2, N2);
      output_buffer.assign(buffer_size, 0);                  // Clear output
      std::vector<uint8_t> expected_output2 = {0x2A, 0x33};  // lut[5]=2, lut[B]=3
      copy_branches_and_update_cline_index_neon(input_data_ptr, output_data_ptr, N2, lut);
      REQUIRE(std::equal(output_data_ptr, output_data_ptr + N2, expected_output2.begin()));

      // Test N=33 (example where loop runs)
      const size_t         N33           = 33;
      const size_t         buffer_size33 = padding + N33 + padding;
      std::vector<uint8_t> input_buffer33(buffer_size33);
      std::vector<uint8_t> output_buffer33(buffer_size33);
      std::vector<uint8_t> initial_data33(N33);
      std::vector<uint8_t> expected_output33(N33);
      for (size_t i = 0; i < N33; ++i)
         initial_data33[i] = static_cast<uint8_t>(i | ((i % 16) << 4));  // Some pattern
      for (size_t i = 0; i < N33; ++i)
      {
         uint8_t orig         = initial_data33[i];
         uint8_t idx          = orig >> 4;
         uint8_t val          = lut[idx];
         uint8_t low          = orig & 0x0F;
         expected_output33[i] = (val << 4) | low;
      }
      memcpy(input_buffer33.data() + padding, initial_data33.data(), N33);
      const uint8_t* input_data_ptr33  = input_buffer33.data() + padding;
      uint8_t*       output_data_ptr33 = output_buffer33.data() + padding;

      copy_branches_and_update_cline_index_neon(input_data_ptr33, output_data_ptr33, N33, lut);
      REQUIRE(std::equal(output_data_ptr33, output_data_ptr33 + N33, expected_output33.begin()));
   }
#endif

   SECTION("copy_masked_cline_data")
   {
      // Copy elements at indices 1, 3, 4, 7
      uint32_t bitmap = (1 << 1) | (1 << 3) | (1 << 4) | (1 << 7);  // 0b10011010 = 154

      std::vector<ptr_address> source_values(16);
      for (uint32_t i = 0; i < 16; ++i)
      {
         source_values[i] = ptr_address(1000 + i);  // Example values 1000, 1001, ... 1015
      }

      int expected_count = __builtin_popcount(bitmap);
      REQUIRE(expected_count == 4);                          // Check assumption
      std::vector<ptr_address> dest_values(expected_count);  // Allocate exact size
      std::vector<ptr_address> expected_dest = {ptr_address(1001), ptr_address(1003),
                                                ptr_address(1004), ptr_address(1007)};

      copy_masked_cline_data(bitmap, source_values.data(), dest_values.data());
      REQUIRE(dest_values == expected_dest);

      // Test empty bitmap
      uint32_t bitmap_empty         = 0;
      int      expected_count_empty = __builtin_popcount(bitmap_empty);
      REQUIRE(expected_count_empty == 0);
      std::vector<ptr_address> dest_values_empty(expected_count_empty);
      std::vector<ptr_address> expected_dest_empty = {};
      // Need to handle the assert(bitmap != 0) in the function. Let's test non-empty only for now.
      // copy_masked_cline_data(bitmap_empty, source_values.data(), dest_values_empty.data());
      // REQUIRE(dest_values_empty == expected_dest_empty);

      // Test full bitmap (lower 16 bits)
      uint32_t bitmap_full         = 0xFFFF;
      int      expected_count_full = __builtin_popcount(bitmap_full);
      REQUIRE(expected_count_full == 16);
      std::vector<ptr_address> dest_values_full(expected_count_full);
      std::vector<ptr_address> expected_dest_full(16);  // Pre-allocate
      for (uint32_t i = 0; i < 16; ++i)
      {
         expected_dest_full[i] = ptr_address(1000 + i);
      }
      copy_masked_cline_data(bitmap_full, source_values.data(), dest_values_full.data());
      REQUIRE(dest_values_full == expected_dest_full);

      // Test bitmap with only highest bit set
      uint32_t bitmap_high         = (1 << 15);
      int      expected_count_high = __builtin_popcount(bitmap_high);
      REQUIRE(expected_count_high == 1);
      std::vector<ptr_address> dest_values_high(expected_count_high);
      std::vector<ptr_address> expected_dest_high = {ptr_address(1015)};
      copy_masked_cline_data(bitmap_high, source_values.data(), dest_values_high.data());
      REQUIRE(dest_values_high == expected_dest_high);
   }
}

TEST_CASE("InnerNodeUtilBenchmarks", "[inner_node_util][benchmark]")
{
   // === Random Number Generators (defined once for the test case) ===
   std::random_device              rd;
   std::mt19937                    gen(rd());
   std::uniform_int_distribution<> distrib_byte(0, 255);
   std::uniform_int_distribution<> distrib_lut(0, 15);
   std::uniform_int_distribution<> distrib_N(2, 128);  // Use literal for max_N here

   // === Data and Helpers for create_nth_set_bit_table ===
   std::vector<std::array<uint8_t, 16>> bit_table_test_data;
   {
      // Use the generator defined above
      std::uniform_int_distribution<> distrib_bit_table(0, 255);
      constexpr size_t                num_bit_table_sets = 100;
      bit_table_test_data.reserve(num_bit_table_sets);
      for (size_t i = 0; i < num_bit_table_sets; ++i)
      {
         std::array<uint8_t, 16> random_array;
         for (uint8_t& val : random_array)
         {
            val = static_cast<uint8_t>(distrib_bit_table(gen));  // Use correct distribution
         }
         bit_table_test_data.push_back(random_array);
      }
   }
   auto run_bit_table_tests_and_accumulate = [&bit_table_test_data](auto&& func) -> uint64_t
   {
      uint64_t accumulator = 0;
      for (int rep = 0; rep < 100; ++rep)
      {
         for (const auto& data : bit_table_test_data)
         {
            auto result = func(data);
            accumulator += result[15];
         }
      }
      return accumulator;
   };

   // === Data and Helpers for copy_branches_and_update_cline ===
   constexpr size_t max_N_copy       = 128;
   constexpr size_t padding_copy     = 16;  // For NEON safety
   constexpr size_t buffer_size_copy = padding_copy + max_N_copy + padding_copy;
   constexpr size_t num_copy_sets    = 100;

   struct CopyTestData
   {
      std::vector<uint8_t>    input_buffer;
      std::vector<uint8_t>    output_buffer;
      std::array<uint8_t, 16> lut;
   };
   std::vector<CopyTestData> copy_test_data(num_copy_sets);
   {
      // Use generators defined at the start of the TEST_CASE

      for (size_t i = 0; i < num_copy_sets; ++i)
      {
         copy_test_data[i].input_buffer.resize(buffer_size_copy);
         copy_test_data[i].output_buffer.resize(buffer_size_copy);
         for (size_t j = 0; j < max_N_copy; ++j)
         {
            copy_test_data[i].input_buffer[padding_copy + j] =
                static_cast<uint8_t>(distrib_byte(gen));
         }
         for (uint8_t& val : copy_test_data[i].lut)
         {
            val = static_cast<uint8_t>(distrib_lut(gen));
         }
      }
   }
   auto run_copy_tests_and_accumulate = [&copy_test_data, &gen, &distrib_N](auto&& func) -> uint64_t
   {
      uint64_t accumulator = 0;
      for (int rep = 0; rep < 100; ++rep)
      {
         for (size_t i = 0; i < num_copy_sets; ++i)
         {
            size_t         current_N  = distrib_N(gen);  // Generate random N for this iteration
            const uint8_t* input_ptr  = copy_test_data[i].input_buffer.data() + padding_copy;
            uint8_t*       output_ptr = copy_test_data[i].output_buffer.data() + padding_copy;
            // Ensure output buffer is cleared or reset if needed between reps/sets depending on func logic
            // For simple copy/update, overwriting is fine.
            func(input_ptr, output_ptr, current_N, copy_test_data[i].lut);
            accumulator += output_ptr[current_N - 1];
         }
      }
      return accumulator;
   };

   // === Benchmarks ===

   // --- create_nth_set_bit_table Benchmarks ---
   BENCHMARK("Bit Table Scalar")
   {
      return run_bit_table_tests_and_accumulate(psitri::create_nth_set_bit_table_scalar);
   };
#ifdef __ARM_NEON
   BENCHMARK("Bit Table NEON")
   {
      return run_bit_table_tests_and_accumulate(psitri::create_nth_set_bit_table_neon);
   };
#endif

   // --- copy_branches_and_update_cline_index Benchmarks ---
   BENCHMARK("Copy Branches Scalar")
   {
      return run_copy_tests_and_accumulate(psitri::copy_branches_and_update_cline_index_scalar);
   };
#ifdef __ARM_NEON
   BENCHMARK("Copy Branches NEON")
   {
      return run_copy_tests_and_accumulate(psitri::copy_branches_and_update_cline_index_neon);
   };
#endif
}

// --- New TEST_CASE for Small N Copy Benchmarks ---
TEST_CASE("InnerNodeUtilSmallNCopyBenchmarks", "[inner_node_util][benchmark][small_n]")
{
   // === Random Number Generators ===
   std::random_device              rd;
   std::mt19937                    gen(rd());
   std::uniform_int_distribution<> distrib_byte(0, 255);
   std::uniform_int_distribution<> distrib_lut(0, 15);
   constexpr size_t                max_N_copy_small = 15;
   std::uniform_int_distribution<> distrib_N_small(2, max_N_copy_small);

   // === Data Setup for Small N Copy ===
   constexpr size_t padding_copy     = 16;  // Keep padding for NEON safety
   constexpr size_t buffer_size_copy = padding_copy + max_N_copy_small + padding_copy;
   constexpr size_t num_copy_sets    = 100;  // Keep same number of sets

   struct CopyTestData
   {
      std::vector<uint8_t>    input_buffer;
      std::vector<uint8_t>    output_buffer;
      std::array<uint8_t, 16> lut;
   };
   std::vector<CopyTestData> copy_test_data(num_copy_sets);

   {
      for (size_t i = 0; i < num_copy_sets; ++i)
      {
         copy_test_data[i].input_buffer.resize(buffer_size_copy);
         copy_test_data[i].output_buffer.resize(buffer_size_copy);
         // Fill the potentially used part of the input buffer
         for (size_t j = 0; j < max_N_copy_small; ++j)
         {
            copy_test_data[i].input_buffer[padding_copy + j] =
                static_cast<uint8_t>(distrib_byte(gen));
         }
         for (uint8_t& val : copy_test_data[i].lut)
         {
            val = static_cast<uint8_t>(distrib_lut(gen));
         }
      }
   }

   // Helper lambda for small N copy tests
   auto run_copy_tests_small_N = [&copy_test_data, &gen, &distrib_N_small](auto&& func) -> uint64_t
   {
      uint64_t accumulator = 0;
      for (int rep = 0; rep < 100; ++rep)
      {  // Keep 100 reps
         for (size_t i = 0; i < num_copy_sets; ++i)
         {
            size_t         current_N  = distrib_N_small(gen);  // Use small N distribution
            const uint8_t* input_ptr  = copy_test_data[i].input_buffer.data() + padding_copy;
            uint8_t*       output_ptr = copy_test_data[i].output_buffer.data() + padding_copy;
            func(input_ptr, output_ptr, current_N, copy_test_data[i].lut);
            accumulator += output_ptr[current_N - 1];
         }
      }
      return accumulator;
   };

   // === Benchmarks for Small N Copy ===
   BENCHMARK("Small N Copy Scalar")
   {
      return run_copy_tests_small_N(psitri::copy_branches_and_update_cline_index_scalar);
   };
#ifdef __ARM_NEON
   BENCHMARK("Small N Copy NEON")
   {
      return run_copy_tests_small_N(psitri::copy_branches_and_update_cline_index_neon);
   };
#endif
}
