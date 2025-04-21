#include <algorithm>  // For std::find, std::generate
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <limits>               // Required for numeric_limits
#include <numeric>              // For std::iota
#include <random>               // For std::mt19937 and std::uniform_int_distribution
#include <ucc/lower_bound.hpp>  // Include the header with the functions to test
#include <vector>

// Helper function to generate test data
std::vector<uint32_t> generate_data()
{
   std::vector<uint32_t> data(16);
   // Use random data to reduce branch prediction effectiveness
   std::random_device rd;
   std::mt19937       gen(rd());
   // Generate values, leaving space for a guaranteed "not found" value
   std::uniform_int_distribution<uint32_t> distrib(0, std::numeric_limits<uint32_t>::max() - 1);

   std::generate(data.begin(), data.end(), [&]() { return distrib(gen); });
   return data;
}

TEST_CASE("Performance comparison for find_u32x16 variants", "[benchmark][find_u32x16]")
{
   auto            data     = generate_data();
   const uint32_t* data_ptr = data.data();

   // --- Verification Section (uses specific known values) ---
   // Pick test values directly from the generated random data for verification
   uint32_t value_first     = data[0];
   uint32_t value_middle    = data[7];
   uint32_t value_last      = data[15];
   uint32_t value_not_found = std::numeric_limits<uint32_t>::max();
   while (std::find(data.begin(), data.end(), value_not_found) != data.end())
   {
      value_not_found--;
   }

   auto verify = [&](uint32_t value, int expected_index)
   {
      INFO("Verifying value: " << value << " Expected index: " << expected_index);
      int res_neon     = ucc::find_u32x16(data_ptr, value);
      int res_scalar   = ucc::find_u32x16_scalar(data_ptr, value);
      int res_unrolled = ucc::find_u32x16_scalar_unrolled(data_ptr, value);
      REQUIRE(res_neon == expected_index);
      REQUIRE(res_scalar == expected_index);
      REQUIRE(res_unrolled == expected_index);
   };

   verify(value_first, 0);
   verify(value_middle, 7);
   verify(value_last, 15);
   verify(value_not_found, 16);
   // --- End Verification ---

   // --- Benchmark Section (uses varying random search values) ---
   constexpr size_t      num_search_values = 1024;  // Number of different values to search for
   std::vector<uint32_t> search_values(num_search_values);
   std::random_device    rd_search;
   std::mt19937          gen_search(rd_search());
   std::uniform_int_distribution<uint32_t> distrib_search(0, std::numeric_limits<uint32_t>::max());
   std::generate(search_values.begin(), search_values.end(),
                 [&]() { return distrib_search(gen_search); });

   BENCHMARK("find_u32x16 (random)")
   {
      // Cycle through pre-generated random search values
      static size_t i             = 0;
      uint32_t      value_to_find = search_values[i % num_search_values];
      i++;
      return ucc::find_u32x16(data_ptr, value_to_find);
   };

   BENCHMARK("find_u32x16_scalar (random)")
   {
      static size_t i             = 0;
      uint32_t      value_to_find = search_values[i % num_search_values];
      i++;
      return ucc::find_u32x16_scalar(data_ptr, value_to_find);
   };

   BENCHMARK("find_u32x16_scalar_unrolled (random)")
   {
      static size_t i             = 0;
      uint32_t      value_to_find = search_values[i % num_search_values];
      i++;
      return ucc::find_u32x16_scalar_unrolled(data_ptr, value_to_find);
   };
}