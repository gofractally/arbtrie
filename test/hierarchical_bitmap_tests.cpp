#define CATCH_CONFIG_ABORT_AFTER_FAILURE
#include <algorithm>
#include <arbtrie/hierarchical_bitmap.hpp>
#include <bitset>
#include <catch2/catch_template_test_macros.hpp>
#include <catch2/catch_test_macros.hpp>
#include <random>
#include <vector>

// Helper function to find the first set bit in a std::bitset
template <size_t N>
size_t find_first_set_bit(const std::bitset<N>& bits)
{
   for (size_t i = 0; i < N; ++i)
   {
      if (bits[i])
         return i;
   }
   return N;  // If no bits set, return N (which is out of range)
}

// Test sizes to verify different hierarchical levels
TEMPLATE_TEST_CASE_SIG("hierarchical_bitmap operations compared with std::bitset",
                       "[bitmap][reference]",
                       ((size_t N), N),
                       64,     // Level 1 (single word)
                       128,    // Level 2 (small)
                       4096,   // Level 2 (large)
                       65536,  // Level 3
                       100)    // Non-multiple of 64
{
   arbtrie::hierarchical_bitmap<N> hbm;
   std::bitset<N>                  reference;

   SECTION("Default constructor creates empty bitmap")
   {
      REQUIRE(hbm.none());
      REQUIRE(reference.none());
      REQUIRE(hbm.count() == reference.count());
   }

   SECTION("Setting individual bits")
   {
      // Set some bits in both bitmaps
      for (size_t i = 0; i < N; i += 7)
      {
         hbm.set(i);
         reference.set(i);
      }

      // Verify both have the same bits set
      for (size_t i = 0; i < N; ++i)
      {
         REQUIRE(hbm.test(i) == reference.test(i));
      }

      REQUIRE(hbm.count() == reference.count());
   }

   SECTION("Resetting individual bits")
   {
      // First set all bits
      hbm.set();
      reference.set();

      // Reset every 5th bit
      for (size_t i = 0; i < N; i += 5)
      {
         hbm.reset(i);
         reference.reset(i);
      }

      // Verify bits match
      for (size_t i = 0; i < N; ++i)
      {
         REQUIRE(hbm.test(i) == reference.test(i));
      }

      REQUIRE(hbm.count() == reference.count());
   }

   SECTION("Setting all bits")
   {
      hbm.set();
      reference.set();

      REQUIRE(hbm.all());
      REQUIRE(reference.all());
      REQUIRE(hbm.count() == N);
      REQUIRE(hbm.count() == reference.count());
   }

   SECTION("Resetting all bits")
   {
      // First set all bits
      hbm.set();
      reference.set();

      // Then reset all
      hbm.reset();
      reference.reset();

      REQUIRE(hbm.none());
      REQUIRE(reference.none());
      REQUIRE(hbm.count() == 0);
      REQUIRE(hbm.count() == reference.count());
   }

   SECTION("Random bit operations")
   {
      std::mt19937                          rng(42);  // Fixed seed for reproducibility
      std::uniform_int_distribution<size_t> dist(0, N - 1);
      std::uniform_int_distribution<int>    op_dist(0, 1);  // 0 = set, 1 = reset

      // Perform random operations
      for (int i = 0; i < 1000; ++i)
      {
         size_t bit_pos   = dist(rng);
         int    operation = op_dist(rng);

         if (operation == 0)
         {
            hbm.set(bit_pos);
            reference.set(bit_pos);
         }
         else
         {
            hbm.reset(bit_pos);
            reference.reset(bit_pos);
         }

         // Periodically verify the entire bitmap
         if (i % 100 == 0)
         {
            for (size_t j = 0; j < N; ++j)
            {
               REQUIRE(hbm.test(j) == reference.test(j));
            }
            REQUIRE(hbm.count() == reference.count());
         }
      }
   }

   SECTION("countr_zero behavior")
   {
      // Initially all bits are set in both
      hbm.reset();
      reference.reset();

      // Empty bitmap should return N (invalid_index)
      if (reference.none())
      {
         REQUIRE(hbm.countr_zero() == N);
         REQUIRE(hbm.countr_zero() == arbtrie::hierarchical_bitmap<N>::invalid_index);
      }
      else
      {
         size_t expected = find_first_set_bit(reference);
         REQUIRE(hbm.countr_zero() == expected);
      }

      // Set a specific bit and verify countr_zero returns it
      size_t test_bit = std::min<size_t>(N / 2, N - 1);
      hbm.set(test_bit);
      reference.set(test_bit);

      REQUIRE(hbm.countr_zero() == find_first_set_bit(reference));

      // Set first bit and verify
      hbm.reset();
      reference.reset();
      hbm.set(0);
      reference.set(0);

      REQUIRE(hbm.countr_zero() == 0);
      REQUIRE(hbm.countr_zero() == find_first_set_bit(reference));

      // Set last bit and verify
      hbm.reset();
      reference.reset();
      hbm.set(N - 1);
      reference.set(N - 1);

      REQUIRE(hbm.countr_zero() == N - 1);
      REQUIRE(hbm.countr_zero() == find_first_set_bit(reference));
   }

   SECTION("find_first_unset behavior")
   {
      // When bitmap is full
      hbm.set();
      reference.set();

      if (reference.all())
      {
         REQUIRE(hbm.find_first_unset() == arbtrie::hierarchical_bitmap<N>::invalid_index);
      }
      else
      {
         size_t expected = 0;
         while (expected < N && reference.test(expected))
            ++expected;
         REQUIRE(hbm.find_first_unset() == expected);
      }

      // Reset a specific bit and verify find_first_unset returns it
      size_t test_bit = std::min<size_t>(N / 2, N - 1);
      hbm.reset(test_bit);
      reference.reset(test_bit);

      REQUIRE(hbm.find_first_unset() == test_bit);

      // Reset first bit and verify
      hbm.set();
      reference.set();
      hbm.reset(0);
      reference.reset(0);

      REQUIRE(hbm.find_first_unset() == 0);

      // Reset last bit and verify
      hbm.set();
      reference.set();
      hbm.reset(N - 1);
      reference.reset(N - 1);

      // For small bitmaps find_first_unset should find the correct bit
      if (N <= 128)
      {
         REQUIRE(hbm.find_first_unset() == N - 1);
      }
   }

   SECTION("Iterator functionality")
   {
      // Set specific bits
      std::vector<size_t> set_positions;
      for (size_t i = 0; i < N; i += 13)
      {
         hbm.set(i);
         reference.set(i);
         set_positions.push_back(i);
      }

      // Check iterator traversal
      std::vector<size_t> found_positions;
      int                 i = 0;
      for (auto it = hbm.begin(); it != hbm.end(); ++it)
      {
         REQUIRE(*it == i);
         found_positions.push_back(*it);
         i += 13;
      }

      REQUIRE(found_positions == set_positions);

      // Check reverse iterator traversal (if supported)
      if (!set_positions.empty())
      {
         std::vector<size_t> reverse_positions;
         auto                it  = hbm.end();
         auto                fit = found_positions.end();

         do
         {
            --it;
            --fit;
            REQUIRE(*it == *fit);
         } while (fit != found_positions.begin());
      }
   }

   SECTION("countl_zero and countr_zero")
   {
      // When bitmap is empty
      hbm.reset();
      reference.reset();

      REQUIRE(hbm.countl_zero() == N);
      REQUIRE(hbm.countr_zero() == N);

      // Test countl_zero with specific patterns
      hbm.reset();
      reference.reset();

      // Set a bit in the middle
      size_t mid_bit = N / 2;
      hbm.set(mid_bit);
      reference.set(mid_bit);

      REQUIRE(hbm.countl_zero() == N - mid_bit - 1);
      REQUIRE(hbm.countr_zero() == mid_bit);

      // Set the highest bit
      hbm.reset();
      reference.reset();
      hbm.set(N - 1);
      reference.set(N - 1);

      REQUIRE(hbm.countl_zero() == 0);
      REQUIRE(hbm.countr_zero() == N - 1);

      // Set the lowest bit
      hbm.reset();
      reference.reset();
      hbm.set(0);
      reference.set(0);

      REQUIRE(hbm.countl_zero() == N - 1);
      REQUIRE(hbm.countr_zero() == 0);
   }

   SECTION("unset_first_set and set_first_unset")
   {
      // Test unset_first_set
      hbm.reset();
      reference.reset();

      // Setting some bits
      for (size_t i = 10; i < N; i += 20)
      {
         hbm.set(i);
         reference.set(i);
      }

      // Expected first set bit
      size_t expected_first_set = 10;
      REQUIRE(hbm.countr_zero() == expected_first_set);

      // Unset it and check it's gone
      REQUIRE(hbm.unset_first_set() == expected_first_set);
      REQUIRE(!hbm.test(expected_first_set));

      // The next bit should now be the first set
      if (10 + 20 < N)
      {
         REQUIRE(hbm.countr_zero() == 10 + 20);
      }

      // Test set_first_unset
      hbm.set();

      // Reset some bits
      for (size_t i = 15; i < N; i += 25)
      {
         hbm.reset(i);
      }

      // Expected first unset bit
      size_t expected_first_unset = 15;
      REQUIRE(hbm.find_first_unset() == expected_first_unset);

      // Set it and check it's set
      REQUIRE(hbm.set_first_unset() == expected_first_unset);
      REQUIRE(hbm.test(expected_first_unset));

      // The next bit should now be the first unset
      if (15 + 25 < N)
      {
         REQUIRE(hbm.find_first_unset() == 15 + 25);
      }
   }

   SECTION("Check invariants")
   {
      // Empty bitmap should maintain invariants
      REQUIRE(hbm.check_invariants());

      // Full bitmap should maintain invariants
      hbm.set();
      REQUIRE(hbm.check_invariants());

      // Random pattern should maintain invariants
      hbm.reset();
      std::mt19937                          rng(42);
      std::uniform_int_distribution<size_t> dist(0, N - 1);

      for (int i = 0; i < 100; ++i)
      {
         size_t bit_pos = dist(rng);
         hbm.set(bit_pos);
         REQUIRE(hbm.check_invariants());
      }
   }
}

// Additional test for larger bitmap sizes
TEST_CASE("hierarchical_bitmap large size operations", "[bitmap][large]")
{
   constexpr size_t                N = 1048576;  // 1M bits
   arbtrie::hierarchical_bitmap<N> hbm;

   SECTION("Basic operations on large bitmap")
   {
      // Verify empty state
      REQUIRE(hbm.none());
      REQUIRE(hbm.count() == 0);

      // Set specific patterns
      for (size_t i = 0; i < N; i += 10000)
      {
         hbm.set(i);
      }

      // Verify count
      REQUIRE(hbm.count() == (N / 10000) + (N % 10000 != 0 ? 1 : 0));

      // Check countr_zero
      REQUIRE(hbm.countr_zero() == 0);

      // Unset first and check again
      hbm.reset(0);
      REQUIRE(hbm.countr_zero() == 10000);

      // Verify setting and unsetting last bit
      hbm.set(N - 1);
      REQUIRE(hbm.test(N - 1));

      hbm.reset(N - 1);
      REQUIRE(!hbm.test(N - 1));

      // Test setting all bits (might be slow on large bitmaps)
      hbm.set();
      REQUIRE(hbm.all());
      REQUIRE(hbm.count() == N);

      // Test finding first unset bit in a full bitmap
      REQUIRE(hbm.find_first_unset() == arbtrie::hierarchical_bitmap<N>::invalid_index);

      // Reset and verify
      hbm.reset();
      REQUIRE(hbm.none());
   }
}

TEST_CASE("hierarchical_bitmap operations for Level 4 bitmap (1,000,000 bits)", "[bitmap][level4]")
{
   constexpr size_t N = 1'000'000;  // Significantly larger than 262,144 to ensure level 4

   arbtrie::hierarchical_bitmap<N> hbm;
   std::bitset<N>                  reference;  // For comparison

   SECTION("Basic operations")
   {
      // Initially all bits should be 0
      REQUIRE(hbm.none());
      REQUIRE(hbm.count() == 0);

      // Test at different regions of the bitmap to exercise all levels
      // Lower bits (L0)
      hbm.set(0);
      reference.set(0);
      REQUIRE(hbm.test(0) == reference.test(0));

      // Middle bits (across L1/L2 boundaries)
      hbm.set(64 * 64 - 1);  // End of first L1 word
      hbm.set(64 * 64);      // Start of second L1 word
      reference.set(64 * 64 - 1);
      reference.set(64 * 64);
      REQUIRE(hbm.test(64 * 64 - 1) == reference.test(64 * 64 - 1));
      REQUIRE(hbm.test(64 * 64) == reference.test(64 * 64));

      // Higher bits (across L2/L3 boundaries)
      hbm.set(64 * 64 * 64 - 1);  // End of first L2 word
      hbm.set(64 * 64 * 64);      // Start of second L2 word
      reference.set(64 * 64 * 64 - 1);
      reference.set(64 * 64 * 64);
      REQUIRE(hbm.test(64 * 64 * 64 - 1) == reference.test(64 * 64 * 64 - 1));
      REQUIRE(hbm.test(64 * 64 * 64) == reference.test(64 * 64 * 64));

      // Very high bits (deep in L3)
      hbm.set(N - 1);  // Last bit
      hbm.set(N / 2);  // Middle bit
      reference.set(N - 1);
      reference.set(N / 2);
      REQUIRE(hbm.test(N - 1) == reference.test(N - 1));
      REQUIRE(hbm.test(N / 2) == reference.test(N / 2));

      // Reset some bits
      hbm.reset(0);
      hbm.reset(N - 1);
      reference.reset(0);
      reference.reset(N - 1);
      REQUIRE(hbm.test(0) == reference.test(0));
      REQUIRE(hbm.test(N - 1) == reference.test(N - 1));

      // Check bit count
      REQUIRE(hbm.count() == reference.count());
   }

   SECTION("Bulk operations")
   {
      // Test set() and reset() for all bits
      hbm.set();
      REQUIRE(hbm.all());
      REQUIRE(hbm.count() == N);

      hbm.reset();
      REQUIRE(hbm.none());
      REQUIRE(hbm.count() == 0);
   }

   SECTION("First-bit finding operations")
   {
      // Initially all bits are 0, first unset should be 0
      REQUIRE(hbm.find_first_unset() == 0);

      // Set all bits
      hbm.set();

      // Now all bits are 1, no unset bits
      REQUIRE(hbm.find_first_unset() == arbtrie::hierarchical_bitmap<N>::invalid_index);

      // Reset a specific bit in each level region and verify find_first_unset finds it

      // Test with a bit at position 42
      hbm.reset(42);
      REQUIRE(hbm.find_first_unset() == 42);

      // Set it back and try at a higher level (L1 range)
      hbm.set(42);
      hbm.reset(5000);
      REQUIRE(hbm.find_first_unset() == 5000);

      // Set it back and try at L2 range
      hbm.set(5000);
      hbm.reset(70000);
      REQUIRE(hbm.find_first_unset() == 70000);

      // Set it back and try at L3 range
      hbm.set(70000);
      hbm.reset(500000);
      REQUIRE(hbm.find_first_unset() == 500000);

      // Test set_first_unset
      hbm.reset(42);
      hbm.reset(5000);
      hbm.reset(70000);
      REQUIRE(hbm.set_first_unset() == 42);  // Should set bit 42
      REQUIRE(hbm.test(42) == true);
      REQUIRE(hbm.set_first_unset() == 5000);  // Should set bit 5000
      REQUIRE(hbm.test(5000) == true);

      // Test countl_zero and countr_zero
      hbm.reset();
      REQUIRE(hbm.countl_zero() == N);
      REQUIRE(hbm.countr_zero() == N);

      // Set highest bit
      hbm.set(N - 1);
      REQUIRE(hbm.countl_zero() == 0);
      REQUIRE(hbm.countr_zero() == N - 1);

      // Set lowest bit
      hbm.set(0);
      REQUIRE(hbm.countl_zero() == 0);
      REQUIRE(hbm.countr_zero() == 0);
   }

   SECTION("Iterator functionality")
   {
      // Set specific bits at different levels
      std::vector<size_t> set_positions = {
          0,       // L0
          100,     // L0
          1000,    // L0
          10000,   // L1
          100000,  // L2
          500000,  // L3
          N - 1    // Highest bit
      };

      for (auto pos : set_positions)
      {
         hbm.set(pos);
      }

      // Check forward iterator traversal
      std::vector<size_t> found_positions;
      for (auto it = hbm.begin(); it != hbm.end(); ++it)
      {
         found_positions.push_back(*it);
      }

      REQUIRE(found_positions == set_positions);

      // Check reverse iterator traversal
      if (!set_positions.empty())
      {
         std::vector<size_t> reverse_positions;
         auto                it = hbm.end();
         do
         {
            --it;
            reverse_positions.push_back(*it);
            if (*it == set_positions.front())
               break;
         } while (true);

         std::vector<size_t> expected_reverse(set_positions.rbegin(), set_positions.rend());
         REQUIRE(reverse_positions == expected_reverse);
      }
   }

   SECTION("check_invariants")
   {
      // Ensure the hierarchical structure maintains its invariants
      // when manipulating bits at different levels

      // Start with empty bitmap
      hbm.reset();
      REQUIRE(hbm.check_invariants());

      // Set various bits at different levels
      std::vector<size_t> test_positions = {
          0,       // L0
          63,      // L0 (edge)
          64,      // L0 (edge)
          4095,    // L1 (edge)
          4096,    // L1 (edge)
          262143,  // L2 (edge)
          262144,  // L2 (edge)
          500000,  // Mid-range
          N - 1    // Highest bit
      };

      for (auto pos : test_positions)
      {
         hbm.set(pos);
         REQUIRE(hbm.check_invariants());
         hbm.reset(pos);
         REQUIRE(hbm.check_invariants());
      }

      // Set all and ensure invariants hold
      hbm.set();
      REQUIRE(hbm.check_invariants());

      // Reset all and ensure invariants hold
      hbm.reset();
      REQUIRE(hbm.check_invariants());
   }
}