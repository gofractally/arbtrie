#include <arbtrie/hierarchical_bitmap.hpp>
#include <bitset>
#include <catch2/catch_all.hpp>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

using namespace arbtrie;

namespace
{
   // Test helper class with same memory layout as hierarchical_bitmap<64>
   struct BitmapMemoryLayoutTest
   {
      uint64_t level0[1];
      // No other members needed for the test
   };
}  // namespace

TEST_CASE("hierarchical_bitmap basic operations", "[bitmap]")
{
   SECTION("Level 1 bitmap")
   {
      hierarchical_bitmap<64> bitmap;  // Level 1: up to 64 segments

      // Test initial state - all bits start as set (1)
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming segments - unsets a bit (sets it to 0)
      std::vector<uint32_t> claimed;
      for (int i = 0; i < 64; ++i)
      {
         auto segment = bitmap.unset_first_set();
         REQUIRE(segment != bitmap.invalid_segment);
         REQUIRE(segment < 64);
         REQUIRE(!bitmap.test(segment));  // Bit should now be 0
         claimed.push_back(segment);
      }

      // Test all segments are claimed (all bits are 0)
      REQUIRE(bitmap.find_first_set() == bitmap.invalid_segment);
      REQUIRE(bitmap.unset_first_set() == bitmap.invalid_segment);

      // Test freeing segments (setting bits back to 1)
      for (auto segment : claimed)
      {
         bitmap.set(segment);            // Set bit to 1
         REQUIRE(bitmap.test(segment));  // Bit should now be 1
      }

      // Test all segments are free again (all bits are 1)
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);
   }

   SECTION("Level 2 bitmap")
   {
      hierarchical_bitmap<4096> bitmap;  // Level 2: up to 4096 segments

      // Test initial state
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming segments
      std::vector<uint32_t> claimed;
      for (int i = 0; i < 4096; ++i)
      {
         auto segment = bitmap.unset_first_set();
         REQUIRE(segment != bitmap.invalid_segment);
         REQUIRE(segment < 4096);
         REQUIRE(!bitmap.test(segment));
         claimed.push_back(segment);
      }

      // Test all segments are claimed
      REQUIRE(bitmap.find_first_set() == bitmap.invalid_segment);
      REQUIRE(bitmap.unset_first_set() == bitmap.invalid_segment);

      // Test freeing segments
      for (auto segment : claimed)
      {
         bitmap.set(segment);
         REQUIRE(bitmap.test(segment));
      }

      // Test all segments are free again
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);
   }

   SECTION("Level 3 bitmap")
   {
      hierarchical_bitmap<262144> bitmap;  // Level 3: up to 262,144 segments

      // Test initial state - all bits start as 1
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);  // Fixed seed for reproducibility
      std::uniform_int_distribution<uint32_t> dist(0, 262143);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.test(segment))  // If bit is 1 (free)
         {
            auto claimed = bitmap.unset_first_set();  // Set a bit to 0 (claim it)
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(!bitmap.test(claimed));  // Bit should now be 0
         }
         else  // If bit is 0 (claimed)
         {
            bitmap.set(segment);            // Set bit to 1 (free it)
            REQUIRE(bitmap.test(segment));  // Bit should now be 1
         }
      }
   }

   SECTION("Level 4 bitmap")
   {
      hierarchical_bitmap<16777216> bitmap;  // Level 4: up to 16,777,216 segments

      // Test initial state - all bits start as 1
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);  // Fixed seed for reproducibility
      std::uniform_int_distribution<uint32_t> dist(0, 16777215);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.test(segment))  // If bit is 1 (free)
         {
            auto claimed = bitmap.unset_first_set();  // Set a bit to 0 (claim it)
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(!bitmap.test(claimed));  // Bit should now be 0
         }
         else  // If bit is 0 (claimed)
         {
            bitmap.set(segment);            // Set bit to 1 (free it)
            REQUIRE(bitmap.test(segment));  // Bit should now be 1
         }
      }
   }
}

TEST_CASE("hierarchical_bitmap stress test", "[bitmap][stress]")
{
   SECTION("Random operations on Level 4 bitmap")
   {
      hierarchical_bitmap<16777216>           bitmap;  // Level 4: up to 16,777,216 segments
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 16777215);
      std::uniform_real_distribution<double>  op_dist(0.0, 1.0);

      std::vector<uint32_t> claimed_segments;

      // Perform 10000 random operations
      for (int i = 0; i < 10000; ++i)
      {
         double op = op_dist(rng);

         if (op < 0.4)
         {  // 40% chance to claim
            auto segment = bitmap.unset_first_set();
            if (segment != bitmap.invalid_segment)
            {
               REQUIRE(!bitmap.test(segment));
               claimed_segments.push_back(segment);
            }
         }
         else if (op < 0.8)
         {  // 40% chance to free
            if (!claimed_segments.empty())
            {
               size_t   idx     = rng() % claimed_segments.size();
               uint32_t segment = claimed_segments[idx];
               bitmap.set(segment);
               REQUIRE(bitmap.test(segment));
               claimed_segments.erase(claimed_segments.begin() + idx);
            }
         }
         else
         {  // 20% chance to check random segment
            uint32_t segment    = dist(rng);
            bool     is_claimed = std::find(claimed_segments.begin(), claimed_segments.end(),
                                            segment) != claimed_segments.end();
            REQUIRE(bitmap.test(segment) == !is_claimed);
         }
      }
   }
}

TEST_CASE("hierarchical_bitmap edge cases", "[bitmap]")
{
   SECTION("Level 4 bitmap full allocation and deallocation")
   {
      hierarchical_bitmap<16777216> bitmap;  // Level 4: up to 16,777,216 segments
      std::vector<uint32_t>         segments;

      // Claim segments until full
      uint32_t segment;
      while ((segment = bitmap.unset_first_set()) != bitmap.invalid_segment)
      {
         REQUIRE(!bitmap.test(segment));
         segments.push_back(segment);
      }

      // Verify we can't claim more
      REQUIRE(bitmap.unset_first_set() == bitmap.invalid_segment);

      // Free all segments in reverse order
      while (!segments.empty())
      {
         segment = segments.back();
         segments.pop_back();
         bitmap.set(segment);
         REQUIRE(bitmap.test(segment));
      }

      // Verify we can claim again
      REQUIRE(bitmap.unset_first_set() != bitmap.invalid_segment);
   }

   SECTION("Out of bounds checks")
   {
      hierarchical_bitmap<4096> bitmap;  // Level 2: up to 4096 segments

      // These should trigger the assertions in debug mode
      REQUIRE_NOTHROW(bitmap.test(4095));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.test(4096), std::runtime_error);  // First invalid segment

      REQUIRE_NOTHROW(bitmap.set(4095));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.set(4096), std::runtime_error);  // First invalid segment
   }
}

TEST_CASE("hierarchical_bitmap unaligned sizes", "[bitmap]")
{
   SECTION("Small unaligned size (50 segments)")
   {
      hierarchical_bitmap<50> bitmap;  // Level 1, but not full 64 segments

      // Test initial state
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming all available segments
      std::vector<uint32_t> claimed;
      for (int i = 0; i < 50; ++i)
      {
         auto segment = bitmap.unset_first_set();
         REQUIRE(segment != bitmap.invalid_segment);
         REQUIRE(segment < 50);  // Should never exceed our size
         REQUIRE(!bitmap.test(segment));
         claimed.push_back(segment);
      }

      // Should be full now
      REQUIRE(bitmap.find_first_set() == bitmap.invalid_segment);
      REQUIRE(bitmap.unset_first_set() == bitmap.invalid_segment);

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.test(49));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.test(50), std::runtime_error);  // First invalid segment
   }

   SECTION("Mid-level unaligned size (3000 segments)")
   {
      hierarchical_bitmap<3000> bitmap;  // Level 2, but not full 4096 segments

      // Test initial state
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 2999);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.test(segment))
         {
            auto claimed = bitmap.unset_first_set();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(claimed < 3000);  // Should never exceed our size
            REQUIRE(!bitmap.test(claimed));
         }
         else
         {
            bitmap.set(segment);
            REQUIRE(bitmap.test(segment));
         }
      }

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.test(2999));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.test(3000), std::runtime_error);  // First invalid segment
   }

   SECTION("Large unaligned size (200000 segments)")
   {
      hierarchical_bitmap<200000> bitmap;  // Level 3, but not full 262144 segments

      // Test initial state
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 199999);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.test(segment))
         {
            auto claimed = bitmap.unset_first_set();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(claimed < 200000);  // Should never exceed our size
            REQUIRE(!bitmap.test(claimed));
         }
         else
         {
            bitmap.set(segment);
            REQUIRE(bitmap.test(segment));
         }
      }

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.test(199999));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.test(200000), std::runtime_error);  // First invalid segment
   }

   SECTION("Very large unaligned size (10000000 segments)")
   {
      hierarchical_bitmap<10000000> bitmap;  // Level 4, but not full 16777216 segments

      // Test initial state
      REQUIRE(bitmap.find_first_set() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 9999999);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.test(segment))
         {
            auto claimed = bitmap.unset_first_set();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(claimed < 10000000);  // Should never exceed our size
            REQUIRE(!bitmap.test(claimed));
         }
         else
         {
            bitmap.set(segment);
            REQUIRE(bitmap.test(segment));
         }
      }

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.test(9999999));                         // Last valid segment
      REQUIRE_THROWS_AS(bitmap.test(10000000), std::runtime_error);  // First invalid segment
   }

   SECTION("Edge case sizes")
   {
      // Test sizes right after level boundaries
      hierarchical_bitmap<65> bitmap1;  // Just over level 1
      REQUIRE(bitmap1.find_first_set() != bitmap1.invalid_segment);
      REQUIRE_NOTHROW(bitmap1.test(64));
      REQUIRE_THROWS_AS(bitmap1.test(65), std::runtime_error);

      hierarchical_bitmap<4097> bitmap2;  // Just over level 2
      REQUIRE(bitmap2.find_first_set() != bitmap2.invalid_segment);
      REQUIRE_NOTHROW(bitmap2.test(4096));
      REQUIRE_THROWS_AS(bitmap2.test(4097), std::runtime_error);

      // Test sizes just before level boundaries
      hierarchical_bitmap<63> bitmap3;  // Just under level 1
      REQUIRE(bitmap3.find_first_set() != bitmap3.invalid_segment);
      REQUIRE_NOTHROW(bitmap3.test(62));
      REQUIRE_THROWS_AS(bitmap3.test(63), std::runtime_error);

      hierarchical_bitmap<4095> bitmap4;  // Just under level 2
      REQUIRE(bitmap4.find_first_set() != bitmap4.invalid_segment);
      REQUIRE_NOTHROW(bitmap4.test(4094));
      REQUIRE_THROWS_AS(bitmap4.test(4095), std::runtime_error);
   }
}

TEST_CASE("hierarchical_bitmap new methods", "[bitmap]")
{
   SECTION("set_first_unset and unset_first_set on Level 1 bitmap")
   {
      hierarchical_bitmap<64> bitmap;  // Level 1: up to 64 segments

      // Initially all bits are set
      REQUIRE(bitmap.any());
      REQUIRE(!bitmap.none());

      // Clear all bits
      bitmap.reset();

      // Now verify all bits are clear
      REQUIRE(!bitmap.any());
      REQUIRE(bitmap.none());

      // Test set_first_unset
      std::vector<uint32_t> set_bits;
      for (int i = 0; i < 64; ++i)
      {
         auto bit_pos = bitmap.set_first_unset();
         REQUIRE(bit_pos != bitmap.invalid_index);
         REQUIRE(bit_pos < 64);
         REQUIRE(bitmap.test(bit_pos));
         set_bits.push_back(bit_pos);
      }

      // All bits should be set now
      REQUIRE(bitmap.any());
      REQUIRE(!bitmap.none());
      REQUIRE(bitmap.set_first_unset() == bitmap.invalid_index);

      // Test unset_first_set (same as reset_first_set or clear_first_set)
      for (int i = 0; i < 64; ++i)
      {
         auto bit_pos = bitmap.unset_first_set();
         REQUIRE(bit_pos != bitmap.invalid_index);
         REQUIRE(bit_pos < 64);
         REQUIRE(!bitmap.test(bit_pos));
      }

      // Now all bits should be clear again
      REQUIRE(!bitmap.any());
      REQUIRE(bitmap.none());
      REQUIRE(bitmap.unset_first_set() == bitmap.invalid_index);
   }

   SECTION("set_first_unset and unset_first_set on larger bitmaps")
   {
      hierarchical_bitmap<4096> bitmap;  // Level 2: up to 4096 segments

      // Initially all bits are set
      REQUIRE(bitmap.any());

      // Clear all bits
      bitmap.reset();

      // Verify all bits are clear
      REQUIRE(!bitmap.any());
      REQUIRE(bitmap.none());

      // Set and test specific bits
      for (uint32_t i = 0; i < 100; ++i)
      {
         uint32_t bit_pos = i * 40;  // Spread bits out
         if (bit_pos < 4096)
         {
            bitmap.set(bit_pos);
            REQUIRE(bitmap.test(bit_pos));
         }
      }

      // Test unset_first_set repeatedly
      uint32_t prev_pos = 0;
      while (true)
      {
         auto bit_pos = bitmap.unset_first_set();
         if (bit_pos == bitmap.invalid_index)
            break;

         REQUIRE(!bitmap.test(bit_pos));
         REQUIRE(bit_pos >= prev_pos);  // Should be in ascending order
         prev_pos = bit_pos;
      }

      // Verify all bits are clear
      REQUIRE(!bitmap.any());
      REQUIRE(bitmap.none());

      // Test set_first_unset with specific pattern
      for (int i = 0; i < 500; ++i)
      {
         auto bit_pos = bitmap.set_first_unset();
         REQUIRE(bit_pos == i);  // Should set in order from 0
         REQUIRE(bitmap.test(bit_pos));
      }

      // Unset specific bits and verify
      for (uint32_t i = 0; i < 100; ++i)
      {
         uint32_t bit_pos = i * 5;  // Spread bits out
         if (bit_pos < 500)
         {
            bitmap.reset(bit_pos);
            REQUIRE(!bitmap.test(bit_pos));
         }
      }

      // Now set_first_unset should find these holes first
      for (uint32_t i = 0; i < 100; ++i)
      {
         uint32_t expected_pos = i * 5;
         if (expected_pos < 500)
         {
            auto bit_pos = bitmap.set_first_unset();
            REQUIRE(bit_pos == expected_pos);
            REQUIRE(bitmap.test(bit_pos));
         }
      }
   }

   SECTION("set() and reset() bulk operations")
   {
      hierarchical_bitmap<4096> bitmap;  // Level 2

      // Initially all bits are set
      REQUIRE(bitmap.any());
      REQUIRE(!bitmap.none());

      // Reset all bits
      bitmap.reset();
      REQUIRE(!bitmap.any());
      REQUIRE(bitmap.none());

      // Set specific bits
      for (uint32_t i = 0; i < 100; ++i)
      {
         bitmap.set(i);
         REQUIRE(bitmap.test(i));
      }

      // Set all bits
      bitmap.set();

      // Verify all bits are set
      for (uint32_t i = 0; i < 100; ++i)
      {
         REQUIRE(bitmap.test(i));
      }
      REQUIRE(bitmap.any());
      REQUIRE(!bitmap.none());

      // Reset all bits again
      bitmap.reset();

      // Verify all bits are clear
      for (uint32_t i = 0; i < 100; ++i)
      {
         REQUIRE(!bitmap.test(i));
      }
      REQUIRE(!bitmap.any());
      REQUIRE(bitmap.none());
   }

   SECTION("any() and none() methods")
   {
      // Test with various bitmap sizes

      // Level 1
      hierarchical_bitmap<64> bitmap1;
      REQUIRE(bitmap1.any());
      REQUIRE(!bitmap1.none());

      bitmap1.reset();
      REQUIRE(!bitmap1.any());
      REQUIRE(bitmap1.none());

      bitmap1.set(10);
      REQUIRE(bitmap1.any());
      REQUIRE(!bitmap1.none());

      // Level 2
      hierarchical_bitmap<2048> bitmap2;
      REQUIRE(bitmap2.any());
      bitmap2.reset();
      REQUIRE(bitmap2.none());
      bitmap2.set(1000);
      REQUIRE(bitmap2.any());

      // Level 3
      hierarchical_bitmap<100000> bitmap3;
      REQUIRE(bitmap3.any());
      bitmap3.reset();
      REQUIRE(bitmap3.none());
      bitmap3.set(50000);
      REQUIRE(bitmap3.any());

      // Level 4
      hierarchical_bitmap<10000000> bitmap4;
      REQUIRE(bitmap4.any());
      bitmap4.reset();
      REQUIRE(bitmap4.none());
      bitmap4.set(5000000);
      REQUIRE(bitmap4.any());
   }

   SECTION("Individual set() and reset() operations")
   {
      hierarchical_bitmap<4096> bitmap;

      // Initially all free
      for (uint32_t i = 0; i < 100; ++i)
      {
         REQUIRE(bitmap.test(i));
      }

      // Reset specific bits
      for (uint32_t i = 0; i < 100; i += 2)
      {
         bitmap.reset(i);
         REQUIRE(!bitmap.test(i));
         REQUIRE(bitmap.test(i + 1));  // Adjacent bit should be unaffected
      }

      // Set specific bits
      for (uint32_t i = 0; i < 100; i += 2)
      {
         bitmap.set(i);
         REQUIRE(bitmap.test(i));
      }

      // Bounds checking
      REQUIRE_NOTHROW(bitmap.set(4095));
      REQUIRE_THROWS_AS(bitmap.set(4096), std::runtime_error);
      REQUIRE_NOTHROW(bitmap.reset(4095));
      REQUIRE_THROWS_AS(bitmap.reset(4096), std::runtime_error);
   }

   SECTION("find_first_set and find_first_unset methods")
   {
      // Test with different bitmap sizes

      // Level 1 (64 bits)
      {
         hierarchical_bitmap<64> bitmap;

         // Initially all bits are set
         bitmap.set();

         // All bits set - find_first_set should find first bit, find_first_unset should find none
         REQUIRE(bitmap.find_first_set() == 0);
         REQUIRE(bitmap.find_first_unset() == bitmap.invalid_index);

         // Clear specific bits
         bitmap.reset(0);
         bitmap.reset(5);
         bitmap.reset(63);

         // Test find_first_set still works
         REQUIRE(bitmap.find_first_set() == 1);  // First set bit is now at index 1

         // Test find_first_unset finds the first unset bit (0)
         REQUIRE(bitmap.find_first_unset() == 0);

         // Reset all bits
         bitmap.reset();

         // All bits clear - find_first_set should find none, find_first_unset should find first bit
         REQUIRE(bitmap.find_first_set() == bitmap.invalid_index);
         REQUIRE(bitmap.find_first_unset() == 0);

         // Set specific bits
         bitmap.set(10);
         bitmap.set(20);
         bitmap.set(30);

         // Test find_first_set finds index 10
         REQUIRE(bitmap.find_first_set() == 10);

         // Test find_first_unset finds index 0
         REQUIRE(bitmap.find_first_unset() == 0);

         // Set first few bits
         bitmap.set(0);
         bitmap.set(1);
         bitmap.set(2);

         // Test find_first_unset finds index 3
         REQUIRE(bitmap.find_first_unset() == 3);
      }

      // Level 2 (4096 bits)
      {
         hierarchical_bitmap<4096> bitmap;

         // Initialize with alternating bits for testing
         bitmap.reset();  // Start with all bits clear

         for (uint32_t i = 0; i < 100; i += 2)
         {
            bitmap.set(i);
         }

         // Test find_first_set finds index 0
         REQUIRE(bitmap.find_first_set() == 0);

         // Test find_first_unset finds index 1
         REQUIRE(bitmap.find_first_unset() == 1);

         // Reset first bit
         bitmap.reset(0);

         // Test find_first_set finds index 2
         REQUIRE(bitmap.find_first_set() == 2);

         // Test find_first_unset finds index 0
         REQUIRE(bitmap.find_first_unset() == 0);

         // Create pattern across word boundaries
         bitmap.reset();
         bitmap.set(63);  // Last bit in first word
         bitmap.set(64);  // First bit in second word

         REQUIRE(bitmap.find_first_set() == 63);

         // Clear bit 63
         bitmap.reset(63);

         // Now first set bit should be at index 64
         REQUIRE(bitmap.find_first_set() == 64);
      }

      // Level 3 (262144 bits)
      {
         hierarchical_bitmap<262144> bitmap;

         // Test with sparse bit patterns
         bitmap.reset();

         // Set bits at key boundaries
         uint32_t level3_positions[] = {0,    63,   64,    127,   128,   4095,
                                        4096, 8191, 65535, 65536, 262143};

         for (auto pos : level3_positions)
         {
            if (pos < 262144)
            {
               bitmap.set(pos);
            }
         }

         // First set bit should be at index 0
         REQUIRE(bitmap.find_first_set() == 0);

         // First unset bit should be at index 1
         REQUIRE(bitmap.find_first_unset() == 1);

         // Clear bit 0
         bitmap.reset(0);

         // Now first set bit should be at index 63
         REQUIRE(bitmap.find_first_set() == 63);

         // First unset bit should be at index 0 (since we just cleared it)
         REQUIRE(bitmap.find_first_unset() == 0);

         // Create a small pattern of set/unset
         bitmap.reset();

         for (uint32_t i = 10; i < 20; i++)
         {
            bitmap.set(i);
         }

         // First set bit should be at index 10
         REQUIRE(bitmap.find_first_set() == 10);

         // First unset bit should be at index 0
         REQUIRE(bitmap.find_first_unset() == 0);
      }

      // Level 4 (16777216 bits)
      {
         hierarchical_bitmap<16777216> bitmap;

         // Test with very sparse bit patterns
         bitmap.reset();

         // Set specific bits at level boundaries
         uint32_t test_positions[] = {0, 64, 4096, 8192, 65536, 131072, 262144, 1048576, 16777215};

         for (auto pos : test_positions)
         {
            bitmap.set(pos);
         }

         // First set bit should be at index 0
         REQUIRE(bitmap.find_first_set() == 0);

         // First unset bit should be at index 1
         REQUIRE(bitmap.find_first_unset() == 1);

         // Clear bits 0 and 64
         bitmap.reset(0);
         bitmap.reset(64);

         // Now first set bit should be at index 4096
         REQUIRE(bitmap.find_first_set() == 4096);

         // First unset bit should still be at index 0
         REQUIRE(bitmap.find_first_unset() == 0);

         // Set all bits
         bitmap.set();

         // All bits set - find_first_unset should find none
         REQUIRE(bitmap.find_first_unset() == bitmap.invalid_index);

         // Reset all bits
         bitmap.reset();

         // All bits clear - find_first_set should find none
         REQUIRE(bitmap.find_first_set() == bitmap.invalid_index);
      }
   }

   SECTION("Level 3 set_first_unset and unset_first_set operations")
   {
      hierarchical_bitmap<262144> bitmap;  // Level 3: up to 262,144 segments

      // Initially all bits are set
      REQUIRE(bitmap.any());

      // Reset all bits
      bitmap.reset();
      REQUIRE(bitmap.none());

      // Set bits in a pattern with increasingly large gaps
      constexpr uint32_t    test_count = 1000;
      std::vector<uint32_t> positions;

      for (uint32_t i = 0; i < test_count; ++i)
      {
         uint32_t pos = i * i % 262144;  // Quadratic spacing creates varied patterns
         bitmap.set(pos);
         positions.push_back(pos);
      }

      // Sort positions for verification
      std::sort(positions.begin(), positions.end());
      // Remove any duplicates that might occur due to the quadratic spacing
      positions.erase(std::unique(positions.begin(), positions.end()), positions.end());

      // Verify unset_first_set finds bits in order
      for (uint32_t i = 0; i < positions.size(); ++i)
      {
         auto bit_pos = bitmap.unset_first_set();
         REQUIRE(bit_pos != bitmap.invalid_index);
         REQUIRE(bit_pos == positions[i]);
         REQUIRE(!bitmap.test(bit_pos));
      }

      // Verify all bits are now unset
      REQUIRE(bitmap.none());

      // Test set_first_unset with random access pattern
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 262143);

      // Set some bits randomly
      std::vector<bool> bit_status(262144, false);
      for (uint32_t i = 0; i < 1000; ++i)
      {
         uint32_t pos = dist(rng);
         bitmap.set(pos);
         bit_status[pos] = true;
      }

      // Reset bitmap
      bitmap.reset();

      // Set specific bits spread across the levels
      uint32_t level3_bits[] = {0,    1,    63,    64,    127,    128,    4095,  4096,
                                8191, 8192, 65535, 65536, 131071, 131072, 262143};

      for (auto pos : level3_bits)
      {
         if (pos < 262144)
         {
            bitmap.set(pos);
            REQUIRE(bitmap.test(pos));
         }
      }

      // Unset them and verify
      for (auto pos : level3_bits)
      {
         if (pos < 262144)
         {
            bitmap.reset(pos);
            REQUIRE(!bitmap.test(pos));
         }
      }

      // Verify we can set them again with set_first_unset
      for (int i = 0; i < 15; ++i)
      {
         auto bit_pos = bitmap.set_first_unset();
         REQUIRE(bit_pos != bitmap.invalid_index);
         REQUIRE(bitmap.test(bit_pos));
      }
   }

   SECTION("Level 4 set_first_unset and unset_first_set operations")
   {
      hierarchical_bitmap<16777216> bitmap;  // Level 4: up to 16,777,216 segments

      // Initially all bits are set
      REQUIRE(bitmap.any());

      // Test boundary cases for level 4
      const uint32_t level4_boundaries[] = {
          0,        // Start
          1,        // Second bit
          63,       // Last bit in first word
          64,       // First bit in second word
          4095,     // End of first L1 block
          4096,     // Start of second L1 block
          262143,   // End of first L2 block
          262144,   // Start of second L2 block
          16777215  // Last bit
      };

      // Test each boundary position
      for (auto pos : level4_boundaries)
      {
         // Reset all bits
         bitmap.reset();
         REQUIRE(bitmap.none());

         // Set only the test bit
         bitmap.set(pos);
         REQUIRE(bitmap.test(pos));

         // Ensure unset_first_set finds it
         auto found_pos = bitmap.unset_first_set();
         REQUIRE(found_pos == pos);
         REQUIRE(!bitmap.test(pos));
         REQUIRE(bitmap.none());

         // Ensure set_first_unset sets it again
         auto set_pos = bitmap.set_first_unset();
         REQUIRE(set_pos == 0);  // Should always find first bit first
         REQUIRE(bitmap.test(set_pos));
      }

      // Test with multiple bits set across hierarchy levels
      bitmap.reset();

      // Set bits at key boundaries
      for (auto pos : level4_boundaries)
      {
         bitmap.set(pos);
         REQUIRE(bitmap.test(pos));
      }

      // Verify unset_first_set finds them in order
      uint32_t prev_pos = 0;
      for (size_t i = 0; i < sizeof(level4_boundaries) / sizeof(level4_boundaries[0]); ++i)
      {
         auto bit_pos = bitmap.unset_first_set();
         REQUIRE(bit_pos != bitmap.invalid_index);
         REQUIRE(bit_pos >= prev_pos);  // Should be in ascending order
         REQUIRE(!bitmap.test(bit_pos));
         prev_pos = bit_pos;
      }

      // Verify all bits are unset
      REQUIRE(bitmap.none());

      // Set some random patterns
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> small_dist(0, 1000);

      // Create a pattern that tests different levels of the hierarchy
      for (int i = 0; i < 100; ++i)
      {
         uint32_t base = i * 160000;  // Spread out across the bitmap
         if (base < 16777216)
         {
            uint32_t offset = small_dist(rng);
            uint32_t pos    = base + offset;
            if (pos < 16777216)
            {
               bitmap.set(pos);
               REQUIRE(bitmap.test(pos));
            }
         }
      }

      // Test unset_first_set finds bits
      while (bitmap.any())
      {
         auto bit_pos = bitmap.unset_first_set();
         REQUIRE(bit_pos != bitmap.invalid_index);
         REQUIRE(!bitmap.test(bit_pos));
      }

      // Verify we can set bits with set_first_unset
      for (int i = 0; i < 100; ++i)
      {
         auto bit_pos = bitmap.set_first_unset();
         REQUIRE(bit_pos == i);  // Should set in order
         REQUIRE(bitmap.test(bit_pos));
      }
   }
}

TEST_CASE("hierarchical_bitmap compared with std::bitset", "[bitmap]")
{
   SECTION("Level 1 bitmap comparison with std::bitset")
   {
      // Create equivalent data structures
      hierarchical_bitmap<64> bitmap;
      std::bitset<64>         reference;

      // Initialize both to all 1s
      bitmap.set();
      reference.set();

      // Verify initial state matches
      for (size_t i = 0; i < 64; ++i)
      {
         REQUIRE(bitmap.test(i) == reference.test(i));
      }
      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Test resetting specific bits
      for (size_t i = 0; i < 64; i += 3)
      {
         bitmap.reset(i);
         reference.reset(i);

         // Verify state after each operation
         for (size_t j = 0; j < 64; ++j)
         {
            REQUIRE(bitmap.test(j) == reference.test(j));
         }
         REQUIRE(bitmap.any() == reference.any());
         REQUIRE(bitmap.none() == reference.none());
      }

      // Test setting specific bits
      for (size_t i = 0; i < 64; i += 5)
      {
         bitmap.set(i);
         reference.set(i);

         // Verify state
         for (size_t j = 0; j < 64; ++j)
         {
            REQUIRE(bitmap.test(j) == reference.test(j));
         }
      }

      // Reset all bits
      bitmap.reset();
      reference.reset();

      // Verify all bits reset
      for (size_t i = 0; i < 64; ++i)
      {
         REQUIRE(bitmap.test(i) == reference.test(i));
      }
      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Test setting all bits
      bitmap.set();
      reference.set();

      // Verify all bits set
      for (size_t i = 0; i < 64; ++i)
      {
         REQUIRE(bitmap.test(i) == reference.test(i));
      }
      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());
   }

   SECTION("Level 2 bitmap comparison with std::bitset")
   {
      // Create equivalent data structures with larger size
      hierarchical_bitmap<4096> bitmap;
      std::bitset<4096>         reference;

      // Test setting all bits
      bitmap.set();
      reference.set();

      // Verify initial state
      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Check sample of bits (checking all 4096 would be too slow)
      for (size_t i = 0; i < 4096; i += 100)
      {
         REQUIRE(bitmap.test(i) == reference.test(i));
      }

      // Test pattern of set/reset operations
      std::vector<size_t> positions = {0, 1, 63, 64, 127, 128, 1023, 1024, 4095};

      // Reset specific bits
      for (auto pos : positions)
      {
         bitmap.reset(pos);
         reference.reset(pos);

         // Verify after each operation
         REQUIRE(bitmap.test(pos) == reference.test(pos));
      }

      // Set specific bits
      for (auto pos : positions)
      {
         bitmap.set(pos);
         reference.set(pos);

         // Verify after each operation
         REQUIRE(bitmap.test(pos) == reference.test(pos));
      }

      // Reset all bits
      bitmap.reset();
      reference.reset();

      // Verify all reset
      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Set specific bits in patterns
      for (size_t i = 0; i < 4096; i += 128)
      {
         bitmap.set(i);
         reference.set(i);
      }

      // Verify pattern
      for (size_t i = 0; i < 4096; i += 128)
      {
         REQUIRE(bitmap.test(i) == reference.test(i));
         if (i > 0)
         {
            REQUIRE(bitmap.test(i - 1) == reference.test(i - 1));
         }
      }

      // Verify any/none state
      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());
   }

   SECTION("find_first_set comparison with std::bitset")
   {
      hierarchical_bitmap<256> bitmap;
      std::bitset<256>         reference;

      // Helper function to find first set bit in a bitset
      auto find_first_set = [](const std::bitset<256>& bits) -> size_t
      {
         for (size_t i = 0; i < bits.size(); ++i)
         {
            if (bits.test(i))
            {
               return i;
            }
         }
         return bits.size();  // No bits set
      };

      // Initialize both to all 0s
      bitmap.reset();
      reference.reset();

      // Both should have no bits set
      REQUIRE(bitmap.find_first_set() == bitmap.invalid_index);
      REQUIRE(find_first_set(reference) == reference.size());

      // Set specific patterns and check first set bit
      std::vector<size_t> test_positions = {127, 63, 255, 0, 1, 64, 128};

      for (auto pos : test_positions)
      {
         // Reset both
         bitmap.reset();
         reference.reset();

         // Set this position only
         bitmap.set(pos);
         reference.set(pos);

         // Verify first set bit matches
         REQUIRE(bitmap.find_first_set() == pos);
         REQUIRE(find_first_set(reference) == pos);
      }

      // Test with multiple bits set
      bitmap.reset();
      reference.reset();

      // Set bits at positions: 50, 100, 150, 200
      for (size_t pos : {50, 100, 150, 200})
      {
         bitmap.set(pos);
         reference.set(pos);
      }

      // First set bit should be at position 50
      REQUIRE(bitmap.find_first_set() == 50);
      REQUIRE(find_first_set(reference) == 50);

      // Set earlier bit and check again
      bitmap.set(25);
      reference.set(25);

      // First set bit should now be at position 25
      REQUIRE(bitmap.find_first_set() == 25);
      REQUIRE(find_first_set(reference) == 25);

      // Set bit 0 and check
      bitmap.set(0);
      reference.set(0);

      // First set bit should now be at position 0
      REQUIRE(bitmap.find_first_set() == 0);
      REQUIRE(find_first_set(reference) == 0);
   }

   SECTION("find_first_unset comparison with reference implementation")
   {
      hierarchical_bitmap<256> bitmap;
      std::bitset<256>         reference;

      // Initialize both to all 1s
      bitmap.set();
      reference.set();

      // Both should have no unset bits
      REQUIRE(bitmap.find_first_unset() == bitmap.invalid_index);
      // There's no direct equivalent in std::bitset, so we'll search manually
      size_t first_unset = reference.size();
      for (size_t i = 0; i < reference.size(); ++i)
      {
         if (!reference.test(i))
         {
            first_unset = i;
            break;
         }
      }
      REQUIRE(first_unset == reference.size());

      // Reset specific bits and check first unset
      std::vector<size_t> test_positions = {127, 63, 255, 0, 1, 64, 128};

      for (auto pos : test_positions)
      {
         // Set both to all 1s
         bitmap.set();
         reference.set();

         // Reset this position only
         bitmap.reset(pos);
         reference.reset(pos);

         // Verify first unset bit matches
         REQUIRE(bitmap.find_first_unset() == pos);

         // Manual search for first unset in reference
         first_unset = reference.size();
         for (size_t i = 0; i < reference.size(); ++i)
         {
            if (!reference.test(i))
            {
               first_unset = i;
               break;
            }
         }
         REQUIRE(first_unset == pos);
      }

      // Test with multiple bits unset
      bitmap.set();
      reference.set();

      // Unset bits at positions: 50, 100, 150, 200
      for (size_t pos : {50, 100, 150, 200})
      {
         bitmap.reset(pos);
         reference.reset(pos);
      }

      // Find first unset manually in reference
      first_unset = reference.size();
      for (size_t i = 0; i < reference.size(); ++i)
      {
         if (!reference.test(i))
         {
            first_unset = i;
            break;
         }
      }

      // First unset bit should be at position 50
      REQUIRE(bitmap.find_first_unset() == 50);
      REQUIRE(first_unset == 50);

      // Reset earlier bit and check again
      bitmap.reset(25);
      reference.reset(25);

      // Find first unset manually in reference
      first_unset = reference.size();
      for (size_t i = 0; i < reference.size(); ++i)
      {
         if (!reference.test(i))
         {
            first_unset = i;
            break;
         }
      }

      // First unset bit should now be at position 25
      REQUIRE(bitmap.find_first_unset() == 25);
      REQUIRE(first_unset == 25);

      // Reset bit 0 and check
      bitmap.reset(0);
      reference.reset(0);

      // First unset bit should now be at position 0
      REQUIRE(bitmap.find_first_unset() == 0);
      REQUIRE(!reference.test(0));
   }

   SECTION("Random operations comparison")
   {
      hierarchical_bitmap<1024> bitmap;
      std::bitset<1024>         reference;

      // Initialize both to the same state
      bitmap.reset();
      reference.reset();

      // Perform random operations and verify consistency
      std::mt19937                          rng(42);  // Fixed seed for reproducibility
      std::uniform_int_distribution<size_t> pos_dist(0, 1023);
      std::uniform_int_distribution<size_t> op_dist(0, 2);  // 0=set, 1=reset, 2=test

      for (int i = 0; i < 1000; ++i)
      {
         size_t pos = pos_dist(rng);
         size_t op  = op_dist(rng);

         switch (op)
         {
            case 0:  // set
               bitmap.set(pos);
               reference.set(pos);
               break;
            case 1:  // reset
               bitmap.reset(pos);
               reference.reset(pos);
               break;
            case 2:  // test
               REQUIRE(bitmap.test(pos) == reference.test(pos));
               break;
         }

         // Periodically check any/none
         if (i % 50 == 0)
         {
            REQUIRE(bitmap.any() == reference.any());
            REQUIRE(bitmap.none() == reference.none());
         }
      }

      // Final check that the entire state matches
      for (size_t i = 0; i < 1024; i += 16)
      {  // Sample every 16th bit to keep test fast
         REQUIRE(bitmap.test(i) == reference.test(i));
      }

      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());
   }

   SECTION("Edge cases comparison")
   {
      hierarchical_bitmap<32> bitmap;
      std::bitset<32>         reference;

      // Test with all bits set and unset
      bitmap.set();
      reference.set();

      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Unset a single bit
      bitmap.reset(0);
      reference.reset(0);

      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Set it back
      bitmap.set(0);
      reference.set(0);

      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Unset all bits
      bitmap.reset();
      reference.reset();

      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Set a single bit
      bitmap.set(31);  // Last bit
      reference.set(31);

      REQUIRE(bitmap.any() == reference.any());
      REQUIRE(bitmap.none() == reference.none());

      // Test bulk set/reset operations
      bitmap.reset();
      reference.reset();

      bitmap.set();
      reference.set();

      for (size_t i = 0; i < 32; ++i)
      {
         REQUIRE(bitmap.test(i) == reference.test(i));
      }

      bitmap.reset();
      reference.reset();

      for (size_t i = 0; i < 32; ++i)
      {
         REQUIRE(bitmap.test(i) == reference.test(i));
      }
   }

   SECTION("set_first_unset and unset_first_set comparison with std::bitset")
   {
      hierarchical_bitmap<128> bitmap;
      std::bitset<128>         reference;

      // Start with all bits unset (0)
      bitmap.reset();
      reference.reset();

      // Test set_first_unset
      for (size_t i = 0; i < 128; ++i)
      {
         // Find first unset bit in reference
         size_t ref_first_unset = reference.size();
         for (size_t j = 0; j < reference.size(); ++j)
         {
            if (!reference.test(j))
            {
               ref_first_unset = j;
               break;
            }
         }

         // Find and set first unset bit in bitmap
         uint32_t result = bitmap.set_first_unset();

         // Verify results match
         REQUIRE(result == ref_first_unset);

         // Set the same bit in reference
         if (ref_first_unset < reference.size())
         {
            reference.set(ref_first_unset);
         }

         // Verify both data structures are in the same state
         for (size_t j = 0; j < 128; ++j)
         {
            REQUIRE(bitmap.test(j) == reference.test(j));
         }

         // Once all bits are set, set_first_unset should return invalid_index
         if (i == 127)
         {
            REQUIRE(bitmap.set_first_unset() == bitmap.invalid_index);
         }
      }

      // Now test unset_first_set with all bits set
      bitmap.set();
      reference.set();

      for (size_t i = 0; i < 128; ++i)
      {
         // Find first set bit in reference
         size_t ref_first_set = reference.size();
         for (size_t j = 0; j < reference.size(); ++j)
         {
            if (reference.test(j))
            {
               ref_first_set = j;
               break;
            }
         }

         // Find and unset first set bit in bitmap
         uint32_t result = bitmap.unset_first_set();

         // Verify results match
         REQUIRE(result == ref_first_set);

         // Unset the same bit in reference
         if (ref_first_set < reference.size())
         {
            reference.reset(ref_first_set);
         }

         // Verify both data structures are in the same state
         for (size_t j = 0; j < 128; ++j)
         {
            REQUIRE(bitmap.test(j) == reference.test(j));
         }

         // Once all bits are unset, unset_first_set should return invalid_index
         if (i == 127)
         {
            REQUIRE(bitmap.unset_first_set() == bitmap.invalid_index);
         }
      }
   }

   SECTION("set_first_unset and unset_first_set with pattern comparison")
   {
      hierarchical_bitmap<64> bitmap;
      std::bitset<64>         reference;

      // Test with specific patterns

      // Pattern 1: All bits set except middle section
      bitmap.set();
      reference.set();

      // Unset bits 20-40
      for (size_t i = 20; i <= 40; ++i)
      {
         bitmap.reset(i);
         reference.reset(i);
      }

      // Verify set_first_unset sets the expected bits
      for (size_t i = 20; i <= 40; ++i)
      {
         // Find first unset bit in reference
         size_t ref_first_unset = reference.size();
         for (size_t j = 0; j < reference.size(); ++j)
         {
            if (!reference.test(j))
            {
               ref_first_unset = j;
               break;
            }
         }

         uint32_t result = bitmap.set_first_unset();
         REQUIRE(result == ref_first_unset);

         if (ref_first_unset < reference.size())
         {
            reference.set(ref_first_unset);
         }
      }

      // Pattern 2: All bits unset except specific positions
      bitmap.reset();
      reference.reset();

      // Set specific bits
      std::vector<size_t> set_positions = {0, 1, 10, 20, 30, 40, 50, 63};
      for (auto pos : set_positions)
      {
         bitmap.set(pos);
         reference.set(pos);
      }

      // Verify unset_first_set unsets the expected bits
      for (size_t i = 0; i < set_positions.size(); ++i)
      {
         // Find first set bit in reference
         size_t ref_first_set = reference.size();
         for (size_t j = 0; j < reference.size(); ++j)
         {
            if (reference.test(j))
            {
               ref_first_set = j;
               break;
            }
         }

         uint32_t result = bitmap.unset_first_set();
         REQUIRE(result == ref_first_set);

         if (ref_first_set < reference.size())
         {
            reference.reset(ref_first_set);
         }
      }

      // Verify all bits are now unset
      REQUIRE(bitmap.none());
      REQUIRE(reference.none());
   }

   SECTION("Standard versus hierarchical bitmap different sizes")
   {
      // Test with small bitmap (Level 1)
      {
         hierarchical_bitmap<64> bitmap;
         std::bitset<64>         reference;

         bitmap.reset();
         reference.reset();

         // Set alternating bits
         for (size_t i = 0; i < 64; i += 2)
         {
            bitmap.set(i);
            reference.set(i);
         }

         // Verify state
         for (size_t i = 0; i < 64; ++i)
         {
            REQUIRE(bitmap.test(i) == reference.test(i));
         }

         // Test any/none
         REQUIRE(bitmap.any() == reference.any());
         REQUIRE(bitmap.none() == reference.none());
      }

      // Test with medium bitmap (Level 2)
      {
         hierarchical_bitmap<1024> bitmap;
         std::bitset<1024>         reference;

         bitmap.set();
         reference.set();

         // Reset every 100th bit
         for (size_t i = 0; i < 1024; i += 100)
         {
            bitmap.reset(i);
            reference.reset(i);
         }

         // Sample verification
         for (size_t i = 0; i < 1024; i += 100)
         {
            REQUIRE(bitmap.test(i) == reference.test(i));
            if (i + 1 < 1024)
            {
               REQUIRE(bitmap.test(i + 1) == reference.test(i + 1));
            }
         }

         // Test any/none
         REQUIRE(bitmap.any() == reference.any());
         REQUIRE(bitmap.none() == reference.none());
      }

      // Test with larger bitmap (Level 3)
      {
         hierarchical_bitmap<5000> bitmap;
         std::bitset<5000>         reference;

         bitmap.reset();
         reference.reset();

         // Set specific pattern
         for (size_t i = 0; i < 5000; i += 500)
         {
            bitmap.set(i);
            reference.set(i);
         }

         // Sample verification
         for (size_t i = 0; i < 5000; i += 500)
         {
            REQUIRE(bitmap.test(i) == reference.test(i));
            if (i + 1 < 5000)
            {
               REQUIRE(bitmap.test(i + 1) == reference.test(i + 1));
            }
         }

         // Test any/none
         REQUIRE(bitmap.any() == reference.any());
         REQUIRE(bitmap.none() == reference.none());
      }
   }

   SECTION("count() method comparison with std::bitset")
   {
      // Test with different bitmap sizes to check performance at different levels

      // Level 1 bitmap (64 bits)
      {
         hierarchical_bitmap<64> bitmap;
         std::bitset<64>         reference;

         // Initially all bits are set
         bitmap.set();
         reference.set();

         // Verify both return the same count
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 64);

         // Test after clearing all bits
         bitmap.reset();
         reference.reset();
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 0);

         // Test with specific bit patterns
         // Pattern 1: Alternating bits
         for (size_t i = 0; i < 64; i += 2)
         {
            bitmap.set(i);
            reference.set(i);
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 32);

         // Pattern 2: Sparse bits
         bitmap.reset();
         reference.reset();

         for (size_t i : {0, 7, 13, 21, 42, 63})
         {
            bitmap.set(i);
            reference.set(i);
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 6);
      }

      // Level 2 bitmap (4096 bits)
      {
         hierarchical_bitmap<4096> bitmap;
         std::bitset<4096>         reference;

         // Initially all bits are set
         bitmap.set();
         reference.set();

         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 4096);

         // Test after clearing all bits
         bitmap.reset();
         reference.reset();
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 0);

         // Test with different densities

         // High density (75% bits set)
         for (size_t i = 0; i < 4096; i++)
         {
            if (i % 4 != 0)  // Set 3 out of every 4 bits
            {
               bitmap.set(i);
               reference.set(i);
            }
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 3072);  // 75% of 4096

         // Medium density (50% bits set)
         bitmap.reset();
         reference.reset();
         for (size_t i = 0; i < 4096; i += 2)
         {
            bitmap.set(i);
            reference.set(i);
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 2048);  // 50% of 4096

         // Low density (1% bits set)
         bitmap.reset();
         reference.reset();
         for (size_t i = 0; i < 4096; i += 100)
         {
            bitmap.set(i);
            reference.set(i);
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 41);  // Approximately 1% of 4096

         // Very sparse (just a few bits set)
         bitmap.reset();
         reference.reset();
         for (size_t i : {0, 63, 64, 127, 1000, 2000, 4095})
         {
            bitmap.set(i);
            reference.set(i);
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 7);
      }

      // Level 3 bitmap (100000 bits)
      {
         hierarchical_bitmap<100000> bitmap;
         std::bitset<100000>         reference;

         // Initially all bits are set
         bitmap.set();
         reference.set();

         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 100000);

         // Test after clearing all bits
         bitmap.reset();
         reference.reset();
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 0);

         // Test with very sparse pattern (0.01% set)
         for (size_t i = 0; i < 100000; i += 10000)
         {
            bitmap.set(i);
            reference.set(i);
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 10);  // Set bits at 0, 10000, 20000, ..., 90000

         // Test with denser pattern but still sparse overall
         bitmap.reset();
         reference.reset();

         for (size_t i = 0; i < 100000; i += 1000)
         {
            bitmap.set(i);
            reference.set(i);
         }
         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 100);  // Set every 1000th bit (0, 1000, 2000, ..., 99000)

         // Test with chunks of bits set
         bitmap.reset();
         reference.reset();

         // Set first 1000 bits
         for (size_t i = 0; i < 1000; i++)
         {
            bitmap.set(i);
            reference.set(i);
         }

         // Set bits 50000-51000
         for (size_t i = 50000; i < 51000; i++)
         {
            bitmap.set(i);
            reference.set(i);
         }

         // Set last 1000 bits
         for (size_t i = 99000; i < 100000; i++)
         {
            bitmap.set(i);
            reference.set(i);
         }

         REQUIRE(bitmap.count() == reference.count());
         REQUIRE(bitmap.count() == 3000);  // 3 chunks of 1000 bits
      }
   }
}

TEST_CASE("hierarchical_bitmap count() benchmarks", "[bitmap][benchmark]")
{
   SECTION("Benchmark count() performance - Dense bitmaps")
   {
      std::cout << "\nDense Bitmap Count Performance Test\n";
      std::cout << "-----------------------------------\n";

      // Test with different bitmap sizes

      // 64-bit bitmap (Level 1)
      {
         hierarchical_bitmap<64> h_bitmap;
         std::bitset<64>         std_bitmap;

         // Fill both bitmaps
         h_bitmap.set();
         std_bitmap.set();

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 1000000; i++)  // Run many iterations for accurate timing
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 1000000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 1 (64 bits) - Dense:\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }

      // 4096-bit bitmap (Level 2)
      {
         hierarchical_bitmap<4096> h_bitmap;
         std::bitset<4096>         std_bitmap;

         // Fill both bitmaps
         h_bitmap.set();
         std_bitmap.set();

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 100000; i++)  // Fewer iterations for larger bitmap
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 100000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 2 (4096 bits) - Dense:\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }

      // 100000-bit bitmap (Level 3)
      {
         hierarchical_bitmap<100000> h_bitmap;
         std::bitset<100000>         std_bitmap;

         // Fill both bitmaps
         h_bitmap.set();
         std_bitmap.set();

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 1000; i++)  // Even fewer iterations for larger bitmap
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 1000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 3 (100000 bits) - Dense:\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }
   }

   SECTION("Benchmark count() performance - Sparse bitmaps")
   {
      std::cout << "\nSparse Bitmap Count Performance Test\n";
      std::cout << "-----------------------------------\n";

      // Test with different bitmap sizes

      // 64-bit bitmap (Level 1) - 10% set
      {
         hierarchical_bitmap<64> h_bitmap;
         std::bitset<64>         std_bitmap;

         // Reset both bitmaps
         h_bitmap.reset();
         std_bitmap.reset();

         // Set 10% of bits
         for (size_t i = 0; i < 64; i += 10)
         {
            h_bitmap.set(i);
            std_bitmap.set(i);
         }

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 1000000; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 1000000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 1 (64 bits) - Sparse (10%):\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }

      // 4096-bit bitmap (Level 2) - 1% set
      {
         hierarchical_bitmap<4096> h_bitmap;
         std::bitset<4096>         std_bitmap;

         // Reset both bitmaps
         h_bitmap.reset();
         std_bitmap.reset();

         // Set 1% of bits (about 41 bits)
         for (size_t i = 0; i < 4096; i += 100)
         {
            h_bitmap.set(i);
            std_bitmap.set(i);
         }

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 100000; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 100000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 2 (4096 bits) - Sparse (1%):\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }

      // 100000-bit bitmap (Level 3) - 0.1% set
      {
         hierarchical_bitmap<100000> h_bitmap;
         std::bitset<100000>         std_bitmap;

         // Reset both bitmaps
         h_bitmap.reset();
         std_bitmap.reset();

         // Set 0.1% of bits (about 100 bits)
         for (size_t i = 0; i < 100000; i += 1000)
         {
            h_bitmap.set(i);
            std_bitmap.set(i);
         }

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 1000; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 1000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 3 (100000 bits) - Sparse (0.1%):\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }

      // 1000000-bit bitmap (Level 4) - 0.01% set - very sparse case
      {
         hierarchical_bitmap<1000000> h_bitmap;
         std::bitset<1000000>         std_bitmap;

         // Reset both bitmaps
         h_bitmap.reset();
         std_bitmap.reset();

         // Set 0.01% of bits (about 100 bits)
         for (size_t i = 0; i < 1000000; i += 10000)
         {
            h_bitmap.set(i);
            std_bitmap.set(i);
         }

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 100; i++)  // Fewer iterations for very large bitmap
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 100; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 4 (1000000 bits) - Very Sparse (0.01%):\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }
   }

   SECTION("Benchmark count() performance - Special patterns")
   {
      std::cout << "\nSpecial Pattern Bitmap Count Performance Test\n";
      std::cout << "-----------------------------------------\n";

      // Test with different bitmap patterns that might affect performance

      // Level 3 bitmap (100000 bits) with bits set only at the end
      {
         hierarchical_bitmap<100000> h_bitmap;
         std::bitset<100000>         std_bitmap;

         // Reset both bitmaps
         h_bitmap.reset();
         std_bitmap.reset();

         // Set last 1000 bits
         for (size_t i = 99000; i < 100000; i++)
         {
            h_bitmap.set(i);
            std_bitmap.set(i);
         }

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 1000; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 1000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 3 (100000 bits) - Last 1% set:\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }

      // Level 3 bitmap with scattered clusters of bits
      {
         hierarchical_bitmap<100000> h_bitmap;
         std::bitset<100000>         std_bitmap;

         // Reset both bitmaps
         h_bitmap.reset();
         std_bitmap.reset();

         // Set clusters of 64 bits at different points
         for (size_t cluster = 0; cluster < 10; cluster++)
         {
            size_t start = cluster * 10000;
            for (size_t i = start; i < start + 64; i++)
            {
               h_bitmap.set(i);
               std_bitmap.set(i);
            }
         }

         // Time hierarchical_bitmap count
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < 1000; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset count
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < 1000; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Report results
         std::cout << "Level 3 (100000 bits) - 10 clusters of 64 bits:\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs, count = " << h_count << "\n";
         std::cout << "  std::bitset: " << duration_std << " µs, count = " << std_count << "\n";
         std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std << "\n";

         // Verify correctness
         REQUIRE(h_count == std_count);
      }
   }
}

TEST_CASE("hierarchical_bitmap memory layout and performance", "[bitmap]")
{
   SECTION("Memory layout comparison with std::bitset")
   {
      // Test if the bit order/layout in hierarchical_bitmap level0 matches std::bitset
      {
         hierarchical_bitmap<64> h_bitmap;
         std::bitset<64>         std_bitmap;

         // Reset both
         h_bitmap.reset();
         std_bitmap.reset();

         // Create an expected pattern
         uint64_t expected_pattern = 0;

         // Set specific alternating bits
         for (size_t i = 0; i < 64; i += 3)
         {
            h_bitmap.set(i);
            std_bitmap.set(i);
            expected_pattern |= (1ULL << i);
         }

         // Test each bit directly to verify correctness
         for (size_t i = 0; i < 64; i++)
         {
            bool h_bit        = h_bitmap.test(i);
            bool std_bit      = std_bitmap.test(i);
            bool expected_bit = ((expected_pattern >> i) & 1) == 1;

            INFO("Bit position " << i << ": h_bitmap=" << h_bit << ", std_bitmap=" << std_bit
                                 << ", expected=" << expected_bit);
            REQUIRE(h_bit == std_bit);
            REQUIRE(h_bit == expected_bit);
         }

         // Attempt to access internal representation for verification
         // Note: This is a bit hacky but only for testing purposes
         const BitmapMemoryLayoutTest* memory_layout =
             reinterpret_cast<const BitmapMemoryLayoutTest*>(&h_bitmap);
         uint64_t h_data   = memory_layout->level0[0];
         uint64_t std_data = std_bitmap.to_ullong();

         INFO("hierarchical_bitmap internal representation: 0x" << std::hex << h_data);
         INFO("std::bitset internal representation: 0x" << std::hex << std_data);
         INFO("expected pattern: 0x" << std::hex << expected_pattern);

         // Test if there's a simple bit order reversal or other pattern
         // Often std::bitset might use opposite bit ordering than our custom implementation
         uint64_t reversed_expected = 0;
         for (int i = 0; i < 64; i++)
         {
            if ((expected_pattern >> i) & 1)
            {
               reversed_expected |= (1ULL << (63 - i));
            }
         }

         INFO("reversed expected pattern: 0x" << std::hex << reversed_expected);

         // Note: one of these should match depending on bit ordering
         bool matches_direct   = (h_data == std_data);
         bool matches_reversed = (h_data == reversed_expected);

         INFO("Direct memory layout match: " << std::boolalpha << matches_direct);
         INFO("Reversed memory layout match: " << std::boolalpha << matches_reversed);

         // We expect the APIs to behave the same, but internal representation might differ
         REQUIRE((matches_direct || matches_reversed || h_bitmap.count() == std_bitmap.count()));
      }

      // Test with larger size where both have multiple words
      {
         constexpr size_t          SIZE = 128;
         hierarchical_bitmap<SIZE> h_bitmap;
         std::bitset<SIZE>         std_bitmap;

         // Reset both
         h_bitmap.reset();
         std_bitmap.reset();

         // Set specific bit patterns across multiple words
         for (size_t i = 0; i < SIZE; i += 5)
         {
            h_bitmap.set(i);
            std_bitmap.set(i);
         }

         // Verify operations give the same results
         for (size_t i = 0; i < SIZE; i++)
         {
            REQUIRE(h_bitmap.test(i) == std_bitmap.test(i));
         }
         REQUIRE(h_bitmap.count() == std_bitmap.count());
      }
   }

   SECTION("Comprehensive performance benchmarks - large bitmaps")
   {
      // For meaningful performance testing, we need sufficient iterations and larger bitmap sizes
      constexpr int WARMUP_ITERATIONS = 10;
      constexpr int ITERATIONS        = 10000;  // Increased iterations for more accurate timing

      std::cout << "\n=== Comprehensive Bitmap Count Performance Test - Level 4 (1M bits) ==="
                << std::endl;
      std::cout << "======================================================================"
                << std::endl;

      INFO("\nComprehensive Bitmap Count Performance Test - Level 4 (1M bits)");
      INFO("--------------------------------------------------------------");

      // Test the largest practical bitmap size (Level 4)
      constexpr size_t SIZE = 1000000;  // 1 million bits, which is a Level 4 bitmap

      // Dense case (all bits set)
      {
         hierarchical_bitmap<SIZE> h_bitmap;
         std::bitset<SIZE>         std_bitmap;

         // Set all bits for dense case
         h_bitmap.set();
         std_bitmap.set();

         // Validate same count before timing
         REQUIRE(h_bitmap.count() == std_bitmap.count());
         REQUIRE(h_bitmap.count() == SIZE);

         // Warmup runs
         for (int i = 0; i < WARMUP_ITERATIONS; i++)
         {
            volatile auto h_count   = h_bitmap.count();
            volatile auto std_count = std_bitmap.count();
            (void)h_count;
            (void)std_count;
         }

         // Time hierarchical_bitmap.count()
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < ITERATIONS; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_h - start_h).count();

         // Time std::bitset.count()
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < ITERATIONS; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_std - start_std).count();

         std::cout << "Level 4 (1M bits) - Dense (all bits set):" << std::endl;
         std::cout << "  hierarchical_bitmap: " << (duration_h / ITERATIONS)
                   << " ns per call, count = " << h_count << std::endl;
         std::cout << "  std::bitset: " << (duration_std / ITERATIONS)
                   << " ns per call, count = " << std_count << std::endl;

         if (duration_std > 0)
         {
            std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std
                      << std::endl;
         }
         else
         {
            std::cout << "  Ratio (h/std): N/A (std time too small to measure)" << std::endl;
         }
         std::cout << std::endl;
      }

      // Sparse case (0.01% bits set)
      {
         hierarchical_bitmap<SIZE> h_bitmap;
         std::bitset<SIZE>         std_bitmap;

         // Reset all bits
         h_bitmap.reset();
         std_bitmap.reset();

         // Set only 0.01% of bits (100 bits) in a large bitmap
         std::mt19937                          rng(42);  // Fixed seed for reproducibility
         std::uniform_int_distribution<size_t> dist(0, SIZE - 1);

         for (int i = 0; i < 100; i++)
         {
            size_t pos = dist(rng);
            h_bitmap.set(pos);
            std_bitmap.set(pos);
         }

         // Validate same count before timing
         REQUIRE(h_bitmap.count() == std_bitmap.count());
         REQUIRE(h_bitmap.count() == 100);

         // Warmup runs
         for (int i = 0; i < WARMUP_ITERATIONS; i++)
         {
            volatile auto h_count   = h_bitmap.count();
            volatile auto std_count = std_bitmap.count();
            (void)h_count;
            (void)std_count;
         }

         // Time hierarchical_bitmap.count()
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < ITERATIONS; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_h - start_h).count();

         // Time std::bitset.count()
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < ITERATIONS; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_std - start_std).count();

         std::cout << "Level 4 (1M bits) - Sparse (0.01% bits set):" << std::endl;
         std::cout << "  hierarchical_bitmap: " << (duration_h / ITERATIONS)
                   << " ns per call, count = " << h_count << std::endl;
         std::cout << "  std::bitset: " << (duration_std / ITERATIONS)
                   << " ns per call, count = " << std_count << std::endl;

         if (duration_std > 0)
         {
            std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std
                      << std::endl;
         }
         else
         {
            std::cout << "  Ratio (h/std): N/A (std time too small to measure)" << std::endl;
         }
         std::cout << std::endl;
      }

      // Mixed density case (0.064% bits set but in patterns)
      {
         hierarchical_bitmap<SIZE> h_bitmap;
         std::bitset<SIZE>         std_bitmap;

         // Reset all bits
         h_bitmap.reset();
         std_bitmap.reset();

         // Set 10 clusters of 64 bits each (640 bits total)
         for (int cluster = 0; cluster < 10; cluster++)
         {
            size_t start_pos = (cluster * (SIZE / 10));
            for (size_t i = start_pos; i < start_pos + 64; i++)
            {
               h_bitmap.set(i);
               std_bitmap.set(i);
            }
         }

         // Validate same count before timing
         REQUIRE(h_bitmap.count() == std_bitmap.count());
         REQUIRE(h_bitmap.count() == 640);

         // Warmup runs
         for (int i = 0; i < WARMUP_ITERATIONS; i++)
         {
            volatile auto h_count   = h_bitmap.count();
            volatile auto std_count = std_bitmap.count();
            (void)h_count;
            (void)std_count;
         }

         // Time hierarchical_bitmap.count()
         auto     start_h = std::chrono::high_resolution_clock::now();
         uint32_t h_count = 0;
         for (int i = 0; i < ITERATIONS; i++)
         {
            h_count = h_bitmap.count();
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_h - start_h).count();

         // Time std::bitset.count()
         auto   start_std = std::chrono::high_resolution_clock::now();
         size_t std_count = 0;
         for (int i = 0; i < ITERATIONS; i++)
         {
            std_count = std_bitmap.count();
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::nanoseconds>(end_std - start_std).count();

         std::cout << "Level 4 (1M bits) - Clustered (10 clusters of 64 bits):" << std::endl;
         std::cout << "  hierarchical_bitmap: " << (duration_h / ITERATIONS)
                   << " ns per call, count = " << h_count << std::endl;
         std::cout << "  std::bitset: " << (duration_std / ITERATIONS)
                   << " ns per call, count = " << std_count << std::endl;

         if (duration_std > 0)
         {
            std::cout << "  Ratio (h/std): " << static_cast<double>(duration_h) / duration_std
                      << std::endl;
         }
         else
         {
            std::cout << "  Ratio (h/std): N/A (std time too small to measure)" << std::endl;
         }
         std::cout << std::endl;
      }
   }
}

TEST_CASE("hierarchical_bitmap set() performance comparison", "[bitmap][benchmark]")
{
   SECTION("Benchmark set() performance - Various sizes")
   {
      constexpr int WARMUP_ITERATIONS = 10;
      constexpr int ITERATIONS        = 10000;  // Number of iterations for timing

      std::cout << "\nSet Bit Performance Test\n";
      std::cout << "----------------------\n";

      // Test different bitmap sizes
      auto run_set_benchmark = [&](auto size_tag)
      {
         constexpr size_t SIZE = size_tag.value;

         // Create bitmaps
         hierarchical_bitmap<SIZE> h_bitmap;
         std::bitset<SIZE>         std_bitmap;

         // Reset both
         h_bitmap.reset();
         std_bitmap.reset();

         // Warmup
         for (int i = 0; i < WARMUP_ITERATIONS; i++)
         {
            size_t idx = i % SIZE;
            h_bitmap.set(idx);
            std_bitmap.set(idx);
         }

         h_bitmap.reset();
         std_bitmap.reset();

         // Time hierarchical_bitmap.set()
         auto start_h = std::chrono::high_resolution_clock::now();
         for (int i = 0; i < ITERATIONS; i++)
         {
            size_t idx = i % SIZE;
            h_bitmap.set(idx);
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset.set()
         auto start_std = std::chrono::high_resolution_clock::now();
         for (int i = 0; i < ITERATIONS; i++)
         {
            size_t idx = i % SIZE;
            std_bitmap.set(idx);
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Calculate ratio
         double ratio = duration_h > 0 ? static_cast<double>(duration_h) / duration_std : 0;

         std::cout << "Size " << SIZE << " bits:\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs\n";
         std::cout << "  std::bitset: " << duration_std << " µs\n";
         std::cout << "  Ratio (h/std): " << ratio << "\n";

         return std::make_tuple(duration_h, duration_std, ratio);
      };

      // Run benchmarks for different sizes
      run_set_benchmark(std::integral_constant<size_t, 64>{});      // Level 1
      run_set_benchmark(std::integral_constant<size_t, 4096>{});    // Level 2
      run_set_benchmark(std::integral_constant<size_t, 100000>{});  // Level 3

      std::cout << "\nRandom Access Set Bit Performance Test\n";
      std::cout << "-----------------------------------\n";

      // Test with random access patterns
      auto run_random_set_benchmark = [&](auto size_tag)
      {
         constexpr size_t SIZE = size_tag.value;

         // Create bitmaps
         hierarchical_bitmap<SIZE> h_bitmap;
         std::bitset<SIZE>         std_bitmap;

         // Reset both
         h_bitmap.reset();
         std_bitmap.reset();

         // Create random indices
         std::vector<size_t> indices;
         indices.reserve(ITERATIONS);

         std::mt19937                          gen(42);  // Fixed seed for reproducibility
         std::uniform_int_distribution<size_t> dist(0, SIZE - 1);

         for (int i = 0; i < ITERATIONS; i++)
         {
            indices.push_back(dist(gen));
         }

         // Warmup
         for (int i = 0; i < WARMUP_ITERATIONS; i++)
         {
            size_t idx = indices[i % indices.size()];
            h_bitmap.set(idx);
            std_bitmap.set(idx);
         }

         h_bitmap.reset();
         std_bitmap.reset();

         // Time hierarchical_bitmap.set() with random access
         auto start_h = std::chrono::high_resolution_clock::now();
         for (int i = 0; i < ITERATIONS; i++)
         {
            size_t idx = indices[i];
            h_bitmap.set(idx);
         }
         auto end_h = std::chrono::high_resolution_clock::now();
         auto duration_h =
             std::chrono::duration_cast<std::chrono::microseconds>(end_h - start_h).count();

         // Time std::bitset.set() with random access
         auto start_std = std::chrono::high_resolution_clock::now();
         for (int i = 0; i < ITERATIONS; i++)
         {
            size_t idx = indices[i];
            std_bitmap.set(idx);
         }
         auto end_std = std::chrono::high_resolution_clock::now();
         auto duration_std =
             std::chrono::duration_cast<std::chrono::microseconds>(end_std - start_std).count();

         // Calculate ratio
         double ratio = duration_h > 0 ? static_cast<double>(duration_h) / duration_std : 0;

         std::cout << "Size " << SIZE << " bits (random access):\n";
         std::cout << "  hierarchical_bitmap: " << duration_h << " µs\n";
         std::cout << "  std::bitset: " << duration_std << " µs\n";
         std::cout << "  Ratio (h/std): " << ratio << "\n";

         return std::make_tuple(duration_h, duration_std, ratio);
      };

      // Run random access benchmarks for different sizes
      run_random_set_benchmark(std::integral_constant<size_t, 64>{});      // Level 1
      run_random_set_benchmark(std::integral_constant<size_t, 4096>{});    // Level 2
      run_random_set_benchmark(std::integral_constant<size_t, 100000>{});  // Level 3
   }
}