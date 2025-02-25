#include <arbtrie/hierarchical_bitmap.hpp>
#include <catch2/catch_all.hpp>
#include <random>
#include <vector>

using namespace arbtrie;

TEST_CASE("hierarchical_bitmap basic operations", "[bitmap]")
{
   SECTION("Level 1 bitmap")
   {
      hierarchical_bitmap<64> bitmap;  // Level 1: up to 64 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming segments
      std::vector<uint32_t> claimed;
      for (int i = 0; i < 64; ++i)
      {
         auto segment = bitmap.claim_first_free();
         REQUIRE(segment != bitmap.invalid_segment);
         REQUIRE(segment < 64);
         REQUIRE(!bitmap.is_free(segment));
         claimed.push_back(segment);
      }

      // Test all segments are claimed
      REQUIRE(bitmap.find_first_free() == bitmap.invalid_segment);
      REQUIRE(bitmap.claim_first_free() == bitmap.invalid_segment);

      // Test freeing segments
      for (auto segment : claimed)
      {
         bitmap.mark_free(segment);
         REQUIRE(bitmap.is_free(segment));
      }

      // Test all segments are free again
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);
   }

   SECTION("Level 2 bitmap")
   {
      hierarchical_bitmap<4096> bitmap;  // Level 2: up to 4096 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming segments
      std::vector<uint32_t> claimed;
      for (int i = 0; i < 4096; ++i)
      {
         auto segment = bitmap.claim_first_free();
         REQUIRE(segment != bitmap.invalid_segment);
         REQUIRE(segment < 4096);
         REQUIRE(!bitmap.is_free(segment));
         claimed.push_back(segment);
      }

      // Test all segments are claimed
      REQUIRE(bitmap.find_first_free() == bitmap.invalid_segment);
      REQUIRE(bitmap.claim_first_free() == bitmap.invalid_segment);

      // Test freeing segments
      for (auto segment : claimed)
      {
         bitmap.mark_free(segment);
         REQUIRE(bitmap.is_free(segment));
      }

      // Test all segments are free again
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);
   }

   SECTION("Level 3 bitmap")
   {
      hierarchical_bitmap<262144> bitmap;  // Level 3: up to 262,144 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);  // Fixed seed for reproducibility
      std::uniform_int_distribution<uint32_t> dist(0, 262143);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.is_free(segment))
         {
            auto claimed = bitmap.claim_first_free();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(!bitmap.is_free(claimed));
         }
         else
         {
            bitmap.mark_free(segment);
            REQUIRE(bitmap.is_free(segment));
         }
      }
   }

   SECTION("Level 4 bitmap")
   {
      hierarchical_bitmap<16777216> bitmap;  // Level 4: up to 16,777,216 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);  // Fixed seed for reproducibility
      std::uniform_int_distribution<uint32_t> dist(0, 16777215);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.is_free(segment))
         {
            auto claimed = bitmap.claim_first_free();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(!bitmap.is_free(claimed));
         }
         else
         {
            bitmap.mark_free(segment);
            REQUIRE(bitmap.is_free(segment));
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
            auto segment = bitmap.claim_first_free();
            if (segment != bitmap.invalid_segment)
            {
               REQUIRE(!bitmap.is_free(segment));
               claimed_segments.push_back(segment);
            }
         }
         else if (op < 0.8)
         {  // 40% chance to free
            if (!claimed_segments.empty())
            {
               size_t   idx     = rng() % claimed_segments.size();
               uint32_t segment = claimed_segments[idx];
               bitmap.mark_free(segment);
               REQUIRE(bitmap.is_free(segment));
               claimed_segments.erase(claimed_segments.begin() + idx);
            }
         }
         else
         {  // 20% chance to check random segment
            uint32_t segment    = dist(rng);
            bool     is_claimed = std::find(claimed_segments.begin(), claimed_segments.end(),
                                            segment) != claimed_segments.end();
            REQUIRE(bitmap.is_free(segment) == !is_claimed);
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
      while ((segment = bitmap.claim_first_free()) != bitmap.invalid_segment)
      {
         REQUIRE(!bitmap.is_free(segment));
         segments.push_back(segment);
      }

      // Verify we can't claim more
      REQUIRE(bitmap.claim_first_free() == bitmap.invalid_segment);

      // Free all segments in reverse order
      while (!segments.empty())
      {
         segment = segments.back();
         segments.pop_back();
         bitmap.mark_free(segment);
         REQUIRE(bitmap.is_free(segment));
      }

      // Verify we can claim again
      REQUIRE(bitmap.claim_first_free() != bitmap.invalid_segment);
   }

   SECTION("Out of bounds checks")
   {
      hierarchical_bitmap<4096> bitmap;  // Level 2: up to 4096 segments

      // These should trigger the assertions in debug mode
      REQUIRE_NOTHROW(bitmap.is_free(4095));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.is_free(4096), std::runtime_error);  // First invalid segment

      REQUIRE_NOTHROW(bitmap.mark_free(4095));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.mark_free(4096), std::runtime_error);  // First invalid segment
   }
}

TEST_CASE("hierarchical_bitmap unaligned sizes", "[bitmap]")
{
   SECTION("Small unaligned size (50 segments)")
   {
      hierarchical_bitmap<50> bitmap;  // Level 1, but not full 64 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming all available segments
      std::vector<uint32_t> claimed;
      for (int i = 0; i < 50; ++i)
      {
         auto segment = bitmap.claim_first_free();
         REQUIRE(segment != bitmap.invalid_segment);
         REQUIRE(segment < 50);  // Should never exceed our size
         REQUIRE(!bitmap.is_free(segment));
         claimed.push_back(segment);
      }

      // Should be full now
      REQUIRE(bitmap.find_first_free() == bitmap.invalid_segment);
      REQUIRE(bitmap.claim_first_free() == bitmap.invalid_segment);

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.is_free(49));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.is_free(50), std::runtime_error);  // First invalid segment
   }

   SECTION("Mid-level unaligned size (3000 segments)")
   {
      hierarchical_bitmap<3000> bitmap;  // Level 2, but not full 4096 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 2999);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.is_free(segment))
         {
            auto claimed = bitmap.claim_first_free();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(claimed < 3000);  // Should never exceed our size
            REQUIRE(!bitmap.is_free(claimed));
         }
         else
         {
            bitmap.mark_free(segment);
            REQUIRE(bitmap.is_free(segment));
         }
      }

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.is_free(2999));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.is_free(3000), std::runtime_error);  // First invalid segment
   }

   SECTION("Large unaligned size (200000 segments)")
   {
      hierarchical_bitmap<200000> bitmap;  // Level 3, but not full 262144 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 199999);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.is_free(segment))
         {
            auto claimed = bitmap.claim_first_free();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(claimed < 200000);  // Should never exceed our size
            REQUIRE(!bitmap.is_free(claimed));
         }
         else
         {
            bitmap.mark_free(segment);
            REQUIRE(bitmap.is_free(segment));
         }
      }

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.is_free(199999));                        // Last valid segment
      REQUIRE_THROWS_AS(bitmap.is_free(200000), std::runtime_error);  // First invalid segment
   }

   SECTION("Very large unaligned size (10000000 segments)")
   {
      hierarchical_bitmap<10000000> bitmap;  // Level 4, but not full 16777216 segments

      // Test initial state
      REQUIRE(bitmap.find_first_free() != bitmap.invalid_segment);

      // Test claiming and freeing random segments
      std::mt19937                            rng(42);
      std::uniform_int_distribution<uint32_t> dist(0, 9999999);

      std::vector<uint32_t> segments;
      for (int i = 0; i < 1000; ++i)
      {
         uint32_t segment = dist(rng);
         segments.push_back(segment);

         if (bitmap.is_free(segment))
         {
            auto claimed = bitmap.claim_first_free();
            REQUIRE(claimed != bitmap.invalid_segment);
            REQUIRE(claimed < 10000000);  // Should never exceed our size
            REQUIRE(!bitmap.is_free(claimed));
         }
         else
         {
            bitmap.mark_free(segment);
            REQUIRE(bitmap.is_free(segment));
         }
      }

      // Verify bounds checking
      REQUIRE_NOTHROW(bitmap.is_free(9999999));                         // Last valid segment
      REQUIRE_THROWS_AS(bitmap.is_free(10000000), std::runtime_error);  // First invalid segment
   }

   SECTION("Edge case sizes")
   {
      // Test sizes right after level boundaries
      hierarchical_bitmap<65> bitmap1;  // Just over level 1
      REQUIRE(bitmap1.find_first_free() != bitmap1.invalid_segment);
      REQUIRE_NOTHROW(bitmap1.is_free(64));
      REQUIRE_THROWS_AS(bitmap1.is_free(65), std::runtime_error);

      hierarchical_bitmap<4097> bitmap2;  // Just over level 2
      REQUIRE(bitmap2.find_first_free() != bitmap2.invalid_segment);
      REQUIRE_NOTHROW(bitmap2.is_free(4096));
      REQUIRE_THROWS_AS(bitmap2.is_free(4097), std::runtime_error);

      // Test sizes just before level boundaries
      hierarchical_bitmap<63> bitmap3;  // Just under level 1
      REQUIRE(bitmap3.find_first_free() != bitmap3.invalid_segment);
      REQUIRE_NOTHROW(bitmap3.is_free(62));
      REQUIRE_THROWS_AS(bitmap3.is_free(63), std::runtime_error);

      hierarchical_bitmap<4095> bitmap4;  // Just under level 2
      REQUIRE(bitmap4.find_first_free() != bitmap4.invalid_segment);
      REQUIRE_NOTHROW(bitmap4.is_free(4094));
      REQUIRE_THROWS_AS(bitmap4.is_free(4095), std::runtime_error);
   }
}