#pragma once
#include <array>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

/**
 * @file hierarchical_bitmap.hpp
 * 
 * A hierarchical bitmap implementation optimized for efficiently managing large sets of segments
 * or blocks, particularly useful for memory and storage allocation systems. The bitmap uses a
 * multi-level tree structure to provide O(1) operations for finding, claiming, and freeing segments.
 * 
 * Design Overview:
 * ---------------
 * The bitmap is organized in a hierarchical tree structure where:
 * - Level 0 (bottom): Contains the actual segment allocation bits (1 bit per segment)
 * - Level 1-N (above): Each bit represents whether any segments are free in the subtree below
 * 
 * Capacity and Storage Requirements:
 * -------------------------------
 * For managing N segments, storage is calculated per level, where each level manages
 * groups of 64 segments from the level below. Storage at each level is rounded up
 * to the nearest word (64 bits).
 * 
 * Level 1 (N ≤ 64):
 * - Level 0: ceil(N/64) words
 * - Total: 1 word
 * 
 * Level 2 (64 < N ≤ 4096):
 * - Level 0: ceil(N/64) words
 * - Level 1: 1 word
 * - Total: ceil(N/64) + 1 words
 * 
 * Level 3 (4096 < N ≤ 262144):
 * - Level 0: ceil(N/64) words
 * - Level 1: ceil(N/4096) words
 * - Level 2: 1 word
 * - Total: ceil(N/64) + ceil(N/4096) + 1 words
 * 
 * Level 4 (262144 < N ≤ 16777216):
 * - Level 0: ceil(N/64) words
 * - Level 1: ceil(N/4096) words
 * - Level 2: ceil(N/262144) words
 * - Level 3: 1 word
 * - Total: ceil(N/64) + ceil(N/4096) + ceil(N/262144) + 1 words
 * 
 * Example sizes:
 * - 50 segments: 1 word (8 bytes)
 * - 1000 segments: 17 words (136 bytes)
 * - 10000 segments: 159 words (1,272 bytes)
 * - 1000000 segments: 15,760 words (126,080 bytes)
 * 
 * Note: Storage is allocated statically at compile time based on MaxSegments,
 * with arrays sized exactly as needed for each level.
 * 
 * Key Features:
 * ------------
 * 1. Cache-Friendly Design:
 *    - Uses 64-bit words aligned on cache lines
 *    - Minimizes memory accesses when searching for free segments
 *    - Higher levels provide quick rejection of fully allocated regions
 * 
 * 2. Fast Operations:
 *    - O(1) claim_first_free(): Find and claim the first available segment
 *    - O(1) mark_free(): Release a previously claimed segment
 *    - O(1) is_free(): Check if a specific segment is available
 *    - O(1) find_first_free(): Look up first free segment without claiming
 * 
 * 3. Memory Efficiency:
 *    - Minimal overhead per segment (1 bit per segment plus hierarchy)
 *    - Automatic coalescing of free space information in higher levels
 *    - No additional metadata required per segment
 * 
 * Usage Example:
 * -------------
 * @code
 * hierarchical_bitmap<4096> bitmap;  // Creates a bitmap supporting 4,096 segments
 * 
 * // Claim a segment
 * uint32_t segment = bitmap.claim_first_free();
 * if (segment != bitmap.invalid_segment) {
 *     // Use the segment...
 * }
 * 
 * // Release it when done
 * bitmap.mark_free(segment);
 * @endcode
 * 
 * Implementation Notes:
 * -------------------
 * - Uses __builtin_ctzll for efficient first-set-bit finding
 * - Maintains parent bits automatically when child states change
 * - Throws std::runtime_error for out-of-bounds access
 * - Specialized implementations for levels 1-4 for optimal performance
 * 
 * Performance Characteristics:
 * -------------------------
 * - claim_first_free(): 2-5 memory accesses (depending on level)
 * - mark_free(): 1-4 memory accesses (depending on level)
 * - is_free(): 1 memory access
 * - find_first_free(): Same as claim_first_free() but no writes
 * 
 * Worst Case Memory Usage (64-bit words):
 * -----------
 * - Level 1: 8 bytes (1 word)
 * - Level 2: 520 bytes (65 words)
 * - Level 3: 33KB (4,161 words)
 * - Level 4: 2.1MB (266,305 words)
 */

namespace arbtrie
{
   template <uint32_t MaxSegments>
   class hierarchical_bitmap
   {
     public:
      static constexpr uint32_t bits_per_word   = 64;  // Number of bits in a uint64_t
      static constexpr uint32_t invalid_segment = std::numeric_limits<uint32_t>::max();

      // Helper to calculate required level for N segments
      static constexpr uint32_t calc_level(uint64_t n)
      {
         uint32_t level    = 1;
         uint64_t capacity = bits_per_word;
         while (capacity < n)
         {
            capacity *= bits_per_word;
            level++;
         }
         return level;
      }

      // Helper to calculate number of words needed at each level
      static constexpr uint32_t calc_level_words(uint32_t level, uint64_t max_segments)
      {
         if (level == 0)
         {
            return (max_segments + bits_per_word - 1) / bits_per_word;
         }
         return (calc_level_words(level - 1, max_segments) + bits_per_word - 1) / bits_per_word;
      }

     private:
      static constexpr uint32_t required_level = calc_level(MaxSegments);

      // Calculate array sizes for each level at compile time
      static constexpr uint32_t l0_words = calc_level_words(0, MaxSegments);
      static constexpr uint32_t l1_words = calc_level_words(1, MaxSegments);
      static constexpr uint32_t l2_words = calc_level_words(2, MaxSegments);
      static constexpr uint32_t l3_words = calc_level_words(3, MaxSegments);

      // Level arrays sized exactly as needed
      alignas(64) uint64_t level0[l0_words];
      alignas(64) uint64_t level1[required_level > 1 ? l1_words : 0];
      alignas(64) uint64_t level2[required_level > 2 ? l2_words : 0];
      alignas(64) uint64_t level3[required_level > 3 ? l3_words : 0];

     public:
      // Helper to mask off bits beyond MaxSegments in the last word
      static constexpr uint64_t get_last_word_mask()
      {
         uint32_t bits_in_last_word = MaxSegments % bits_per_word;
         if (bits_in_last_word == 0)
            return ~0ULL;                         // All bits if perfectly aligned
         return (1ULL << bits_in_last_word) - 1;  // Only bits needed for MaxSegments
      }

      // Constructor: Initialize all bits to 1 (free)
      hierarchical_bitmap()
      {
         // Initialize all full words
         for (uint32_t i = 0; i < l0_words - 1; ++i)
            level0[i] = ~0ULL;

         // Initialize last word with proper mask
         level0[l0_words - 1] = get_last_word_mask();

         if constexpr (required_level > 1)
         {
            for (auto& x : level1)
               x = ~0ULL;
            if constexpr (required_level > 2)
            {
               for (auto& x : level2)
                  x = ~0ULL;
               if constexpr (required_level > 3)
               {
                  for (auto& x : level3)
                     x = ~0ULL;
               }
            }
         }
      }

      uint32_t find_first_free() const
      {
         if constexpr (required_level == 1)
         {
            if (level0[0] == 0)
               return invalid_segment;
            uint32_t bit = __builtin_ctzll(level0[0]);
            return bit >= MaxSegments ? invalid_segment : bit;
         }
         else if constexpr (required_level == 2)
         {
            if (level1[0] == 0)
               return invalid_segment;
            uint32_t l1_bit  = __builtin_ctzll(level1[0]);
            uint64_t l0_val  = level0[l1_bit];
            uint32_t bit_pos = __builtin_ctzll(l0_val);
            uint32_t segment = (l1_bit * bits_per_word) + bit_pos;
            return segment >= MaxSegments ? invalid_segment : segment;
         }
         else if constexpr (required_level == 3)
         {
            if (level2[0] == 0)
               return invalid_segment;
            uint32_t l2_bit  = __builtin_ctzll(level2[0]);
            uint64_t l1_val  = level1[l2_bit];
            uint32_t l1_bit  = __builtin_ctzll(l1_val);
            uint32_t l0_idx  = (l2_bit * bits_per_word) + l1_bit;
            uint64_t l0_val  = level0[l0_idx];
            uint32_t bit_pos = __builtin_ctzll(l0_val);
            uint32_t segment = (l0_idx * bits_per_word) + bit_pos;
            return segment >= MaxSegments ? invalid_segment : segment;
         }
         else
         {
            if (level3[0] == 0)
               return invalid_segment;
            uint32_t l3_bit  = __builtin_ctzll(level3[0]);
            uint64_t l2_val  = level2[l3_bit];
            uint32_t l2_bit  = __builtin_ctzll(l2_val);
            uint32_t l1_idx  = (l3_bit * bits_per_word) + l2_bit;
            uint64_t l1_val  = level1[l1_idx];
            uint32_t l1_bit  = __builtin_ctzll(l1_val);
            uint32_t l0_idx  = (l1_idx * bits_per_word) + l1_bit;
            uint64_t l0_val  = level0[l0_idx];
            uint32_t bit_pos = __builtin_ctzll(l0_val);
            uint32_t segment = (l0_idx * bits_per_word) + bit_pos;
            return segment >= MaxSegments ? invalid_segment : segment;
         }
      }

      uint32_t claim_first_free()
      {
         if constexpr (required_level == 1)
         {
            return claim_level1();
         }
         else if constexpr (required_level == 2)
         {
            return claim_level2();
         }
         else if constexpr (required_level == 3)
         {
            return claim_level3();
         }
         else
         {
            return claim_level4();
         }
      }

      void mark_free(uint32_t idx)
      {
         if (idx >= MaxSegments)
            throw std::runtime_error("Index out of range");

         uint32_t l0_idx  = idx / bits_per_word;
         uint32_t bit_pos = idx % bits_per_word;
         uint64_t old_l0  = level0[l0_idx];
         level0[l0_idx] |= (1ULL << bit_pos);

         if constexpr (required_level > 1)
         {
            if (old_l0 == 0)
            {
               uint32_t l1_idx = l0_idx / bits_per_word;
               uint32_t l1_bit = l0_idx % bits_per_word;
               uint64_t old_l1 = level1[l1_idx];
               level1[l1_idx] |= (1ULL << l1_bit);

               if constexpr (required_level > 2)
               {
                  if (old_l1 == 0)
                  {
                     uint32_t l2_idx = l1_idx / bits_per_word;
                     uint32_t l2_bit = l1_idx % bits_per_word;
                     uint64_t old_l2 = level2[l2_idx];
                     level2[l2_idx] |= (1ULL << l2_bit);

                     if constexpr (required_level > 3)
                     {
                        if (old_l2 == 0)
                        {
                           uint32_t l3_bit = l2_idx % bits_per_word;
                           level3[0] |= (1ULL << l3_bit);
                        }
                     }
                  }
               }
            }
         }
      }

      bool is_free(uint32_t idx) const
      {
         if (idx >= MaxSegments)
            throw std::runtime_error("Index out of range");

         uint32_t l0_idx  = idx / bits_per_word;
         uint32_t bit_pos = idx % bits_per_word;
         return (level0[l0_idx] & (1ULL << bit_pos)) != 0;
      }

     private:
      uint32_t claim_level1()
      {
         if (level0[0] == 0)
            return invalid_segment;
         uint32_t bit = __builtin_ctzll(level0[0]);
         level0[0] &= ~(1ULL << bit);
         return bit;
      }

      uint32_t claim_level2()
      {
         if (level1[0] == 0)
            return invalid_segment;
         uint32_t l1_bit  = __builtin_ctzll(level1[0]);
         uint64_t l0_val  = level0[l1_bit];
         uint32_t bit_pos = __builtin_ctzll(l0_val);
         uint32_t segment = (l1_bit * bits_per_word) + bit_pos;
         if (segment >= MaxSegments)
            return invalid_segment;
         level0[l1_bit] &= ~(1ULL << bit_pos);
         if (level0[l1_bit] == 0)
            level1[0] &= ~(1ULL << l1_bit);
         return segment;
      }

      uint32_t claim_level3()
      {
         if (level2[0] == 0)
            return invalid_segment;
         uint32_t l2_bit  = __builtin_ctzll(level2[0]);
         uint64_t l1_val  = level1[l2_bit];
         uint32_t l1_bit  = __builtin_ctzll(l1_val);
         uint32_t l0_idx  = (l2_bit * bits_per_word) + l1_bit;
         uint64_t l0_val  = level0[l0_idx];
         uint32_t bit_pos = __builtin_ctzll(l0_val);
         uint32_t segment = (l0_idx * bits_per_word) + bit_pos;
         if (segment >= MaxSegments)
            return invalid_segment;
         level0[l0_idx] &= ~(1ULL << bit_pos);
         if (level0[l0_idx] == 0)
         {
            level1[l2_bit] &= ~(1ULL << l1_bit);
            if (level1[l2_bit] == 0)
               level2[0] &= ~(1ULL << l2_bit);
         }
         return segment;
      }

      uint32_t claim_level4()
      {
         if (level3[0] == 0)
            return invalid_segment;
         uint32_t l3_bit  = __builtin_ctzll(level3[0]);
         uint64_t l2_val  = level2[l3_bit];
         uint32_t l2_bit  = __builtin_ctzll(l2_val);
         uint32_t l1_idx  = (l3_bit * bits_per_word) + l2_bit;
         uint64_t l1_val  = level1[l1_idx];
         uint32_t l1_bit  = __builtin_ctzll(l1_val);
         uint32_t l0_idx  = (l1_idx * bits_per_word) + l1_bit;
         uint64_t l0_val  = level0[l0_idx];
         uint32_t bit_pos = __builtin_ctzll(l0_val);
         uint32_t segment = (l0_idx * bits_per_word) + bit_pos;
         if (segment >= MaxSegments)
            return invalid_segment;
         level0[l0_idx] &= ~(1ULL << bit_pos);
         if (level0[l0_idx] == 0)
         {
            level1[l1_idx] &= ~(1ULL << l1_bit);
            if (level1[l1_idx] == 0)
            {
               level2[l3_bit] &= ~(1ULL << l2_bit);
               if (level2[l3_bit] == 0)
                  level3[0] &= ~(1ULL << l3_bit);
            }
         }
         return segment;
      }
   };

}  // namespace arbtrie