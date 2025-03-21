#pragma once
#include <cstdint>
#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace sal
{
#if defined(__ARM_NEON)
   inline int first_match64_neon(const uint16x8x4_t& chunk1,
                                 const uint16x8x4_t& chunk2,
                                 uint16_t            match_byte)
   {
      uint16x8_t dup          = vdupq_n_u16(match_byte);
      uint16x8_t eqmask_low   = vceqq_u16(chunk1.val[0], dup);
      uint16x8_t eqmask_high  = vceqq_u16(chunk1.val[1], dup);
      uint16x8_t eqmask_low2  = vceqq_u16(chunk1.val[2], dup);
      uint16x8_t eqmask_high2 = vceqq_u16(chunk1.val[3], dup);

      uint16x8_t c2eqmask_low   = vceqq_u16(chunk2.val[0], dup);
      uint16x8_t c2eqmask_high  = vceqq_u16(chunk2.val[1], dup);
      uint16x8_t c2eqmask_low2  = vceqq_u16(chunk2.val[2], dup);
      uint16x8_t c2eqmask_high2 = vceqq_u16(chunk2.val[3], dup);

      auto low   = vshrn_n_u16(eqmask_low, 4);
      auto high  = vshrn_n_u16(eqmask_high, 4);
      auto low2  = vshrn_n_u16(eqmask_low2, 4);
      auto high2 = vshrn_n_u16(eqmask_high2, 4);
      auto c0    = vcombine_u8(low, high);
      auto c1    = vcombine_u8(low2, high2);

      auto c2low   = vshrn_n_u16(c2eqmask_low, 4);
      auto c2high  = vshrn_n_u16(c2eqmask_high, 4);
      auto c2low2  = vshrn_n_u16(c2eqmask_low2, 4);
      auto c2high2 = vshrn_n_u16(c2eqmask_high2, 4);
      auto c2      = vcombine_u8(c2low, c2high);
      auto c3      = vcombine_u8(c2low2, c2high2);

      uint8x16_t t0 = vsriq_n_u8(c1, c0, 1);
      uint8x16_t t1 = vsriq_n_u8(c3, c2, 1);
      uint8x16_t t2 = vsriq_n_u8(t1, t0, 2);
      uint8x16_t t3 = vsriq_n_u8(t2, t2, 4);
      uint8x8_t  t4 = vshrn_n_u16(vreinterpretq_u16_u8(t3), 4);

      uint64_t u64  = vget_lane_u64(vreinterpret_u64_u8(t4), 0);
      auto     r0   = std::countr_zero(u64);
      auto     r0d4 = r0 / 4;
      auto     r0m4 = r0 % 4;
      return r0d4 + 16 * r0m4;
   }
   inline int first_match32_neon(const uint16x8x4_t& chunk1, uint16_t match_byte)
   {
      uint16x8_t dup          = vdupq_n_u16(match_byte);
      uint16x8_t eqmask_low   = vceqq_u16(chunk1.val[0], dup);
      uint16x8_t eqmask_high  = vceqq_u16(chunk1.val[1], dup);
      uint16x8_t eqmask_low2  = vceqq_u16(chunk1.val[2], dup);
      uint16x8_t eqmask_high2 = vceqq_u16(chunk1.val[3], dup);

      auto low   = vshrn_n_u16(eqmask_low, 4);
      auto high  = vshrn_n_u16(eqmask_high, 4);
      auto low2  = vshrn_n_u16(eqmask_low2, 4);
      auto high2 = vshrn_n_u16(eqmask_high2, 4);
      auto c0    = vcombine_u8(low, high);
      auto c1    = vcombine_u8(low2, high2);

      uint8x16_t t0 = vsriq_n_u8(c1, c0, 1);
      uint8x16_t t1 = vdupq_n_u8(0);  //vsriq_n_u8(c3, c2, 1);
      uint8x16_t t2 = vsriq_n_u8(t1, t0, 2);
      uint8x16_t t3 = vsriq_n_u8(t2, t2, 4);
      uint8x8_t  t4 = vshrn_n_u16(vreinterpretq_u16_u8(t3), 4);

      uint64_t u64  = vget_lane_u64(vreinterpret_u64_u8(t4), 0);
      auto     r0   = std::countr_zero(u64);
      auto     r0d4 = r0 / 4;
      auto     r0m4 = r0 % 4;
      return r0d4 + 16 * r0m4;
   }

   inline int find_min_index64_neon(uint16_t* __attribute__((aligned(64))) original_counters)
   {
      // Inform compiler about alignment
      original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 64);

      // Load 8 chunks in 2 operations (32 elements each)
      // Load first 32 elements (4 chunks of 8) with a single instruction, do not change
      uint16x8x4_t chunks1 = vld1q_u16_x4(original_counters);
      // Load second 32 elements (4 chunks of 8) with a single instruction, do not change
      uint16x8x4_t chunks2 = vld1q_u16_x4(original_counters + 32);

      // Find minimum of each chunk and directly create a vector with all minimums
      uint16x8_t all_mins = {vminvq_u16(chunks1.val[0]), vminvq_u16(chunks1.val[1]),
                             vminvq_u16(chunks1.val[2]), vminvq_u16(chunks1.val[3]),
                             vminvq_u16(chunks2.val[0]), vminvq_u16(chunks2.val[1]),
                             vminvq_u16(chunks2.val[2]), vminvq_u16(chunks2.val[3])};

      // Find the global minimum with a single NEON operation
      uint16_t global_min = vminvq_u16(all_mins);

      return first_match64_neon(chunks1, chunks2, global_min);
   }
   inline int find_min_index32_neon(uint16_t* __attribute__((aligned(64))) original_counters)
   {
      // Inform compiler about alignment
      original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 64);

      // Load 32 elements (4 chunks of 8) with a single instruction
      uint16x8x4_t chunks1 = vld1q_u16_x4(original_counters);

      // Find minimums in each chunk using vminvq_u16
      uint16_t min0 = vminvq_u16(chunks1.val[0]);
      uint16_t min1 = vminvq_u16(chunks1.val[1]);
      uint16_t min2 = vminvq_u16(chunks1.val[2]);
      uint16_t min3 = vminvq_u16(chunks1.val[3]);

      // Find minimum of each chunk and directly create a vector with all minimums
      uint16x8_t all_mins = {min0, min1, min2, min3, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF};

      // Find the global minimum with a single NEON operation
      uint16_t global_min = vminvq_u16(all_mins);

      return first_match32_neon(chunks1, global_min);
   }
#endif

   /**
    * @return the index [0, 31] of the value in the array that is the minimum
    * Implements a tournament-style reduction algorithm with branchless comparisons
    */
   inline int find_min_index32_tournament(const uint16_t* values)
   {
      // Store values and indices combined: high 16 bits = value, low 16 bits = index
      uint32_t tournament[16];

      // First round: combine values and indices (32 -> 16)
      for (int i = 0, j = 0; i < 32; i += 2, j++)
      {
         // Compare values and select winner with index
         int cmp = (values[i + 1] < values[i]);
         // Store value in high 16 bits, index in low 16 bits
         tournament[j] = ((uint32_t)values[i + cmp] << 16) | (uint32_t)(i + cmp);
      }

      // Round 2: 8 comparisons (16 -> 8)
      for (int i = 0, j = 0; i < 16; i += 2, j++)
      {
         // Direct comparison of combined values
         int cmp       = (tournament[i + 1] < tournament[i]);
         tournament[j] = tournament[i + cmp];
      }

      // Round 3: 4 comparisons (8 -> 4)
      for (int i = 0, j = 0; i < 8; i += 2, j++)
      {
         // Direct comparison of combined values
         int cmp       = (tournament[i + 1] < tournament[i]);
         tournament[j] = tournament[i + cmp];
      }

      // Round 4: 2 comparisons (4 -> 2) - unrolled
      int cmp1      = (tournament[1] < tournament[0]);
      tournament[0] = tournament[cmp1];

      int cmp2      = (tournament[3] < tournament[2]);
      tournament[1] = tournament[2 + cmp2];

      // Final round: 1 comparison (2 -> 1)
      int cmp = (tournament[1] < tournament[0]);

      // Return the index part (low 16 bits) of the winner
      return tournament[cmp] & 0xFFFF;
   }

   /**
    * @return the index [0, 63] of the value in the array that is the minimum
    * Implements a tournament-style reduction algorithm with branchless comparisons
    */
   inline int find_min_index64_tournament(const uint16_t* values)
   {
      // Store values and indices combined: high 16 bits = value, low 16 bits = index
      uint32_t tournament[32];

      // First round: combine values and indices (64 -> 32)
      for (int i = 0, j = 0; i < 64; i += 2, j++)
      {
         // Compare values and select winner with index
         int cmp = (values[i + 1] < values[i]);
         // Store value in high 16 bits, index in low 16 bits
         tournament[j] = ((uint32_t)values[i + cmp] << 16) | (uint32_t)(i + cmp);
      }

      // Round 2: 16 comparisons (32 -> 16)
      for (int i = 0, j = 0; i < 32; i += 2, j++)
      {
         // Direct comparison of combined values
         int cmp       = (tournament[i + 1] < tournament[i]);
         tournament[j] = tournament[i + cmp];
      }

      // Round 3: 8 comparisons (16 -> 8)
      for (int i = 0, j = 0; i < 16; i += 2, j++)
      {
         // Direct comparison of combined values
         int cmp       = (tournament[i + 1] < tournament[i]);
         tournament[j] = tournament[i + cmp];
      }

      // Round 4: 4 comparisons (8 -> 4)
      for (int i = 0, j = 0; i < 8; i += 2, j++)
      {
         // Direct comparison of combined values
         int cmp       = (tournament[i + 1] < tournament[i]);
         tournament[j] = tournament[i + cmp];
      }

      // Round 5: 2 comparisons (4 -> 2) - unrolled
      int cmp1      = (tournament[1] < tournament[0]);
      tournament[0] = tournament[cmp1];

      int cmp2      = (tournament[3] < tournament[2]);
      tournament[1] = tournament[2 + cmp2];

      // Final round: 1 comparison (2 -> 1)
      int cmp = (tournament[1] < tournament[0]);

      // Return the index part (low 16 bits) of the winner
      return tournament[cmp] & 0xFFFF;
   }

   /**
    * @return the index [0, 31] of one of the values in the array that is the minimum
    */
   inline int find_min_index_32(const uint16_t* values)
   {
#if defined(__ARM_NEON)
      return find_min_index32_neon(const_cast<uint16_t*>(values));
#else
      return find_min_index32_tournament(values);
#endif
   }
   inline int find_min_index_64(const uint16_t* values)
   {
#if defined(__ARM_NEON)
      return find_min_index64_neon(const_cast<uint16_t*>(values));
#else
      return find_min_index64_tournament(values);
#endif
   }
}  // namespace sal
