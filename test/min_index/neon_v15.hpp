
#include <bitset>  // For std::bitset
#include <cassert>

#include <cstdint>
#include <iomanip>   // For setw
#include <iostream>  // For cout

#ifdef __ARM_NEON
#include <arm_neon.h>
#endif

// Function declaration for the match mask function provided by the user
#ifdef __ARM_NEON
/*
uint64_t NEON_i8x64_MatchMask(const uint8_t* ptr, uint8_t match_byte)
{
   uint8x16x4_t src  = vld4q_u8(ptr);
   uint8x16_t   dup  = vdupq_n_u8(match_byte);
   uint8x16_t   cmp0 = vceqq_u8(src.val[0], dup);
   uint8x16_t   cmp1 = vceqq_u8(src.val[1], dup);
   uint8x16_t   cmp2 = vceqq_u8(src.val[2], dup);
   uint8x16_t   cmp3 = vceqq_u8(src.val[3], dup);

   uint8x16_t t0 = vsriq_n_u8(cmp1, cmp0, 1);
   uint8x16_t t1 = vsriq_n_u8(cmp3, cmp2, 1);
   uint8x16_t t2 = vsriq_n_u8(t1, t0, 2);
   uint8x16_t t3 = vsriq_n_u8(t2, t2, 4);
   uint8x8_t  t4 = vshrn_n_u16(vreinterpretq_u16_u8(t3), 4);
   return vget_lane_u64(vreinterpret_u64_u8(t4), 0);
}
*/
inline uint64_t NEON_i8x64_MatchMask(uint8x16x4_t src, uint8_t match_byte)
{
   uint8x16_t dup  = vdupq_n_u8(match_byte);
   uint8x16_t cmp0 = vceqq_u8(src.val[0], dup);
   uint8x16_t cmp1 = vceqq_u8(src.val[1], dup);
   uint8x16_t cmp2 = vceqq_u8(src.val[2], dup);
   uint8x16_t cmp3 = vceqq_u8(src.val[3], dup);

   uint8x16_t t0 = vsriq_n_u8(cmp1, cmp0, 1);
   uint8x16_t t1 = vsriq_n_u8(cmp3, cmp2, 1);
   uint8x16_t t2 = vsriq_n_u8(t1, t0, 2);
   uint8x16_t t3 = vsriq_n_u8(t2, t2, 4);
   uint8x8_t  t4 = vshrn_n_u16(vreinterpretq_u16_u8(t3), 4);
   return vget_lane_u64(vreinterpret_u64_u8(t4), 0);
}
inline int first_match64(const uint16x8x4_t& chunk1,
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
   /*
   std::cout << "r0: " << r0 << "  " << r0d4 << " m4: " << r0m4 << " result: " << (r0d4 + 16 * r0m4)
             << "  vs " << index << "\n";
   std::cout << "l0: " << std::countl_zero(u64) / 4 << "   vs " << index << "\n";
   */
}
inline int first_match32(const uint16x8x4_t& chunk1, uint16_t match_byte)
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
   /*
   std::cout << "r0: " << r0 << "  " << r0d4 << " m4: " << r0m4 << " result: " << (r0d4 + 16 * r0m4)
             << "  vs " << index << "\n";
   std::cout << "l0: " << std::countl_zero(u64) / 4 << "   vs " << index << "\n";
   */
}

inline int NEON_i16x32_FirstMatch(uint16x8x4_t src, uint16_t match_word)
{
   uint16x8_t dup  = vdupq_n_u16(match_word);
   uint16x8_t cmp0 = vceqq_u16(src.val[0], dup);
   uint16x8_t cmp1 = vceqq_u16(src.val[1], dup);
   uint16x8_t cmp2 = vceqq_u16(src.val[2], dup);
   uint16x8_t cmp3 = vceqq_u16(src.val[3], dup);

   // combine 1 bit from cmp0 and cmp1 into first 2 bits of cmp1
   uint16x8_t t0 = vsriq_n_u16(cmp1, cmp0, 1);  // shift insert cmp0 into cmp1 by 1 bit
   // combine 1 bit from cmp2 and cmp3 into first 2 bits of cmp3
   uint16x8_t t1 = vsriq_n_u16(cmp3, cmp2, 1);  // shift insert cmp2 into cmp3 by 1 bit
   // combine 2 bits from cmp0&1 + 2 bits from cmp2&3 into first 4 bits of t2
   uint16x8_t t2 = vsriq_n_u16(t1, t0, 2);  // shift insert t0 into t1 by 2 bits
   uint16x8_t t3 = vsriq_n_u16(t2, t2, 4);  // shift insert t2 into t2 by 4 bits
   uint8x8_t  t4 = vshrn_n_u16(t3, 4);      // narrow the results to 8 bits

   uint64_t  mask     = vget_lane_u64(vreinterpret_u64_u8(t4), 0);
   int       idx      = __builtin_clzll(mask);
   const int lookup[] = {
       31, 23, 15, 7, -1, -1, -1,
       -1,  // 0-7
       30, 22, 14, 6, -1, -1, -1,
       -1,  // 8-15
       29, 21, 13, 5, -1, -1, -1,
       -1,  // 16-23
       28, 20, 12, 4, -1, -1, -1,
       -1,  // 24-31
       27, 19, 11, 3, -1, -1, -1,
       -1,  // 32-39
       26, 18, 10, 2, -1, -1, -1,
       -1,  // 40-47
       25, 17, 9,  1, -1, -1, -1,
       -1,  // 48-55
       24, 16, 8,  0,
   };
   return lookup[idx];
}

uint64_t neonmovemask_bulk(uint8x16_t p0, uint8x16_t p1, uint8x16_t p2, uint8x16_t p3)
{
   const uint8x16_t bitmask1 = {0x01, 0x10, 0x01, 0x10, 0x01, 0x10, 0x01, 0x10,
                                0x01, 0x10, 0x01, 0x10, 0x01, 0x10, 0x01, 0x10};
   const uint8x16_t bitmask2 = {0x02, 0x20, 0x02, 0x20, 0x02, 0x20, 0x02, 0x20,
                                0x02, 0x20, 0x02, 0x20, 0x02, 0x20, 0x02, 0x20};
   const uint8x16_t bitmask3 = {0x04, 0x40, 0x04, 0x40, 0x04, 0x40, 0x04, 0x40,
                                0x04, 0x40, 0x04, 0x40, 0x04, 0x40, 0x04, 0x40};
   const uint8x16_t bitmask4 = {0x08, 0x80, 0x08, 0x80, 0x08, 0x80, 0x08, 0x80,
                                0x08, 0x80, 0x08, 0x80, 0x08, 0x80, 0x08, 0x80};

   uint8x16_t t0  = vandq_u8(p0, bitmask1);
   uint8x16_t t1  = vbslq_u8(bitmask2, p1, t0);
   uint8x16_t t2  = vbslq_u8(bitmask3, p2, t1);
   uint8x16_t tmp = vbslq_u8(bitmask4, p3, t2);
   uint8x16_t sum = vpaddq_u8(tmp, tmp);
   return vgetq_lane_u64(vreinterpretq_u64_u8(sum), 0);
}

/// returns a mask with 2 bits for each bit in the source vector
inline uint64_t NEON_i16x32_MatchMask2(uint16x8x4_t src, uint16_t match_word)
{
   uint16x8_t dup  = vdupq_n_u16(match_word);
   uint16x8_t cmp0 = vceqq_u16(src.val[0], dup);
   uint16x8_t cmp1 = vceqq_u16(src.val[1], dup);
   uint16x8_t cmp2 = vceqq_u16(src.val[2], dup);
   uint16x8_t cmp3 = vceqq_u16(src.val[3], dup);

   const uint8x16_t bitmask1 = {0x01, 0x10, 0x01, 0x10, 0x01, 0x10, 0x01, 0x10,
                                0x01, 0x10, 0x01, 0x10, 0x01, 0x10, 0x01, 0x10};
   const uint8x16_t bitmask2 = {0x02, 0x20, 0x02, 0x20, 0x02, 0x20, 0x02, 0x20,
                                0x02, 0x20, 0x02, 0x20, 0x02, 0x20, 0x02, 0x20};
   const uint8x16_t bitmask3 = {0x04, 0x40, 0x04, 0x40, 0x04, 0x40, 0x04, 0x40,
                                0x04, 0x40, 0x04, 0x40, 0x04, 0x40, 0x04, 0x40};
   const uint8x16_t bitmask4 = {0x08, 0x80, 0x08, 0x80, 0x08, 0x80, 0x08, 0x80,
                                0x08, 0x80, 0x08, 0x80, 0x08, 0x80, 0x08, 0x80};

   uint8x16_t p0 = vreinterpretq_u8_u16(cmp0);
   uint8x16_t p1 = vreinterpretq_u8_u16(cmp1);
   uint8x16_t p2 = vreinterpretq_u8_u16(cmp2);
   uint8x16_t p3 = vreinterpretq_u8_u16(cmp3);

   uint8x16_t t0  = vandq_u8(p0, bitmask1);
   uint8x16_t t1  = vbslq_u8(bitmask2, p1, t0);
   uint8x16_t t2  = vbslq_u8(bitmask3, p2, t1);
   uint8x16_t tmp = vbslq_u8(bitmask4, p3, t2);
   uint8x16_t sum = vpaddq_u8(tmp, tmp);
   return vgetq_lane_u64(vreinterpretq_u64_u8(sum), 0);
}
inline uint32_t NEON_i16x32_FindFirst(uint16x8x4_t src, uint16_t match_word)
{
   uint16x8_t dup  = vdupq_n_u16(match_word);
   uint16x8_t cmp0 = vceqq_u16(src.val[0], dup);
   uint16x8_t cmp1 = vceqq_u16(src.val[1], dup);
   uint16x8_t cmp2 = vceqq_u16(src.val[2], dup);
   uint16x8_t cmp3 = vceqq_u16(src.val[3], dup);

   uint16x8_t indices0 = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices1 = {8, 9, 10, 11, 12, 13, 14, 15};
   uint16x8_t indices2 = {16, 17, 18, 19, 20, 21, 22, 23};
   uint16x8_t indices3 = {24, 25, 26, 27, 28, 29, 30, 31};
   uint16x8_t indices4 = {32, 33, 34, 35, 36, 37, 38, 39};
   uint16x8_t indices5 = {40, 41, 42, 43, 44, 45, 46, 47};
   uint16x8_t indices6 = {48, 49, 50, 51, 52, 53, 54, 55};
   uint16x8_t indices7 = {56, 57, 58, 59, 60, 61, 62, 63};

   // Insert counter bits into index values:
   // Shift counters right to clear lower bits, then insert at bit position 6
   // This preserves the index in lower 6 bits and puts counter value in upper bits
   indices0 = vsliq_n_u16(indices0, cmp0, 6);
   indices1 = vsliq_n_u16(indices1, cmp1, 6);
   indices2 = vsliq_n_u16(indices2, cmp2, 6);
   indices3 = vsliq_n_u16(indices3, cmp3, 6);
   return 0;
}
// Match mask function for 16-bit words (32 elements)
inline uint32_t NEON_i16x32_MatchMask(uint16x8x4_t src, uint16_t match_word)
{
   uint16x8_t dup = vdupq_n_u16(match_word);

   // Compare each element to match word (0xFFFF for match, 0 for non-match)
   uint16x8_t cmp0 = vceqq_u16(src.val[0], dup);  // bits 0-7
   uint16x8_t cmp1 = vceqq_u16(src.val[1], dup);  // bits 8-15
   uint16x8_t cmp2 = vceqq_u16(src.val[2], dup);  // bits 16-23
   uint16x8_t cmp3 = vceqq_u16(src.val[3], dup);  // bits 24-31

   uint16_t pre_shift0[8], pre_shift1[8], pre_shift2[8], pre_shift3[8];
   vst1q_u16(pre_shift0, cmp0);
   vst1q_u16(pre_shift1, cmp1);
   vst1q_u16(pre_shift2, cmp2);
   vst1q_u16(pre_shift3, cmp3);

   // Create NEON vectors directly with bit masks for positions
   // Each vector has bits set according to element position
   uint16x8_t mask0 = {0x0001, 0x0002, 0x0004, 0x0008, 0x0010, 0x0020, 0x0040, 0x0080};  // Bits 0-7
   uint16x8_t mask1 = {0x0100, 0x0200, 0x0400, 0x0800,
                       0x1000, 0x2000, 0x4000, 0x8000};  // Bits 8-15

   // Apply masks using logic AND - if element matches, corresponding bit will be set
   uint16x8_t masked0 = vandq_u16(cmp0, mask0);  // Bits 0-7
   uint16x8_t masked1 = vandq_u16(cmp1, mask1);  // Bits 8-15
   uint16x8_t masked2 = vandq_u16(cmp2, mask0);  // Bits 16-23 (reusing mask0)
   uint16x8_t masked3 = vandq_u16(cmp3, mask1);  // Bits 24-31 (reusing mask1)

   // Combine results with bitwise OR for each pair
   uint16x8_t combined_low  = vorrq_u16(masked0, masked1);  // Bits 0-15
   uint16x8_t combined_high = vorrq_u16(masked2, masked3);  // Bits 16-31 (will be shifted later)

   // Sum all elements in combined vectors using horizontal add
   // First reduction - reduce each vector separately
   uint16x8_t sum_vec_low  = vpaddq_u16(combined_low, combined_low);    // Pairwise add low
   uint16x8_t sum_vec_high = vpaddq_u16(combined_high, combined_high);  // Pairwise add high
   // Combine the partially reduced vectors
   uint16x8_t combined_sum = vcombine_u16(vget_low_u16(sum_vec_low), vget_low_u16(sum_vec_high));

   // Continue reduction on the combined vector
   combined_sum = vpaddq_u16(combined_sum, combined_sum);  // Second reduction

   combined_sum = vpaddq_u16(combined_sum, combined_sum);  // Final reduction

   // Extract the sums as a single 32-bit value (low 16 bits = sum_low, high 16 bits = sum_high)
   uint32_t sum_combined = vgetq_lane_u32(vreinterpretq_u32_u16(combined_sum), 0);

   return sum_combined;
}
#endif

/**
 * Find the approximate index of the minimum value in an array of 64 16-bit counters
 * using NEON SIMD instructions.
 *
 * This v15 implementation:
 * 1. Uses optimized loading of chunks
 * 2. Skips index blending and directly finds the global minimum 
 * 3. Uses NEON_i8x64_MatchMask to create bit masks where values equal the minimum
 * 4. Uses ctz (count trailing zeros) to efficiently identify the index
 *
 * @param original_counters Pointer to the array of counters (must be 128-byte aligned)
 * @param start The starting index in the array
 * @return The index of the minimum value (or an approximation for large values)
 */
#if defined(__ARM_NEON)
int find_approx_min_index_neon_v15_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Load 8 chunks in 2 operations (32 elements each)
   uint16x8x4_t chunks1;
   uint16x8x4_t chunks2;

   // Load first 32 elements (4 chunks of 8) with a single instruction, do not change
   chunks1 = vld1q_u16_x4(&original_counters[start]);
   // Load second 32 elements (4 chunks of 8) with a single instruction, do not change
   chunks2 = vld1q_u16_x4(&original_counters[start + 32]);

   // Find minimum of each chunk and directly create a vector with all minimums
   uint16x8_t all_mins = {vminvq_u16(chunks1.val[0]), vminvq_u16(chunks1.val[1]),
                          vminvq_u16(chunks1.val[2]), vminvq_u16(chunks1.val[3]),
                          vminvq_u16(chunks2.val[0]), vminvq_u16(chunks2.val[1]),
                          vminvq_u16(chunks2.val[2]), vminvq_u16(chunks2.val[3])};

   // Find the global minimum with a single NEON operation
   uint16_t global_min = vminvq_u16(all_mins);

   // Verify global_min exists in original counters
   // Debug print: show original counters and highlight those matching global_min
   /*
   std::cout << "Original counters (global min = " << global_min << "):" << std::endl;
   const int width = 6;  // Width for each counter in output

   // Print first row - counters with highlighting for global min
   for (int i = 0; i < 64; i++)
   {
      bool is_min = (original_counters[start + i] == global_min);
      if (is_min)
      {
         std::cout << "\033[1;31m";  // Red text for minimum values
      }
      std::cout << std::setw(width) << original_counters[start + i];
      if (is_min)
      {
         std::cout << "\033[0m";  // Reset text color
      }
   }
   std::cout << std::endl;
   */

   return start + first_match64(chunks1, chunks2, global_min);
   // Initialize mask as uint64x2_t

   uint64_t mask0 = NEON_i16x32_MatchMask(chunks1, global_min);
   uint64_t mask1 = uint64_t(NEON_i16x32_MatchMask(chunks2, global_min)) << 32;
   uint64_t mask  = mask0 | mask1;

   //  std::cout << "mask: " << std::bitset<64>(mask) << std::endl;
   // std::cout << "right 0: " << __builtin_ctzll(mask) << std::endl;

   // return start + NEON_i16x32_FirstMatch(chunks1, global_min);
   return start + __builtin_ctzll(mask);
}

/**
 * Find the approximate index of the minimum value in an array of 32 16-bit counters
 * using NEON SIMD instructions.
 *
 * This v15 implementation:
 * 1. Uses optimized loading of chunks
 * 2. Skips index blending and directly finds the global minimum 
 * 3. Uses NEON_i8x64_MatchMask to create a bit mask where values equal the minimum
 * 4. Uses ctz (count trailing zeros) to efficiently identify the index
 *
 * @param original_counters Pointer to the array of counters (must be 128-byte aligned)
 * @param start The starting index in the array
 * @return The index of the minimum value (or an approximation for large values)
 */
int find_approx_min_index_neon_v15_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   uint16x8x4_t chunks1;

   // Load 32 elements (4 chunks of 8) with a single instruction
   chunks1 = vld1q_u16_x4(&original_counters[start]);

   // Find minimums in each chunk using vminvq_u16
   uint16_t min0 = vminvq_u16(chunks1.val[0]);
   uint16_t min1 = vminvq_u16(chunks1.val[1]);
   uint16_t min2 = vminvq_u16(chunks1.val[2]);
   uint16_t min3 = vminvq_u16(chunks1.val[3]);

   // Find minimum of each chunk and directly create a vector with all minimums
   uint16x8_t all_mins = {min0, min1, min2, min3, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF};

   // Find the global minimum with a single NEON operation
   uint16_t global_min = vminvq_u16(all_mins);

   return start + first_match32(chunks1, global_min);

   // Create mask using the 16-bit match mask function
   uint32_t mask = NEON_i16x32_MatchMask(chunks1, global_min);
   // uint64_t mask2 = NEON_i16x32_MatchMask2(chunks1, global_min);
   //std::cout << "mask: " << std::bitset<32>(mask) << std::endl;
   //std::cout << "mask2: " << std::bitset<64>(mask2) << std::endl;

   // Use count trailing zeros to find the index of the first match
   return start + __builtin_ctz(mask);
}
#else
// Fallback for non-NEON platforms
int find_approx_min_index_neon_v15_64(uint16_t* original_counters, int start)
{
   // Simple scalar implementation for non-NEON platforms
   int      min_idx = start;
   uint16_t min_val = original_counters[start];

   for (int i = start + 1; i < start + 64; i++)
   {
      if (original_counters[i] < min_val)
      {
         min_val = original_counters[i];
         min_idx = i;
      }
   }

   return min_idx;
}

int find_approx_min_index_neon_v15_32(uint16_t* original_counters, int start)
{
   // Simple scalar implementation for non-NEON platforms
   int      min_idx = start;
   uint16_t min_val = original_counters[start];

   for (int i = start + 1; i < start + 32; i++)
   {
      if (original_counters[i] < min_val)
      {
         min_val = original_counters[i];
         min_idx = i;
      }
   }

   return min_idx;
}
#endif