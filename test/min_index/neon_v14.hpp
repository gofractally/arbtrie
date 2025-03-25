#pragma once
#include <cstdint>
#include "utils.hpp"

// Forward declaration for the scalar implementations
int find_approx_min_index_scalar_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start);
int find_approx_min_index_scalar(uint16_t* __attribute__((aligned(128))) original_counters,
                                 int                                     start1,
                                 int                                     start2);
int find_approx_min_index_scalar_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start);

#if defined(__ARM_NEON)
#include <arm_neon.h>

// NEON v14 implementation (64 values) with optimized loading
int find_approx_min_index_neon_v14_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Load 8 chunks in 2 operations (32 elements each)
   uint16x8x4_t chunks1;
   uint16x8x4_t chunks2;

   // Load first 32 elements (4 chunks of 8)
   chunks1.val[0] = vld1q_u16(&original_counters[start]);
   chunks1.val[1] = vld1q_u16(&original_counters[start + 8]);
   chunks1.val[2] = vld1q_u16(&original_counters[start + 16]);
   chunks1.val[3] = vld1q_u16(&original_counters[start + 24]);

   // Load second 32 elements (4 chunks of 8)
   chunks2.val[0] = vld1q_u16(&original_counters[start + 32]);
   chunks2.val[1] = vld1q_u16(&original_counters[start + 40]);
   chunks2.val[2] = vld1q_u16(&original_counters[start + 48]);
   chunks2.val[3] = vld1q_u16(&original_counters[start + 56]);

   // Find minimum of each chunk and directly create a vector with all minimums
   uint16_t min_values[8] = {vminvq_u16(chunks1.val[0]), vminvq_u16(chunks1.val[1]),
                             vminvq_u16(chunks1.val[2]), vminvq_u16(chunks1.val[3]),
                             vminvq_u16(chunks2.val[0]), vminvq_u16(chunks2.val[1]),
                             vminvq_u16(chunks2.val[2]), vminvq_u16(chunks2.val[3])};

   // Load all minimums into a single vector for final comparison
   uint16x8_t all_mins = vld1q_u16(min_values);

   // Find the global minimum with a single NEON operation
   uint16_t global_min = vminvq_u16(all_mins);

   uint16x8_t dup  = vdupq_n_u16(global_min);
   uint16x8_t cmp0 = (vceqq_u16(chunks1.val[0], dup));
   uint16x8_t cmp1 = (vceqq_u16(chunks1.val[1], dup));
   uint16x8_t cmp2 = (vceqq_u16(chunks1.val[2], dup));
   uint16x8_t cmp3 = (vceqq_u16(chunks1.val[3], dup));
   uint16x8_t cmp4 = (vceqq_u16(chunks2.val[0], dup));
   uint16x8_t cmp5 = (vceqq_u16(chunks2.val[1], dup));
   uint16x8_t cmp6 = (vceqq_u16(chunks2.val[2], dup));
   uint16x8_t cmp7 = (vceqq_u16(chunks2.val[3], dup));

   // Create index vectors directly without loading from arrays
   uint16x8_t indices0 = {1, 2, 3, 4, 5, 6, 7, 8};
   uint16x8_t indices1 = {9, 10, 11, 12, 13, 14, 15, 16};
   uint16x8_t indices2 = {17, 18, 19, 20, 21, 22, 23, 24};
   uint16x8_t indices3 = {25, 26, 27, 28, 29, 30, 31, 32};
   uint16x8_t indices4 = {33, 34, 35, 36, 37, 38, 39, 40};
   uint16x8_t indices5 = {41, 42, 43, 44, 45, 46, 47, 48};
   uint16x8_t indices6 = {49, 50, 51, 52, 53, 54, 55, 56};
   uint16x8_t indices7 = {57, 58, 59, 60, 61, 62, 63, 64};

   // Insert counter bits into index values:
   // Shift counters right to clear lower bits, then insert at bit position 6
   // This preserves the index in lower 6 bits and puts counter value in upper bits
   indices0 = vsliq_n_u16(indices0, cmp0, 8);
   indices1 = vsliq_n_u16(indices1, cmp1, 8);
   indices2 = vsliq_n_u16(indices2, cmp2, 8);
   indices3 = vsliq_n_u16(indices3, cmp3, 8);
   indices4 = vsliq_n_u16(indices4, cmp4, 8);
   indices5 = vsliq_n_u16(indices5, cmp5, 8);
   indices6 = vsliq_n_u16(indices6, cmp6, 8);
   indices7 = vsliq_n_u16(indices7, cmp7, 8);

   uint16x8_t max_values2 = {vmaxvq_u16(indices0), vmaxvq_u16(indices1), vmaxvq_u16(indices2),
                             vmaxvq_u16(indices3), vmaxvq_u16(indices4), vmaxvq_u16(indices5),
                             vmaxvq_u16(indices6), vmaxvq_u16(indices7)};

   uint16_t max_values2_arr[8];
   vst1q_u16(max_values2_arr, max_values2);

   // Narrow max_values2 to 8-bit values
   uint8x8_t narrow_maxvals_low  = vmovn_u16(max_values2);
   uint8x8_t narrow_maxvals_high = vshrn_n_u16(max_values2, 8);
   // Get trailing zeros from narrow_maxvals interpreted as uint64
   uint64_t narrow_bits_low  = vget_lane_u64(vreinterpret_u64_u8(narrow_maxvals_low), 0);
   uint64_t narrow_bits_high = vget_lane_u64(vreinterpret_u64_u8(narrow_maxvals_high), 0);
   auto     narrow_bits      = narrow_bits_low & narrow_bits_high;
   // printf("narrow_bits: %016lx %016lx  %016lx\n", narrow_bits_low, narrow_bits_high, narrow_bits);
   auto byte    = __builtin_ctzll(narrow_bits) / 8;
   auto shifted = narrow_bits >> byte * 8;
   shifted &= 0xff;
   return start + shifted - 1;
   //return start + __builtin_ctzll(narrow_bits) + byte * 8;
   //   uint16_t min_values2[8] = {vminvq_u16(indices0), vminvq_u16(indices1), vminvq_u16(indices2),
   //                             vminvq_u16(indices3), vminvq_u16(indices4), vminvq_u16(indices5),
   //                            vminvq_u16(indices6), vminvq_u16(indices7)};

   // Load all minimums into a single vector for final comparison
   //   uint16x8_t all_mins2 = vld1q_u16(max_values2);

   // Find the global minimum with a single NEON operation
   //uint16_t global_min2 = vmaxvq_u16(all_mins2);

   // Extract the original index from the combined value
   //  return start + (global_min2 & 0x3F);
}

// NEON v14 implementation (32 values) with optimized loading
int find_approx_min_index_neon_v14_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Load all 32 elements (4 chunks of 8) in one structured load
   uint16x8x4_t chunks;
   chunks.val[0] = vld1q_u16(&original_counters[start]);
   chunks.val[1] = vld1q_u16(&original_counters[start + 8]);
   chunks.val[2] = vld1q_u16(&original_counters[start + 16]);
   chunks.val[3] = vld1q_u16(&original_counters[start + 24]);

   // Create index vectors directly
   uint16x8_t indices0 = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices1 = {8, 9, 10, 11, 12, 13, 14, 15};
   uint16x8_t indices2 = {16, 17, 18, 19, 20, 21, 22, 23};
   uint16x8_t indices3 = {24, 25, 26, 27, 28, 29, 30, 31};

   // Insert counter bits into index values:
   // Shift counters right to clear lower bits, then insert at bit position 5
   // This preserves the index in lower 5 bits and puts counter value in upper bits
   indices0 = vsliq_n_u16(indices0, chunks.val[0], 5);
   indices1 = vsliq_n_u16(indices1, chunks.val[1], 5);
   indices2 = vsliq_n_u16(indices2, chunks.val[2], 5);
   indices3 = vsliq_n_u16(indices3, chunks.val[3], 5);

   // Find minimum in each chunk directly
   uint16_t min0 = vminvq_u16(indices0);
   uint16_t min1 = vminvq_u16(indices1);
   uint16_t min2 = vminvq_u16(indices2);
   uint16_t min3 = vminvq_u16(indices3);

   // Load all minimums into a single vector for final comparison
   uint16x4_t all_mins = {min0, min1, min2, min3};

   // Find global minimum with a single NEON operation
   uint16_t global_min = vminv_u16(all_mins);

   // Extract the original index from the combined value (mask with 0x1F for 5 bits)
   return start + (global_min & 0x1F);
}

#else
// Forward declaration
int find_approx_min_index_scalar_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start);
int find_approx_min_index_scalar(uint16_t* __attribute__((aligned(128))) original_counters,
                                 int                                     start1,
                                 int                                     start2);

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v14_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_32(original_counters, start);
}

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v14_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_64(original_counters, start);
}
#endif