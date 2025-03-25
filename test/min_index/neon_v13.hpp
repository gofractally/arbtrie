#pragma once
#include <cstdint>
#include "utils.hpp"

// Forward declaration for the scalar implementations
int find_approx_min_index_scalar_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start);
int find_approx_min_index_scalar(uint16_t* __attribute__((aligned(128))) original_counters,
                                 int                                     start1,
                                 int                                     start2);

#if defined(__ARM_NEON)
#include <arm_neon.h>

// NEON v13 implementation (64 values)
int find_approx_min_index_neon_v13_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start + 32]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start + 40]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start + 48]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start + 56]);

   // Create index vectors directly without loading from arrays
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
   indices0 = vsliq_n_u16(indices0, chunk0, 6);
   indices1 = vsliq_n_u16(indices1, chunk1, 6);
   indices2 = vsliq_n_u16(indices2, chunk2, 6);
   indices3 = vsliq_n_u16(indices3, chunk3, 6);
   indices4 = vsliq_n_u16(indices4, chunk4, 6);
   indices5 = vsliq_n_u16(indices5, chunk5, 6);
   indices6 = vsliq_n_u16(indices6, chunk6, 6);
   indices7 = vsliq_n_u16(indices7, chunk7, 6);

   // Find minimum of each chunk and directly create a vector with all minimums
   //uint16_t   min_values[8] = ;
   uint16x8_t all_mins = {vminvq_u16(indices0), vminvq_u16(indices1), vminvq_u16(indices2),
                          vminvq_u16(indices3), vminvq_u16(indices4), vminvq_u16(indices5),
                          vminvq_u16(indices6), vminvq_u16(indices7)};

   //vld1q_u16(min_values);

   // Find the global minimum with a single NEON operation
   uint16_t global_min = vminvq_u16(all_mins);

   // Extract the original index from the combined value
   return start + (global_min & 0x3F);
}

// NEON v13 implementation (32 values)
int find_approx_min_index_neon_v13_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start + 24]);

   // Create index vectors directly
   uint16x8_t indices0 = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices1 = {8, 9, 10, 11, 12, 13, 14, 15};
   uint16x8_t indices2 = {16, 17, 18, 19, 20, 21, 22, 23};
   uint16x8_t indices3 = {24, 25, 26, 27, 28, 29, 30, 31};

   // Insert counter bits into index values:
   // Shift counters right to clear lower bits, then insert at bit position 5
   // This preserves the index in lower 5 bits and puts counter value in upper bits
   indices0 = vsliq_n_u16(indices0, chunk0, 5);
   indices1 = vsliq_n_u16(indices1, chunk1, 5);
   indices2 = vsliq_n_u16(indices2, chunk2, 5);
   indices3 = vsliq_n_u16(indices3, chunk3, 5);

   // Find minimum in each chunk directly with local variables
   uint16_t min0 = vminvq_u16(indices0);
   uint16_t min1 = vminvq_u16(indices1);
   uint16_t min2 = vminvq_u16(indices2);
   uint16_t min3 = vminvq_u16(indices3);

   // Tournament-style comparisons for the final minimums
   uint16_t min01      = (min0 < min1) ? min0 : min1;
   uint16_t min23      = (min2 < min3) ? min2 : min3;
   uint16_t global_min = (min01 < min23) ? min01 : min23;

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
int find_approx_min_index_neon_v13(uint16_t* __attribute__((aligned(128))) original_counters,
                                   int                                     start1,
                                   int                                     start2)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar(original_counters, start1, start2);
}

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v13_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_32(original_counters, start);
}

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v13_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_64(original_counters, start);
}
#endif