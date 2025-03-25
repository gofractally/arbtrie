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

// Forward declaration for the NEON implementations
int find_approx_min_index_neon_v11_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start);

// NEON v11 implementation (32 values)
int find_approx_min_index_neon_v11_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start + 24]);

   // Create mask with all bits set except lowest 5 bits (0xFFE0)
   uint16x8_t mask = vdupq_n_u16(0xFFE0);

   // Create index vectors
   const uint16_t index_values0[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   const uint16_t index_values1[8] = {8, 9, 10, 11, 12, 13, 14, 15};
   const uint16_t index_values2[8] = {16, 17, 18, 19, 20, 21, 22, 23};
   const uint16_t index_values3[8] = {24, 25, 26, 27, 28, 29, 30, 31};

   uint16x8_t indices0 = vld1q_u16(index_values0);
   uint16x8_t indices1 = vld1q_u16(index_values1);
   uint16x8_t indices2 = vld1q_u16(index_values2);
   uint16x8_t indices3 = vld1q_u16(index_values3);

   // Process all chunks in parallel: mask out lowest 5 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);

   // Combine masked counter with index
   uint16x8_t combined0 = vorrq_u16(masked_chunk0, indices0);
   uint16x8_t combined1 = vorrq_u16(masked_chunk1, indices1);
   uint16x8_t combined2 = vorrq_u16(masked_chunk2, indices2);
   uint16x8_t combined3 = vorrq_u16(masked_chunk3, indices3);

   // Find minimums in each chunk using vminvq_u16
   uint16_t mins_array[4] = {vminvq_u16(combined0), vminvq_u16(combined1), vminvq_u16(combined2),
                             vminvq_u16(combined3)};

   // Tournament-style comparisons for the final minimums
   uint16_t min01      = (mins_array[0] < mins_array[1]) ? mins_array[0] : mins_array[1];
   uint16_t min23      = (mins_array[2] < mins_array[3]) ? mins_array[2] : mins_array[3];
   uint16_t global_min = (min01 < min23) ? min01 : min23;

   // Extract the original index from the combined value (mask with 0x1F for 5 bits)
   return start + (global_min & 0x1F);
}

// NEON v11b implementation (32 values with sentinel approach)
int find_approx_min_index_neon_v11b_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                       int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start + 24]);

   // Create mask with all bits set except lowest 5 bits (0xFFE0)
   uint16x8_t mask = vdupq_n_u16(0xFFE0);

   // Create index vectors
   const uint16_t index_values0[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   const uint16_t index_values1[8] = {8, 9, 10, 11, 12, 13, 14, 15};
   const uint16_t index_values2[8] = {16, 17, 18, 19, 20, 21, 22, 23};
   const uint16_t index_values3[8] = {24, 25, 26, 27, 28, 29, 30, 31};

   uint16x8_t indices0 = vld1q_u16(index_values0);
   uint16x8_t indices1 = vld1q_u16(index_values1);
   uint16x8_t indices2 = vld1q_u16(index_values2);
   uint16x8_t indices3 = vld1q_u16(index_values3);

   // Process all chunks in parallel: mask out lowest 5 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);

   // Combine masked counter with index
   uint16x8_t combined0 = vorrq_u16(masked_chunk0, indices0);
   uint16x8_t combined1 = vorrq_u16(masked_chunk1, indices1);
   uint16x8_t combined2 = vorrq_u16(masked_chunk2, indices2);
   uint16x8_t combined3 = vorrq_u16(masked_chunk3, indices3);

   // Find minimums in each chunk using vminvq_u16
   uint16_t mins_array[8] = {
       vminvq_u16(combined0),
       vminvq_u16(combined1),
       vminvq_u16(combined2),
       vminvq_u16(combined3),
       0xFFFF,
       0xFFFF,
       0xFFFF,
       0xFFFF  // Add sentinel values that will never be chosen as minimum
   };

   uint16x8_t all_mins = vld1q_u16(mins_array);

   // Find the global minimum
   uint16_t global_min = vminvq_u16(all_mins);

   // Extract the original index from the combined value (mask with 0x1F for 5 bits)
   return start + (global_min & 0x1F);
}

// NEON v11 implementation (64 values)
int find_approx_min_index_neon_v11_64(uint16_t* __attribute__((aligned(128))) original_counters,
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

   // Create mask with all bits set except lowest 6 bits (0xFFC0)
   uint16x8_t mask = vdupq_n_u16(0xFFC0);

   // Create index vectors directly
   uint16x8_t indices0 = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices1 = {8, 9, 10, 11, 12, 13, 14, 15};
   uint16x8_t indices2 = {16, 17, 18, 19, 20, 21, 22, 23};
   uint16x8_t indices3 = {24, 25, 26, 27, 28, 29, 30, 31};
   uint16x8_t indices4 = {32, 33, 34, 35, 36, 37, 38, 39};
   uint16x8_t indices5 = {40, 41, 42, 43, 44, 45, 46, 47};
   uint16x8_t indices6 = {48, 49, 50, 51, 52, 53, 54, 55};
   uint16x8_t indices7 = {56, 57, 58, 59, 60, 61, 62, 63};

   // Process all chunks in parallel: mask out lowest 6 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);
   uint16x8_t masked_chunk4 = vandq_u16(chunk4, mask);
   uint16x8_t masked_chunk5 = vandq_u16(chunk5, mask);
   uint16x8_t masked_chunk6 = vandq_u16(chunk6, mask);
   uint16x8_t masked_chunk7 = vandq_u16(chunk7, mask);

   // Combine masked counter with index
   uint16x8_t combined0 = vorrq_u16(masked_chunk0, indices0);
   uint16x8_t combined1 = vorrq_u16(masked_chunk1, indices1);
   uint16x8_t combined2 = vorrq_u16(masked_chunk2, indices2);
   uint16x8_t combined3 = vorrq_u16(masked_chunk3, indices3);
   uint16x8_t combined4 = vorrq_u16(masked_chunk4, indices4);
   uint16x8_t combined5 = vorrq_u16(masked_chunk5, indices5);
   uint16x8_t combined6 = vorrq_u16(masked_chunk6, indices6);
   uint16x8_t combined7 = vorrq_u16(masked_chunk7, indices7);

   // Find minimums in each chunk and create a vector with all minimums in one step
   uint16x8_t all_mins = {vminvq_u16(combined0), vminvq_u16(combined1), vminvq_u16(combined2),
                          vminvq_u16(combined3), vminvq_u16(combined4), vminvq_u16(combined5),
                          vminvq_u16(combined6), vminvq_u16(combined7)};

   // Find the global minimum with a single NEON operation
   uint16_t global_min = vminvq_u16(all_mins);

   // Extract the original index from the combined value
   return start + (global_min & 0x3F);
}
#else
// Forward declaration
int find_approx_min_index_scalar_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start);
int find_approx_min_index_scalar(uint16_t* __attribute__((aligned(128))) original_counters,
                                 int                                     start1,
                                 int                                     start2);

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v11(uint16_t* __attribute__((aligned(128))) original_counters,
                                   int                                     start1,
                                   int                                     start2)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar(original_counters, start1, start2);
}

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v11_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_32(original_counters, start);
}

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v11b_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                       int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_32(original_counters, start);
}

// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v11_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                      int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_64(original_counters, start);
}
#endif

// The backward compatibility function is already defined both inside
// the #if defined(__ARM_NEON) block and in the #else block, so we don't
// need to redefine it here