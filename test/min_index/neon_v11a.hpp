#pragma once
#include <cstdint>
#include <cstring>  // For std::memcpy
#include "utils.hpp"

// Forward declaration for the scalar implementations
int find_approx_min_index_scalar_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start);
int find_approx_min_index_scalar(uint16_t* __attribute__((aligned(128))) original_counters,
                                 int                                     start1,
                                 int                                     start2);

#if defined(__ARM_NEON)
#include <arm_neon.h>

// NEON v11a implementation (32 values)
int find_approx_min_index_neon_v11a_32(uint16_t* __attribute__((aligned(128))) original_counters,
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

   // Create the bitmask directly: set one bit for each index position based on the index portion
   uint64_t bitmask = (1ULL << (mins_array[0] & 0x1F)) | (1ULL << (mins_array[1] & 0x1F)) |
                      (1ULL << (mins_array[2] & 0x1F)) | (1ULL << (mins_array[3] & 0x1F));

   // Find first set bit position (this is the index of the minimum value)
   int min_idx = count_trailing_zeros(bitmask);

   // Return the absolute index
   return start + min_idx;
}

#else
// Fallback to scalar implementation when NEON is not available
int find_approx_min_index_neon_v11a_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                       int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_32(original_counters, start);
}
#endif