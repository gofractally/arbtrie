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

// NEON v13a implementation (32 values)
int find_approx_min_index_neon_v13a_32(uint16_t* __attribute__((aligned(128))) original_counters,
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
#if defined(__aarch64__)
   uint16x8_t indices0 = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices1 = {8, 9, 10, 11, 12, 13, 14, 15};
   uint16x8_t indices2 = {16, 17, 18, 19, 20, 21, 22, 23};
   uint16x8_t indices3 = {24, 25, 26, 27, 28, 29, 30, 31};
#else
   // For 32-bit ARM, load from array
   const uint16_t index_values0[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   const uint16_t index_values1[8] = {8, 9, 10, 11, 12, 13, 14, 15};
   const uint16_t index_values2[8] = {16, 17, 18, 19, 20, 21, 22, 23};
   const uint16_t index_values3[8] = {24, 25, 26, 27, 28, 29, 30, 31};
   uint16x8_t     indices0         = vld1q_u16(index_values0);
   uint16x8_t     indices1         = vld1q_u16(index_values1);
   uint16x8_t     indices2         = vld1q_u16(index_values2);
   uint16x8_t     indices3         = vld1q_u16(index_values3);
#endif

   // Insert counter bits into index values:
   // Shift counters right to clear lower bits, then insert at bit position 5
   // This preserves the index in lower 5 bits and puts counter value in upper bits
   indices0 = vsliq_n_u16(indices0, chunk0, 5);
   indices1 = vsliq_n_u16(indices1, chunk1, 5);
   indices2 = vsliq_n_u16(indices2, chunk2, 5);
   indices3 = vsliq_n_u16(indices3, chunk3, 5);

   // Find minimums in each chunk using vminvq_u16
   uint16_t mins_array[4] = {vminvq_u16(indices0), vminvq_u16(indices1), vminvq_u16(indices2),
                             vminvq_u16(indices3)};

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
int find_approx_min_index_neon_v13a_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                       int                                     start)
{
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);
   return find_approx_min_index_scalar_32(original_counters, start);
}
#endif