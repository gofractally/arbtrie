#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// Detect architecture
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#define ARCH_X86
#if defined(__SSE4_1__) || defined(HAS_SSE41)
#include <smmintrin.h>  // For SSE4.1
#define HAS_SSE41
#endif
#elif defined(__arm__) || defined(__aarch64__) || defined(_M_ARM) || defined(_M_ARM64)
#define ARCH_ARM
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#ifdef __aarch64__
#include <arm_neon.h>
#else
#include <arm_neon.h>
#endif
// Don't redefine HAS_NEON if it's already defined via CMake
#ifndef HAS_NEON
#define HAS_NEON
#endif
#endif
#endif

// ANSI color codes for terminal output
#define RESET_COLOR "\033[0m"
#define ORANGE_COLOR "\033[38;5;208m"
#define GREEN_COLOR "\033[32m"

// Scalar implementation (works on any architecture)
int find_approx_min_index_scalar(uint16_t* original_counters, int start1, int start2)
{
   int      min_idx = start1;
   uint16_t min_val = original_counters[start1];

   // Check first cache line
   for (int i = start1; i < start1 + 32; i++)
   {
      if (original_counters[i] < min_val)
      {
         min_val = original_counters[i];
         min_idx = i;
      }
   }

   // Check second cache line
   for (int i = start2; i < start2 + 32; i++)
   {
      if (original_counters[i] < min_val)
      {
         min_val = original_counters[i];
         min_idx = i;
      }
   }

   return min_idx;
}

// Branchless tournament reduction scalar implementation
int find_approx_min_index_tournament(uint16_t* original_counters, int start1, int start2)
{
   // Store values and indices from both cache lines
   uint16_t values[64];
   int      indices[64];

   // Initialize with original values
   for (int i = 0; i < 32; i++)
   {
      values[i]  = original_counters[start1 + i];
      indices[i] = start1 + i;

      values[i + 32]  = original_counters[start2 + i];
      indices[i + 32] = start2 + i;
   }

   // Tournament reduction, implemented without branches

   // Round 1: 32 comparisons (64 -> 32)
   for (int i = 0; i < 32; i++)
   {
      int idx1 = i * 2;
      int idx2 = i * 2 + 1;

      // Compare values and use result to conditionally select indices
      bool     is_less = values[idx2] < values[idx1];
      uint32_t mask    = -is_less;  // All 1s if true, all 0s if false

      values[i]  = (values[idx2] & mask) | (values[idx1] & ~mask);
      indices[i] = (indices[idx2] & mask) | (indices[idx1] & ~mask);
   }

   // Round 2: 16 comparisons (32 -> 16)
   for (int i = 0; i < 16; i++)
   {
      int idx1 = i * 2;
      int idx2 = i * 2 + 1;

      bool     is_less = values[idx2] < values[idx1];
      uint32_t mask    = -is_less;

      values[i]  = (values[idx2] & mask) | (values[idx1] & ~mask);
      indices[i] = (indices[idx2] & mask) | (indices[idx1] & ~mask);
   }

   // Round 3: 8 comparisons (16 -> 8)
   for (int i = 0; i < 8; i++)
   {
      int idx1 = i * 2;
      int idx2 = i * 2 + 1;

      bool     is_less = values[idx2] < values[idx1];
      uint32_t mask    = -is_less;

      values[i]  = (values[idx2] & mask) | (values[idx1] & ~mask);
      indices[i] = (indices[idx2] & mask) | (indices[idx1] & ~mask);
   }

   // Round 4: 4 comparisons (8 -> 4)
   for (int i = 0; i < 4; i++)
   {
      int idx1 = i * 2;
      int idx2 = i * 2 + 1;

      bool     is_less = values[idx2] < values[idx1];
      uint32_t mask    = -is_less;

      values[i]  = (values[idx2] & mask) | (values[idx1] & ~mask);
      indices[i] = (indices[idx2] & mask) | (indices[idx1] & ~mask);
   }

   // Round 5: 2 comparisons (4 -> 2)
   for (int i = 0; i < 2; i++)
   {
      int idx1 = i * 2;
      int idx2 = i * 2 + 1;

      bool     is_less = values[idx2] < values[idx1];
      uint32_t mask    = -is_less;

      values[i]  = (values[idx2] & mask) | (values[idx1] & ~mask);
      indices[i] = (indices[idx2] & mask) | (indices[idx1] & ~mask);
   }

   // Round 6: Final comparison (2 -> 1)
   bool     is_less = values[1] < values[0];
   uint32_t mask    = -is_less;

   int final_idx = (indices[1] & mask) | (indices[0] & ~mask);

   return final_idx;
}

#ifdef HAS_NEON
// Count trailing zeros - returns position of first set bit
inline int count_trailing_zeros(uint64_t mask)
{
   if (mask == 0)
      return -1;
#if defined(__GNUC__) || defined(__clang__)
   return __builtin_ctzll(mask);  // GCC and Clang have builtin for 64-bit
#else
   // Fallback implementation
   for (int i = 0; i < 64; i++)
   {
      if (mask & (1ULL << i))
         return i;
   }
   return -1;
#endif
}

// Helper to convert a NEON vector comparison result to a uint64_t bitmask
inline uint64_t neon_to_mask(uint16x8_t cmp)
{
   uint64_t mask = 0;

   // Extract each lane as uint16_t first, then use bit operations
   uint16_t lane0 = vgetq_lane_u16(cmp, 0);
   uint16_t lane1 = vgetq_lane_u16(cmp, 1);
   uint16_t lane2 = vgetq_lane_u16(cmp, 2);
   uint16_t lane3 = vgetq_lane_u16(cmp, 3);
   uint16_t lane4 = vgetq_lane_u16(cmp, 4);
   uint16_t lane5 = vgetq_lane_u16(cmp, 5);
   uint16_t lane6 = vgetq_lane_u16(cmp, 6);
   uint16_t lane7 = vgetq_lane_u16(cmp, 7);

   // Set corresponding bits based on non-zero lanes
   // Each lane will be either 0 (false) or 0xFFFF (true) from vceqq comparison
   mask |= (lane0 != 0) ? 0x0001ULL : 0;
   mask |= (lane1 != 0) ? 0x0002ULL : 0;
   mask |= (lane2 != 0) ? 0x0004ULL : 0;
   mask |= (lane3 != 0) ? 0x0008ULL : 0;
   mask |= (lane4 != 0) ? 0x0010ULL : 0;
   mask |= (lane5 != 0) ? 0x0020ULL : 0;
   mask |= (lane6 != 0) ? 0x0040ULL : 0;
   mask |= (lane7 != 0) ? 0x0080ULL : 0;

   return mask;
}

// ARM NEON implementation (v3)
int find_approx_min_index_neon_v3(uint16_t* original_counters, int start1, int start2)
{
   // Step 1: Load all chunks using NEON instructions
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Step 2: Find the minimum value in each chunk using NEON vectorization
   // Instead of storing in arrays, we'll work directly with NEON registers

   // Process chunk0
   uint16x4_t low0    = vget_low_u16(chunk0);
   uint16x4_t high0   = vget_high_u16(chunk0);
   uint16x4_t min0_v4 = vpmin_u16(low0, high0);
   min0_v4            = vpmin_u16(min0_v4, min0_v4);
   min0_v4            = vpmin_u16(min0_v4, min0_v4);
   uint16_t min0      = vget_lane_u16(min0_v4, 0);

   // Find index of min0 in chunk0
   uint16x8_t cmp0  = vceqq_u16(chunk0, vdupq_n_u16(min0));
   uint64_t   mask0 = neon_to_mask(cmp0);
   int        idx0  = count_trailing_zeros(mask0);

   // Process chunk1
   uint16x4_t low1    = vget_low_u16(chunk1);
   uint16x4_t high1   = vget_high_u16(chunk1);
   uint16x4_t min1_v4 = vpmin_u16(low1, high1);
   min1_v4            = vpmin_u16(min1_v4, min1_v4);
   min1_v4            = vpmin_u16(min1_v4, min1_v4);
   uint16_t min1      = vget_lane_u16(min1_v4, 0);

   // Find index of min1 in chunk1
   uint16x8_t cmp1  = vceqq_u16(chunk1, vdupq_n_u16(min1));
   uint64_t   mask1 = neon_to_mask(cmp1);
   int        idx1  = count_trailing_zeros(mask1);

   // Process chunk2
   uint16x4_t low2    = vget_low_u16(chunk2);
   uint16x4_t high2   = vget_high_u16(chunk2);
   uint16x4_t min2_v4 = vpmin_u16(low2, high2);
   min2_v4            = vpmin_u16(min2_v4, min2_v4);
   min2_v4            = vpmin_u16(min2_v4, min2_v4);
   uint16_t min2      = vget_lane_u16(min2_v4, 0);

   // Find index of min2 in chunk2
   uint16x8_t cmp2  = vceqq_u16(chunk2, vdupq_n_u16(min2));
   uint64_t   mask2 = neon_to_mask(cmp2);
   int        idx2  = count_trailing_zeros(mask2);

   // Process chunk3
   uint16x4_t low3    = vget_low_u16(chunk3);
   uint16x4_t high3   = vget_high_u16(chunk3);
   uint16x4_t min3_v4 = vpmin_u16(low3, high3);
   min3_v4            = vpmin_u16(min3_v4, min3_v4);
   min3_v4            = vpmin_u16(min3_v4, min3_v4);
   uint16_t min3      = vget_lane_u16(min3_v4, 0);

   // Find index of min3 in chunk3
   uint16x8_t cmp3  = vceqq_u16(chunk3, vdupq_n_u16(min3));
   uint64_t   mask3 = neon_to_mask(cmp3);
   int        idx3  = count_trailing_zeros(mask3);

   // Process chunk4
   uint16x4_t low4    = vget_low_u16(chunk4);
   uint16x4_t high4   = vget_high_u16(chunk4);
   uint16x4_t min4_v4 = vpmin_u16(low4, high4);
   min4_v4            = vpmin_u16(min4_v4, min4_v4);
   min4_v4            = vpmin_u16(min4_v4, min4_v4);
   uint16_t min4      = vget_lane_u16(min4_v4, 0);

   // Find index of min4 in chunk4
   uint16x8_t cmp4  = vceqq_u16(chunk4, vdupq_n_u16(min4));
   uint64_t   mask4 = neon_to_mask(cmp4);
   int        idx4  = count_trailing_zeros(mask4);

   // Process chunk5
   uint16x4_t low5    = vget_low_u16(chunk5);
   uint16x4_t high5   = vget_high_u16(chunk5);
   uint16x4_t min5_v4 = vpmin_u16(low5, high5);
   min5_v4            = vpmin_u16(min5_v4, min5_v4);
   min5_v4            = vpmin_u16(min5_v4, min5_v4);
   uint16_t min5      = vget_lane_u16(min5_v4, 0);

   // Find index of min5 in chunk5
   uint16x8_t cmp5  = vceqq_u16(chunk5, vdupq_n_u16(min5));
   uint64_t   mask5 = neon_to_mask(cmp5);
   int        idx5  = count_trailing_zeros(mask5);

   // Process chunk6
   uint16x4_t low6    = vget_low_u16(chunk6);
   uint16x4_t high6   = vget_high_u16(chunk6);
   uint16x4_t min6_v4 = vpmin_u16(low6, high6);
   min6_v4            = vpmin_u16(min6_v4, min6_v4);
   min6_v4            = vpmin_u16(min6_v4, min6_v4);
   uint16_t min6      = vget_lane_u16(min6_v4, 0);

   // Find index of min6 in chunk6
   uint16x8_t cmp6  = vceqq_u16(chunk6, vdupq_n_u16(min6));
   uint64_t   mask6 = neon_to_mask(cmp6);
   int        idx6  = count_trailing_zeros(mask6);

   // Process chunk7
   uint16x4_t low7    = vget_low_u16(chunk7);
   uint16x4_t high7   = vget_high_u16(chunk7);
   uint16x4_t min7_v4 = vpmin_u16(low7, high7);
   min7_v4            = vpmin_u16(min7_v4, min7_v4);
   min7_v4            = vpmin_u16(min7_v4, min7_v4);
   uint16_t min7      = vget_lane_u16(min7_v4, 0);

   // Find index of min7 in chunk7
   uint16x8_t cmp7  = vceqq_u16(chunk7, vdupq_n_u16(min7));
   uint64_t   mask7 = neon_to_mask(cmp7);
   int        idx7  = count_trailing_zeros(mask7);

   // Step 3: Directly find global minimum from the individual minimums
   // Create NEON vector of minimums from each chunk using NEON operations
   // First combine min0-min3 into a vector
   uint16x4_t mins_low = vdup_n_u16(0);  // Initialize with zeros
   mins_low            = vset_lane_u16(min0, mins_low, 0);
   mins_low            = vset_lane_u16(min1, mins_low, 1);
   mins_low            = vset_lane_u16(min2, mins_low, 2);
   mins_low            = vset_lane_u16(min3, mins_low, 3);

   // Then combine min4-min7 into another vector
   uint16x4_t mins_high = vdup_n_u16(0);  // Initialize with zeros
   mins_high            = vset_lane_u16(min4, mins_high, 0);
   mins_high            = vset_lane_u16(min5, mins_high, 1);
   mins_high            = vset_lane_u16(min6, mins_high, 2);
   mins_high            = vset_lane_u16(min7, mins_high, 3);

   // Combine the two 64-bit vectors into one 128-bit vector
   uint16x8_t all_mins = vcombine_u16(mins_low, mins_high);

   // Now find the global minimum using NEON operations
   uint16x4_t mins_low_part  = vget_low_u16(all_mins);
   uint16x4_t mins_high_part = vget_high_u16(all_mins);
   uint16x4_t mins_pmin      = vpmin_u16(mins_low_part, mins_high_part);
   uint16x4_t mins_pmin2     = vpmin_u16(mins_pmin, mins_pmin);
   uint16x4_t mins_pmin3     = vpmin_u16(mins_pmin2, mins_pmin2);
   uint16_t   global_min     = vget_lane_u16(mins_pmin3, 0);

   // Find which chunk has the global minimum
   uint16x8_t global_cmp    = vceqq_u16(all_mins, vdupq_n_u16(global_min));
   uint64_t   global_mask   = neon_to_mask(global_cmp);
   int        min_chunk_idx = count_trailing_zeros(global_mask);

   // Array of local indices for direct lookup
   const int local_indices[8] = {idx0, idx1, idx2, idx3, idx4, idx5, idx6, idx7};

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_indices[min_chunk_idx];
}

// ARM NEON implementation that embeds indices in lower 3 bits (v4)
int find_approx_min_index_neon_v4(uint16_t* original_counters, int start1, int start2)
{
   // Step 1: Load all chunks using NEON instructions
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Step 2: Clear lowest 3 bits to prepare for index embedding
   // Create mask with all bits set except the lowest 3 bits
   uint16x8_t mask = vdupq_n_u16(0xFFF8);  // ~0x0007 = 0xFFF8

   // Mask out the lowest 3 bits of each chunk
   chunk0 = vandq_u16(chunk0, mask);
   chunk1 = vandq_u16(chunk1, mask);
   chunk2 = vandq_u16(chunk2, mask);
   chunk3 = vandq_u16(chunk3, mask);
   chunk4 = vandq_u16(chunk4, mask);
   chunk5 = vandq_u16(chunk5, mask);
   chunk6 = vandq_u16(chunk6, mask);
   chunk7 = vandq_u16(chunk7, mask);

   // Step 3: Embed indices into the lowest 3 bits

   // Create vectors with indices 0-7 for each lane
   uint16_t   index_values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices         = vld1q_u16(index_values);

   // Embed the indices
   chunk0 = vorrq_u16(chunk0, indices);
   chunk1 = vorrq_u16(chunk1, indices);
   chunk2 = vorrq_u16(chunk2, indices);
   chunk3 = vorrq_u16(chunk3, indices);
   chunk4 = vorrq_u16(chunk4, indices);
   chunk5 = vorrq_u16(chunk5, indices);
   chunk6 = vorrq_u16(chunk6, indices);
   chunk7 = vorrq_u16(chunk7, indices);

   // Step 4: Find minimum value in each chunk
   uint16x4_t min0_v4 = vpmin_u16(vget_low_u16(chunk0), vget_high_u16(chunk0));
   min0_v4            = vpmin_u16(min0_v4, min0_v4);
   min0_v4            = vpmin_u16(min0_v4, min0_v4);
   uint16_t min0      = vget_lane_u16(min0_v4, 0);

   uint16x4_t min1_v4 = vpmin_u16(vget_low_u16(chunk1), vget_high_u16(chunk1));
   min1_v4            = vpmin_u16(min1_v4, min1_v4);
   min1_v4            = vpmin_u16(min1_v4, min1_v4);
   uint16_t min1      = vget_lane_u16(min1_v4, 0);

   uint16x4_t min2_v4 = vpmin_u16(vget_low_u16(chunk2), vget_high_u16(chunk2));
   min2_v4            = vpmin_u16(min2_v4, min2_v4);
   min2_v4            = vpmin_u16(min2_v4, min2_v4);
   uint16_t min2      = vget_lane_u16(min2_v4, 0);

   uint16x4_t min3_v4 = vpmin_u16(vget_low_u16(chunk3), vget_high_u16(chunk3));
   min3_v4            = vpmin_u16(min3_v4, min3_v4);
   min3_v4            = vpmin_u16(min3_v4, min3_v4);
   uint16_t min3      = vget_lane_u16(min3_v4, 0);

   uint16x4_t min4_v4 = vpmin_u16(vget_low_u16(chunk4), vget_high_u16(chunk4));
   min4_v4            = vpmin_u16(min4_v4, min4_v4);
   min4_v4            = vpmin_u16(min4_v4, min4_v4);
   uint16_t min4      = vget_lane_u16(min4_v4, 0);

   uint16x4_t min5_v4 = vpmin_u16(vget_low_u16(chunk5), vget_high_u16(chunk5));
   min5_v4            = vpmin_u16(min5_v4, min5_v4);
   min5_v4            = vpmin_u16(min5_v4, min5_v4);
   uint16_t min5      = vget_lane_u16(min5_v4, 0);

   uint16x4_t min6_v4 = vpmin_u16(vget_low_u16(chunk6), vget_high_u16(chunk6));
   min6_v4            = vpmin_u16(min6_v4, min6_v4);
   min6_v4            = vpmin_u16(min6_v4, min6_v4);
   uint16_t min6      = vget_lane_u16(min6_v4, 0);

   uint16x4_t min7_v4 = vpmin_u16(vget_low_u16(chunk7), vget_high_u16(chunk7));
   min7_v4            = vpmin_u16(min7_v4, min7_v4);
   min7_v4            = vpmin_u16(min7_v4, min7_v4);
   uint16_t min7      = vget_lane_u16(min7_v4, 0);

   // Step 5: Gather all chunk minimums and find global minimum
   uint16x4_t mins_low = vdup_n_u16(0);
   mins_low            = vset_lane_u16(min0, mins_low, 0);
   mins_low            = vset_lane_u16(min1, mins_low, 1);
   mins_low            = vset_lane_u16(min2, mins_low, 2);
   mins_low            = vset_lane_u16(min3, mins_low, 3);

   uint16x4_t mins_high = vdup_n_u16(0);
   mins_high            = vset_lane_u16(min4, mins_high, 0);
   mins_high            = vset_lane_u16(min5, mins_high, 1);
   mins_high            = vset_lane_u16(min6, mins_high, 2);
   mins_high            = vset_lane_u16(min7, mins_high, 3);

   uint16x8_t all_mins = vcombine_u16(mins_low, mins_high);

   // Find the global minimum
   uint16x4_t mins_low_part  = vget_low_u16(all_mins);
   uint16x4_t mins_high_part = vget_high_u16(all_mins);
   uint16x4_t global_min_v4  = vpmin_u16(mins_low_part, mins_high_part);
   global_min_v4             = vpmin_u16(global_min_v4, global_min_v4);
   global_min_v4             = vpmin_u16(global_min_v4, global_min_v4);
   uint16_t global_min       = vget_lane_u16(global_min_v4, 0);

   // Step 6: Find which chunk contains the global minimum
   uint16x8_t global_cmp    = vceqq_u16(all_mins, vdupq_n_u16(global_min));
   uint64_t   global_mask   = neon_to_mask(global_cmp);
   int        min_chunk_idx = count_trailing_zeros(global_mask);

   // Step 7: Extract the index from within the chunk (lowest 3 bits)
   int local_idx = global_min & 0x7;  // Extract bits 0-2

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_idx;
}

// Optimized NEON implementation with parallel processing (v5)
int find_approx_min_index_neon_v5(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create mask with all bits set except lowest 3 bits (0xFFF8)
   uint16x8_t mask = vdupq_n_u16(0xFFF8);

   // Create index vectors for all 8 lanes at once
   uint16_t   index_values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices         = vld1q_u16(index_values);

   // Process all chunks in parallel: mask out lowest 3 bits and embed indices
   // This does in one step what v4 did in two steps
   chunk0 = vorrq_u16(vandq_u16(chunk0, mask), indices);
   chunk1 = vorrq_u16(vandq_u16(chunk1, mask), indices);
   chunk2 = vorrq_u16(vandq_u16(chunk2, mask), indices);
   chunk3 = vorrq_u16(vandq_u16(chunk3, mask), indices);
   chunk4 = vorrq_u16(vandq_u16(chunk4, mask), indices);
   chunk5 = vorrq_u16(vandq_u16(chunk5, mask), indices);
   chunk6 = vorrq_u16(vandq_u16(chunk6, mask), indices);
   chunk7 = vorrq_u16(vandq_u16(chunk7, mask), indices);

   // Find minimums using NEON's pairwise min reduction
   uint16x4_t min0 = vpmin_u16(vget_low_u16(chunk0), vget_high_u16(chunk0));
   uint16x4_t min1 = vpmin_u16(vget_low_u16(chunk1), vget_high_u16(chunk1));
   uint16x4_t min2 = vpmin_u16(vget_low_u16(chunk2), vget_high_u16(chunk2));
   uint16x4_t min3 = vpmin_u16(vget_low_u16(chunk3), vget_high_u16(chunk3));
   uint16x4_t min4 = vpmin_u16(vget_low_u16(chunk4), vget_high_u16(chunk4));
   uint16x4_t min5 = vpmin_u16(vget_low_u16(chunk5), vget_high_u16(chunk5));
   uint16x4_t min6 = vpmin_u16(vget_low_u16(chunk6), vget_high_u16(chunk6));
   uint16x4_t min7 = vpmin_u16(vget_low_u16(chunk7), vget_high_u16(chunk7));

   // Further pairwise reduction to get the minimum for each chunk
   min0 = vpmin_u16(min0, min0);
   min0 = vpmin_u16(min0, min0);
   min1 = vpmin_u16(min1, min1);
   min1 = vpmin_u16(min1, min1);
   min2 = vpmin_u16(min2, min2);
   min2 = vpmin_u16(min2, min2);
   min3 = vpmin_u16(min3, min3);
   min3 = vpmin_u16(min3, min3);
   min4 = vpmin_u16(min4, min4);
   min4 = vpmin_u16(min4, min4);
   min5 = vpmin_u16(min5, min5);
   min5 = vpmin_u16(min5, min5);
   min6 = vpmin_u16(min6, min6);
   min6 = vpmin_u16(min6, min6);
   min7 = vpmin_u16(min7, min7);
   min7 = vpmin_u16(min7, min7);

   // Extract the minimums into scalar values
   uint16_t chunk_mins[8];
   chunk_mins[0] = vget_lane_u16(min0, 0);
   chunk_mins[1] = vget_lane_u16(min1, 0);
   chunk_mins[2] = vget_lane_u16(min2, 0);
   chunk_mins[3] = vget_lane_u16(min3, 0);
   chunk_mins[4] = vget_lane_u16(min4, 0);
   chunk_mins[5] = vget_lane_u16(min5, 0);
   chunk_mins[6] = vget_lane_u16(min6, 0);
   chunk_mins[7] = vget_lane_u16(min7, 0);

   // Find the global minimum among all chunk minimums using scalar code
   // This is more efficient than going back to NEON for just 8 values
   uint16_t global_min    = chunk_mins[0];
   int      min_chunk_idx = 0;

   for (int i = 1; i < 8; i++)
   {
      if (chunk_mins[i] < global_min)
      {
         global_min    = chunk_mins[i];
         min_chunk_idx = i;
      }
   }

   // Extract the index from the global minimum (lowest 3 bits)
   int local_idx = global_min & 0x7;

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_idx;
}

// ARM NEON v6 implementation (fully vectorized global minimum)
int find_approx_min_index_neon_v6(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create mask with all bits set except lowest 3 bits (0xFFF8)
   uint16x8_t mask = vdupq_n_u16(0xFFF8);

   // Create index vectors for all 8 lanes at once
   uint16_t   index_values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices         = vld1q_u16(index_values);

   // Process all chunks in parallel: mask out lowest 3 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);
   uint16x8_t masked_chunk4 = vandq_u16(chunk4, mask);
   uint16x8_t masked_chunk5 = vandq_u16(chunk5, mask);
   uint16x8_t masked_chunk6 = vandq_u16(chunk6, mask);
   uint16x8_t masked_chunk7 = vandq_u16(chunk7, mask);

   // Embed indices directly (in one step, combining mask and embed)
   chunk0 = vorrq_u16(masked_chunk0, indices);
   chunk1 = vorrq_u16(masked_chunk1, indices);
   chunk2 = vorrq_u16(masked_chunk2, indices);
   chunk3 = vorrq_u16(masked_chunk3, indices);
   chunk4 = vorrq_u16(masked_chunk4, indices);
   chunk5 = vorrq_u16(masked_chunk5, indices);
   chunk6 = vorrq_u16(masked_chunk6, indices);
   chunk7 = vorrq_u16(masked_chunk7, indices);

   // Find minimums in each chunk (using pairwise minimum reduction)
   uint16x4_t min0   = vpmin_u16(vget_low_u16(chunk0), vget_high_u16(chunk0));
   min0              = vpmin_u16(min0, min0);
   min0              = vpmin_u16(min0, min0);
   uint16_t min0_val = vget_lane_u16(min0, 0);

   uint16x4_t min1   = vpmin_u16(vget_low_u16(chunk1), vget_high_u16(chunk1));
   min1              = vpmin_u16(min1, min1);
   min1              = vpmin_u16(min1, min1);
   uint16_t min1_val = vget_lane_u16(min1, 0);

   uint16x4_t min2   = vpmin_u16(vget_low_u16(chunk2), vget_high_u16(chunk2));
   min2              = vpmin_u16(min2, min2);
   min2              = vpmin_u16(min2, min2);
   uint16_t min2_val = vget_lane_u16(min2, 0);

   uint16x4_t min3   = vpmin_u16(vget_low_u16(chunk3), vget_high_u16(chunk3));
   min3              = vpmin_u16(min3, min3);
   min3              = vpmin_u16(min3, min3);
   uint16_t min3_val = vget_lane_u16(min3, 0);

   uint16x4_t min4   = vpmin_u16(vget_low_u16(chunk4), vget_high_u16(chunk4));
   min4              = vpmin_u16(min4, min4);
   min4              = vpmin_u16(min4, min4);
   uint16_t min4_val = vget_lane_u16(min4, 0);

   uint16x4_t min5   = vpmin_u16(vget_low_u16(chunk5), vget_high_u16(chunk5));
   min5              = vpmin_u16(min5, min5);
   min5              = vpmin_u16(min5, min5);
   uint16_t min5_val = vget_lane_u16(min5, 0);

   uint16x4_t min6   = vpmin_u16(vget_low_u16(chunk6), vget_high_u16(chunk6));
   min6              = vpmin_u16(min6, min6);
   min6              = vpmin_u16(min6, min6);
   uint16_t min6_val = vget_lane_u16(min6, 0);

   uint16x4_t min7   = vpmin_u16(vget_low_u16(chunk7), vget_high_u16(chunk7));
   min7              = vpmin_u16(min7, min7);
   min7              = vpmin_u16(min7, min7);
   uint16_t min7_val = vget_lane_u16(min7, 0);

   // Create vectors with all minimums for vectorized comparison
   uint16x4_t mins_low = vdup_n_u16(0);
   mins_low            = vset_lane_u16(min0_val, mins_low, 0);
   mins_low            = vset_lane_u16(min1_val, mins_low, 1);
   mins_low            = vset_lane_u16(min2_val, mins_low, 2);
   mins_low            = vset_lane_u16(min3_val, mins_low, 3);

   uint16x4_t mins_high = vdup_n_u16(0);
   mins_high            = vset_lane_u16(min4_val, mins_high, 0);
   mins_high            = vset_lane_u16(min5_val, mins_high, 1);
   mins_high            = vset_lane_u16(min6_val, mins_high, 2);
   mins_high            = vset_lane_u16(min7_val, mins_high, 3);

   // Combine the two 64-bit vectors into one 128-bit vector
   uint16x8_t all_mins = vcombine_u16(mins_low, mins_high);

   // Find the global minimum using NEON operations
   uint16x4_t mins_pmin = vpmin_u16(vget_low_u16(all_mins), vget_high_u16(all_mins));
   mins_pmin            = vpmin_u16(mins_pmin, mins_pmin);
   mins_pmin            = vpmin_u16(mins_pmin, mins_pmin);
   uint16_t global_min  = vget_lane_u16(mins_pmin, 0);

   // Create a mask for values equal to the global minimum
   uint16x8_t min_mask = vceqq_u16(all_mins, vdupq_n_u16(global_min));

   // Convert the comparison result to a bitmask
   uint64_t chunk_mask = neon_to_mask(min_mask);

   // Find the first (lowest index) chunk that has the minimum value
   int min_chunk_idx = count_trailing_zeros(chunk_mask);

   // Extract the index from the global minimum (lowest 3 bits)
   int local_idx = global_min & 0x7;

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_idx;
}

// ARM NEON v7 implementation (using vminv/vminvq)
int find_approx_min_index_neon_v7(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create mask with all bits set except lowest 3 bits (0xFFF8)
   uint16x8_t mask = vdupq_n_u16(0xFFF8);

   // Create index vectors for all 8 lanes at once
   uint16_t   index_values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices         = vld1q_u16(index_values);

   // Process all chunks in parallel: mask out lowest 3 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);
   uint16x8_t masked_chunk4 = vandq_u16(chunk4, mask);
   uint16x8_t masked_chunk5 = vandq_u16(chunk5, mask);
   uint16x8_t masked_chunk6 = vandq_u16(chunk6, mask);
   uint16x8_t masked_chunk7 = vandq_u16(chunk7, mask);

   // Embed indices directly (in one step, combining mask and embed)
   chunk0 = vorrq_u16(masked_chunk0, indices);
   chunk1 = vorrq_u16(masked_chunk1, indices);
   chunk2 = vorrq_u16(masked_chunk2, indices);
   chunk3 = vorrq_u16(masked_chunk3, indices);
   chunk4 = vorrq_u16(masked_chunk4, indices);
   chunk5 = vorrq_u16(masked_chunk5, indices);
   chunk6 = vorrq_u16(masked_chunk6, indices);
   chunk7 = vorrq_u16(masked_chunk7, indices);

   // Find minimums in each chunk directly using vminvq_u16
   uint16_t min0_val = vminvq_u16(chunk0);
   uint16_t min1_val = vminvq_u16(chunk1);
   uint16_t min2_val = vminvq_u16(chunk2);
   uint16_t min3_val = vminvq_u16(chunk3);
   uint16_t min4_val = vminvq_u16(chunk4);
   uint16_t min5_val = vminvq_u16(chunk5);
   uint16_t min6_val = vminvq_u16(chunk6);
   uint16_t min7_val = vminvq_u16(chunk7);

   // Find global minimum directly
   uint16_t global_min = min0_val;
   global_min          = (min1_val < global_min) ? min1_val : global_min;
   global_min          = (min2_val < global_min) ? min2_val : global_min;
   global_min          = (min3_val < global_min) ? min3_val : global_min;
   global_min          = (min4_val < global_min) ? min4_val : global_min;
   global_min          = (min5_val < global_min) ? min5_val : global_min;
   global_min          = (min6_val < global_min) ? min6_val : global_min;
   global_min          = (min7_val < global_min) ? min7_val : global_min;

   // Find which chunk has the global minimum
   int min_chunk_idx = 0;
   if (min1_val == global_min)
      min_chunk_idx = 1;
   else if (min2_val == global_min)
      min_chunk_idx = 2;
   else if (min3_val == global_min)
      min_chunk_idx = 3;
   else if (min4_val == global_min)
      min_chunk_idx = 4;
   else if (min5_val == global_min)
      min_chunk_idx = 5;
   else if (min6_val == global_min)
      min_chunk_idx = 6;
   else if (min7_val == global_min)
      min_chunk_idx = 7;

   // Extract the index from the global minimum (lowest 3 bits)
   int local_idx = global_min & 0x7;

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_idx;
}

// ARM NEON v8 implementation (fully vectorized min detection)
int find_approx_min_index_neon_v8(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create mask with all bits set except lowest 3 bits (0xFFF8)
   uint16x8_t mask = vdupq_n_u16(0xFFF8);

   // Create index vectors for all 8 lanes at once
   uint16_t   index_values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices         = vld1q_u16(index_values);

   // Process all chunks in parallel: mask out lowest 3 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);
   uint16x8_t masked_chunk4 = vandq_u16(chunk4, mask);
   uint16x8_t masked_chunk5 = vandq_u16(chunk5, mask);
   uint16x8_t masked_chunk6 = vandq_u16(chunk6, mask);
   uint16x8_t masked_chunk7 = vandq_u16(chunk7, mask);

   // Embed indices directly (in one step, combining mask and embed)
   chunk0 = vorrq_u16(masked_chunk0, indices);
   chunk1 = vorrq_u16(masked_chunk1, indices);
   chunk2 = vorrq_u16(masked_chunk2, indices);
   chunk3 = vorrq_u16(masked_chunk3, indices);
   chunk4 = vorrq_u16(masked_chunk4, indices);
   chunk5 = vorrq_u16(masked_chunk5, indices);
   chunk6 = vorrq_u16(masked_chunk6, indices);
   chunk7 = vorrq_u16(masked_chunk7, indices);

   // Find minimums in each chunk directly using vminvq_u16
   uint16_t min0_val = vminvq_u16(chunk0);
   uint16_t min1_val = vminvq_u16(chunk1);
   uint16_t min2_val = vminvq_u16(chunk2);
   uint16_t min3_val = vminvq_u16(chunk3);
   uint16_t min4_val = vminvq_u16(chunk4);
   uint16_t min5_val = vminvq_u16(chunk5);
   uint16_t min6_val = vminvq_u16(chunk6);
   uint16_t min7_val = vminvq_u16(chunk7);

   // Create a vector with all the minimum values from each chunk
   uint16_t   mins_array[8] = {min0_val, min1_val, min2_val, min3_val,
                               min4_val, min5_val, min6_val, min7_val};
   uint16x8_t all_mins      = vld1q_u16(mins_array);

   // Find the global minimum across all chunks using vminvq_u16
   uint16_t global_min = vminvq_u16(all_mins);

   // Create a mask for values equal to the global minimum
   uint16x8_t min_mask = vceqq_u16(all_mins, vdupq_n_u16(global_min));

   // Convert the comparison result to a bitmask
   uint64_t chunk_mask = neon_to_mask(min_mask);

   // Find the first (lowest index) chunk that has the minimum value
   int min_chunk_idx = count_trailing_zeros(chunk_mask);

   // Extract the index from the global minimum (lowest 3 bits)
   int local_idx = global_min & 0x7;

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_idx;
}

// ARM NEON v9 implementation (optimized memory access)
int find_approx_min_index_neon_v9(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create mask with all bits set except lowest 3 bits (0xFFF8)
   uint16x8_t mask = vdupq_n_u16(0xFFF8);

   // Create index vectors for all 8 lanes at once
   uint16_t   index_values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices         = vld1q_u16(index_values);

   // Process all chunks in parallel: mask out lowest 3 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);
   uint16x8_t masked_chunk4 = vandq_u16(chunk4, mask);
   uint16x8_t masked_chunk5 = vandq_u16(chunk5, mask);
   uint16x8_t masked_chunk6 = vandq_u16(chunk6, mask);
   uint16x8_t masked_chunk7 = vandq_u16(chunk7, mask);

   // Embed indices directly (in one step, combining mask and embed)
   chunk0 = vorrq_u16(masked_chunk0, indices);
   chunk1 = vorrq_u16(masked_chunk1, indices);
   chunk2 = vorrq_u16(masked_chunk2, indices);
   chunk3 = vorrq_u16(masked_chunk3, indices);
   chunk4 = vorrq_u16(masked_chunk4, indices);
   chunk5 = vorrq_u16(masked_chunk5, indices);
   chunk6 = vorrq_u16(masked_chunk6, indices);
   chunk7 = vorrq_u16(masked_chunk7, indices);

   // Find minimums in each chunk directly using vminvq_u16
   uint16_t min0_val = vminvq_u16(chunk0);
   uint16_t min1_val = vminvq_u16(chunk1);
   uint16_t min2_val = vminvq_u16(chunk2);
   uint16_t min3_val = vminvq_u16(chunk3);
   uint16_t min4_val = vminvq_u16(chunk4);
   uint16_t min5_val = vminvq_u16(chunk5);
   uint16_t min6_val = vminvq_u16(chunk6);
   uint16_t min7_val = vminvq_u16(chunk7);

   // Find global minimum with direct comparisons (no memory indirection)
   uint16_t global_min    = min0_val;
   int      min_chunk_idx = 0;

// Use branchless comparisons for finding both minimum and its index
#define UPDATE_MIN_INDEX(idx)                                 \
   do                                                         \
   {                                                          \
      uint16_t val        = min##idx##_val;                   \
      bool     is_smaller = val < global_min;                 \
      global_min          = is_smaller ? val : global_min;    \
      min_chunk_idx       = is_smaller ? idx : min_chunk_idx; \
   } while (0)

   UPDATE_MIN_INDEX(1);
   UPDATE_MIN_INDEX(2);
   UPDATE_MIN_INDEX(3);
   UPDATE_MIN_INDEX(4);
   UPDATE_MIN_INDEX(5);
   UPDATE_MIN_INDEX(6);
   UPDATE_MIN_INDEX(7);

#undef UPDATE_MIN_INDEX

   // Extract the index from the global minimum (lowest 3 bits)
   int local_idx = global_min & 0x7;

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_idx;
}

// ARM NEON v10 implementation
int find_approx_min_index_neon_v10(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create mask with all bits set except lowest 3 bits (0xFFF8)
   uint16x8_t mask = vdupq_n_u16(0xFFF8);

   // Create index vectors for all 8 lanes at once
   uint16_t   index_values[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   uint16x8_t indices         = vld1q_u16(index_values);

   // Process all chunks in parallel: mask out lowest 3 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);
   uint16x8_t masked_chunk4 = vandq_u16(chunk4, mask);
   uint16x8_t masked_chunk5 = vandq_u16(chunk5, mask);
   uint16x8_t masked_chunk6 = vandq_u16(chunk6, mask);
   uint16x8_t masked_chunk7 = vandq_u16(chunk7, mask);

   // Embed indices directly (in one step, combining mask and embed)
   chunk0 = vorrq_u16(masked_chunk0, indices);
   chunk1 = vorrq_u16(masked_chunk1, indices);
   chunk2 = vorrq_u16(masked_chunk2, indices);
   chunk3 = vorrq_u16(masked_chunk3, indices);
   chunk4 = vorrq_u16(masked_chunk4, indices);
   chunk5 = vorrq_u16(masked_chunk5, indices);
   chunk6 = vorrq_u16(masked_chunk6, indices);
   chunk7 = vorrq_u16(masked_chunk7, indices);

   // Create a vector with all the minimum values from each chunk
   uint16_t mins_array[8] = {vminvq_u16(chunk0), vminvq_u16(chunk1), vminvq_u16(chunk2),
                             vminvq_u16(chunk3), vminvq_u16(chunk4), vminvq_u16(chunk5),
                             vminvq_u16(chunk6), vminvq_u16(chunk7)};

   uint16x8_t all_mins = vld1q_u16(mins_array);

   // Find the global minimum across all chunks using vminvq_u16
   uint16_t global_min = vminvq_u16(all_mins);

   // Create a mask for values equal to the global minimum
   uint16x8_t min_mask = vceqq_u16(all_mins, vdupq_n_u16(global_min));

   // Convert the comparison result to a bitmask
   uint64_t chunk_mask = neon_to_mask(min_mask);

   // Find the first (lowest index) chunk that has the minimum value
   int min_chunk_idx = count_trailing_zeros(chunk_mask);

   // Extract the index from the global minimum (lowest 3 bits)
   int local_idx = global_min & 0x7;

   // Base offsets for each chunk
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return final global index
   return base_offsets[min_chunk_idx] + local_idx;
}
// ARM NEON v10 implementation (6-bit global indices)
int find_approx_min_index_neon_v11(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create mask with all bits set except lowest 6 bits (0xFFC0)
   uint16x8_t mask = vdupq_n_u16(0xFFC0);

   // Create index vectors for all 8 lanes at once
   const uint16_t index_values0[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   const uint16_t index_values1[8] = {8, 9, 10, 11, 12, 13, 14, 15};
   const uint16_t index_values2[8] = {16, 17, 18, 19, 20, 21, 22, 23};
   const uint16_t index_values3[8] = {24, 25, 26, 27, 28, 29, 30, 31};
   const uint16_t index_values4[8] = {32, 33, 34, 35, 36, 37, 38, 39};
   const uint16_t index_values5[8] = {40, 41, 42, 43, 44, 45, 46, 47};
   const uint16_t index_values6[8] = {48, 49, 50, 51, 52, 53, 54, 55};
   const uint16_t index_values7[8] = {56, 57, 58, 59, 60, 61, 62, 63};

   uint16x8_t indices0 = vld1q_u16(index_values0);
   uint16x8_t indices1 = vld1q_u16(index_values1);
   uint16x8_t indices2 = vld1q_u16(index_values2);
   uint16x8_t indices3 = vld1q_u16(index_values3);
   uint16x8_t indices4 = vld1q_u16(index_values4);
   uint16x8_t indices5 = vld1q_u16(index_values5);
   uint16x8_t indices6 = vld1q_u16(index_values6);
   uint16x8_t indices7 = vld1q_u16(index_values7);

   // Process all chunks in parallel: mask out lowest 6 bits and embed indices
   uint16x8_t masked_chunk0 = vandq_u16(chunk0, mask);
   uint16x8_t masked_chunk1 = vandq_u16(chunk1, mask);
   uint16x8_t masked_chunk2 = vandq_u16(chunk2, mask);
   uint16x8_t masked_chunk3 = vandq_u16(chunk3, mask);
   uint16x8_t masked_chunk4 = vandq_u16(chunk4, mask);
   uint16x8_t masked_chunk5 = vandq_u16(chunk5, mask);
   uint16x8_t masked_chunk6 = vandq_u16(chunk6, mask);
   uint16x8_t masked_chunk7 = vandq_u16(chunk7, mask);

   // Embed indices directly (in one step, combining mask and embed)
   chunk0 = vorrq_u16(masked_chunk0, indices0);
   chunk1 = vorrq_u16(masked_chunk1, indices1);
   chunk2 = vorrq_u16(masked_chunk2, indices2);
   chunk3 = vorrq_u16(masked_chunk3, indices3);
   chunk4 = vorrq_u16(masked_chunk4, indices4);
   chunk5 = vorrq_u16(masked_chunk5, indices5);
   chunk6 = vorrq_u16(masked_chunk6, indices6);
   chunk7 = vorrq_u16(masked_chunk7, indices7);

   // Create a vector with all the minimum values from each chunk
   uint16_t mins_array[8] = {vminvq_u16(chunk0), vminvq_u16(chunk1), vminvq_u16(chunk2),
                             vminvq_u16(chunk3), vminvq_u16(chunk4), vminvq_u16(chunk5),
                             vminvq_u16(chunk6), vminvq_u16(chunk7)};

   uint16x8_t all_mins = vld1q_u16(mins_array);

   // Find the global minimum
   uint16_t global_min = vminvq_u16(all_mins);

   // Extract the embedded index (0-63)
   int embedded_index = global_min & 0x3F;

   auto diff = start2 - start1;

   return start1 + diff * (embedded_index >> 5) + (embedded_index & 0x1F);
}

// ARM NEON v12 implementation (6-bit global indices with shift instead of mask+or)
int find_approx_min_index_neon_v12(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

   // Create index vectors for all 8 lanes at once
   const uint16_t index_values0[8] = {0, 1, 2, 3, 4, 5, 6, 7};
   const uint16_t index_values1[8] = {8, 9, 10, 11, 12, 13, 14, 15};
   const uint16_t index_values2[8] = {16, 17, 18, 19, 20, 21, 22, 23};
   const uint16_t index_values3[8] = {24, 25, 26, 27, 28, 29, 30, 31};
   const uint16_t index_values4[8] = {32, 33, 34, 35, 36, 37, 38, 39};
   const uint16_t index_values5[8] = {40, 41, 42, 43, 44, 45, 46, 47};
   const uint16_t index_values6[8] = {48, 49, 50, 51, 52, 53, 54, 55};
   const uint16_t index_values7[8] = {56, 57, 58, 59, 60, 61, 62, 63};

   uint16x8_t indices0 = vld1q_u16(index_values0);
   uint16x8_t indices1 = vld1q_u16(index_values1);
   uint16x8_t indices2 = vld1q_u16(index_values2);
   uint16x8_t indices3 = vld1q_u16(index_values3);
   uint16x8_t indices4 = vld1q_u16(index_values4);
   uint16x8_t indices5 = vld1q_u16(index_values5);
   uint16x8_t indices6 = vld1q_u16(index_values6);
   uint16x8_t indices7 = vld1q_u16(index_values7);

   // Process all chunks with shift operations instead of mask and OR:
   // 1. Shift right by 6 bits to clear lower bits
   // 2. Shift left by 6 bits to make room for indices
   // 3. Add (OR) the indices
   uint16x8_t shift_const = vdupq_n_u16(6);

   // Shift right then left to clear the lower 6 bits
   chunk0 = vshlq_u16(vshrq_n_u16(chunk0, 6), shift_const);
   chunk1 = vshlq_u16(vshrq_n_u16(chunk1, 6), shift_const);
   chunk2 = vshlq_u16(vshrq_n_u16(chunk2, 6), shift_const);
   chunk3 = vshlq_u16(vshrq_n_u16(chunk3, 6), shift_const);
   chunk4 = vshlq_u16(vshrq_n_u16(chunk4, 6), shift_const);
   chunk5 = vshlq_u16(vshrq_n_u16(chunk5, 6), shift_const);
   chunk6 = vshlq_u16(vshrq_n_u16(chunk6, 6), shift_const);
   chunk7 = vshlq_u16(vshrq_n_u16(chunk7, 6), shift_const);

   // Add the indices (same as OR since the lower bits are all zero)
   chunk0 = vaddq_u16(chunk0, indices0);
   chunk1 = vaddq_u16(chunk1, indices1);
   chunk2 = vaddq_u16(chunk2, indices2);
   chunk3 = vaddq_u16(chunk3, indices3);
   chunk4 = vaddq_u16(chunk4, indices4);
   chunk5 = vaddq_u16(chunk5, indices5);
   chunk6 = vaddq_u16(chunk6, indices6);
   chunk7 = vaddq_u16(chunk7, indices7);

   // Use NEON's vminvq instruction to find min in each chunk
   uint16_t mins_array[8] = {vminvq_u16(chunk0), vminvq_u16(chunk1), vminvq_u16(chunk2),
                             vminvq_u16(chunk3), vminvq_u16(chunk4), vminvq_u16(chunk5),
                             vminvq_u16(chunk6), vminvq_u16(chunk7)};

   uint16x8_t all_mins = vld1q_u16(mins_array);

   // Find the global minimum
   uint16_t global_min = vminvq_u16(all_mins);

   // Create a mask for values equal to the global minimum
   uint16x8_t min_mask = vceqq_u16(all_mins, vdupq_n_u16(global_min));

   // Convert the comparison result to a bitmask
   uint64_t chunk_mask = neon_to_mask(min_mask);

   // Find the first (lowest index) chunk that has the minimum value
   int min_chunk_idx = count_trailing_zeros(chunk_mask);

   // Extract the index from the global minimum (lowest 6 bits)
   int local_idx = global_min & 0x3F;

   // Return final global index
   return local_idx;
}

// ARM NEON v13 implementation (VSLI to insert counter bits into index bits)
int find_approx_min_index_neon_v13(uint16_t* original_counters, int start1, int start2)
{
   // Load all chunks at once
   uint16x8_t chunk0 = vld1q_u16(&original_counters[start1]);
   uint16x8_t chunk1 = vld1q_u16(&original_counters[start1 + 8]);
   uint16x8_t chunk2 = vld1q_u16(&original_counters[start1 + 16]);
   uint16x8_t chunk3 = vld1q_u16(&original_counters[start1 + 24]);
   uint16x8_t chunk4 = vld1q_u16(&original_counters[start2]);
   uint16x8_t chunk5 = vld1q_u16(&original_counters[start2 + 8]);
   uint16x8_t chunk6 = vld1q_u16(&original_counters[start2 + 16]);
   uint16x8_t chunk7 = vld1q_u16(&original_counters[start2 + 24]);

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

   // Use NEON's vminvq instruction to find min in each chunk
   uint16_t mins_array[8] = {vminvq_u16(indices0), vminvq_u16(indices1), vminvq_u16(indices2),
                             vminvq_u16(indices3), vminvq_u16(indices4), vminvq_u16(indices5),
                             vminvq_u16(indices6), vminvq_u16(indices7)};

   uint16x8_t all_mins = vld1q_u16(mins_array);

   // Find the global minimum
   uint16_t global_min = vminvq_u16(all_mins);

   // Create a mask for values equal to the global minimum
   uint16x8_t min_mask = vceqq_u16(all_mins, vdupq_n_u16(global_min));

   // Convert the comparison result to a bitmask
   uint64_t chunk_mask = neon_to_mask(min_mask);

   // Find the first (lowest index) chunk that has the minimum value
   int min_chunk_idx = count_trailing_zeros(chunk_mask);

   // Extract the index from the global minimum (lowest 6 bits)
   int local_idx = global_min & 0x3F;

   // Return final global index
   return local_idx;
}

// Use the latest v11 as the default NEON implementation
int find_approx_min_index_neon(uint16_t* original_counters, int start1, int start2)
{
   return find_approx_min_index_neon_v11(original_counters, start1, start2);
}

#endif

#ifdef HAS_SSE41
// x86 SSE4.1 implementation
int find_approx_min_index_sse41(uint16_t* original_counters, int start1, int start2)
{
   // Load all 8 chunks - fully unrolled
   __m128i chunk0 = _mm_load_si128((__m128i*)&original_counters[start1]);
   __m128i chunk1 = _mm_load_si128((__m128i*)&original_counters[start1 + 8]);
   __m128i chunk2 = _mm_load_si128((__m128i*)&original_counters[start1 + 16]);
   __m128i chunk3 = _mm_load_si128((__m128i*)&original_counters[start1 + 24]);
   __m128i chunk4 = _mm_load_si128((__m128i*)&original_counters[start2]);
   __m128i chunk5 = _mm_load_si128((__m128i*)&original_counters[start2 + 8]);
   __m128i chunk6 = _mm_load_si128((__m128i*)&original_counters[start2 + 16]);
   __m128i chunk7 = _mm_load_si128((__m128i*)&original_counters[start2 + 24]);

   // Compute all minpos operations consecutively - fully unrolled
   __m128i minpos_results[8];
   minpos_results[0] = _mm_minpos_epu16(chunk0);
   minpos_results[1] = _mm_minpos_epu16(chunk1);
   minpos_results[2] = _mm_minpos_epu16(chunk2);
   minpos_results[3] = _mm_minpos_epu16(chunk3);
   minpos_results[4] = _mm_minpos_epu16(chunk4);
   minpos_results[5] = _mm_minpos_epu16(chunk5);
   minpos_results[6] = _mm_minpos_epu16(chunk6);
   minpos_results[7] = _mm_minpos_epu16(chunk7);

   // Extract just the minimum values for the final reduction
   __m128i final_mins = _mm_setr_epi16(
       _mm_extract_epi16(minpos_results[0], 0), _mm_extract_epi16(minpos_results[1], 0),
       _mm_extract_epi16(minpos_results[2], 0), _mm_extract_epi16(minpos_results[3], 0),
       _mm_extract_epi16(minpos_results[4], 0), _mm_extract_epi16(minpos_results[5], 0),
       _mm_extract_epi16(minpos_results[6], 0), _mm_extract_epi16(minpos_results[7], 0));

   // Find which chunk contains the global minimum
   __m128i final_minpos  = _mm_minpos_epu16(final_mins);
   int     min_chunk_idx = _mm_extract_epi16(final_minpos, 1);

   // Get the local index directly from the appropriate minpos result
   int local_idx = _mm_extract_epi16(minpos_results[min_chunk_idx], 1);

   // Compute the base offset for the chunk that contained the minimum
   const int base_offsets[8] = {start1, start1 + 8, start1 + 16, start1 + 24,
                                start2, start2 + 8, start2 + 16, start2 + 24};

   // Return the final global index
   return base_offsets[min_chunk_idx] + local_idx;
}
#endif

// Global implementation that uses the best available optimized version
int find_approx_min_index(uint16_t* original_counters, int start1, int start2)
{
#if defined(HAS_NEON)
   return find_approx_min_index_neon_v11(original_counters, start1, start2);
#elif defined(HAS_SSE41)
   return find_approx_min_index_sse41(original_counters, start1, start2);
#else
   return find_approx_min_index_tournament(original_counters, start1, start2);
#endif
}

// For testing purposes, we'll use a 16-byte aligned allocator
template <typename T>
T* aligned_alloc(size_t count, size_t alignment = 16)
{
   void* ptr = nullptr;
#if defined(_WIN32)
   ptr = _aligned_malloc(count * sizeof(T), alignment);
#else
   if (posix_memalign(&ptr, alignment, count * sizeof(T)) != 0)
   {
      ptr = nullptr;
   }
#endif
   return static_cast<T*>(ptr);
}

template <typename T>
void aligned_free(T* ptr)
{
#if defined(_WIN32)
   _aligned_free(ptr);
#else
   free(ptr);
#endif
}

// Benchmark function
void benchmark(int num_iterations, int data_size)
{
   std::random_device rd;
   std::mt19937       gen(rd());
   // Limit random values to be less than 2^10 (1024) to work with the v13 implementation
   std::uniform_int_distribution<> dist(1, 1023);  // Avoid 0 and use values < 2^10

   uint16_t* counters = aligned_alloc<uint16_t>(data_size, 16);

   // Fill with random data
   for (int i = 0; i < data_size; i++)
      counters[i] = dist(gen);

   // Define start positions (removing constexpr)
   int start1 = 0;
   int start2 = data_size / 2;

   // Validate all implementations against the scalar implementation
   int      scalar_min_idx          = find_approx_min_index_scalar(counters, start1, start2);
   uint16_t scalar_min_value        = counters[scalar_min_idx];
   uint16_t scalar_min_value_masked = scalar_min_value & 0xFFC0;  // Mask out lower 6 bits

   auto validate_result = [&](const char* name, auto func) -> bool
   {
      int      min_idx          = func(counters, start1, start2);
      uint16_t min_value        = counters[min_idx];
      uint16_t min_value_masked = min_value & 0xFFC0;  // Mask out lower 6 bits

      bool is_correct = (min_value_masked == scalar_min_value_masked);

      if (!is_correct)
      {
         std::cout << ORANGE_COLOR << "WARNING: " << name << " found different minimum: "
                   << "0x" << std::hex << min_value << " (masked: 0x" << min_value_masked << "), "
                   << "reference found: 0x" << scalar_min_value << " (masked: 0x"
                   << scalar_min_value_masked << ")" << std::dec << RESET_COLOR << std::endl;
      }

      return is_correct;
   };

   struct BenchmarkResult
   {
      std::string name;
      double      time_us;
      double      speedup_vs_scalar;
      bool        correct_min;
   };

   std::vector<BenchmarkResult> results;

   auto benchmark_impl = [&](const char* name, auto func) -> double
   {
      bool correct_min = validate_result(name, func);

      auto         start_time = std::chrono::high_resolution_clock::now();
      volatile int result     = 0;

      for (int i = 0; i < num_iterations; i++)
         result = func(counters, start1, start2);

      // Sanity check to ensure min is actually minimum
      volatile uint16_t min_value = counters[result];
      // Only check the actual ranges we're supposed to search in
      for (int i = 0; i < 32; i++)
      {
         if (counters[i] < min_value)
            std::cerr << "FAIL: Found " << counters[i] << " at " << i << " which is less than "
                      << min_value << " at " << result << std::endl;
      }
      for (int i = data_size / 2; i < data_size / 2 + 32; i++)
      {
         if (counters[i] < min_value)
            std::cerr << "FAIL: Found " << counters[i] << " at " << i << " which is less than "
                      << min_value << " at " << result << std::endl;
      }

      auto end_time = std::chrono::high_resolution_clock::now();
      auto elapsed  = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

      double time_us = static_cast<double>(elapsed.count()) / num_iterations;

      std::cout << std::left << std::setw(40) << name << ": " << std::fixed << std::setprecision(6)
                << time_us << " μs";

      static double scalar_time = 0.0;
      double        speedup     = 0.0;

      if (strcmp(name, "Scalar") == 0)
      {
         scalar_time = time_us;
         speedup     = 1.0;
         std::cout << " (baseline)";
      }
      else
      {
         speedup = scalar_time / time_us;
         std::cout << " (" << speedup << "x faster)";
      }

      if (!correct_min)
      {
         std::cout << " " << ORANGE_COLOR << "⚠ INCORRECT MINIMUM" << RESET_COLOR;
      }

      std::cout << std::endl;

      results.push_back({name, time_us, speedup, correct_min});

      return time_us;
   };

   // Run benchmarks for all implementations
   std::cout << "\nRunning benchmarks...\n" << std::endl;

   auto scalar_time = benchmark_impl("Scalar", find_approx_min_index_scalar);

   // Update the speedup for the scalar implementation
   results[0].speedup_vs_scalar = 1.0;

   auto tournament_time =
       benchmark_impl("Tournament reduction (branchless)", find_approx_min_index_tournament);

#ifdef HAS_NEON
   auto neon_v3_time = benchmark_impl("ARM NEON v3", find_approx_min_index_neon_v3);

   // NEON implementations for accurate performance comparison
   auto neon_v4_time =
       benchmark_impl("ARM NEON v4 (embedded indices)", find_approx_min_index_neon_v4);
   auto neon_v5_time =
       benchmark_impl("ARM NEON v5 (optimized parallel)", find_approx_min_index_neon_v5);
   auto neon_v6_time =
       benchmark_impl("ARM NEON v6 (fully vectorized)", find_approx_min_index_neon_v6);
   auto neon_v7_time =
       benchmark_impl("ARM NEON v7 (using vminv/vminvq)", find_approx_min_index_neon_v7);
   auto neon_v8_time =
       benchmark_impl("ARM NEON v8 (vectorized min detection)", find_approx_min_index_neon_v8);
   auto neon_v9_time =
       benchmark_impl("ARM NEON v9 (optimized memory access)", find_approx_min_index_neon_v9);
   auto neon_v10_time =
       benchmark_impl("ARM NEON v10 (6-bit global indices)", find_approx_min_index_neon_v10);
   auto neon_v11_time =
       benchmark_impl("ARM NEON v11 (direct global indexing)", find_approx_min_index_neon_v11);
   auto neon_v12_time = benchmark_impl("ARM NEON v12 (6-bit global indices with shift)",
                                       find_approx_min_index_neon_v12);
   auto neon_v13_time = benchmark_impl("ARM NEON v13 (6-bit global indices with VSLI)",
                                       find_approx_min_index_neon_v13);
   auto neon_time     = benchmark_impl("ARM NEON (current)", find_approx_min_index_neon);
#endif

#ifdef HAS_SSE41
   auto sse41_time = benchmark_impl("SSE4.1", find_approx_min_index_sse41);
#endif

   auto cross_platform_time = benchmark_impl("Cross-platform", find_approx_min_index);

   // Calculate speedups relative to scalar (should be redundant but ensuring consistency)
   for (auto& result : results)
   {
      // We should already have the correct speedup values from earlier
      // But recalculate just to be sure everything's consistent
      result.speedup_vs_scalar = scalar_time / result.time_us;
   }

   // Find the fastest implementation
   auto fastest_impl = std::min_element(results.begin(), results.end(),
                                        [](const BenchmarkResult& a, const BenchmarkResult& b)
                                        { return a.time_us < b.time_us; });

   // Print results table
   std::cout << "\n--------------------------------------------------------------\n";
   std::cout << "| Algorithm                                     | Time (μs) | Speedup |\n";
   std::cout << "|------------------------------------------------|-----------|---------|"
             << std::endl;

   for (const auto& result : results)
   {
      std::ostringstream time_ss, speedup_ss;

      // Adjust precision based on magnitude
      if (result.time_us < 0.001)
         time_ss << std::fixed << std::setprecision(6);
      else if (result.time_us < 0.01)
         time_ss << std::fixed << std::setprecision(6);
      else if (result.time_us < 0.1)
         time_ss << std::fixed << std::setprecision(5);
      else
         time_ss << std::fixed << std::setprecision(4);

      time_ss << result.time_us;

      // Format speedup with appropriate precision (we expect 10-20x range)
      speedup_ss << std::fixed << std::setprecision(2) << result.speedup_vs_scalar;

      // If this is the fastest implementation, highlight it in green
      if (&result == &(*fastest_impl))
      {
         std::cout << "| \033[32m" << std::left << std::setw(46) << result.name << "| "
                   << std::setw(9) << time_ss.str() << " | " << std::setw(7) << speedup_ss.str()
                   << " |\033[0m" << std::endl;
      }
      else
      {
         std::cout << "| " << std::left << std::setw(46) << result.name << "| " << std::setw(9)
                   << time_ss.str() << " | " << std::setw(7) << speedup_ss.str() << " |"
                   << std::endl;
      }
   }

   std::cout << "--------------------------------------------------------------" << std::endl;

   aligned_free(counters);

   // Print final results table with ranking
   std::cout << "\n======================================================================="
             << std::endl;
   std::cout << "FINAL RESULTS (sorted by speed)" << std::endl;
   std::cout << "======================================================================="
             << std::endl;
   std::cout << std::left << std::setw(50) << "Algorithm"
             << "| " << std::setw(14) << "Time (μs)"
             << "| " << std::setw(10) << "Speedup"
             << "| "
             << "Status" << std::endl;
   std::cout << "-----------------------------------------------------------------------"
             << std::endl;

   // Sort by time (fastest first)
   std::sort(results.begin(), results.end(), [](const BenchmarkResult& a, const BenchmarkResult& b)
             { return a.time_us < b.time_us; });

   for (const auto& result : results)
   {
      std::cout << std::left << std::setw(50) << result.name << "| " << std::fixed
                << std::setprecision(6) << std::setw(14) << result.time_us << "| " << std::fixed
                << std::setprecision(2) << std::setw(10) << result.speedup_vs_scalar;

      if (result.correct_min)
      {
         std::cout << "| " << GREEN_COLOR << "✓ CORRECT" << RESET_COLOR;
      }
      else
      {
         std::cout << "| " << ORANGE_COLOR << "⚠ INCORRECT" << RESET_COLOR;
      }

      std::cout << std::endl;
   }

   std::cout << "======================================================================="
             << std::endl;

   // Find the fastest correct implementation
   auto fastest_correct = std::find_if(results.begin(), results.end(),
                                       [](const BenchmarkResult& r) { return r.correct_min; });
   if (fastest_correct != results.end())
   {
      std::cout << "\nFastest correct implementation: " << GREEN_COLOR << fastest_correct->name
                << " (" << fastest_correct->time_us << " μs, " << fastest_correct->speedup_vs_scalar
                << "x speedup)" << RESET_COLOR << std::endl;
   }
   else
   {
      std::cout << "\n"
                << ORANGE_COLOR << "WARNING: No implementation found the correct minimum!"
                << RESET_COLOR << std::endl;
   }
}

int main()
{
   // Run min_index benchmarks
   std::cout << "Running min_index benchmarks with real performance measurements...\n";
   benchmark(500000, 4096);

   return 0;
}
