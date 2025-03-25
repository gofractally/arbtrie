#pragma once
#include <cstdint>

/**
 * Tournament-based implementation using branchless comparison and 
 * value-index packing for finding minimum across two 32-value segments
 */
int find_approx_min_index_tournament(uint16_t* __attribute__((aligned(128))) original_counters,
                                     int                                     start1,
                                     int                                     start2)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // First find min in segment 1 using optimized tournament algorithm
   uint32_t tournament1[16];

   // First round: combine values and indices (32 -> 16)
   for (int i = 0, j = 0; i < 32; i += 2, j++)
   {
      int base       = start1 + i;
      int cmp        = (original_counters[base + 1] < original_counters[base]);
      tournament1[j] = ((uint32_t)original_counters[base + cmp] << 16) | (uint32_t)(base + cmp);
   }

   // Round 2: 8 comparisons (16 -> 8)
   for (int i = 0, j = 0; i < 16; i += 2, j++)
   {
      int cmp        = (tournament1[i + 1] < tournament1[i]);
      tournament1[j] = tournament1[i + cmp];
   }

   // Round 3: 4 comparisons (8 -> 4)
   for (int i = 0, j = 0; i < 8; i += 2, j++)
   {
      int cmp        = (tournament1[i + 1] < tournament1[i]);
      tournament1[j] = tournament1[i + cmp];
   }

   // Round 4: 2 comparisons (4 -> 2) - unrolled
   int cmp1       = (tournament1[1] < tournament1[0]);
   tournament1[0] = tournament1[cmp1];

   int cmp2       = (tournament1[3] < tournament1[2]);
   tournament1[1] = tournament1[2 + cmp2];

   // Final round for segment 1: 1 comparison (2 -> 1)
   int      cmp          = (tournament1[1] < tournament1[0]);
   uint32_t segment1_min = tournament1[cmp];

   // Now find min in segment 2
   uint32_t tournament2[16];

   // First round: combine values and indices (32 -> 16)
   for (int i = 0, j = 0; i < 32; i += 2, j++)
   {
      int base       = start2 + i;
      int cmp        = (original_counters[base + 1] < original_counters[base]);
      tournament2[j] = ((uint32_t)original_counters[base + cmp] << 16) | (uint32_t)(base + cmp);
   }

   // Round 2: 8 comparisons (16 -> 8)
   for (int i = 0, j = 0; i < 16; i += 2, j++)
   {
      int cmp        = (tournament2[i + 1] < tournament2[i]);
      tournament2[j] = tournament2[i + cmp];
   }

   // Round 3: 4 comparisons (8 -> 4)
   for (int i = 0, j = 0; i < 8; i += 2, j++)
   {
      int cmp        = (tournament2[i + 1] < tournament2[i]);
      tournament2[j] = tournament2[i + cmp];
   }

   // Round 4: 2 comparisons (4 -> 2) - unrolled
   cmp1           = (tournament2[1] < tournament2[0]);
   tournament2[0] = tournament2[cmp1];

   cmp2           = (tournament2[3] < tournament2[2]);
   tournament2[1] = tournament2[2 + cmp2];

   // Final round for segment 2: 1 comparison (2 -> 1)
   cmp                   = (tournament2[1] < tournament2[0]);
   uint32_t segment2_min = tournament2[cmp];

   // Compare the two segment minimums
   cmp                = (segment2_min < segment1_min);
   uint32_t final_min = cmp ? segment2_min : segment1_min;

   // Return the index part (low 16 bits) of the winner
   return final_min & 0xFFFF;
}

/**
 * Tournament implementation for 32 values (1 segment) using branchless comparison
 * and value-index packing for optimal performance
 */
int find_approx_min_index_tournament_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                        int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Store values and indices combined: high 16 bits = value, low 16 bits = index
   uint32_t tournament[16];

   // First round: combine values and indices (32 -> 16)
   for (int i = 0, j = 0; i < 32; i += 2, j++)
   {
      int base      = start + i;
      int cmp       = (original_counters[base + 1] < original_counters[base]);
      tournament[j] = ((uint32_t)original_counters[base + cmp] << 16) | (uint32_t)(base + cmp);
   }

   // Round 2: 8 comparisons (16 -> 8)
   for (int i = 0, j = 0; i < 16; i += 2, j++)
   {
      int cmp       = (tournament[i + 1] < tournament[i]);
      tournament[j] = tournament[i + cmp];
   }

   // Round 3: 4 comparisons (8 -> 4)
   for (int i = 0, j = 0; i < 8; i += 2, j++)
   {
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
 * Tournament implementation for 64 values (1 segment) using branchless comparison
 * and value-index packing for optimal performance
 */
int find_approx_min_index_tournament_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                        int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Store values and indices combined: high 16 bits = value, low 16 bits = index
   uint32_t tournament[32];

   // First round: combine values and indices (64 -> 32)
   for (int i = 0, j = 0; i < 64; i += 2, j++)
   {
      int base      = start + i;
      int cmp       = (original_counters[base + 1] < original_counters[base]);
      tournament[j] = ((uint32_t)original_counters[base + cmp] << 16) | (uint32_t)(base + cmp);
   }

   // Round 2: 16 comparisons (32 -> 16)
   for (int i = 0, j = 0; i < 32; i += 2, j++)
   {
      int cmp       = (tournament[i + 1] < tournament[i]);
      tournament[j] = tournament[i + cmp];
   }

   // Round 3: 8 comparisons (16 -> 8)
   for (int i = 0, j = 0; i < 16; i += 2, j++)
   {
      int cmp       = (tournament[i + 1] < tournament[i]);
      tournament[j] = tournament[i + cmp];
   }

   // Round 4: 4 comparisons (8 -> 4)
   for (int i = 0, j = 0; i < 8; i += 2, j++)
   {
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