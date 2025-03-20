#pragma once
#include <cstdint>

// Tournament-based implementation
int find_approx_min_index_tournament(uint16_t* __attribute__((aligned(128))) original_counters,
                                     int                                     start1,
                                     int                                     start2)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   uint16_t min_value = original_counters[start1];
   int      min_index = start1;

   // Tournament for first 32 values
   for (int i = start1 + 1; i < start1 + 32; i++)
   {
      if (original_counters[i] < min_value)
      {
         min_value = original_counters[i];
         min_index = i;
      }
   }

   // Tournament for second 32 values
   for (int i = start2; i < start2 + 32; i++)
   {
      if (original_counters[i] < min_value)
      {
         min_value = original_counters[i];
         min_index = i;
      }
   }

   return min_index;
}

// Tournament implementation for 32 values (1 segment)
int find_approx_min_index_tournament_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                        int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   uint16_t min_value = original_counters[start];
   int      min_index = start;

   // Tournament for 32 values
   for (int i = start + 1; i < start + 32; i++)
   {
      if (original_counters[i] < min_value)
      {
         min_value = original_counters[i];
         min_index = i;
      }
   }

   return min_index;
}

// New 64-value version of tournament implementation with single start parameter
int find_approx_min_index_tournament_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                        int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Break the 64 elements into pairs and find the minimum of each pair
   int min_idx = start;

   // Tournament-style reduction
   for (int i = 0; i < 32; i++)
   {
      int idx1 = start + i;
      int idx2 = start + i + 32;

      if (original_counters[idx2] < original_counters[idx1])
         min_idx = idx2;
      else
         min_idx = idx1;

      // Store the minimum back into the first position
      original_counters[idx1] = original_counters[min_idx];
   }

   // Now use the 32-value version on the first half that contains all the minimums
   return find_approx_min_index_tournament_32(original_counters, start);
}