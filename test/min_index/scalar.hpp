#pragma once
#include <cstdint>

// Basic scalar implementation - legacy function, will be deprecated
int find_approx_min_index_scalar(uint16_t* __attribute__((aligned(128))) original_counters,
                                 int                                     start1,
                                 int                                     start2)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   int      min_index = start1;
   uint16_t min_value = original_counters[start1];

   for (int i = start1 + 1; i < start1 + 32; i++)
   {
      if (original_counters[i] < min_value)
      {
         min_value = original_counters[i];
         min_index = i;
      }
   }

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

// New 64-value version of scalar implementation with single start parameter
int find_approx_min_index_scalar_64(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   int      min_index = start;
   uint16_t min_value = original_counters[start];

   // Process all 64 values in the segment
   for (int i = start + 1; i < start + 64; i++)
   {
      if (original_counters[i] < min_value)
      {
         min_value = original_counters[i];
         min_index = i;
      }
   }

   return min_index;
}

// 32-value version of scalar implementation - one segment only
int find_approx_min_index_scalar_32(uint16_t* __attribute__((aligned(128))) original_counters,
                                    int                                     start)
{
   // Inform compiler about alignment
   original_counters = (uint16_t*)__builtin_assume_aligned(original_counters, 128);

   // Simply find the minimum in the 32-element segment
   int      min_index = start;
   uint16_t min_value = original_counters[start];

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