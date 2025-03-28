#pragma once
#include <algorithm>
#include <cassert>
#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif
#include <arbtrie/debug.hpp>

namespace arbtrie
{
   /**
    * @return the index of the byte in the unsorted array, or size if not found
    */
   inline const uint8_t* find_byte(const uint8_t* data, uint32_t size, uint8_t byte)
   {
      return std::find(data, data + size, byte);
   }

   /**
    * This was benchmarked to be the fastest implementation for small arrays
    */
   inline uint8_t lower_bound_small(const uint8_t* data, uint32_t size, uint8_t byte)
   {
      assert(size < 8);
      int index = 0;
      // Simple loop-based implementation
      for (size_t i = 0; i < size; i++)
         index += (data[i] < byte);
      return index;
   }
   inline int lowerbound_unroll8(const uint8_t arr[8], uint8_t value)
   {
      // Directly sum all comparisons without storing to an intermediate variable
      // CPU can do these compares and adds in parallel
      return (arr[0] < value) + (arr[1] < value) + (arr[2] < value) + (arr[3] < value) +
             (arr[4] < value) + (arr[5] < value) + (arr[6] < value) + (arr[7] < value);
   }
   inline int lower_bound_small(const uint8_t* arr, uint8_t value, size_t size)
   {
      auto sl  = arr;
      auto slp = sl;
      auto sle = slp + size;

      while (slp != sle && uint8_t(*slp) < value)
         ++slp;
      return slp - sl;
   }
#if defined(__ARM_NEON)
   // NEON variable-length implementation
   inline int lower_bound_neon(const uint8_t* arr, uint8_t value, int size)
   {
      int offset      = 0;
      int total_count = 0;

      uint8x16_t search_val = vdupq_n_u8(value);
      // Create a mask of all 0x01 values for converting 0xFF results to 0x01
      uint8x16_t one_mask   = vdupq_n_u8(1);
      const int  size_min16 = size - 16;

      // Process in chunks of 16 bytes using horizontal add
      while (offset <= size_min16)
      {
         uint8x16_t data = vld1q_u8(arr + offset);

         // Compare: result will be 0xFF where data < search_val, 0 elsewhere
         uint8x16_t cmp_result = vcltq_u8(data, search_val);

         // Mask the comparison result to get 0x01 for matches instead of 0xFF
         uint8x16_t masked_result = vandq_u8(cmp_result, one_mask);

         // Count elements in this chunk that are < value
         // No need to divide, as each match is now exactly 1
         uint16_t chunk_count = vaddlvq_u8(masked_result);

         // If we found a position in this chunk where the value should be
         if (chunk_count < 16)
         {
            // Print masked_result as hex values
            return total_count + chunk_count;
         }

         // All elements in this chunk are < value, move to next chunk
         total_count += 16;
         offset += 16;
      }
      if (offset == size)
         return size;

      return total_count + lower_bound_small(arr + offset, value, size - offset);

      /*
      // Create temporary buffer aligned to 16 bytes
      alignas(16) uint8_t temp[16];
      const auto          remaining_bytes = size - offset;
      memcpy(temp, arr + offset, remaining_bytes);
      memset(temp + remaining_bytes, 0xff, 16 - remaining_bytes);

      uint8x16_t data = vld1q_u8(temp);
      // Compare: result will be 0xFF where data < search_val, 0 elsewhere
      uint8x16_t cmp_result = vcltq_u8(data, search_val);

      // Mask the comparison result to get 0x01 for matches instead of 0xFF
      uint8x16_t masked_result = vandq_u8(cmp_result, one_mask);

      // Count elements in the remaining bytes that are < value
      // No need to divide, as each match is now exactly 1
      uint16_t remaining_count = vaddlvq_u8(masked_result);

      // Subtract any counts from padding bytes
      //remaining_count -= (16 - (size - offset));

      return total_count + remaining_count;
      */
   }
#endif

   // Using find_byte_unroll in a loop to support any size
   inline int lower_bound_scalar(const uint8_t* arr, uint8_t value, size_t size)
   {
      int       offset    = 0;
      const int size_min8 = int(size) - 8;
      // Process in chunks of 8 bytes, using find_byte_unroll for each chunk
      while (offset <= size_min8)
      {
         // Find position within current 8-byte chunk
         const auto* tmp          = arr + offset;
         const auto  pos_in_chunk = (tmp[0] < value) + (tmp[1] < value) + (tmp[2] < value) +
                                   (tmp[3] < value) + (tmp[4] < value) + (tmp[5] < value) +
                                   (tmp[6] < value) + (tmp[7] < value);

         // Check if the value found is actually equal to what we're looking for
         // If it's less than the value, find_byte_unroll will return 8
         if (pos_in_chunk < 8)
            return offset + pos_in_chunk;

         offset += 8;
      }
      return offset + lower_bound_small(arr + offset, value, size - offset);
      /*
      while (offset < size)
      {
         if (arr[offset] >= value)
            return offset;
         offset++;
      }
      return offset;
      */
   }

   /**
    * @return the index of the byte in the sorted array, or size if not found
    */
   inline uint8_t lower_bound(const uint8_t* data, uint32_t size, uint8_t byte)
   {
      if (size < 8)
         return lower_bound_small(data, byte, size);
      else if (size < 16)
         return lower_bound_scalar(data, byte, size);
#if defined(__ARM_NEON)
      return lower_bound_neon(data, byte, size);
#else
      return lower_bound_scalar(data, byte, size);
#endif
   }

   inline int find_byte(const uint8_t* arr, uint8_t value, size_t size)
   {
      uint64_t       target   = value * 0x0101010101010101ULL;  // Broadcast value to all bytes
      const uint8_t* end      = arr + size;
      const uint8_t* last_pos = end - 8;
      const uint8_t* p        = arr;

      // Process full 8-byte chunks without checking inside the loop
      while (p <= last_pos)
      {
         uint64_t       data            = *(const uint64_t*)p;
         const uint64_t data_xor_target = data ^ target;
         uint64_t       mask = (data_xor_target - 0x0101010101010101ULL) & ~data_xor_target;
         mask &= 0x8080808080808080ULL;

         if (mask)
         {
            // Found the byte
            size_t offset = p - arr;
            return offset + (__builtin_ctzll(mask) >> 3);
         }

         p += 8;
      }

      uint64_t last_chunk = -1;
      memcpy(&last_chunk, p, end - p);
      // Process remaining bytes using same pattern
      const uint64_t data_xor_target = last_chunk ^ target;
      uint64_t       mask            = (data_xor_target - 0x0101010101010101ULL) & ~data_xor_target;
      mask &= 0x8080808080808080ULL;

      if (mask)
      {
         size_t offset = p - arr;
         return offset + (__builtin_ctzll(mask) >> 3);
      }

      // Value not found
      return size;
   }

}  // namespace arbtrie