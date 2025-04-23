#pragma once
#include <algorithm>
#include <cassert>
#include <cstring>
#include <string_view>
#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace ucc
{
   // This always returns a view into the first argument
   inline std::string_view common_prefix(std::string_view a, std::string_view b)
   {
      return {a.begin(), std::mismatch(a.begin(), a.end(), b.begin(), b.end()).first};
   }
   /**
    * This was benchmarked to be the fastest implementation for small arrays, likely
    * because the branch predictor can predict the loop end very well and the extra
    * adds are less expensive than the branch mispredict penalty.
    */
   inline uint8_t lower_bound_small(const uint8_t* data, uint32_t size, uint8_t byte) noexcept
   {
      assert(size < 8);
      int index = 0;
      // Simple loop-based implementation
      for (size_t i = 0; i < size; i++)
         index += (data[i] < byte);
      return index;
   }
   /**
    * Because the CPU can do multiple compares and adds in parallel when there
    * isn't a data dependency, this entire method takes 2 cycles to do the compares,
    * entire method takes 2 cycles to do the compares, and 2 cycles to do
    * the adds, for a total of 4 cycles (ideally). 
    */
   inline int lowerbound_unroll8(const uint8_t arr[8], uint8_t value) noexcept
   {
      // Directly sum all comparisons without storing to an intermediate variable
      // CPU can do these compares and adds in parallel
      return (arr[0] < value) + (arr[1] < value) + (arr[2] < value) + (arr[3] < value) +
             (arr[4] < value) + (arr[5] < value) + (arr[6] < value) + (arr[7] < value);
   }
   /*
   inline int lower_bound_small(const uint8_t* arr, size_t size, uint8_t value) noexcept
   {
      auto sl  = arr;
      auto slp = sl;
      auto sle = slp + size;

      while (slp != sle && uint8_t(*slp) < value)
         ++slp;
      return slp - sl;
   }
   */
#if defined(__ARM_NEON)
   // NEON variable-length implementation
   inline int lower_bound_neon(const uint8_t* arr, size_t size, uint8_t value) noexcept
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

         total_count += chunk_count;
         // If we found a position in this chunk where the value should be
         if (chunk_count < 16)
            return total_count;

         // All elements in this chunk are < value, move to next chunk
         offset += 16;
      }
      if (offset == size)
         return size;

      return total_count + lower_bound_small(arr + offset, size - offset, value);
   }
#endif

   // Using find_byte_unroll in a loop to support any size
   inline int lower_bound_scalar(const uint8_t* arr, size_t size, uint8_t value) noexcept
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
      return offset + lower_bound_small(arr + offset, size - offset, value);
   }

   /**
    * @return the index of the byte in the sorted array, or size if not found
    */
   inline uint16_t lower_bound(const uint8_t* data, uint32_t size, uint8_t byte) noexcept
   {
      assert(size < std::numeric_limits<uint16_t>::max());
      if (size < 8)
         return lower_bound_small(data, size, byte);
      else if (size < 16)
         return lower_bound_scalar(data, size, byte);
#if defined(__ARM_NEON)
      return lower_bound_neon(data, size, byte);
#else
      return lower_bound_scalar(data, size, byte);
#endif
   }

   /**
    * This variant assumes that it is safe to read up to 15 bytes past data+size so that
    * SIMD instructions can be used to process the data without checking for bounds or
    * This is shown to be over 2x faster than all other impl, espeically for small arrays.
    * It doesn't matter what data is in the padded bytes, just as long as it is safe to read
    */
   inline uint16_t lower_bound_padded(const uint8_t* data, size_t size, uint8_t byte) noexcept
   {
#if defined(__ARM_NEON)
      const uint8_t* start_data =
          data;  // Keep original start pointer for partial chunk calculation
      uint16_t         total_count = 0;
      const uint8x16_t search_val  = vdupq_n_u8(byte);
      const uint8x16_t one_mask    = vdupq_n_u8(1);  // For converting 0xFF to 0x01

      // Calculate number of full chunks
      const size_t num_full_chunks = size / 16;

      // Process full 16-byte chunks first using a counter-based loop (counting down to zero implicitly)
      for (size_t i = num_full_chunks; i--;)
      {
         // Load 16 bytes directly using the current data pointer
         uint8x16_t data_vec = vld1q_u8(data);

         // Compare: 0xFF where data < byte
         uint8x16_t cmp_lt_byte = vcltq_u8(data_vec, search_val);

         // Mask the comparison result to get 0x01 for matches instead of 0xFF
         uint8x16_t masked_result = vandq_u8(cmp_lt_byte, one_mask);

         // Count elements in this chunk that are < value using horizontal add
         uint16_t chunk_count = vaddlvq_u8(masked_result);

         total_count += chunk_count;
         // Check for early exit
         if (chunk_count < 16)
         {
            // Boundary is within this chunk..we are done
            return total_count;
         }

         // Advance the data pointer for the next chunk
         data += 16;
      }

      // Process the final potentially partial chunk if size is not a multiple of 16
      // Note: This part is only reached if all full chunks contained elements < byte
      // Use vaddlvq for counting here as well, applying the index mask.
      size_t remaining_bytes = size - (num_full_chunks * 16);
      if (remaining_bytes > 0) [[likely]]  // 15 out of 16 times this will be true
      {
         const uint8x16_t indices = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

         // Load the last chunk (reading past end is safe due to padding)
         // 'data' pointer is already positioned correctly after the loop
         uint8x16_t data_vec = vld1q_u8(data);

         // Compare: 0xFF where data < byte
         uint8x16_t cmp_lt_byte = vcltq_u8(data_vec, search_val);

         // Create mask for valid indices within the remaining bytes
         uint8x16_t size_vec = vdupq_n_u8((uint8_t)remaining_bytes);
         uint8x16_t valid_index_mask =
             vcltq_u8(indices, size_vec);  // 0xFF where index < remaining_bytes

         // Combine masks: count only if (data < byte) AND (index is valid)
         uint8x16_t combined_mask = vandq_u8(cmp_lt_byte, valid_index_mask);

         // Mask the combined result to get 0x01 for matches instead of 0xFF
         uint8x16_t masked_result = vandq_u8(combined_mask, one_mask);

         // Count elements in the masked final chunk using horizontal add
         uint16_t chunk_count = vaddlvq_u8(masked_result);

         // Add count from the final chunk to the total
         total_count += chunk_count;
      }

      return total_count;

#else
      // Fallback if NEON is not available
      // Note: This fallback is doesn't leverage the padding guarantee.
      return ucc::lower_bound(data, size, byte);  // Use original pointer for fallback
#endif
   }

   /**
    * NEON-accelerated lower_bound that does *not* assume padding.
    * It processes full chunks directly and copies the final partial chunk 
    * to a temporary buffer to allow safe 16-byte NEON loads.
    */
   inline uint16_t lower_bound_unpadded(const uint8_t* data, size_t size, uint8_t byte) noexcept
   {
      return lower_bound_neon(data, size, byte);
#if defined(__ARM_NEON)
      const uint8_t* start_data =
          data;  // Keep original start pointer for partial chunk calculation/fallback
      uint16_t         total_count = 0;
      const uint8x16_t search_val  = vdupq_n_u8(byte);
      const uint8x16_t one_mask    = vdupq_n_u8(1);  // For converting 0xFF to 0x01

      // Calculate number of full chunks
      const size_t num_full_chunks = size / 16;

      // Process full 16-byte chunks first (safe to read directly)
      for (size_t i = num_full_chunks; i--;)
      {
         // Load 16 bytes directly using the current data pointer
         uint8x16_t data_vec = vld1q_u8(data);

         // Compare: 0xFF where data < byte
         uint8x16_t cmp_lt_byte = vcltq_u8(data_vec, search_val);

         // Mask the comparison result to get 0x01 for matches instead of 0xFF
         uint8x16_t masked_result = vandq_u8(cmp_lt_byte, one_mask);

         // Count elements in this chunk that are < value using horizontal add
         uint16_t chunk_count = vaddlvq_u8(masked_result);

         // Otherwise, at least one element was less. Add the count from this chunk
         // and decide if we need to continue based on whether the chunk was full.
         if (chunk_count < 16)
         {
            return total_count + chunk_count;
         }

         // If we get here, chunk_count must have been 16.
         total_count += 16;

         // Advance the data pointer for the next chunk
         data += 16;
      }

      // Process the final potentially partial chunk using a temporary buffer
      size_t remaining_bytes = size - (num_full_chunks * 16);
      if (remaining_bytes > 0) [[likely]]
      {
         uint8_t temp_buffer[16];
         // Copy remaining data into the temporary buffer
         std::memcpy(temp_buffer, data, remaining_bytes);
         // Optional: Zero out the rest (might not be strictly necessary due to masking below)
         std::memset(temp_buffer + remaining_bytes, 0xff, 16 - remaining_bytes);

         // const uint8x16_t indices = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

         // Load from the temporary buffer (always safe)
         uint8x16_t data_vec = vld1q_u8(temp_buffer);

         // Compare: 0xFF where data < byte
         uint8x16_t cmp_lt_byte = vcltq_u8(data_vec, search_val);

         // Create mask for valid indices within the remaining bytes
         //   uint8x16_t size_vec = vdupq_n_u8((uint8_t)remaining_bytes);
         //      uint8x16_t valid_index_mask =
         //         vcltq_u8(indices, size_vec);  // 0xFF where index < remaining_bytes

         // Combine masks: count only if (data < byte) AND (index is valid)
         //    uint8x16_t combined_mask = vandq_u8(cmp_lt_byte, valid_index_mask);

         // Mask the combined result to get 0x01 for matches instead of 0xFF
         //   uint8x16_t masked_result = vandq_u8(combined_mask, one_mask);

         // Count elements in the masked final chunk using horizontal add
         //    uint16_t chunk_count = vaddlvq_u8(masked_result);
         // Mask the comparison result to get 0x01 for matches instead of 0xFF
         uint8x16_t masked_result = vandq_u8(cmp_lt_byte, one_mask);

         // Count elements in this chunk that are < value using horizontal add
         uint16_t chunk_count = vaddlvq_u8(masked_result);

         // Add count from the final chunk to the total
         total_count += chunk_count;
      }

      return total_count;

#else
      // Fallback if NEON is not available
      return lower_bound_scalar(start_data, size, byte);  // Use original pointer for fallback
#endif
   }

   /**
    * Finds the first occurrence of a byte value in an array.
    * Uses a fast SIMD-like approach by processing 8 bytes at a time.
    * For each 8-byte chunk:
    * 1. Broadcasts target value to all bytes
    * 2. XORs with data to find matches (0 where bytes match)
    * 3. Uses bit manipulation to detect zero bytes
    * 4. Returns index of first match if found
    * 
    * @param arr pointer to byte array to search
    * @param size number of bytes in array
    * @param value byte value to find
    * @return index of first occurrence of value, or size if not found
    */

   inline int find_byte(const uint8_t* arr, size_t size, uint8_t value)
   {
      const uint64_t target   = value * 0x0101010101010101ULL;  // Broadcast value to all bytes
      const uint8_t* end      = arr + size;
      const uint8_t* last_pos = end - 8;
      const uint8_t* p        = arr;

      // Process full 8-byte chunks without checking inside the loop
      while (p <= last_pos)
      {
         const uint64_t data            = *(const uint64_t*)p;
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
      auto remaining = end - p;
      if (remaining >= 4)
      {
         // Process remaining bytes using same pattern
         const uint32_t data_xor_target = *(uint32_t*)p ^ uint32_t(target);
         uint32_t       mask            = (data_xor_target - 0x01010101) & ~data_xor_target;
         mask &= 0x80808080;

         if (mask)
         {
            size_t offset = p - arr;
            return offset + (__builtin_ctzll(mask) >> 3);
         }
         p += 4;
      }
      while (p < end)
      {
         if (*p == value)
            return p - arr;
         ++p;
      }
      // Value not found
      return size;
   }
#include <arm_neon.h>
#if defined(__ARM_NEON)
   /// this is about 4.8x faster than std::find
   inline int find_u32x16_neon(uint32x4_t v0,
                               uint32x4_t v1,
                               uint32x4_t v2,
                               uint32x4_t v3,
                               uint32_t   search_value)
   {
      // Broadcast the search value
      uint32x4_t search_vec = vdupq_n_u32(search_value);

      // Compare
      uint32x4_t cmp0 = vceqq_u32(v0, search_vec);  // e0-e3
      uint32x4_t cmp1 = vceqq_u32(v1, search_vec);  // e4-e7
      uint32x4_t cmp2 = vceqq_u32(v2, search_vec);  // e8-e11
      uint32x4_t cmp3 = vceqq_u32(v3, search_vec);  // e12-e15

      // Narrow to 16-bit
      uint16x4_t narrow0 = vmovn_u32(cmp0);
      uint16x4_t narrow1 = vmovn_u32(cmp1);
      uint16x4_t narrow2 = vmovn_u32(cmp2);
      uint16x4_t narrow3 = vmovn_u32(cmp3);

      // Combine pairs of 16-bit vectors before narrowing
      uint16x8_t combined01 = vcombine_u16(narrow0, narrow1);
      uint16x8_t combined23 = vcombine_u16(narrow2, narrow3);

      // Narrow to 8-bit
      uint8x8_t bytes0_narrowed = vmovn_u16(combined01);  // e0-e7
      uint8x8_t bytes1_narrowed = vmovn_u16(combined23);  // e8-e15

      // Directly get the 64-bit masks from the 8-byte vectors
      uint64_t low64  = vget_lane_u64(vreinterpret_u64_u8(bytes0_narrowed), 0);
      uint64_t high64 = vget_lane_u64(vreinterpret_u64_u8(bytes1_narrowed), 0);

      // Use user's formula: (ctz(mask | bit63) + 1) / 8
      // This yields index 0-7 if found in the 64-bit mask, or 8 if not found.
      uint64_t low  = (__builtin_ctzll(low64 | (1ULL << 63)) + 1) / 8;
      uint64_t high = (__builtin_ctzll(high64 | (1ULL << 63)) + 1) / 8;

      // Branchless calculation using optimized check for (low == 8):
      // low + (high * (low >> 3))
      // If low is 0-7, low >> 3 is 0, result is low.
      // If low is 8, low >> 3 is 1, result is 8 + high (which is 8-15 or 16).
      return low + (high * (low >> 3));
   }
#endif
   inline int find_u32x16_scalar(const uint32_t* arr, uint32_t value)
   {
      return std::find(arr, arr + 16, value) - arr;
   }

   /// this is about 3.5x faster than std::find
   inline int find_u32x16_scalar_unrolled(const uint32_t* arr, size_t size, uint32_t value)
   {
      assert(size <= 16);
      uint64_t result = 1 << size;
      result |= (arr[0] == value) | ((arr[1] == value) << 1) | ((arr[2] == value) << 2) |
                ((arr[3] == value) << 3) | ((arr[4] == value) << 4) | ((arr[5] == value) << 5) |
                ((arr[6] == value) << 6) | ((arr[7] == value) << 7) | ((arr[8] == value) << 8) |
                ((arr[9] == value) << 9) | ((arr[10] == value) << 10) | ((arr[11] == value) << 11) |
                ((arr[12] == value) << 12) | ((arr[13] == value) << 13) |
                ((arr[14] == value) << 14) | ((arr[15] == value) << 15);
      return __builtin_ctzll(result);
   }

#if defined(__ARM_NEON)
   /**
    * Finds the first occurance of value in array of size, where size <= 16 and
    * it is safe to read garbage values for up to size 16 (aka 64 bytes) from arr. 
    */
   inline uint32_t find_u32_padded16_neon(const uint32_t* arr, size_t size, uint32_t search_value)
   {
      uint32x4_t v0 = vld1q_u32(arr);
      uint32x4_t v1 = vld1q_u32(arr + 4);
      uint32x4_t v2 = vld1q_u32(arr + 8);
      uint32x4_t v3 = vld1q_u32(arr + 12);

      uint32x4_t search_vec = vdupq_n_u32(search_value);

      // Compare
      uint32x4_t cmp0 = vceqq_u32(v0, search_vec);  // e0-e3
      uint32x4_t cmp1 = vceqq_u32(v1, search_vec);  // e4-e7
      uint32x4_t cmp2 = vceqq_u32(v2, search_vec);  // e8-e11
      uint32x4_t cmp3 = vceqq_u32(v3, search_vec);  // e12-e15

      // Narrow to 16-bit
      uint16x4_t narrow0 = vmovn_u32(cmp0);
      uint16x4_t narrow1 = vmovn_u32(cmp1);
      uint16x4_t narrow2 = vmovn_u32(cmp2);
      uint16x4_t narrow3 = vmovn_u32(cmp3);

      // Combine pairs of 16-bit vectors before narrowing
      uint16x8_t combined01 = vcombine_u16(narrow0, narrow1);
      uint16x8_t combined23 = vcombine_u16(narrow2, narrow3);

      // Narrow to 8-bit
      uint8x8_t bytes0_narrowed = vmovn_u16(combined01);  // e0-e7
      uint8x8_t bytes1_narrowed = vmovn_u16(combined23);  // e8-e15

      // Directly get the 64-bit masks from the 8-byte vectors
      uint64_t low64  = vget_lane_u64(vreinterpret_u64_u8(bytes0_narrowed), 0);
      uint64_t high64 = vget_lane_u64(vreinterpret_u64_u8(bytes1_narrowed), 0);

      // Use user's formula: (ctz(mask | bit63) + 1) / 8
      // This yields index 0-7 if found in the 64-bit mask, or 8 if not found.
      auto low  = (__builtin_ctzll(low64 | (1ULL << 63)) + 1) / 8;
      auto high = (__builtin_ctzll(high64 | (1ULL << 63)) + 1) / 8;

      // Branchless calculation using optimized check for (low == 8):
      // low + (high * (low >> 3))
      // If low is 0-7, low >> 3 is 0, result is low.
      // If low is 8, low >> 3 is 1, result is 8 + high (which is 8-15 or 16).
      uint32_t result = low + (high * (low >> 3));
      return size < result ? size : result;
   }
#endif
   inline int find_u32x16(const uint32_t* arr, size_t size, uint32_t value)
   {
      assert(size <= 16);
#if defined(__ARM_NEON)
      /*
      uint32x4_t v0 = vld1q_u32(arr);
      uint32x4_t v1 = vld1q_u32(arr + 4);
      uint32x4_t v2 = vld1q_u32(arr + 8);
      uint32x4_t v3 = vld1q_u32(arr + 12);
      return find_u32x16_neon(v0, v1, v2, v3, value);
      */
      return find_u32_padded16_neon(arr, size, value);
#else
      return find_u32x16_scalar_unrolled(arr, size, value);
#endif
   }
}  // namespace ucc