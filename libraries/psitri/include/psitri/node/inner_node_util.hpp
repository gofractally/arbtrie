#pragma once
#ifdef __ARM_NEON
#include <arm_neon.h>
#endif
#include <psitri/node/node.hpp>

#include <array>
#include <cstdint>

namespace psitri
{
   struct cline_freq_table
   {
      std::array<uint8_t, 16> freq_table;
      uint32_t                clines_referenced;
      uint32_t needed_clines() const noexcept { return 32 - __builtin_clz(clines_referenced); }
      uint32_t compressed_clines() const noexcept { return std::popcount(clines_referenced); }
   };

   inline cline_freq_table create_cline_freq_table(const branch* begin, const branch* end)
   {
      cline_freq_table result = {.freq_table = {}, .clines_referenced = 0};
      for (auto b = begin; b != end; ++b)
      {
         auto ln = b->line();
         result.freq_table[ln]++;
         result.clines_referenced |= uint32_t(1) << ln;
      }
      return result;
   }

   /**
    * @brief Creates a 16-byte table where table[i] stores the 0-based rank
    *        of the i-th element among non-zero elements if freq_table[i] is non-zero,
    *        or the count of preceding non-zero elements if freq_table[i] is zero.
    *
    * Branchless scalar implementation.
    *
    * @param freq_table The input 16-byte frequency table (or similar data).
    * @return The calculated 16-byte lookup table.
    */
   inline std::array<uint8_t, 16> create_nth_set_bit_table_scalar(
       const std::array<uint8_t, 16>& freq_table)
   {
      std::array<uint8_t, 16> table = {};
      uint8_t non_zero_count        = 0;  // Tracks the count of non-zero elements found so far

      for (size_t i = 0; i < 16; ++i)
      {
         // Store the count of non-zero elements encountered *before* this index.
         table[i] = non_zero_count;

         // Increment the count if the current element is non-zero.
         // The expression (freq_table[i] != 0) evaluates to 1 (true) if non-zero,
         // and 0 (false) if zero. Adding this result achieves the conditional increment.
         non_zero_count += (freq_table[i] != 0);
      }

      return table;
   }
#ifdef __ARM_NEON
   /**
    * @brief Creates a 16-byte table where table[i] stores the 0-based rank
    *        of the i-th element among non-zero elements if array[i] is non-zero,
    *        or the count of preceding non-zero elements if array[i] is zero.
    *
    * Uses a parallel prefix sum (scan) algorithm with NEON intrinsics.
    * Tests show this is about 33% faster than the scalar version.
    *
    * @param freq_table The input 16-byte frequency table (or similar data).
    * @return The calculated 16-byte lookup table.
    */
   inline std::array<uint8_t, 16> create_nth_set_bit_table_neon(
       const std::array<uint8_t, 16>& freq_table)
   {
      std::array<uint8_t, 16> table = {};

      // Load 16 bytes
      uint8x16_t input = vld1q_u8(freq_table.data());

      // Compare against zero: 0xFF for non-zero, 0x00 for zero
      uint8x16_t mask = vtstq_u8(input, input);

      // Create 'ones' vector: 1 for non-zero input bytes, 0 otherwise
      uint8x16_t ones = vandq_u8(mask, vdupq_n_u8(1));

      // Create a zero vector for padding in shifts
      const uint8x16_t zeros = vdupq_n_u8(0);

      // Compute prefix sum (running sum of 'ones') using LEFT shifts with vextq
      uint8x16_t sum = ones;
      sum            = vaddq_u8(sum, vextq_u8(zeros, sum, 15));  // Shift left 1 (zeros | sum >> 1)
      sum            = vaddq_u8(sum, vextq_u8(zeros, sum, 14));  // Shift left 2
      sum            = vaddq_u8(sum, vextq_u8(zeros, sum, 12));  // Shift left 4
      sum            = vaddq_u8(sum, vextq_u8(zeros, sum, 8));   // Shift left 8

      // Adjust for 0-based indexing: subtract 'ones' (1 if input non-zero, 0 if zero)
      // If input[i] != 0: result[i] = (count up to i) - 1 = 0-based rank
      // If input[i] == 0: result[i] = (count up to i) - 0 = count before i
      uint8x16_t result = vsubq_u8(sum, vandq_u8(mask, vdupq_n_u8(1)));

      // Store result
      vst1q_u8(table.data(), result);

      return table;
   }
#endif
   /**
    * @brief Processes an input array, creating an output array where the high
    *        nibble is replaced via LUT lookup, preserving the low nibble.
    *        Scalar implementation using a standard loop.
    *
    * @pre input_data != nullptr
    * @pre output_data != nullptr
    * @pre N >= 2 && N <= 128 (inherited from NEON version's typical use case, but not strictly required by scalar logic)
    * @pre lut must contain 16 elements.
    *
    * @param input_data Pointer to the beginning of the input data array.
    * @param output_data Pointer to the beginning of the output data array.
    * @param N The number of elements to process.
    * @param lut The 16-byte lookup table.
    */
   inline void copy_branches_and_update_cline_index_scalar(
       const uint8_t*                 input_data,
       uint8_t*                       output_data,
       size_t                         N,
       const std::array<uint8_t, 16>& lut) noexcept  // Keep noexcept for consistency
   {
      assert(input_data != nullptr && "Input data pointer cannot be null");
      assert(output_data != nullptr && "Output data pointer cannot be null");
      // assert(N >= 2 && N <= 128 && "N should be between 2 and 128"); // Optional based on caller guarantees

      for (size_t i = 0; i < N; ++i)
      {
         // Get original byte from input
         const uint8_t original_byte = input_data[i];
         // Extract high nibble (index)
         const uint8_t index = original_byte >> 4;
         // Look up the value in the LUT (ensure index is 0-15)
         // LUT size is fixed at 16, index from >> 4 is always 0-15.
         const uint8_t lut_val = lut[index];
         // Isolate original low nibble
         const uint8_t low_nibble = original_byte & 0x0F;
         // Combine new high nibble (shifted LUT value) and original low nibble
         output_data[i] = (lut_val << 4) | low_nibble;
      }
   }

   inline std::array<uint8_t, 16> create_nth_set_bit_table(
       const std::array<uint8_t, 16>& freq_table)
   {
#ifdef __ARM_NEON
      return create_nth_set_bit_table_neon(freq_table);
#else
      return create_nth_set_bit_table_scalar(freq_table);
#endif
   }

   /**
    * @brief Processes an input array, creating an output array where the high
    *        nibble is replaced via LUT lookup, preserving the low nibble.
    *        Uses a fully branchless approach: performs the final end-aligned
    *        operation first, then loops a calculated number of times for preceding chunks.
    *
    * Benchmarks show this is 2x faster than the scalar version.
    * 
    * This is designed to copy branches while cloning an inner node, it should be the first
    * operation called because it may overwrite other data in the new node while it processes
    * the branches 16 bytes at a time. If there are less than 16 bytes then it will write
    * some garbage data before the input_data pointer (aka earlier parts of inner_node). It
    * will read similar garbage data from the source node. All told this gives us a branchless
    * approach that is as-fast as a byte-by-byte copy while also transforming the indicies pointing
    * to the clines.
    *
    * @pre input_data != nullptr
    * @pre output_data != nullptr
    * @pre N >= 2 && N <= 128
    * @pre It MUST be safe to read up to 15 bytes before input_data and past input_data + N - 1.
    * @pre It MUST be safe to write up to 15 bytes before output_data and past output_data + N - 1.
    *
    * @param input_data Pointer to the beginning of the input data array.
    * @param output_data Pointer to the beginning of the output data array.
    * @param N The number of elements to process.
    * @param lut The 16-byte lookup table.
    */
   inline void copy_branches_and_update_cline_index_neon(
       const uint8_t*                 input_data,
       uint8_t*                       output_data,
       size_t                         N,
       const std::array<uint8_t, 16>& lut) noexcept
   {
      // --- Setup ---
      const uint8x16_t lut_vec         = vld1q_u8(lut.data());
      const uint8x16_t low_nibble_mask = vdupq_n_u8(0x0F);

      // --- 1. Final Unconditional Operation (Aligned to End) ---
      // Calculate offset, load, compute, store for the chunk ending at N-1.
      const size_t final_offset = N - 16;
      {
         uint8x16_t data_vec             = vld1q_u8(input_data + final_offset);
         uint8x16_t indices              = vshrq_n_u8(data_vec, 4);
         uint8x16_t lut_vals_vec         = vqtbl1q_u8(lut_vec, indices);
         uint8x16_t new_high_nibbles     = vshlq_n_u8(lut_vals_vec, 4);
         uint8x16_t original_low_nibbles = vandq_u8(data_vec, low_nibble_mask);
         uint8x16_t result_vec           = vorrq_u8(new_high_nibbles, original_low_nibbles);
         vst1q_u8(output_data + final_offset, result_vec);
      }

      // --- 2. Main Loop (Processes chunks *before* the final one) ---
      // Calculate the number of loop iterations branchlessly.
      // This is the number of full 16-byte chunks before the final one.
      const size_t num_iterations = (N - 1) / 16;

      // Loop processing chunks starting at i=0, 16, ...
      for (size_t k = 0; k < num_iterations; ++k)
      {
         size_t     i                    = k * 16;  // Calculate offset directly
         uint8x16_t data_vec             = vld1q_u8(input_data + i);
         uint8x16_t indices              = vshrq_n_u8(data_vec, 4);
         uint8x16_t lut_vals_vec         = vqtbl1q_u8(lut_vec, indices);
         uint8x16_t new_high_nibbles     = vshlq_n_u8(lut_vals_vec, 4);
         uint8x16_t original_low_nibbles = vandq_u8(data_vec, low_nibble_mask);
         uint8x16_t result_vec           = vorrq_u8(new_high_nibbles, original_low_nibbles);
         vst1q_u8(output_data + i, result_vec);
      }
   }
   inline void copy_branches_and_update_cline_index(const branch*                  input_data,
                                                    branch*                        output_data,
                                                    size_t                         N,
                                                    const std::array<uint8_t, 16>& lut) noexcept
   {
#ifdef __ARM_NEON
      copy_branches_and_update_cline_index_neon((uint8_t*)input_data, (uint8_t*)output_data, N,
                                                lut);
#else
      copy_branches_and_update_cline_index_scalar((uint8_t*)input_data, (uint8_t*)output_data, N,
                                                  lut);
#endif
   }

   /**
    * @brief Copies uint32_t values from source to destination based on a bitmap,
    *        iterating exactly popcount(bitmap) times using GCC/Clang intrinsics directly.
    *        Uses uint32_t for bitmap to align with intrinsic types.
    *
    * @pre source != nullptr
    * @pre destination != nullptr
    * @pre Destination buffer must have space for at least __builtin_popcount(bitmap) elements.
    * @pre Source buffer must contain at least 16 elements if all bits could be set.
    * @pre Only the lower 16 bits of bitmap are considered significant.
    *
    * @param bitmap The 32-bit mask; only the lower 16 bits are used.
    * @param source Pointer to the source array (up to 16 relevant uint32_t values).
    * @param destination Pointer to the destination array.
    * @return The number of elements copied (popcount of the lower 16 bits of the original bitmap).
    */
   inline void copy_masked_cline_data(uint32_t           bitmap,
                                      const ptr_address* source,
                                      ptr_address*       destination) noexcept
   {
      assert(bitmap >> 16 == 0 && "Bitmap must be less than 65536");
      assert(bitmap != 0 && "Bitmap must be non-zero");

      // Calculate total count once using the intrinsic directly on the (potentially 32-bit) input
      // This relies on the user ensuring only the lower 16 bits are set if that's the intent.
      // Or we can calculate based on the masked version:
      int elements_to_copy = __builtin_popcount(bitmap);

      int dest = 0;
      do
      {
         --elements_to_copy;
         // __builtin_ctz(0) is undefined behavior.
         // The loop ensures current_bitmap is not 0 here.
         assert(bitmap != 0 && "ctz input cannot be zero within loop");

         // Copy the value from source using the found index
         destination[dest++] = source[__builtin_ctz(bitmap)];

         // Clear the least significant bit set from the bitmap for the next iteration
         bitmap &= (bitmap - 1);
      } while (elements_to_copy);
   }

}  // namespace psitri