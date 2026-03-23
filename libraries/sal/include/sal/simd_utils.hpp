#pragma once
#include <bit>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif
#if defined(__SSE2__) && defined(__SSSE3__)
#include <immintrin.h>
#include <tmmintrin.h>
#if defined(_MSC_VER)
#include <intrin.h>
#else
#include <x86intrin.h>
#endif
#endif  // __SSE2__ && __SSSE3__

namespace sal
{

   /**
    * @param data is a pointer to an array of 8 uint64_t values
    * @return a bitmask of 1 bit for each byte that is 0xff and 0 for bytes that are 0x00
    */
   inline uint64_t move_mask64_scalar(uint64_t* data)
   {
      const uint64_t magic = 0x000103070f1f3f80ull;
      return (data[0] * magic >> 56) + ((data[1] * magic >> 48) & 0xff00ull) +
             ((data[2] * magic >> 40) & 0xff0000ull) + ((data[3] * magic >> 32) & 0xff000000ull) +
             ((data[4] * magic >> 24) & 0xff00000000ull) +
             ((data[5] * magic >> 16) & 0xff0000000000ull) +
             ((data[6] * magic >> 8) & 0xff000000000000ull) +
             ((data[7] * magic) & 0xff00000000000000ull);
   }

#if defined(__ARM_NEON)
   inline uint64x2_t vmulq_u64(uint64x2_t vecA, uint64_t c)
   {
      uint64_t a[2];
      vst1q_u64(a, vecA);  // Store vecA into array
      a[0] *= c;
      a[1] *= c;
      return vld1q_u64(a);  // Load result back into vector
   }
   inline uint64_t move_mask_neon(uint8x16_t cmp0,
                                  uint8x16_t cmp1,
                                  uint8x16_t cmp2,
                                  uint8x16_t cmp3)
   {
      const constexpr uint64_t magic = 0x000103070f1f3f80ull;
      //  uint64x2_t mult_vec = vdupq_n_u64(mult);

      uint64x2_t cmp0_64 = vmulq_u64(vreinterpretq_u64_u8(cmp0), magic);
      uint64x2_t cmp1_64 = vmulq_u64(vreinterpretq_u64_u8(cmp1), magic);
      uint64x2_t cmp2_64 = vmulq_u64(vreinterpretq_u64_u8(cmp2), magic);
      uint64x2_t cmp3_64 = vmulq_u64(vreinterpretq_u64_u8(cmp3), magic);

      // Precomputed shift and mask vectors (compile-time constants)
      static constexpr const int64x2_t shifts[4] = {
          {-56, -48},  // For cmp0
          {-40, -32},  // For cmp1
          {-24, -16},  // For cmp2
          {-8, 0}      // For cmp3
      };

      cmp0_64 = vshlq_u64(cmp0_64, shifts[0]);
      cmp1_64 = vshlq_u64(cmp1_64, shifts[1]);
      cmp2_64 = vshlq_u64(cmp2_64, shifts[2]);
      cmp3_64 = vshlq_u64(cmp3_64, shifts[3]);

      static constexpr const uint64x2_t masks[4] = {
          {0xff00000000000000ull >> 56, 0xff00000000000000ull >> 48},  // cmp0
          {0xff00000000000000ull >> 40, 0xff00000000000000ull >> 32},  // cmp1
          {0xff00000000000000ull >> 24, 0xff00000000000000ull >> 16},  // cmp2
          {0xff00000000000000ull >> 8, 0xff00000000000000ull >> 0}     // cmp3
      };

      // AND with 0xff00000000000000 shifted right by 7,6,5,4,3,2,1 bytes
      cmp0_64 = vandq_u64(cmp0_64, masks[0]);
      cmp1_64 = vandq_u64(cmp1_64, masks[1]);
      cmp2_64 = vandq_u64(cmp2_64, masks[2]);
      cmp3_64 = vandq_u64(cmp3_64, masks[3]);

      // Sum all the uint64x2_t values in parallel
      uint64x2_t sum01_64 = vaddq_u64(cmp0_64, cmp1_64);
      uint64x2_t sum23_64 = vaddq_u64(cmp2_64, cmp3_64);
      uint64x2_t sum_64   = vaddq_u64(sum01_64, sum23_64);
      return vgetq_lane_u64(sum_64, 0) + vgetq_lane_u64(sum_64, 1);
   }
#endif

   /**
    * @param data must be a 128 byte aligned array of 8 uint64_t values
    * @return a bitmask of 1 bit for each byte that is 0xff and 0 for bytes that are 0x00
    */
   inline uint64_t move_mask64(uint64_t* data)
   {
#if defined(__ARM_NEON)
      // Cast the input pointer to uint8_t*
      const uint8_t* data_u8 = reinterpret_cast<const uint8_t*>(data);

      // Load 64 bytes directly into four 128-bit (16 byte) vectors
      uint8x16_t vec0_u8 = vld1q_u8(data_u8 + 0);   // Load bytes 0-15
      uint8x16_t vec1_u8 = vld1q_u8(data_u8 + 16);  // Load bytes 16-31
      uint8x16_t vec2_u8 = vld1q_u8(data_u8 + 32);  // Load bytes 32-47
      uint8x16_t vec3_u8 = vld1q_u8(data_u8 + 48);  // Load bytes 48-63

      // Call move_mask_neon with the loaded byte vectors
      return move_mask_neon(vec0_u8, vec1_u8, vec2_u8, vec3_u8);
#else
      return move_mask64_scalar(data);
#endif
   }

#if defined(__ARM_NEON)
   inline int max_pop_cnt8_index64_neon(uint8_t* aligned_array)
   {
      uint8x16x4_t chunks = vld1q_u8_x4(aligned_array);
      uint8x16_t   cnt0   = vcntq_u8(chunks.val[0]);
      uint8x16_t   cnt1   = vcntq_u8(chunks.val[1]);
      uint8x16_t   cnt2   = vcntq_u8(chunks.val[2]);
      uint8x16_t   cnt3   = vcntq_u8(chunks.val[3]);

      // Find max between pairs of vectors
      uint8x16_t max01 = vmaxq_u8(cnt0, cnt1);
      uint8x16_t max23 = vmaxq_u8(cnt2, cnt3);

      // Find max between the pairs
      uint8x16_t max_all = vmaxq_u8(max01, max23);
      // Find max across all bytes in max_all
      uint8_t max_value = vmaxvq_u8(max_all);

      // Compare each byte in cnt0 to max_value
      const uint8x16_t max_splat = vdupq_n_u8(max_value);
      uint8x16_t       cmp0      = vceqq_u8(cnt0, max_splat);
      uint8x16_t       cmp1      = vceqq_u8(cnt1, max_splat);
      uint8x16_t       cmp2      = vceqq_u8(cnt2, max_splat);
      uint8x16_t       cmp3      = vceqq_u8(cnt3, max_splat);

      auto mmn = move_mask_neon(cmp0, cmp1, cmp2, cmp3);
      return std::countr_zero(mmn);
   }
#endif
#if defined(__SSE2__) && defined(__SSSE3__)
   // SSE Popcount implementation using PSHUFB (SSSE3) lookup table method
   static inline __m128i sse_popcount_byte(__m128i x)
   {
      const __m128i lookup        = _mm_setr_epi8(0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4);
      const __m128i low_mask      = _mm_set1_epi8(0x0F);
      __m128i       low_nibbles   = _mm_and_si128(x, low_mask);
      __m128i       high_nibbles  = _mm_and_si128(_mm_srli_epi16(x, 4), low_mask);
      __m128i       popcount_low  = _mm_shuffle_epi8(lookup, low_nibbles);
      __m128i       popcount_high = _mm_shuffle_epi8(lookup, high_nibbles);
      return _mm_add_epi8(popcount_low, popcount_high);
   }

   size_t max_pop_cnt8_index64_sse(const uint8_t* data)
   {
      // Assumes data is 16-byte aligned. Use aligned loads.
      __m128i v0 = _mm_load_si128(reinterpret_cast<const __m128i*>(data + 0));
      __m128i v1 = _mm_load_si128(reinterpret_cast<const __m128i*>(data + 16));
      __m128i v2 = _mm_load_si128(reinterpret_cast<const __m128i*>(data + 32));
      __m128i v3 = _mm_load_si128(reinterpret_cast<const __m128i*>(data + 48));

      // Calculate popcounts
      __m128i cnt0 = sse_popcount_byte(v0);
      __m128i cnt1 = sse_popcount_byte(v1);
      __m128i cnt2 = sse_popcount_byte(v2);
      __m128i cnt3 = sse_popcount_byte(v3);

      // Find the max popcount horizontally and vertically
      __m128i max01    = _mm_max_epu8(cnt0, cnt1);
      __m128i max23    = _mm_max_epu8(cnt2, cnt3);
      __m128i max0123  = _mm_max_epu8(max01, max23);
      __m128i pmax1    = _mm_max_epu8(max0123, _mm_srli_si128(max0123, 8));
      __m128i pmax2    = _mm_max_epu8(pmax1, _mm_srli_si128(pmax1, 4));
      __m128i pmax3    = _mm_max_epu8(pmax2, _mm_srli_si128(pmax2, 2));
      __m128i pmax4    = _mm_max_epu8(pmax3, _mm_srli_si128(pmax3, 1));
      __m128i vmax_vec = _mm_shuffle_epi32(pmax4, 0);  // Broadcast max

      // Compare original popcounts with the global maximum
      __m128i cmp0 = _mm_cmpeq_epi8(cnt0, vmax_vec);
      __m128i cmp1 = _mm_cmpeq_epi8(cnt1, vmax_vec);
      __m128i cmp2 = _mm_cmpeq_epi8(cnt2, vmax_vec);
      __m128i cmp3 = _mm_cmpeq_epi8(cnt3, vmax_vec);

      // Create bitmasks from comparison results
      uint32_t mask0 = static_cast<uint32_t>(_mm_movemask_epi8(cmp0));
      uint32_t mask1 = static_cast<uint32_t>(_mm_movemask_epi8(cmp1));
      uint32_t mask2 = static_cast<uint32_t>(_mm_movemask_epi8(cmp2));
      uint32_t mask3 = static_cast<uint32_t>(_mm_movemask_epi8(cmp3));

      // Combine masks
      uint64_t combined_mask = static_cast<uint64_t>(mask0) | (static_cast<uint64_t>(mask1) << 16) |
                               (static_cast<uint64_t>(mask2) << 32) |
                               (static_cast<uint64_t>(mask3) << 48);

      // Find index of first set bit
      if (combined_mask == 0)
      {
         return 0;  // All bytes were 0
      }
      size_t index = count_trailing_zeros(combined_mask);
      return (index < 64) ? index : 0;  // Should be < 64 if mask != 0
   }

#endif  // __SSE2__ && __SSSE3__

   inline int max_pop_cnt8_index64_scalar(const uint8_t* data)
   {
      uint8_t max_popcount = 0;
      int     max_index    = 0;
      if (data == nullptr)
         return 0;
      uint8_t current_popcount = std::popcount(data[0]);
      max_popcount             = current_popcount;
      for (int i = 1; i < 64; ++i)
      {
         current_popcount = std::popcount(data[i]);
         if (current_popcount > max_popcount)
         {
            max_popcount = current_popcount;
            max_index    = i;
         }
      }
      return max_index;
   }

   /**   
    * @param aligned_array is 64 bytes aligned at 128 byte boundary
    * @return an index of the first byte with the most 1s
    */
   inline int max_pop_cnt8_index64(uint8_t* aligned_array)
   {
#if defined(__ARM_NEON)
      return max_pop_cnt8_index64_neon(aligned_array);
#elif defined(__SSE2__) && defined(__SSSE3__)
      return max_pop_cnt8_index64_sse(aligned_array);
#else
      return max_pop_cnt8_index64_scalar(aligned_array);
#endif
   }

   /*
      This function returns a bit per byte but the order is interleaved 
      and not the same as the input order. More testing is needed to see
      if the final byte index can be recovered from the bitmask.

       {bits for arr[0,4,8,12], arr[1,5,9,13], arr[2,6,10,14], arr[3,7,11,15], ...}

   /// @param arr an array of bytes 0 or 0xff
   /// @return a bit per byte that is 0 or 1 if the byte is 0 or 0xff
   inline uint64_t match_mask64_neon(const uint8_t* arr)
   {
      // Load 4x16 bytes de-interleaved
      uint8x16x4_t src = vld4q_u8(ptr);
      // Create a vector with the match_byte duplicated
      uint8x16_t dup = vdupq_n_u8(match_byte);
      // Compare each de-interleaved vector with the duplicated byte
      uint8x16_t cmp0 = vceqq_u8(src.val[0], dup);  // Results for bytes 0, 4, 8, ...
      uint8x16_t cmp1 = vceqq_u8(src.val[1], dup);  // Results for bytes 1, 5, 9, ...
      uint8x16_t cmp2 = vceqq_u8(src.val[2], dup);  // Results for bytes 2, 6, 10, ...
      uint8x16_t cmp3 = vceqq_u8(src.val[3], dup);  // Results for bytes 3, 7, 11, ...

      // Combine results using Shift Right and Insert (Collects MSBs)
      // After this sequence, the relevant bits are packed into the low nibble
      // of each byte in t3.
      uint8x16_t t0 = vsriq_n_u8(cmp1, cmp0, 1);  // Combine 0+1, 4+5, ...
      uint8x16_t t1 = vsriq_n_u8(cmp3, cmp2, 1);  // Combine 2+3, 6+7, ...
      uint8x16_t t2 = vsriq_n_u8(t1, t0, 2);      // Combine (0+1)+(2+3), ...
      uint8x16_t t3 = vsriq_n_u8(t2, t2, 4);  // Pack bits 0..7 into lower nibble of bytes 0/1, etc.
                                              // (and duplicates nibble into upper half)

      // Narrow the results: shift right by 4 and take the lower 8 bits of each 16-bit lane
      // This extracts the packed bits correctly.
      uint8x8_t t4 = vshrn_n_u16(vreinterpretq_u16_u8(t3), 4);

      // Reinterpret the 8 bytes (64 bits) as a uint64_t and return
      return vget_lane_u64(vreinterpret_u64_u8(t4), 0);
   }

   */
}  // namespace sal