#pragma once
#include <cassert>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace ucc
{
   namespace detail
   {

#if defined(__ARM_NEON)
      // Optimized memcpy for 64-byte chunks, this benchmarked 2x faster than
      // memcpy for 64 byte chunks less than 4096 bytes long... when they are
      // known to be aligned multiples of the cacheline size on Apple M4
      inline void memcpy_aligned_64byte_neon(void* __restrict dst,
                                             const void* __restrict src,
                                             size_t n)
      {
         assert(n % 64 == 0);
         uint8_t*       d = (uint8_t*)dst;
         const uint8_t* s = (const uint8_t*)src;
         int count = n / 128;
         n = n % 128;

         while( count-- )
         {
            // First 64 bytes
            uint8x16_t v0 = vld1q_u8(s);
            uint8x16_t v1 = vld1q_u8(s + 16);
            uint8x16_t v2 = vld1q_u8(s + 32);
            uint8x16_t v3 = vld1q_u8(s + 48);
            vst1q_u8(d, v0);
            vst1q_u8(d + 16, v1);
            vst1q_u8(d + 32, v2);
            vst1q_u8(d + 48, v3);
            // Second 64 bytes
            uint8x16_t v4 = vld1q_u8(s + 64);
            uint8x16_t v5 = vld1q_u8(s + 80);
            uint8x16_t v6 = vld1q_u8(s + 96);
            uint8x16_t v7 = vld1q_u8(s + 112);
            vst1q_u8(d + 64, v4);
            vst1q_u8(d + 80, v5);
            vst1q_u8(d + 96, v6);
            vst1q_u8(d + 112, v7);
            s += 128;
            d += 128;
         }
         if (n)
         {
            uint8x16_t v0 = vld1q_u8(s);
            uint8x16_t v1 = vld1q_u8(s + 16);
            uint8x16_t v2 = vld1q_u8(s + 32);
            uint8x16_t v3 = vld1q_u8(s + 48);
            vst1q_u8(d, v0);
            vst1q_u8(d + 16, v1);
            vst1q_u8(d + 32, v2);
            vst1q_u8(d + 48, v3);
         }
      }
#endif

   }  // namespace detail

   inline void memcpy_aligned_64byte(void* __restrict dst, const void* __restrict src, size_t n)
   {
#if defined(__ARM_NEON)
      detail::memcpy_aligned_64byte_neon(dst, src, n);
#else
      memcpy(dst, src, n);
#endif
   }
}  // namespace ucc
