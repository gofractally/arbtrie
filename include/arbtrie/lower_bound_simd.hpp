#include <cstddef>
#include <cstdint>

#if defined(__SSE2__)
#include <emmintrin.h>  // SSE2
#elif defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#include <arm_neon.h>

const char* lower_bound_simd4(const char* arr, size_t n, char x)
{
   const char* ptr = arr;
   const char* end = arr + n;
   // Unrolled linear search for small arrays or initial chunk
   while (ptr + 3 < end)
   {
      if (static_cast<unsigned char>(*ptr) >= static_cast<unsigned char>(x))
         return ptr;
      if (static_cast<unsigned char>(ptr[1]) >= static_cast<unsigned char>(x))
         return ptr + 1;
      if (static_cast<unsigned char>(ptr[2]) >= static_cast<unsigned char>(x))
         return ptr + 2;
      if (static_cast<unsigned char>(ptr[3]) >= static_cast<unsigned char>(x))
         return ptr + 3;
      ptr += 4;
   }
   while (ptr < end)
   {
      if (static_cast<unsigned char>(*ptr) >= static_cast<unsigned char>(x))
         return ptr;
      ++ptr;
   }
   return ptr;
}

const char* lower_bound_simd(const char* arr, size_t n, char x)
{
   if (n == 0)
      return arr;

   // Scalar path for small arrays (< 16 bytes)
   if (n < 16)
   {
      const char* ptr = arr;
      const char* end = arr + n;
      while (ptr < end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
      {
         ++ptr;
      }
      return ptr;
   }

   // SIMD path for larger arrays
   constexpr size_t chunk_size = 16;  // NEON register size
   const size_t     num_chunks = n / chunk_size;
   const size_t     remainder  = n % chunk_size;

   uint8x16_t x_vec    = vdupq_n_u8(static_cast<unsigned char>(x));
   uint16_t   masks[8] = {0};  // Up to 128 bytes / 16 = 8 chunks

   // Process full 16-byte chunks with SIMD, no branching
   for (size_t k = 0; k < num_chunks; ++k)
   {
      const unsigned char* chunk_ptr = reinterpret_cast<const unsigned char*>(arr + k * chunk_size);
      uint8x16_t           chunk     = vld1q_u8(chunk_ptr);
      uint8x16_t           cmp_ge    = vcgeq_u8(chunk, x_vec);  // Elements >= x

      // Convert comparison to a 16-bit mask
      uint16_t  mask = 0;
      uint8x8_t low  = vget_low_u8(cmp_ge);
      uint8x8_t high = vget_high_u8(cmp_ge);
      mask |= (vget_lane_u8(low, 0) ? 1 : 0) << 0;
      mask |= (vget_lane_u8(low, 1) ? 1 : 0) << 1;
      mask |= (vget_lane_u8(low, 2) ? 1 : 0) << 2;
      mask |= (vget_lane_u8(low, 3) ? 1 : 0) << 3;
      mask |= (vget_lane_u8(low, 4) ? 1 : 0) << 4;
      mask |= (vget_lane_u8(low, 5) ? 1 : 0) << 5;
      mask |= (vget_lane_u8(low, 6) ? 1 : 0) << 6;
      mask |= (vget_lane_u8(low, 7) ? 1 : 0) << 7;
      mask |= (vget_lane_u8(high, 0) ? 1 : 0) << 8;
      mask |= (vget_lane_u8(high, 1) ? 1 : 0) << 9;
      mask |= (vget_lane_u8(high, 2) ? 1 : 0) << 10;
      mask |= (vget_lane_u8(high, 3) ? 1 : 0) << 11;
      mask |= (vget_lane_u8(high, 4) ? 1 : 0) << 12;
      mask |= (vget_lane_u8(high, 5) ? 1 : 0) << 13;
      mask |= (vget_lane_u8(high, 6) ? 1 : 0) << 14;
      mask |= (vget_lane_u8(high, 7) ? 1 : 0) << 15;

      masks[k] = mask;
   }

   // Find the first chunk with a transition
   for (size_t k = 0; k < num_chunks; ++k)
   {
      uint16_t mask = masks[k];
      if (mask != 0)
      {
         int p = __builtin_ctz(mask);  // Position of first set bit
         return arr + k * chunk_size + p;
      }
   }

   // Handle remaining bytes with scalar code
   const char* ptr = arr + num_chunks * chunk_size;
   const char* end = arr + n;
   while (ptr < end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
   {
      ++ptr;
   }
   return ptr;
}

const char* lower_bound_simd2(const char* arr, size_t n, char x)
{
   if (n == 0)
      return arr;

   // Switch to scalar for small arrays
   if (n < 16)
   {
      const char* ptr = arr;
      const char* end = arr + n;
      while (ptr < end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
      {
         ++ptr;
      }
      return ptr;
   }

   // NEON SIMD processing
   constexpr size_t chunk_size = 16;
   size_t           k          = 0;
   const uint8x16_t x_vec      = vdupq_n_u8(static_cast<unsigned char>(x));

   // Process full 16-byte chunks
   for (; k < n / chunk_size; ++k)
   {
      const unsigned char* chunk_ptr = reinterpret_cast<const unsigned char*>(arr + k * chunk_size);
      uint8x16_t           chunk     = vld1q_u8(chunk_ptr);
      uint8x16_t           cmp_ge    = vcgeq_u8(chunk, x_vec);  // Compare >= x
      if (vmaxvq_u8(cmp_ge) != 0)
      {  // At least one element >= x
         const char* chunk_start = arr + k * chunk_size;
         const char* chunk_end   = chunk_start + chunk_size;
         const char* ptr         = chunk_start;
         while (ptr < chunk_end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
         {
            ++ptr;
         }
         return ptr;
      }
   }

   // Handle remaining bytes with scalar code
   const char* ptr = arr + k * chunk_size;
   const char* end = arr + n;
   while (ptr < end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
   {
      ++ptr;
   }
   return ptr;
}
#if 0

/**
 * Finds the first position in a sorted array where the element is >= x.
 * @param arr Pointer to the sorted array of chars.
 * @param n Length of the array.
 * @param x Value to search for.
 * @return Pointer to the first element >= x, or arr + n if no such element exists.
 */
inline const char* lower_bound_simd(const char* arr, size_t n, char x)
{
   if (n == 0)
      return arr;

#if defined(__SSE2__)
   // x86 SSE2 Implementation
   constexpr size_t chunk_size = 16;
   size_t           k          = 0;
   const __m128i    flip       = _mm_set1_epi8(0x80);  // For unsigned comparison trick
   const __m128i    x_vec      = _mm_set1_epi8(x);
   const __m128i    x_flip     = _mm_xor_si128(x_vec, flip);

   // Process 16-byte chunks with SSE2
   for (; k < n / chunk_size; ++k)
   {
      __m128i chunk      = _mm_loadu_si128(reinterpret_cast<const __m128i*>(arr + k * chunk_size));
      __m128i chunk_flip = _mm_xor_si128(chunk, flip);
      __m128i cmp_lt     = _mm_cmplt_epi8(chunk_flip, x_flip);  // a < x (unsigned)
      int     mask       = _mm_movemask_epi8(cmp_lt);           // 1 where a < x, 0 where a >= x

      if ((~mask & 0xFFFF) != 0)
      {                                          // At least one element is >= x
         int p = __builtin_ctz(~mask & 0xFFFF);  // First position where a >= x
         return arr + k * chunk_size + p;
      }
   }

#elif defined(__ARM_NEON)
   // ARM NEON Implementation
   constexpr size_t chunk_size = 16;
   size_t           k          = 0;
   const uint8x16_t x_vec      = vdupq_n_u8(static_cast<unsigned char>(x));

   // Process 16-byte chunks with NEON
   for (; k < n / chunk_size; ++k)
   {
      uint8x16_t chunk  = vld1q_u8(reinterpret_cast<const unsigned char*>(arr + k * chunk_size));
      uint8x16_t cmp_ge = vcgeq_u8(chunk, x_vec);  // a >= x
      if (vmaxvq_u8(cmp_ge) != 0)
      {  // At least one element is >= x
         const char* chunk_start = arr + k * chunk_size;
         const char* chunk_end   = chunk_start + chunk_size;
         const char* ptr         = chunk_start;
         // Scalar search within the chunk using pointer increment
         while (ptr < chunk_end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
         {
            ++ptr;
         }
         return ptr;
      }
   }

#else
   // Scalar Implementation (Fallback for other architectures)
   const char* ptr = arr;
   const char* end = arr + n;
   // Use pointer increment instead of array indexing
   while (ptr < end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
   {
      ++ptr;
   }
   return ptr;
#endif

   // Scalar search for remaining bytes (after vectorized chunks if any)
   const char* ptr = arr + k * chunk_size;
   const char* end = arr + n;
   // Optimized with pointer incrementation
   while (ptr < end && static_cast<unsigned char>(*ptr) < static_cast<unsigned char>(x))
   {
      ++ptr;
   }
   return ptr;
}
#endif