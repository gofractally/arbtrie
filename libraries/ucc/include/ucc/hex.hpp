#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#if defined(__aarch64__) || defined(_M_ARM64)
#include <arm_neon.h>
#define UCC_HEX_NEON 1
#elif defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#if defined(__SSSE3__)
#define UCC_HEX_SSSE3 1
#endif
#endif

namespace ucc
{

namespace detail
{
   inline constexpr int8_t unhex_lut[256] = {
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,-1,-1,-1,-1,-1,-1,
       -1,10,11,12,13,14,15,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,10,11,12,13,14,15,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
       -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
   };

   // Scalar fallback: 256-entry LUT of uint16_t, one 16-bit store per byte.
   inline constexpr uint16_t hex_lut[256] = {
#define UCC_H2(b) static_cast<uint16_t>( \
   ("0123456789abcdef"[(b) >> 4]) | \
   ("0123456789abcdef"[(b) & 0xf] << 8))
#define UCC_H16(r) UCC_H2((r)*16+0),UCC_H2((r)*16+1),UCC_H2((r)*16+2),UCC_H2((r)*16+3), \
                   UCC_H2((r)*16+4),UCC_H2((r)*16+5),UCC_H2((r)*16+6),UCC_H2((r)*16+7), \
                   UCC_H2((r)*16+8),UCC_H2((r)*16+9),UCC_H2((r)*16+10),UCC_H2((r)*16+11), \
                   UCC_H2((r)*16+12),UCC_H2((r)*16+13),UCC_H2((r)*16+14),UCC_H2((r)*16+15)
      UCC_H16(0), UCC_H16(1), UCC_H16(2), UCC_H16(3),
      UCC_H16(4), UCC_H16(5), UCC_H16(6), UCC_H16(7),
      UCC_H16(8), UCC_H16(9), UCC_H16(10), UCC_H16(11),
      UCC_H16(12), UCC_H16(13), UCC_H16(14), UCC_H16(15)
#undef UCC_H16
#undef UCC_H2
   };
}  // namespace detail

inline void to_hex(const void* data, size_t len, char* dest)
{
   auto src = static_cast<const uint8_t*>(data);
   size_t i = 0;

#if UCC_HEX_NEON
   // NEON: 16 bytes → 32 hex chars per iteration.
   // Split each byte into hi/lo nibbles, then use vtbl to map nibble → ASCII.
   const uint8x16_t ascii = vld1q_u8(reinterpret_cast<const uint8_t*>("0123456789abcdef"));
   const uint8x16_t mask  = vdupq_n_u8(0x0f);

   for (; i + 16 <= len; i += 16)
   {
      uint8x16_t v  = vld1q_u8(src + i);
      uint8x16_t hi = vshrq_n_u8(v, 4);
      uint8x16_t lo = vandq_u8(v, mask);
      uint8x16_t hex_hi = vqtbl1q_u8(ascii, hi);
      uint8x16_t hex_lo = vqtbl1q_u8(ascii, lo);
      // Interleave: [h0,l0,h1,l1,...,h15,l15]
      uint8x16x2_t zipped = vzipq_u8(hex_hi, hex_lo);
      vst1q_u8(reinterpret_cast<uint8_t*>(dest + i * 2),      zipped.val[0]);
      vst1q_u8(reinterpret_cast<uint8_t*>(dest + i * 2 + 16), zipped.val[1]);
   }
#elif UCC_HEX_SSSE3
   // SSSE3: 16 bytes → 32 hex chars via pshufb.
   const __m128i ascii = _mm_setr_epi8('0','1','2','3','4','5','6','7',
                                        '8','9','a','b','c','d','e','f');
   const __m128i mask  = _mm_set1_epi8(0x0f);

   for (; i + 16 <= len; i += 16)
   {
      __m128i v  = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i));
      __m128i hi = _mm_and_si128(_mm_srli_epi16(v, 4), mask);
      __m128i lo = _mm_and_si128(v, mask);
      __m128i hex_hi = _mm_shuffle_epi8(ascii, hi);
      __m128i hex_lo = _mm_shuffle_epi8(ascii, lo);
      __m128i out_lo = _mm_unpacklo_epi8(hex_hi, hex_lo);
      __m128i out_hi = _mm_unpackhi_epi8(hex_hi, hex_lo);
      _mm_storeu_si128(reinterpret_cast<__m128i*>(dest + i * 2),      out_lo);
      _mm_storeu_si128(reinterpret_cast<__m128i*>(dest + i * 2 + 16), out_hi);
   }
#endif

   // Scalar tail
   for (; i < len; ++i)
   {
      uint16_t pair = detail::hex_lut[src[i]];
      std::memcpy(dest + i * 2, &pair, 2);
   }
}

inline std::string to_hex(const void* data, size_t len)
{
   std::string s;
   s.resize(len * 2);
   to_hex(data, len, s.data());
   return s;
}

inline std::string to_hex(std::string_view sv)
{
   return to_hex(sv.data(), sv.size());
}

inline bool from_hex(std::string_view hex, void* out, size_t out_len)
{
   if (hex.size() != out_len * 2)
      return false;
   auto dst = static_cast<uint8_t*>(out);
   for (size_t i = 0; i < out_len; ++i)
   {
      auto hi = detail::unhex_lut[static_cast<uint8_t>(hex[i * 2])];
      auto lo = detail::unhex_lut[static_cast<uint8_t>(hex[i * 2 + 1])];
      if (hi < 0 || lo < 0)
         return false;
      dst[i] = static_cast<uint8_t>((hi << 4) | lo);
   }
   return true;
}

inline std::string from_hex(std::string_view hex)
{
   std::string out;
   out.resize(hex.size() / 2);
   if (!from_hex(hex, out.data(), out.size()))
      out.clear();
   return out;
}

}  // namespace ucc
