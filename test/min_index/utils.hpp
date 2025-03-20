#pragma once
#include <cstdint>

#if defined(__ARM_NEON)
#include <arm_neon.h>

// Utility function to count trailing zeros in a bitmask
inline int count_trailing_zeros(uint64_t mask)
{
   if (mask == 0)
      return 64;

#if defined(__GNUC__) || defined(__clang__)
   return __builtin_ctzll(mask);
#else
   int count = 0;
   while ((mask & 1) == 0)
   {
      count++;
      mask >>= 1;
   }
   return count;
#endif
}

// Utility function to convert a NEON vector comparison result to a bitmask
inline uint64_t neon_to_mask(uint16x8_t cmp)
{
   // Extract each lane as uint16_t first, then use bit operations
   uint16_t lane0 = vgetq_lane_u16(cmp, 0);
   uint16_t lane1 = vgetq_lane_u16(cmp, 1);
   uint16_t lane2 = vgetq_lane_u16(cmp, 2);
   uint16_t lane3 = vgetq_lane_u16(cmp, 3);
   uint16_t lane4 = vgetq_lane_u16(cmp, 4);
   uint16_t lane5 = vgetq_lane_u16(cmp, 5);
   uint16_t lane6 = vgetq_lane_u16(cmp, 6);
   uint16_t lane7 = vgetq_lane_u16(cmp, 7);

   // Set corresponding bits based on non-zero lanes
   // Each lane will be either 0 (false) or 0xFFFF (true) from vceqq comparison
   uint64_t mask = 0;
   mask |= (lane0 ? 1ULL : 0);
   mask |= (lane1 ? 1ULL << 1 : 0);
   mask |= (lane2 ? 1ULL << 2 : 0);
   mask |= (lane3 ? 1ULL << 3 : 0);
   mask |= (lane4 ? 1ULL << 4 : 0);
   mask |= (lane5 ? 1ULL << 5 : 0);
   mask |= (lane6 ? 1ULL << 6 : 0);
   mask |= (lane7 ? 1ULL << 7 : 0);

   return mask;
}
#endif