#include <sal/hint.hpp>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace sal
{
   /// @brief  works with any multiple of 8, aigned on 16 byte boundary
   /// TODO: doesn't support ignoring zeros, but could be added with a different variatin
   /// @param indices
   /// @param hint_count
   void calculate_hint_neon(hint& h, uint16_t* indices, uint16_t hint_count)
   {
      //uint16x8x4_t chunks[4] = {vld1q_u16_x4(indices), vld1q_u16_x4(indices + 32),
      //                          vld1q_u16_x4(indices + 64), vld1q_u16_x4(indices + 96)};

      auto      mask63 = vdupq_n_u16(63);
      uint16_t* end    = indices + hint_count;
      while (indices < end)
      {
         uint16x8_t hints              = vld1q_u16(indices);
         uint16x8_t pages              = vshrq_n_u16(hints, 9);
         uint16x8_t page_idx           = vshrq_n_u16(hints, 15);
         uint16x8_t bit_positions      = vandq_u16(pages, mask63);
         uint16x8_t cacheline_indicies = vandq_u16(vshrq_n_u16(hints, 3), mask63);

         h.pages[vgetq_lane_u16(page_idx, 0)] |= (1ULL << vgetq_lane_u16(bit_positions, 0));
         h.pages[vgetq_lane_u16(page_idx, 1)] |= (1ULL << vgetq_lane_u16(bit_positions, 1));
         h.pages[vgetq_lane_u16(page_idx, 2)] |= (1ULL << vgetq_lane_u16(bit_positions, 2));
         h.pages[vgetq_lane_u16(page_idx, 3)] |= (1ULL << vgetq_lane_u16(bit_positions, 3));
         h.pages[vgetq_lane_u16(page_idx, 4)] |= (1ULL << vgetq_lane_u16(bit_positions, 4));
         h.pages[vgetq_lane_u16(page_idx, 5)] |= (1ULL << vgetq_lane_u16(bit_positions, 5));
         h.pages[vgetq_lane_u16(page_idx, 6)] |= (1ULL << vgetq_lane_u16(bit_positions, 6));
         h.pages[vgetq_lane_u16(page_idx, 7)] |= (1ULL << vgetq_lane_u16(bit_positions, 7));

         h.cachelines[vgetq_lane_u16(cacheline_indicies, 0)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 0));
         h.cachelines[vgetq_lane_u16(cacheline_indicies, 1)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 1));
         h.cachelines[vgetq_lane_u16(cacheline_indicies, 2)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 2));
         h.cachelines[vgetq_lane_u16(cacheline_indicies, 3)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 3));
         h.cachelines[vgetq_lane_u16(cacheline_indicies, 4)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 4));
         h.cachelines[vgetq_lane_u16(cacheline_indicies, 5)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 5));
         h.cachelines[vgetq_lane_u16(cacheline_indicies, 6)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 6));
         h.cachelines[vgetq_lane_u16(cacheline_indicies, 7)] |=
             (1ULL << vgetq_lane_u16(bit_positions, 7));
         indices += 8;
      }
   }
   void calculate_hint_scalar(hint& h, uint16_t* indices, uint16_t count)
   {
      auto end = indices + count;
      while (indices < end)
      {
         uint16_t value = *indices;
         ++indices;
         if (value == 0)
            continue;
         uint16_t page         = value >> 9;
         uint16_t page_idx     = page >> 6;
         uint16_t bit_position = page & 63;
         h.pages[page_idx] |= (1ULL << bit_position);
         uint16_t cacheline_index = (value >> 3) & 63;
         h.cachelines[page_idx] |= (1ULL << cacheline_index);
      }
   }

   void hint::calculate(uint16_t* indices, uint16_t count)
   {
#if defined(__ARM_NEON)
      if (count)
      {
         // Calculate unaligned prefix elements (to get to 16-byte alignment)
         uint16_t* original_indices = indices;
         uint16_t  prefix_count     = 0;

         if (reinterpret_cast<uintptr_t>(indices) & 0xF)
         {
            // Calculate how many uint16_t elements needed to reach alignment
            prefix_count = ((16 - (reinterpret_cast<uintptr_t>(indices) & 0xF)) / sizeof(uint16_t));
            // Make sure we don't exceed total count
            prefix_count = (prefix_count > count) ? count : prefix_count;
            indices += prefix_count;
            count -= prefix_count;

            // Process unaligned prefix with scalar function
            calculate_hint_scalar(*this, original_indices, prefix_count);
         }

         // Handle aligned middle with NEON
         uint16_t neon_count = count & ~7;
         if (neon_count)
         {
            calculate_hint_neon(*this, indices, neon_count);
            indices += neon_count;
            count &= 7;
         }

         // Handle remaining elements with scalar
         if (count)
            calculate_hint_scalar(*this, indices, count);
      }
#else
      calculate_hint_scalar(*this, indices, count);
#endif
   }

}  // namespace sal
