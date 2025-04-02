#include <sal/hint.hpp>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace sal
{
   inline int find_byte(const uint8_t arr[8], uint8_t value)
   {
      uint64_t data   = *(const uint64_t*)arr;          // Load 8 bytes
      uint64_t target = value * 0x0101010101010101ULL;  // Broadcast value to all bytes
      uint64_t mask   = ((data ^ target) - 0x0101010101010101ULL) & ~(data ^ target);
      mask &= 0x8080808080808080ULL;  // Extract high bits
      if (mask == 0)
         return -1;
      return __builtin_ctzll(mask) >> 3;
   }

   /**
    * Same as find_byte but assumes the value will be found.
    * Removes the branch that handles the not-found case.
    */
   inline int find_byte_nocheck(const uint8_t arr[8], uint8_t value)
   {
      uint64_t       data   = *(const uint64_t*)arr;          // Load 8 bytes
      uint64_t       target = value * 0x0101010101010101ULL;  // Broadcast value to all bytes
      const uint64_t data_xor_target = data ^ target;
      uint64_t       mask            = (data_xor_target - 0x0101010101010101ULL) & ~data_xor_target;
      mask &= 0x8080808080808080ULL;      // Extract high bits
      return __builtin_ctzll(mask) >> 3;  // Assume value is found
   }

   /**
    *  Identifies up to first 8 unique pages and the cachelines on the pages that
    *  have at least one hint present. Any cachelines on pages that are not part
    *  of the first 8 unique pages are not included in the hint.
    */
   void calculate_hint_simple(hint& h, uint16_t* indicies, uint16_t count)
   {
      int unique_pages = 0;
      for (uint16_t i = 0; i < count; ++i)
      {
         const uint16_t value = indicies[i];
         if (value == 0)
            continue;  /// skip this value, it has no hint

         const uint16_t page = value >> 9;  /// 512 ptrs' page
         // there are max 128 pages, so we need to know if we are in first or second half
         const uint8_t page_idx = page >> 6;  /// divide by 64, 0 or 1
         // the index bit that represets the page is page % 64
         const uint8_t  page_bit_index = (page & 63);
         const uint64_t page_bit       = 1ULL << page_bit_index;

         // this is all fine... but.. the hint would need to have 128  uint64_t (1kb)
         // to store all possible cacheline hints... or we need to prune the cachelines
         // to a smaller set, say first 8 unique pages

         bool     unique_page = (h.pages[page_idx] & page_bit) == 0;
         uint16_t upi;
         if (unique_page)
         {
            if (unique_pages >= 8)
               continue;  /// skip this page, too many already
            h.pages[page_idx] |= page_bit;
            upi                          = unique_pages;
            h.page_order[unique_pages++] = page_idx;
         }
         else
         {
            /// we have already seen this page, so we need to find the cacheline index
            upi = find_byte_nocheck(h.page_order, page_idx);
         }

         // there are 8 ids per cacheline, so the global cacheline idnex is indices[i]/8
         // there are 64 cachelines per page, so we need global cacheline index % 64
         const uint16_t cl_idx = (indicies[i] / 8) % 64;

         h.cachelines[upi] |= 1ULL << cl_idx;
      }
   }
   uint64_t hint::get_cachelines_for_page(uint8_t page) const
   {
      auto idx = find_byte(page_order, page);
      if (idx != -1)
         return 0;
      return cachelines[idx];
   }

   void hint::calculate(uint16_t* indices, uint16_t count)
   {
      if (count)
      {
         calculate_hint_simple(*this, indices, count);
      }
   }

}  // namespace sal
