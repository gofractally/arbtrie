#pragma once
#include <cstdint>
#include <cstring>

namespace sal
{
   struct hint
   {
      hint()
      {
         memset(pages, 0, sizeof(pages));
         memset(cachelines, 0, sizeof(cachelines));
         memset(page_order, 0, sizeof(page_order));
      }
      static hint any()
      {
         hint h;
         memset(h.pages, 0xff, sizeof(h.pages));
         memset(h.cachelines, 0xff, sizeof(h.cachelines));
         return h;
      }
      uint64_t get_cachelines_for_page(uint8_t page) const;

      uint64_t pages[2];
      uint8_t  page_order[8];
      uint64_t cachelines[8];

      void calculate(uint16_t* indices, uint16_t count);
   };

}  // namespace sal
