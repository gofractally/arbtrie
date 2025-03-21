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
      }
      static hint any()
      {
         hint h;
         memset(h.pages, 0xff, sizeof(h.pages));
         memset(h.cachelines, 0xff, sizeof(h.cachelines));
         return h;
      }
      uint64_t pages[2];
      uint64_t cachelines[8];

      void calculate(uint16_t* indices, uint16_t count);
   };

}  // namespace sal
