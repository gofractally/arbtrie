#pragma once
#include <cassert>
#include <cstdint>

namespace psitri
{
   /**
    * Wide inner nodes keep the same "control-block cline + index within cline"
    * idea as compact inner nodes, but spend two bytes per child so the cline
    * slot can exceed 15. The low nibble remains the child index inside a
    * control-block cacheline; the remaining 12 bits are the cline slot.
    */
   struct wide_branch
   {
      uint16_t branch_data;
      void     set_line_index(uint16_t line, uint8_t index) noexcept
      {
         assert(line < 4096);
         assert(index < 16);
         branch_data = uint16_t((line << 4) | index);
      }
      uint16_t line() const noexcept { return branch_data >> 4; }
      uint8_t  index() const noexcept { return branch_data & 0x0f; }
   } __attribute__((packed));
   static_assert(sizeof(wide_branch) == 2);
}  // namespace psitri
