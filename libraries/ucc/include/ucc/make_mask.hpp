#pragma once
#include <cstdint>
namespace ucc
{
   template <uint32_t offset, uint32_t width>
   constexpr uint64_t make_mask()
   {
      static_assert(offset + width <= 64);
      return (uint64_t(-1) >> (64 - width)) << offset;
   }

}  // namespace ucc