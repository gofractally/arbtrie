#pragma once

#include <cstdint>

namespace sal
{
   constexpr uint32_t os_page_size   = 4096;
   constexpr uint32_t cacheline_size = 64;

   /**
    * Only 64 active threads can be used due to
    * fixed size data structures. 
    */
   constexpr uint32_t max_threads = 64;
}  // namespace sal
