#pragma once
#include <sal/allocator_session_impl.hpp>

namespace sal
{
   inline void allocator::release(ptr_address adr) noexcept
   {
      get_session()->release(adr);
   }
}  // namespace sal