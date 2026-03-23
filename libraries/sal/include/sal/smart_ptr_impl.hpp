#pragma once
#include <sal/allocator_impl.hpp>
#include <sal/smart_ptr.hpp>

namespace sal
{
   bool smart_ptr_base::is_read_only() const noexcept
   {
      if (is_valid())
         return _asession->is_read_only(_adr);
      return true;
   }

}  // namespace sal
