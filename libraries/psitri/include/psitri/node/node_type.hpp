#pragma once
#include <cstdint>
#include <ostream>
#include <sal/alloc_header.hpp>

namespace psitri
{
   /**
    * Stable on-disk/SAL type ids for PsiTri node objects.
    *
    * Concrete node layouts live in their own headers; this file is only the
    * numeric type registry used by sal::alloc_header dispatch.
    */
   enum class node_type : uint8_t
   {
      inner = (uint8_t)sal::header_type::start_user_type,
      inner_prefix,
      leaf,
      value,
      value_index,
      wide_inner,
      direct_inner,
      bplus_inner
   };

   inline std::ostream& operator<<(std::ostream& os, node_type t)
   {
      static const char* names[] = {"inner",       "inner_prefix", "leaf",         "value",
                                    "value_index", "wide_inner",   "direct_inner", "bplus_inner",
                                    "unknown"};
      auto               idx     = static_cast<size_t>(t) - static_cast<size_t>(node_type::inner);
      os << (idx < 8 ? names[idx] : names[8]);
      return os;
   }
}  // namespace psitri
