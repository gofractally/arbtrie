#pragma once
#include <sal/shared_ptr_alloc.hpp>

namespace arbtrie
{
   using id_address     = sal::ptr_address;
   using id_address_seq = sal::ptr_address_seq;
   using id_region      = sal::ptr_address::region_type;
   using id_index       = sal::ptr_address::index_type;
}  // namespace arbtrie
