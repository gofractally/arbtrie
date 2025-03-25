#pragma once
#include <sal/shared_ptr_alloc.hpp>
namespace arbtrie
{
   using block_number  = sal::block_allocator::block_number;
   using id_alloc      = sal::shared_ptr_alloc;
   using id_allocation = sal::allocation;
   using id_address    = sal::ptr_address;
   using id_region     = sal::ptr_address::region_type;
   using id_index      = sal::ptr_address::index_type;

}  // namespace arbtrie
