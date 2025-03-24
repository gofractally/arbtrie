#pragma once
#include <sal/block_allocator.hpp>

namespace arbtrie
{
   using block_allocator = sal::block_allocator;
   using block_number    = sal::block_allocator::block_number;
}  // namespace arbtrie
