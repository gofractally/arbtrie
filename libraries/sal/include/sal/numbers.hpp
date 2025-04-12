#pragma once
#include <ucc/typed_int.hpp>

namespace sal
{
   using segment_number           = ucc::typed_int<uint32_t, struct segment_number_tag>;
   using allocator_session_number = ucc::typed_int<uint32_t, struct allocator_session_number_tag>;

   /**
    * The index to a control_block of a shared pointer.
    */
   using ptr_address                             = ucc::typed_int<uint32_t, struct ptr_address_tag>;
   static constexpr ptr_address null_ptr_address = ptr_address(-1ul);
}  // namespace sal
