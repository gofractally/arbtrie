#pragma once
#include <cassert>
#include <chrono>
#include <ucc/typed_int.hpp>

namespace sal
{

   using msec_timestamp = ucc::typed_int<uint64_t, struct msec_tag>;
   using usec_timestamp = ucc::typed_int<uint64_t, struct usec_tag>;
   inline msec_timestamp get_current_time_msec()
   {
      return msec_timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now().time_since_epoch())
                                .count());
   }
   inline usec_timestamp get_current_time_usec()
   {
      return usec_timestamp(std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now().time_since_epoch())
                                .count());
   }

}  // namespace sal
