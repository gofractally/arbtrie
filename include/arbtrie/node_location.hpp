#pragma once
#include <arbtrie/config.hpp>
#include <cassert>
#include <sal/block_allocator.hpp>
#include <sal/location.hpp>

namespace arbtrie
{
   using offset_ptr    = sal::block_allocator::offset_ptr;
   using node_location = sal::location;

   inline segment_number get_segment_num(node_location loc)
   {
      return loc.absolute_address() / segment_size;
   }
   inline uint32_t get_segment_offset(node_location loc)
   {
      return loc.absolute_address() % segment_size;
   }

   /**
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wbitfield-constant-conversion"
#endif
*/
   static constexpr const node_location end_of_freelist = node_location::from_cacheline(-1);
   // initial location before seg_allocator assigns it
   static constexpr const node_location null_node = node_location::from_cacheline(-2);
   /*
#ifdef __clang__
#pragma clang diagnostic pop
#endif
*/

}  // namespace arbtrie
