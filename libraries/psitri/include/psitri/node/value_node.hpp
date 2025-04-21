#pragma once
#include <psitri/node/node.hpp>

namespace psitri
{

   /**
    * Value nodes no longer require prefix or
    * ability to handle subtree because we no longer have 
    * the pathological case of a radix node having 256 binary
    * nodes with 1 key each... thanks to b+ style inner nodes which
    * don't require a key byte to be consumed, binary nodes can easily
    * be split in half rather than spawn large numbers of branches when
    * full.
    */
   struct value_node : public node
   {
      static constexpr node_type type_id = node_type::value;
      uint32_t                   data_size : 31;
      uint32_t                   is_subtree : 1;
      uint8_t                    data[/*data_size*/];
   };

}  // namespace psitri
