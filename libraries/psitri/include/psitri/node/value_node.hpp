#pragma once

namespace psitri {

   /**
    * Value nodes no longer require prefix or
    * ability to handle subtree because we no longer have 
    * the pathological case of a radix node having 256 binary
    * nodes with 1 key each... thanks to b+ style inner nodes which
    * don't require a key byte to be consumed, binary nodes can easily
    * be split in half rather than spawn large numbers of branches when
    * full.
    */
   struct value_node : public node {
     uint32_t data_size; // becakse node size is always a multiple of 64 and values might not be
     uint8_t  data[data_size];
   };

}
