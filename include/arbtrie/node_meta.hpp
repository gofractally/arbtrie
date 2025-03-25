#pragma once
#include <arbtrie/address.hpp>
#include <arbtrie/config.hpp>
#include <arbtrie/debug.hpp>
#include <arbtrie/node_location.hpp>
#include <arbtrie/util.hpp>
#include <cassert>
#include <iostream>
#include <optional>
#include <sal/shared_ptr.hpp>
#include <string_view>
#include <variant>
namespace arbtrie
{
   enum node_type : uint8_t
   {
      freelist  = 0,  // not initialized/invalid, must be first enum
      binary    = 1,  // binary search
      value     = 2,  // just the data, no key
      setlist   = 3,  // list of branches
      full      = 4,  // 256 full id_type
      bitset    = 5,  // 1 bit per present branch
      undefined = 6,  // no type has been defined yet
      unused    = 7

      // future, requires taking a bit, or removing undefined/freelist
      //index    = 7,  // 256 index buffer to id_type
      //bitfield = 8,
      //merge    = 9,  // delta applied to existing node
   };
   static constexpr int num_types         = 7;
   static const char*   node_type_names[] = {"freelist", "binary", "value",    "setlist",
                                             "full",     "bitset", "undefined"};

   inline std::ostream& operator<<(std::ostream& out, node_type t)
   {
      if (t < node_type::undefined) [[likely]]
         return out << node_type_names[t];
      return out << "undefined(" << int(t) << ")";
   }

   /**
    * @class node_meta
    * 
    * This class is the core of the arbtrie memory managment algorithm
    * and is responsible for a majority of the lock-free properties. It
    * manages 8 bytes of "meta" information on every node in the trie
    * including its current location, reference count, and type. 
    *
    * Because node_meta is an atomic type and we desire to minimize 
    * the number of atomic accesses, @ref node_meta is templated on
    * the storage type so the majority of the API can be used on the
    * temporary read from the atomic. See node_meta<>::temp_type
    * 
    * Primary Operations:
    *     retain() / release() - reference counting
    *     read/pending flags for cache signalling
    *     compare_exchange_location() - move the node to a new location
    * 
    * Assumptions:
    *     Only unique owners are able to modify the data
    *     pointed at by the current location, assuming that location
    *     hasn't been made write protected yet.
    */
   using node_meta_type = sal::shared_ptr;
   using temp_meta_type = sal::shared_ptr::shared_ptr_data;

}  // namespace arbtrie
