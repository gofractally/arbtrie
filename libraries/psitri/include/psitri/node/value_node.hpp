#pragma once
#include <psitri/node/node.hpp>
#include <psitri/value_type.hpp>
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
      static uint32_t            alloc_size(value_view v)
      {
         return ucc::round_up_multiple<64>(sizeof(value_node) + v.size());
      }
      uint32_t num_branches() const { return 1; }

      static uint32_t alloc_size(const value_type& v)
      {
         if (v.is_view())
            return ucc::round_up_multiple<64>(sizeof(value_node) + v.size());
         else
            return ucc::round_up_multiple<64>(sizeof(value_node) + sizeof(ptr_address));
      }
      value_node(uint32_t asize, ptr_address_seq seq, value_view v)
          : node(asize, type_id, seq), data_size(v.size()), is_subtree(false)
      {
         std::memcpy(data, v.data(), v.size());
      }
      value_node(uint32_t asize, ptr_address_seq seq, const value_type& v)
          : node(asize, type_id, seq), data_size(v.size()), is_subtree(v.is_subtree())
      {
         assert(v.is_view() or v.is_subtree());
         if (v.is_view())
            std::memcpy(data, v.view().data(), v.size());
         else
            *((ptr_address*)data) = v.address();
      }

      void visit_branches(std::invocable<ptr_address> auto&& lam) const
      {
         if (is_subtree)
            lam(*(const ptr_address*)data);
      }

      value_view get_data() const { return value_view((const char*)data, data_size); }

      uint32_t data_size : 31;
      uint32_t is_subtree : 1;
      uint8_t  data[/*data_size*/];
   };

   template <typename T>
   concept is_value_node = std::same_as<std::remove_cvref_t<std::remove_pointer_t<T>>, value_node>;
}  // namespace psitri
