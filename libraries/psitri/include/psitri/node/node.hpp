#pragma once
#include <cstdint>
#include <sal/alloc_header.hpp>
#include <sal/shared_ptr_alloc.hpp>
#include <string_view>
#include <ucc/typed_int.hpp>

namespace psitri
{
   using ptr_address     = sal::ptr_address;
   using ptr_address_seq = sal::ptr_address_seq;
   using key_view        = std::string_view;
   using value_view      = std::string_view;
   using branch_number   = ucc::typed_int<uint8_t, struct branch_number_tag>;

   /**
    * The type of node, inner, leaf, or value, extends the types
    * sal::alloc_header::header_type enum.
    */
   enum class node_type : uint8_t
   {
      inner = (uint8_t)sal::header_type::start_user_type,
      leaf,
      value
   };

   /**
    * Each node maintains a list of ptr_address that point to the
    * cacheline, a brnach is an position(line) in this list plus an index
    * the cacheline (index). Each line is points to 128 bytes or
    * 16 sal::shared_ptr objects, in accordance to Intel and Apple fetching
    * 128 bytes of RAM at a time to L3 cache.  In this way we can reference
    * tri branches is as few as 1.25 bytes per branch if the allocator helps
    * us with good locality.
    */
   struct branch
   {
      uint8_t line : 4;   ///< cacheline address index
      uint8_t index : 4;  ///< index into cacheline
   } __attribute__((packed));
   static_assert(sizeof(branch) == 1);

   /**
    * Base class for all nodes in the psitri tree, it mostly just wraps and redefines
    * methods from sal::alloc_header to use node_type instead of sal::header_type.
    */
   class node : public sal::alloc_header
   {
     public:
      constexpr node_type type() const noexcept { return (node_type)alloc_header::type(); };
      const node*         tail() const noexcept { return reinterpret_cast<const node*>(next()); }
      node* tail() noexcept { return reinterpret_cast<node*>(((char*)this) + size()); }

     protected:
      node(uint32_t asize, node_type t, ptr_address_seq seq)
          : sal::alloc_header(asize, sal::header_type(t), seq)
      {
      }

   } __attribute__((packed));
   static_assert(sizeof(node) == 12);
}  // namespace psitri
