#pragma once
#include <ucc/typed_int.hpp>

namespace sal
{
   using segment_number           = ucc::typed_int<uint32_t, struct segment_number_tag>;
   using allocator_session_number = ucc::typed_int<uint32_t, struct allocator_session_number_tag>;
   using root_object_number       = ucc::typed_int<uint32_t, struct root_object_number_tag>;
   static constexpr root_object_number null_root_index = root_object_number(-1u);

   /**
    * The index to a control_block of a shared pointer.
    */
   using ptr_address                             = ucc::typed_int<uint32_t, struct ptr_address_tag>;
   static constexpr ptr_address null_ptr_address = ptr_address(-1u);

   /// A tree reference: the root node address paired with a version control
   /// block address. The version number (41 bits) is stored in the ver
   /// control block's cacheline_offset field. Two u32 ptr_addresses pack
   /// into a single u64 for atomic load/store.
   struct tree_id
   {
      ptr_address root;
      ptr_address ver;

      constexpr tree_id() noexcept : root(null_ptr_address), ver(null_ptr_address) {}
      constexpr tree_id(ptr_address r, ptr_address v = null_ptr_address) noexcept : root(r), ver(v) {}

      /// Pack into u64 for atomic load/store (root in low 32, ver in high 32)
      uint64_t pack() const noexcept { return uint64_t(*root) | (uint64_t(*ver) << 32); }
      static tree_id unpack(uint64_t packed) noexcept
      {
         return {ptr_address(uint32_t(packed)), ptr_address(uint32_t(packed >> 32))};
      }

      bool is_valid() const noexcept { return root != null_ptr_address; }

      friend bool operator==(const tree_id&, const tree_id&) noexcept = default;
   };
   static constexpr tree_id null_tree_id{};
   // Reserved high ptr_address range for application sentinels (null, tombstones,
   // transient promote markers, …). The control-block allocator hands out indices
   // from 0 up, so it can only collide with this range after ~4.3B live control
   // blocks (impossible in practice). SAL treats any value in the range as a
   // non-address: the alloc-hint path SKIPS it instead of resolving it to a
   // (wildly out-of-bounds) cacheline, and the deref/prefetch helpers reject it —
   // so a pointer column that doubles as an alloc hint can safely hold sentinels.
   static constexpr uint32_t    reserved_ptr_address_count = 16;
   static constexpr ptr_address first_reserved_ptr_address =
       ptr_address(uint32_t(-1) - (reserved_ptr_address_count - 1));   // 0xFFFFFFF0
   inline constexpr bool is_reserved_ptr_address(ptr_address a) noexcept
   {
      return *a >= *first_reserved_ptr_address;
   }

   /// Opaque 64-bit payload published with each root. SAL stores it
   /// atomically alongside the root_object_number; the user chooses the
   /// encoding (psitri packs root_addr+ver_addr into it). SAL never
   /// inspects the bits.
   using root_payload                              = std::uint64_t;
   inline constexpr root_payload null_root_payload = 0ull;
}  // namespace sal
