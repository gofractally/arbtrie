#pragma once
#include <sal/allocator.hpp>
#include <string_view>

namespace psitri::dwal
{
   /// Values stored in the DWAL btree_map. Can be live data, a subtree
   /// reference, or a tombstone (deletion marker that shadows lower layers).
   struct btree_value
   {
      enum class kind : uint8_t
      {
         data,
         subtree,
         tombstone
      };

      kind             type    = kind::data;
      std::string_view data    = {};       // arena-backed (kind::data only)
      sal::tree_id     subtree = {};       // PsiTri subtree identifier (kind::subtree only)

      static btree_value make_data(std::string_view d) noexcept
      {
         return {kind::data, d, {}};
      }

      static btree_value make_subtree(sal::tree_id tid) noexcept
      {
         return {kind::subtree, {}, tid};
      }

      static btree_value make_tombstone() noexcept { return {kind::tombstone, {}, {}}; }

      bool is_tombstone() const noexcept { return type == kind::tombstone; }
      bool is_subtree() const noexcept { return type == kind::subtree; }
      bool is_data() const noexcept { return type == kind::data; }
   };

}  // namespace psitri::dwal
