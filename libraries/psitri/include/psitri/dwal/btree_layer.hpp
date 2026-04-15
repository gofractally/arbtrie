#pragma once
#include <art/art_map.hpp>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/range_tombstone_list.hpp>

#include <cstring>
#include <string_view>

namespace psitri::dwal
{
   /// A btree layer: ART map + range tombstones.
   ///
   /// Keys are stored in the ART arena. Value data (string_view payloads)
   /// is also stored in the ART arena — no separate pool needed.
   /// The arena is freed as a unit when the layer is discarded.
   struct btree_layer
   {
      using map_type = art::art_map<btree_value>;
      using iterator = map_type::iterator;

      map_type             map;
      range_tombstone_list tombstones;
      uint32_t             generation = 0;

      btree_layer() : map(1u << 20)
      {
         tombstones.set_copy_fn(
             [](void* ctx, std::string_view src) -> std::string_view
             {
                return static_cast<btree_layer*>(ctx)->store_string(src);
             },
             this);
      }

      // Non-copyable, non-movable (arena addresses must stay stable).
      btree_layer(const btree_layer&)            = delete;
      btree_layer& operator=(const btree_layer&) = delete;
      btree_layer(btree_layer&&)                 = delete;
      btree_layer& operator=(btree_layer&&)      = delete;

      /// Copy a string into the ART arena, returning a stable string_view.
      /// Used for range tombstone bounds (not for value data — that goes inline in leaves).
      std::string_view store_string(std::string_view src)
      {
         if (src.empty())
            return {};
         auto& arena = map.get_arena();
         auto  off   = arena.allocate(src.size());
         auto* buf   = arena.as<char>(off);
         std::memcpy(buf, src.data(), src.size());
         return {buf, src.size()};
      }

      /// Store a key/data-value pair into the map.
      /// Value data is stored inline in the ART leaf allocation — no separate alloc.
      void store_data(std::string_view key, std::string_view value)
      {
         // upsert_inline stores value bytes right after the btree_value struct
         // in the leaf. We then fix up the data string_view to point at the
         // inline copy.
         auto* bv = map.upsert_inline(key, btree_value::make_data({}), value);
         if (bv && !value.empty())
         {
            // The inline data starts right after the btree_value struct
            auto* inline_ptr = reinterpret_cast<const char*>(bv) + sizeof(btree_value);
            bv->data = {inline_ptr, value.size()};
         }
      }

      /// Store a key/subtree pair.
      void store_subtree(std::string_view key, sal::tree_id tid)
      {
         map.upsert(key, btree_value::make_subtree(tid));
      }

      /// Store a tombstone for a key.
      void store_tombstone(std::string_view key)
      {
         map.upsert(key, btree_value::make_tombstone());
      }

      /// Number of entries (including tombstones).
      size_t size() const noexcept { return map.size(); }

      /// True if both the map and tombstone list are empty.
      bool empty() const noexcept { return map.empty() && tombstones.empty(); }
   };

}  // namespace psitri::dwal
