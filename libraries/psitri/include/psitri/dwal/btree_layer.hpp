#pragma once
#include <art/art_map.hpp>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/range_tombstone_list.hpp>

#include <cstring>
#include <memory_resource>
#include <string>
#include <string_view>

namespace psitri::dwal
{
   /// A btree layer: ART map + range tombstones + bump allocator pool.
   ///
   /// Keys are stored in the ART arena. Value data (string_view payloads)
   /// is stored in the PMR pool for stable pointers. The pool is freed
   /// as a unit when the layer is discarded.
   struct btree_layer
   {
      using map_type = art::art_map<btree_value>;
      using iterator = map_type::iterator;

      std::pmr::monotonic_buffer_resource pool;
      map_type                            map;
      range_tombstone_list                tombstones;
      uint32_t                            generation = 0;

      btree_layer() : map(1u << 20)
      {
         tombstones.set_copy_fn(
             [](void* ctx, std::string_view src) -> std::string_view
             {
                return static_cast<btree_layer*>(ctx)->store_string(src);
             },
             this);
      }

      // Non-copyable, non-movable (pool addresses must stay stable).
      btree_layer(const btree_layer&)            = delete;
      btree_layer& operator=(const btree_layer&) = delete;
      btree_layer(btree_layer&&)                 = delete;
      btree_layer& operator=(btree_layer&&)      = delete;

      /// Copy a string into the pool, returning a stable string_view.
      /// Used for value data and range tombstone bounds.
      std::string_view store_string(std::string_view src)
      {
         if (src.empty())
            return {};
         auto* buf = static_cast<char*>(pool.allocate(src.size(), 1));
         std::memcpy(buf, src.data(), src.size());
         return {buf, src.size()};
      }

      /// Store a key/data-value pair into the map.
      /// Value data is copied to the pool for stable pointers.
      void store_data(std::string_view key, std::string_view value)
      {
         auto pool_val = store_string(value);
         map.upsert(key, btree_value::make_data(pool_val));
      }

      /// Store a key/subtree pair.
      void store_subtree(std::string_view key, sal::ptr_address addr)
      {
         map.upsert(key, btree_value::make_subtree(addr));
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
