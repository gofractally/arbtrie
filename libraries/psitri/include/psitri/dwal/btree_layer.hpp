#pragma once
#include <absl/container/btree_map.h>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/range_tombstone_list.hpp>

#include <cstring>
#include <memory_resource>
#include <string_view>

namespace psitri::dwal
{
   /// A btree layer: map + range tombstones + bump allocator pool.
   /// Keys and data values in the map are string_views pointing into the pool.
   /// The pool is freed as a unit when the layer is discarded.
   struct btree_layer
   {
      using map_type = absl::btree_map<std::string_view, btree_value>;

      std::pmr::monotonic_buffer_resource pool;
      map_type                            map;
      range_tombstone_list                tombstones;
      uint32_t                            generation = 0;

      /// Copy a string into the pool, returning a stable string_view.
      std::string_view store_string(std::string_view src)
      {
         if (src.empty())
            return {};
         auto* buf = static_cast<char*>(pool.allocate(src.size(), 1));
         std::memcpy(buf, src.data(), src.size());
         return {buf, src.size()};
      }

      /// Store a key/data-value pair into the map. Copies both into the pool.
      void store_data(std::string_view key, std::string_view value)
      {
         auto pool_key = store_string(key);
         auto pool_val = store_string(value);
         map.insert_or_assign(pool_key, btree_value::make_data(pool_val));
      }

      /// Store a key/subtree pair. Key is copied to pool; address is stored directly.
      void store_subtree(std::string_view key, sal::ptr_address addr)
      {
         auto pool_key = store_string(key);
         map.insert_or_assign(pool_key, btree_value::make_subtree(addr));
      }

      /// Store a tombstone for a key. Key is copied to pool.
      void store_tombstone(std::string_view key)
      {
         auto pool_key = store_string(key);
         map.insert_or_assign(pool_key, btree_value::make_tombstone());
      }

      /// Number of entries (including tombstones).
      size_t size() const noexcept { return map.size(); }

      /// True if both the map and tombstone list are empty.
      bool empty() const noexcept { return map.empty() && tombstones.empty(); }
   };

}  // namespace psitri::dwal
