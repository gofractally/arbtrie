#pragma once
#include <absl/container/btree_map.h>
#include <psitri/dwal/btree_value.hpp>
#include <psitri/dwal/range_tombstone_list.hpp>

#include <cstring>
#include <memory_resource>
#include <string>
#include <string_view>

namespace psitri::dwal
{
   /// A btree layer: map + range tombstones + bump allocator pool.
   ///
   /// Keys are std::pmr::string backed by the pool — short keys (≤22 bytes)
   /// stay inline in the btree node via SSO, avoiding pointer chases during
   /// comparisons. The btree's own node allocations also come from the pool.
   /// The entire pool is freed as a unit when the layer is discarded.
   struct btree_layer
   {
      using pmr_alloc = std::pmr::polymorphic_allocator<
          std::pair<const std::pmr::string, btree_value>>;
      using map_type = absl::btree_map<std::pmr::string, btree_value, std::less<>, pmr_alloc>;

      std::pmr::monotonic_buffer_resource pool;
      map_type                            map;
      range_tombstone_list                tombstones;
      uint32_t                            generation = 0;

      btree_layer() : map(pmr_alloc(&pool))
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

      /// Create a pmr::string key backed by our pool.
      std::pmr::string make_key(std::string_view src)
      {
         return std::pmr::string(src, &pool);
      }

      /// Store a key/data-value pair into the map.
      /// Key uses pmr::string (SSO for short keys), value data copied to pool.
      void store_data(std::string_view key, std::string_view value)
      {
         auto pool_val = store_string(value);
         map.insert_or_assign(make_key(key), btree_value::make_data(pool_val));
      }

      /// Store a key/subtree pair.
      void store_subtree(std::string_view key, sal::ptr_address addr)
      {
         map.insert_or_assign(make_key(key), btree_value::make_subtree(addr));
      }

      /// Store a tombstone for a key.
      void store_tombstone(std::string_view key)
      {
         map.insert_or_assign(make_key(key), btree_value::make_tombstone());
      }

      /// Number of entries (including tombstones).
      size_t size() const noexcept { return map.size(); }

      /// True if both the map and tombstone list are empty.
      bool empty() const noexcept { return map.empty() && tombstones.empty(); }
   };

}  // namespace psitri::dwal
