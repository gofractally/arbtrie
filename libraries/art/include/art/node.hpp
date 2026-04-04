#pragma once
#include <art/arena.hpp>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace art
{
   // ── Node types ────────────────────────────────────────────────────────────
   // Tagged pointers: bit 0 of offset_t distinguishes leaf from inner node.
   // Inner nodes start with a node_header; leaves start with leaf_header.

   enum class node_type : uint8_t
   {
      setlist = 0,
      node256 = 1,
   };

   // ── Node header (8 bytes) ─────────────────────────────────────────────────
   //
   // Offset  Size  Field
   // 0       1     type            (node_type: setlist or node256)
   // 1       1     num_children
   // 2       2     partial_len     (uint16_t)
   // 4       4     value_off       (offset_t to leaf for prefix-keys, or null_offset)

   struct node_header
   {
      node_type type;
      uint8_t   num_children;
      uint16_t  partial_len;
      offset_t  value_off;
   };

   static_assert(sizeof(node_header) == 8);

   // ── Setlist node ──────────────────────────────────────────────────────────
   //
   // Cacheline-rounded. Layout in arena:
   //   [node_header: 8 bytes]
   //   [capacity: 1 byte]
   //   [keys[capacity]: capacity bytes, sorted (num_children used)]
   //   [padding to 4-byte alignment]
   //   [children[capacity]: capacity * 4 bytes, offset_t (tagged)]
   //   [partial[partial_len]: variable]
   //   [pad to cacheline boundary]
   //
   // capacity >= num_children. In-place memmove mutation when capacity allows.

   static constexpr uint8_t setlist_max_children = 48;

   /// Byte offset of children[] within a setlist node, given capacity.
   inline uint32_t setlist_children_offset(uint8_t capacity) noexcept
   {
      return (sizeof(node_header) + 1u + capacity + 3u) & ~3u;
   }

   /// Compute total allocation size for a setlist node (rounded up to cacheline).
   inline uint32_t setlist_alloc_size(uint8_t num_children, uint16_t prefix_len) noexcept
   {
      uint32_t total =
          setlist_children_offset(num_children) + uint32_t(num_children) * sizeof(offset_t) + prefix_len;
      return (total + cacheline_size - 1) & ~(cacheline_size - 1);
   }

   /// Compute max children capacity that fits in a given allocation size.
   inline uint8_t compute_setlist_capacity(uint32_t alloc_size, uint16_t prefix_len) noexcept
   {
      if (alloc_size <= sizeof(node_header) + 1 + prefix_len)
         return 0;
      // Each child ≈ 5 bytes (1 key + 4 offset) + ~1 byte alignment overhead
      uint8_t cap =
          static_cast<uint8_t>(std::min<uint32_t>((alloc_size - sizeof(node_header) - 1 - prefix_len) / 5,
                                                   setlist_max_children));
      // Adjust down if alignment padding makes it not fit
      while (cap > 0)
      {
         uint32_t needed = setlist_children_offset(cap) + uint32_t(cap) * sizeof(offset_t) + prefix_len;
         if (needed <= alloc_size)
            return cap;
         --cap;
      }
      return 0;
   }

   /// Access helpers for setlist fields given a pointer to the node_header.
   struct setlist_view
   {
      node_header* hdr;

      uint8_t  num_children() const noexcept { return hdr->num_children; }
      uint16_t partial_len() const noexcept { return hdr->partial_len; }

      uint8_t capacity() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(hdr)[sizeof(node_header)];
      }

      uint8_t* keys() noexcept
      {
         return reinterpret_cast<uint8_t*>(hdr) + sizeof(node_header) + 1;
      }
      const uint8_t* keys() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(hdr) + sizeof(node_header) + 1;
      }

      offset_t* children() noexcept
      {
         return reinterpret_cast<offset_t*>(reinterpret_cast<uint8_t*>(hdr) +
                                            setlist_children_offset(capacity()));
      }
      const offset_t* children() const noexcept
      {
         return reinterpret_cast<const offset_t*>(reinterpret_cast<const uint8_t*>(hdr) +
                                                  setlist_children_offset(capacity()));
      }

      uint8_t* partial() noexcept
      {
         return reinterpret_cast<uint8_t*>(children() + capacity());
      }
      const uint8_t* partial() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(children() + capacity());
      }

      std::string_view prefix() const noexcept
      {
         return {reinterpret_cast<const char*>(partial()), hdr->partial_len};
      }
   };

   // ── Node256 ───────────────────────────────────────────────────────────────
   //
   // Layout in arena:
   //   [node_header: 8 bytes]
   //   [children[256]: 1024 bytes, offset_t; null_offset = empty]
   //   [partial[partial_len]: variable]
   //   [pad to cacheline boundary]

   static constexpr uint32_t node256_children_offset = sizeof(node_header);
   static constexpr uint32_t node256_fixed_size      = sizeof(node_header) + 256 * sizeof(offset_t);

   inline uint32_t node256_alloc_size(uint16_t prefix_len) noexcept
   {
      uint32_t total = node256_fixed_size + prefix_len;
      return (total + cacheline_size - 1) & ~(cacheline_size - 1);
   }

   struct node256_view
   {
      node_header* hdr;

      uint8_t  num_children() const noexcept { return hdr->num_children; }
      uint16_t partial_len() const noexcept { return hdr->partial_len; }

      offset_t* children() noexcept
      {
         return reinterpret_cast<offset_t*>(reinterpret_cast<uint8_t*>(hdr) +
                                            node256_children_offset);
      }
      const offset_t* children() const noexcept
      {
         return reinterpret_cast<const offset_t*>(reinterpret_cast<const uint8_t*>(hdr) +
                                                  node256_children_offset);
      }

      uint8_t* partial() noexcept
      {
         return reinterpret_cast<uint8_t*>(hdr) + node256_fixed_size;
      }
      const uint8_t* partial() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(hdr) + node256_fixed_size;
      }

      std::string_view prefix() const noexcept
      {
         return {reinterpret_cast<const char*>(partial()), hdr->partial_len};
      }
   };

   // ── Leaf ──────────────────────────────────────────────────────────────────
   //
   // Cacheline-rounded arena allocation:
   //   [key_len: 4 bytes]
   //   [key_data: key_len bytes]
   //   [padding to 8-byte alignment]
   //   [value: sizeof(Value) bytes]

   struct leaf_header
   {
      uint32_t key_len;
   };

   static_assert(sizeof(leaf_header) == 4);

   /// Compute value offset within a leaf (after key + padding).
   inline uint32_t leaf_value_offset(uint32_t key_len) noexcept
   {
      return (sizeof(leaf_header) + key_len + 7u) & ~7u;
   }

   /// Compute total leaf allocation size.
   template <typename Value>
   uint32_t leaf_alloc_size(uint32_t key_len) noexcept
   {
      return leaf_value_offset(key_len) + sizeof(Value);
   }

   /// Access helpers for leaf fields.
   template <typename Value>
   struct leaf_view
   {
      leaf_header* hdr;

      uint32_t key_len() const noexcept { return hdr->key_len; }

      const char* key_data() const noexcept
      {
         return reinterpret_cast<const char*>(hdr) + sizeof(leaf_header);
      }

      std::string_view key() const noexcept { return {key_data(), hdr->key_len}; }

      Value* value() noexcept
      {
         return reinterpret_cast<Value*>(reinterpret_cast<uint8_t*>(hdr) +
                                         leaf_value_offset(hdr->key_len));
      }

      const Value* value() const noexcept
      {
         return reinterpret_cast<const Value*>(reinterpret_cast<const uint8_t*>(hdr) +
                                               leaf_value_offset(hdr->key_len));
      }
   };

   // ── Allocation helpers ────────────────────────────────────────────────────

   /// Save prefix to stack buffer if it points into the arena.
   /// Returns the (possibly relocated) data pointer.
   inline const char* save_prefix_if_arena(const arena&     a,
                                           std::string_view prefix,
                                           char*            buf,
                                           uint32_t         buf_size) noexcept
   {
      const char* p = prefix.data();
      if (p >= a.base() && p < a.base() + a.capacity() && !prefix.empty())
      {
         assert(prefix.size() <= buf_size);
         (void)buf_size;
         std::memcpy(buf, p, prefix.size());
         return buf;
      }
      return p;
   }

   /// Allocate a leaf in the arena, copy key and value. Returns tagged offset.
   template <typename Value>
   offset_t make_leaf(arena& a, std::string_view key, const Value& value) noexcept
   {
      uint32_t size = leaf_alloc_size<Value>(key.size());
      offset_t off  = a.allocate(size);
      auto*    lh   = a.as<leaf_header>(off);
      lh->key_len   = key.size();
      std::memcpy(reinterpret_cast<char*>(lh) + sizeof(leaf_header), key.data(), key.size());
      auto* val = reinterpret_cast<Value*>(reinterpret_cast<uint8_t*>(lh) +
                                           leaf_value_offset(key.size()));
      *val      = value;
      return tag_leaf(off);
   }

   /// Allocate a setlist node with the given prefix. Children/keys are uninitialized.
   inline offset_t make_setlist(arena& a, uint8_t num_children, std::string_view prefix) noexcept
   {
      char         pfx_buf[512];
      const char*  pfx_data = save_prefix_if_arena(a, prefix, pfx_buf, sizeof(pfx_buf));
      uint16_t     pfx_len  = prefix.size();

      uint32_t size = setlist_alloc_size(num_children, pfx_len);
      uint8_t  cap  = compute_setlist_capacity(size, pfx_len);
      assert(cap >= num_children);

      offset_t off = a.allocate(size);
      auto*    hdr = a.as<node_header>(off);
      hdr->type         = node_type::setlist;
      hdr->num_children = num_children;
      hdr->partial_len  = pfx_len;
      hdr->value_off    = null_offset;

      // Store capacity byte
      reinterpret_cast<uint8_t*>(hdr)[sizeof(node_header)] = cap;

      // Zero the keys + padding region for deterministic SIMD reads
      uint8_t* keys_start = reinterpret_cast<uint8_t*>(hdr) + sizeof(node_header) + 1;
      uint32_t keys_region = setlist_children_offset(cap) - sizeof(node_header) - 1;
      std::memset(keys_start, 0, keys_region);

      // Copy prefix
      if (pfx_len > 0)
      {
         setlist_view sv{hdr};
         std::memcpy(sv.partial(), pfx_data, pfx_len);
      }

      return off;
   }

   /// Allocate a node256 with the given prefix. Children initialized to null_offset.
   inline offset_t make_node256(arena& a, std::string_view prefix) noexcept
   {
      char         pfx_buf[512];
      const char*  pfx_data = save_prefix_if_arena(a, prefix, pfx_buf, sizeof(pfx_buf));
      uint16_t     pfx_len  = prefix.size();

      uint32_t size = node256_alloc_size(pfx_len);
      offset_t off  = a.allocate(size);
      auto*    hdr  = a.as<node_header>(off);
      hdr->type         = node_type::node256;
      hdr->num_children = 0;
      hdr->partial_len  = pfx_len;
      hdr->value_off    = null_offset;

      // Initialize all children to null_offset
      node256_view nv{hdr};
      std::memset(nv.children(), 0xFF, 256 * sizeof(offset_t));

      // Copy prefix
      if (pfx_len > 0)
         std::memcpy(nv.partial(), pfx_data, pfx_len);

      return off;
   }

}  // namespace art
