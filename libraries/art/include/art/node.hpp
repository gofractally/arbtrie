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

   // ── Node header (12 bytes) ────────────────────────────────────────────────
   //
   // Offset  Size  Field
   // 0       1     type            (node_type: setlist or node256)
   // 1       1     num_children
   // 2       2     partial_len     (uint16_t)
   // 4       4     value_off       (offset_t to leaf for prefix-keys, or null_offset)
   // 8       4     cow_seq_gap     packed: cow_seq (25 bits) | tail_gap (7 bits)
   //
   // cow_seq: COW generation — nodes with cow_seq < current must be copied before mutation
   // tail_gap: free 4-byte slots between children[n-1] and prefix (setlist split-growth)

   struct node_header
   {
      node_type type;
      uint8_t   num_children;
      uint16_t  partial_len;
      offset_t  value_off;
      uint32_t  cow_seq_gap;  // bits 31-7: cow_seq (25 bits), bits 6-0: tail_gap (7 bits)

      uint32_t cow_seq() const noexcept { return cow_seq_gap >> 7; }
      uint8_t  tail_gap() const noexcept { return cow_seq_gap & 0x7F; }

      void set_cow_seq(uint32_t seq) noexcept
      {
         cow_seq_gap = (seq << 7) | (cow_seq_gap & 0x7F);
      }
      void set_tail_gap(uint8_t gap) noexcept
      {
         cow_seq_gap = (cow_seq_gap & ~uint32_t(0x7F)) | (gap & 0x7F);
      }
      void set_cow_seq_gap(uint32_t seq, uint8_t gap) noexcept
      {
         cow_seq_gap = (seq << 7) | (gap & 0x7F);
      }
   };

   static_assert(sizeof(node_header) == 12);

   // ── Setlist node (split-growth layout) ───────────────────────────────────
   //
   // Cacheline-rounded. Layout in arena:
   //   [node_header: 16 bytes]
   //   [alloc_cachelines: 1 byte]   (allocation size / cacheline_size)
   //   [keys[0..n-1]: n bytes]      grows rightward →
   //   [... free space ...]         shared between keys and children
   //   [children[0..n-1]: n*4 B]    grows leftward ← (children centered, drift tracked by tail_gap)
   //   [partial[partial_len]]       fixed at tail of allocation
   //   [pad to cacheline boundary]
   //
   // Keys and children are in the same logical order: keys[i] corresponds to
   // children[i]. Children are physically stored before the prefix, growing
   // leftward. The free space between keys and children absorbs growth from
   // both sides.
   //
   // tail_gap (in node_header) = number of free 4-byte slots between
   // children[n-1] and the prefix. Used to locate the children block.
   //
   // On insert, the smaller half of the children array is shifted (toward the
   // nearer edge), reducing average memmove cost by ~40% vs packed layout.
   // On COW copy, children are recentered in the free space.

   static constexpr uint8_t setlist_max_children = 128;

   /// Offset where keys begin within a setlist node.
   static constexpr uint32_t setlist_keys_offset = sizeof(node_header) + 1;

   /// Compute minimum allocation size for a setlist node (rounded up to cacheline).
   inline uint32_t setlist_alloc_size(uint8_t num_children, uint16_t prefix_len) noexcept
   {
      // header + alloc_cachelines byte + keys + children + prefix
      uint32_t total = setlist_keys_offset + num_children + uint32_t(num_children) * sizeof(offset_t) + prefix_len;
      return (total + cacheline_size - 1) & ~(cacheline_size - 1);
   }

   /// Compute max children that fit in a given allocation size.
   inline uint8_t compute_setlist_capacity(uint32_t alloc_size, uint16_t prefix_len) noexcept
   {
      if (alloc_size <= setlist_keys_offset + prefix_len)
         return 0;
      // Each child needs 5 bytes (1 key + 4 offset)
      uint32_t available = alloc_size - setlist_keys_offset - prefix_len;
      return static_cast<uint8_t>(std::min<uint32_t>(available / 5, setlist_max_children));
   }

   /// Access helpers for setlist fields given a pointer to the node_header.
   struct setlist_view
   {
      node_header* hdr;

      uint8_t  num_children() const noexcept { return hdr->num_children; }
      uint16_t partial_len() const noexcept { return hdr->partial_len; }

      /// Allocation size in bytes, derived from the alloc_cachelines byte.
      uint32_t alloc_size() const noexcept
      {
         return uint32_t(reinterpret_cast<const uint8_t*>(hdr)[sizeof(node_header)]) * cacheline_size;
      }

      /// Max children this allocation can hold.
      uint8_t capacity() const noexcept
      {
         return compute_setlist_capacity(alloc_size(), hdr->partial_len);
      }

      uint8_t* keys() noexcept
      {
         return reinterpret_cast<uint8_t*>(hdr) + setlist_keys_offset;
      }
      const uint8_t* keys() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(hdr) + setlist_keys_offset;
      }

      /// Children array: located from the tail via tail_gap.
      /// children[0] is at children_start, children[n-1] is closest to prefix.
      offset_t* children() noexcept
      {
         uint32_t children_end = alloc_size() - hdr->partial_len - uint32_t(hdr->tail_gap()) * 4;
         uint32_t children_start = children_end - uint32_t(hdr->num_children) * sizeof(offset_t);
         return reinterpret_cast<offset_t*>(reinterpret_cast<uint8_t*>(hdr) + children_start);
      }
      const offset_t* children() const noexcept
      {
         uint32_t children_end = alloc_size() - hdr->partial_len - uint32_t(hdr->tail_gap()) * 4;
         uint32_t children_start = children_end - uint32_t(hdr->num_children) * sizeof(offset_t);
         return reinterpret_cast<const offset_t*>(reinterpret_cast<const uint8_t*>(hdr) + children_start);
      }

      /// Prefix is fixed at the tail of the allocation.
      uint8_t* partial() noexcept
      {
         return reinterpret_cast<uint8_t*>(hdr) + alloc_size() - hdr->partial_len;
      }
      const uint8_t* partial() const noexcept
      {
         return reinterpret_cast<const uint8_t*>(hdr) + alloc_size() - hdr->partial_len;
      }

      std::string_view prefix() const noexcept
      {
         return {reinterpret_cast<const char*>(partial()), hdr->partial_len};
      }

      /// Free space between keys end and children start (in bytes).
      uint32_t free_space() const noexcept
      {
         uint32_t keys_end = setlist_keys_offset + hdr->num_children;
         uint32_t children_end = alloc_size() - hdr->partial_len - uint32_t(hdr->tail_gap()) * 4;
         uint32_t children_start = children_end - uint32_t(hdr->num_children) * sizeof(offset_t);
         return children_start - keys_end;
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
   // Cacheline-rounded arena allocation. Two modes:
   //
   // Fixed-value mode (art_map<T> where T is a fixed-size type):
   //   [key_len: 4 bytes]
   //   [key_data: key_len bytes]
   //   [padding to 8-byte alignment]
   //   [value: sizeof(Value) bytes]
   //
   // Inline KV mode (art_map<kv_value> — variable-length key + value):
   //   [key_len: 4 bytes]
   //   [val_len: 4 bytes]
   //   [key_data: key_len bytes]
   //   [value_data: val_len bytes]
   //   [pad to cacheline boundary]

   struct leaf_header
   {
      uint32_t key_len;
      uint32_t cow_seq;  // COW generation — skip allocation if leaf already owned by writer
   };

   static_assert(sizeof(leaf_header) == 8);

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

   /// Access helpers for leaf fields (fixed-value mode).
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

   // ── Leaf with inline trailing data ──────────────────────────────────────
   //
   // For Value types that contain a string_view pointing to variable-length
   // data, the data can be stored inline in the leaf allocation (right after
   // the fixed-size Value struct). This avoids a separate allocation.
   //
   // Layout: [key_len:4][key_data:N][pad to 8B][Value:sizeof(V)][inline_data:M][pad]
   //
   // The caller passes the data to make_leaf_with_inline_data(), which
   // allocates the leaf with extra space and copies the data inline.
   // The Value's string_view is then pointed at the inline copy.

   /// Allocate a leaf with extra inline data after the Value struct.
   /// Returns tagged offset. The inline data starts at value_offset + sizeof(Value).
   template <typename Value>
   offset_t make_leaf_with_inline_data(arena&           a,
                                       std::string_view key,
                                       const Value&     value,
                                       std::string_view inline_data,
                                       uint32_t         cow_seq = 0)
   {
      uint32_t val_off  = leaf_value_offset(key.size());
      uint32_t total    = val_off + sizeof(Value) + inline_data.size();
      // Don't cacheline-round here — let arena.allocate() handle it
      offset_t off      = a.allocate(total);
      auto*    lh       = a.as<leaf_header>(off);
      lh->key_len       = key.size();
      lh->cow_seq       = cow_seq;
      std::memcpy(reinterpret_cast<char*>(lh) + sizeof(leaf_header),
                  key.data(), key.size());
      auto* val = reinterpret_cast<Value*>(
          reinterpret_cast<uint8_t*>(lh) + val_off);
      *val = value;
      if (!inline_data.empty())
      {
         char* data_dst = reinterpret_cast<char*>(val) + sizeof(Value);
         std::memcpy(data_dst, inline_data.data(), inline_data.size());
      }
      return tag_leaf(off);
   }

   // ── Allocation helpers ────────────────────────────────────────────────────

   /// Save prefix to stack buffer if it points into the arena.
   /// Returns the (possibly relocated) data pointer.
   inline const char* save_prefix_if_arena(const arena&     a,
                                           std::string_view prefix,
                                           char*            buf,
                                           uint32_t         buf_size) noexcept
   {
      // Prefix length must fit in uint16_t (node_header::partial_len).
      // A value exceeding this indicates memory corruption.
      assert(prefix.size() <= 0xFFFF);

      const char* p = prefix.data();
      if (p >= a.base() && p < a.base() + a.bytes_used() && !prefix.empty())
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
   offset_t make_leaf(arena& a, std::string_view key, const Value& value,
                      uint32_t cow_seq = 0)
   {
      uint32_t size = leaf_alloc_size<Value>(key.size());
      offset_t off  = a.allocate(size);
      auto*    lh   = a.as<leaf_header>(off);
      lh->key_len   = key.size();
      lh->cow_seq   = cow_seq;
      std::memcpy(reinterpret_cast<char*>(lh) + sizeof(leaf_header), key.data(), key.size());
      auto* val = reinterpret_cast<Value*>(reinterpret_cast<uint8_t*>(lh) +
                                           leaf_value_offset(key.size()));
      *val      = value;
      return tag_leaf(off);
   }

   /// Allocate a setlist node with the given prefix. Children/keys are uninitialized.
   /// Children are centered in the free space between keys and prefix.
   inline offset_t make_setlist(arena& a, uint8_t num_children, std::string_view prefix)
   {
      char         pfx_buf[512];
      const char*  pfx_data = save_prefix_if_arena(a, prefix, pfx_buf, sizeof(pfx_buf));
      uint16_t     pfx_len  = prefix.size();

      uint32_t size = setlist_alloc_size(num_children, pfx_len);
      assert(size % cacheline_size == 0);
      assert(size / cacheline_size <= 255);

      uint8_t  cap = compute_setlist_capacity(size, pfx_len);
      assert(cap >= num_children);

      offset_t off = a.allocate(size);
      auto*    hdr = a.as<node_header>(off);
      hdr->type         = node_type::setlist;
      hdr->num_children = num_children;
      hdr->partial_len  = pfx_len;
      hdr->value_off    = null_offset;

      // Store alloc_cachelines byte (replaces old capacity byte)
      reinterpret_cast<uint8_t*>(hdr)[sizeof(node_header)] =
          static_cast<uint8_t>(size / cacheline_size);

      // Center children in the free space.
      uint32_t children_region = size - pfx_len - setlist_keys_offset - num_children;
      uint32_t children_bytes = uint32_t(num_children) * sizeof(offset_t);
      uint32_t free_bytes = children_region - children_bytes;
      uint32_t total_free_slots = free_bytes / 4;
      uint8_t  tg = static_cast<uint8_t>(std::min<uint32_t>(total_free_slots / 2, 0x7F));
      hdr->set_cow_seq_gap(0, tg);

      // Zero the keys region for deterministic SIMD reads
      uint8_t* keys_start = reinterpret_cast<uint8_t*>(hdr) + setlist_keys_offset;
      // Zero enough for the max capacity to keep SIMD reads clean
      std::memset(keys_start, 0, cap);

      // Copy prefix at the tail
      if (pfx_len > 0)
      {
         uint8_t* pfx_dst = reinterpret_cast<uint8_t*>(hdr) + size - pfx_len;
         std::memcpy(pfx_dst, pfx_data, pfx_len);
      }

      return off;
   }

   /// Allocate a node256 with the given prefix. Children initialized to null_offset.
   inline offset_t make_node256(arena& a, std::string_view prefix)
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
      hdr->cow_seq_gap  = 0;

      // Initialize all children to null_offset
      node256_view nv{hdr};
      std::memset(nv.children(), 0xFF, 256 * sizeof(offset_t));

      // Copy prefix
      if (pfx_len > 0)
         std::memcpy(nv.partial(), pfx_data, pfx_len);

      return off;
   }

}  // namespace art
