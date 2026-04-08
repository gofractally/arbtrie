#pragma once
#include <art/detail/node256.hpp>
#include <art/detail/setlist.hpp>
#include <algorithm>
#include <cstring>

namespace art
{
   // ── Internal helpers ──────────────────────────────────────────────────────

   namespace detail
   {
      /// Find child slot in any inner node.
      template <typename Arena>
      inline offset_t* find_child(Arena& a, offset_t node_off, uint8_t byte) noexcept
      {
         auto* hdr = a.template as<node_header>(node_off);
         if (hdr->type == node_type::setlist)
            return setlist_find_child(hdr, byte);
         else
            return node256_find_child(hdr, byte);
      }

      /// Add a child to any inner node. Returns new node offset (may change).
      template <typename Arena>
      inline offset_t add_child(Arena&   a,
                                offset_t node_off,
                                uint8_t  byte,
                                offset_t child_off) noexcept
      {
         auto* hdr = a.template as<node_header>(node_off);
         if (hdr->type == node_type::node256)
         {
            node256_add_child(hdr, byte, child_off);
            return node_off;
         }
         if (hdr->num_children < setlist_max_children)
            return setlist_add_child(a, node_off, byte, child_off);
         else
            return setlist_grow_to_256(a, node_off, byte, child_off);
      }

      /// Clone an inner node with a new prefix.
      template <typename Arena>
      inline offset_t clone_with_prefix(Arena&           a,
                                        offset_t         old_off,
                                        std::string_view new_prefix) noexcept
      {
         auto* old_hdr = a.template as<node_header>(old_off);

         if (old_hdr->type == node_type::setlist)
         {
            setlist_view old_sv{old_hdr};
            uint8_t      n = old_sv.num_children();

            offset_t new_off = make_setlist(a, n, new_prefix);
            auto*    new_hdr = a.template as<node_header>(new_off);
            new_hdr->value_off = old_hdr->value_off;
            new_hdr->set_cow_seq(old_hdr->cow_seq());

            // Re-read old pointers (arena alloc above may invalidate)
            old_hdr = a.template as<node_header>(old_off);
            old_sv  = setlist_view{old_hdr};

            // Copy keys and children into the new (recentered) layout
            setlist_view new_sv{new_hdr};
            std::memcpy(new_sv.keys(), old_sv.keys(), n);
            std::memcpy(new_sv.children(), old_sv.children(), n * sizeof(offset_t));
            return new_off;
         }
         else
         {
            offset_t new_off = make_node256(a, new_prefix);
            auto*    new_hdr = a.template as<node_header>(new_off);

            old_hdr = a.template as<node_header>(old_off);
            node256_view old_nv{old_hdr};

            new_hdr->num_children = old_hdr->num_children;
            new_hdr->value_off    = old_hdr->value_off;
            new_hdr->set_cow_seq(old_hdr->cow_seq());

            node256_view new_nv{new_hdr};
            std::memcpy(new_nv.children(), old_nv.children(), 256 * sizeof(offset_t));
            return new_off;
         }
      }

      /// Get prefix of any inner node.
      template <typename Arena>
      inline std::string_view get_prefix(const Arena& a, offset_t node_off) noexcept
      {
         auto* hdr = a.template as<node_header>(node_off);
         if (hdr->type == node_type::setlist)
            return setlist_view{const_cast<node_header*>(hdr)}.prefix();
         else
            return node256_view{const_cast<node_header*>(hdr)}.prefix();
      }

      /// Write a child offset into a parent node's slot for the given byte.
      template <typename Arena>
      inline void write_child(Arena&   a,
                              offset_t parent_off,
                              uint8_t  byte,
                              offset_t new_child) noexcept
      {
         auto* hdr = a.template as<node_header>(parent_off);
         if (hdr->type == node_type::setlist)
         {
            offset_t* slot = setlist_find_child(hdr, byte);
            assert(slot);
            *slot = new_child;
         }
         else
         {
            node256_view nv{hdr};
            nv.children()[byte] = new_child;
         }
      }

      /// Deep-copy an inner node for COW, recentering children in the new allocation.
      /// Sets cow_seq on the copy. Returns the new node offset.
      template <typename Arena>
      inline offset_t cow_copy_node(Arena&   a,
                                    offset_t old_off,
                                    uint32_t new_cow_seq) noexcept
      {
         auto* old_hdr = a.template as<node_header>(old_off);

         if (old_hdr->type == node_type::setlist)
         {
            setlist_view old_sv{old_hdr};
            uint8_t      n = old_sv.num_children();

            // Save prefix (arena-resident) before allocation
            std::string saved_prefix(old_sv.prefix());

            // Allocate new node with same child count (recentered by make_setlist)
            offset_t new_off = make_setlist(a, n, saved_prefix);
            auto*    new_hdr = a.template as<node_header>(new_off);

            // Re-read old pointers
            old_hdr = a.template as<node_header>(old_off);
            old_sv  = setlist_view{old_hdr};

            new_hdr->value_off = old_hdr->value_off;
            new_hdr->set_cow_seq(new_cow_seq);

            setlist_view new_sv{new_hdr};
            std::memcpy(new_sv.keys(), old_sv.keys(), n);
            std::memcpy(new_sv.children(), old_sv.children(), n * sizeof(offset_t));
            return new_off;
         }
         else
         {
            setlist_view dummy{old_hdr};  // just to get prefix
            std::string saved_prefix(node256_view{old_hdr}.prefix());

            offset_t new_off = make_node256(a, saved_prefix);
            auto*    new_hdr = a.template as<node_header>(new_off);

            old_hdr = a.template as<node_header>(old_off);

            new_hdr->num_children = old_hdr->num_children;
            new_hdr->value_off    = old_hdr->value_off;
            new_hdr->set_cow_seq(new_cow_seq);

            node256_view new_nv{new_hdr};
            node256_view old_nv{old_hdr};
            std::memcpy(new_nv.children(), old_nv.children(), 256 * sizeof(offset_t));
            return new_off;
         }
      }

   }  // namespace detail

   // ── Path entry for iterative traversal ────────────────────────────────────

   struct path_entry
   {
      offset_t node_off;
      uint8_t  byte;
   };

   namespace detail
   {
      /// Ensure a node is mutable for the current cow_seq. If it's shared
      /// (cow_seq < current), copy it and update the parent pointer.
      /// Returns the (possibly new) offset of the mutable node.
      template <typename Arena>
      inline offset_t cow_ensure_mutable(Arena&       a,
                                         offset_t     cur,
                                         uint32_t     current_cow_seq,
                                         offset_t&    root,
                                         path_entry*  path,
                                         uint8_t      path_len) noexcept
      {
         auto* hdr = a.template as<node_header>(cur);
         if (hdr->cow_seq() >= current_cow_seq)
            return cur;  // already mutable

         offset_t new_off = cow_copy_node(a, cur, current_cow_seq);

         // Update parent's child pointer (or root)
         if (path_len == 0)
            root = new_off;
         else
            write_child(a, path[path_len - 1].node_off, path[path_len - 1].byte, new_off);

         return new_off;
      }
   }  // namespace detail

   // ── get() — fast point lookup, inlined dispatch + prefetch ────────────────

   template <typename Value, typename Arena>
   Value* get(Arena& a, offset_t root, std::string_view key) noexcept
   {
      offset_t cur   = root;
      uint32_t depth = 0;

      while (cur != null_offset)
      {
         if (is_leaf(cur))
         {
            auto lv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(cur))};
            return (lv.key() == key) ? lv.value() : nullptr;
         }

         auto*    hdr  = a.template as<node_header>(cur);
         uint16_t plen = hdr->partial_len;
         bool     is_sl = (hdr->type == node_type::setlist);

         // Prefix length check
         if (key.size() - depth < plen)
            return nullptr;

         // Prefix byte comparison
         if (plen > 0)
         {
            const uint8_t* pdata =
                is_sl ? setlist_view{hdr}.partial() : node256_view{hdr}.partial();
            if (std::memcmp(key.data() + depth, pdata, plen) != 0)
               return nullptr;
         }
         depth += plen;

         // Key terminates at this inner node (prefix-key)
         if (depth == key.size())
         {
            if (hdr->value_off == null_offset)
               return nullptr;
            return leaf_view<Value>{a.template as<leaf_header>(untag_leaf(hdr->value_off))}.value();
         }

         // Child lookup — single type dispatch
         uint8_t  byte = static_cast<uint8_t>(key[depth++]);
         offset_t next;

         if (is_sl)
         {
            setlist_view sv{hdr};
            uint16_t     pos = ucc::lower_bound_padded(sv.keys(), sv.num_children(), byte);
            if (pos >= sv.num_children() || sv.keys()[pos] != byte)
               return nullptr;
            next = sv.children()[pos];
         }
         else
         {
            next = node256_view{hdr}.children()[byte];
            if (next == null_offset)
               return nullptr;
         }

         // Prefetch next node's cache line
         __builtin_prefetch(a.template as<char>(is_leaf(next) ? untag_leaf(next) : next), 0, 3);
         cur = next;
      }
      return nullptr;
   }

   // ── Helper: allocate leaf, possibly with inline data ─────────────────────

   namespace detail
   {
      template <typename Value, typename Arena>
      offset_t alloc_leaf(Arena& a, std::string_view key, const Value& value,
                          std::string_view inline_data, uint32_t cow_seq = 0)
      {
         if (inline_data.empty())
            return make_leaf<Value>(a, key, value, cow_seq);
         else
            return make_leaf_with_inline_data<Value>(a, key, value, inline_data, cow_seq);
      }
   }

   // ── upsert() — iterative insert or overwrite ─────────────────────────────

   template <typename Value, typename Arena>
   std::pair<Value*, bool> upsert_inline(Arena&           a,
                                         offset_t&        root,
                                         std::string_view key,
                                         const Value&     value,
                                         std::string_view inline_data,
                                         uint32_t         cow_seq = 0) noexcept;

   template <typename Value, typename Arena>
   std::pair<Value*, bool> upsert(Arena&           a,
                                  offset_t&        root,
                                  std::string_view key,
                                  const Value&     value,
                                  uint32_t         cow_seq = 0) noexcept
   {
      return upsert_inline<Value>(a, root, key, value, {}, cow_seq);
   }

   template <typename Value, typename Arena>
   std::pair<Value*, bool> upsert_inline(Arena&           a,
                                         offset_t&        root,
                                         std::string_view key,
                                         const Value&     value,
                                         std::string_view inline_data,
                                         uint32_t         cow_seq) noexcept
   {
      // ── Empty tree ─────────────────────────────────────────────────────
      if (root == null_offset)
      {
         root    = detail::alloc_leaf(a, key, value, inline_data, cow_seq);
         auto lv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(root))};
         return {lv.value(), true};
      }

      path_entry path[128];
      uint8_t    path_len = 0;
      offset_t   cur      = root;
      uint32_t   depth    = 0;

      for (;;)
      {
         // ── LEAF ─────────────────────────────────────────────────────────
         if (is_leaf(cur))
         {
            auto lv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(cur))};

            // Exact match — overwrite
            if (lv.key() == key)
            {
               auto* lh = a.template as<leaf_header>(untag_leaf(cur));
               if (lh->cow_seq < cow_seq || !inline_data.empty())
               {
                  // Allocate a new leaf: COW requires it (shared with snapshot),
                  // or inline_data requires it (old leaf may have different size).
                  offset_t new_leaf = detail::alloc_leaf(a, key, value, inline_data, cow_seq);
                  if (path_len == 0)
                     root = new_leaf;
                  else
                     detail::write_child(a, path[path_len - 1].node_off,
                                         path[path_len - 1].byte, new_leaf);
                  auto nlv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(new_leaf))};
                  return {nlv.value(), false};
               }
               *lv.value() = value;
               return {lv.value(), false};
            }

            // Split: find divergence
            std::string_view existing_key = lv.key();
            uint32_t         ek_size      = existing_key.size();
            uint32_t         max_cmp = std::min<uint32_t>(ek_size, key.size());
            uint32_t         common  = depth;
            while (common < max_cmp && existing_key[common] == key[common])
               ++common;

            // Save arena-resident data before allocations
            uint8_t existing_diverge = 0;
            if (common < ek_size)
               existing_diverge = static_cast<uint8_t>(existing_key[common]);

            std::string_view shared_prefix(key.data() + depth, common - depth);

            // Create new leaf (may realloc arena)
            offset_t new_leaf = detail::alloc_leaf(a, key, value, inline_data, cow_seq);

            // Build split node
            uint8_t  child_count = 0;
            uint8_t  kbuf[2];
            offset_t cbuf[2];

            if (common < ek_size)
            {
               kbuf[child_count]   = existing_diverge;
               cbuf[child_count++] = cur;
            }
            if (common < key.size())
            {
               uint8_t nb  = static_cast<uint8_t>(key[common]);
               uint8_t pos = (child_count > 0 && nb > kbuf[0]) ? 1 : 0;
               if (pos == 0 && child_count > 0)
               {
                  kbuf[1] = kbuf[0];
                  cbuf[1] = cbuf[0];
               }
               kbuf[pos]   = nb;
               cbuf[pos]   = new_leaf;
               child_count++;
            }

            assert(child_count > 0);
            offset_t new_node = make_setlist(a, child_count, shared_prefix);
            auto*    new_hdr  = a.template as<node_header>(new_node);
            setlist_view new_sv{new_hdr};
            std::memcpy(new_sv.keys(), kbuf, child_count);
            std::memcpy(new_sv.children(), cbuf, child_count * sizeof(offset_t));

            if (common >= ek_size)
               new_hdr->value_off = cur;
            if (common >= key.size())
               new_hdr->value_off = new_leaf;

            // Write back to parent
            if (path_len == 0)
               root = new_node;
            else
               detail::write_child(a, path[path_len - 1].node_off, path[path_len - 1].byte,
                                   new_node);

            auto nlv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(new_leaf))};
            return {nlv.value(), true};
         }

         // ── INNER NODE ──────────────────────────────────────────────────

         // COW check: if this node is shared with a snapshot, copy it first
         if (cow_seq > 0)
            cur = detail::cow_ensure_mutable(a, cur, cow_seq, root, path, path_len);

         auto*    hdr  = a.template as<node_header>(cur);
         uint16_t plen = hdr->partial_len;
         bool     is_sl = (hdr->type == node_type::setlist);

         // Get prefix pointer
         const uint8_t* pdata =
             is_sl ? setlist_view{hdr}.partial() : node256_view{hdr}.partial();

         // Find prefix mismatch
         uint32_t max_check = std::min<uint32_t>(plen, key.size() - depth);
         uint32_t mismatch  = 0;
         while (mismatch < max_check && pdata[mismatch] == static_cast<uint8_t>(key[depth + mismatch]))
            ++mismatch;

         if (mismatch < plen)
         {
            // PREFIX MISMATCH — split this inner node
            std::string_view shared(key.data() + depth, mismatch);

            // Save arena-resident data
            uint8_t     old_byte = pdata[mismatch];
            std::string remaining_prefix(reinterpret_cast<const char*>(pdata) + mismatch + 1,
                                         plen - mismatch - 1);

            offset_t new_leaf = detail::alloc_leaf(a, key, value, inline_data, cow_seq);

            uint8_t  child_count = 0;
            uint8_t  kbuf[2];
            offset_t cbuf[2];

            offset_t cloned     = detail::clone_with_prefix(a, cur, remaining_prefix);
            kbuf[child_count]   = old_byte;
            cbuf[child_count++] = cloned;

            if (depth + mismatch < key.size())
            {
               uint8_t nb  = static_cast<uint8_t>(key[depth + mismatch]);
               uint8_t pos = (nb > old_byte) ? 1 : 0;
               if (pos == 0)
               {
                  kbuf[1] = kbuf[0];
                  cbuf[1] = cbuf[0];
               }
               kbuf[pos]   = nb;
               cbuf[pos]   = new_leaf;
               child_count++;
            }

            offset_t split = make_setlist(a, child_count, shared);
            auto*    shdr  = a.template as<node_header>(split);
            setlist_view ssv{shdr};
            std::memcpy(ssv.keys(), kbuf, child_count);
            std::memcpy(ssv.children(), cbuf, child_count * sizeof(offset_t));

            if (depth + mismatch >= key.size())
               shdr->value_off = new_leaf;

            if (path_len == 0)
               root = split;
            else
               detail::write_child(a, path[path_len - 1].node_off, path[path_len - 1].byte, split);

            auto nlv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(new_leaf))};
            return {nlv.value(), true};
         }

         // Full prefix match
         depth += plen;

         // Key terminates at this inner node
         if (depth == key.size())
         {
            hdr = a.template as<node_header>(cur);
            if (hdr->value_off != null_offset)
            {
               auto* lh = a.template as<leaf_header>(untag_leaf(hdr->value_off));
               if (lh->cow_seq < cow_seq || !inline_data.empty())
               {
                  // COW or inline_data requires new leaf allocation
                  offset_t new_leaf = detail::alloc_leaf(a, key, value, inline_data, cow_seq);
                  a.template as<node_header>(cur)->value_off = new_leaf;
                  auto nlv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(new_leaf))};
                  return {nlv.value(), false};
               }
               auto lv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(hdr->value_off))};
               *lv.value() = value;
               return {lv.value(), false};
            }
            offset_t new_leaf = detail::alloc_leaf(a, key, value, inline_data, cow_seq);
            a.template as<node_header>(cur)->value_off = new_leaf;
            auto lv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(new_leaf))};
            return {lv.value(), true};
         }

         // Find child
         uint8_t  byte = static_cast<uint8_t>(key[depth++]);
         offset_t child;
         bool     found = false;

         // Re-derive hdr (prefix data may have been used but no alloc happened)
         hdr = a.template as<node_header>(cur);
         if (hdr->type == node_type::setlist)
         {
            setlist_view sv{hdr};
            uint16_t     pos = ucc::lower_bound_padded(sv.keys(), sv.num_children(), byte);
            if (pos < sv.num_children() && sv.keys()[pos] == byte)
            {
               child = sv.children()[pos];
               found = true;
            }
         }
         else
         {
            child = node256_view{hdr}.children()[byte];
            found = (child != null_offset);
         }

         if (!found)
         {
            // Add new leaf as child
            offset_t new_leaf = detail::alloc_leaf(a, key, value, inline_data, cow_seq);
            offset_t new_cur  = detail::add_child(a, cur, byte, new_leaf);
            if (new_cur != cur)
            {
               if (path_len == 0)
                  root = new_cur;
               else
                  detail::write_child(a, path[path_len - 1].node_off, path[path_len - 1].byte,
                                      new_cur);
            }
            auto nlv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(new_leaf))};
            return {nlv.value(), true};
         }

         // Prefetch and descend
         __builtin_prefetch(a.template as<char>(is_leaf(child) ? untag_leaf(child) : child), 0, 3);
         path[path_len++] = {cur, byte};
         cur = child;
      }
   }

   // ── erase() — iterative delete ────────────────────────────────────────────

   namespace detail
   {
      /// Try to collapse a single-child setlist into its child (path compression).
      /// Returns the replacement offset, or cur if no collapse needed.
      template <typename Arena>
      inline offset_t try_collapse(Arena& a, offset_t cur)
      {
         auto* hdr = a.template as<node_header>(cur);
         if (hdr->num_children != 1 || hdr->value_off != null_offset ||
             hdr->type != node_type::setlist)
            return cur;

         setlist_view sv{hdr};
         offset_t     only_child = sv.children()[0];
         uint8_t      child_byte = sv.keys()[0];

         if (is_leaf(only_child))
            return only_child;

         // Merge prefixes: parent_prefix + child_byte + child_prefix
         std::string_view pp = sv.prefix();
         std::string_view cp = get_prefix(a, only_child);
         uint32_t         merged_len = pp.size() + 1 + cp.size();
         char             merged[65536];
         std::memcpy(merged, pp.data(), pp.size());
         merged[pp.size()] = child_byte;
         std::memcpy(merged + pp.size() + 1, cp.data(), cp.size());
         return clone_with_prefix(a, only_child, {merged, merged_len});
      }
   }  // namespace detail

   template <typename Value, typename Arena>
   bool erase(Arena& a, offset_t& root, std::string_view key, uint32_t cow_seq = 0)
   {
      if (root == null_offset)
         return false;

      path_entry path[128];
      uint8_t    path_len = 0;
      offset_t   cur      = root;
      uint32_t   depth    = 0;

      for (;;)
      {
         if (is_leaf(cur))
         {
            auto lv = leaf_view<Value>{a.template as<leaf_header>(untag_leaf(cur))};
            if (lv.key() != key)
               return false;

            // Remove this leaf from parent
            if (path_len == 0)
            {
               root = null_offset;
               return true;
            }

            auto&    parent = path[path_len - 1];
            auto*    phdr   = a.template as<node_header>(parent.node_off);

            if (phdr->type == node_type::setlist)
            {
               setlist_view psv{phdr};
               uint16_t     pos =
                   ucc::lower_bound_padded(psv.keys(), psv.num_children(), parent.byte);
               assert(pos < psv.num_children() && psv.keys()[pos] == parent.byte);

               if (phdr->num_children == 1)
               {
                  // Parent becomes childless
                  offset_t replacement =
                      (phdr->value_off != null_offset) ? phdr->value_off : null_offset;
                  if (path_len <= 1)
                     root = replacement;
                  else
                     detail::write_child(a, path[path_len - 2].node_off,
                                         path[path_len - 2].byte, replacement);
               }
               else
               {
                  detail::setlist_remove_child(a, parent.node_off, pos);
                  // Check for collapse
                  offset_t collapsed = detail::try_collapse(a, parent.node_off);
                  if (collapsed != parent.node_off)
                  {
                     if (path_len <= 1)
                        root = collapsed;
                     else
                        detail::write_child(a, path[path_len - 2].node_off,
                                            path[path_len - 2].byte, collapsed);
                  }
               }
            }
            else
            {
               // Parent is node256
               node256_view pnv{phdr};
               pnv.children()[parent.byte] = null_offset;
               phdr->num_children--;

               if (phdr->num_children == 0 && phdr->value_off == null_offset)
               {
                  if (path_len <= 1)
                     root = null_offset;
                  else
                     detail::write_child(a, path[path_len - 2].node_off,
                                         path[path_len - 2].byte, null_offset);
               }
            }
            return true;
         }

         // INNER NODE — COW check + prefix
         if (cow_seq > 0)
            cur = detail::cow_ensure_mutable(a, cur, cow_seq, root, path, path_len);

         auto*    hdr  = a.template as<node_header>(cur);
         uint16_t plen = hdr->partial_len;

         if (key.size() - depth < plen)
            return false;

         if (plen > 0)
         {
            const uint8_t* pdata = (hdr->type == node_type::setlist)
                                       ? setlist_view{hdr}.partial()
                                       : node256_view{hdr}.partial();
            if (std::memcmp(key.data() + depth, pdata, plen) != 0)
               return false;
         }
         depth += plen;

         // Key terminates — erase prefix-key value
         if (depth == key.size())
         {
            if (hdr->value_off == null_offset)
               return false;
            hdr->value_off = null_offset;

            if (hdr->num_children == 0)
            {
               offset_t replacement = null_offset;
               if (path_len == 0)
                  root = replacement;
               else
                  detail::write_child(a, path[path_len - 1].node_off,
                                      path[path_len - 1].byte, replacement);
            }
            else
            {
               offset_t collapsed = detail::try_collapse(a, cur);
               if (collapsed != cur)
               {
                  if (path_len == 0)
                     root = collapsed;
                  else
                     detail::write_child(a, path[path_len - 1].node_off,
                                         path[path_len - 1].byte, collapsed);
               }
            }
            return true;
         }

         // Find child and descend
         uint8_t  byte = static_cast<uint8_t>(key[depth++]);
         offset_t child;

         hdr = a.template as<node_header>(cur);
         if (hdr->type == node_type::setlist)
         {
            setlist_view sv{hdr};
            uint16_t     pos = ucc::lower_bound_padded(sv.keys(), sv.num_children(), byte);
            if (pos >= sv.num_children() || sv.keys()[pos] != byte)
               return false;
            child = sv.children()[pos];
         }
         else
         {
            child = node256_view{hdr}.children()[byte];
            if (child == null_offset)
               return false;
         }

         path[path_len++] = {cur, byte};
         cur = child;
      }
   }

}  // namespace art
