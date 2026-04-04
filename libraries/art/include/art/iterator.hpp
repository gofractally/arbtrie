#pragma once
#include <art/detail/node256.hpp>
#include <art/detail/setlist.hpp>
#include <array>
#include <cstring>

namespace art
{
   template <typename Value>
   class art_iterator
   {
     public:
      art_iterator() = default;

      std::string_view key() const noexcept
      {
         assert(_arena && _leaf_off != null_offset);
         auto lv = leaf_view<Value>{_arena->as<leaf_header>(untag_leaf(_leaf_off))};
         return lv.key();
      }

      Value& value() const noexcept
      {
         assert(_arena && _leaf_off != null_offset);
         auto lv = leaf_view<Value>{_arena->as<leaf_header>(untag_leaf(_leaf_off))};
         return *lv.value();
      }

      art_iterator& operator++() noexcept
      {
         advance();
         return *this;
      }

      art_iterator operator++(int) noexcept
      {
         auto copy = *this;
         advance();
         return copy;
      }

      bool operator==(const art_iterator& o) const noexcept
      {
         return _leaf_off == o._leaf_off;
      }
      bool operator!=(const art_iterator& o) const noexcept { return !(*this == o); }

     private:
      template <typename V2>
      friend class art_map;  // for construction

      template <typename V>
      friend art_iterator<V> make_begin(arena& a, offset_t root);

      template <typename V>
      friend art_iterator<V> make_lower_bound(arena& a, offset_t root, std::string_view key);

      struct path_entry
      {
         offset_t node_off;
         uint16_t child_index;  // position in setlist, or byte value for node256
      };

      arena*   _arena    = nullptr;
      offset_t _leaf_off = null_offset;
      uint8_t  _depth    = 0;

      std::array<path_entry, 128> _stack;

      void push(offset_t node_off, uint16_t child_index) noexcept
      {
         assert(_depth < 128);
         _stack[_depth++] = {node_off, child_index};
      }

      /// Descend to the leftmost leaf from a given starting offset.
      void descend_to_leftmost(offset_t off) noexcept
      {
         while (off != null_offset && !is_leaf(off))
         {
            auto* hdr = _arena->as<node_header>(off);

            // If this node has a value_off, it represents a key that terminates here.
            if (hdr->value_off != null_offset)
            {
               push(off, UINT16_MAX);  // sentinel: value_off was visited
               _leaf_off = hdr->value_off;
               return;
            }

            if (hdr->type == node_type::setlist)
            {
               setlist_view sv{hdr};
               if (sv.num_children() == 0)
                  break;
               push(off, 0);
               off = sv.children()[0];
            }
            else
            {
               uint16_t first = detail::node256_first_child(hdr);
               if (first >= 256)
                  break;
               push(off, first);
               node256_view nv{hdr};
               off = nv.children()[first];
            }
         }

         _leaf_off = (off != null_offset && is_leaf(off)) ? off : null_offset;
      }

      void advance() noexcept
      {
         // Walk up the stack to find the next unvisited child
         while (_depth > 0)
         {
            auto& top = _stack[_depth - 1];
            auto* hdr = _arena->as<node_header>(top.node_off);

            if (top.child_index == UINT16_MAX)
            {
               // We just visited this node's value_off. Now visit its first child.
               if (hdr->type == node_type::setlist)
               {
                  setlist_view sv{hdr};
                  if (sv.num_children() > 0)
                  {
                     top.child_index = 0;
                     descend_to_leftmost(sv.children()[0]);
                     return;
                  }
               }
               else
               {
                  uint16_t first = detail::node256_first_child(hdr);
                  if (first < 256)
                  {
                     top.child_index = first;
                     node256_view nv{hdr};
                     descend_to_leftmost(nv.children()[first]);
                     return;
                  }
               }
               // No children — pop
               _depth--;
               continue;
            }

            // Advance to next sibling
            if (hdr->type == node_type::setlist)
            {
               setlist_view sv{hdr};
               uint16_t     next = top.child_index + 1;
               if (next < sv.num_children())
               {
                  top.child_index = next;
                  descend_to_leftmost(sv.children()[next]);
                  return;
               }
            }
            else
            {
               uint16_t next = detail::node256_next_child(
                   hdr, static_cast<uint8_t>(top.child_index));
               if (next < 256)
               {
                  top.child_index = next;
                  node256_view nv{hdr};
                  descend_to_leftmost(nv.children()[next]);
                  return;
               }
            }

            _depth--;
         }

         // Exhausted — become end iterator
         _leaf_off = null_offset;
      }
   };

   /// Create a begin() iterator.
   template <typename Value>
   art_iterator<Value> make_begin(arena& a, offset_t root)
   {
      art_iterator<Value> it;
      if (root == null_offset)
         return it;
      it._arena = &a;
      it.descend_to_leftmost(root);
      return it;
   }

   /// Create a lower_bound() iterator.
   template <typename Value>
   art_iterator<Value> make_lower_bound(arena& a, offset_t root, std::string_view key)
   {
      art_iterator<Value> it;
      if (root == null_offset)
         return it;
      it._arena = &a;

      offset_t cur   = root;
      uint32_t depth = 0;

      while (cur != null_offset)
      {
         if (is_leaf(cur))
         {
            auto lv = leaf_view<Value>{a.as<leaf_header>(untag_leaf(cur))};
            if (lv.key() >= key)
            {
               it._leaf_off = cur;
               return it;
            }
            // This leaf is < key — need to advance from stack
            it._leaf_off = cur;
            it.advance();
            return it;
         }

         auto*            hdr    = a.as<node_header>(cur);
         std::string_view prefix = (hdr->type == node_type::setlist)
                                       ? setlist_view{hdr}.prefix()
                                       : node256_view{hdr}.prefix();
         uint16_t         plen = prefix.size();

         // Compare prefix with key
         uint32_t remaining = key.size() - depth;
         uint32_t cmp_len   = std::min<uint32_t>(plen, remaining);
         int      cmp       = std::memcmp(prefix.data(), key.data() + depth, cmp_len);

         if (cmp > 0 || (cmp == 0 && plen > remaining))
         {
            it.descend_to_leftmost(cur);
            return it;
         }

         if (cmp < 0)
         {
            it._leaf_off = null_offset;
            it.advance();
            return it;
         }

         // Prefix matches
         depth += plen;

         if (depth >= key.size())
         {
            it.descend_to_leftmost(cur);
            return it;
         }

         uint8_t byte = static_cast<uint8_t>(key[depth]);
         depth++;

         if (hdr->type == node_type::setlist)
         {
            setlist_view sv{hdr};
            uint16_t     pos =
                ucc::lower_bound_padded(sv.keys(), sv.num_children(), byte);

            if (pos >= sv.num_children())
            {
               it.advance();
               return it;
            }

            it.push(cur, pos);

            if (sv.keys()[pos] > byte)
            {
               it.descend_to_leftmost(sv.children()[pos]);
               return it;
            }

            // Exact match — continue descent
            cur = sv.children()[pos];
         }
         else
         {
            uint16_t slot = detail::node256_lower_bound(hdr, byte);
            if (slot >= 256)
            {
               it.advance();
               return it;
            }

            it.push(cur, slot);

            if (slot > byte)
            {
               node256_view nv{hdr};
               it.descend_to_leftmost(nv.children()[slot]);
               return it;
            }

            node256_view nv{hdr};
            cur = nv.children()[slot];
         }
      }

      // Fell off — advance from stack
      if (it._leaf_off == null_offset)
         it.advance();
      return it;
   }

}  // namespace art
