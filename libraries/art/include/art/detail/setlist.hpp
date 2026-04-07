#pragma once
#include <art/node.hpp>
#include <ucc/lower_bound.hpp>
#include <cstring>
#include <string>

namespace art::detail
{
   /// Find child in a setlist node. Returns pointer to the child offset_t slot,
   /// or nullptr if the byte is not present.
   inline offset_t* setlist_find_child(node_header* hdr, uint8_t byte) noexcept
   {
      setlist_view sv{hdr};
      uint16_t     pos = ucc::lower_bound_padded(sv.keys(), sv.num_children(), byte);
      if (pos < sv.num_children() && sv.keys()[pos] == byte)
         return &sv.children()[pos];
      return nullptr;
   }

   inline const offset_t* setlist_find_child(const node_header* hdr, uint8_t byte) noexcept
   {
      return setlist_find_child(const_cast<node_header*>(hdr), byte);
   }

   /// Find the lower_bound position in a setlist: first child with key >= byte.
   /// Returns the index [0, num_children]. If == num_children, all keys < byte.
   inline uint16_t setlist_lower_bound(const node_header* hdr, uint8_t byte) noexcept
   {
      setlist_view sv{const_cast<node_header*>(hdr)};
      return ucc::lower_bound_padded(sv.keys(), sv.num_children(), byte);
   }

   /// Insert a child into a setlist node using split-growth layout.
   /// Keys grow rightward from header, children grow leftward from tail.
   /// The smaller half of the children array is shifted to minimize memmove cost.
   /// Returns the node offset (same if in-place, new if reallocated).
   inline offset_t setlist_add_child(arena&   a,
                                     offset_t node_off,
                                     uint8_t  byte,
                                     offset_t child_off)
   {
      auto*        hdr = a.as<node_header>(node_off);
      setlist_view sv{hdr};

      uint8_t n = sv.num_children();
      assert(n < setlist_max_children);

      // Find insertion position in sorted keys
      uint16_t pos = ucc::lower_bound_padded(sv.keys(), n, byte);
      assert(pos >= n || sv.keys()[pos] != byte);  // no duplicate

      if (sv.free_space() >= 5)  // 1 key byte + 4 child bytes
      {
         // ── In-place insert ────────────────────────────────────────────
         uint8_t*  keys     = sv.keys();
         offset_t* children = sv.children();

         // Insert key: shift keys[pos..n-1] right by 1
         std::memmove(keys + pos + 1, keys + pos, n - pos);
         keys[pos] = byte;

         // Insert child: shift smaller half of children array.
         // Children are at the tail. We need to grow the array by one slot.
         // We can either shift children[0..pos-1] left by 4 bytes (into free space)
         // or shift children[pos..n-1] right by 4 bytes (into tail gap).
         //
         // Pick the smaller side to minimize memmove.
         if (pos <= n / 2 || hdr->tail_gap() == 0)
         {
            // Shift left portion left (toward keys) — grow children leftward
            // New children start 4 bytes earlier
            offset_t* new_children = children - 1;
            if (pos > 0)
               std::memmove(new_children, children, pos * sizeof(offset_t));
            new_children[pos] = child_off;
            // tail_gap unchanged (we grew into the left free space)
         }
         else
         {
            // Shift right portion right (toward prefix) — grow into tail gap
            if (n - pos > 0)
               std::memmove(children + pos + 1, children + pos, (n - pos) * sizeof(offset_t));
            children[pos] = child_off;
            hdr->set_tail_gap(hdr->tail_gap() - 1);
         }

         hdr->num_children = n + 1;
         return node_off;
      }

      // ── Capacity exhausted: reallocate to larger cacheline-rounded block ──
      uint8_t new_n = n + 1;

      // Save prefix before arena alloc may invalidate pointers
      std::string saved_prefix(sv.prefix());

      offset_t new_off = make_setlist(a, new_n, saved_prefix);
      auto*    new_hdr = a.as<node_header>(new_off);

      // Re-read old pointers (arena may have reallocated... though vm_arena doesn't,
      // but keep the pattern for safety)
      hdr = a.as<node_header>(node_off);
      sv  = setlist_view{hdr};

      new_hdr->value_off = hdr->value_off;
      new_hdr->set_cow_seq(hdr->cow_seq());

      setlist_view new_sv{new_hdr};

      // Copy keys with insertion
      uint8_t*       new_keys = new_sv.keys();
      const uint8_t* old_keys = sv.keys();
      std::memcpy(new_keys, old_keys, pos);
      new_keys[pos] = byte;
      std::memcpy(new_keys + pos + 1, old_keys + pos, n - pos);

      // Copy children with insertion (centered in new allocation)
      offset_t*       new_children = new_sv.children();
      const offset_t* old_children = sv.children();
      std::memcpy(new_children, old_children, pos * sizeof(offset_t));
      new_children[pos] = child_off;
      std::memcpy(new_children + pos + 1, old_children + pos, (n - pos) * sizeof(offset_t));

      return new_off;
   }

   /// Remove a child from a setlist node at the given position.
   /// Shifts the smaller half to close the gap. Returns node_off, or null_offset
   /// if the node becomes empty (caller handles that case).
   inline offset_t setlist_remove_child(arena& a, offset_t node_off, uint16_t pos) noexcept
   {
      auto*        hdr = a.as<node_header>(node_off);
      setlist_view sv{hdr};

      uint8_t n = sv.num_children();
      assert(pos < n);
      assert(n > 0);

      if (n == 1)
         return null_offset;  // caller handles empty case

      uint8_t*  keys     = sv.keys();
      offset_t* children = sv.children();

      // Remove key: shift keys[pos+1..n-1] left by 1
      std::memmove(keys + pos, keys + pos + 1, n - pos - 1);
      // Zero the freed key slot for deterministic SIMD reads
      keys[n - 1] = 0;

      // Remove child: shift smaller half.
      if (pos < n / 2)
      {
         // Shift left portion right (children[0..pos-1] shift right by 4)
         // This shrinks from the left, increasing left free space
         if (pos > 0)
            std::memmove(children + 1, children, pos * sizeof(offset_t));
         // Children array now starts one slot to the right (don't need to update
         // tail_gap since we shrank from the left)
      }
      else
      {
         // Shift right portion left (children[pos+1..n-1] shift left by 4)
         if (n - pos - 1 > 0)
            std::memmove(children + pos, children + pos + 1, (n - pos - 1) * sizeof(offset_t));
         // Freed one slot on the right — increase tail_gap
         hdr->set_tail_gap(hdr->tail_gap() + 1);
      }

      hdr->num_children = n - 1;
      return node_off;
   }

   /// Grow a full setlist into a node256.
   /// Returns the new node256 offset. The byte/child_off is the new child to add.
   inline offset_t setlist_grow_to_256(arena&   a,
                                       offset_t node_off,
                                       uint8_t  byte,
                                       offset_t child_off)
   {
      auto*        old_hdr = a.as<node_header>(node_off);
      setlist_view old_sv{old_hdr};

      offset_t new_off = make_node256(a, old_sv.prefix());
      auto*    new_hdr = a.as<node_header>(new_off);

      // Re-read old pointers (arena may have reallocated)
      old_hdr = a.as<node_header>(node_off);
      old_sv  = setlist_view{old_hdr};

      new_hdr->value_off = old_hdr->value_off;
      new_hdr->set_cow_seq(old_hdr->cow_seq());

      node256_view new_nv{new_hdr};

      // Scatter existing children
      uint8_t n = old_sv.num_children();
      for (uint8_t i = 0; i < n; ++i)
         new_nv.children()[old_sv.keys()[i]] = old_sv.children()[i];
      new_hdr->num_children = n;

      // Add the new child
      new_nv.children()[byte] = child_off;
      new_hdr->num_children++;

      return new_off;
   }

}  // namespace art::detail
