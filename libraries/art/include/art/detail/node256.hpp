#pragma once
#include <art/node.hpp>

namespace art::detail
{
   /// Find child in a node256. Returns pointer to the child offset_t slot,
   /// or nullptr if the slot is empty (null_offset).
   inline offset_t* node256_find_child(node_header* hdr, uint8_t byte) noexcept
   {
      node256_view nv{hdr};
      offset_t*    slot = &nv.children()[byte];
      return (*slot != null_offset) ? slot : nullptr;
   }

   inline const offset_t* node256_find_child(const node_header* hdr, uint8_t byte) noexcept
   {
      return node256_find_child(const_cast<node_header*>(hdr), byte);
   }

   /// Add a child to a node256 at the given byte.
   /// Asserts the slot is currently empty.
   inline void node256_add_child(node_header* hdr, uint8_t byte, offset_t child_off) noexcept
   {
      node256_view nv{hdr};
      assert(nv.children()[byte] == null_offset);
      nv.children()[byte] = child_off;
      hdr->num_children++;
   }

   /// Remove a child from a node256 at the given byte.
   /// Returns true if removed, false if slot was already empty.
   inline bool node256_remove_child(node_header* hdr, uint8_t byte) noexcept
   {
      node256_view nv{hdr};
      if (nv.children()[byte] == null_offset)
         return false;
      nv.children()[byte] = null_offset;
      hdr->num_children--;
      return true;
   }

   /// Find the first occupied child slot >= byte in a node256.
   /// Returns the byte value [0-255], or 256 if none found.
   inline uint16_t node256_lower_bound(const node_header* hdr, uint8_t byte) noexcept
   {
      node256_view nv{const_cast<node_header*>(hdr)};
      for (uint16_t i = byte; i < 256; ++i)
      {
         if (nv.children()[i] != null_offset)
            return i;
      }
      return 256;
   }

   /// Find the first occupied child slot in a node256.
   /// Returns the byte value [0-255], or 256 if empty.
   inline uint16_t node256_first_child(const node_header* hdr) noexcept
   {
      return node256_lower_bound(hdr, 0);
   }

   /// Find the next occupied child slot after the given byte.
   /// Returns the byte value [0-255], or 256 if none found.
   inline uint16_t node256_next_child(const node_header* hdr, uint8_t after_byte) noexcept
   {
      if (after_byte == 255)
         return 256;
      return node256_lower_bound(hdr, after_byte + 1);
   }

   /// Find the last occupied child slot in a node256.
   /// Returns the byte value [0-255], or 256 if empty.
   inline uint16_t node256_last_child(const node_header* hdr) noexcept
   {
      node256_view nv{const_cast<node_header*>(hdr)};
      for (int i = 255; i >= 0; --i)
      {
         if (nv.children()[i] != null_offset)
            return static_cast<uint16_t>(i);
      }
      return 256;
   }

   /// Find the previous occupied child slot before the given byte.
   /// Returns the byte value [0-255], or 256 if none found.
   inline uint16_t node256_prev_child(const node_header* hdr, uint8_t before_byte) noexcept
   {
      if (before_byte == 0)
         return 256;
      node256_view nv{const_cast<node_header*>(hdr)};
      for (int i = before_byte - 1; i >= 0; --i)
      {
         if (nv.children()[i] != null_offset)
            return static_cast<uint16_t>(i);
      }
      return 256;
   }

}  // namespace art::detail
