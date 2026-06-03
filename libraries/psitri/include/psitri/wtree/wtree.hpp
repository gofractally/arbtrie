#pragma once
// psitri::wtree public surface (Stage A: single tiny LEAF build + point find).
// Inner routing, splits, the tier×width matrix, and the varlen value model land in
// later stages. This slice exists to compile + smoke-test the typed-node foundation.

#include <psitri/wtree/node.hpp>

namespace psitri::wtree
{
   // Build a single `tiny` LEAF from sorted, distinct u32 keys + u64 values.
   // Leaf layout: stems[0..N) = keys; slot[i+1] = value for stems[i] (slot[0] =
   // terminal, unused here). Returns the leaf's ptr_address.
   inline ptr_address build_tiny_leaf(std::span<const uint32_t> keys,
                                      std::span<const uint64_t> vals) noexcept
   {
      const uint16_t n = uint16_t(keys.size());
      const ptr_address a = session().alloc<tiny>(sal::alloc_hint{}, uint8_t(0), uint8_t(0), n);
      tiny* nd = deref<tiny>(a);
      uint32_t* st = nd->u32_stems();
      for (uint16_t i = 0; i < n; ++i) st[i] = keys[i];
      for (uint16_t i = 0; i < n; ++i)
         std::memcpy(nd->value_at(uint16_t(i + 1)), &vals[i], tiny::kValueBytes);
      return a;
   }

   // Point lookup in a tiny leaf: returns the value bytes or nullptr (absent).
   inline const uint8_t* find_in_tiny_leaf(const tiny* nd, uint32_t q) noexcept
   {
      const uint16_t k = nd->count_le(q);                 // # stems ≤ q, in [0,N]
      if (k > 0 && nd->u32_stems()[k - 1] == q)           // exact match at stems[k-1]
         return nd->value_at(k);                          // value lives at slot k
      return nullptr;
   }
}  // namespace psitri::wtree
