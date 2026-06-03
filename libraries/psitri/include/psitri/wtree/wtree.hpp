#pragma once
// psitri::wtree public surface (Stage A: tiny u32 B-tree — build, find, incremental
// insert with in-place slack + split). Inner tiers (medium/big), u16 widths, and the
// varlen value model land in later stages.

#include <psitri/wtree/node.hpp>

namespace psitri::wtree
{
   // Nodes are built at capacity so inserts grow IN PLACE (shift) until full, then split.
   // Leaf cap is kept small: leaf inserts pay an O(live/2) shift, so a tighter leaf means
   // far less memmove per insert (the dominant insert cost) — the deeper tree it implies
   // costs little because routing is divs-indexed (~2 cachelines/level). Inners are mutated
   // only on a child split (rare), so they run wide for high fanout / a shallow tree.
   inline constexpr uint16_t kLeafCap  = 128;
   inline constexpr uint16_t kInnerCap = 512;
   inline constexpr uint16_t kTinyCap  = 512;   // max — sizes the on-stack split scratch

   // Build a `tiny` LEAF (fill ctor does the work) with `cap` slack. keys sorted+distinct.
   inline ptr_address build_tiny_leaf(std::span<const uint32_t> keys,
                                      std::span<const uint64_t> vals,
                                      uint16_t cap = kLeafCap) noexcept
   {
      const uint32_t sz = tiny::alloc_size(0, 0, cap);
      return session().alloc<tiny>(sz, sal::alloc_hint{}, uint8_t(0), cap, keys, vals);
   }

   // Build a `tiny` INNER over `divs` + `kids` (kids.size()==divs+1), `cap` slack.
   inline ptr_address build_tiny_inner(uint8_t height, std::span<const uint32_t> divs,
                                       std::span<const ptr_address> kids,
                                       uint16_t cap = kInnerCap) noexcept
   {
      const uint32_t sz = tiny::alloc_size(height, 0, cap);
      return session().alloc<tiny>(sz, sal::alloc_hint{}, height, cap, divs, kids);
   }

   // Descend root → hosting leaf (routing by the u32 key), then point-lookup.
   inline const uint8_t* find(ptr_address root, uint32_t key) noexcept
   {
      ptr_address cur = root;
      for (;;)
      {
         node_base* nd = deref<node_base>(cur);
         if (nd->is_leaf())
            return with_node(nd, [&](auto* t) { return t->find_value(key); });
         cur = with_node(nd, [&](auto* t) { return t->route(key); });
      }
   }

   // ── Incremental insert (unique) ──────────────────────────────────────
   // Nodes carry cap slack → insert grows IN PLACE (shift); a full node splits,
   // rewriting THIS node to the left half in place (id kept → parent skips repoint)
   // and allocating a fresh right (checklist C0 unique-split). The only allocator
   // traffic per insert is on a split. NOTE Stage A: no MVCC/sync, so nodes are
   // writable in place (const_cast); COW-on-write is added with the snapshot model.
   struct ins_result { bool split; ptr_address addr; uint32_t div; ptr_address right; };
   inline ins_result r_inplace(ptr_address a) noexcept { return {false, a, 0, kNoChild}; }
   inline ins_result r_split(ptr_address l, uint32_t d, ptr_address r) noexcept { return {true, l, d, r}; }

   // Stage A descends with raw deref (no per-level atomic refcount): no MVCC/sync and
   // single-threaded in-place mutation, so the tree is held alive by `root` alone. (The
   // snapshot/COW model threads a smart_ref here instead.)
   inline ins_result leaf_insert(ptr_address addr, uint32_t key, uint64_t val) noexcept
   {
      tiny*          leaf = deref<tiny>(addr);
      const uint16_t n    = leaf->num_stems_;
      uint32_t*      st   = leaf->u32_stems();
      const uint16_t pos  = leaf->count_le(key);
      if (pos > 0 && st[pos - 1] == key)                       // update in place
      {
         std::memcpy(leaf->value_at(pos), &val, tiny::kValueBytes);
         return r_inplace(addr);
      }
      if (n < leaf->cap_)                                      // in-place insert (shift)
      {
         for (uint16_t j = n; j > pos; --j) st[j] = st[j - 1];
         st[pos] = key;
         std::memmove(leaf->value_at(uint16_t(pos + 2)), leaf->value_at(uint16_t(pos + 1)),
                      size_t(n - pos) * tiny::kValueBytes);
         std::memcpy(leaf->value_at(uint16_t(pos + 1)), &val, tiny::kValueBytes);
         leaf->num_stems_ = uint16_t(n + 1);
         leaf->update_divs_from(pos);
         return r_inplace(addr);
      }
      // full → split. Materialize merged, rewrite THIS node = left half in place, alloc right.
      uint32_t k2[kTinyCap + 2];
      uint64_t v2[kTinyCap + 2];
      uint16_t m = 0;
      for (uint16_t j = 0; j < n; ++j)
      {
         k2[m] = st[j];
         std::memcpy(&v2[m], leaf->value_at(uint16_t(j + 1)), tiny::kValueBytes);
         ++m;
      }
      for (uint16_t j = m; j > pos; --j) { k2[j] = k2[j - 1]; v2[j] = v2[j - 1]; }
      k2[pos] = key; v2[pos] = val; ++m;
      const uint16_t h = uint16_t(m / 2);
      for (uint16_t j = 0; j < h; ++j) st[j] = k2[j];                       // left half in place
      for (uint16_t j = 0; j < h; ++j) std::memcpy(leaf->value_at(uint16_t(j + 1)), &v2[j], tiny::kValueBytes);
      leaf->num_stems_ = h;
      leaf->regen_divs();
      const ptr_address R = build_tiny_leaf(std::span<const uint32_t>{k2 + h, size_t(m - h)},
                                            std::span<const uint64_t>{v2 + h, size_t(m - h)});
      return r_split(addr, k2[h], R);
   }

   // Absorb a child split (left==reused child id, div, right) into this inner at slot i.
   inline ins_result inner_absorb(ptr_address addr, uint16_t i, ptr_address left,
                                  uint32_t div, ptr_address right) noexcept
   {
      tiny*          inner = deref<tiny>(addr);
      const uint16_t n     = inner->num_stems_;
      uint32_t*      st    = inner->u32_stems();
      ptr_address*   ch    = inner->children();
      if (n < inner->cap_)                                     // in-place insert (shift)
      {
         for (uint16_t j = n; j > i; --j) st[j] = st[j - 1];
         st[i] = div;
         for (uint16_t j = uint16_t(n + 1); j > i + 1; --j) ch[j] = ch[j - 1];
         ch[i] = left; ch[uint16_t(i + 1)] = right;
         inner->num_stems_ = uint16_t(n + 1);
         inner->update_divs_from(i);
         return r_inplace(addr);
      }
      // full → split inner: promote s2[hh]; rewrite this = left in place, alloc right.
      uint32_t    s2[kTinyCap + 2];
      ptr_address c2[kTinyCap + 3];
      uint16_t sn = 0, cn = 0;
      for (uint16_t j = 0; j < i; ++j) s2[sn++] = st[j];
      s2[sn++] = div;
      for (uint16_t j = i; j < n; ++j) s2[sn++] = st[j];
      for (uint16_t j = 0; j < i; ++j) c2[cn++] = ch[j];
      c2[cn++] = left; c2[cn++] = right;
      for (uint16_t j = uint16_t(i + 1); j <= n; ++j) c2[cn++] = ch[j];     // sn=n+1, cn=n+2
      const uint8_t  height = inner->height_;
      const uint16_t hh     = uint16_t(sn / 2);
      for (uint16_t j = 0; j < hh; ++j) st[j] = s2[j];                      // left stems
      for (uint16_t j = 0; j <= hh; ++j) ch[j] = c2[j];                     // left children (hh+1)
      inner->num_stems_ = hh;
      inner->regen_divs();
      const ptr_address Rr = build_tiny_inner(height,
                                              std::span<const uint32_t>{s2 + hh + 1, size_t(sn - hh - 1)},
                                              std::span<const ptr_address>{c2 + hh + 1, size_t(cn - hh - 1)});
      return r_split(addr, s2[hh], Rr);
   }

   inline ins_result insert_rec(ptr_address addr, uint32_t key, uint64_t val) noexcept
   {
      const tiny* nd = deref<tiny>(addr);
      if (nd->is_leaf()) return leaf_insert(addr, key, val);
      const uint16_t    i     = nd->count_le(key);
      const ptr_address child = nd->children()[i];
      const ins_result  r     = insert_rec(child, key, val);
      if (!r.split) return r_inplace(addr);                   // child kept its id → no repoint
      return inner_absorb(addr, i, r.addr, r.div, r.right);
   }

   // Insert one (u32 key, u64 value); returns the (possibly new) root.
   inline ptr_address insert(ptr_address root, uint32_t key, uint64_t val) noexcept
   {
      if (root == kNoChild)
      {
         const uint32_t k = key;
         const uint64_t v = val;
         return build_tiny_leaf(std::span<const uint32_t>{&k, 1}, std::span<const uint64_t>{&v, 1});
      }
      const ins_result r = insert_rec(root, key, val);
      if (!r.split) return r.addr;                            // == root (id kept)
      const uint8_t     newh    = uint8_t(deref<node_base>(r.addr)->height_ + 1);
      const ptr_address kids[2] = {r.addr, r.right};
      return build_tiny_inner(newh, std::span<const uint32_t>{&r.div, 1}, std::span<const ptr_address>{kids, 2});
   }
}  // namespace psitri::wtree
