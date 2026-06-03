#pragma once
// psitri::wtree public surface (Stage A: single tiny LEAF build + point find).
// Inner routing, splits, the tier×width matrix, and the varlen value model land in
// later stages. This slice exists to compile + smoke-test the typed-node foundation.

#include <psitri/wtree/node.hpp>

namespace psitri::wtree
{
   // Tiny capacity: a tiny node holds up to kTinyCap stems before it splits (Stage A
   // has only the tiny form; Medium/Big absorb larger single nodes in later stages).
   inline constexpr uint16_t kTinyCap = 256;

   // Build a `tiny` LEAF (fill ctor does the work). keys sorted+distinct, vals parallel.
   inline ptr_address build_tiny_leaf(std::span<const uint32_t> keys,
                                      std::span<const uint64_t> vals) noexcept
   {
      const uint32_t sz = tiny::alloc_size(0, 0, uint16_t(keys.size()));
      return session().alloc<tiny>(sz, sal::alloc_hint{}, uint8_t(0), keys, vals);
   }

   // Build a `tiny` INNER over `divs` + `kids` (kids.size()==divs+1). height = child+1.
   inline ptr_address build_tiny_inner(uint8_t height, std::span<const uint32_t> divs,
                                       std::span<const ptr_address> kids) noexcept
   {
      const uint32_t sz = tiny::alloc_size(height, 0, uint16_t(divs.size()));
      return session().alloc<tiny>(sz, sal::alloc_hint{}, height, divs, kids);
   }

   // ── Incremental insert (unique; remake via realloc per the checklist) ─
   // realloc reuses the node's ptr_address (children transfer, old bytes record_freed
   // WITHOUT destroy → no retain/release, no double-free), so the parent never
   // repoints on a non-split; a split reuses the id for the LEFT half + a fresh id for
   // the right (checklist C0 unique-split).
   struct ins_result { bool split; ptr_address addr; uint32_t div; ptr_address right; };
   inline ins_result r_inplace(ptr_address a) noexcept { return {false, a, 0, kNoChild}; }
   inline ins_result r_split(ptr_address l, uint32_t d, ptr_address r) noexcept { return {true, l, d, r}; }

   using node_ref = sal::smart_ref<sal::alloc_header>;

   inline ins_result leaf_insert(node_ref& nref, uint32_t key, uint64_t val) noexcept
   {
      const tiny*     leaf = static_cast<const tiny*>(nref.obj());
      const uint16_t  n    = leaf->num_stems();
      const uint32_t* st   = leaf->u32_stems();
      const uint16_t  pos  = leaf->count_le(key);
      const bool      upd  = (pos > 0 && st[pos - 1] == key);

      uint32_t k2[kTinyCap + 2];
      uint64_t v2[kTinyCap + 2];
      uint16_t m = 0;
      for (uint16_t j = 0; j < n; ++j)
      {
         k2[m] = st[j];
         std::memcpy(&v2[m], leaf->value_at(uint16_t(j + 1)), tiny::kValueBytes);
         ++m;
      }
      if (upd)
         v2[pos - 1] = val;                          // replace value, count unchanged
      else
      {
         for (uint16_t j = m; j > pos; --j) { k2[j] = k2[j - 1]; v2[j] = v2[j - 1]; }
         k2[pos] = key; v2[pos] = val; ++m;
      }

      if (m <= kTinyCap)                             // fits → remake in place (same id)
      {
         const uint32_t sz = tiny::alloc_size(0, 0, m);
         auto ref = session().realloc<tiny>(nref, sz, uint8_t(0),
                                            std::span<const uint32_t>{k2, m},
                                            std::span<const uint64_t>{v2, m});
         return r_inplace(ref.address());
      }
      // overflow → split: LEFT reuses leaf's id, RIGHT is fresh; divider = R's first key.
      const uint16_t h   = uint16_t(m / 2);
      const uint32_t szL = tiny::alloc_size(0, 0, h);
      auto Lref = session().realloc<tiny>(nref, szL, uint8_t(0),
                                          std::span<const uint32_t>{k2, h},
                                          std::span<const uint64_t>{v2, h});
      const ptr_address R = build_tiny_leaf(std::span<const uint32_t>{k2 + h, size_t(m - h)},
                                            std::span<const uint64_t>{v2 + h, size_t(m - h)});
      return r_split(Lref.address(), k2[h], R);
   }

   // Absorb a child split (left,div,right) into this inner at slot i (ch[i] was the
   // child, now == left/reused). Remake in place; split the inner if it overflows.
   inline ins_result inner_absorb(node_ref& nref, uint16_t i, ptr_address left,
                                  uint32_t div, ptr_address right) noexcept
   {
      const tiny*        inner = static_cast<const tiny*>(nref.obj());
      const uint16_t     n     = inner->num_stems();
      const uint8_t      height = inner->height_;
      const uint32_t*    st    = inner->u32_stems();
      const ptr_address* ch    = inner->children();

      uint32_t    s2[kTinyCap + 2];
      ptr_address c2[kTinyCap + 3];
      uint16_t sn = 0, cn = 0;
      for (uint16_t j = 0; j < i; ++j) s2[sn++] = st[j];
      s2[sn++] = div;
      for (uint16_t j = i; j < n; ++j) s2[sn++] = st[j];
      for (uint16_t j = 0; j < i; ++j) c2[cn++] = ch[j];
      c2[cn++] = left; c2[cn++] = right;
      for (uint16_t j = uint16_t(i + 1); j <= n; ++j) c2[cn++] = ch[j];   // sn=n+1, cn=n+2

      if (sn <= kTinyCap)                            // fits → remake in place (children transfer)
      {
         const uint32_t sz = tiny::alloc_size(height, 0, sn);
         auto ref = session().realloc<tiny>(nref, sz, height,
                                            std::span<const uint32_t>{s2, sn},
                                            std::span<const ptr_address>{c2, cn});
         return r_inplace(ref.address());
      }
      // overflow → split inner: promote s2[hh]; LEFT reuses id, RIGHT fresh.
      const uint16_t hh  = uint16_t(sn / 2);
      const uint32_t szL = tiny::alloc_size(height, 0, hh);
      auto Lref = session().realloc<tiny>(nref, szL, height,
                                          std::span<const uint32_t>{s2, hh},
                                          std::span<const ptr_address>{c2, size_t(hh) + 1});
      const ptr_address Rr = build_tiny_inner(height,
                                              std::span<const uint32_t>{s2 + hh + 1, size_t(sn - hh - 1)},
                                              std::span<const ptr_address>{c2 + hh + 1, size_t(cn - hh - 1)});
      return r_split(Lref.address(), s2[hh], Rr);
   }

   inline ins_result insert_rec(node_ref& nref, uint32_t key, uint64_t val) noexcept
   {
      const tiny* nd = static_cast<const tiny*>(nref.obj());
      if (nd->is_leaf()) return leaf_insert(nref, key, val);
      const uint16_t    i     = nd->count_le(key);
      const ptr_address child = nd->children()[i];
      node_ref          cref  = session().get_ref<sal::alloc_header>(child);
      const ins_result  r     = insert_rec(cref, key, val);
      if (!r.split) return r_inplace(nref.address());        // child kept its id → no repoint
      return inner_absorb(nref, i, r.addr, r.div, r.right);  // child split → absorb divider+right
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
      node_ref         nref = session().get_ref<sal::alloc_header>(root);
      const ins_result r    = insert_rec(nref, key, val);
      if (!r.split) return r.addr;                           // == root (reused id)
      const uint8_t     newh    = uint8_t(deref<node_base>(r.addr)->height_ + 1);
      const ptr_address kids[2] = {r.addr, r.right};
      return build_tiny_inner(newh, std::span<const uint32_t>{&r.div, 1}, std::span<const ptr_address>{kids, 2});
   }

   // Descend from the root to the hosting leaf (routing by the u32 key at each inner),
   // then point-lookup. Returns the value bytes or nullptr.
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
}  // namespace psitri::wtree
