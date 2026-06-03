#pragma once
// psitri::wtree — the one word-tree engine (clean rewrite).
//
// Design (doc/node_design_patterns.md §H): ONE SAL `node_type` tag = routing-tier ×
// stem-width matrix {tiny,medium,big}×{u32,u16}; `height` = leaf(0)/inner(>0); the
// N+1 slot column holds child ptr_address (inner) or value-slots (leaf). Distinct SAL
// types per tag via 3-tier CRTP (node_base / node_form<Derived> / concrete) so a
// wrong-width stem read is a COMPILE error.
//
// HARD RULES (node_design_patterns.md): no heap on hot paths; ptr_address(0) is a
// REAL address, null = kNoChild (0xFFFFFFFF) — never leave a slot 0; benchmark only
// build/release.
//
// Stage A: header + CRTP + `tiny` (u32, linear count_le), leaf (fixed 8-B value) +
// inner. tiny16/medium/big and the varlen value model land in later stages.

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>

#include <psitri/node/node.hpp>          // psitri::node (12-B SAL header), node_type, ptr_address
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <ucc/round.hpp>

namespace psitri::wtree
{
   using sal::ptr_address;
   using session_t = sal::allocator_session;

   // Per-thread session bridge (node_design_patterns §D): set at the top of each
   // public op so node free-functions don't thread a session param.
   inline thread_local session_t* g_session = nullptr;
   inline session_t&              session() noexcept { return *g_session; }
   template <typename T = sal::alloc_header>
   inline T* deref(ptr_address a) noexcept { return session().template get_ptr<T>(a); }

   // ptr_address(0) is a VALID allocatable address; the null/empty sentinel is the
   // reserved 0xFFFFFFFF. Never use 0 / a default-constructed address as "absent".
   inline constexpr ptr_address kNoChild = sal::null_ptr_address;
   inline bool is_ext_slot(ptr_address c) noexcept { return !sal::is_reserved_ptr_address(c); }

   inline constexpr std::size_t kCacheLine   = 128;
   inline constexpr uint16_t    kStemsPerCl  = uint16_t(kCacheLine / 4);   // 32 u32 stems / cacheline
   inline uint32_t cl_round(uint32_t b) noexcept { return uint32_t(ucc::round_up_multiple<kCacheLine>(b)); }
   inline uint16_t ndivs(uint16_t n) noexcept { return uint16_t((n + kStemsPerCl - 1) / kStemsPerCl); }

   // ── Tier 1: non-template base (form-independent header) ───────────────
   // Layout: [psitri::node 12B][hpf u16][num_stems u16][cap u16]
   //         [prefix: prefix_len u32 words] then form-specific regions (cacheline-aligned).
   //
   // height / prefix_len / flags share one u16 via bitfields. On little-endian (our
   // target) the first-declared field gets the LOW bits, so is_leaf is a cheap mask on
   // height_. NOT a portable on-disk layout (endianness flips field order) — fine, the
   // format is single-target. Bit budget: height needs ~3 (depth caps ~4 with 512-fanout
   // inners), flags needs 1 (fixed_values). prefix_len is the only one bounded by DATA
   // (key words consumed here), so it takes the whole surplus (11 bits = up to 2047 words).
   class node_base : public psitri::node
   {
     public:
      uint16_t height_     : 4;   // 12: 0 = leaf, >0 = inner   (low bits → cheap is_leaf)
      uint16_t flags_      : 1;   //     bit0 = fixed_values (leaf)
      uint16_t prefix_len_ : 11;  //     # of u32 prefix words consumed at this node
      uint16_t num_stems_;        // 14: N live routing stems (N+1 slots)
      uint16_t cap_;              // 16: stem CAPACITY (regions sized for cap; grow ≤ cap)

      static constexpr uint16_t kHeaderBytes = 18;
      static constexpr uint16_t kFlagFixedValues = 0x01;
      uint16_t capacity() const noexcept { return cap_; }

      uint8_t  height()       const noexcept { return uint8_t(height_); }
      bool     is_leaf()      const noexcept { return height_ == 0; }
      bool     is_inner()     const noexcept { return height_ != 0; }
      uint16_t num_stems()    const noexcept { return num_stems_; }
      uint16_t n_slots()      const noexcept { return uint16_t(num_stems_ + 1); }
      uint8_t  prefix_len()   const noexcept { return uint8_t(prefix_len_); }
      bool     fixed_values() const noexcept { return flags_ & kFlagFixedValues; }

      uint8_t*       base()       noexcept { return reinterpret_cast<uint8_t*>(this); }
      const uint8_t* base() const noexcept { return reinterpret_cast<const uint8_t*>(this); }

      uint32_t*       prefix()       noexcept { return reinterpret_cast<uint32_t*>(base() + kHeaderBytes); }
      const uint32_t* prefix() const noexcept { return reinterpret_cast<const uint32_t*>(base() + kHeaderBytes); }
      std::span<const uint32_t> prefix_span() const noexcept { return {prefix(), prefix_len()}; }

      // Offset where the form-specific regions (stems, then slots) begin: after the
      // header + prefix, cacheline-aligned. (Forms with an extension override.)
      uint32_t regions_off() const noexcept
      {
         return cl_round(uint32_t(kHeaderBytes) + uint32_t(prefix_len()) * 4u);
      }

     protected:
      node_base(uint32_t asize, node_type t, ptr_address_seq seq,
                uint8_t height, uint16_t prefix_len, uint8_t flags, uint16_t cap) noexcept
          : psitri::node(asize, t, seq),
            height_(height), flags_(flags), prefix_len_(prefix_len),
            num_stems_(0), cap_(cap)
      {
         assert(height <= 0xF && flags <= 0x1 && prefix_len <= 0x7FF);
      }
   } __attribute__((packed));
   static_assert(sizeof(node_base) == 18, "wtree node_base header must be 18 B");

   // ── Stem search (branchless SIMD-friendly; node_design_patterns §H) ───
   // upper_bound: count of stems ≤ q, in [0,N]. NEVER binary search.
   inline uint16_t count_le_u32(const uint32_t* stems, uint16_t n, uint32_t q) noexcept
   {
      uint16_t c = 0;
      for (uint16_t j = 0; j < n; ++j) c = uint16_t(c + (stems[j] <= q));   // vectorizes
      return c;
   }

   // ── Tier 2: CRTP middle (routing expressed once via Derived's accessors) ──
   template <class Derived>
   class node_form : public node_base
   {
     public:
      // Derived provides: u32_stems()/u16_stems() (its width), children(), count_le(q).
      const Derived* self() const noexcept { return static_cast<const Derived*>(this); }
      Derived*       self()       noexcept { return static_cast<Derived*>(this); }

      // Inner descent: route q to a child slot. (Leaf uses Derived's value lookup.)
      ptr_address route(uint32_t q) const noexcept { return self()->children()[self()->count_le(q)]; }

     protected:
      using node_base::node_base;
   } __attribute__((packed));

   // ── Tier 3: concrete forms ────────────────────────────────────────────
   // `tiny` — u32 stems, branchless linear count_le. Serves leaf (fixed 8-B value
   // records) and inner (child ptr slots), distinguished by height. No extension.
   class tiny : public node_form<tiny>
   {
     public:
      static constexpr node_type type_id     = node_type::wtree_tiny;
      static constexpr uint16_t  kValueBytes = 8;   // Stage A: fixed u64 value

      // regions sized for CAPACITY (slack for in-place grow), each cacheline-aligned:
      //   [divs: ndivs(cap) u32 — first stem of each 32-stem group, Medium-tier index]
      //   [stems: cap u32]  [slots: (cap+1) × slot_stride]
      // num_stems_ are live. The divs index lets count_le touch ~2 cachelines (divs +
      // 1 group) instead of scanning all stems (the Medium routing tier).
      uint32_t divs_off()  const noexcept { return regions_off(); }
      uint32_t stems_off() const noexcept { return divs_off() + cl_round(uint32_t(ndivs(cap_)) * 4u); }
      uint32_t slots_off() const noexcept { return stems_off() + cl_round(uint32_t(cap_) * 4u); }
      static uint32_t slot_stride(bool leaf) noexcept { return leaf ? kValueBytes : uint32_t(sizeof(ptr_address)); }

      uint32_t*       divs_ptr()       noexcept { return reinterpret_cast<uint32_t*>(base() + divs_off()); }
      const uint32_t* divs_ptr() const noexcept { return reinterpret_cast<const uint32_t*>(base() + divs_off()); }
      uint32_t*       u32_stems()       noexcept { return reinterpret_cast<uint32_t*>(base() + stems_off()); }
      const uint32_t* u32_stems() const noexcept { return reinterpret_cast<const uint32_t*>(base() + stems_off()); }
      // NOTE: intentionally NO u16_stems() on this form → a u16 read here won't compile.

      // Rebuild the divs index from the live stems (call after a bulk stem rewrite:
      // build, split). Cheap — touches the already-warm stems region.
      void regen_divs() noexcept { update_divs_from(0); }

      // Refresh only the divs whose group's first stem could have changed. An insert at
      // `pos` shifts stems[pos..] right, so divs[j]=stems[j*32] changes only for groups
      // g >= pos/32. Used on in-place insert to avoid rescanning every group.
      void update_divs_from(uint16_t pos) noexcept
      {
         const uint32_t* st = u32_stems();
         uint32_t*       d  = divs_ptr();
         const uint16_t  K  = ndivs(num_stems_);
         for (uint16_t j = uint16_t(pos / kStemsPerCl); j < K; ++j) d[j] = st[uint16_t(j * kStemsPerCl)];
      }

      // inner: children are ptr_address slots.
      ptr_address*       children()       noexcept { return reinterpret_cast<ptr_address*>(base() + slots_off()); }
      const ptr_address* children() const noexcept { return reinterpret_cast<const ptr_address*>(base() + slots_off()); }
      // leaf: value records are fixed kValueBytes columns (slot i = value for stem i).
      uint8_t*       value_at(uint16_t i)       noexcept { return base() + slots_off() + size_t(i) * kValueBytes; }
      const uint8_t* value_at(uint16_t i) const noexcept { return base() + slots_off() + size_t(i) * kValueBytes; }

      // Adaptive upper_bound: linear SIMD scan when small (≤ one cacheline of stems),
      // else the Medium divs-index (scan ndivs dividers → the group → scan that group).
      uint16_t count_le(uint32_t q) const noexcept
      {
         const uint16_t n = num_stems_;
         if (n <= kStemsPerCl) return count_le_u32(u32_stems(), n, q);
         const uint32_t* d  = divs_ptr();
         const uint16_t  K  = ndivs(n);
         uint16_t        gc = 0;
         for (uint16_t j = 0; j < K; ++j) gc = uint16_t(gc + (d[j] <= q));   // group: first div > q
         if (gc == 0) return 0;
         const uint16_t  g  = uint16_t(gc - 1);
         const uint16_t  lo = uint16_t(g * kStemsPerCl);
         const uint16_t  hi = uint16_t(std::min<uint32_t>(uint32_t(g + 1) * kStemsPerCl, n));
         const uint32_t* st = u32_stems();
         uint16_t        off = 0;
         for (uint16_t j = lo; j < hi; ++j) off = uint16_t(off + (st[j] <= q));   // scan one group
         return uint16_t(lo + off);
      }

      // Leaf point lookup (u32 key → value bytes or nullptr). stems[k-1]==q ⇒ value
      // for that key lives at slot k (slot 0 = terminal). Fixed kValueBytes record.
      const uint8_t* find_value(uint32_t q) const noexcept
      {
         const uint16_t k = count_le(q);
         if (k > 0 && u32_stems()[k - 1] == q) return value_at(k);
         return nullptr;
      }

      // ── alloc_size ↔ ctor (node_design_patterns §A). Regions sized for `cap`. ──
      static uint32_t alloc_size(uint8_t height, uint16_t prefix_len, uint16_t cap) noexcept
      {
         const uint32_t d_off = cl_round(uint32_t(kHeaderBytes) + uint32_t(prefix_len) * 4u);
         const uint32_t s_off = d_off + cl_round(uint32_t(ndivs(cap)) * 4u);   // divs index
         const uint32_t sl_off = s_off + cl_round(uint32_t(cap) * 4u);
         const uint32_t slot_bytes = uint32_t(cap + 1) * slot_stride(height == 0);
         return sl_off + cl_round(slot_bytes);
      }
      // LEAF fill ctor (alloc_size↔ctor contract): regions sized for `cap`, keys live.
      // stems = keys; slot[i+1] = vals[i] (slot 0 = terminal, unused at Stage A).
      tiny(uint32_t asize, ptr_address_seq seq, uint8_t height, uint16_t cap,
           std::span<const uint32_t> keys, std::span<const uint64_t> vals) noexcept
          : node_form<tiny>(asize, type_id, seq, height, 0, kFlagFixedValues, cap)
      {
         num_stems_ = uint16_t(keys.size());
         uint32_t* st = u32_stems();
         for (uint16_t i = 0; i < num_stems_; ++i) st[i] = keys[i];
         for (uint16_t i = 0; i < num_stems_; ++i)
            std::memcpy(value_at(uint16_t(i + 1)), &vals[i], kValueBytes);
         regen_divs();
      }
      // INNER fill ctor: regions sized for `cap`; stems = divs; children = kids (divs+1).
      tiny(uint32_t asize, ptr_address_seq seq, uint8_t height, uint16_t cap,
           std::span<const uint32_t> divs, std::span<const ptr_address> kids) noexcept
          : node_form<tiny>(asize, type_id, seq, height, 0, uint8_t(0), cap)
      {
         num_stems_ = uint16_t(divs.size());
         uint32_t* st = u32_stems();
         for (uint16_t i = 0; i < num_stems_; ++i) st[i] = divs[i];
         ptr_address* ch = children();
         for (uint16_t i = 0; i <= num_stems_; ++i) ch[i] = kids[i];
         regen_divs();
      }

      // ── SAL lifecycle ──────────────────────────────────────────────────
      bool pf_second_cl() const noexcept { return true; }
      uint32_t cow_size() const noexcept { return size(); }
      void copy_to(sal::alloc_header* dst) const noexcept
      {
         std::memcpy(dst, this, size());
      }
      void compact_to(sal::alloc_header* dst) const noexcept { copy_to(dst); }
      void destroy(const sal::allocator_session_ptr& s) const noexcept
      {
         if (is_leaf()) return;                       // fixed-value leaf owns no ext children
         const ptr_address* c = children();
         const uint16_t ns = n_slots();
         for (uint16_t i = 0; i < ns; ++i)
            if (is_ext_slot(c[i])) s->release(c[i]);
      }
      void visit_children(const std::function<void(ptr_address)>& v) const noexcept
      {
         if (is_leaf()) return;
         const ptr_address* c = children();
         const uint16_t ns = n_slots();
         for (uint16_t i = 0; i < ns; ++i)
            if (is_ext_slot(c[i])) v(c[i]);
      }
   } __attribute__((packed));

   // ── Dispatch (switch on tag once → monomorphic typed call) ────────────
   template <class F>
   inline auto with_node(node_base* nd, F&& f)
   {
      switch (nd->type())
      {
         case node_type::wtree_tiny: return f(static_cast<tiny*>(nd));
         // tiny16/medium/medium16/big/big16 land in later stages.
         default: return f(static_cast<tiny*>(nd));   // Stage A: only tiny exists
      }
   }

}  // namespace psitri::wtree
