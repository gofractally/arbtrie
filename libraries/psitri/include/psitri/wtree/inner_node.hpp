#pragma once

// inner_node — v3 routing page.
//
// Same conceptual shape as leaf_node (common_header dispatch, stems +
// child_ptrs growing from opposite ends), but:
//   - Header is 128 B (one cacheline), reserving 96 B for the
//     INDEXED-page interp + 2-bit correction machinery (recip,
//     corrections_packed[88], base32, max32, global_shift).
//   - Page total = cap_in_cls · 128 (header + index region).  Active
//     stems/children region = (cap_in_cls − 1) · 128.  Minimum
//     cap_in_cls = 2.
//
// Index layout (past byte 128):
//   stems[K]   u32  → grows forward from byte 128
//   …
//   child_ptrs[K+1]  void*  ← grows backward from page end
//
// Lookup dispatch:
//   small inners (num_keys < kGateTrivialBelow): simple SIMD count_le
//   wide inners (num_keys ≥ kGateTrivialBelow): interp + 2-bit
//     correction predicts the right ±3 cacheline window, then SIMD
//     count_le within ~32 stems (1-2 CLs) instead of scanning all
//     stems.

#include "leaf_node.hpp"          // common_header, upsert_result, kCacheLine, etc.
#include "apgm_predictor.hpp"
#include "apply.hpp"             // apply<mode>(ref, op) dispatcher

#include <functional>            // std::function (SAL visit_children vtable)

namespace psitri::wtree {

// ─── Diagnostic counters (predictor accuracy + correction stats) ──
inline size_t g_inner_lookups        = 0;
inline size_t g_inner_walk_fwd_steps = 0;   // total extra CL scans walking forward
inline size_t g_inner_walk_bwd_steps = 0;   // total extra CL scans walking backward
inline size_t g_inner_walks_any      = 0;   // lookups that walked at least once
inline size_t g_inner_max_walk       = 0;   // longest walk seen in any single lookup
// Correction-table stats (filled by rebuild_corrections):
inline int    g_inner_max_delta_pos  = 0;   // largest stored delta (positive)
inline int    g_inner_max_delta_neg  = 0;   // largest stored delta (negative; ≤ 0)
inline size_t g_inner_max_excess     = 0;   // largest (c_max - c_min) per bucket
inline size_t g_inner_delta_hist[32] = {0}; // histogram of |delta|, capped at 31
inline size_t g_inner_excess_hist[8] = {0}; // histogram of excess in [0..7]
inline void inner_reset_counters() noexcept {
    g_inner_lookups         = 0;
    g_inner_walk_fwd_steps  = 0;
    g_inner_walk_bwd_steps  = 0;
    g_inner_walks_any       = 0;
    g_inner_max_walk        = 0;
    g_inner_max_delta_pos   = 0;
    g_inner_max_delta_neg   = 0;
    g_inner_max_excess      = 0;
    for (auto& v : g_inner_delta_hist)  v = 0;
    for (auto& v : g_inner_excess_hist) v = 0;
}

// Crossover where the APGM predictor beats full SIMD scan: empirically
// ~96-128 stems on M-series.  Smaller inners use the count_le scalar
// fast path that doesn't need a predictor.
inline constexpr uint16_t kInnerGateTrivialBelow  = 64;

// ─── inner_node header (128 B = 1 cacheline) ──────────────────────
//
// v4 replacement: instead of v3's single-regression + 4-bit corrections
// predictor (recip + base32 + shift_amt + corrections_packed = 105 B),
// we use a piecewise-linear predictor in struct-of-arrays layout
// (apgm_header = 96 B; see inner_apgm_predictor.hpp).  Frees up 9 B
// for explicit padding / future fields.
struct inner_node : public node {
    static constexpr node_type type_id = node_type::wtree_inner;
    // Page total = cap_in_cls * 128 bytes.
    static uint32_t alloc_size(uint16_t cap_in_cls) noexcept {
        return uint32_t(cap_in_cls) * uint32_t(kCacheLine);
    }

    uint16_t      cap_in_cls;      // 12..13  (total page size in 128-B cls)
    uint16_t      tail_alloc_pos;  // 14..15  (reserved for future heap on inner)
    uint16_t      num_keys;        // 16..17  (K = divider count; K+1 children)
    uint8_t       prefix_len;      // 18      (0 = no prefix; 1 = inline u32; >1 = tail-stored)
    uint8_t       _pad0;           // 19
    uint32_t      prefix_data;     // 20..23  (inline u32 when prefix_len==1; tail offset when >1)
    uint8_t       _pad1[8];        // 24..31  (reserved — future smallest-stem cache, etc.)
    apgm_header   predictor;       // 32..127  (96 B SoA piecewise-linear predictor)

    // SAL placement ctor: (asize, addr_seq) injected by the allocator.
    inner_node(uint32_t asize, ptr_address_seq seq, uint16_t cap_in_cls) noexcept;  // after inner_init

    // ── SAL destructor vtable (registered via register_type_ops) ──────
    // destroy() releases every child ptr_address — this is the proper
    // recursive free the compactor relies on. On relocate/grow/split the
    // children are MOVED to the new node(s) and the old node is neutered
    // (inner_free_moved) so its destroy() releases nothing (no double-free).
    void destroy(const sal::allocator_session_ptr& session) const noexcept;       // after inner_child_addr
    void visit_children(const std::function<void(sal::ptr_address)>& v) const noexcept;

    // COW copy: only the ACTIVE regions — header+stems at the front and the
    // live children at the tail — skipping the free gap between them. Same-size
    // dst (cow_size stays full). Defined out-of-line (needs inner_children_base).
    void copy_to(sal::alloc_header* dst) const noexcept;

    // ── apply() contract for op_insert (insert one divider+child at pos) ──
    // alloc_size plans into op (result cap + copy mode), 0 if it would exceed
    // the max page (caller splits). The ctor (copy/grow path) and apply
    // (in-place path) share splice_insert<bool InPlace> so the compiler emits
    // the optimal branch-free form for each — no runtime src==dst check.
    static uint32_t alloc_size(const inner_node& src, struct op_insert& op) noexcept;
    inner_node(uint32_t asize, ptr_address_seq seq,
               const inner_node& src, struct op_insert& op) noexcept;  // defined at EOF
    void apply(struct op_insert& op) noexcept;                          // defined at EOF

    // ── apply() contract for op_replace_slot (multi-stem slot replace) ──
    static uint32_t alloc_size(const inner_node& src, struct op_replace_slot& op) noexcept;
    inner_node(uint32_t asize, ptr_address_seq seq,
               const inner_node& src, struct op_replace_slot& op) noexcept;  // at EOF
    void apply(struct op_replace_slot& op) noexcept;                         // at EOF
};
static_assert(sizeof(inner_node) == 128, "inner_node header must fit one cacheline");
static_assert(offsetof(inner_node, cap_in_cls)          == 12);
static_assert(offsetof(inner_node, tail_alloc_pos)      == 14);
static_assert(offsetof(inner_node, num_keys)            == 16);
static_assert(offsetof(inner_node, prefix_len)          == 18);
static_assert(offsetof(inner_node, prefix_data)         == 20);
static_assert(offsetof(inner_node, predictor)           == 32);

inline constexpr size_t kInnerHeaderBytes = sizeof(inner_node);   // 128

// Op work-ticket for "insert one (divider, child) at branch position pos".
// Inputs are filled by the caller; the plan fields are filled by
// inner_node::alloc_size and read back by the ctor / apply (compute-once).
struct op_insert {
    // inputs
    uint16_t    pos     = 0;            // insert divider at stems[pos], child at children[pos+1]
    uint32_t    divider = 0;
    ptr_address child   = kNoChild;
    // plan (filled by alloc_size)
    uint16_t    result_cap = 0;         // cap_in_cls of the result
    copy_mode   copy       = copy_mode::fast;
};

// Op work-ticket for "replace child[pos] with a sorted run of (n_stems+1)
// children and n_stems dividers" (split-bubble / promote absorb). Result has
// K + n_stems stems. alloc_size returns 0 if it would exceed the max page
// (caller B-tree-splits). Going through apply() makes the GROW path realloc
// same-id (address-stable) so the parent can skip the COW.
struct op_replace_slot {
    // inputs
    uint16_t              pos           = 0;
    uint16_t              n_stems       = 0;
    const uint32_t*       repl_stems    = nullptr;   // [n_stems]
    common_header* const* repl_children = nullptr;   // [n_stems + 1]
    // plan (filled by alloc_size)
    uint16_t              result_cap    = 0;
    copy_mode             copy          = copy_mode::fast;
};

// ─── Pointer-interconvertibility helpers ──────────────────────────
inline common_header* as_header(inner_node* p)             noexcept { return reinterpret_cast<common_header*>(p); }
inline const common_header* as_header(const inner_node* p) noexcept { return reinterpret_cast<const common_header*>(p); }
inline inner_node* as_inner(common_header* h) noexcept {
    assert(h->type() == node_type::wtree_inner);
    return reinterpret_cast<inner_node*>(h);
}
inline const inner_node* as_inner(const common_header* h) noexcept {
    assert(h->type() == node_type::wtree_inner);
    return reinterpret_cast<const inner_node*>(h);
}
// upsert_result builders taking concrete inner_node*.
inline upsert_result upsert_inplace(inner_node* p) noexcept {
    return upsert_inplace(as_header(p));
}
inline upsert_result upsert_split(inner_node* l, uint32_t div, inner_node* r) noexcept {
    return upsert_split(as_header(l), div, as_header(r));
}

// ─── Inner allocator (SAL via the per-thread active session) ──────
// Allocation runs the inner_node ctor (header + inner_init fields). Returns
// the deref'd mutable pointer; the ptr_address is recoverable via address().
// inner_free is SHALLOW (children are transferred on grow/split, and the tree
// frees them via the recursive free_subtree_ — matches v4's malloc free).
inline inner_node* inner_alloc_sized(uint16_t cap_in_cls,
                                     ptr_address hint_near = kNoChild) noexcept {
    ptr_address a = (hint_near == kNoChild)
        ? wsession().alloc<inner_node>(alloc_hint{}, cap_in_cls)
        : wsession().alloc<inner_node>(alloc_hint{&hint_near, 1}, cap_in_cls);
    return wderef<inner_node>(a);
}
inline void inner_free(inner_node* p) noexcept { wsession().release(p->address()); }

// Make an inner writable before in-place mutation (see cow_leaf). In-place
// when unique+writable (hot path, no copy); copy_on_write to fresh writable
// bytes when read-only (synced) or shared — ptr_address unchanged (CB moves).
inline inner_node* cow_inner(inner_node* p) noexcept {
    auto        ref = wsession().get_ref<inner_node>(p->address());
    inner_node* w   = p;
    ref.modify([&](inner_node* n) { w = n; });
    return w;
}

// Clone an inner to a FRESH ptr_address (new control block), copying the
// payload. Unlike cow_inner (which preserves the address via a CB move), this
// FORKS: the original is untouched and stays valid for a snapshot. Used only by
// the shared-mode upsert path. Children are NOT retained here — the caller
// retains the off-path siblings it keeps (the on-path child is path-copied).
// inner_node::copy_to clobbers dst's 12-byte SAL header with src's (right for a
// COW-move, wrong for a fork), so save/restore dst's header around it.
inline inner_node* clone_inner_fresh(const inner_node* src) noexcept {
    inner_node* dst = inner_alloc_sized(src->cap_in_cls, src->address());
    uint8_t     hdr[sizeof(sal::alloc_header)];
    std::memcpy(hdr, dst, sizeof(hdr));
    src->copy_to(dst);
    std::memcpy(dst, hdr, sizeof(hdr));
    return dst;
}

// ─── Capacity ─────────────────────────────────────────────────────
//
// Stage 2 (SAL): children are 4-byte ptr_addresses (was u48 raw pointers).
// Reads/writes are exact 4-byte loads/stores, so no trailing pad is needed.
// Denser than the v4 u48 layout.
//
// Active index region = (cap_in_cls − 1) · 128 bytes.
//   K · 4 (stems) + (K+1) · 4 (children) ≤ (cap_in_cls − 1) · 128
//   K · 8 + 4 ≤ (cap_in_cls − 1) · 128
//   K ≤ ((cap_in_cls − 1) · 128 − 4) / 8
inline constexpr uint16_t kInnerMaxPageCls = 512;
inline constexpr size_t   kInnerChildBytes = sizeof(ptr_address);  // 4 (was u48)
inline constexpr size_t   kInnerChildPad   = 0;                    // exact 4B reads

inline constexpr uint16_t inner_cap_for(uint16_t cap_in_cls) noexcept {
    if (cap_in_cls < 2) return 0;
    const int32_t avail = (int32_t(cap_in_cls) - 1) * int32_t(kCacheLine)
                          - int32_t(kInnerChildBytes + kInnerChildPad);
    if (avail <= 0) return 0;
    return uint16_t(uint32_t(avail) / (sizeof(uint32_t) + kInnerChildBytes));
}
inline constexpr uint16_t kInnerCapMin    = inner_cap_for(3);                          // 35 stems
inline constexpr uint16_t kInnerCapFull   = inner_cap_for(uint16_t(kLeafPageCls));     // 396 (4 KB)

inline uint16_t inner_cap_in_cls_for(uint16_t num_keys) noexcept {
    const uint32_t need = (sizeof(uint32_t) + kInnerChildBytes) * uint32_t(num_keys)
                        + uint32_t(kInnerChildBytes + kInnerChildPad)
                        + uint32_t(kInnerHeaderBytes);
    uint32_t cls = (need + kCacheLine - 1) / kCacheLine;
    if (cls < 3) cls = 3;
    if (cls > kInnerMaxPageCls) cls = kInnerMaxPageCls;
    return uint16_t(cls);
}

// ─── stems[] / child_ptrs[] accessors ─────────────────────────────
inline uint32_t* inner_stems(inner_node* p) noexcept {
    return reinterpret_cast<uint32_t*>(
        reinterpret_cast<uint8_t*>(p) + kInnerHeaderBytes);
}
inline const uint32_t* inner_stems(const inner_node* p) noexcept {
    return reinterpret_cast<const uint32_t*>(
        reinterpret_cast<const uint8_t*>(p) + kInnerHeaderBytes);
}
// Children layout: [child0:4][child1:4]...[child{cap}:4] (ptr_address each)
// sitting at the END of the page.
inline uint8_t* inner_children_base(inner_node* p) noexcept {
    const uint16_t cap = inner_cap_for(p->cap_in_cls);
    return reinterpret_cast<uint8_t*>(p) + size_t(p->cap_in_cls) * kCacheLine
           - size_t(cap + 1) * kInnerChildBytes - kInnerChildPad;
}
inline const uint8_t* inner_children_base(const inner_node* p) noexcept {
    const uint16_t cap = inner_cap_for(p->cap_in_cls);
    return reinterpret_cast<const uint8_t*>(p) + size_t(p->cap_in_cls) * kCacheLine
           - size_t(cap + 1) * kInnerChildBytes - kInnerChildPad;
}
// Read the raw ptr_address stored at child[pos] (no deref). kNoChild = empty.
inline ptr_address inner_child_addr(const inner_node* p, uint16_t pos) noexcept {
    const uint8_t* slot = inner_children_base(p) + size_t(pos) * kInnerChildBytes;
    ptr_address a;
    std::memcpy(&a, slot, sizeof(a));   // 4-byte ptr_address
    return a;
}
// Read child[pos] as a deref'd node pointer (via the active session).
// Empty slot (kNoChild) → nullptr, so `if (!child)` keeps working.
inline common_header* inner_child(const inner_node* p, uint16_t pos) noexcept {
    const ptr_address a = inner_child_addr(p, pos);
    if (a == kNoChild) return nullptr;
    return wderef<node>(a);
}
// Write child[pos]. nullptr installs an empty (kNoChild) sentinel slot —
// used by the K=1 promotion path to mark a [X+1, next) range with no keys.
inline void inner_set_child(inner_node* p, uint16_t pos, common_header* child) noexcept {
    uint8_t*    slot = inner_children_base(p) + size_t(pos) * kInnerChildBytes;
    ptr_address a    = child ? child->address() : kNoChild;
    std::memcpy(slot, &a, sizeof(a));
}
// Store a raw child ptr_address directly (no deref) — used when relocating
// children between pages (split/replace) where the address is already known.
inline void inner_set_child_addr(inner_node* p, uint16_t pos, ptr_address a) noexcept {
    uint8_t* slot = inner_children_base(p) + size_t(pos) * kInnerChildBytes;
    std::memcpy(slot, &a, sizeof(a));
}
// Drop divider stems[pos] and child[pos+1] (the inverse of inner_insert_after_
// split). Used by remove's leaf-merge: child[pos] now holds the merged node and
// child[pos+1] is gone. Shifts the compacted stems[]/children[] runs left.
inline void inner_remove_entry(inner_node* p, uint16_t pos) noexcept {
    const uint16_t K  = p->num_keys;
    uint32_t*      st = inner_stems(p);
    uint8_t*       ch = inner_children_base(p);
    const uint16_t tail = uint16_t(K - 1 - pos);   // entries after the removed one
    if (tail) {
        std::memmove(st + pos, st + pos + 1, size_t(tail) * sizeof(uint32_t));
        std::memmove(ch + size_t(pos + 1) * kInnerChildBytes,
                     ch + size_t(pos + 2) * kInnerChildBytes,
                     size_t(tail) * kInnerChildBytes);
    }
    p->num_keys = uint16_t(K - 1);
}

// ── SAL destructor vtable definitions ─────────────────────────────
// destroy(): release every real child (skip empty/sentinel slots). This is
// the recursive free SAL/the compactor invoke on final release.
inline void inner_node::destroy(const sal::allocator_session_ptr& session) const noexcept {
    const ptr_address* c   = reinterpret_cast<const ptr_address*>(inner_children_base(this));
    const ptr_address* end = c + (num_keys + 1);
    for (; c < end; ++c)
        if (*c != kNoChild && *c != kSentinelChildAddr) session->release(*c);
}
inline void inner_node::visit_children(
    const std::function<void(sal::ptr_address)>& v) const noexcept {
    const ptr_address* c   = reinterpret_cast<const ptr_address*>(inner_children_base(this));
    const ptr_address* end = c + (num_keys + 1);
    for (; c < end; ++c)
        if (*c != kNoChild && *c != kSentinelChildAddr) v(*c);
}
// Active-only COW copy: header+stems (front) and the (num_keys+1) live
// children (tail), skipping the free gap. Same-size dst, so children sit at
// the same offset (children_base depends only on cap_in_cls, copied in front).
inline void inner_node::copy_to(sal::alloc_header* dst) const noexcept {
    const uint8_t* s = reinterpret_cast<const uint8_t*>(this);
    uint8_t*       d = reinterpret_cast<uint8_t*>(dst);
    const size_t   total = size_t(cap_in_cls) * kCacheLine;
    // Branchless: head (header+stems) + tail (live children), both 64-aligned.
    // thi <= total always (live children <= capacity); an overlap when the gap
    // is <64B copies the same bytes twice (idempotent).
    const size_t   fhi   = ucc::round_up_multiple<64>(kInnerHeaderBytes + size_t(num_keys) * sizeof(uint32_t));
    const uint16_t cap   = inner_cap_for(cap_in_cls);
    const size_t   cbase = total - size_t(cap + 1) * kInnerChildBytes - kInnerChildPad;
    const size_t   tlo   = ucc::round_down_multiple<64>(cbase);
    const size_t   thi   = ucc::round_up_multiple<64>(cbase + size_t(num_keys + 1) * kInnerChildBytes);
    ucc::memcpy_aligned_64byte(d,       s,       fhi);        // head: header + stems
    ucc::memcpy_aligned_64byte(d + tlo, s + tlo, thi - tlo);  // tail: live children
}

// Free an inner whose children have been MOVED to other node(s) (grow/split).
// Neuter it first (num_keys=0, child[0]=empty) so its destroy() — run later by
// the compactor draining the release queue — releases nothing. This is the
// "move + skip the destructor" relocate semantics: the moved children keep
// their single owning reference in the destination node.
inline void inner_free_moved(inner_node* p) noexcept {
    p->num_keys = 0;
    inner_set_child(p, 0, nullptr);   // child[0] = kNoChild
    inner_free(p);
}

// (4-bit corrections accessors removed — APGM replaces the corrections
// table entirely.  See inner_apgm_predictor.hpp for the new storage.)

// ─── Init ────────────────────────────────────────────────────────
//
// Inner pages are ALWAYS allocated full-size (4 KB, cap_in_cls=32).
// This puts children[] at a fixed CL-aligned byte (1536) so the 2-CL
// prefetch in inner_lookup_le_indexed covers every 32-children block
// exactly.  No grow path needed; when an inner fills, it splits.
//
// (Mirrors v2's fixed-size 64 KB inner_radix — same trade-off: a small
// memory waste in early-life root inners in exchange for a single
// layout, fixed offsets, and no per-grow memmove.)
// Only writes the header fields we'll read before they get set by
// rebuild_corrections / insert paths.  Stems past num_keys are never
// read (count_le scans exactly num_keys; scan_cl is bounded by n).
// Children past num_keys are never read.
//
// `cap_in_cls` is the active page size in 128-B cachelines (≥ 3 so
// alignment-rounded cap is non-zero).  Allocator may give a larger
// buffer; this function only initializes the active region.
// Sets only the wtree fields; the 12-byte SAL header (size/type/addr) is set
// by the ctor (= alloc). Safe to call on an already-allocated inner (re-init).
inline void inner_init(inner_node* p, uint16_t cap_in_cls = uint16_t(kLeafPageCls)) noexcept {
    if (cap_in_cls < 3) cap_in_cls = 3;
    p->cap_in_cls     = cap_in_cls;
    p->tail_alloc_pos = 0;
    p->num_keys       = 0;
    p->prefix_len     = 0;   // no self-asserted prefix yet
    p->prefix_data    = 0;
    p->_pad0          = 0;
    std::memset(p->_pad1, 0, sizeof(p->_pad1));
    // Zero-init the predictor block — segment_count=0 means "no
    // predictor built yet, fall back to count_le on lookup."
    std::memset(&p->predictor, 0, sizeof(p->predictor));
}

// SAL placement ctor: header from (asize, seq), then the wtree fields.
inline inner_node::inner_node(uint32_t asize, ptr_address_seq seq,
                              uint16_t cap_in_cls) noexcept
    : node(asize, type_id, seq) {
    inner_init(this, cap_in_cls);
}

// ─── count_le with CL-chunk SIMD + scalar tail ────────────────────
//
// Scans exactly num_keys stems — never over-reads past, so no sentinel
// pre-fill needed.  CL-chunk early-exit (same pattern as leaf_count_lt):
// once a 32-stem CL has any stem > q, the upper-bound is in this CL;
// return immediately.
//
// Used for the small-inner path (n < kInnerGateTrivialBelow=32).  At
// that scale full_cls=0 and the entire scan is the scalar tail; the
// CL loop is dormant but free.
inline uint16_t inner_count_le(const uint32_t* stems, uint16_t num_keys,
                                uint16_t /*cap*/, uint32_t q) noexcept {
    constexpr size_t kStemsPerCL = kCacheLine / sizeof(uint32_t); // 32
    const uint32x4_t tgt = vdupq_n_u32(q);
    const size_t full_cls = size_t(num_keys) / kStemsPerCL;
    const uint32_t* p          = stems;
    const uint32_t* const cl_end  = stems + full_cls * kStemsPerCL;
    const uint32_t* const end     = stems + num_keys;
    for (; p < cl_end; p += kStemsPerCL) {
        int32_t s = 0;
        #pragma unroll
        for (size_t c = 0; c < 8; ++c) {
            const uint32x4_t v = vld1q_u32(p + c * 4);
            const uint32x4_t cc = vcleq_u32(v, tgt);
            s += vaddvq_s32(vreinterpretq_s32_u32(cc));
        }
        const uint32_t k = uint32_t(-s);
        if (k < kStemsPerCL) return uint16_t(uintptr_t(p - stems) + k);
    }
    // Tail: SIMD over remaining mult-of-4 quads (v2 pattern — much
    // faster than scalar for small N where the CL chunks didn't fire).
    int32_t s = 0;
    const size_t tail_remaining = size_t(end - p);
    const size_t tail_quads     = tail_remaining >> 2;
    for (size_t q4 = 0; q4 < tail_quads; ++q4, p += 4) {
        const uint32x4_t v  = vld1q_u32(p);
        const uint32x4_t cc = vcleq_u32(v, tgt);
        s += vaddvq_s32(vreinterpretq_s32_u32(cc));
    }
    uint16_t count = uint16_t(full_cls * kStemsPerCL + size_t(-s));
    // Scalar tail for the up-to-3 non-mult-of-4 trailing stems.
    for (; p < end; ++p) {
        if (*p <= q) ++count;
        else break;   // sorted asc → rest are > q
    }
    return count;
}

// ─── inner_lookup_le_indexed: APGM segment lookup + adaptive 1-CL scan
//
// For wide inner pages (n_div ≥ kInnerGateTrivialBelow).  The APGM
// predictor returns a sample-space position within ±eps of the true
// sample-position of q; ×sample_step → stems-space estimate; adaptive
// 1-CL SIMD count_le window finds the exact position.
inline uint16_t inner_lookup_le_indexed(const inner_node* p, uint32_t q) noexcept {
    const uint16_t n = p->num_keys;
    if (n == 0) return 0;
    const uint32_t* stems = inner_stems(p);
    // Below-range short-circuit: compare against the LIVE stems[0]
    // (not the predictor's segment_keys[0]) — the predictor lags
    // behind recent inserts up to one stride, and a small key
    // inserted late could leave segment_keys[0] > stems[0].
    if (q < stems[0]) return 0;

    const uint16_t num_cls = uint16_t((n + 31) / 32);

    // Predict stems-space position via APGM (clamped to valid range).
    // If the predictor is stale or returns an underflow-induced
    // garbage value, the adaptive walk still lands on the correct
    // position; the clamp here just keeps the walk's starting CL
    // index in bounds.
    uint32_t pred_stem = apgm_lookup_stem_position(&p->predictor, q);
    if (pred_stem >= uint32_t(n)) pred_stem = uint32_t(n - 1);
    uint32_t cl = pred_stem >> 5;   // / 32  (stems per CL)
    if (cl >= num_cls) cl = uint32_t(num_cls - 1);

    // Children prefetch: 32 packed children = 192 B, just under 2 CLs.
    const uint8_t* const ch_base = inner_children_base(p);
    __builtin_prefetch(ch_base + size_t(cl) * 32 * kInnerChildBytes,                    0, 3);
    __builtin_prefetch(ch_base + size_t(cl) * 32 * kInnerChildBytes + kCacheLine, 0, 3);

    const uint32x4_t tgt = vdupq_n_u32(q);
    auto scan_cl = [&](uint32_t c, uint16_t& out_k) -> bool {
        const size_t start = size_t(c) * 32;
        const size_t end   = (start + 32 <= size_t(n)) ? start + 32 : size_t(n);
        const uint16_t sn  = uint16_t(end - start);
        const uint32_t* const sp = stems + start;
        if (sn == 32) {
            int32_t s = 0;
            #pragma unroll
            for (size_t i = 0; i < 8; ++i) {
                const uint32x4_t v  = vld1q_u32(sp + i * 4);
                const uint32x4_t lt = vcleq_u32(v, tgt);
                s += vaddvq_s32(vreinterpretq_s32_u32(lt));
            }
            const uint16_t k = uint16_t(-s);
            out_k = k;
            return k == 32;
        }
        uint16_t k = 0;
        for (uint16_t i = 0; i < sn; ++i) {
            if (sp[i] <= q) ++k;
            else break;
        }
        out_k = k;
        return k == sn;
    };

#ifdef INNER_STATS
    ++g_inner_lookups;
    size_t this_walk = 0;
#endif
    uint16_t k;
    bool overflow = scan_cl(cl, k);
    while (overflow && cl + 1 < num_cls) {
        ++cl;
#ifdef INNER_STATS
        ++g_inner_walk_fwd_steps;
        ++this_walk;
#endif
        __builtin_prefetch(ch_base + size_t(cl) * 32 * kInnerChildBytes,                    0, 3);
        __builtin_prefetch(ch_base + size_t(cl) * 32 * kInnerChildBytes + kCacheLine, 0, 3);
        overflow = scan_cl(cl, k);
    }
    while (k == 0 && cl > 0 && stems[size_t(cl) * 32 - 1] > q) {
        --cl;
#ifdef INNER_STATS
        ++g_inner_walk_bwd_steps;
        ++this_walk;
#endif
        __builtin_prefetch(ch_base + size_t(cl) * 32 * kInnerChildBytes,                    0, 3);
        __builtin_prefetch(ch_base + size_t(cl) * 32 * kInnerChildBytes + kCacheLine, 0, 3);
        scan_cl(cl, k);
    }
#ifdef INNER_STATS
    if (this_walk) ++g_inner_walks_any;
    if (this_walk > g_inner_max_walk) g_inner_max_walk = this_walk;
#endif
    uint32_t pos = cl * 32 + k;
    if (pos > n) pos = n;
    return uint16_t(pos);
}

// ─── Dispatch: pick fast path based on n_div ─────────────────────
inline uint16_t inner_find_branch(const inner_node* p, uint32_t q) noexcept {
    if (p->num_keys >= kInnerGateTrivialBelow) {
        return inner_lookup_le_indexed(p, q);
    }
    const uint32_t* stems = inner_stems(p);
    return inner_count_le(stems, p->num_keys, inner_cap_for(p->cap_in_cls), q);
}

// ─── rebuild_corrections: recompute recip + corrections table ─────
//
// Called after every mutation that changes the divider array
// (insert_after_split, split).  When n_div < kInnerGateTrivialBelow
// the table is unused (fast path skips it) — only base32/max32 stay
// updated for the interp path's bounds-check.  Returns true if the
// gate passes; false means the page should be split to relieve the
// per-bucket spread (caller may ignore and stay on the fast path).
inline bool inner_rebuild_corrections(inner_node* p) noexcept {
    const uint16_t n = p->num_keys;
    if (n == 0) {
        std::memset(&p->predictor, 0, sizeof(p->predictor));
        return true;
    }
    // Small inners use the scalar count_le fast path; the predictor is
    // unused.  Still zero its block so any stale state from a prior life
    // (post-free reuse) doesn't confuse later asserts.
    if (n < kInnerGateTrivialBelow) {
        std::memset(&p->predictor, 0, sizeof(p->predictor));
        return true;
    }
    // Build the APGM predictor.  apgm_build_adaptive walks an eps
    // ladder (1, 2, 3, 4, 6, 8, ...) and returns the smallest eps that
    // fits in ≤ 9 segments.  Returns 0 on total failure (n too large
    // or distribution too pathological for the ladder).  Treat zero as
    // "no predictor available" — lookup will see segment_count == 0
    // and fall back to a full SIMD scan.
    const uint32_t* stems = inner_stems(p);
    (void)apgm_build_adaptive(&p->predictor, stems, uint32_t(n));
    return true;
}

// ─── Structural counters (used by insert/split below) ────────────
inline size_t g_inner_btree_split  = 0;
inline size_t g_inner_split_unsafe = 0;   // could not find delta=1-safe split point
inline size_t g_absorb_k1_first_split  = 0;
inline size_t g_absorb_k1_second_split = 0;

// ─── inner_insert_after_split with std::vector-style grow ─────────
//
// Returns the inner_node pointer after insert.  If the page was full
// AND not at max size, grows in-place (allocates 2× page, copies data,
// frees old, inserts into new) and returns the new pointer.  Caller
// must update parent's child slot to the returned pointer.
//
// Returns nullptr if page was full AND at max cap (caller must split).
// Insert (divider, right) after a child split. Now a thin wrapper over the
// apply()/op_insert pattern: one op work-ticket, dispatched to the in-place
// splice (fits + writable) or the remake ctor (grow / COW), which fuses the
// copy and the slot-open in a single pass (no copy-then-memmove double move).
// realloc relocates same-id and frees the old bytes WITHOUT destroy (children
// move to the new node), so no neuter is needed. Returns the resulting node,
// or nullptr if at max page size (caller must split).
inline inner_node* inner_insert_after_split(inner_node* p, uint16_t branch_pos,
                                              uint32_t divider, common_header* right) noexcept {
    op_insert op{};
    op.pos     = branch_pos;
    op.divider = divider;
    op.child   = right ? right->address() : kNoChild;
    auto            ref = wsession().get_ref<inner_node>(p->address());
    const ptr_address r = apply<upsert_mode::unique_upsert>(ref, op);
    if (r == kNoChild) return nullptr;          // exceeds max page → caller splits
    return wderef<inner_node>(r);
}

// ─── inner_split_at ───────────────────────────────────────────────
// Split src at an explicit position k (0 < k < K).  L gets stems[0..k-1]
// and children[0..k]; up_divider = stems[k]; R gets stems[k+1..K-1] and
// children[k+1..K].  Callers that don't care about k use inner_split.
inline bool inner_split_at(const inner_node* src,
                            inner_node* L, inner_node* R,
                            uint32_t* out_divider, uint16_t k) noexcept {
    const uint16_t K = src->num_keys;
    if (K < 2 || k == 0 || k >= K) return false;
    *out_divider = inner_stems(src)[k];

    // Inherit src's page size — caller allocated L/R at that size.
    inner_init(L, src->cap_in_cls);
    inner_init(R, src->cap_in_cls);

    std::memcpy(inner_stems(L), inner_stems(src), k * sizeof(uint32_t));
    std::memcpy(inner_children_base(L), inner_children_base(src),
                size_t(k + 1) * kInnerChildBytes);
    L->num_keys = k;

    const uint16_t r_n = uint16_t(K - k - 1);
    if (r_n > 0) {
        std::memcpy(inner_stems(R), inner_stems(src) + k + 1,
                    r_n * sizeof(uint32_t));
    }
    std::memcpy(inner_children_base(R),
                inner_children_base(src) + size_t(k + 1) * kInnerChildBytes,
                size_t(r_n + 1) * kInnerChildBytes);
    R->num_keys = r_n;

    (void)inner_rebuild_corrections(L);
    (void)inner_rebuild_corrections(R);
    return true;
}

// Default split point = K/2 (midpoint).
inline bool inner_split(const inner_node* src,
                         inner_node* L, inner_node* R,
                         uint32_t* out_divider) noexcept {
    return inner_split_at(src, L, R, out_divider, uint16_t(src->num_keys >> 1));
}

// Diagnostic counters for the K=1 promote path.
inline size_t g_promote_trigger_drop       = 0;
inline size_t g_promote_too_short          = 0;
inline size_t g_promote_src_has_terminal   = 0;
inline size_t g_promote_src_radix_too_thin = 0;   // K_ < 2 in source radix
inline size_t g_promote_src_no_radix       = 0;   // offs[0] not radix-tagged
inline size_t g_enforce_called             = 0;   // every inner_enforce_invariant_at hit
inline size_t g_enforce_delta1_seen        = 0;   // delta=1 pair triggered the check
// Step-2 verification: count cases where parent's delta=1 pair fires
// but the child's self-asserted prefix doesn't match.  Must stay 0
// throughout all tests for step 3 (switch to child-side prefix) to
// be safe.
inline size_t g_prefix_check_mismatch      = 0;
inline size_t g_prefix_check_seen          = 0;   // total delta=1 + inner-child checks
// Step-3 routing invariant: when descending to a child with
// prefix_len > 0, the query's key[depth] MUST equal child.prefix_data.
// A mismatch means the parent over-routed (its stem range for this
// slot includes values other than the child's claimed prefix —
// usually because of an accidental delta=1 stem pair).  Should stay 0.
inline size_t g_routing_inconsistency      = 0;
inline size_t g_enforce_child_already_inner= 0;   // child was inner; nothing to do
inline size_t g_enforce_child_k_gt_1       = 0;   // leaf with K>1 under a delta=1; SHOULD NOT HAPPEN
inline size_t g_enforce_promote_ok         = 0;   // successful promotion
inline size_t g_enforce_promote_fail       = 0;   // promotion failed → INVARIANT BROKEN

// ─── leaf_promote_k1_restructure: build the sub-inner ─────────────
//
// Helper shared by leaf_promote_k1 (called from leaf_upsert when a
// K=1 leaf can't accept a new key) and inner_enforce_invariant_at
// (called after any inner_insert_after_split to maintain the
// "delta=1 ⟺ depth-advanced child" invariant).
//
// Walks the leaf's layer-1 radix branches as spans (no scratch buffer),
// partitions them across two new leaves L1/L2 at depth+1, builds a
// sub-inner above them, and frees the source leaf.  Does NOT insert
// any new key.  Returns nullptr if the leaf can't be promoted (no
// radix subtree, or layer-1 has < 2 branches).
//
// On success, *out_stem holds the original leaf's single stem value X.
inline inner_node* leaf_promote_k1_restructure(leaf_node* L, uint16_t value_len,
                                                 uint32_t* out_stem) noexcept {
    assert(L->num_keys == 1);
    *out_stem = leaf_stems(L)[0];
    const uint16_t root_off_tagged = leaf_offs(L)[0];
    if (!offs_is_radix(root_off_tagged)) {
        ++g_promote_src_no_radix;
        return nullptr;
    }
    const uint16_t root_off = root_off_tagged & kRadixOffMask;
    const auto* root_rn = reinterpret_cast<const radix_node*>(leaf_at(L, root_off));
    const uint8_t K_ = root_rn->n_entries();
    if (K_ < 1) {
        ++g_promote_src_radix_too_thin;
        return nullptr;
    }
    // The source radix may also have a terminal_off entry (a key that
    // ends right after L's stem).  v4_lf's inner_node has no terminal
    // slot yet, so the terminal entry would be silently lost.  Refuse
    // to promote in this case — caller drops the trigger key but the
    // existing entries are preserved in the original leaf.
    // TODO: add terminal_off to inner_node so this case is handled.
    if (root_rn->terminal_off() != kLeafOffSentinel) {
        ++g_promote_src_has_terminal;
        return nullptr;
    }
    // Compressed root: sub_stems live at depth+1+path_words, but inner
    // nodes can't carry path words yet.  Depth-advance ONE path word
    // per promote: produce L1=empty + L2 with one stem (= path[0])
    // whose subgraph is the original compressed_rn minus its first
    // path word.  Subsequent promotes peel one word at a time until
    // pw=0; then the regular branch below takes over.  This mirrors
    // the pre-Phase-2 K=1 chain's promote semantics word-for-word.
    if (root_rn->has_path()) {
        const uint8_t  pw_orig   = root_rn->path_words();
        const uint32_t head_word = root_rn->path_data()[0];
        const uint8_t  cap_orig  = root_rn->cap_slots();
        const uint8_t  n_orig    = K_;
        const bool     ht_orig   = root_rn->has_term();
        const uint16_t term_orig = root_rn->terminal_off();

        uint32_t path_buf[128];
        assert(pw_orig <= sizeof(path_buf) / sizeof(uint32_t));
        std::memcpy(path_buf, root_rn->path_data(), size_t(pw_orig) * 4);
        uint32_t sub_buf[128];
        uint16_t offs_buf[128];
        assert(n_orig <= sizeof(sub_buf) / sizeof(uint32_t));
        std::memcpy(sub_buf,  root_rn->sub_stems(), size_t(n_orig) * 4);
        std::memcpy(offs_buf, root_rn->offs(),      size_t(n_orig) * 2);

        leaf_node* L1 = leaf_alloc(L->address());   leaf_init(L1);
        leaf_node* L2 = leaf_alloc(L1->address());  leaf_init(L2);

        const uint8_t new_pw = uint8_t(pw_orig - 1);
        const uint16_t new_subrn_off = alloc_radix_node(L2, cap_orig, new_pw, ht_orig);
        if (new_subrn_off == kLeafOffSentinel) {
            leaf_free(L1); leaf_free(L2);
            ++g_promote_src_radix_too_thin;
            return nullptr;
        }
        {
            auto* new_subrn = reinterpret_cast<radix_node*>(leaf_at(L2, new_subrn_off));
            if (new_pw > 0) {
                std::memcpy(new_subrn->path_data(), path_buf + 1, size_t(new_pw) * 4);
            }
        }
        // Deep-copy each sub_stem's subgraph from L to L2.
        bool deep_failed = false;
        for (uint8_t i = 0; i < n_orig && !deep_failed; ++i) {
            const uint16_t copied = copy_subgraph_(L, offs_buf[i], L2, value_len);
            if (copied == kLeafOffSentinel) { deep_failed = true; break; }
            auto* new_subrn = reinterpret_cast<radix_node*>(leaf_at(L2, new_subrn_off));
            new_subrn->sub_stems()[i] = sub_buf[i];
            new_subrn->offs()[i]      = copied;
        }
        if (!deep_failed && ht_orig && term_orig != kLeafOffSentinel) {
            const uint16_t copied = copy_subgraph_(L, term_orig, L2, value_len);
            if (copied == kLeafOffSentinel) { deep_failed = true; }
            else {
                auto* new_subrn = reinterpret_cast<radix_node*>(leaf_at(L2, new_subrn_off));
                new_subrn->set_terminal_off(copied);
            }
        }
        if (deep_failed) {
            leaf_free(L1); leaf_free(L2);
            ++g_promote_src_radix_too_thin;
            return nullptr;
        }
        {
            auto* new_subrn = reinterpret_cast<radix_node*>(leaf_at(L2, new_subrn_off));
            new_subrn->set_n_entries(n_orig);
        }

        leaf_stems(L2)[0] = head_word;
        leaf_offs(L2)[0]  = uint16_t(new_subrn_off | kRadixTagBit);
        L2->num_keys      = 1;
        L2->num_branches  = 1;

        inner_node* new_inner = inner_alloc_sized(8, L1->address());  // near its children
        inner_init(new_inner, 8);
        inner_stems(new_inner)[0] = head_word;
        inner_set_child(new_inner, 0, as_header(L1));
        inner_set_child(new_inner, 1, as_header(L2));
        new_inner->num_keys = 1;
        // Self-asserted prefix: this sub_inner is the depth-advanced
        // subtree for L's single stem X.  (Parent's delta=1 pair will
        // route to it; the prefix here is the child's own statement
        // that "I am the X-subtree".)
        new_inner->prefix_len  = 1;
        new_inner->prefix_data = *out_stem;
        (void)inner_rebuild_corrections(new_inner);

        leaf_free(L);
        return new_inner;
    }

    std::span<const uint32_t> sub_stems(root_rn->sub_stems(), K_);
    std::span<const uint16_t> sub_offs (root_rn->offs(),      K_);
    const uint8_t k = uint8_t(K_ / 2);
    const uint32_t divider_sub = sub_stems[k];

    leaf_node* L1 = leaf_alloc(L->address());   leaf_init(L1);
    leaf_node* L2 = leaf_alloc(L1->address());  leaf_init(L2);

    bool populate_failed = false;
    auto populate = [&](leaf_node* dst, std::span<const uint32_t> stems_in,
                        std::span<const uint16_t> offs_in) noexcept {
        for (size_t i = 0; i < stems_in.size(); ++i) {
            uint16_t cap = leaf_stems_for(dst->cap_in_cls);
            while (dst->num_keys >= cap) {
                if (!leaf_grow_index(dst)) { populate_failed = true; return; }
                cap = leaf_stems_for(dst->cap_in_cls);
            }
            const uint16_t new_off = copy_subgraph_(L, offs_in[i], dst, value_len);
            if (new_off == kLeafOffSentinel) { populate_failed = true; return; }
            const bool was_radix = offs_is_radix(offs_in[i]);
            const uint16_t pos = dst->num_keys;
            leaf_stems(dst)[pos] = stems_in[i];
            leaf_offs(dst)[pos]  = was_radix ? uint16_t(new_off | kRadixTagBit) : new_off;
            ++dst->num_keys;
            if (was_radix) ++dst->num_branches;
        }
    };
    populate(L1, sub_stems.subspan(0, k), sub_offs.subspan(0, k));
    if (!populate_failed) populate(L2, sub_stems.subspan(k), sub_offs.subspan(k));
    if (populate_failed) {
        // L2 couldn't hold its share — typically for deeply-shared
        // prefixes where a single sub_stem's subgraph exceeds 4 KB.
        // Free both halves and refuse promote.  Caller drops the
        // trigger; existing entries remain in the original leaf.
        leaf_free(L1); leaf_free(L2);
        ++g_promote_src_radix_too_thin;   // overload — populate-overflow
        return nullptr;
    }

    inner_node* new_inner = inner_alloc_sized(8, L1->address());   // 1 KB initial; near children
    inner_init(new_inner, 8);
    inner_stems(new_inner)[0] = divider_sub;
    inner_set_child(new_inner, 0, as_header(L1));
    inner_set_child(new_inner, 1, as_header(L2));
    new_inner->num_keys = 1;
    // Self-asserted prefix (see has_path branch above for rationale).
    new_inner->prefix_len  = 1;
    new_inner->prefix_data = *out_stem;
    (void)inner_rebuild_corrections(new_inner);

    leaf_free(L);
    return new_inner;
}

// ─── leaf_promote_k1: full K=1 promotion (with trigger-key insert) ─
inline upsert_result leaf_promote_k1(leaf_node* L,
                                       uint16_t depth,
                                       word_key_type key,
                                       std::span<const uint8_t> value) noexcept {
    // Edge case: trigger key has no word at depth+1 (key ends exactly
    // at depth).  The sub-leaves we'd create are at depth+1 and can't
    // accept a key with no key[depth+1].  Inner nodes don't yet have a
    // terminal_off slot for end-of-key entries at their depth.  Surface
    // this as a recoverable drop for now — TODO: add terminal_off to
    // inner_node so these keys can be stored at the inner level.
    if (size_t(depth) + 1 >= key.size()) {
        ++g_promote_too_short;
        return { nullptr, nullptr, 0, 0 };
    }
    uint32_t X = 0;
    inner_node* sub = leaf_promote_k1_restructure(L, uint16_t(value.size()), &X);
    if (!sub) return { nullptr, nullptr, 0, 0 };
    // Insert the triggering key into the appropriate sub-leaf.
    const uint32_t divider_sub = inner_stems(sub)[0];
    const uint32_t new_sub = key[depth + 1];
    leaf_node* target = (new_sub < divider_sub)
        ? as_leaf(inner_child(sub, 0))
        : as_leaf(inner_child(sub, 1));
    if (!leaf_insert(target, key, uint16_t(depth + 1), value)) {
        // Target is too full for the trigger (typically because populate
        // copied most of the source's content into target for the K_=1
        // case).  Recursively K=1 promote target — this depth-advances
        // its content one more level and gives the trigger fresh room.
        // The new sub_inner_2 replaces target in sub.children; sub's
        // stems gain a delta=1 marker for the new depth-advance.
        const uint16_t target_pos = (new_sub < divider_sub) ? 0 : 1;
        auto inner_p = leaf_promote_k1(target, uint16_t(depth + 1), key, value);
        if (!inner_p.left || inner_p.right != kSentinelChild()) {
            // Recursive promote also failed (truly out of room, or
            // returned a non-promote shape).  Drop the trigger; keep
            // the outer sub_inner so the migrated content stays alive.
            ++g_promote_trigger_drop;
            return upsert_promote(as_header(sub), /*prev_div=*/X, /*sentinel_div=*/X + 1u);
        }
        // inner_p = upsert_promote(sub_inner_2, X2, X2+1).
        // Replace sub.children[target_pos] with sub_inner_2.  Add X2
        // and X2+1 stems around it to create the depth-advance signal.
        //
        // Layout transform (target_pos == 1):
        //   BEFORE: sub.stems=[divider_sub],
        //           children=[L1, target (just freed)]
        //   AFTER:  sub.stems=[divider_sub, X2, X2+1],
        //           children=[L1, sentinel, sub_inner_2, sentinel]
        //
        // For target_pos == 0, layout is mirrored — but K_=1 case only
        // ever puts trigger in target=L2 (since key[depth+1] == divider_sub
        // ≥ divider_sub).  Assert that here.
        assert(target_pos == 1 && "K_=1 promote always routes trigger to L2");
        // sub already has children[1] = target (which leaf_promote_k1
        // just freed and replaced).  Clear it and rebuild.
        inner_set_child(sub, 1, nullptr);   // sentinel for [divider_sub, X2)
        const uint32_t X2  = inner_p.prev_divider;
        const uint32_t X2p = inner_p.divider;
        // Insert X2 with sub_inner_2 at position 1.
        inner_node* g1 = inner_insert_after_split(sub, 1, X2, inner_p.left);
        inner_node* cur1 = g1 ? g1 : sub;
        // Insert X2+1 sentinel at position 2.
        inner_node* g2 = inner_insert_after_split(cur1, 2, X2p, nullptr);
        inner_node* final_sub = g2 ? g2 : cur1;
        return upsert_promote(as_header(final_sub),
                              /*prev_div=*/X, /*sentinel_div=*/X + 1u);
    }
    return upsert_promote(as_header(sub), /*prev_div=*/X, /*sentinel_div=*/X + 1u);
}

// ─── inner_enforce_invariant_at: maintain delta=1 ⟺ promoted ──────
//
// After any inner_insert_after_split that bubbled a divider into a
// parent, check whether the new divider creates a delta=1 pair with
// either neighbor.  If so, the affected child slot is restricted to
// keys with a single first-u32 value, so its child must be in the
// depth-advanced form.  If it's still a leaf, promote it.
//
// value_len is needed because copy_subgraph_ needs it (leaf-internal
// detail that promotion uses).
inline void inner_enforce_invariant_at(inner_node* p, uint16_t pos, uint16_t value_len) noexcept {
    ++g_enforce_called;
    if (pos == 0 || pos >= p->num_keys) return;
    const uint32_t* stems = inner_stems(p);
    if (stems[pos] - stems[pos - 1] != 1u) return;
    ++g_enforce_delta1_seen;
    common_header* child = inner_child(p, pos);
    if (!child) return;
    if (child->type() != node_type::wtree_leaf) {
        ++g_enforce_child_already_inner;
        return;
    }
    leaf_node* L = as_leaf(child);
    if (L->num_keys != 1) {
        ++g_enforce_child_k_gt_1;
        return;
    }
    uint32_t orig_stem = 0;
    inner_node* sub = leaf_promote_k1_restructure(L, value_len, &orig_stem);
    if (!sub) {
        ++g_enforce_promote_fail;
        return;
    }
    ++g_enforce_promote_ok;
    inner_set_child(p, pos, as_header(sub));
}

// ─── op_insert: templated splice + apply() contract ──────────────────
// splice_insert<InPlace>: write the post-insert stems[]/children[] into dst.
// Compile-time specialized — the in-place case (dst==src, same cap) is a pure
// overlapping shift; the copy case (dst!=src, maybe grown) is non-overlapping
// copies. No runtime src==dst branch on the hot path.
template <splice_mode M>
inline void splice_insert(inner_node* dst, const inner_node* src,
                          uint16_t pos, uint32_t divider, ptr_address child) noexcept {
    const uint16_t K  = src->num_keys;
    uint32_t*      ds = inner_stems(dst);
    uint8_t*       dc = inner_children_base(dst);
    if constexpr (M == splice_mode::in_place) {
        std::memmove(ds + pos + 1, ds + pos, size_t(K - pos) * sizeof(uint32_t));
        std::memmove(dc + size_t(pos + 2) * kInnerChildBytes,
                     dc + size_t(pos + 1) * kInnerChildBytes,
                     size_t(K - pos) * kInnerChildBytes);
        ds[pos] = divider;
        std::memcpy(dc + size_t(pos + 1) * kInnerChildBytes, &child, sizeof(child));
    } else {
        const uint32_t* ss = inner_stems(src);
        const uint8_t*  sc = inner_children_base(src);
        std::memcpy(ds, ss, size_t(pos) * sizeof(uint32_t));
        ds[pos] = divider;
        std::memcpy(ds + pos + 1, ss + pos, size_t(K - pos) * sizeof(uint32_t));
        std::memcpy(dc, sc, size_t(pos + 1) * kInnerChildBytes);
        std::memcpy(dc + size_t(pos + 1) * kInnerChildBytes, &child, sizeof(child));
        std::memcpy(dc + size_t(pos + 2) * kInnerChildBytes,
                    sc + size_t(pos + 1) * kInnerChildBytes,
                    size_t(K - pos) * kInnerChildBytes);
    }
    dst->num_keys = uint16_t(K + 1);
}

// Stride-gated APGM rebuild (amortized — matches inner_insert_after_split):
// rebuilding every insert would be O(n^2); the adaptive 1-CL lookup walk
// absorbs the drift between rebuilds.
inline void inner_maybe_rebuild(inner_node* p) noexcept {
    const uint16_t K1 = p->num_keys;
    const uint16_t stride = (K1 >= 1024u) ? uint16_t(K1 >> 5) : uint16_t(32);
    if (K1 % stride == 0u || K1 < kInnerGateTrivialBelow)
        (void)inner_rebuild_corrections(p);
}

inline uint32_t inner_node::alloc_size(const inner_node& src, op_insert& op) noexcept {
    const uint16_t K1 = uint16_t(src.num_keys + 1);
    if (K1 <= inner_cap_for(src.cap_in_cls)) {
        op.result_cap = src.cap_in_cls;                          // fits current page
    } else {
        const uint32_t dbl = uint32_t(src.cap_in_cls) * 2u;     // grow like std::vector
        if (dbl > kInnerMaxPageCls) return 0;                   // at max → caller splits
        op.result_cap = uint16_t(dbl);
    }
    op.copy = copy_mode::fast;                                   // inner has no dead space
    return uint32_t(op.result_cap) * uint32_t(kCacheLine);
}

inline void inner_node::apply(op_insert& op) noexcept {          // in-place
    splice_insert<splice_mode::in_place>(this, this, op.pos, op.divider, op.child);
    inner_maybe_rebuild(this);
}

inline inner_node::inner_node(uint32_t asize, ptr_address_seq seq,
                              const inner_node& src, op_insert& op) noexcept
    : node(asize, type_id, seq) {                                // copy / grow
    cap_in_cls     = op.result_cap;
    tail_alloc_pos = 0;
    prefix_len     = src.prefix_len;
    prefix_data    = src.prefix_data;
    _pad0          = 0;
    std::memset(_pad1, 0, sizeof(_pad1));
    std::memcpy(&predictor, &src.predictor, sizeof(predictor));
    splice_insert<splice_mode::fresh_copy>(this, &src, op.pos, op.divider, op.child);
    // Grow keeps stems unchanged-but-shifted; a rebuild on the fresh page is
    // due per the same stride gate.
    inner_maybe_rebuild(this);
}

// ─── op_replace_slot: multi-stem splice (replace child[pos] with a run) ───
// Replaces 1 child at pos with (n_stems+1) children + n_stems dividers. Same
// compile-time in_place / fresh_copy specialization as splice_insert.
template <splice_mode M>
inline void splice_replace(inner_node* dst, const inner_node* src,
                           op_replace_slot& op) noexcept {
    const uint16_t K  = src->num_keys;
    const uint16_t p  = op.pos;
    const uint16_t ns = op.n_stems;
    uint32_t* ds = inner_stems(dst);
    uint8_t*  dc = inner_children_base(dst);
    if constexpr (M == splice_mode::in_place) {
        // dst == src, same cap: shift tail right by ns, drop the run in.
        if (K > p) {
            std::memmove(ds + p + ns, ds + p, size_t(K - p) * sizeof(uint32_t));
            std::memmove(dc + size_t(p + 1 + ns) * kInnerChildBytes,
                         dc + size_t(p + 1) * kInnerChildBytes,
                         size_t(K - p) * kInnerChildBytes);
        }
        std::memcpy(ds + p, op.repl_stems, size_t(ns) * sizeof(uint32_t));
        for (uint16_t i = 0; i <= ns; ++i)
            inner_set_child(dst, uint16_t(p + i), op.repl_children[i]);
    } else {
        // dst != src (grown / COW): copy prefix, run, then shifted tail.
        const uint32_t* ss = inner_stems(src);
        const uint8_t*  sc = inner_children_base(src);
        if (p > 0) {
            std::memcpy(ds, ss, size_t(p) * sizeof(uint32_t));
            std::memcpy(dc, sc, size_t(p) * kInnerChildBytes);
        }
        std::memcpy(ds + p, op.repl_stems, size_t(ns) * sizeof(uint32_t));
        for (uint16_t i = 0; i <= ns; ++i)
            inner_set_child(dst, uint16_t(p + i), op.repl_children[i]);
        if (K > p) {
            std::memcpy(ds + p + ns, ss + p, size_t(K - p) * sizeof(uint32_t));
            std::memcpy(dc + size_t(p + 1 + ns) * kInnerChildBytes,
                        sc + size_t(p + 1) * kInnerChildBytes,
                        size_t(K - p) * kInnerChildBytes);
        }
    }
    dst->num_keys = uint16_t(K + ns);
}

inline uint32_t inner_node::alloc_size(const inner_node& src, op_replace_slot& op) noexcept {
    const uint16_t K_new   = uint16_t(src.num_keys + op.n_stems);
    const uint16_t need    = inner_cap_in_cls_for(K_new);
    if (K_new > inner_cap_for(need)) return 0;                   // exceeds max → split
    op.result_cap = (K_new <= inner_cap_for(src.cap_in_cls)) ? src.cap_in_cls : need;
    op.copy = copy_mode::fast;
    return uint32_t(op.result_cap) * uint32_t(kCacheLine);
}

inline void inner_node::apply(op_replace_slot& op) noexcept {    // in-place
    splice_replace<splice_mode::in_place>(this, this, op);
    inner_maybe_rebuild(this);
}

inline inner_node::inner_node(uint32_t asize, ptr_address_seq seq,
                              const inner_node& src, op_replace_slot& op) noexcept
    : node(asize, type_id, seq) {                                // copy / grow
    cap_in_cls     = op.result_cap;
    tail_alloc_pos = 0;
    prefix_len     = src.prefix_len;
    prefix_data    = src.prefix_data;
    _pad0          = 0;
    std::memset(_pad1, 0, sizeof(_pad1));
    std::memcpy(&predictor, &src.predictor, sizeof(predictor));
    splice_replace<splice_mode::fresh_copy>(this, &src, op);
    (void)inner_rebuild_corrections(this);   // fresh page → full rebuild (run drift)
}

}  // namespace psitri::wtree
