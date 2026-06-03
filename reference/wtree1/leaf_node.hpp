#pragma once

// leaf_node — v3 leaf page (4 KB), high-fanout redesign.
//
//   Index: per-stem entries paired as (u32 stem, u16 offs).
//     - stem = first u32 of each key at this leaf's depth
//     - offs = end-relative byte offset (low 15 bits) + radix-tag (high bit)
//   1-entry stem: offs → value-region directly (tag bit clear).
//   Collision (≥ 2 entries on the same stem): offs → radix node (tag bit set).
//
//   value-region: [u8 suffix_len] [suffix bytes] [value bytes].
//                  Allocated from the leaf's tail-heap.
//   radix node:   [u8 n_entries, u8 cap_slots, u16 pad,
//                  u32 sub_stems[cap_slots], u16 offs[cap_slots]].
//                 cap_slots grows by 4 (kRadixGrowChunk) when full.
//                 sub_stems uses 0xFFFFFFFF sentinel for unused slots.
//                 offs[] follows the same tag-bit convention, so a radix
//                 node can itself contain nested radix roots when
//                 collisions are deep.
//
//   Sentinels (SIMD-friendly):
//     stems[i]      = 0xFFFFFFFF when unused
//     offs[i]       = 0xFFFF     when unused
//     sub_stems[i]  = 0xFFFFFFFF when unused
//
// Layout in a 4 KB leaf:
//
//   byte  0   [psitri::node (12 B)]
//   byte 12   [u16 tail_alloc_pos] [u8 cap_in_cls] [u8 num_keys]
//             [u8 num_branches]   [3 B pad]
//   byte 20   stems[K]  u32  → grows forward
//             …
//             (≤ 5 B padding in the middle, ever)
//             …
//             offs[K]   u16  ← grows backward from byte (cap_in_cls·128)
//   byte cap_in_cls·128
//             heap: value-regions + radix nodes, grows down from byte 4096

#include "common.hpp"
#include <psitri/upsert_mode.hpp>

#include <algorithm>
#include <arm_neon.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>   // std::aligned_alloc, std::free
#include <cstring>
#include <span>

namespace psitri::wtree {

// ─── Public typedefs ───────────────────────────────────────────────
// byte_key_type — what users pass at the public API: opaque bytes.
//                 Mult-of-4 length is required (asserted at boundary).
// word_key_type — internal: span<const uint32_t> of byteswapped words.
//                 memcmp on the underlying bytes gives the same ordering
//                 as the original byte memcmp, so suffix equality checks
//                 can use a single memcmp on size_bytes().
// value_type    — opaque user payload bytes (tree-wide length).
using byte_key_type = std::span<const uint8_t>;
using word_key_type = std::span<const uint32_t>;
using value_type    = std::span<const uint8_t>;

// ─── Sizes ────────────────────────────────────────────────────────
inline constexpr size_t   kLeafPageBytes      = 4096;
inline constexpr size_t   kLeafPageCls        = kLeafPageBytes / kCacheLine;  // 32

// ─── Node header ───────────────────────────────────────────────────
// The 12-byte header is now sal::alloc_header (via psitri::node, aliased
// as `psitri::node` in common.hpp). `node->type()` returns node_type;
// the wtree node-kind tags are node_type::wtree_leaf / wtree_inner.
// `node->size()` is the allocation size in bytes.

// ─── upsert_result: uniform return type from any page-level upsert ─
//
//   left           = primary (possibly-rewritten) child; parent assigns
//                    to its branch slot unconditionally.
//   right          = new sibling to insert AT pos+1 (null = no insert).
//                    A non-null `right` may be `kSentinelChild` to signal
//                    that the parent should insert an EMPTY slot at pos+1
//                    (no real subtree); this is part of the K=1 promotion
//                    path where parent gains an `X+1` sentinel divider.
//   divider        = the u32 separator key for `right` (valid iff right!=null).
//   prev_divider   = if non-zero, parent ALSO replaces its existing
//                    stems[pos-1] with this value before doing anything
//                    else.  Used by K=1 promotion when the leaf's single
//                    stem `X` differs from the parent's divider that
//                    routed to it: parent's divider tightens to `X` so
//                    the [X, X+1) slot is exactly the depth-advanced
//                    sub-tree's domain.  Zero = no replacement.
//
// Outcomes encoded uniformly:
//   in-place        { this,        nullptr,         _,    0 }
//   rewritten       { new_self,    nullptr,         _,    0 }
//   split           { new_left,    new_right,       div,  0 }
//   K=1 promotion   { new_inner,   kSentinelChild,  X+1,  X }   (X = leaf's only stem)
//   drop / failure  { nullptr,     nullptr,         _,    0 }
// Extra slot for upsert_result when a single operation produces more than
// the {left, right, divider} layout can express.  Heap-allocated ONLY
// when needed (rare paths), so the common 2-way case stays POD-and-fast.
//
// Encodes additional children and dividers to install AROUND the primary
// result.  Conceptually the parent slot at pos is filled by the sequence:
//   prepend_child? (with prepend_divider before left)
//   left
//   divider
//   right (or kSentinelChild marker)
//   [if K=1 promote: implicit X+1 sentinel slot]
//   tail[0]_divider, tail[0]_child
//   tail[1]_divider, tail[1]_child
//   ...
//
// Fixed-size for simplicity; 4-slot tail handles all currently-observed
// cases.  Caller (node_upsert_) deletes the extra after absorption.
struct upsert_extra {
    // Pre-promote prepend region: 0..N entries placed BEFORE the primary
    // bubble (left or the K=1 promote's [sentinel, new_inner, sentinel]
    // pieces).  Each prepend_dividers[i] separates prepend_children[i]
    // from the NEXT child in the sequence (so prepend_dividers[N-1] sits
    // between the last prepend and the first primary child).
    uint8_t        n_prepend       = 0;
    psitri::node* prepend_children[4]{};
    uint32_t       prepend_dividers[4]{};
    // Post-primary tail region.  Same shape mirrored.
    uint8_t        n_tail          = 0;
    psitri::node* tail_children[4]{};
    uint32_t       tail_dividers[4]{};
};

struct upsert_result {
    psitri::node* left;
    psitri::node* right;
    uint32_t       divider;
    uint32_t       prev_divider;
    upsert_extra*  extra = nullptr;   // owned; absorber frees
    // REMOVE mode only: `left` carries the (possibly forked) child; `emptied`
    // means the child subtree is now empty so the parent drops its slot. A
    // remove miss is {left=nullptr, emptied=false}; upsert never sets emptied.
    bool           emptied = false;
};
// In-RAM currency stays raw node* (valid in unique mode — no relocation).
// Only PAGE storage uses ptr_address (inner_child/inner_set_child convert at
// the boundary). kSentinelChild() is a transient marker, never stored in a
// page; addr 1 (128-B-misaligned) can never be a real deref'd node pointer.
inline psitri::node* kSentinelChild() noexcept {
    return reinterpret_cast<psitri::node*>(static_cast<uintptr_t>(1));
}

inline upsert_result upsert_inplace(psitri::node* p) noexcept {
    return { p, nullptr, 0u, 0u };
}
inline upsert_result upsert_split(psitri::node* l, uint32_t div, psitri::node* r) noexcept {
    return { l, r, div, 0u };
}
inline upsert_result upsert_promote(psitri::node* new_inner,
                                      uint32_t prev_div, uint32_t sentinel_div) noexcept {
    return { new_inner, kSentinelChild(), sentinel_div, prev_div };
}

// ─── Leaf node header (24 B; extended for child-side prefix slot) ──
//
// History: V2-style (128-B header with stems at CL 1 boundary, offs at
// fixed byte 1008) was measured WORSE for v4_lf's workload: at 1M
// random keys, the 108-byte head reservation reduced heap from 3968 →
// 2648 bytes, forcing earlier splits — leaves jumped from 13878 →
// 17535 (entries/leaf dropped from 73 → 57), AND the fixed offs at
// byte 1008 spans CL 7-8 (separate from stems CL 1-3) instead of the
// dynamic offs that overlaps the last stems CL.  Net: more leaves,
// more CL touches per find.  Keeping the header tight.
//
// prefix_len / prefix_data encode the child's self-asserted common
// prefix at the PARENT's depth (the "I am the depth-advanced subtree
// for keys whose key[parent_depth] == prefix_stem" signal).  Replaces
// the parent's fragile delta=1-pair encoding.  prefix_len==0 means no
// prefix (regular child at parent's depth).  prefix_len==1 means
// prefix_data IS the single-word u32 prefix.  prefix_len>1 means
// prefix_data is a tail-region offset to a prefix_len-word buffer
// (multi-word path compression at the inner-routing level — collapses
// chains of single-word depth advances into one node).
struct leaf_node : public node {
    static constexpr node_type type_id = node_type::wtree_leaf;
    // Leaf is always physically 4 KB; cap_in_cls grows the index region in-page.
    static uint32_t alloc_size() noexcept { return uint32_t(kLeafPageBytes); }

    uint16_t      tail_alloc_pos;  // 12..13
    uint8_t       cap_in_cls;      // 14
    uint8_t       num_branches;    // 15
    uint16_t      num_keys;        // 16..17
    uint8_t       prefix_len;      // 18
    uint8_t       _pad;            // 19
    uint32_t      prefix_data;     // 20..23
    uint16_t      dead_bytes;      // 24..25  heap bytes abandoned (collisions / radix grow)
    uint16_t      _pad2;           // 26..27  reserved

    // SAL placement ctor: (asize, addr_seq) injected by the allocator.
    leaf_node(uint32_t asize, ptr_address_seq seq) noexcept;  // defined after leaf_init

    // Size-aware ACTIVE-region copy (used by both copy_to/COW and compact_to).
    // Head (header+index, [0, cap*128)) → dst[0,head); the heap (last
    // tail_alloc_pos bytes, rounded up to 64) → the DST's page-end. Heap
    // offsets are page-end-relative, so they stay valid whether dst is LARGER
    // than src (COW → 4 KB, maximal free heap for future inserts) or SMALLER
    // (compaction → packed head+heap, the free gap between them dropped). Two
    // 64-aligned block copies; an empty heap makes the second a no-op.
    void copy_to(sal::alloc_header* dst) const noexcept {
        const uint8_t* s = reinterpret_cast<const uint8_t*>(this);
        uint8_t*       d = reinterpret_cast<uint8_t*>(dst);
        const size_t   head  = size_t(cap_in_cls) * kCacheLine;             // 128-aligned
        const size_t   heap  = ucc::round_up_multiple<64>(size_t(tail_alloc_pos));
        const uint32_t dsize = dst->size();                                 // dst's allocated size
        // The head copy clobbers dst's 12-byte alloc_header with src's — fine
        // when sizes match, but here dst may be larger (COW) or smaller
        // (compaction), so save/restore dst's header (esp. _size; _checksum is
        // recomputed by the caller). Heap is page-end-relative → placed at
        // dst's own page-end, valid regardless of dst size.
        uint8_t hdr[sizeof(sal::alloc_header)];
        std::memcpy(hdr, d, sizeof(hdr));
        ucc::memcpy_aligned_64byte(d, s, head);
        ucc::memcpy_aligned_64byte(d + dsize - heap, s + size() - heap, heap);
        std::memcpy(d, hdr, sizeof(hdr));
    }
    // COW expands to a full 4 KB page → maximal free heap for future inserts.
    // (A subsequent compaction packs it back down; see compact_size.)
    uint32_t cow_size() const noexcept { return uint32_t(kLeafPageBytes); }
    // Compaction packs the leaf to head + heap (drops the free gap). The heap's
    // page-end-relative offsets survive the shrink unchanged; a later write
    // COWs it back up to 4 KB.
    uint32_t compact_size() const noexcept {
        return uint32_t(size_t(cap_in_cls) * kCacheLine
                        + ucc::round_up_multiple<64>(size_t(tail_alloc_pos)));
    }
    void compact_to(sal::alloc_header* dst) const noexcept { copy_to(dst); }

    // ── Lifecycle over out-of-line value refs (big / subtree) ─────────
    // A value_region of type big/subtree owns a single ptr_address stored in
    // its value bytes.  These walk every reachable value_region (current
    // versions; the alt/history chain is added in §4 — STUBBED here) and:
    //   destroy()         — release each owned ref (SAL final-release hook).
    //   visit_children()  — report each owned ref (compactor relocation walk).
    //   retain_children() — retain each owned ref (wtree's MVCC share point;
    //                       NOT a SAL hook — copy_to stays byte-only).
    // Defined after the value_region / radix walk helpers below.  Inline-value
    // leaves carry no refs, so all three are no-ops until big/subtree land.
    void destroy(const sal::allocator_session_ptr& session) const noexcept;
    void visit_children(const std::function<void(sal::ptr_address)>& v) const noexcept;
    void retain_children(const sal::allocator_session_ptr& session) const noexcept;
};
static_assert(sizeof(leaf_node) == 28, "leaf_node header must be 28 B");
static_assert(offsetof(leaf_node, tail_alloc_pos)   == 12);
static_assert(offsetof(leaf_node, cap_in_cls)       == 14);
static_assert(offsetof(leaf_node, num_branches)     == 15);
static_assert(offsetof(leaf_node, num_keys)         == 16);
static_assert(offsetof(leaf_node, prefix_len)       == 18);
static_assert(offsetof(leaf_node, prefix_data)      == 20);
static_assert(offsetof(leaf_node, dead_bytes)       == 24);

inline constexpr uint16_t kLeafHeaderBytes = sizeof(leaf_node);  // 28

// ─── Pointer-interconvertibility helpers ──────────────────────────
// Every concrete node type has psitri::node as its first member, so
// reinterpret_cast between psitri::node* and the concrete type is
// well-defined (standard-layout + pointer-interconvertibility rules,
// C++17 §6.9.2/4).  Use these helpers rather than raw casts at call
// sites — the assertion documents the invariant and catches bugs.
inline psitri::node* as_header(leaf_node* p)             noexcept { return reinterpret_cast<psitri::node*>(p); }
inline const psitri::node* as_header(const leaf_node* p) noexcept { return reinterpret_cast<const psitri::node*>(p); }
inline leaf_node* as_leaf(psitri::node* h) noexcept {
    assert(h->type() == node_type::wtree_leaf);
    return reinterpret_cast<leaf_node*>(h);
}
inline const leaf_node* as_leaf(const psitri::node* h) noexcept {
    assert(h->type() == node_type::wtree_leaf);
    return reinterpret_cast<const leaf_node*>(h);
}
// upsert_result builders taking concrete leaf_node*.
inline upsert_result upsert_inplace(leaf_node* p) noexcept {
    return upsert_inplace(as_header(p));
}
inline upsert_result upsert_split(leaf_node* l, uint32_t div, leaf_node* r) noexcept {
    return upsert_split(as_header(l), div, as_header(r));
}

// ─── Leaf allocator (SAL via the per-thread active session) ───────
// Allocation runs the leaf_node ctor (header + leaf_init fields). We return
// the deref'd mutable pointer (unique-mode stable); the child's ptr_address
// is recoverable via p->address() (used by leaf_free / inner_set_child).
inline leaf_node* leaf_alloc(ptr_address hint_near = kNoChild) noexcept {
    ptr_address a = (hint_near == kNoChild)
        ? wsession().alloc<leaf_node>(alloc_hint{})
        : wsession().alloc<leaf_node>(alloc_hint{&hint_near, 1});
    return wderef<leaf_node>(a);
}
inline void leaf_free(leaf_node* p) noexcept { wsession().release(p->address()); }

// Make a leaf writable before in-place mutation: get_ref + modify(). If the
// node is unique and in a writable segment, modify() returns it in place (no
// copy — the hot path); if it is read-only (synced) or shared, copy_on_write
// relocates it to fresh writable bytes (the control block moves, so the
// ptr_address is unchanged). Returns the writable pointer. Mirrors the
// proven get_ref().modify() pattern; all subsequent edits use the result.
inline leaf_node* cow_leaf(leaf_node* p) noexcept {
    auto       ref = wsession().get_ref<leaf_node>(p->address());
    leaf_node* w   = p;
    ref.modify([&](leaf_node* n) { w = n; });
    return w;
}

// Clone a leaf to a FRESH ptr_address (new control block), copying the page.
// Unlike cow_leaf (which preserves the address via a CB move), this FORKS: the
// original is untouched and stays valid for a snapshot. Used only by shared-mode
// upsert. leaf_node::copy_to already save/restores dst's SAL header, so no
// manual fixup. Value refs (big/subtree) are NOT retained here — the caller
// calls retain_children so both versions own them.
inline leaf_node* clone_leaf_fresh(const leaf_node* src) noexcept {
    leaf_node* dst = leaf_alloc(src->address());
    src->copy_to(dst);
    return dst;
}

// ─── Diagnostic counters (single-threaded; reset via setters) ─────
inline size_t g_leaf_value_hits = 0;
inline size_t g_leaf_radix_hits = 0;
inline size_t g_leaf_miss       = 0;
inline void leaf_reset_counters() noexcept {
    g_leaf_value_hits = 0;
    g_leaf_radix_hits = 0;
    g_leaf_miss       = 0;
}

// ─── Sentinels + tag-bit convention ────────────────────────────────
inline constexpr uint32_t kLeafStemSentinel = 0xFFFFFFFFu;
inline constexpr uint16_t kLeafOffSentinel  = 0xFFFFu;
inline constexpr uint16_t kRadixTagBit      = 0x8000u;   // high bit of u16 offs
inline constexpr uint16_t kRadixOffMask     = 0x7FFFu;
inline constexpr uint8_t  kRadixGrowChunk   = 4;
inline constexpr uint8_t  kRadixCapMin      = 4;

// ─── Capacity-by-cl helpers (dynamic offs based on cap_in_cls) ────
inline constexpr uint16_t leaf_index_bytes(uint8_t cap_in_cls) noexcept {
    return uint16_t(uint32_t(cap_in_cls) * kCacheLine - kLeafHeaderBytes);
}
inline constexpr uint16_t leaf_stems_for(uint8_t cap_in_cls) noexcept {
    return uint16_t(leaf_index_bytes(cap_in_cls) / 6);
}
inline constexpr uint16_t kLeafCapMinStems = leaf_stems_for(1);                // 18
inline constexpr uint16_t kLeafCapMaxStems = leaf_stems_for(uint8_t(kLeafPageCls));

// ─── stems[] / offs[] accessors ───────────────────────────────────
inline uint32_t* leaf_stems(leaf_node* p) noexcept {
    return reinterpret_cast<uint32_t*>(
        reinterpret_cast<uint8_t*>(p) + kLeafHeaderBytes);
}
inline const uint32_t* leaf_stems(const leaf_node* p) noexcept {
    return reinterpret_cast<const uint32_t*>(
        reinterpret_cast<const uint8_t*>(p) + kLeafHeaderBytes);
}
inline uint16_t* leaf_offs(leaf_node* p) noexcept {
    const uint16_t cap     = leaf_stems_for(p->cap_in_cls);
    const size_t   end_idx = size_t(p->cap_in_cls) * kCacheLine;
    return reinterpret_cast<uint16_t*>(
        reinterpret_cast<uint8_t*>(p) + end_idx - size_t(cap) * sizeof(uint16_t));
}
inline const uint16_t* leaf_offs(const leaf_node* p) noexcept {
    const uint16_t cap     = leaf_stems_for(p->cap_in_cls);
    const size_t   end_idx = size_t(p->cap_in_cls) * kCacheLine;
    return reinterpret_cast<const uint16_t*>(
        reinterpret_cast<const uint8_t*>(p) + end_idx - size_t(cap) * sizeof(uint16_t));
}

// ─── End-relative offset deref ────────────────────────────────────
//
// Heap objects live at [page_end − tail_alloc_pos, page_end).  An
// offset value `off` (low 15 bits, after stripping tag) places the
// object at page + page_end − off.
// page_end is the node's actual size() (4 KB when writable; smaller when this
// leaf has been compacted), so a compacted leaf's heap — placed at its own
// page-end with the same page-end-relative offsets — reads back correctly.
inline uint8_t* leaf_at(leaf_node* p, uint16_t off) noexcept {
    return reinterpret_cast<uint8_t*>(p) + p->size() - (off & kRadixOffMask);
}
inline const uint8_t* leaf_at(const leaf_node* p, uint16_t off) noexcept {
    return reinterpret_cast<const uint8_t*>(p) + p->size() - (off & kRadixOffMask);
}

inline bool offs_is_radix(uint16_t off) noexcept {
    return (off & kRadixTagBit) != 0;
}

// ─── value-region (heap object) — v2 (MVCC-ready) ─────────────────
//
//   [u8 suffix_words] [u8 type_alt] [u8 value_len]
//   [suffix u32s (suffix_words * 4 bytes)] [value bytes (value_len)]
//
// Total size = 3 + suffix_words*4 + value_len.  value_len is now stored
// PER ENTRY (variable-length values), replacing the old tree-wide const.
// type_alt packs [type:3 | has_alt:1 | ver:4]:
//   type  — inline_data / null / tomb / big / subtree (big & subtree:
//           value[] is a 4-byte ptr_address; v1 only writes inline_data)
//   has_alt — an older-versions chain follows (MVCC; v1: 0)
//   ver   — index into the leaf version table (MVCC; v1: 0 = current-only)
// suffix is the byteswapped u32 representation, so memcmp gives u32 order.
struct value_region {
    uint8_t  suffix_words;   // +0
    uint8_t  type_alt;       // +1  [type:3 | has_alt:1 | ver:4]
    uint8_t  value_len;      // +2  per-entry value length (0..255)
    // suffix u32s at +3 ; value bytes at +3 + suffix_words*4

    enum vtype : uint8_t { inline_data = 0, null_val = 1, tomb = 2, big = 3, subtree = 4 };
    uint8_t type()    const noexcept { return type_alt & 0x07; }
    bool    has_alt() const noexcept { return (type_alt & 0x08) != 0; }
    uint8_t ver()     const noexcept { return uint8_t(type_alt >> 4); }
    void    set_type(uint8_t t)  noexcept { type_alt = uint8_t((type_alt & ~0x07) | (t & 0x07)); }
    void    set_has_alt(bool h)  noexcept { type_alt = uint8_t(h ? (type_alt | 0x08) : (type_alt & ~0x08)); }
    void    set_ver(uint8_t v)   noexcept { type_alt = uint8_t((type_alt & 0x0f) | (v << 4)); }

    static constexpr uint16_t kHeaderBytes = 3;
    static constexpr uint16_t size_bytes(uint16_t swords, uint16_t vlen) noexcept {
        return uint16_t(uint32_t(kHeaderBytes) + uint32_t(swords) * 4u + vlen);
    }
    uint16_t total_bytes() const noexcept { return size_bytes(suffix_words, value_len); }
    const uint8_t* suffix_bytes() const noexcept {
        return reinterpret_cast<const uint8_t*>(this) + kHeaderBytes;
    }
    uint8_t* suffix_bytes() noexcept {
        return reinterpret_cast<uint8_t*>(this) + kHeaderBytes;
    }
    // Byte view of the suffix.  We DON'T expose a span<uint32_t> here
    // because value_region sits at an arbitrary heap byte offset — the
    // u32-aligned cast would be UB.  All suffix work in this file uses
    // either memcmp on bytes (equality) or std::memcpy into a local
    // uint32_t (suffix_first_u32).
    std::span<const uint8_t> suffix() const noexcept {
        return std::span(suffix_bytes(), size_t(suffix_words) * 4);
    }
    std::span<uint8_t> suffix() noexcept {
        return std::span(suffix_bytes(), size_t(suffix_words) * 4);
    }
    // value(): the param is ignored (kept for call-site compat); the value
    // pointer is fixed by suffix_words. Length is the stored value_len.
    const uint8_t* value(uint16_t = 0) const noexcept {
        return suffix_bytes() + size_t(suffix_words) * 4;
    }
    uint8_t* value(uint16_t = 0) noexcept {
        return suffix_bytes() + size_t(suffix_words) * 4;
    }
    std::span<const uint8_t> value_span() const noexcept {
        return std::span(value(), size_t(value_len));
    }
    // big/subtree carry a single 4-byte ptr_address in their value bytes
    // (value_len == 4).  These let the lifecycle walk find owned refs.
    bool owns_child_ptr() const noexcept {
        const uint8_t t = type();
        return t == big || t == subtree;
    }
    sal::ptr_address child_ptr() const noexcept {
        sal::ptr_address a;
        std::memcpy(&a, value(), sizeof(a));
        return a;
    }
};

// ─── radix_node (heap object) ─────────────────────────────────────
//
// 2-byte packed header followed by a variable payload:
//
//   byte 0  [bit 7: has_path][bits 6:0: n_entries]    n_entries ≤ 127
//   byte 1  [bit 7: has_term][bits 6:0: cap_slots]    cap_slots ≤ 127
//
//   if has_path:
//     byte 2     [u8 path_words] [u8 _pad[3]]
//     byte 6     [u32 path[path_words]]
//   u32 sub_stems[cap_slots]   ← 2-byte aligned (ARM64/x86 OK)
//   u16 offs[cap_slots]        ← tag-bit convention (nested-radix possible)
//   if has_term: u16 terminal_off
//
// terminal_off: end-relative offset of a value_region for a key that
// ENDS exactly at this radix's depth (the key has no more u32 words
// past the path that led here).  Accessor returns kLeafOffSentinel when
// has_term=0.  This is the end-of-key marker that lets variable-length
// keys sharing a prefix coexist — a key like "Aaron" (5 chars) can sit
// at terminal_off of a radix node whose sub_stems hold the continuations
// for longer keys like "Aaronic", "Aaronical", etc.  The terminal
// value_region's suffix_words is 0 (nothing past the matched path).
//
// Min size (cap=4, no path, no term): 2 + 24 = 26 B.
// With terminal:                       2 + 24 + 2 = 28 B (matches old).
// With compressed path of P words, cap=2, with term:
//                                      2 + 4 + P*4 + 12 + 2 = 20 + P*4 B.
struct radix_node {
    uint8_t b0;   // [bit 7: has_path][bits 6:0: n_entries]
    uint8_t b1;   // [bit 7: has_term][bits 6:0: cap_slots]

    static constexpr uint8_t kHasFlag    = 0x80;
    static constexpr uint8_t kCountMask  = 0x7F;

    // ─── Packed header accessors ──────────────────────────────────
    uint8_t n_entries() const noexcept { return b0 & kCountMask; }
    bool    has_path()  const noexcept { return (b0 & kHasFlag) != 0; }
    uint8_t cap_slots() const noexcept { return b1 & kCountMask; }
    bool    has_term()  const noexcept { return (b1 & kHasFlag) != 0; }

    void set_n_entries(uint8_t n) noexcept {
        assert(n <= kCountMask);
        b0 = uint8_t((b0 & kHasFlag) | (n & kCountMask));
    }
    void set_has_path(bool b) noexcept {
        b0 = uint8_t((b0 & kCountMask) | (b ? kHasFlag : 0));
    }
    void set_cap_slots(uint8_t c) noexcept {
        assert(c <= kCountMask);
        b1 = uint8_t((b1 & kHasFlag) | (c & kCountMask));
    }
    void set_has_term(bool b) noexcept {
        b1 = uint8_t((b1 & kCountMask) | (b ? kHasFlag : 0));
    }

    // ─── Payload layout helpers ───────────────────────────────────
    // sub_stems[] starts after the optional path header.
    size_t sub_stems_byte_offset_() const noexcept {
        return has_path() ? size_t(2 + 4 + size_t(path_words()) * 4)
                          : size_t(2);
    }

    // ─── Path block (only valid when has_path()) ──────────────────
    uint8_t path_words() const noexcept {
        return has_path()
            ? *(reinterpret_cast<const uint8_t*>(this) + 2)
            : uint8_t(0);
    }
    void set_path_words(uint8_t pw) noexcept {
        assert(has_path());
        *(reinterpret_cast<uint8_t*>(this) + 2) = pw;
    }
    uint32_t* path_data() noexcept {
        assert(has_path());
        return reinterpret_cast<uint32_t*>(
            reinterpret_cast<uint8_t*>(this) + 2 + 4);
    }
    const uint32_t* path_data() const noexcept {
        assert(has_path());
        return reinterpret_cast<const uint32_t*>(
            reinterpret_cast<const uint8_t*>(this) + 2 + 4);
    }

    // ─── sub_stems[] and offs[] ───────────────────────────────────
    uint32_t* sub_stems() noexcept {
        return reinterpret_cast<uint32_t*>(
            reinterpret_cast<uint8_t*>(this) + sub_stems_byte_offset_());
    }
    const uint32_t* sub_stems() const noexcept {
        return reinterpret_cast<const uint32_t*>(
            reinterpret_cast<const uint8_t*>(this) + sub_stems_byte_offset_());
    }
    uint16_t* offs() noexcept {
        return reinterpret_cast<uint16_t*>(
            reinterpret_cast<uint8_t*>(this)
            + sub_stems_byte_offset_()
            + size_t(cap_slots()) * sizeof(uint32_t));
    }
    const uint16_t* offs() const noexcept {
        return reinterpret_cast<const uint16_t*>(
            reinterpret_cast<const uint8_t*>(this)
            + sub_stems_byte_offset_()
            + size_t(cap_slots()) * sizeof(uint32_t));
    }

    // ─── Optional terminal_off (only valid when has_term()) ───────
    // Accessor returns kLeafOffSentinel when has_term=0 so callers can
    // continue to use "== kLeafOffSentinel" as the "no terminal" check
    // uniformly.
    uint16_t terminal_off() const noexcept {
        if (!has_term()) return kLeafOffSentinel;
        const auto* p = reinterpret_cast<const uint8_t*>(this)
            + sub_stems_byte_offset_()
            + size_t(cap_slots()) * (sizeof(uint32_t) + sizeof(uint16_t));
        uint16_t v;
        std::memcpy(&v, p, sizeof(v));
        return v;
    }
    void set_terminal_off(uint16_t v) noexcept {
        assert(has_term());
        auto* p = reinterpret_cast<uint8_t*>(this)
            + sub_stems_byte_offset_()
            + size_t(cap_slots()) * (sizeof(uint32_t) + sizeof(uint16_t));
        std::memcpy(p, &v, sizeof(v));
    }

    // ─── Total bytes for a given (cap_slots, path_words, has_term) ─
    static constexpr uint16_t size_bytes(uint8_t cap_slots,
                                          uint8_t path_words = 0,
                                          bool    has_term  = true) noexcept {
        uint32_t total = 2u;
        if (path_words > 0) total += 4u + uint32_t(path_words) * 4u;
        total += uint32_t(cap_slots) * (sizeof(uint32_t) + sizeof(uint16_t));
        if (has_term) total += 2u;
        return uint16_t(total);
    }
};
static_assert(sizeof(radix_node) == 2, "radix_node header must be 2 B");

// ─── SIMD count_lt (full-capacity scan, sentinel-padded) ──────────
//
// Scans cap u32s.  Unused slots are 0xFFFFFFFF and never count as <q,
// so we get the correct insertion position without an n-bound check.
// Caller guarantees cap is a multiple of 4 (for stems[] this is true
// because index slot pairs are 6 B and we round to multiples of 4
// slots; for sub_stems[] of a radix node, cap_slots is always
// multiple of 4).
inline uint16_t count_lt_capped(const uint32_t* stems, uint16_t cap, uint32_t q) noexcept {
    const uint32x4_t tgt = vdupq_n_u32(q);
    int32_t sum = 0;
    for (uint16_t i = 0; i < cap; i += 4) {
        const uint32x4_t v = vld1q_u32(stems + i);
        const uint32x4_t c = vcltq_u32(v, tgt);
        sum += vaddvq_s32(vreinterpretq_s32_u32(c));
    }
    // Each "less" lane contributes -1 to vaddvq sum.
    return uint16_t(-sum);
}

// ─── Heap allocation (tail-bump) ──────────────────────────────────
//
// Returns end-relative offset suitable for storing in offs[] (with the
// caller setting the tag bit if it's a radix node).  Returns
// kLeafOffSentinel if not enough free space between the index region
// and the heap.
//
// Allocation size is rounded UP to a multiple of 4 so the resulting
// pointer is 4-byte aligned (radix_node has u32 fields).  Pages are
// kLeafPageBytes-aligned and kLeafPageBytes is a multiple of 4, so
// (page_end - multiple_of_4) is 4-byte aligned.
inline uint16_t leaf_heap_alloc(leaf_node* p, uint16_t bytes) noexcept {
    const uint16_t bytes4 = uint16_t((bytes + 3u) & ~uint16_t(3));
    const size_t index_end = size_t(p->cap_in_cls) * kCacheLine;
    // Available heap = size() − index_end (size() is 4 KB on any writable leaf,
    // since COW always expands to a full page; heap_alloc only runs on writable
    // leaves).  Used so far = tail_alloc_pos.  Reject if alloc would exceed it.
    const uint32_t avail    = uint32_t(p->size() - index_end);
    const uint32_t new_used = uint32_t(p->tail_alloc_pos) + uint32_t(bytes4);
    if (new_used > avail) return kLeafOffSentinel;
    p->tail_alloc_pos = uint16_t(new_used);
    return p->tail_alloc_pos;
}

// ─── Initialization ───────────────────────────────────────────────
//
// Fixed layout — only writes the few header fields we'll consult.
// Stems / offs / heap go untouched.
// Sets only the wtree fields; the 12-byte SAL header (size/type/addr) is set
// by the ctor (= alloc). Safe to call on an already-allocated leaf (re-init).
inline void leaf_init(leaf_node* p) noexcept {
    p->tail_alloc_pos = 0;
    p->cap_in_cls     = 1;   // start small; grow via leaf_grow_index
    p->num_branches   = 0;
    p->prefix_len     = 0;   // no self-asserted prefix yet
    p->prefix_data    = 0;
    p->num_keys       = 0;
    p->dead_bytes     = 0;
    p->_pad2          = 0;
}

// Record `raw` heap bytes (a value_region / radix_node about to be abandoned)
// as dead, matching leaf_heap_alloc's 4-byte rounding so the counter tracks
// tail_alloc_pos exactly. Saturates at u16 max (counter is advisory + a safe
// shrink hint — never over-counts because of the cap, see compact sizing).
inline void leaf_add_dead(leaf_node* p, uint16_t raw) noexcept {
    const uint32_t rounded = (uint32_t(raw) + 3u) & ~uint32_t(3);
    const uint32_t d = uint32_t(p->dead_bytes) + rounded;
    p->dead_bytes = uint16_t(d > 0xFFFFu ? 0xFFFFu : d);
}

// SAL placement ctor: header from (asize, seq), then the wtree fields.
inline leaf_node::leaf_node(uint32_t asize, ptr_address_seq seq) noexcept
    : node(asize, type_id, seq) {
    leaf_init(this);
}

// ─── Round capacity up to multiple of 4 for SIMD ──────────────────
//
// count_lt_capped processes 4 stems at a time.  We always allocate
// stems[] with at least leaf_stems_for(cap_in_cls) slots, but the
// SIMD scan reads in groups of 4.  The "extra" up-to-3 slots past
// leaf_stems_for() can overlap with the start of the offs[] region,
// which is fine since unused stem slots are 0xFFFFFFFF sentinels and
// the SIMD scan only counts them in the negative direction.
//
// Wait — the offs[] region holds u16 values, not u32 sentinels.  So
// reading u32s past leaf_stems_for() would interpret offs bytes as
// stem values.  Those WON'T be 0xFFFFFFFF.  We must round DOWN, not
// UP — i.e., scan only the multiple-of-4 stems that fit cleanly.
//
// For leaf_stems_for(1)=18, scan 16 stems (multiple of 4).  Slots
// 16..17 are real stems but won't be scanned.  Fall back to scalar
// for the tail (up to 3 stems).
inline uint16_t leaf_scan_cap_simd(uint16_t cap) noexcept {
    return uint16_t(cap & ~uint16_t(3));   // round DOWN
}

// count_lt with EARLY EXIT (v2 leaf_u32_suffix::find_pos_ pattern).
//
// Stems are sorted ascending: once a 32-stem cacheline reports k<32
// less-than matches, the rest of the array is all ≥ q — return
// immediately.  Halves the average scan vs reading the full live
// region for random keys.  Tail (< 32 stems left) is scalar with
// early termination on stems[i] ≥ q.
inline uint16_t leaf_count_lt(const uint32_t* stems, uint16_t num_keys,
                                uint16_t /*cap*/, uint32_t q) noexcept {
    constexpr size_t kStemsPerCL = kCacheLine / sizeof(uint32_t); // 32
    const uint32x4_t tgt  = vdupq_n_u32(q);
    const size_t     full_cls = size_t(num_keys) / kStemsPerCL;
    const uint32_t* p          = stems;
    const uint32_t* const cl_end = stems + full_cls * kStemsPerCL;
    const uint32_t* const end    = stems + num_keys;
    for (; p < cl_end; p += kStemsPerCL) {
        int32_t s = 0;
        #pragma unroll
        for (size_t c = 0; c < 8; ++c) {
            const uint32x4_t v  = vld1q_u32(p + c * 4);
            const uint32x4_t lt = vcltq_u32(v, tgt);
            s += vaddvq_s32(vreinterpretq_s32_u32(lt));
        }
        const uint32_t k = uint32_t(-s);
        if (k < kStemsPerCL) return uint16_t(uintptr_t(p - stems) + k);
    }
    for (; p < end; ++p) {
        if (*p >= q) return uint16_t(uintptr_t(p - stems));
    }
    return num_keys;
}

// ─── Forward decl: radix walk used by leaf_find ───────────────────
inline value_type radix_find(const leaf_node* p, uint16_t root_off,
                                  word_key_type key,
                                  uint16_t consumed_words,
                                  uint16_t value_len) noexcept;

// ─── leaf_find ────────────────────────────────────────────────────
//
// Look up `key` (already pre-bswap'd into u32 words) at this leaf's
// `depth`.  Returns pointer to the value or nullptr.
//
// Walk:
//   1. count_lt on stems[] → pos.
//   2. If stems[pos] != q, miss.
//   3. Dispatch on offs[pos]: value-region (verify full suffix) or
//      radix root (descend via radix_find).
inline value_type leaf_find(const leaf_node* p,
                                 word_key_type key,
                                 uint16_t depth, uint16_t value_len) noexcept {
    assert(depth < key.size());
    const uint32_t  q          = key[depth];
    const uint8_t* const pbase = reinterpret_cast<const uint8_t*>(p);
    const uint8_t  cap_in_cls  = p->cap_in_cls;
    const uint16_t cap         = leaf_stems_for(cap_in_cls);
    const uint16_t num_keys    = p->num_keys;
    const uint32_t* stems = reinterpret_cast<const uint32_t*>(pbase + kLeafHeaderBytes);
    const uint16_t* offs = reinterpret_cast<const uint16_t*>(
        pbase + size_t(cap_in_cls) * kCacheLine - size_t(cap) * sizeof(uint16_t));
    // Prefetch offs CLs (skip for tiny leaves where everything fits CL 0).
    if (cap_in_cls >= 2) {
        __builtin_prefetch(offs, 0, 1);
        const size_t offs_second_cl = size_t(num_keys > 64) * size_t(kCacheLine);
        __builtin_prefetch(reinterpret_cast<const char*>(offs) + offs_second_cl, 0, 1);
    }

    const uint16_t pos = leaf_count_lt(stems, num_keys, cap, q);
    if (pos >= num_keys) {
#ifdef LEAF_STATS
        ++g_leaf_miss;
#endif
        return value_type{};
    }
    const uint16_t off = offs[pos];   // warm from prefetch
    // Prefetch value_region NOW — stems[pos] verify + radix check +
    // suffix-len compute below gives ~10+ cycles to overlap the cold
    // heap CL.
    const uint16_t off_unmasked = uint16_t(off & kRadixOffMask);
    const auto* vr = reinterpret_cast<const value_region*>(
        pbase + p->size() - off_unmasked);   // page-end-relative (size = 4 KB, or smaller if compacted)
    __builtin_prefetch(vr, 0, 3);

    if (stems[pos] != q) {
#ifdef LEAF_STATS
        ++g_leaf_miss;
#endif
        return value_type{};
    }
    assert(off != kLeafOffSentinel);
    if (offs_is_radix(off)) {
#ifdef LEAF_STATS
        ++g_leaf_radix_hits;
#endif
        return radix_find(p, off_unmasked, key, /*consumed_words=*/uint16_t(depth + 1),
                          value_len);
    }
#ifdef LEAF_STATS
    ++g_leaf_value_hits;
#endif
    // Suffix is the remainder of the key past (depth+1) consumed words.
    const auto suffix = key.subspan(size_t(depth) + 1);
    if (vr->suffix_words != uint8_t(suffix.size())) return value_type{};
    if (!suffix.empty() &&
        std::memcmp(vr->suffix_bytes(), suffix.data(), suffix.size_bytes()) != 0) {
        return value_type{};
    }
    return vr->value_span();
}

// ─── radix_find ───────────────────────────────────────────────────
//
// Walks the radix tree.  Each level consumes one more u32 from k_u32
// past the leaf's depth.  Verifies the remaining suffix in the
// terminal value-region.
inline value_type radix_find(const leaf_node* p, uint16_t root_off,
                                  word_key_type key,
                                  uint16_t consumed_words,
                                  uint16_t value_len) noexcept {
    const uint8_t* const page_end = reinterpret_cast<const uint8_t*>(p) + p->size();
    uint16_t off = root_off;
    while (true) {
        const auto* rn = reinterpret_cast<const radix_node*>(page_end - off);
        // Path-aware descent: when has_path, the query must match the
        // compressed path words before any sub_stem / terminal handling.
        if (rn->has_path()) {
            const uint8_t pw = rn->path_words();
            if (size_t(consumed_words) + pw > key.size()) return value_type{};
            if (std::memcmp(rn->path_data(),
                            key.data() + consumed_words,
                            size_t(pw) * 4) != 0) {
                return value_type{};
            }
            consumed_words = uint16_t(consumed_words + pw);
        }
        // End-of-key: query has no more words past this radix's depth.
        // Look at the terminal slot; if present, that's the match.
        if (consumed_words >= key.size()) {
            const uint16_t toff = rn->terminal_off();
            if (toff == kLeafOffSentinel) return value_type{};
            const auto* vr = reinterpret_cast<const value_region*>(page_end - toff);
            assert(vr->suffix_words == 0);
            return vr->value_span();
        }
        const uint32_t qw   = key[consumed_words];
        const uint16_t rcap = rn->cap_slots();
        const uint32_t* sub = rn->sub_stems();
        const uint16_t rpos = count_lt_capped(sub, rcap, qw);
        if (rpos >= rn->n_entries() || sub[rpos] != qw) return value_type{};
        const uint16_t next_off = rn->offs()[rpos];
        assert(next_off != kLeafOffSentinel);
        ++consumed_words;
        if (!offs_is_radix(next_off)) {
            const auto* vr = reinterpret_cast<const value_region*>(
                page_end - next_off);
            const auto suffix = key.subspan(consumed_words);
            if (vr->suffix_words != uint8_t(suffix.size())) return value_type{};
            if (!suffix.empty() &&
                std::memcmp(vr->suffix_bytes(), suffix.data(), suffix.size_bytes()) != 0) {
                return value_type{};
            }
            return vr->value_span();
        }
        off = next_off & kRadixOffMask;
    }
}

// ─── leaf_live_bytes: sum REACHABLE heap objects (4-byte-rounded) ──────
// Walks every stem's value_region / radix subgraph the way leaf_heap_alloc
// sized them. dead_bytes must always equal tail_alloc_pos - leaf_live_bytes
// (verified in tests). Also the exact basis for a dead-recovering compaction.
inline uint32_t leaf_subgraph_bytes_(const leaf_node* p, uint16_t off_tagged,
                                     uint16_t value_len) noexcept {
    const uint16_t off = off_tagged & kRadixOffMask;
    if (!offs_is_radix(off_tagged)) {
        const auto* vr = reinterpret_cast<const value_region*>(leaf_at(p, off));
        return uint32_t(value_region::size_bytes(vr->suffix_words, vr->value_len) + 3u) & ~3u;
    }
    const auto* rn = reinterpret_cast<const radix_node*>(leaf_at(p, off));
    uint32_t sum = uint32_t(radix_node::size_bytes(rn->cap_slots(), rn->path_words(),
                                                   rn->has_term()) + 3u) & ~3u;
    if (rn->has_term() && rn->terminal_off() != kLeafOffSentinel)
        sum += leaf_subgraph_bytes_(p, rn->terminal_off(), value_len);   // untagged value_region
    const uint8_t n = rn->n_entries();
    const uint16_t* os = rn->offs();
    for (uint8_t i = 0; i < n; ++i)
        if (os[i] != kLeafOffSentinel)
            sum += leaf_subgraph_bytes_(p, os[i], value_len);
    return sum;
}
inline uint32_t leaf_live_bytes(const leaf_node* p, uint16_t value_len) noexcept {
    uint32_t sum = 0;
    const uint16_t* offs = leaf_offs(p);
    for (uint16_t i = 0; i < p->num_keys; ++i)
        if (offs[i] != kLeafOffSentinel)
            sum += leaf_subgraph_bytes_(p, offs[i], value_len);
    return sum;
}

// ─── value_region walk (lifecycle / GC) ───────────────────────────
// Visit every reachable value_region in the leaf, recursing the in-leaf radix
// the same way leaf_subgraph_bytes_ does (terminal + each live sub-slot).  fn
// is invoked with (const value_region*).  CURRENT versions only — the alt/
// history chain walk is added in §4 (STUB: every value_region here is the
// single current version).
template <typename F>
inline void leaf_for_each_value_region_(const leaf_node* p, uint16_t off_tagged,
                                        F& fn) noexcept {
    if (!offs_is_radix(off_tagged)) {
        fn(reinterpret_cast<const value_region*>(leaf_at(p, off_tagged & kRadixOffMask)));
        return;
    }
    const auto* rn = reinterpret_cast<const radix_node*>(leaf_at(p, off_tagged & kRadixOffMask));
    if (rn->has_term() && rn->terminal_off() != kLeafOffSentinel)
        leaf_for_each_value_region_(p, rn->terminal_off(), fn);
    const uint8_t   n  = rn->n_entries();
    const uint16_t* os = rn->offs();
    for (uint8_t i = 0; i < n; ++i)
        if (os[i] != kLeafOffSentinel)
            leaf_for_each_value_region_(p, os[i], fn);
}
template <typename F>
inline void leaf_for_each_value_region(const leaf_node* p, F&& fn) noexcept {
    const uint16_t* offs = leaf_offs(p);
    for (uint16_t i = 0; i < p->num_keys; ++i)
        if (offs[i] != kLeafOffSentinel)
            leaf_for_each_value_region_(p, offs[i], fn);
}

// ── Lifecycle method definitions (declared in struct leaf_node) ───
inline void leaf_node::destroy(const sal::allocator_session_ptr& session) const noexcept {
    leaf_for_each_value_region(this, [&](const value_region* vr) {
        if (vr->owns_child_ptr()) session->release(vr->child_ptr());
    });
}
inline void leaf_node::visit_children(
    const std::function<void(sal::ptr_address)>& v) const noexcept {
    leaf_for_each_value_region(this, [&](const value_region* vr) {
        if (vr->owns_child_ptr()) v(vr->child_ptr());
    });
}
inline void leaf_node::retain_children(const sal::allocator_session_ptr& session) const noexcept {
    leaf_for_each_value_region(this, [&](const value_region* vr) {
        if (vr->owns_child_ptr()) session->retain(vr->child_ptr());
    });
}

// Release the out-of-line ref a value_region owns (big/subtree) just before its
// heap space is abandoned (overwrite-replace / remove / prune).  No-op for
// inline/null/tomb.  NOT called on collision re-placement, where the value
// bytes (and thus the ref) are MOVED to a fresh region — ownership transfers,
// no release.  Reads the active session via g_wtree_session.
inline void leaf_retire_value_region_(const value_region* vr) noexcept {
    if (vr->owns_child_ptr()) wsession().release(vr->child_ptr());
}

// Retain every out-of-line value ref (big/subtree) the leaf owns. Called by the
// shared-mode eager-clone: the fresh clone and the snapshot's original now both
// own the refs. Pairs with the overwrite/remove retire-release so the snapshot
// keeps its values. No-op while values are inline-only.
inline void leaf_retain_value_refs_(const leaf_node* p) noexcept {
    leaf_for_each_value_region(p, [](const value_region* vr) {
        if (vr->owns_child_ptr()) wsession().retain(vr->child_ptr());
    });
}

// ─── leaf_insert (forward decl; defined below) ────────────────────
//
// Returns:
//   true  on success (entry inserted or updated in place)
//   false if leaf is full (caller must split)
inline bool leaf_insert(leaf_node* p,
                         word_key_type key,
                         uint16_t depth,
                         value_type value) noexcept;

// ─── leaf_grow_index ─────────────────────────────────────────────
//
// Increase cap_in_cls by 1.  Re-positions offs[] to the new end of
// the index region (memmove of existing num_keys offs bytes).
inline bool leaf_grow_index(leaf_node* p) noexcept {
    if (p->cap_in_cls >= kLeafPageCls) return false;
    const size_t new_cap_in_cls = p->cap_in_cls + 1;
    const size_t new_index_end  = new_cap_in_cls * kCacheLine;
    if (kLeafPageBytes - size_t(p->tail_alloc_pos) < new_index_end) return false;
    const uint16_t new_cap = leaf_stems_for(uint8_t(new_cap_in_cls));
    const uint16_t* old_offs = leaf_offs(p);
    uint8_t* const pbytes = reinterpret_cast<uint8_t*>(p);
    uint16_t* new_offs = reinterpret_cast<uint16_t*>(
        pbytes + new_index_end - size_t(new_cap) * sizeof(uint16_t));
    std::memmove(new_offs, old_offs, size_t(p->num_keys) * sizeof(uint16_t));
    p->cap_in_cls = uint8_t(new_cap_in_cls);
    return true;
}

// ─── Helpers: read first u32 from a value-region's suffix ─────────
//
// When promoting a 1-entry-stem to a radix on collision, we need the
// "next u32 word" of the existing entry's key.  With u32-byteswapped
// storage, this is a memcpy of the first 4 bytes (returns 0 for an
// empty suffix).  Bytes may be unaligned in the page heap.
inline uint32_t suffix_first_u32(std::span<const uint8_t> suffix_bytes_view) noexcept {
    if (suffix_bytes_view.size() < 4) return 0u;
    uint32_t v;
    std::memcpy(&v, suffix_bytes_view.data(), 4);
    return v;
}

// ─── radix_node alloc + grow ──────────────────────────────────────
//
// Allocate a fresh radix node with `cap_slots` (multiple of kRadixGrowChunk).
// `path_words` > 0 turns on has_path; the caller must populate path_data()
// afterwards.  `has_term` decides whether the trailing 2-byte terminal_off
// slot is reserved (initialised to kLeafOffSentinel).
//
// Defaults preserve the pre-refactor footprint: no path block, terminal
// slot reserved.  Phase-2 path-compressed chain nodes pass has_term=false
// to avoid the 2 wasted bytes when they're known to never carry a
// terminal entry.
//
// Returns end-relative offset, or kLeafOffSentinel on heap exhaustion.
inline uint16_t alloc_radix_node(leaf_node* p,
                                   uint8_t cap_slots,
                                   uint8_t path_words = 0,
                                   bool    has_term  = true) noexcept {
    const uint16_t bytes = radix_node::size_bytes(cap_slots, path_words, has_term);
    const uint16_t off = leaf_heap_alloc(p, bytes);
    if (off == kLeafOffSentinel) return kLeafOffSentinel;
    auto* rn = reinterpret_cast<radix_node*>(leaf_at(p, off));
    rn->b0 = uint8_t((path_words > 0) ? radix_node::kHasFlag : 0);
    rn->b1 = uint8_t((has_term ? radix_node::kHasFlag : 0) | (cap_slots & radix_node::kCountMask));
    if (path_words > 0) rn->set_path_words(path_words);
    if (has_term) rn->set_terminal_off(kLeafOffSentinel);   // no end-of-key entry yet
    auto* ss = rn->sub_stems();
    auto* os = rn->offs();
    for (uint16_t i = 0; i < cap_slots; ++i) ss[i] = kLeafStemSentinel;
    for (uint16_t i = 0; i < cap_slots; ++i) os[i] = kLeafOffSentinel;
    return off;
}

// Grow a radix node by +kRadixGrowChunk cap_slots.  Preserves path_words,
// path[], has_term, and terminal_off across the realloc.  Returns new
// offset (tag bit clear).  Old node's heap bytes leak until a compactor
// reclaims them.
inline uint16_t grow_radix_node(leaf_node* p, uint16_t old_off) noexcept {
    auto* old_rn = reinterpret_cast<radix_node*>(leaf_at(p, old_off));
    const uint8_t new_cap = uint8_t(old_rn->cap_slots() + kRadixGrowChunk);
    const uint8_t pw      = old_rn->path_words();
    const bool    ht      = old_rn->has_term();
    const uint16_t new_off = alloc_radix_node(p, new_cap, pw, ht);
    if (new_off == kLeafOffSentinel) return kLeafOffSentinel;
    // alloc_radix_node may have moved memory; re-load old pointer.
    old_rn = reinterpret_cast<radix_node*>(leaf_at(p, old_off));
    auto* new_rn = reinterpret_cast<radix_node*>(leaf_at(p, new_off));
    const uint8_t n = old_rn->n_entries();
    new_rn->set_n_entries(n);
    if (pw > 0) std::memcpy(new_rn->path_data(), old_rn->path_data(), size_t(pw) * 4);
    if (ht) new_rn->set_terminal_off(old_rn->terminal_off());
    std::memcpy(new_rn->sub_stems(), old_rn->sub_stems(), n * sizeof(uint32_t));
    std::memcpy(new_rn->offs(),      old_rn->offs(),      n * sizeof(uint16_t));
    // The old node's heap bytes are now unreachable → record as dead.
    leaf_add_dead(p, radix_node::size_bytes(uint8_t(new_cap - kRadixGrowChunk), pw, ht));
    return new_off;
}

// ─── leaf_value_overwrite ────────────────────────────────────────
//
// Overwrite the value of an existing value_region (the key already
// resolved to this slot).  When the new length equals the stored
// length the value is patched in place and the original offset is
// returned.  On a size change a fresh region (same suffix) is
// allocated, the old one is marked dead, and the NEW offset is
// returned — the caller must write it back into its slot (and reload
// any container pointer, since the heap alloc may have moved the heap
// base in the caller's view).  Returns kLeafOffSentinel when the heap
// is full → caller falls back to grow/split.
inline uint16_t leaf_value_overwrite(leaf_node* p, uint16_t cur_off,
                                     value_type value) noexcept {
    auto*          vr      = reinterpret_cast<value_region*>(leaf_at(p, cur_off));
    const uint16_t new_len = uint16_t(value.size());
    if (vr->value_len == new_len) {
        // In-place patch.  leaf_value_overwrite only ever writes INLINE bytes,
        // so it must not silently overwrite a big/subtree ptr while leaving the
        // type tag set (the bytes would later be misread as a live ptr).  Once
        // big-typed writes land they need a dedicated path that releases the old
        // ref and rewrites the type; this assert is the tripwire for that.
        assert(!vr->owns_child_ptr()
               && "big/subtree in-place overwrite needs type handling (§ later)");
        std::memcpy(vr->value(), value.data(), new_len);
        return cur_off;                       // in place; slot unchanged
    }
    // Size change → re-home into a fresh region.  Snapshot suffix first
    // (the alloc may bump the heap past `vr`).
    const uint8_t  swords    = vr->suffix_words;
    const uint16_t old_total = vr->total_bytes();
    uint8_t        suf_buf[256 * 4];
    assert(size_t(swords) * 4 <= sizeof(suf_buf));
    if (swords > 0) std::memcpy(suf_buf, vr->suffix_bytes(), size_t(swords) * 4);
    const uint16_t noff = leaf_heap_alloc(p, value_region::size_bytes(swords, new_len));
    if (noff == kLeafOffSentinel) return kLeafOffSentinel;   // old intact → no drop
    auto* nvr = reinterpret_cast<value_region*>(leaf_at(p, noff));
    nvr->suffix_words = swords; nvr->value_len = uint8_t(new_len); nvr->type_alt = 0;
    if (swords > 0) std::memcpy(nvr->suffix_bytes(), suf_buf, size_t(swords) * 4);
    std::memcpy(nvr->value(), value.data(), new_len);
    // Old region is now abandoned and its value REPLACED (not moved) → drop any
    // ref it owned, then mark its bytes dead.  vr is still valid (heap alloc
    // bump-allocates; existing objects don't move).
    leaf_retire_value_region_(vr);
    leaf_add_dead(p, old_total);
    return noff;
}

// ─── radix_insert ────────────────────────────────────────────────
//
// Insert (or update) one entry into a radix tree rooted at root_off.
// Returns the (possibly updated) root offset of the radix tree (tag
// bit clear).  Returns kLeafOffSentinel on heap exhaustion.
//
// On collision with another existing entry, recursively allocates
// deeper radix levels until the keys distinguish.
inline uint16_t radix_insert(leaf_node* p, uint16_t root_off,
                              word_key_type key,
                              uint16_t consumed_words,
                              value_type value) noexcept {
    const uint16_t value_len = uint16_t(value.size());
    // Walk down to the right radix node.  Caller already has root_off
    // (no tag bit).  We may grow nodes along the way.
    uint16_t cur_off = root_off;
    // Track the chain of (parent_off, slot_in_parent) so we can update
    // pointers after a child grows.
    // Max chain depth = number of radix levels = key.size() in u32 words.
    // kVarlenEncBuf=432B → 108 u32s max.  Size to 128 for safety.
    struct ChainEntry { uint16_t parent_off; uint16_t slot; };
    ChainEntry chain[128];
    uint16_t chain_n = 0;

    // Helper: write a fresh value-region with the given suffix words
    // (u32 span) and value bytes.  Returns the heap offset, or sentinel
    // on allocation failure.
    auto write_value_region = [&](std::span<const uint32_t> suffix_words_span)
        -> uint16_t {
        const uint16_t voff = leaf_heap_alloc(
            p, value_region::size_bytes(uint16_t(suffix_words_span.size()), value_len));
        if (voff == kLeafOffSentinel) return kLeafOffSentinel;
        auto* vr = reinterpret_cast<value_region*>(leaf_at(p, voff));
        vr->suffix_words = uint8_t(suffix_words_span.size()); vr->value_len = uint8_t(value_len); vr->type_alt = 0;
        if (!suffix_words_span.empty()) {
            std::memcpy(vr->suffix_bytes(), suffix_words_span.data(),
                        suffix_words_span.size_bytes());
        }
        std::memcpy(vr->value(value_len), value.data(), value_len);
        return voff;
    };

    while (true) {
        auto* rn = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
        // ─── Path-aware descent ───────────────────────────────────
        // Compressed nodes carry path_words() leading u32s that the
        // query must match exactly before any sub_stem / terminal
        // handling.  A mid-path divergence triggers a Patricia split
        // (rewrites the compressed node into a root + tail pair).
        if (rn->has_path()) {
            const uint8_t pw = rn->path_words();
            const uint32_t* path = rn->path_data();
            const size_t max_match = (size_t(consumed_words) < key.size())
                ? std::min<size_t>(size_t(pw), key.size() - consumed_words)
                : 0;
            size_t match_len = 0;
            while (match_len < max_match) {
                if (path[match_len] != key[consumed_words + match_len]) break;
                ++match_len;
            }
            if (match_len == pw) {
                // Full path match: advance past path and fall through to
                // the existing terminal / sub_stem logic.
                consumed_words = uint16_t(consumed_words + pw);
            } else {
                // ─── Patricia split ───────────────────────────────
                // Old layout: rn { path=[A..pw-1], sub_stems, offs, [term] }
                // New:       root { path=[A..match_len-1], 1 or 2 sub_stems }
                //             ├─ NEW value (terminal or sub_stem)
                //             └─ tail { path=[match_len+1..pw-1], original
                //                       sub_stems / offs / terminal }
                //
                // Snapshot the old node's state into stack buffers FIRST,
                // since alloc_radix_node may shift the heap pointer (well —
                // tail/root land at lower offsets but rn stays put; still
                // be defensive).
                const uint8_t cap_old = rn->cap_slots();
                const bool    ht_old  = rn->has_term();
                const uint16_t term_old = rn->terminal_off();
                const uint8_t  n_old  = rn->n_entries();
                uint32_t path_buf[128];
                assert(pw <= sizeof(path_buf) / sizeof(uint32_t));
                std::memcpy(path_buf, path, size_t(pw) * 4);
                uint32_t sub_buf[128];
                uint16_t offs_buf[128];
                assert(n_old <= sizeof(sub_buf) / sizeof(uint32_t));
                std::memcpy(sub_buf,  rn->sub_stems(), size_t(n_old) * 4);
                std::memcpy(offs_buf, rn->offs(),      size_t(n_old) * 2);

                // ── Allocate the tail (carries path words AFTER the
                // divergent one — i.e. path_buf[match_len+1 .. pw-1]).
                const uint8_t tail_pw = uint8_t(pw - match_len - 1);
                const uint16_t tail_off = alloc_radix_node(p, cap_old, tail_pw, ht_old);
                if (tail_off == kLeafOffSentinel) return kLeafOffSentinel;
                auto* tail_rn = reinterpret_cast<radix_node*>(leaf_at(p, tail_off));
                if (tail_pw > 0) {
                    std::memcpy(tail_rn->path_data(),
                                path_buf + match_len + 1,
                                size_t(tail_pw) * 4);
                }
                std::memcpy(tail_rn->sub_stems(), sub_buf,  size_t(n_old) * 4);
                std::memcpy(tail_rn->offs(),      offs_buf, size_t(n_old) * 2);
                if (ht_old) tail_rn->set_terminal_off(term_old);
                tail_rn->set_n_entries(n_old);

                // ── Allocate NEW's value_region.  If the new key ends
                // exactly at the split point, it goes to root's terminal_off;
                // otherwise it's an ordinary entry under root[sub_stems[?]].
                const bool new_ends_at_split =
                    (size_t(consumed_words) + match_len >= key.size());
                const uint32_t tail_first_word = path_buf[match_len];
                uint16_t new_voff;
                if (new_ends_at_split) {
                    new_voff = write_value_region(word_key_type{});
                } else {
                    new_voff = write_value_region(
                        key.subspan(size_t(consumed_words) + match_len + 1));
                }
                if (new_voff == kLeafOffSentinel) return kLeafOffSentinel;

                // ── Allocate root node (path = match_len leading words).
                const uint8_t root_pw = uint8_t(match_len);
                const uint16_t root_off_new = alloc_radix_node(
                    p, kRadixCapMin, root_pw, /*has_term=*/true);
                if (root_off_new == kLeafOffSentinel) return kLeafOffSentinel;
                auto* root_rn = reinterpret_cast<radix_node*>(leaf_at(p, root_off_new));
                if (root_pw > 0) {
                    std::memcpy(root_rn->path_data(), path_buf, size_t(root_pw) * 4);
                }
                if (new_ends_at_split) {
                    root_rn->sub_stems()[0] = tail_first_word;
                    root_rn->offs()[0]      = uint16_t(tail_off | kRadixTagBit);
                    root_rn->set_terminal_off(new_voff);
                    root_rn->set_n_entries(1);
                } else {
                    const uint32_t new_div_word = key[consumed_words + match_len];
                    const bool tail_first = (tail_first_word < new_div_word);
                    root_rn->sub_stems()[0] = tail_first ? tail_first_word : new_div_word;
                    root_rn->offs()[0]      = tail_first ? uint16_t(tail_off | kRadixTagBit) : new_voff;
                    root_rn->sub_stems()[1] = tail_first ? new_div_word : tail_first_word;
                    root_rn->offs()[1]      = tail_first ? new_voff : uint16_t(tail_off | kRadixTagBit);
                    root_rn->set_n_entries(2);
                }
                // The old compressed node (its content moved to tail/root) is
                // now unreachable → record as dead.
                leaf_add_dead(p, radix_node::size_bytes(cap_old, pw, ht_old));
                // ── Splice into parent (or update root_off).
                if (chain_n > 0) {
                    auto& back = chain[chain_n - 1];
                    auto* par = reinterpret_cast<radix_node*>(leaf_at(p, back.parent_off));
                    par->offs()[back.slot] = uint16_t(root_off_new | kRadixTagBit);
                } else {
                    root_off = root_off_new;
                }
                return root_off;
            }
        }
        // End-of-key: new key has no more words.  Set/replace the
        // terminal slot with a value_region containing just the value.
        if (consumed_words >= key.size()) {
            const uint16_t cur_term = rn->terminal_off();
            if (cur_term == kLeafOffSentinel) {
                const uint16_t voff = write_value_region(word_key_type{});
                if (voff == kLeafOffSentinel) return kLeafOffSentinel;
                rn = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
                rn->set_terminal_off(voff);
            } else {
                // Overwrite existing terminal value (resize-aware).
                const uint16_t nvoff = leaf_value_overwrite(p, cur_term, value);
                if (nvoff == kLeafOffSentinel) return kLeafOffSentinel;
                if (nvoff != cur_term) {
                    rn = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
                    rn->set_terminal_off(nvoff);
                }
            }
            return root_off;
        }
        const uint32_t qw = key[consumed_words];
        const uint16_t rcap = rn->cap_slots();
        uint32_t* sub = rn->sub_stems();
        uint16_t pos = count_lt_capped(sub, rcap, qw);
        const bool exact = (pos < rcap && sub[pos] == qw);

        if (exact) {
            uint16_t* offs = rn->offs();
            uint16_t  child_off = offs[pos];
            if (child_off == kLeafOffSentinel) {
                const uint16_t voff = write_value_region(key.subspan(size_t(consumed_words) + 1));
                if (voff == kLeafOffSentinel) return kLeafOffSentinel;
                rn = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
                rn->offs()[pos] = voff;
                rn->set_n_entries(uint8_t(rn->n_entries() + 1));
                return root_off;
            }
            if (offs_is_radix(child_off)) {
                chain[chain_n++] = {cur_off, pos};
                cur_off = child_off & kRadixOffMask;
                ++consumed_words;
                continue;
            }
            // Child is a value-region at slot key[consumed_words]=qw.
            // OLD value_region's suffix represents key[consumed_words+1..].
            // Promote into a sub_rn at depth+1 (= consumed_words+1) with
            // both OLD and NEW placed correctly.
            auto* old_vr = reinterpret_cast<value_region*>(leaf_at(p, child_off));
            const uint8_t  old_swords = old_vr->suffix_words;
            const uint16_t old_value_len = old_vr->value_len;
            const auto     new_after  = key.subspan(size_t(consumed_words) + 1);
            // Exact dup? same suffix length + same bytes → overwrite value.
            if (old_swords == uint8_t(new_after.size())
                && (new_after.empty()
                    || std::memcmp(old_vr->suffix_bytes(), new_after.data(),
                                   new_after.size_bytes()) == 0)) {
                const uint16_t nvoff = leaf_value_overwrite(p, child_off, value);
                if (nvoff == kLeafOffSentinel) return kLeafOffSentinel;
                if (nvoff != child_off) {
                    rn = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
                    rn->offs()[pos] = nvoff;
                }
                return root_off;
            }
            // Build sub_rn at consumed_words+1 and place OLD/NEW correctly.
            const uint16_t sub_root = alloc_radix_node(p, kRadixCapMin);
            if (sub_root == kLeafOffSentinel) return kLeafOffSentinel;
            // Snapshot OLD's value + remaining (post-K) bytes BEFORE any
            // further alloc shifts heap.
            uint8_t old_val_buf[256];
            assert(old_value_len <= sizeof(old_val_buf));
            old_vr = reinterpret_cast<value_region*>(leaf_at(p, child_off));
            std::memcpy(old_val_buf, old_vr->value(), old_value_len);
            uint8_t old_suf_buf[256 * 4];
            assert(size_t(old_swords) * 4 <= sizeof(old_suf_buf));
            if (old_swords > 0) {
                std::memcpy(old_suf_buf, old_vr->suffix_bytes(), size_t(old_swords) * 4);
            }
            // Place OLD in sub_rn.  OLD's first suffix word becomes
            // sub_stem (if any); rest becomes value_region's suffix.
            // If OLD has no suffix → goes to terminal_off.
            const auto place_old = [&](uint16_t sub_root_off) -> bool {
                if (old_swords == 0) {
                    // OLD ends at consumed_words.  Goes to terminal.
                    const uint16_t voff = leaf_heap_alloc(
                        p, value_region::size_bytes(0, old_value_len));
                    if (voff == kLeafOffSentinel) return false;
                    auto* vr = reinterpret_cast<value_region*>(leaf_at(p, voff));
                    vr->suffix_words = 0; vr->value_len = uint8_t(old_value_len); vr->type_alt = 0;
                    std::memcpy(vr->value(), old_val_buf, old_value_len);
                    auto* sr = reinterpret_cast<radix_node*>(leaf_at(p, sub_root_off));
                    sr->set_terminal_off(voff);
                    return true;
                }
                // OLD has ≥1 word past consumed_words.  Build entry.
                uint32_t old_first;
                std::memcpy(&old_first, old_suf_buf, 4);
                const uint8_t old_rem = uint8_t(old_swords - 1);
                const uint16_t voff = leaf_heap_alloc(
                    p, value_region::size_bytes(old_rem, old_value_len));
                if (voff == kLeafOffSentinel) return false;
                auto* vr = reinterpret_cast<value_region*>(leaf_at(p, voff));
                vr->suffix_words = old_rem; vr->value_len = uint8_t(old_value_len); vr->type_alt = 0;
                if (old_rem > 0) {
                    std::memcpy(vr->suffix_bytes(), old_suf_buf + 4, size_t(old_rem) * 4);
                }
                std::memcpy(vr->value(), old_val_buf, old_value_len);
                auto* sr = reinterpret_cast<radix_node*>(leaf_at(p, sub_root_off));
                sr->sub_stems()[0] = old_first;
                sr->offs()[0]      = voff;
                sr->set_n_entries(1);
                return true;
            };
            if (!place_old(sub_root)) return kLeafOffSentinel;
            // Now insert NEW into sub_rn via a recursive radix_insert,
            // which will correctly handle terminal vs sub_stem placement
            // and any further collisions.
            const uint16_t deep = radix_insert(
                p, sub_root, key, uint16_t(consumed_words + 1), value);
            if (deep == kLeafOffSentinel) return kLeafOffSentinel;
            rn = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
            rn->offs()[pos] = uint16_t(deep | kRadixTagBit);
            // OLD value_region (its data moved under sub_rn) is now dead.
            leaf_add_dead(p, value_region::size_bytes(old_swords, old_value_len));
            return root_off;
        }

        // Stem doesn't match — insert beside in this radix node.
        if (rn->n_entries() >= rn->cap_slots()) {
            const uint16_t grown = grow_radix_node(p, cur_off);
            if (grown == kLeafOffSentinel) return kLeafOffSentinel;
            if (chain_n > 0) {
                auto& back = chain[chain_n - 1];
                auto* par = reinterpret_cast<radix_node*>(leaf_at(p, back.parent_off));
                par->offs()[back.slot] = uint16_t(grown | kRadixTagBit);
            } else {
                root_off = grown;
            }
            cur_off = grown;
            rn = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
            sub = rn->sub_stems();
        }
        // Allocate value-region for the new entry, shift, write.
        const uint16_t voff = write_value_region(key.subspan(size_t(consumed_words) + 1));
        if (voff == kLeafOffSentinel) return kLeafOffSentinel;
        // Reload pointers in case heap shifted.
        rn  = reinterpret_cast<radix_node*>(leaf_at(p, cur_off));
        sub = rn->sub_stems();
        uint16_t* offs2 = rn->offs();
        const uint16_t n = rn->n_entries();
        if (pos < n) {
            std::memmove(sub + pos + 1,  sub + pos,
                         (n - pos) * sizeof(uint32_t));
            std::memmove(offs2 + pos + 1, offs2 + pos,
                         (n - pos) * sizeof(uint16_t));
        }
        sub[pos]   = qw;
        offs2[pos] = voff;
        rn->set_n_entries(uint8_t(n + 1));
        return root_off;
    }
}

// ─── leaf_insert (definition) ─────────────────────────────────────
inline bool leaf_insert(leaf_node* p,
                         word_key_type key,
                         uint16_t depth,
                         value_type value) noexcept {
    const uint16_t value_len = uint16_t(value.size());
    if (depth >= key.size()) return false;
    const uint32_t q = key[depth];
    const uint16_t cap = leaf_stems_for(p->cap_in_cls);
    const uint16_t num_keys = p->num_keys;
    uint32_t* stems = leaf_stems(p);
    uint16_t* offs  = leaf_offs(p);
    const uint16_t pos = leaf_count_lt(stems, num_keys, cap, q);
    const bool exact = (pos < num_keys && stems[pos] == q);

    if (exact) {
        const uint16_t off = offs[pos];
        if (off == kLeafOffSentinel) return false;  // shouldn't happen
        if (offs_is_radix(off)) {
            const uint16_t new_root = radix_insert(
                p, off & kRadixOffMask, key, /*consumed=*/uint16_t(depth + 1), value);
            if (new_root == kLeafOffSentinel) return false;
            offs = leaf_offs(p);
            offs[pos] = uint16_t(new_root | kRadixTagBit);
            return true;
        }
        // Value-region at this slot — possible exact dup or collision.
        auto* old_vr = reinterpret_cast<value_region*>(leaf_at(p, off));
        const auto new_suffix = key.subspan(size_t(depth) + 1);
        if (old_vr->suffix_words == uint8_t(new_suffix.size())
            && (new_suffix.empty() ||
                std::memcmp(old_vr->suffix_bytes(), new_suffix.data(),
                            new_suffix.size_bytes()) == 0)) {
            const uint16_t nvoff = leaf_value_overwrite(p, off, value);
            if (nvoff == kLeafOffSentinel) return false;   // heap full → split & retry
            if (nvoff != off) { offs = leaf_offs(p); offs[pos] = nvoff; }
            return true;
        }
        // Collision: build a sub_rn at depth+1 and place BOTH OLD and
        // NEW correctly.  When OLD and NEW share many leading suffix
        // words, build a FLAT chain of K_=1 sub_rns down to the
        // divergence point — instead of the recursive radix_insert
        // path which allocates a fresh value_region for OLD at every
        // level (O(N²) bytes for deeply-shared prefixes; exceeds 4 KB
        // leaf for ~25+ shared u32s).
        //
        // Snapshot OLD's value + suffix bytes BEFORE further alloc.
        const uint8_t old_swords = old_vr->suffix_words;
        const uint16_t old_value_len = old_vr->value_len;
        uint8_t old_val_buf[256];
        assert(old_value_len <= sizeof(old_val_buf));
        std::memcpy(old_val_buf, old_vr->value(), old_value_len);
        uint8_t old_suf_buf[256 * 4];
        assert(size_t(old_swords) * 4 <= sizeof(old_suf_buf));
        if (old_swords > 0) {
            std::memcpy(old_suf_buf, old_vr->suffix_bytes(), size_t(old_swords) * 4);
        }
        // The OLD value_region at this slot is replaced by the sub_rn built
        // below (both the common-prefix and no-shared-prefix branches) → dead.
        // (On a mid-build failure the leaf is split next, resetting dead.)
        leaf_add_dead(p, value_region::size_bytes(old_swords, old_value_len));

        // Compute common-prefix length between OLD's suffix and NEW's
        // suffix (starting at depth+1).  If >0, we can build a flat
        // chain that avoids the O(N²) value_region blowup.
        size_t common = 0;
        const size_t max_common = std::min<size_t>(
            size_t(old_swords), new_suffix.size());
        while (common < max_common) {
            uint32_t old_w, new_w;
            std::memcpy(&old_w, old_suf_buf + common * 4, 4);
            new_w = new_suffix[common];
            if (old_w != new_w) break;
            ++common;
        }
        if (common >= 1) {
            // Build ONE compressed sub_rn at depth+1 carrying path_words=common.
            // Holds OLD and NEW under its sub_stems (or terminal if one of
            // them ends exactly at the path end).  Replaces the older flat
            // chain of K_=1 sub_rns — same logical structure, ~1/N the bytes.
            //
            // OLD and NEW cannot both end at the path end (that was the
            // exact-dup early-return up the function); at most one does.
            const bool old_ends_at_path = (size_t(common) == size_t(old_swords));
            const bool new_ends_at_path = (size_t(common) == new_suffix.size());
            assert(!(old_ends_at_path && new_ends_at_path));

            const uint16_t sub_root = alloc_radix_node(
                p, kRadixCapMin, /*path_words=*/uint8_t(common), /*has_term=*/true);
            if (sub_root == kLeafOffSentinel) return false;

            // OLD's diverging stem (if any) and remaining suffix.
            uint32_t old_div_word    = 0;
            uint8_t  old_tail_words  = 0;
            if (!old_ends_at_path) {
                std::memcpy(&old_div_word, old_suf_buf + common * 4, 4);
                old_tail_words = uint8_t(old_swords - common - 1);
            }
            // NEW's diverging stem (if any) and remaining suffix.
            uint32_t new_div_word = 0;
            std::span<const uint32_t> new_tail{};
            if (!new_ends_at_path) {
                new_div_word = new_suffix[common];
                new_tail     = new_suffix.subspan(common + 1);
            }

            const uint16_t old_voff = leaf_heap_alloc(
                p, value_region::size_bytes(old_tail_words, old_value_len));
            if (old_voff == kLeafOffSentinel) return false;
            const uint16_t new_voff = leaf_heap_alloc(
                p, value_region::size_bytes(uint16_t(new_tail.size()), value_len));
            if (new_voff == kLeafOffSentinel) return false;

            // Populate OLD value_region.
            auto* old_new_vr = reinterpret_cast<value_region*>(leaf_at(p, old_voff));
            old_new_vr->suffix_words = old_tail_words; old_new_vr->value_len = uint8_t(old_value_len); old_new_vr->type_alt = 0;
            if (old_tail_words > 0) {
                std::memcpy(old_new_vr->suffix_bytes(),
                            old_suf_buf + (common + 1) * 4,
                            size_t(old_tail_words) * 4);
            }
            std::memcpy(old_new_vr->value(), old_val_buf, old_value_len);
            // Populate NEW value_region.
            auto* new_new_vr = reinterpret_cast<value_region*>(leaf_at(p, new_voff));
            new_new_vr->suffix_words = uint8_t(new_tail.size()); new_new_vr->value_len = uint8_t(value_len); new_new_vr->type_alt = 0;
            if (!new_tail.empty()) {
                std::memcpy(new_new_vr->suffix_bytes(),
                            new_tail.data(),
                            new_tail.size_bytes());
            }
            std::memcpy(new_new_vr->value(value_len), value.data(), value_len);

            // Populate the compressed sub_rn.
            auto* rn = reinterpret_cast<radix_node*>(leaf_at(p, sub_root));
            std::memcpy(rn->path_data(), old_suf_buf, size_t(common) * 4);
            if (old_ends_at_path) {
                // OLD lives in terminal; NEW is the sole sub_stem.
                rn->set_terminal_off(old_voff);
                rn->sub_stems()[0] = new_div_word;
                rn->offs()[0]      = new_voff;
                rn->set_n_entries(1);
            } else if (new_ends_at_path) {
                rn->set_terminal_off(new_voff);
                rn->sub_stems()[0] = old_div_word;
                rn->offs()[0]      = old_voff;
                rn->set_n_entries(1);
            } else {
                // Both diverge with a word — sort by stem.
                const bool old_first = (old_div_word < new_div_word);
                rn->sub_stems()[0] = old_first ? old_div_word : new_div_word;
                rn->offs()[0]      = old_first ? old_voff     : new_voff;
                rn->sub_stems()[1] = old_first ? new_div_word : old_div_word;
                rn->offs()[1]      = old_first ? new_voff     : old_voff;
                rn->set_n_entries(2);
            }
            // Install in leaf's index.
            offs = leaf_offs(p);
            offs[pos] = uint16_t(sub_root | kRadixTagBit);
            ++p->num_branches;
            return true;
        }

        // No shared suffix words — fall through to the original
        // single-level collision path below.
        const uint16_t sub_root = alloc_radix_node(p, kRadixCapMin);
        if (sub_root == kLeafOffSentinel) return false;
        // Place OLD: terminal_off if no more words, else sub_stem.
        if (old_swords == 0) {
            // OLD ends at depth.  Goes to terminal slot of sub_rn.
            const uint16_t voff = leaf_heap_alloc(
                p, value_region::size_bytes(0, old_value_len));
            if (voff == kLeafOffSentinel) return false;
            auto* vr = reinterpret_cast<value_region*>(leaf_at(p, voff));
            vr->suffix_words = 0; vr->value_len = uint8_t(old_value_len); vr->type_alt = 0;
            std::memcpy(vr->value(), old_val_buf, old_value_len);
            auto* sr = reinterpret_cast<radix_node*>(leaf_at(p, sub_root));
            sr->set_terminal_off(voff);
        } else {
            // OLD has ≥1 word past depth.  Use first as sub_stem.
            uint32_t old_first;
            std::memcpy(&old_first, old_suf_buf, 4);
            const uint8_t old_rem = uint8_t(old_swords - 1);
            const uint16_t voff = leaf_heap_alloc(
                p, value_region::size_bytes(old_rem, old_value_len));
            if (voff == kLeafOffSentinel) return false;
            auto* vr = reinterpret_cast<value_region*>(leaf_at(p, voff));
            vr->suffix_words = old_rem; vr->value_len = uint8_t(old_value_len); vr->type_alt = 0;
            if (old_rem > 0) {
                std::memcpy(vr->suffix_bytes(), old_suf_buf + 4, size_t(old_rem) * 4);
            }
            std::memcpy(vr->value(), old_val_buf, old_value_len);
            auto* sr = reinterpret_cast<radix_node*>(leaf_at(p, sub_root));
            sr->sub_stems()[0] = old_first;
            sr->offs()[0]      = voff;
            sr->set_n_entries(1);
        }

        // Insert NEW via recursive radix_insert (which handles
        // terminal / exact-match / insert-beside cases uniformly).
        const uint16_t deep = radix_insert(
            p, sub_root, key, /*consumed=*/uint16_t(depth + 1), value);
        if (deep == kLeafOffSentinel) return false;
        offs = leaf_offs(p);
        offs[pos] = uint16_t(deep | kRadixTagBit);
        ++p->num_branches;
        return true;
    }

    // No stem match — insert-beside in leaf.
    if (p->num_keys >= cap) {
        if (!leaf_grow_index(p)) return false;
        return leaf_insert(p, key, depth, value);
    }
    // Allocate value-region for new entry.
    const auto new_suffix = key.subspan(size_t(depth) + 1);
    const uint16_t voff = leaf_heap_alloc(
        p, value_region::size_bytes(uint16_t(new_suffix.size()), value_len));
    if (voff == kLeafOffSentinel) return false;
    auto* vr = reinterpret_cast<value_region*>(leaf_at(p, voff));
    vr->suffix_words = uint8_t(new_suffix.size()); vr->value_len = uint8_t(value_len); vr->type_alt = 0;
    if (!new_suffix.empty()) {
        std::memcpy(vr->suffix_bytes(), new_suffix.data(), new_suffix.size_bytes());
    }
    std::memcpy(vr->value(value_len), value.data(), value_len);
    // Reload index pointers (heap shift doesn't move them but be safe).
    stems = leaf_stems(p);
    offs  = leaf_offs(p);
    // Shift stems[pos..num_keys) → stems[pos+1..num_keys+1).  num_keys
    // grows by 1; sentinel padding remains at the tail.
    const uint16_t n = p->num_keys;
    if (pos < n) {
        std::memmove(stems + pos + 1, stems + pos, (n - pos) * sizeof(uint32_t));
        std::memmove(offs  + pos + 1, offs  + pos, (n - pos) * sizeof(uint16_t));
    }
    stems[pos] = q;
    offs[pos]  = voff;
    ++p->num_keys;
    return true;
}

// ─── Capacity hint for leaf_init ──────────────────────────────────
inline uint8_t leaf_cap_in_cls_for(uint16_t num_stems) noexcept {
    const uint32_t need = uint32_t(num_stems) * 6u + uint32_t(kLeafHeaderBytes);
    uint32_t cls = (need + kCacheLine - 1) / kCacheLine;
    if (cls < 1) cls = 1;
    if (cls > kLeafPageCls) cls = kLeafPageCls;
    return uint8_t(cls);
}
inline bool leaf_ensure_index_cap_(leaf_node* p, uint8_t target) noexcept {
    while (p->cap_in_cls < target) {
        if (!leaf_grow_index(p)) return false;
    }
    return true;
}
inline bool leaf_init_with_cap(leaf_node* p, uint8_t cap_in_cls) noexcept {
    leaf_init(p);
    return leaf_ensure_index_cap_(p, cap_in_cls);
}

// ─── copy_subgraph: deep-copy one stem's offs subgraph from src→dst
//
// Walks value_region / radix_node nodes referenced by `src_off_tagged`,
// allocating equivalents in dst's heap.  Returns the dst-side tagged
// offset (preserving the radix-tag bit) or kLeafOffSentinel on dst-heap
// exhaustion.
//
// src is read-only; dst grows.  Recursion depth ≤ key-words-past-stem,
// which is at most ~8 for 32-byte encoded keys.
inline uint16_t copy_subgraph_(const leaf_node* src, uint16_t src_off_tagged,
                                leaf_node* dst, uint16_t value_len) noexcept {
    const bool is_radix = offs_is_radix(src_off_tagged);
    const uint16_t src_off = src_off_tagged & kRadixOffMask;
    if (!is_radix) {
        const auto* src_vr = reinterpret_cast<const value_region*>(
            leaf_at(src, src_off));
        const uint16_t bytes = src_vr->total_bytes();
        const uint16_t new_off = leaf_heap_alloc(dst, bytes);
        if (new_off == kLeafOffSentinel) return kLeafOffSentinel;
        std::memcpy(leaf_at(dst, new_off), src_vr, bytes);
        return new_off;   // tag bit clear
    }
    // src is read-only — sub_stems()/offs() pointers are stable across
    // the recursion.  dst's heap bump-allocates forward but never moves
    // the block we already allocated at new_off.
    const auto* src_rn = reinterpret_cast<const radix_node*>(leaf_at(src, src_off));
    const uint8_t  n_entries     = src_rn->n_entries();
    const uint8_t  src_cap       = src_rn->cap_slots();
    const uint8_t  src_pw        = src_rn->path_words();
    const bool     src_ht        = src_rn->has_term();
    const uint16_t src_terminal  = src_rn->terminal_off();
    const uint32_t* src_sub      = src_rn->sub_stems();
    const uint16_t* src_offs_arr = src_rn->offs();
    // Snapshot path words BEFORE alloc (src is in src page, alloc grows dst).
    uint32_t path_buf[128];
    assert(src_pw <= sizeof(path_buf) / sizeof(uint32_t));
    if (src_pw > 0) std::memcpy(path_buf, src_rn->path_data(), size_t(src_pw) * 4);
    const uint16_t new_off = alloc_radix_node(dst, src_cap, src_pw, src_ht);
    if (new_off == kLeafOffSentinel) return kLeafOffSentinel;
    auto* dst_rn = reinterpret_cast<radix_node*>(leaf_at(dst, new_off));
    if (src_pw > 0) std::memcpy(dst_rn->path_data(), path_buf, size_t(src_pw) * 4);
    uint32_t* dst_sub = dst_rn->sub_stems();
    uint16_t* dst_offs = dst_rn->offs();
    for (uint8_t i = 0; i < n_entries; ++i) {
        const uint16_t copied = copy_subgraph_(src, src_offs_arr[i], dst, value_len);
        if (copied == kLeafOffSentinel) return kLeafOffSentinel;
        dst_sub[i]  = src_sub[i];
        dst_offs[i] = copied;
    }
    // Copy terminal_off (deep-copy the value_region it points to).
    if (src_ht && src_terminal != kLeafOffSentinel) {
        const uint16_t copied = copy_subgraph_(src, src_terminal, dst, value_len);
        if (copied == kLeafOffSentinel) return kLeafOffSentinel;
        // Reload dst_rn (heap may have shifted? no — heap grows downward,
        // dst_rn was allocated first so it's at a higher address. Stable.)
        dst_rn = reinterpret_cast<radix_node*>(leaf_at(dst, new_off));
        dst_rn->set_terminal_off(copied);
    }
    dst_rn->set_n_entries(n_entries);
    return uint16_t(new_off | kRadixTagBit);
}

// ─── subgraph_bytes_: deep size of a stem's payload (radix/value) ─
//
// Walks the stem's subgraph and returns total heap bytes consumed.
// Used by leaf_split to pick a byte-balanced split point so neither
// half ends up so full it can't accept a subsequent insert.
inline uint16_t subgraph_bytes_(const leaf_node* p, uint16_t off_tagged,
                                  uint16_t value_len) noexcept {
    const bool is_radix = offs_is_radix(off_tagged);
    const uint16_t off = off_tagged & kRadixOffMask;
    if (!is_radix) {
        const auto* vr = reinterpret_cast<const value_region*>(leaf_at(p, off));
        return vr->total_bytes();
    }
    const auto* rn = reinterpret_cast<const radix_node*>(leaf_at(p, off));
    uint16_t total = radix_node::size_bytes(rn->cap_slots(), rn->path_words(), rn->has_term());
    const uint8_t n = rn->n_entries();
    for (uint8_t i = 0; i < n; ++i) {
        total = uint16_t(total + subgraph_bytes_(p, rn->offs()[i], value_len));
    }
    const uint16_t toff = rn->terminal_off();
    if (toff != kLeafOffSentinel) {
        total = uint16_t(total + subgraph_bytes_(p, toff, value_len));
    }
    return total;
}

// ─── leaf_split: split src into two fresh 4 KB leaves L, R ────────
//
// Splits at a BYTE-WEIGHTED stem boundary: walks each stem's subgraph
// to find k such that L's heap usage ≈ R's heap usage.  Stem-count
// midpoint (k = N/2) is the fast path; we only walk-and-rebalance
// when that midpoint puts one half over 60% of total heap (the
// "heavy stem" case that causes post-split-insert drops).
//
// Caller has already allocated zero'd L and R (kLeafPageBytes each).
// On success: L and R are populated, *out_divider = R's first stem.
// On failure (dst heap exhaustion — should never happen for half a
// full leaf): returns false with partial dst state; caller frees both.
inline bool leaf_split(const leaf_node* src,
                        leaf_node* L, leaf_node* R,
                        uint16_t value_len,
                        uint32_t* out_divider) noexcept {
    const uint16_t N = src->num_keys;
    if (N < 2) return false;   // degenerate; caller must promote/deepen
    const uint16_t* src_offs  = leaf_offs(src);

    // Compute per-stem subgraph size, then find k minimising max(L,R)
    // heap usage.  k must be in [1, N-1].
    uint16_t weights[256];   // up to leaf_stems_for(kLeafPageCls) ≤ 254
    assert(N <= sizeof(weights) / sizeof(weights[0]));
    uint32_t total = 0;
    for (uint16_t i = 0; i < N; ++i) {
        weights[i] = subgraph_bytes_(src, src_offs[i], value_len);
        total += weights[i];
    }
    uint16_t k = 1;
    uint32_t best_imbalance = UINT32_MAX;
    uint32_t l_sum = 0;
    for (uint16_t i = 0; i < N - 1; ++i) {
        l_sum += weights[i];
        const uint32_t r_sum = total - l_sum;
        const uint32_t imb = (l_sum > r_sum) ? (l_sum - r_sum) : (r_sum - l_sum);
        if (imb < best_imbalance) {
            best_imbalance = imb;
            k = uint16_t(i + 1);
        }
    }
    if (k == 0 || k == N) return false;
    const uint16_t r_n = uint16_t(N - k);
    const uint8_t  L_cap_cls = leaf_cap_in_cls_for(k);
    const uint8_t  R_cap_cls = leaf_cap_in_cls_for(r_n);
    if (!leaf_init_with_cap(L, L_cap_cls)) return false;
    if (!leaf_init_with_cap(R, R_cap_cls)) return false;

    const uint32_t* src_stems = leaf_stems(src);
    // L/R were sized up-front (leaf_init_with_cap) — their index doesn't
    // move during copy_subgraph_, so leaf_stems/leaf_offs pointers are
    // stable across the loop.
    uint32_t* L_stems = leaf_stems(L);
    uint16_t* L_offs  = leaf_offs(L);
    uint32_t* R_stems = leaf_stems(R);
    uint16_t* R_offs  = leaf_offs(R);

    // Copy left half.
    for (uint16_t i = 0; i < k; ++i) {
        const uint16_t copied = copy_subgraph_(src, src_offs[i], L, value_len);
        if (copied == kLeafOffSentinel) return false;
        L_stems[i] = src_stems[i];
        L_offs[i]  = copied;
    }
    L->num_keys = k;

    // Copy right half.
    for (uint16_t i = k; i < N; ++i) {
        const uint16_t copied = copy_subgraph_(src, src_offs[i], R, value_len);
        if (copied == kLeafOffSentinel) return false;
        const uint16_t r_idx = uint16_t(i - k);
        R_stems[r_idx] = src_stems[i];
        R_offs[r_idx]  = copied;
    }
    R->num_keys = r_n;

    // num_branches: count tagged entries in each half.
    auto count_branches = [](const leaf_node* p) noexcept {
        const uint16_t* o = leaf_offs(p);
        uint8_t c = 0;
        for (uint16_t i = 0; i < p->num_keys; ++i) {
            if (offs_is_radix(o[i])) ++c;
        }
        return c;
    };
    L->num_branches = count_branches(L);
    R->num_branches = count_branches(R);

    *out_divider = leaf_stems(R)[0];
    return true;
}

// ─── leaf_merge_: combine two adjacent leaves into one (B-tree merge) ──
// A's stems are all < B's stems (A = child[pos], B = child[pos+1] of the same
// inner, separated by divider stems[pos]). Builds a fresh leaf M = A ++ B via
// copy_subgraph_ (drops dead space), keeping the order. Returns false (M left
// for the caller to free) if the combined live data wouldn't fit one page.
inline bool leaf_merge_(leaf_node* M, const leaf_node* A, const leaf_node* B,
                        uint16_t value_len) noexcept {
    const uint16_t na = A->num_keys, nb = B->num_keys;
    const uint16_t nm = uint16_t(na + nb);
    assert(na > 0 && nb > 0);   // callers only merge two non-empty leaf siblings
    if (nm > leaf_stems_for(uint8_t(kLeafPageCls))) return false;   // combined index won't fit

    const uint16_t* ao = leaf_offs(A);
    const uint16_t* bo = leaf_offs(B);
    uint32_t heap = 0;
    for (uint16_t i = 0; i < na; ++i) heap += subgraph_bytes_(A, ao[i], value_len);
    for (uint16_t i = 0; i < nb; ++i) heap += subgraph_bytes_(B, bo[i], value_len);
    const uint8_t cap_cls = leaf_cap_in_cls_for(nm);
    if (uint32_t(cap_cls) * kCacheLine + heap > kLeafPageBytes) return false;   // won't fit

    if (!leaf_init_with_cap(M, cap_cls)) return false;
    uint32_t* ms = leaf_stems(M);
    uint16_t* mo = leaf_offs(M);
    const uint32_t* as_ = leaf_stems(A);
    const uint32_t* bs_ = leaf_stems(B);
    for (uint16_t i = 0; i < na; ++i) {
        const uint16_t c = copy_subgraph_(A, ao[i], M, value_len);
        if (c == kLeafOffSentinel) return false;
        ms[i] = as_[i]; mo[i] = c;
    }
    for (uint16_t i = 0; i < nb; ++i) {
        const uint16_t c = copy_subgraph_(B, bo[i], M, value_len);
        if (c == kLeafOffSentinel) return false;
        ms[na + i] = bs_[i]; mo[na + i] = c;
    }
    M->num_keys = nm;
    uint8_t br = 0;
    for (uint16_t i = 0; i < nm; ++i) if (offs_is_radix(mo[i])) ++br;
    M->num_branches = br;
    return true;
}

// ─── leaf_promote_k1: K=1 leaf split (defined in inner_node.hpp) ──
//
// Called when leaf_insert fails on a leaf that holds exactly one stem
// X with a radix subtree.  Defined in inner_node.hpp because it needs
// to construct an inner_node (which inner_node.hpp defines after this
// header is included).
inline upsert_result leaf_promote_k1(leaf_node* L,
                                       uint16_t depth,
                                       word_key_type key,
                                       std::span<const uint8_t> value) noexcept;

// Diagnostic counters for drop paths.  All zero means no drops.
inline size_t g_drop_split_failed             = 0;   // leaf_split returned false
inline size_t g_drop_post_split_insert_failed = 0;   // post-split leaf_insert failed
inline size_t g_drop_k1_split_insert_failed   = 0;   // K=1 q≠X new-leaf insert failed
inline size_t g_drop_promote_returned_null    = 0;   // leaf_promote_k1 returned nullptr

// ─── leaf_upsert: insert with split fallback ──────────────────────
//
// Returns upsert_result:
//   in-place insert → { src, nullptr, 0 }
//   leaf full, K≥2  → split into 2 new leaves, insert key, return
//                      { L, R, divider }
//   leaf full, K==1 → promote: 1-stem leaf becomes sub-inner + 2 sub-
//                      leaves at depth+1, insert key in the right half.
template <upsert_mode mode>
inline upsert_result leaf_upsert(sal::smart_ref<leaf_node>& lref,
                                   word_key_type key,
                                   uint16_t depth,
                                   std::span<const uint8_t> value) noexcept {
    const uint16_t value_len = uint16_t(value.size());
    // Make the leaf writable. UNIQUE: modify() through the THREADED smart_ref —
    // reuses the fetch the caller already made (in place if writable, else SAL
    // relocates the same CB, keeping the address). SHARED: FORK to a fresh-address
    // private copy and retain its value refs, so the snapshot's old leaf is
    // untouched. After this, `p` is the node we mutate; in shared mode it is the
    // private clone, so every leaf_free(p)/split/return-p below disposes of the
    // CLONE while the original survives for the snapshot.
    leaf_node* p;
    if constexpr (mode.is_shared()) {
        p = clone_leaf_fresh(lref.obj());
        leaf_retain_value_refs_(p);
    } else {
        lref.modify([&](leaf_node* n) { p = n; });
    }
    if (leaf_insert(p, key, depth, value)) {
        return upsert_inplace(p);
    }
    // Insert failed.  K==1 — two sub-cases:
    //   (a) new key's first u32 MATCHES the K=1 stem → K=1 promote
    //       (depth-advance the existing X-key radix into a sub-inner +
    //        2 sub-leaves, place the trigger).
    //   (b) new key's first u32 DIFFERS from the K=1 stem → can't
    //       promote (the trigger doesn't belong in an X-key sub-inner).
    //       Instead: split into [original K=1 leaf, fresh leaf with new
    //       key], with a divider in between.  Parent absorbs as a normal
    //       upsert_split.
    if (p->num_keys == 1) {
        const uint32_t q = (depth < key.size()) ? key[depth] : 0u;
        const uint32_t X = leaf_stems(p)[0];
        if (q == X) {
            auto r = leaf_promote_k1(p, depth, key, value);
            if (!r.left) ++g_drop_promote_returned_null;
            return r;
        }
        // Case (b): split-into-two-leaves with K=1 stems.
        leaf_node* new_lf = leaf_alloc(p->address());   // sibling-local
        leaf_init(new_lf);
        if (!leaf_insert(new_lf, key, depth, value)) {
            leaf_free(new_lf);
            ++g_drop_k1_split_insert_failed;
            return { nullptr, nullptr, 0, 0 };
        }
        if (q < X) {
            // new_lf has q-keys (smaller), p has X-keys (larger).
            return upsert_split(new_lf, /*divider=*/X, p);
        }
        // q > X: p has X-keys (smaller), new_lf has q-keys (larger).
        return upsert_split(p, /*divider=*/q, new_lf);
    }
    // K≥2 → normal split. Allocate the split family sibling-local: L near
    // the source p, R next to L — packs their control blocks onto shared
    // CB cachelines (16/line), shrinking the CB working-set footprint.
    leaf_node* L = leaf_alloc(p->address());
    leaf_node* R = leaf_alloc(L->address());
    uint32_t divider = 0;
    if (!leaf_split(p, L, R, value_len, &divider)) {
        leaf_free(L); leaf_free(R);
        ++g_drop_split_failed;
        return { nullptr, nullptr, 0, 0 };
    }
    const uint32_t q = (depth < key.size()) ? key[depth] : 0u;
    leaf_node* target = (q < divider) ? L : R;
    if (!leaf_insert(target, key, depth, value)) {
        // Target's heap is too full for the new entry.  Two recovery
        // strategies, in order:
        //
        //   1. target.num_keys == 1 — K=1 promote on target.  Depth-
        //      advances target's content into a sub-inner + 2 sub-leaves
        //      at depth+1; the trigger fits in the freed space.  Returns
        //      a K=1 promote with `extra` carrying the outer split's
        //      OTHER half so the parent can splice all 4 pieces.
        //
        //   2. target.num_keys >= 2 — re-split target.  Produces 3
        //      leaves total (other-half + T_L + T_R) and the trigger
        //      lives in whichever T_? its stem routes to.  Returns a
        //      "3-way split" upsert_result (left, right via tail, with
        //      `extra` carrying the third).
        //
        // Strategy 1 (K=1 promote on target):
        if (target->num_keys == 1) {
            const uint32_t qe    = (depth < key.size()) ? key[depth] : 0u;
            const uint32_t Xtgt  = leaf_stems(target)[0];
            if (qe == Xtgt) {
                auto promo = leaf_promote_k1(target, depth, key, value);
                if (promo.left) {
                    leaf_node* sibling = (target == L) ? R : L;
                    promo.extra = new upsert_extra();
                    if (target == L) {
                        // Layout at parent's slot becomes:
                        //   [sentinel-[A,X), sub_inner-[X,X+1),
                        //    sentinel-[X+1,divider), R-[divider,B)]
                        // The promote primary contributes the first 3
                        // children + dividers X, X+1.  Tail adds R with
                        // `divider` as the boundary.
                        promo.extra->n_tail = 1;
                        promo.extra->tail_children[0] = as_header(sibling);
                        promo.extra->tail_dividers[0] = divider;
                    } else {
                        // target == R.  Sibling=L sits BEFORE the promote.
                        // Layout: [L-[A,divider), sentinel-[divider,X),
                        //          sub_inner-[X,X+1), sentinel-[X+1,B)]
                        // Prepend L with the outer divider before the
                        // promote's first sentinel.
                        promo.extra->n_prepend           = 1;
                        promo.extra->prepend_children[0] = as_header(sibling);
                        promo.extra->prepend_dividers[0] = divider;
                    }
                    leaf_free(p);
                    return promo;
                }
            }
        }
        // Strategy 2: target.num_keys >= 2 — re-split target and recurse.
        // Compose the recursive PRIMARY with our outer_other + sub_other
        // by splicing them into the result's upsert_extra (n_prepend
        // before, n_tail after).  Each recursive layer halves the leaf
        // heap; the recursion terminates either at inplace success or
        // at K=1 promote on a now-single-stem sub-leaf.
        if (target->num_keys >= 2) {
            const uint32_t qr = (depth < key.size()) ? key[depth] : 0u;
            leaf_node* T_L = leaf_alloc(target->address());
            leaf_node* T_R = leaf_alloc(T_L->address());
            uint32_t inner_div = 0;
            if (leaf_split(target, T_L, T_R, value_len, &inner_div)) {
                const bool      sub_is_TL  = (qr < inner_div);
                leaf_node* const sub_target = sub_is_TL ? T_L : T_R;
                leaf_node* const sub_other  = sub_is_TL ? T_R : T_L;
                // sub_target is a freshly-allocated private split half →
                // always UNIQUE (mutate in place; never fork a fresh node).
                auto sref = wsession().get_ref<leaf_node>(sub_target->address());
                upsert_result sub_r =
                    leaf_upsert<upsert_mode::unique_upsert>(sref, key, depth, value);
                if (sub_r.left) {
                    const bool       target_was_R = (target == R);
                    leaf_node* const outer_other  = target_was_R ? L : R;
                    leaf_free(target);
                    leaf_free(p);

                    // Build the "before" and "after" sequences around
                    // sub_r's primary, then splice them into sub_r->extra.
                    //
                    //   target_was_R | sub_is_TL ⇒ before=[L],     after=[T_R]
                    //   target_was_R | sub_is_TR ⇒ before=[L,T_L], after=[]
                    //   target_was_L | sub_is_TL ⇒ before=[],      after=[T_R,R]
                    //   target_was_L | sub_is_TR ⇒ before=[T_L],   after=[R]
                    psitri::node* before_c[2]{};
                    uint32_t       before_d[2]{};   // div AFTER each before[i]
                    uint8_t        n_before = 0;
                    psitri::node* after_c[2]{};
                    uint32_t       after_d[2]{};    // div BEFORE each after[i]
                    uint8_t        n_after = 0;
                    if (target_was_R) {
                        before_c[n_before] = as_header(outer_other);
                        before_d[n_before++] = divider;
                        if (sub_is_TL) {
                            after_c[n_after]   = as_header(sub_other);
                            after_d[n_after++] = inner_div;
                        } else {
                            before_c[n_before] = as_header(sub_other);
                            before_d[n_before++] = inner_div;
                        }
                    } else {
                        if (sub_is_TL) {
                            after_c[n_after]   = as_header(sub_other);
                            after_d[n_after++] = inner_div;
                        } else {
                            before_c[n_before] = as_header(sub_other);
                            before_d[n_before++] = inner_div;
                        }
                        after_c[n_after]   = as_header(outer_other);
                        after_d[n_after++] = divider;
                    }

                    if (!sub_r.extra) sub_r.extra = new upsert_extra();
                    upsert_extra* const ex = sub_r.extra;

                    // Splice `before_c[]` ahead of any existing prepends.
                    if (n_before > 0) {
                        assert(uint16_t(ex->n_prepend) + n_before
                               <= sizeof(ex->prepend_children) / sizeof(ex->prepend_children[0]));
                        // Shift existing prepends right by n_before (work
                        // back-to-front so we don't overwrite).
                        for (auto* end = ex->prepend_children + ex->n_prepend,
                                  *src = end - 1,
                                  *dst = end - 1 + n_before;
                             src >= ex->prepend_children;
                             --src, --dst) {
                            *dst = *src;
                            ex->prepend_dividers[dst - ex->prepend_children] =
                                ex->prepend_dividers[src - ex->prepend_children];
                        }
                        psitri::node** pc = ex->prepend_children;
                        uint32_t*       pd = ex->prepend_dividers;
                        psitri::node* const* sc_end = before_c + n_before;
                        psitri::node* const* sc = before_c;
                        const uint32_t*       sd = before_d;
                        while (sc < sc_end) {
                            *pc++ = *sc++;
                            *pd++ = *sd++;
                        }
                        ex->n_prepend = uint8_t(ex->n_prepend + n_before);
                    }
                    // Append `after_c[]` after any existing tails.
                    if (n_after > 0) {
                        assert(uint16_t(ex->n_tail) + n_after
                               <= sizeof(ex->tail_children) / sizeof(ex->tail_children[0]));
                        psitri::node** tc = ex->tail_children + ex->n_tail;
                        uint32_t*       td = ex->tail_dividers + ex->n_tail;
                        psitri::node* const* sc_end = after_c + n_after;
                        psitri::node* const* sc = after_c;
                        const uint32_t*       sd = after_d;
                        while (sc < sc_end) {
                            *tc++ = *sc++;
                            *td++ = *sd++;
                        }
                        ex->n_tail = uint8_t(ex->n_tail + n_after);
                    }
                    return sub_r;
                }
                // Recursive drop: sub_target was freed by the recursive
                // call.  sub_other is still ours.
                leaf_free(sub_other);
            } else {
                leaf_free(T_L); leaf_free(T_R);
            }
        }
        // Last resort: nothing more we can do.
        leaf_free(L); leaf_free(R);
        ++g_drop_post_split_insert_failed;
        return { nullptr, nullptr, 0, 0 };
    }
    leaf_free(p);
    return upsert_split(L, divider, R);
}

// ─── leaf_remove ──────────────────────────────────────────────────
// Remove `key` (consumed `depth` words) from a WRITABLE leaf `p`. Returns
// true iff the key was present and removed; sets *now_empty when the leaf has
// no stems left afterwards. The removed value_region's heap bytes are recorded
// as dead (reclaimed on a later compaction rebuild). Keys living in an in-leaf
// radix subtree are handled by radix_remove.
inline bool radix_remove(leaf_node* p, uint16_t root_off, word_key_type key,
                         uint16_t consumed, uint16_t value_len,
                         bool* subtree_empty) noexcept;   // defined below

inline bool leaf_remove(leaf_node* p, word_key_type key, uint16_t depth,
                        uint16_t value_len, bool* now_empty) noexcept {
    *now_empty = (p->num_keys == 0);
    if (depth >= key.size()) return false;
    const uint32_t  q   = key[depth];
    const uint16_t  cap = leaf_stems_for(p->cap_in_cls);
    uint32_t*       stems = leaf_stems(p);
    uint16_t*       offs  = leaf_offs(p);
    const uint16_t  n   = p->num_keys;
    const uint16_t  pos = leaf_count_lt(stems, n, cap, q);
    if (pos >= n || stems[pos] != q) return false;

    const uint16_t off = offs[pos];
    bool drop_stem;
    if (offs_is_radix(off)) {
        bool sub_empty = false;
        if (!radix_remove(p, uint16_t(off & kRadixOffMask), key,
                          uint16_t(depth + 1), value_len, &sub_empty))
            return false;                       // key not in the radix subtree
        offs = leaf_offs(p);                    // (radix_remove may not move, but reload)
        if (!sub_empty) { *now_empty = (p->num_keys == 0); return true; }
        --p->num_branches;
        drop_stem = true;                       // subtree emptied → remove the stem too
    } else {
        auto* vr = reinterpret_cast<value_region*>(leaf_at(p, off));
        const auto suffix = key.subspan(size_t(depth) + 1);
        if (vr->suffix_words != uint8_t(suffix.size()) ||
            (!suffix.empty() && std::memcmp(vr->suffix_bytes(), suffix.data(),
                                            suffix.size_bytes()) != 0))
            return false;                       // suffix mismatch → not present
        leaf_retire_value_region_(vr);          // drop owned big/subtree ref
        leaf_add_dead(p, vr->total_bytes());
        drop_stem = true;
    }

    if (drop_stem) {
        if (uint16_t(pos + 1) < n) {
            std::memmove(stems + pos, stems + pos + 1, size_t(n - pos - 1) * sizeof(uint32_t));
            std::memmove(offs  + pos, offs  + pos + 1, size_t(n - pos - 1) * sizeof(uint16_t));
        }
        --p->num_keys;
    }
    *now_empty = (p->num_keys == 0);
    return true;
}

// Remove `key` from the radix subtree rooted at `root_off` (in writable leaf
// `p`). Returns true iff present+removed; sets *subtree_empty when this node
// ends up with no sub_stems and no terminal (caller drops the pointer to it).
// v1 does NOT collapse single-entry nodes (correct, slightly space-suboptimal);
// emptied nodes / removed value_regions are recorded as dead.
inline bool radix_remove(leaf_node* p, uint16_t root_off, word_key_type key,
                         uint16_t consumed, uint16_t value_len,
                         bool* subtree_empty) noexcept {
    *subtree_empty = false;
    auto* rn = reinterpret_cast<radix_node*>(leaf_at(p, root_off));

    if (rn->has_path()) {
        const uint8_t pw = rn->path_words();
        if (size_t(consumed) + pw > key.size()) return false;
        if (std::memcmp(rn->path_data(), key.data() + consumed, size_t(pw) * 4) != 0)
            return false;
        consumed = uint16_t(consumed + pw);
    }

    auto node_dead_if_empty = [&]() {
        if (rn->n_entries() == 0 && rn->terminal_off() == kLeafOffSentinel) {
            leaf_add_dead(p, radix_node::size_bytes(rn->cap_slots(), rn->path_words(),
                                                    rn->has_term()));
            *subtree_empty = true;
        }
    };

    // End-of-key → terminal slot.
    if (consumed >= key.size()) {
        const uint16_t t = rn->terminal_off();
        if (t == kLeafOffSentinel) return false;
        const auto* tvr = reinterpret_cast<const value_region*>(leaf_at(p, t));
        leaf_retire_value_region_(tvr);         // drop owned big/subtree ref
        leaf_add_dead(p, tvr->total_bytes());
        rn->set_terminal_off(kLeafOffSentinel);
        node_dead_if_empty();
        return true;
    }

    const uint32_t qw  = key[consumed];
    uint32_t*      sub = rn->sub_stems();
    uint16_t*      os  = rn->offs();
    const uint8_t  ne  = rn->n_entries();
    const uint16_t pos = count_lt_capped(sub, rn->cap_slots(), qw);
    if (pos >= ne || sub[pos] != qw) return false;

    const uint16_t child = os[pos];
    if (offs_is_radix(child)) {
        bool csub_empty = false;
        if (!radix_remove(p, uint16_t(child & kRadixOffMask), key,
                          uint16_t(consumed + 1), value_len, &csub_empty))
            return false;
        if (!csub_empty) return true;            // child still holds keys → keep entry
        // reload (recursion didn't move rn, but be safe)
        rn = reinterpret_cast<radix_node*>(leaf_at(p, root_off));
        sub = rn->sub_stems(); os = rn->offs();
    } else {
        auto* vr = reinterpret_cast<value_region*>(leaf_at(p, child));
        const auto suffix = key.subspan(size_t(consumed) + 1);
        if (vr->suffix_words != uint8_t(suffix.size()) ||
            (!suffix.empty() && std::memcmp(vr->suffix_bytes(), suffix.data(),
                                            suffix.size_bytes()) != 0))
            return false;
        leaf_retire_value_region_(vr);          // drop owned big/subtree ref
        leaf_add_dead(p, vr->total_bytes());
    }

    // Drop entry `pos` (shift the compacted [0, n_entries) run left). Reset the
    // vacated tail slot to sentinels — radix lookups use count_lt_capped, which
    // scans the FULL capacity and relies on kLeafStemSentinel for unused slots;
    // leaving a stale value there makes count_lt_capped miscount.
    const uint8_t nn = uint8_t(ne - 1);
    if (pos < nn) {
        std::memmove(sub + pos, sub + pos + 1, size_t(nn - pos) * sizeof(uint32_t));
        std::memmove(os  + pos, os  + pos + 1, size_t(nn - pos) * sizeof(uint16_t));
    }
    sub[nn] = kLeafStemSentinel;
    os[nn]  = kLeafOffSentinel;
    rn->set_n_entries(nn);
    node_dead_if_empty();
    return true;
}

}  // namespace psitri::wtree
