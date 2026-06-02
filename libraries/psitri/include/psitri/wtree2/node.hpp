#pragma once
// wtree2/node.hpp — unified SAL node for the wtree2 rewrite.
//
// See doc/wtree2.md for the full design. This is PHASE 1: the node record, the
// cacheline-aligned region layout, sizing, and accessors. Later phases layer in:
//   - the inner estimator (APGM) region (between fixed header and prefix),
//   - the MVCC version table + history (after prefix; empty when absent),
//   - the terminal value (header, gated by has_terminal),
//   - the leaf value payloads and the optional u16 LOCAL inline-radix column.
// Those are marked as SEAMS in header_content_bytes() / the slot accessors so the
// region math has exactly one place to grow.
//
// One unified record: a `shape` flag (leaf|inner) picks the slot semantics, so
// there is a single node_type (wtree2_node) and a single set of SAL type_ops
// that branch on shape internally (lifecycle in a later phase).

#include <cstddef>
#include <cstdint>
#include <span>

#include <psitri/node/node.hpp>          // psitri::node (12-B SAL header base), node_type, ptr_address
#include <psitri/wtree/apgm_predictor.hpp>   // APGM piecewise-linear stem predictor (§6)
#include <psitri/wtree2/scratch.hpp>     // thread-local bump arena (no heap scratch)
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <ucc/round.hpp>                 // ucc::round_up_multiple<N>
#include <algorithm>
#include <cassert>
#include <cstring>

namespace psitri::wtree2 {

// Apple Silicon / x86 fetch unit (64-B lines paired). Regions align to this.
inline constexpr std::size_t kCacheLine = 128;

using node_type       = psitri::node_type;
using ptr_address     = sal::ptr_address;
using ptr_address_seq = sal::ptr_address_seq;
using alloc_hint      = sal::alloc_hint;
using session_t       = sal::allocator_session;
using apgm_header     = psitri::wtree::apgm_header;

// Every node2 carries an APGM estimator (96 B) right after the fixed header,
// before the prefix — inners route stems by it, leaves search their stems by it.
// (Tiny nodes skip the build and fall back to binary search; the 96 B is cheap
// next to the cold cacheline the predictor removes from each level's search.)
inline constexpr uint32_t kEstimatorBytes = uint32_t(sizeof(apgm_header));   // 96

// Build the estimator only for nodes wide enough that the predicted-hit + short
// correction walk beats a binary search. Below this, binary search over the few
// (cacheline-local, HW-prefetch-friendly) stems is cheaper than the predictor's
// segment math + walk — measured: leaf-width (~80) nodes regress with it on.
inline constexpr uint16_t kApgmMinStems = 256;

inline constexpr ptr_address kNoChild = sal::null_ptr_address;

// ── Slot encoding (doc/wtree2.md §3) ──────────────────────────────────
// One u32 child column carries every slot kind, discriminated by value range:
//   EXT   : a real allocatable ptr_address (child node / big value / subtree).
//   LOCAL : a reserved-range value encoding a 4-byte-aligned tail-relative
//           offset to an inline record in THIS node (no parallel offset column).
//   named : the top-16 reserved values (kNoChild / tombstone / …) → empty etc.
// LOCAL records are byte-copied on COW (no refcount); the lifecycle walk filters
// on is_ext_slot() so it touches only real addresses.
inline bool is_ext_slot(ptr_address c) noexcept
{
    return !sal::is_reserved_ptr_address(c);
}
inline bool is_local_slot(ptr_address c) noexcept
{
    return sal::is_reserved_ptr_address(c)
        && *c < *sal::first_named_sentinel_ptr_address;
}
inline uint32_t local_tail_off(ptr_address c) noexcept    // bytes from node end
{
    return (*c - *sal::first_reserved_ptr_address) * 4u;
}
inline ptr_address make_local_slot(uint32_t tail_off) noexcept
{
    return ptr_address(*sal::first_reserved_ptr_address + (tail_off >> 2));
}
// Largest tail offset a LOCAL slot can encode (4-byte units below the named band).
inline constexpr uint32_t kMaxLocalTailOff =
    (*sal::first_named_sentinel_ptr_address - *sal::first_reserved_ptr_address - 1u) * 4u;

// Three shapes (algo §3.1). leaf == LEAF_VARLEN (sub-radix + value_region heap,
// for mixed entry sizes / big values); leaf_fixed == LEAF_FIXED (SoA columns,
// no offset table / no heap — every entry shares a stride, so record j is at a
// computed offset). inner routes. is_leaf() covers both leaf kinds.
enum class shape : uint8_t { leaf = 0, inner = 1, leaf_fixed = 2 };

// ── Header flags ──────────────────────────────────────────────────────
inline constexpr uint8_t kFlagHasTerminal = 0x01;   // a key ends at prefix-end → terminal value present

// ─────────────────────────────────────────────────────────────────────
// Unified node record. The 12-byte psitri::node base (SAL alloc_header:
// size/type/seq/addr) occupies bytes [0,12); wtree2 fields follow.
//
// Physical layout (cacheline-aligned regions; offsets via the helpers below):
//
//   [0  ..12)   psitri::node            (SAL alloc_header)
//   [12 ..20)   wtree2 fixed header     (num_stems, shape, prefix_len, flags, pad)
//   [20 ..  )   inline header content   : (SEAM: terminal, inner estimator) + prefix[prefix_len]
//   stems_off:  stems[num_stems]        u32, cl-aligned
//   children_off: children[num_stems+1] ptr_address (u32), cl-aligned
//   (SEAM: local_offs[num_stems+1] u16 when inline-radix enabled; versions; history tail)
// ─────────────────────────────────────────────────────────────────────
struct node2 : public psitri::node
{
    static constexpr node_type type_id = node_type::wtree2_node;

    uint16_t num_stems;    // 12..13   N dividers → N+1 slots
    uint8_t  shape_;       // 14       shape::{leaf,inner,leaf_fixed}
    uint8_t  prefix_len;   // 15       Patricia prefix length, in u32 words
    uint8_t  flags;        // 16       kFlag* bits
    uint8_t  fx_rem_words; // 17       LEAF_FIXED: key suffix words per entry
    uint8_t  fx_val_len;   // 18       LEAF_FIXED: value bytes per entry
    uint8_t  fx_cap;       // 19       LEAF_FIXED: stem CAPACITY (≥ num_stems; slack for
                           //          in-place grow). Region offsets use it, not num_stems.

    // ── Trivial accessors ─────────────────────────────────────────────
    shape    get_shape()      const noexcept { return shape(shape_); }
    bool     is_inner()       const noexcept { return shape_ == uint8_t(shape::inner); }
    bool     is_leaf()        const noexcept { return shape_ != uint8_t(shape::inner); }   // varlen OR fixed
    bool     is_leaf_fixed()  const noexcept { return shape_ == uint8_t(shape::leaf_fixed); }
    bool     has_terminal()   const noexcept { return (flags & kFlagHasTerminal) != 0; }
    uint16_t n_slots()        const noexcept { return uint16_t(num_stems + 1); }
    uint16_t fx_stride()      const noexcept { return uint16_t(uint16_t(fx_rem_words) * 4u + fx_val_len); }

    // SAL pf2 hint: prefetch the 2nd 128-B line on get_ref. The whole header +
    // estimator + short prefix live in line 0 (≤128 B), so line 1 is the STEMS
    // region (stems_off rounds to 128) — which every find reads immediately after
    // the header. So it pays for EVERY node, and most of all for tiny/cold leaves
    // whose stems line is a guaranteed cold miss. (A query into a wide BigInner
    // lands its predicted stem cl elsewhere, but that node is hot and the extra
    // prefetch is cheap.) Always on.
    bool pf_second_cl() const noexcept { return true; }

    uint8_t*       base()       noexcept { return reinterpret_cast<uint8_t*>(this); }
    const uint8_t* base() const noexcept { return reinterpret_cast<const uint8_t*>(this); }

    // ── Region math ───────────────────────────────────────────────────
    // The fixed header ends at byte 20. `header_content_bytes()` is the single
    // SEAM where later phases add the terminal value and (inner) estimator
    // BEFORE the inline prefix; everything downstream is derived from it.
    static constexpr uint16_t kFixedHeaderBytes = 20;

    // Every node carries an APGM estimator right after the fixed header, before
    // the prefix — inners route stems by it, leaves search their stems by it. The
    // 96 B is negligible vs the cold cacheline it removes from the search.
    apgm_header*       estimator()       noexcept { return reinterpret_cast<apgm_header*>(base() + kFixedHeaderBytes); }
    const apgm_header* estimator() const noexcept { return reinterpret_cast<const apgm_header*>(base() + kFixedHeaderBytes); }
    static constexpr uint32_t est_bytes() noexcept { return kEstimatorBytes; }

    uint32_t header_content_bytes() const noexcept
    {
        return uint32_t(kFixedHeaderBytes) + est_bytes() + uint32_t(prefix_len) * 4u;
    }

    // Inline Patricia prefix (u32 words), after the estimator.
    uint32_t prefix_off() const noexcept { return uint32_t(kFixedHeaderBytes) + est_bytes(); }
    uint32_t*       prefix()       noexcept { return reinterpret_cast<uint32_t*>(base() + prefix_off()); }
    const uint32_t* prefix() const noexcept { return reinterpret_cast<const uint32_t*>(base() + prefix_off()); }
    std::span<const uint32_t> prefix_words() const noexcept { return {prefix(), prefix_len}; }

    uint32_t stems_off() const noexcept
    {
        return uint32_t(ucc::round_up_multiple<kCacheLine>(header_content_bytes()));
    }
    uint32_t children_off() const noexcept
    {
        return stems_off()
             + uint32_t(ucc::round_up_multiple<kCacheLine>(uint32_t(num_stems) * 4u));
    }
    // Index-only size (no value tail). Inner == size(); leaf < size() (has tail).
    uint32_t total_bytes() const noexcept
    {
        return children_off()
             + uint32_t(ucc::round_up_multiple<kCacheLine>(uint32_t(n_slots()) * 4u));
    }

    uint32_t*       stems()       noexcept { return reinterpret_cast<uint32_t*>(base() + stems_off()); }
    const uint32_t* stems() const noexcept { return reinterpret_cast<const uint32_t*>(base() + stems_off()); }
    std::span<const uint32_t> stem_span() const noexcept { return {stems(), num_stems}; }

    ptr_address*       children()       noexcept { return reinterpret_cast<ptr_address*>(base() + children_off()); }
    const ptr_address* children() const noexcept { return reinterpret_cast<const ptr_address*>(base() + children_off()); }
    std::span<const ptr_address> child_span() const noexcept { return {children(), n_slots()}; }

    // Offset where the value-region tail begins (right after the cl-aligned
    // children region) = the index-only size. LOCAL slots address into [this, end).
    static uint32_t tail_off_for(uint16_t n_stems, uint8_t prefix_len) noexcept
    {
        const uint32_t hdr   = uint32_t(kFixedHeaderBytes) + est_bytes()
                             + uint32_t(prefix_len) * 4u;
        const uint32_t s_off = uint32_t(ucc::round_up_multiple<kCacheLine>(hdr));
        const uint32_t c_off = s_off
                             + uint32_t(ucc::round_up_multiple<kCacheLine>(uint32_t(n_stems) * 4u));
        return c_off + uint32_t(ucc::round_up_multiple<kCacheLine>(uint32_t(n_stems + 1) * 4u));
    }
    // Total byte size for (prefix_len, num_stems, value-tail). Inner: tail_bytes==0.
    // Leaf: passes the packed value-region tail. Both carry the estimator.
    static uint32_t size_for(uint16_t n_stems, uint8_t prefix_len,
                             uint32_t tail_bytes = 0) noexcept
    {
        return tail_off_for(n_stems, prefix_len)
             + uint32_t(ucc::round_up_multiple<kCacheLine>(tail_bytes));
    }
    uint32_t tail_off() const noexcept { return tail_off_for(num_stems, prefix_len); }

    // ── LEAF_FIXED region math (algo §3.1) ────────────────────────────
    // No children region, no value heap: stems then a packed record column of
    // fixed stride. record j = records_base + j·stride. Header carries stride
    // (fx_rem_words / fx_val_len) so records need no per-entry header.
    static uint32_t fixed_records_off(uint16_t n_stems, uint8_t prefix_len) noexcept
    {
        const uint32_t hdr   = uint32_t(kFixedHeaderBytes) + est_bytes()
                             + uint32_t(prefix_len) * 4u;
        const uint32_t s_off = uint32_t(ucc::round_up_multiple<kCacheLine>(hdr));
        return s_off + uint32_t(ucc::round_up_multiple<kCacheLine>(uint32_t(n_stems) * 4u));
    }
    static uint32_t fixed_size_for(uint16_t n_stems, uint8_t prefix_len,
                                   uint32_t record_bytes) noexcept
    {
        return fixed_records_off(n_stems, prefix_len)
             + uint32_t(ucc::round_up_multiple<kCacheLine>(record_bytes));
    }
    // Capacity-based: the stems/records regions are sized for fx_cap so a grow up
    // to capacity doesn't move them (in-place insert). num_stems ≤ fx_cap are live.
    uint32_t       records_off() const noexcept { return fixed_records_off(fx_cap, prefix_len); }
    uint8_t*       records()       noexcept { return base() + records_off(); }
    const uint8_t* records() const noexcept { return base() + records_off(); }
    const uint8_t* record(uint16_t j) const noexcept { return records() + size_t(j) * fx_stride(); }
    uint8_t*       record(uint16_t j)       noexcept { return records() + size_t(j) * fx_stride(); }

    // ── SAL alloc contract ────────────────────────────────────────────
    // session.alloc<node2>(hint, shp, n_stems, prefix_len) forwards these to
    // alloc_size (for the byte size) and the placement ctor (header init). The
    // bulk constructor then fills prefix/stems/children. cow_size/compact_size/
    // copy_to/destroy/visit_children use the alloc_header defaults for now
    // (real lifecycle = Phase 6).
    // For LEAF_FIXED, tail_bytes is the packed record-column size and there is no
    // children region; for inner / LEAF_VARLEN it's the value-tail past children.
    static uint32_t alloc_size(shape shp, uint16_t n_stems, uint8_t prefix_len,
                               uint32_t tail_bytes = 0) noexcept
    {
        return shp == shape::leaf_fixed
                   ? fixed_size_for(n_stems, prefix_len, tail_bytes)
                   : size_for(n_stems, prefix_len, tail_bytes);
    }
    // inner / LEAF_VARLEN ctor. Sets only the fields these shapes use; fx_* are
    // leaf_fixed-only and left untouched (unread for these shapes) — no wasted writes.
    node2(uint32_t asize, ptr_address_seq seq,
          shape shp, uint16_t n_stems_, uint8_t prefix_len_,
          uint32_t /*tail_bytes*/ = 0) noexcept
        : psitri::node(asize, type_id, seq)
    {
        num_stems  = n_stems_;
        shape_     = uint8_t(shp);
        prefix_len = prefix_len_;
        flags      = 0;
    }

    // LEAF_FIXED alloc + ctor: regions sized for stem CAPACITY `cap`, `n` live
    // entries, fixed stride (sw key-suffix words + vl value bytes). Every header
    // field is set ONCE here — callers fill prefix/stems/records and do not
    // re-touch the header.
    static uint32_t alloc_size(uint16_t /*n*/, uint16_t cap, uint8_t prefix_len,
                               uint8_t sw, uint8_t vl) noexcept
    {
        return fixed_size_for(cap, prefix_len, uint32_t(cap) * (uint32_t(sw) * 4u + vl));
    }
    node2(uint32_t asize, ptr_address_seq seq,
          uint16_t n, uint16_t cap, uint8_t prefix_len_, uint8_t sw, uint8_t vl) noexcept
        : psitri::node(asize, type_id, seq)
    {
        num_stems    = n;
        shape_       = uint8_t(shape::leaf_fixed);
        prefix_len   = prefix_len_;
        flags        = 0;
        fx_rem_words = sw;
        fx_val_len   = vl;
        fx_cap       = uint8_t(cap);
        // Leaves never carry a predictor (cap < kApgmMinStems); mark it absent so
        // node_stem_ub takes the linear-scan path. Set once here — no finish_estimator.
        estimator()->segment_count = 0;
    }
};

// ── Compile-time layout verification (PHASE 1 "test") ─────────────────
static_assert(sizeof(psitri::node) == 12, "SAL node header must be 12 B");
static_assert(offsetof(node2, num_stems)  == 12, "num_stems at byte 12");
static_assert(offsetof(node2, shape_)     == 14, "shape at byte 14");
static_assert(offsetof(node2, prefix_len) == 15, "prefix_len at byte 15");
static_assert(offsetof(node2, flags)        == 16, "flags at byte 16");
static_assert(offsetof(node2, fx_rem_words) == 17, "fx_rem_words at byte 17");
static_assert(offsetof(node2, fx_val_len)   == 18, "fx_val_len at byte 18");
static_assert(sizeof(node2) == 20, "wtree2 fixed header must be 20 B");
static_assert(node2::kFixedHeaderBytes == 20);

// ── SAL session bridge (per-thread, mirrors wtree's pattern) ──────────
inline thread_local session_t* g_wtree2_session = nullptr;
inline session_t& w2session() noexcept { return *g_wtree2_session; }

template <typename T = node2>
inline T* w2deref(ptr_address a) noexcept { return w2session().template get_ptr<T>(a); }

inline void node2_free(node2* p) noexcept { w2session().release(p->address()); }

// Recursively release a subtree (EXT children first, then self). LOCAL/named
// slots own no node. This is the basis for the Phase-6 lifecycle destroy(); for
// now it's an explicit free used by tests/teardown since node2::destroy is the
// alloc_header no-op default.
inline void free_subtree(node2* nd) noexcept
{
    // LEAF_FIXED has no children region (inline records only) → nothing to recurse.
    if (!nd->is_leaf_fixed())
    {
        const uint16_t     ns = nd->n_slots();
        const ptr_address* ch = nd->children();
        for (uint16_t i = 0; i < ns; ++i)
            if (is_ext_slot(ch[i])) free_subtree(w2session().get_ptr<node2>(ch[i]));
    }
    node2_free(nd);
}

// ── Stem search (algo §6) ─────────────────────────────────────────────
// upper_bound slot index: count of stems ≤ q, in [0, n]. slots[0] handles
// key ≤ stems[0] AND key-ends-here; slots[n] handles key > stems[n-1].
//
// BRANCHLESS LINEAR SCAN (algo §6, small-node mode). Counts how many stems are
// ≤ q with no data-dependent branches: the per-element body is a compare folded
// into an accumulator, so the compiler vectorizes it (NEON/AVX compare + add)
// and it streams the stems region in order — HW-prefetch-friendly. This beats
// binary search at the small widths it runs on: binary search mispredicts a
// branch per step and probes cachelines in a random, un-prefetchable order. We
// never binary-search; wide nodes use the predictor (below) instead.
inline uint16_t stem_upper_bound(const uint32_t* stems, uint16_t n, uint32_t q) noexcept
{
    uint16_t c = 0;
    for (uint16_t j = 0; j < n; ++j)
        c = uint16_t(c + (stems[j] <= q));   // 0/1, no branch → vectorizes
    return c;
}

// Predictor-seeded upper_bound (algo §6, wide-node mode). The APGM estimator maps
// q → an approximate stem position in ~1 cacheline touch; a short branchless
// correction walk lands on the EXACT upper_bound, so the result is identical to a
// full scan regardless of predictor accuracy — only the cachelines touched differ
// (≈1 + a few vs ⌈n·4/128⌉ for a full scan). Caller guarantees segment_count != 0.
inline uint16_t stem_upper_bound_apgm(const apgm_header* h, const uint32_t* stems,
                                      uint16_t n, uint32_t q) noexcept
{
    const uint32_t p = apgm_lookup_stem_position(h, q);
    uint16_t       i = (p >= n) ? n : uint16_t(p);
    while (i > 0 && stems[i - 1] > q) --i;     // walk left  if over-predicted
    while (i < n && stems[i] <= q)    ++i;     // walk right if under-predicted
    return i;
}

// Stem search dispatch (inner routing AND leaf lookup), NO binary search:
//   wide node  (estimator built) → predictor + short correction walk
//   small node (no estimator)    → branchless linear scan
// Every node2 carries the estimator slot; the build threshold (kApgmMinStems)
// decides which mode each node uses.
inline uint16_t node_stem_ub(const node2* nd, uint32_t q) noexcept
{
    const apgm_header* h = nd->estimator();
    if (h->segment_count)
        return stem_upper_bound_apgm(h, nd->stems(), nd->num_stems, q);
    return stem_upper_bound(nd->stems(), nd->num_stems, q);
}

// ── Prefix absorption (algo §9.2) ─────────────────────────────────────
// Longest common prefix (in u32 words) over a sorted set of key-word spans.
// Because the set is sorted, the LCP of the whole set == LCP(first, last).
inline uint16_t longest_common_prefix(std::span<const uint32_t> first,
                                      std::span<const uint32_t> last) noexcept
{
    const size_t lim = std::min(first.size(), last.size());
    uint16_t     i   = 0;
    while (i < lim && first[i] == last[i]) ++i;
    return i;
}

// ── Bulk-in-order constructor: INNER (algo §9.1, inner case) ──────────
// Build a fresh inner node2 from a prefix and a sorted (stems, children) pair
// with children.size() == stems.size()+1 (btree-page semantics). Children are
// stored as-is (EXT ptr_addresses); the builder does NOT retain — the caller
// transfers ownership of one ref per child (matches unique-mode construction).
inline node2* build_inner(std::span<const uint32_t>    prefix,
                          std::span<const uint32_t>    stems,
                          std::span<const ptr_address> children,
                          ptr_address                  hint = kNoChild) noexcept
{
    assert(children.size() == stems.size() + 1);
    assert(prefix.size() <= 255 && stems.size() <= 0xFFFFu);
    const uint16_t n   = uint16_t(stems.size());
    const uint8_t  pfx = uint8_t(prefix.size());
    const ptr_address a =
        (hint == kNoChild)
            ? w2session().alloc<node2>(alloc_hint{}, shape::inner, n, pfx)
            : w2session().alloc<node2>(alloc_hint{&hint, 1}, shape::inner, n, pfx);
    node2* nd = w2deref(a);
    if (pfx) std::memcpy(nd->prefix(), prefix.data(), size_t(pfx) * 4u);
    if (n)   std::memcpy(nd->stems(), stems.data(), size_t(n) * 4u);
    std::memcpy(nd->children(), children.data(), children.size() * sizeof(ptr_address));
    // APGM estimator (§6): build only for inners wide enough to beat binary search;
    // a failed fit (too irregular for ≤9 segments) leaves segment_count==0 → the
    // lookup path falls back to binary search.
    apgm_header* est   = nd->estimator();
    est->segment_count = 0;
    if (n >= kApgmMinStems && apgm_build_adaptive(est, nd->stems(), n) == 0)
        est->segment_count = 0;
    return nd;
}

// ── find DESCENT (algo §5; Phase 3a — routing only) ───────────────────
// Descend from `nd` matching each node's Patricia prefix (the only step that
// consumes key words) and routing by stems (which do NOT consume a word).
// Returns the LEAF reached for `key`, or nullptr if the key is absent (prefix
// mismatch — the "child rejects out-of-range" model, no sentinels — or an empty
// slot). Leaf-internal value retrieval is Phase 3b (needs the value model).
inline node2* find_leaf(node2* nd, std::span<const uint32_t> key, uint16_t depth = 0) noexcept
{
    for (;;)
    {
        // §5.1 prefix match — the ONLY consumer of key words.
        const uint16_t  pl  = nd->prefix_len;
        const uint32_t* pfx = nd->prefix();
        for (uint16_t i = 0; i < pl; ++i)
            if (size_t(depth) + i >= key.size() || key[depth + i] != pfx[i])
                return nullptr;                       // mismatch / key too short → absent
        depth = uint16_t(depth + pl);

        if (nd->is_leaf())
            return nd;                                // reached the leaf hosting (or not) the key

        // §5.2 stem search — terminal (key ends here) routes to slot 0.
        const uint16_t i = (depth >= key.size())
                               ? uint16_t(0)
                               : node_stem_ub(nd, key[depth]);
        const ptr_address c = nd->children()[i];
        if (c == kNoChild)
            return nullptr;                           // empty slot → absent
        nd = w2deref(c);                              // §5.3 depth NOT advanced by the stem match
    }
}

// ── Leaf value record (DIRECT inline; big/tree are EXT slots) ─────────
// An inline entry under a leaf stem: the key suffix past the stem + the value.
// Suffix is compared as bytes (kHdr=2 isn't u32-aligned), which matches equality
// on the encoded key words. (type tag / MVCC versions are a later phase.)
struct value_region
{
    uint8_t suffix_words;   // +0  key words after the stem
    uint8_t value_len;      // +1  inline value bytes (0..255)

    static constexpr uint16_t kHdr = 2;
    const uint8_t* suffix_bytes() const noexcept { return reinterpret_cast<const uint8_t*>(this) + kHdr; }
    uint8_t*       suffix_bytes()       noexcept { return reinterpret_cast<uint8_t*>(this) + kHdr; }
    const uint8_t* value() const noexcept { return suffix_bytes() + size_t(suffix_words) * 4u; }
    uint8_t*       value()       noexcept { return suffix_bytes() + size_t(suffix_words) * 4u; }
    std::span<const uint8_t> value_span() const noexcept { return {value(), value_len}; }
    static uint16_t size_bytes(uint8_t sw, uint8_t vl) noexcept
    {
        return uint16_t(kHdr + uint16_t(sw) * 4u + vl);
    }
    uint16_t total() const noexcept { return size_bytes(suffix_words, value_len); }
};

struct leaf_entry
{
    std::span<const uint32_t> key;
    std::span<const uint8_t>  value;
};

// ── Bulk-build a LEAF_VARLEN (algo §9.1, leaf case) ───────────────────
// DIRECT inline values, DISTINCT first-words at this depth (no collisions → EXT,
// no terminal). Each entry's first un-prefix word is a stem; its (suffix,value)
// is an inline value_region in the tail, addressed by a LOCAL slot. slot[0] =
// empty. Used when entry sizes vary (mixed suffix or value lengths) or big values
// are present; uniform-size leaves go to build_leaf_fixed via the build_leaf
// dispatcher. (Big values → EXT slots land in a later phase.)
inline node2* build_leaf_varlen(std::span<const leaf_entry> ents, uint16_t depth,
                                ptr_address hint = kNoChild) noexcept
{
    assert(!ents.empty());
    uint16_t lcp = longest_common_prefix(
        ents.front().key.subspan(depth), ents.back().key.subspan(depth));
    // §9.2 exception: never absorb the FULL remaining key into the prefix — leave
    // ≥1 word as the stem, so every entry has a stem (no terminal/underflow). For
    // a 1-entry chunk lcp == remaining, so this caps it to remaining-1.
    uint16_t min_rem = 0xFFFFu;
    for (const auto& e : ents)
        min_rem = std::min<uint16_t>(min_rem, uint16_t(e.key.size() - depth));
    if (lcp >= min_rem) lcp = uint16_t(min_rem - 1);
    const uint8_t  pfx        = uint8_t(lcp);
    const uint16_t base_depth = uint16_t(depth + pfx);
    const uint16_t n          = uint16_t(ents.size());

    // Plan the value-tail (4-byte-aligned regions).
    uint32_t tail = 0;
    for (const auto& e : ents)
    {
        assert(e.key.size() > base_depth);   // Phase 3b: no terminal (key extends past prefix)
        const uint16_t sw = uint16_t(e.key.size() - base_depth - 1);
        tail += uint32_t(ucc::round_up_multiple<4>(
            value_region::size_bytes(uint8_t(sw), uint8_t(e.value.size()))));
    }

    const ptr_address a =
        (hint == kNoChild)
            ? w2session().alloc<node2>(alloc_hint{}, shape::leaf, n, pfx, tail)
            : w2session().alloc<node2>(alloc_hint{&hint, 1}, shape::leaf, n, pfx, tail);
    node2* nd = w2deref(a);

    if (pfx) std::memcpy(nd->prefix(), ents.front().key.data() + depth, size_t(pfx) * 4u);
    uint32_t*    st = nd->stems();
    ptr_address* ch = nd->children();
    ch[0] = kNoChild;                                  // slot 0 = terminal/empty
    uint32_t off        = nd->tail_off();              // forward offset of the value-tail
    uint32_t prev_first = 0;
    for (uint16_t j = 0; j < n; ++j)
    {
        const auto&    e     = ents[j];
        const uint32_t first = e.key[base_depth];
        assert(j == 0 || first > prev_first);          // distinct, ascending
        prev_first = first;
        st[j] = first;
        const uint16_t sw = uint16_t(e.key.size() - base_depth - 1);
        auto* vr = reinterpret_cast<value_region*>(nd->base() + off);
        vr->suffix_words = uint8_t(sw);
        vr->value_len    = uint8_t(e.value.size());
        if (sw)              std::memcpy(vr->suffix_bytes(),
                                         reinterpret_cast<const uint8_t*>(e.key.data() + base_depth + 1),
                                         size_t(sw) * 4u);
        if (!e.value.empty()) std::memcpy(vr->value(), e.value.data(), e.value.size());
        ch[j + 1] = make_local_slot(off);
        off += uint32_t(ucc::round_up_multiple<4>(vr->total()));
    }
    // APGM estimator (§6) over the leaf stems — turns the in-leaf upper_bound from
    // a multi-cacheline binary search into a predicted hit + short correction walk.
    apgm_header* est   = nd->estimator();
    est->segment_count = 0;
    if (n >= kApgmMinStems && apgm_build_adaptive(est, nd->stems(), n) == 0)
        est->segment_count = 0;
    return nd;
}

// ── Bulk-build a LEAF_FIXED (algo §3.1) ───────────────────────────────
// Every entry shares the same suffix-word count (sw) and value length (vl), so
// there is no offset table and no per-record header: [stems][record column],
// record j = records_base + j·(sw·4 + vl) = suffix(sw words) ++ value(vl bytes).
// Caller (build_leaf) has verified uniformity and computed pfx/base_depth/sw/vl.
inline node2* build_leaf_fixed(std::span<const leaf_entry> ents, uint16_t depth,
                               uint8_t pfx, uint16_t base_depth, uint8_t sw, uint8_t vl,
                               ptr_address hint) noexcept
{
    const uint16_t n = uint16_t(ents.size());
    // Bulk leaves are exact (capacity == n); the leaf_fixed ctor sets every header
    // field once (num_stems / fx_cap / fx_rem_words / fx_val_len).
    const ptr_address a =
        (hint == kNoChild)
            ? w2session().alloc<node2>(alloc_hint{}, n, n, pfx, sw, vl)
            : w2session().alloc<node2>(alloc_hint{&hint, 1}, n, n, pfx, sw, vl);
    node2* nd = w2deref(a);

    if (pfx) std::memcpy(nd->prefix(), ents.front().key.data() + depth, size_t(pfx) * 4u);
    const uint16_t stride = nd->fx_stride();
    uint32_t* st  = nd->stems();
    uint8_t*  rec = nd->records();
    uint32_t  prev_first = 0;
    for (uint16_t j = 0; j < n; ++j)
    {
        const auto&    e     = ents[j];
        const uint32_t first = e.key[base_depth];
        assert(j == 0 || first > prev_first);          // distinct, ascending
        assert(uint16_t(e.key.size() - base_depth - 1) == sw && e.value.size() == vl);
        prev_first = first;
        st[j] = first;
        uint8_t* r = rec + size_t(j) * stride;
        if (sw) std::memcpy(r, reinterpret_cast<const uint8_t*>(e.key.data() + base_depth + 1),
                            size_t(sw) * 4u);
        if (vl) std::memcpy(r + size_t(sw) * 4u, e.value.data(), vl);
    }
    // (estimator absent for leaves — set once in the ctor.)
    return nd;
}

// ── LEAF dispatcher: pick FIXED (uniform sizes) or VARLEN ─────────────
// Computes the shared Patricia prefix (§9.2 cap) and base depth, then routes to
// the fixed builder when every entry has identical suffix-word count and value
// length (and both fit a byte) — else the varlen builder.
inline node2* build_leaf(std::span<const leaf_entry> ents, uint16_t depth,
                         ptr_address hint = kNoChild) noexcept
{
    assert(!ents.empty());
    uint16_t lcp = longest_common_prefix(
        ents.front().key.subspan(depth), ents.back().key.subspan(depth));
    uint16_t min_rem = 0xFFFFu;
    for (const auto& e : ents)
        min_rem = std::min<uint16_t>(min_rem, uint16_t(e.key.size() - depth));
    if (lcp >= min_rem) lcp = uint16_t(min_rem - 1);
    const uint8_t  pfx        = uint8_t(lcp);
    const uint16_t base_depth = uint16_t(depth + pfx);

    const size_t sw0 = ents.front().key.size() - base_depth - 1;
    const size_t vl0 = ents.front().value.size();
    bool uniform = (sw0 <= 0xFF && vl0 <= 0xFF);
    for (const auto& e : ents)
        if (e.key.size() - base_depth - 1 != sw0 || e.value.size() != vl0) { uniform = false; break; }

    if (uniform)
        return build_leaf_fixed(ents, depth, pfx, base_depth, uint8_t(sw0), uint8_t(vl0), hint);
    return build_leaf_varlen(ents, depth, hint);
}

// ── find (descent + in-leaf lookup) — completes find over wtree2 ──────
// Returns the value span on hit; an empty span with .data()==nullptr on absent
// (so a present zero-length value is distinguishable from absent).
inline std::span<const uint8_t> find_value(node2* nd, std::span<const uint32_t> key) noexcept
{
    uint16_t depth = 0;
    for (;;)
    {
        // (the stems line is line 1 of the node — prefetched at get_ref via the
        //  control-block pf2 bit, so no explicit stems prefetch is needed here.)
        const uint16_t  pl  = nd->prefix_len;
        const uint32_t* pfx = nd->prefix();
        for (uint16_t i = 0; i < pl; ++i)
            if (size_t(depth) + i >= key.size() || key[depth + i] != pfx[i]) return {};
        depth = uint16_t(depth + pl);

        if (nd->is_leaf())
        {
            if (depth >= key.size())
                return {};                                  // terminal (slot 0) — not yet stored
            const uint32_t q = key[depth];

            if (nd->is_leaf_fixed())
            {
                // LEAF_FIXED: record j = records_base + j·stride, computed directly
                // — no children[] indirection. Prefetch the record column so its
                // cold miss overlaps the stem scan.
                __builtin_prefetch(nd->records(), 0, 1);
                const uint16_t c = node_stem_ub(nd, q);
                if (c == 0 || nd->stems()[c - 1] != q) return {};   // q not a stem → absent
                const uint8_t  sw  = nd->fx_rem_words;
                if (size_t(depth) + 1 + sw != key.size()) return {};
                const uint8_t* r = nd->record(uint16_t(c - 1));
                if (sw && std::memcmp(r, reinterpret_cast<const uint8_t*>(key.data() + depth + 1),
                                      size_t(sw) * 4u) != 0) return {};
                return {r + size_t(sw) * 4u, nd->fx_val_len};
            }

            // LEAF_VARLEN: prefetch the children (slot) region so its cold miss
            // overlaps the stem scan; the slot's offset locates the value_region.
            __builtin_prefetch(nd->children(), 0, 1);
            const uint16_t i = node_stem_ub(nd, q);
            if (i == 0 || nd->stems()[i - 1] != q) return {};   // q not a stem → absent
            const ptr_address s = nd->children()[i];
            if (!is_local_slot(s)) return {};               // empty / EXT (Phase 3b: DIRECT only)
            auto* vr = reinterpret_cast<const value_region*>(nd->base() + local_tail_off(s));
            const uint16_t sw = vr->suffix_words;
            if (size_t(depth) + 1 + sw != key.size()) return {};
            if (sw && std::memcmp(vr->suffix_bytes(),
                                  reinterpret_cast<const uint8_t*>(key.data() + depth + 1),
                                  size_t(sw) * 4u) != 0) return {};
            return vr->value_span();
        }

        const uint16_t i = (depth >= key.size())
                               ? uint16_t(0)
                               : node_stem_ub(nd, key[depth]);
        const ptr_address c = nd->children()[i];
        if (!is_ext_slot(c)) return {};                     // empty slot → absent
        nd = w2deref(c);
    }
}

// ── Recursive bulk constructor (algo §9.1, full) ──────────────────────
// Builds a whole subtree from a SORTED entry stream. Handles prefix absorption
// (Case 2), collisions (a same-first-word bucket recurses one radix level
// deeper → EXT child), leaf packing (distinct singletons chunked into leaves),
// and B-tree-ing the radix children when they exceed one inner. Correct by
// construction (no in-place mutation / split-during-insert). For FIXED-length
// distinct keys (no entry is a prefix of another) so terminals don't arise.
inline constexpr uint32_t kLeafTailBudget = 3072;   // value-tail bytes per leaf
inline constexpr uint32_t kLeafCountCap   = 200;    // entries per leaf
inline constexpr uint32_t kInnerStemCap   = 4096;   // dividers per inner (~32 KB)

// B-tree the (divs[0..D), kids[0..D+1)) radix children into inner(s) under prefix.
// Scratch arrays come from the thread-local arena (no heap); caller holds a mark.
inline ptr_address build_inner_tree(std::span<const uint32_t> prefix,
                                    uint32_t* divs, size_t D, ptr_address* kids) noexcept
{
    for (;;)
    {
        if (D <= kInnerStemCap)
            return build_inner(prefix, {divs, D}, {kids, D + 1})->address();
        uint32_t*    nd  = scratch_alloc<uint32_t>(D);
        ptr_address* nk  = scratch_alloc<ptr_address>(D + 1);
        size_t       ndi = 0, nki = 0, a = 0;
        for (;;)
        {
            const size_t b = std::min(a + (kInnerStemCap + 1), D + 1);   // ≤ cap+1 kids
            nk[nki++] = build_inner({}, {divs + a, b - 1 - a}, {kids + a, b - a})->address();
            if (b <= D) { nd[ndi++] = divs[b - 1]; a = b; }             // promote separator
            else break;
        }
        divs = nd;
        D    = ndi;
        kids = nk;
    }
}

inline ptr_address bulk_build(std::span<const leaf_entry> E, uint16_t depth) noexcept
{
    assert(!E.empty());
    const uint8_t  pfx  = uint8_t(longest_common_prefix(
        E.front().key.subspan(depth), E.back().key.subspan(depth)));
    const uint16_t base = uint16_t(depth + pfx);
    auto word_at = [&](const leaf_entry& e) { return e.key[base]; };
    auto round4  = [](uint16_t x) { return uint32_t(ucc::round_up_multiple<4>(x)); };

    // Single-leaf fast path: all distinct at `base` and fits one leaf.
    {
        bool     distinct = true;
        uint32_t tail     = 0;
        for (size_t i = 0; i < E.size(); ++i)
        {
            if (i && word_at(E[i]) == word_at(E[i - 1])) { distinct = false; break; }
            tail += round4(value_region::size_bytes(
                uint8_t(E[i].key.size() - base - 1), uint8_t(E[i].value.size())));
        }
        if (distinct && tail <= kLeafTailBudget && E.size() <= kLeafCountCap)
            return build_leaf(E, depth)->address();
    }

    // One chunk starting at i → (is_collision_bucket, end_index). A collision
    // bucket (>1 entry sharing word[base]) recurses; a singleton run packs into a
    // leaf up to the byte/count budget. Used by both the count and build passes.
    auto advance = [&](size_t i) -> std::pair<bool, size_t> {
        size_t j = i + 1;
        while (j < E.size() && word_at(E[j]) == word_at(E[i])) ++j;
        if (j - i > 1) return {true, j};
        size_t   k = i;
        uint32_t t = 0, n = 0;
        while (k < E.size())
        {
            size_t kk = k + 1;
            while (kk < E.size() && word_at(E[kk]) == word_at(E[k])) ++kk;
            if (kk - k > 1) break;                                    // collision ahead → flush
            const uint32_t rs = round4(value_region::size_bytes(
                uint8_t(E[k].key.size() - base - 1), uint8_t(E[k].value.size())));
            if (n > 0 && (t + rs > kLeafTailBudget || n >= kLeafCountCap)) break;
            t += rs; ++n; ++k;
        }
        return {false, k};
    };

    scratch_mark mark;
    size_t       nb = 0;                                              // child (bucket) count
    for (size_t i = 0; i < E.size();) { nb += 1; i = advance(i).second; }

    uint32_t*    divs = scratch_alloc<uint32_t>(nb);                  // ≤ nb-1 used
    ptr_address* kids = scratch_alloc<ptr_address>(nb);
    size_t       nd = 0, nk = 0;
    for (size_t i = 0; i < E.size();)
    {
        auto [collision, end] = advance(i);
        const ptr_address child = collision ? bulk_build(E.subspan(i, end - i), base)
                                            : build_leaf(E.subspan(i, end - i), base)->address();
        if (nk > 0) divs[nd++] = word_at(E[i]);
        kids[nk++] = child;
        i = end;
    }
    std::span<const uint32_t> pfx_words(E.front().key.data() + depth, pfx);
    return build_inner_tree(pfx_words, divs, nd, kids);
}

}  // namespace psitri::wtree2
