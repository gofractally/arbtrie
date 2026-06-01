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
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <ucc/round.hpp>                 // ucc::round_up_multiple<N>
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

inline constexpr ptr_address kNoChild = sal::null_ptr_address;

enum class shape : uint8_t { leaf = 0, inner = 1 };

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
    uint8_t  shape_;       // 14       shape::{leaf,inner}
    uint8_t  prefix_len;   // 15       Patricia prefix length, in u32 words
    uint8_t  flags;        // 16       kFlag* bits
    uint8_t  _pad[3];      // 17..19   reserved (future header fields)

    // ── Trivial accessors ─────────────────────────────────────────────
    shape    get_shape()    const noexcept { return shape(shape_); }
    bool     is_leaf()      const noexcept { return shape_ == uint8_t(shape::leaf); }
    bool     is_inner()     const noexcept { return shape_ == uint8_t(shape::inner); }
    bool     has_terminal() const noexcept { return (flags & kFlagHasTerminal) != 0; }
    uint16_t n_slots()      const noexcept { return uint16_t(num_stems + 1); }

    uint8_t*       base()       noexcept { return reinterpret_cast<uint8_t*>(this); }
    const uint8_t* base() const noexcept { return reinterpret_cast<const uint8_t*>(this); }

    // ── Region math ───────────────────────────────────────────────────
    // The fixed header ends at byte 20. `header_content_bytes()` is the single
    // SEAM where later phases add the terminal value and (inner) estimator
    // BEFORE the inline prefix; everything downstream is derived from it.
    static constexpr uint16_t kFixedHeaderBytes = 20;

    uint32_t header_content_bytes() const noexcept
    {
        // PHASE 1: fixed header + inline prefix words.
        // SEAM (later): + terminal_value_bytes() + (is_inner() ? estimator_bytes() : 0)
        return uint32_t(kFixedHeaderBytes) + uint32_t(prefix_len) * 4u;
    }

    // Inline Patricia prefix (u32 words), at the end of the header content.
    uint32_t prefix_off() const noexcept { return uint32_t(kFixedHeaderBytes); }
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
    // Total allocated size for a node with the current num_stems/prefix_len.
    // (SEAM: later phases add the local_offs/version/history regions here.)
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

    // Compute the byte size a node would need for (prefix_len, num_stems) — used
    // by the (Phase 2) bulk constructor to size the allocation before writing.
    static uint32_t size_for(uint16_t n_stems, uint8_t prefix_len) noexcept
    {
        const uint32_t hdr   = uint32_t(kFixedHeaderBytes) + uint32_t(prefix_len) * 4u;
        const uint32_t s_off = uint32_t(ucc::round_up_multiple<kCacheLine>(hdr));
        const uint32_t c_off = s_off
                             + uint32_t(ucc::round_up_multiple<kCacheLine>(uint32_t(n_stems) * 4u));
        return c_off + uint32_t(ucc::round_up_multiple<kCacheLine>(uint32_t(n_stems + 1) * 4u));
    }

    // ── SAL alloc contract ────────────────────────────────────────────
    // session.alloc<node2>(hint, shp, n_stems, prefix_len) forwards these to
    // alloc_size (for the byte size) and the placement ctor (header init). The
    // bulk constructor then fills prefix/stems/children. cow_size/compact_size/
    // copy_to/destroy/visit_children use the alloc_header defaults for now
    // (real lifecycle = Phase 6).
    static uint32_t alloc_size(shape, uint16_t n_stems, uint8_t prefix_len) noexcept
    {
        return size_for(n_stems, prefix_len);
    }
    node2(uint32_t asize, ptr_address_seq seq,
          shape shp, uint16_t n_stems_, uint8_t prefix_len_) noexcept
        : psitri::node(asize, type_id, seq)
    {
        num_stems  = n_stems_;
        shape_     = uint8_t(shp);
        prefix_len = prefix_len_;
        flags      = 0;
        _pad[0] = _pad[1] = _pad[2] = 0;
    }
};

// ── Compile-time layout verification (PHASE 1 "test") ─────────────────
static_assert(sizeof(psitri::node) == 12, "SAL node header must be 12 B");
static_assert(offsetof(node2, num_stems)  == 12, "num_stems at byte 12");
static_assert(offsetof(node2, shape_)     == 14, "shape at byte 14");
static_assert(offsetof(node2, prefix_len) == 15, "prefix_len at byte 15");
static_assert(offsetof(node2, flags)      == 16, "flags at byte 16");
static_assert(sizeof(node2) == 20, "wtree2 fixed header must be 20 B");
static_assert(node2::kFixedHeaderBytes == 20);

// ── SAL session bridge (per-thread, mirrors wtree's pattern) ──────────
inline thread_local session_t* g_wtree2_session = nullptr;
inline session_t& w2session() noexcept { return *g_wtree2_session; }

template <typename T = node2>
inline T* w2deref(ptr_address a) noexcept { return w2session().template get_ptr<T>(a); }

inline void node2_free(node2* p) noexcept { w2session().release(p->address()); }

// ── Stem search (algo §6) ─────────────────────────────────────────────
// upper_bound slot index: count of stems ≤ q, in [0, n]. slots[0] handles
// key ≤ stems[0] AND key-ends-here; slots[n] handles key > stems[n-1].
// Scalar for Phase 2 (SIMD/predictor are later search-mode work).
inline uint16_t stem_upper_bound(const uint32_t* stems, uint16_t n, uint32_t q) noexcept
{
    uint16_t i = 0;
    while (i < n && stems[i] <= q) ++i;
    return i;
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
                               : stem_upper_bound(nd->stems(), nd->num_stems, key[depth]);
        const ptr_address c = nd->children()[i];
        if (c == kNoChild)
            return nullptr;                           // empty slot → absent
        nd = w2deref(c);                              // §5.3 depth NOT advanced by the stem match
    }
}

}  // namespace psitri::wtree2
