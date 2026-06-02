#pragma once
// wtree2/insert.hpp — incremental single-key insert (algo §7), UNIQUE mode.
//
// HARD RULE: no heap allocations. All node memory is SAL alloc (bump-pointer
// persistent allocator); transient scratch is the thread-local arena
// (scratch.hpp); the common paths mutate/rebuild via memcpy with NO scratch at
// all. There is no std::vector anywhere here.
//
// Single-writer, in-place: the parent's child pointer is overwritten in place; a
// touched leaf is rebuilt by SAL alloc + memcpy splice (no gather). Splits return
// a Split result (always exactly one divider + two children), which the parent
// merges. The rare collision / non-uniform-size case falls back to an
// arena-gather + bulk_build (also heap-free). MVCC/COW snapshots are a later phase.

#include <psitri/wtree2/node.hpp>
#include <psitri/wtree2/scratch.hpp>
#include <ucc/fast_memcpy.hpp>   // ucc::memcpy_aligned_64byte (cacheline-aligned fast copy)

namespace psitri::wtree2 {

// Insert result for a slot: InPlace (subtree now at addr) or Split (replace the
// slot with [left, right] separated by divider `div`). A split is ALWAYS binary,
// so no dynamic arrays are needed.
struct ins_result
{
    bool        split   = false;
    bool        flatten = false;    // addr is a pl0 inner whose children/dividers the
    ptr_address addr    = kNoChild; //   parent should ABSORB at the slot (not nest)
    uint32_t    div     = 0;        // Split divider (right group's first word)
    ptr_address left    = kNoChild; // Split children
    ptr_address right   = kNoChild;
};
inline ins_result in_place(ptr_address a) noexcept { return {false, false, a, 0, kNoChild, kNoChild}; }
inline ins_result split_at(uint32_t d, ptr_address l, ptr_address r) noexcept
{
    return {true, false, kNoChild, d, l, r};
}
inline ins_result flatten_into(ptr_address inner) noexcept
{
    return {false, true, inner, 0, kNoChild, kNoChild};
}

// Descent ownership mode (psitri upsert_mode): start `unique` (this writer solely
// owns the spine — mutate in place); the moment a node has ref()>1 (an MVCC
// snapshot pinned it) flip to `shared` and FORK the rest of the path so the
// snapshot's nodes are never mutated. Compile-time so the shared branches vanish
// from the unique fast path.
enum class umode { unique, shared };

// SHARED fork: the new (forked) node and the snapshot's old node both reference the
// off-path children, so each needs +1. The replaced slot `skip` is NOT retained —
// its old child stays owned solely by the snapshot; the fork points slot `skip` at
// the freshly-forked child instead. (Inline-value leaves have no ext children.)
inline void retain_off_path(const node2* inner, uint16_t skip) noexcept
{
    const ptr_address* ch = inner->children();
    const uint16_t     ns = inner->n_slots();
    for (uint16_t j = 0; j < ns; ++j)
        if (j != skip && is_ext_slot(ch[j])) w2session().retain(ch[j]);
}
inline void retain_all_children(const node2* inner) noexcept
{
    const ptr_address* ch = inner->children();
    const uint16_t     ns = inner->n_slots();
    for (uint16_t j = 0; j < ns; ++j)
        if (is_ext_slot(ch[j])) w2session().retain(ch[j]);
}

// ── Low-level node builders (SAL alloc + memcpy; no heap) ─────────────
inline void finish_estimator(node2* nd) noexcept
{
    apgm_header* est   = nd->estimator();
    est->segment_count = 0;
    if (nd->num_stems >= kApgmMinStems && apgm_build_adaptive(est, nd->stems(), nd->num_stems) == 0)
        est->segment_count = 0;
}

// Allocate a LEAF_FIXED with `cap` stem capacity (regions sized for cap) but only
// `n` live entries. cap≥n gives slack for in-place growth.
inline node2* alloc_leaf_fixed(uint16_t n, uint16_t cap, uint8_t pl, uint8_t sw,
                               uint8_t vl, alloc_hint hint = {}) noexcept
{
    // One ctor sets every header field once. The hint = the parent's branch span:
    // the allocator clusters the new node's control block onto a sibling's cacheline
    // so incremental allocs keep the bulk-like ptr_address locality that makes
    // descent touch few control-block cachelines.
    return w2deref(w2session().alloc<node2>(hint, n, cap, pl, sw, vl));
}

// Active-leaf capacity: a LEAF_FIXED is constructed roomy — sized to ~a 4 KB page
// — so inserts grow it in place; the compactor later contracts a settled leaf to
// its exact occupancy (node2::compact_to) and a modify re-expands it. Capacity is
// the entries that fit 4 KB (stem 4 B + record `stride` B each, after the header),
// capped < kApgmMinStems so a leaf never carries a predictor (and fits fx_cap u8).
inline constexpr uint32_t kLeafPageBytes = 4096;
inline uint16_t leaf_capacity(uint16_t stride) noexcept
{
    const uint32_t usable = kLeafPageBytes - 128u;   // header + estimator + pad
    uint32_t       c      = usable / (4u + stride);   // 4 B stem + record
    if (c > kApgmMinStems - 1) c = kApgmMinStems - 1;
    return uint16_t(c ? c : 1);
}

// ── Generic descent / dispatch ────────────────────────────────────────
// The node is carried as a smart_ref fetched at dispatch and threaded BY REFERENCE
// down the recursion (the psitri tree_ops / wt1 node_modify_ pattern). In-place
// mutations go through `nref.modify()`, which copy-on-writes a sync()-finalized
// (read-only) node into the writable segment, KEEPS its ptr_address, and updates
// the ref — so the post-COW pointer is consistent and parent links stay valid.
// Read-only descent uses nref.obj(); only the actual mutation site COWs.
template <umode M>
inline ins_result insert_rec(sal::smart_ref<sal::alloc_header>& nref,
                             std::span<const uint32_t> key,
                             std::span<const uint8_t> val, uint16_t depth) noexcept;

// A rebuilt subtree is flattened into the parent when it is a pl0 inner (same
// radix level — its children/dividers belong directly in the parent, not nested);
// a single leaf or a deeper (prefixed) subtree replaces the slot in place.
inline ins_result rebuilt_result(ptr_address sub) noexcept
{
    node2* ns = w2deref(sub);
    if (ns->is_inner() && ns->prefix_len == 0) return flatten_into(sub);
    return in_place(sub);
}

// Rare fallback (collision: same stem different suffix; or a non-uniform-size
// entry that breaks LEAF_FIXED): gather this leaf's entries + the new one into
// the arena and rebuild via bulk_build. Heap-free (arena + SAL). depth is BEFORE
// the leaf's own prefix.
// SHARED mode forks (build the new leaf, leave `leaf` for the snapshot); UNIQUE
// releases the old leaf. Leaves are inline-value → no child refs to retain.
template <umode M>
inline ins_result leaf_rebuild_arena(node2* leaf, std::span<const uint32_t> head,
                                     uint16_t depth, std::span<const uint32_t> key,
                                     std::span<const uint8_t> val,
                                     alloc_hint phint = {}) noexcept
{
    scratch_mark   mark;
    const uint16_t pl  = leaf->prefix_len;
    const uint32_t* pfx = leaf->prefix();
    const uint32_t* st  = leaf->stems();
    const uint16_t  n   = leaf->num_stems;
    const uint16_t  cap = uint16_t(n + 1);

    leaf_entry* ents = scratch_alloc<leaf_entry>(cap);
    uint16_t    cnt  = 0;

    auto emit = [&](uint32_t stem, const uint8_t* suffix, uint16_t sw, const uint8_t* v,
                    uint16_t vl) {
        const uint16_t kw = uint16_t(depth + pl + 1 + sw);
        uint32_t*      k  = scratch_alloc<uint32_t>(kw);
        std::memcpy(k, head.data(), size_t(depth) * 4u);
        std::memcpy(k + depth, pfx, size_t(pl) * 4u);
        k[depth + pl] = stem;
        std::memcpy(k + depth + pl + 1, suffix, size_t(sw) * 4u);
        uint8_t* vb = scratch_alloc<uint8_t>(vl ? vl : 1);
        std::memcpy(vb, v, vl);
        ents[cnt++] = {{k, kw}, {vb, vl}};
    };

    if (leaf->is_leaf_fixed())
    {
        const uint8_t sw = leaf->fx_rem_words, vl = leaf->fx_val_len;
        for (uint16_t j = 0; j < n; ++j)                    // sorted order (gap-aware)
        {
            const uint8_t* r = leaf->fx_record(j);
            emit(leaf->fx_stem(j), r, sw, r + size_t(sw) * 4u, vl);
        }
    }
    else
    {
        const ptr_address* ch = leaf->children();
        for (uint16_t j = 0; j < n; ++j)
        {
            const ptr_address s = ch[j + 1];
            if (!is_local_slot(s)) continue;
            auto* vr = reinterpret_cast<const value_region*>(leaf->base() + local_tail_off(s));
            emit(st[j], vr->suffix_bytes(), vr->suffix_words, vr->value(), vr->value_len);
        }
    }

    // Insert the new key in ascending order (replace value on exact-key match).
    uint16_t ip = 0;
    for (; ip < cnt; ++ip)
    {
        const auto& k = ents[ip].key;
        if (k.size() == key.size() && std::equal(k.begin(), k.end(), key.begin()))
        {
            uint8_t* vb = scratch_alloc<uint8_t>(val.size() ? val.size() : 1);
            std::memcpy(vb, val.data(), val.size());
            ents[ip].value = {vb, val.size()};
            const ptr_address sub = bulk_build({ents, cnt}, depth, phint);
            if constexpr (M == umode::unique) release_node(leaf);
            return rebuilt_result(sub);
        }
        if (std::lexicographical_compare(key.begin(), key.end(), k.begin(), k.end())) break;
    }
    for (uint16_t m = cnt; m > ip; --m) ents[m] = ents[m - 1];
    {
        uint32_t* k = scratch_alloc<uint32_t>(key.size());
        std::memcpy(k, key.data(), key.size() * 4u);
        uint8_t* vb = scratch_alloc<uint8_t>(val.size() ? val.size() : 1);
        std::memcpy(vb, val.data(), val.size());
        ents[ip] = {{k, key.size()}, {vb, val.size()}};
    }
    ++cnt;

    const ptr_address sub = bulk_build({ents, cnt}, depth, phint);
    if constexpr (M == umode::unique) release_node(leaf);
    return rebuilt_result(sub);   // pl0 inner → flattened into parent (e.g. collision)
}

// §7.2 Patricia split: key matches node->prefix[0..j) then diverges. Works for
// inner (trunc = build_inner) and leaf (trunc = leaf re-prefixed) alike.
// SHARED forks: the new `trunc` copies `node`'s children, so they become shared
// (snapshot + trunc) and are retained; `node` is left for the snapshot. UNIQUE
// releases `node` (its children were cloned 1:1 into trunc → clone-transfer, no neuter).
template <umode M>
inline ins_result case2_split(node2* node, std::span<const uint32_t> key,
                              std::span<const uint8_t> val, uint16_t depth, uint16_t j,
                              alloc_hint phint = {}) noexcept
{
    const uint32_t* P          = node->prefix();
    const uint16_t  pl         = node->prefix_len;
    const uint32_t  node_pivot = P[j];
    const uint32_t  key_pivot  = key[depth + j];

    node2* trunc;
    if (node->is_leaf_fixed())
    {
        // Re-prefix the leaf: same entries, prefix P[j..pl). Copy in SORTED order
        // (source may have a central gap) into the contiguous clone.
        const uint16_t n      = node->num_stems;
        const uint16_t stride = node->fx_stride();
        trunc = alloc_leaf_fixed(n, n, uint8_t(pl - j), node->fx_rem_words, node->fx_val_len, phint);
        std::memcpy(trunc->prefix(), P + j, size_t(pl - j) * 4u);
        uint32_t* ts = trunc->stems();
        uint8_t*  tr = trunc->records();
        for (uint16_t i = 0; i < n; ++i)
        {
            ts[i] = node->fx_stem(i);
            std::memcpy(tr + size_t(i) * stride, node->fx_record(i), stride);
        }
    }
    else  // inner (or varlen leaf, which also has stems/children)
    {
        // node may be a grid (stems not stored) → regenerate them for the re-prefixed
        // clone. build_inner re-detects, so trunc stays a grid if the stems still fit.
        scratch_mark mark;
        uint32_t* nst = scratch_alloc<uint32_t>(node->num_stems ? node->num_stems : 1);
        read_stems(node, nst);
        trunc = build_inner({P + j, size_t(pl - j)}, {nst, node->num_stems}, node->child_span(), phint);
    }

    leaf_entry e{key, val};
    node2*     nl = build_leaf({&e, 1}, uint16_t(depth + j), phint);

    // Divider = the RIGHT child's pivot (the larger): node_stem_ub routes q==div
    // to the right slot (upper_bound).
    uint32_t    div;
    ptr_address k0, k1;
    if (node_pivot < key_pivot) { div = key_pivot;  k0 = trunc->address(); k1 = nl->address(); }
    else                        { div = node_pivot; k0 = nl->address();    k1 = trunc->address(); }
    ptr_address kids[2] = {k0, k1};
    node2*      small   = build_inner({P, j}, {&div, 1}, {kids, 2}, phint);
    if constexpr (M == umode::shared)
    {
        if (node->is_inner())
            retain_all_children(node);   // trunc copied node's children → now shared with snapshot
    }
    // trunc cloned ALL of node's children 1:1 (no consumed slot) → psitri clone
    // transfer: retain + deep-release, no neuter, no COW (read-only-safe).
    else
        release_node_cloned(node);
    return in_place(small->address());
}

// Build a LEAF_FIXED holding merged entries [a,b) of (old leaf + new entry at
// `pos`), with the given prefix. The merged index m maps: m<pos→old[m],
// m==pos→new, m>pos→old[m-1].
inline node2* build_fixed_range(node2* old, uint16_t pos, uint32_t qstem,
                                const uint8_t* qsuffix, std::span<const uint8_t> qval,
                                uint16_t a, uint16_t b, const uint32_t* prefix,
                                uint8_t pl, uint16_t cap, alloc_hint hint = {}) noexcept
{
    const uint8_t  sw     = old->fx_rem_words, vl = old->fx_val_len;
    const uint16_t stride = old->fx_stride();
    const uint16_t cnt    = uint16_t(b - a);
    node2*         nd     = alloc_leaf_fixed(cnt, cap, pl, sw, vl, hint);
    if (pl) std::memcpy(nd->prefix(), prefix, size_t(pl) * 4u);
    // Lay the cnt entries out with a CENTRAL gap so the half has slack to grow in
    // place: head group [0,h) at the front, tail group [h,cnt) at the column end.
    const uint16_t h = uint16_t(cnt / 2);
    nd->leaf_split   = uint8_t(h);
    uint32_t* st = nd->stems();
    uint8_t*  rc = nd->records();
    for (uint16_t o = 0; o < cnt; ++o)
    {
        const uint16_t m    = uint16_t(a + o);              // merged sorted index
        const uint16_t phys = (o < h) ? o : uint16_t(cap - (cnt - o));
        if (m == pos)
        {
            st[phys] = qstem;
            std::memcpy(rc + size_t(phys) * stride, qsuffix, size_t(sw) * 4u);
            std::memcpy(rc + size_t(phys) * stride + size_t(sw) * 4u, qval.data(), vl);
        }
        else
        {
            const uint16_t s = uint16_t(m < pos ? m : m - 1);   // old sorted index
            st[phys] = old->fx_stem(s);
            std::memcpy(rc + size_t(phys) * stride, old->fx_record(s), stride);
        }
    }
    return nd;   // (leaf estimator absent — set in ctor)
}

// Insert at SORTED position `pos` in place, shifting only the nearer side toward
// the central gap (head side if pos≤split, else tail side) — ~n/4 vs n/2. The leaf
// MUST have slack (num_stems < fx_cap). Address unchanged.
inline void leaf_fixed_shift_insert(node2* leaf, uint16_t pos, uint32_t qstem,
                                    const uint8_t* qsuffix, std::span<const uint8_t> qval) noexcept
{
    const uint16_t n      = leaf->num_stems;
    const uint16_t h      = leaf->leaf_split;
    const uint16_t cap    = leaf->fx_cap;
    const uint8_t  sw     = leaf->fx_rem_words, vl = leaf->fx_val_len;
    const uint16_t stride = leaf->fx_stride();
    uint32_t*      st     = leaf->stems();
    uint8_t*       rc     = leaf->records();
    auto write = [&](uint16_t phys) {
        st[phys] = qstem;
        std::memcpy(rc + size_t(phys) * stride, qsuffix, size_t(sw) * 4u);
        std::memcpy(rc + size_t(phys) * stride + size_t(sw) * 4u, qval.data(), vl);
    };
    if (pos <= h)                                          // grow the HEAD into the gap
    {
        std::memmove(st + pos + 1, st + pos, size_t(h - pos) * 4u);
        std::memmove(rc + size_t(pos + 1) * stride, rc + size_t(pos) * stride,
                     size_t(h - pos) * stride);
        write(pos);
        leaf->leaf_split = uint16_t(h + 1);
    }
    else                                                   // grow the TAIL into the gap
    {
        const uint16_t tb = uint16_t(cap - (n - h));       // old tail base (physical)
        const uint16_t j  = uint16_t(pos - h);             // entries before the insert in tail
        std::memmove(st + tb - 1, st + tb, size_t(j) * 4u);
        std::memmove(rc + size_t(tb - 1) * stride, rc + size_t(tb) * stride,
                     size_t(j) * stride);
        write(uint16_t(tb - 1 + j));
    }
    leaf->num_stems = uint16_t(n + 1);     // (< kApgmMinStems → no estimator to rebuild)
}

// Insert a new (distinct) entry into a LEAF_FIXED at sorted index `pos`:
//   slack available → shift in place (no alloc, no full copy);
//   capacity full but under budget → realloc to budget capacity (ucc fast copy of
//                                    the whole live region, once) then shift;
//   over budget → balanced split.
inline ins_result leaf_fixed_add(node2* leaf, uint16_t pos, uint32_t qstem,
                                 const uint8_t* qsuffix, std::span<const uint8_t> qval,
                                 alloc_hint phint) noexcept
{
    const uint16_t n      = leaf->num_stems;
    const uint8_t  pl     = leaf->prefix_len;
    const uint16_t stride = leaf->fx_stride();
    const uint16_t bcap   = leaf_capacity(stride);

    if (uint32_t(n + 1) <= bcap)
    {
        if (n < leaf->fx_cap)                                   // in-place shift (the hot path)
        {
            leaf_fixed_shift_insert(leaf, pos, qstem, qsuffix, qval);
            return in_place(leaf->address());
        }
        // Capacity exhausted but still under budget → grow to budget cap once,
        // rebuilding with the new entry + a fresh central gap (gap-aware read).
        node2* nl = build_fixed_range(leaf, pos, qstem, qsuffix, qval, 0, uint16_t(n + 1),
                                      leaf->prefix(), pl, bcap, phint);
        release_node(leaf);
        return in_place(nl->address());
    }

    // Overflow → balanced split. New entry at merged index `pos`; split at k.
    const uint16_t total = uint16_t(n + 1);
    const uint16_t k     = uint16_t(total / 2);
    const uint32_t div   = (k == pos) ? qstem : leaf->fx_stem(uint16_t(k < pos ? k : k - 1));

    // Halves are PREFIXLESS pages at this radix word, allocated at budget capacity
    // so they have slack to grow in place; the leaf's prefix (if any) re-roots. The
    // halves go back to the parent → cluster near its branches (phint).
    node2* L = build_fixed_range(leaf, pos, qstem, qsuffix, qval, 0, k, nullptr, 0, bcap, phint);
    node2* R = build_fixed_range(leaf, pos, qstem, qsuffix, qval, k, total, nullptr, 0, bcap, phint);

    if (pl == 0)
    {
        release_node(leaf);
        return split_at(div, L->address(), R->address());
    }
    ptr_address kids[2] = {L->address(), R->address()};
    node2*      top     = build_inner({leaf->prefix(), pl}, {&div, 1}, {kids, 2}, phint);
    release_node(leaf);
    return in_place(top->address());
}

// LEAF insert. `gdepth` = depth before the leaf's prefix (for the arena gather /
// re-build, whose keys are reconstructed from `head` = key[0..gdepth)). `sdepth` =
// depth AFTER the leaf's prefix (the stem word position).
inline ins_result leaf_insert(node2* leaf, std::span<const uint32_t> key,
                              std::span<const uint8_t> val, std::span<const uint32_t> head,
                              uint16_t gdepth, uint16_t sdepth, alloc_hint phint) noexcept
{
    if (!leaf->is_leaf_fixed())   // varlen leaf → arena fallback (no insert tests hit this yet)
        return leaf_rebuild_arena<umode::unique>(leaf, head, gdepth, key, val, phint);

    const uint32_t q   = key[sdepth];
    const uint16_t pos = leaf_stem_ub(leaf, q);            // gap-aware sorted upper_bound
    const uint8_t  sw  = leaf->fx_rem_words, vl = leaf->fx_val_len;

    if (pos > 0 && leaf->fx_stem(uint16_t(pos - 1)) == q)
    {
        // Existing stem: duplicate (same suffix → update) or collision.
        uint8_t* rec = leaf->fx_record(uint16_t(pos - 1));
        const bool same = (size_t(sdepth) + 1 + sw == key.size()) &&
                          (sw == 0 || std::memcmp(rec, key.data() + sdepth + 1,
                                                  size_t(sw) * 4u) == 0);
        if (same)
        {
            std::memcpy(rec + size_t(sw) * 4u, val.data(), vl);   // value update in place
            return in_place(leaf->address());
        }
        return leaf_rebuild_arena<umode::unique>(leaf, head, gdepth, key, val, phint);   // collision (rare)
    }

    // New entry — must keep the fixed stride, else convert (rare).
    if (uint16_t(key.size() - sdepth - 1) != sw || val.size() != vl)
        return leaf_rebuild_arena<umode::unique>(leaf, head, gdepth, key, val, phint);

    return leaf_fixed_add(leaf, pos, q,
                          reinterpret_cast<const uint8_t*>(key.data() + sdepth + 1), val, phint);
}

// Absorb `ni` new dividers + `ni`+1 new children into `inner` at slot i (the old
// child[i] is replaced by the new run). Rebuild if it fits the stem cap, else
// Case-1 split (prefixless → promote to parent; prefixed → re-root). The merged
// arrays live in the arena. This is the one path for both a binary child split
// (ni==1) and a flatten (ni == a pl0 sub-inner's stem count).
// `inner` is WRITABLE in unique mode (the caller COW'd it) and the snapshot-shared
// OLD node in shared mode. UNIQUE: grow in place / realloc, release the old. SHARED:
// never mutate `inner` — retain the off-path siblings (now shared by snapshot +
// fork), build the new node(s), and DON'T release `inner` (the snapshot keeps it).
template <umode M>
inline ins_result inner_absorb(node2* inner, uint16_t i, const uint32_t* ins_stems,
                               uint16_t ni, const ptr_address* ins_kids,
                               alloc_hint phint,
                               sal::smart_ref<sal::alloc_header>* self = nullptr) noexcept
{
    const uint16_t n = inner->num_stems;
    const uint16_t m = uint16_t(n + ni);               // merged stem count
    const op_inner_absorb op{i, ni, ins_stems, ins_kids};

    // A grid inner stores no stems (routing is base+shift), so it can't be mutated in
    // place — fall through to the relocate, whose op-ctor regenerates the stems and
    // re-detects (the insert almost always breaks the grid → downgrade to explicit).
    if constexpr (M == umode::unique)
    {
        // In-place grow using cacheline-rounding slack (children_off unchanged) —
        // one-pass splice into the SAME buffer (no realloc, no copy). Skipped for a
        // grid inner (no stored stems to memmove).
        const uint16_t stem_cap = uint16_t(ucc::round_up_multiple<kCacheLine>(uint32_t(n) * 4u) / 4u);
        const uint16_t slot_cap = uint16_t(
            ucc::round_up_multiple<kCacheLine>(uint32_t(n + 1) * 4u) / 4u);
        if (!is_grid(inner) && m <= stem_cap && uint16_t(m + 1) <= slot_cap)
        {
            uint32_t*    ws = inner->stems();
            ptr_address* wk = inner->children();
            std::memmove(wk + i + ni + 1, wk + i + 1, size_t(n - i) * sizeof(ptr_address));
            std::memcpy(wk + i, ins_kids, size_t(ni + 1) * sizeof(ptr_address));
            std::memmove(ws + i + ni, ws + i, size_t(n - i) * 4u);
            std::memcpy(ws + i, ins_stems, size_t(ni) * 4u);
            inner->num_stems = m;
            if (m >= kApgmMinStems) finish_estimator(inner);
            return in_place(inner->address());
        }
    }
    else  // shared
        retain_off_path(inner, i);          // siblings now shared by snapshot + fork

    // NON-SPLIT relocate: the (src, op) ctor splices src→fresh-bytes in ONE pass
    // (no scratch, no copy-then-move).
    if (m <= kInnerStemCap)
    {
        if constexpr (M == umode::unique)
        {
            // GROW: realloc THROUGH the ref → the relocated inner KEEPS its id, so the
            // parent never repoints. realloc frees the old bytes without destroy
            // (children transfer, no release). It returns a borrowed ref to the new
            // bytes; we only need the (unchanged) address. `self`/`inner` are left
            // pointing at the old record_freed bytes, which stay valid for the rest of
            // this insert under the read lock (smart_ref is non-owning) — never reread.
            // self is always set in unique (caller invariant).
            assert(self);
            return in_place(w2session().realloc<node2>(*self, *inner, op).address());
        }
        else  // shared FORK: fresh id, snapshot keeps `inner`
            return in_place(w2deref(w2session().alloc<node2>(phint, *inner, op))->address());
    }

    // SPLIT (rare: m > cap) — materialize the merged sequence once, partition.
    const uint8_t      pl = inner->prefix_len;
    const uint32_t*    P  = inner->prefix();
    const ptr_address* ch = inner->children();
    scratch_mark mark;
    uint32_t*    st = scratch_alloc<uint32_t>(n ? n : 1);   // inner's stems (regen if grid)
    read_stems(inner, st);
    uint32_t*    ms = scratch_alloc<uint32_t>(m);
    ptr_address* mk = scratch_alloc<ptr_address>(uint32_t(m) + 1);
    std::memcpy(ms, st, size_t(i) * 4u);
    std::memcpy(ms + i, ins_stems, size_t(ni) * 4u);
    std::memcpy(ms + i + ni, st + i, size_t(n - i) * 4u);
    std::memcpy(mk, ch, size_t(i) * sizeof(ptr_address));
    std::memcpy(mk + i, ins_kids, size_t(ni + 1) * sizeof(ptr_address));
    std::memcpy(mk + i + ni + 1, ch + i + 1, size_t(n - i) * sizeof(ptr_address));

    const uint16_t k    = uint16_t(m / 2);
    const uint32_t pdiv = ms[k];                                   // promoted divider
    node2* Lc = build_inner({}, {ms, k}, {mk, size_t(k) + 1}, phint);
    node2* Rc = build_inner({}, {ms + k + 1, size_t(m - k - 1)}, {mk + k + 1, size_t(m - k)}, phint);
    if (pl)
    {
        ptr_address kids[2] = {Lc->address(), Rc->address()};
        node2*      top     = build_inner({P, pl}, {&pdiv, 1}, {kids, 2}, phint);
        if constexpr (M == umode::unique) release_node_moved(inner);   // children moved to new node
        return in_place(top->address());
    }
    if constexpr (M == umode::unique) release_node_moved(inner);   // children moved to Lc/Rc
    return split_at(pdiv, Lc->address(), Rc->address());
}

// Binary child split → absorb one divider + two children.
template <umode M>
inline ins_result inner_splice(node2* inner, uint16_t i, uint32_t div, ptr_address left,
                               ptr_address right, alloc_hint phint,
                               sal::smart_ref<sal::alloc_header>* self = nullptr) noexcept
{
    ptr_address kids[2] = {left, right};
    return inner_absorb<M>(inner, i, &div, 1, kids, phint, self);
}

// Flatten a pl0 sub-inner `sib` into `inner` at slot i (merge its dividers +
// children). `sib` is always a fresh/forked node (from the child's rebuild) → released
// in both modes; `inner` is released only in unique mode (snapshot keeps it in shared).
template <umode M>
inline ins_result inner_flatten(node2* inner, uint16_t i, node2* sib, alloc_hint phint,
                                sal::smart_ref<sal::alloc_header>* self = nullptr) noexcept
{
    // sib may itself be a grid (its stems aren't stored) → regenerate them into scratch
    // before splicing. sib's children are always stored.
    scratch_mark mark;
    uint32_t* sst = scratch_alloc<uint32_t>(sib->num_stems ? sib->num_stems : 1);
    read_stems(sib, sst);
    ins_result r = inner_absorb<M>(inner, i, sst, sib->num_stems, sib->children(), phint, self);
    release_node_moved(sib);   // sib is always FRESH (born writable) → neuter, no COW needed
    return r;
}

// Make the node writable (COW if it sits in a sync()-finalized read-only segment)
// THROUGH the threaded ref, so the ref tracks the new location. Returns the
// writable node2*; the ptr_address (nref.address()) is unchanged.
inline node2* writable(sal::smart_ref<sal::alloc_header>& nref) noexcept
{
    node2* w = nullptr;
    nref.as<node2>().modify([&](node2* n) { w = n; });
    return w;
}

// `phint` = the PARENT's branch span — any node this level passes back to the
// parent (split siblings, a rebuilt/forked node) is allocated with it, so its
// control block clusters onto a sibling's cacheline (ptr_address locality). When
// recursing we pass THIS node's branch span as the child's phint; a node that
// becomes a child of THIS node (sentinel leaf) uses THIS node's span too.
template <umode M>
inline ins_result insert_rec(sal::smart_ref<sal::alloc_header>& nref,
                             std::span<const uint32_t> key,
                             std::span<const uint8_t> val, uint16_t depth,
                             alloc_hint phint) noexcept
{
    // Ownership flip (psitri pattern): a unique descent that meets a snapshot-pinned
    // node (ref>1) re-enters in shared mode and FORKS the rest of the path.
    if constexpr (M == umode::unique)
        if (nref.ref() > 1)
            return insert_rec<umode::shared>(nref, key, val, depth, phint);

    node2* nd = const_cast<node2*>(static_cast<const node2*>(nref.obj()));
    // Prefix match. INNER divergence → Patricia split (case2); LEAF → re-pack. In
    // shared mode these fork (free_old=false) and the snapshot keeps `nd`.
    const bool      leaf = nd->is_leaf();
    const uint16_t  pl   = nd->prefix_len;
    const uint32_t* pfx  = nd->prefix();
    for (uint16_t i = 0; i < pl; ++i)
        if (size_t(depth) + i >= key.size() || key[depth + i] != pfx[i])
            return leaf ? leaf_rebuild_arena<M>(nd, key.subspan(0, depth), depth, key, val, phint)
                        : case2_split<M>(nd, key, val, depth, i, phint);
    const uint16_t leaf_depth = depth;            // depth before this node's prefix
    depth = uint16_t(depth + pl);

    if (leaf)
    {
        if constexpr (M == umode::unique)         // unique: mutate the leaf in place (COW via ref)
            return leaf_insert(writable(nref), key, val, key.subspan(0, leaf_depth), leaf_depth,
                               depth, phint);
        else                                      // shared: fork into a fresh leaf, keep the old
            return leaf_rebuild_arena<M>(nd, key.subspan(0, leaf_depth), leaf_depth, key, val, phint);
    }

    // This inner's branch span — the hint for its own children + the descent.
    // Resolve children() ONCE (children_off recompute is not free) and reuse.
    const ptr_address* ch   = nd->children();
    const alloc_hint   mybr{ch, nd->n_slots()};

    assert(depth < key.size());                   // terminal keys: later phase
    const uint16_t    i = node_stem_ub(nd, key[depth]);
    const ptr_address c = ch[i];
    if (!is_ext_slot(c))                          // empty slot → fill with a fresh leaf
    {
        leaf_entry        e{key, val};
        const ptr_address nl = build_leaf({&e, 1}, depth, mybr)->address();   // child of nd → nd's span
        if constexpr (M == umode::unique)
        {
            writable(nref)->children()[i] = nl;
            return in_place(nref.address());
        }
        else                                      // fork this inner, point slot i at the new leaf
            return inner_absorb<umode::shared>(nd, i, nullptr, 0, &nl, phint);
    }

    auto       cref = w2session().template get_ref<sal::alloc_header>(c);
    ins_result r    = insert_rec<M>(cref, key, val, depth, mybr);   // child's phint = THIS node's span
    if (r.flatten)                                // absorb the child's siblings (widen, don't nest)
    {
        if constexpr (M == umode::unique)
            return inner_flatten<M>(writable(nref), i, w2deref(r.addr), phint, &nref);
        else
            return inner_flatten<M>(nd, i, w2deref(r.addr), phint);
    }
    if (!r.split)
    {
        if constexpr (M == umode::unique)
        {
            if (r.addr != c)                      // child relocated/rebuilt → repoint
                writable(nref)->children()[i] = r.addr;
            return in_place(nref.address());      // unchanged child → no write, no COW
        }
        else                                      // shared: child always forked → fork + repoint
            return inner_absorb<umode::shared>(nd, i, nullptr, 0, &r.addr, phint);
    }
    if constexpr (M == umode::unique)
        return inner_splice<M>(writable(nref), i, r.div, r.left, r.right, phint, &nref);
    else
        return inner_splice<M>(nd, i, r.div, r.left, r.right, phint);
}

// ── insert ────────────────────────────────────────────────────────────
inline ptr_address insert(ptr_address root, std::span<const uint32_t> key,
                          std::span<const uint8_t> val) noexcept
{
    if (root == kNoChild)
    {
        leaf_entry e{key, val};
        return build_leaf({&e, 1}, 0)->address();
    }
    auto       nref       = w2session().template get_ref<sal::alloc_header>(root);  // ref at dispatch
    const bool was_shared = nref.ref() > 1;       // root pinned by a snapshot → forked below
    // Top-level hint: cluster root-level allocations near the old root.
    const ptr_address rootspan[1] = {root};
    ins_result r = insert_rec<umode::unique>(nref, key, val, 0, alloc_hint{rootspan, 1});
    ptr_address new_root;
    if (r.flatten || !r.split)
        new_root = r.addr;                        // flatten/in-place: the result is the new root
    else
    {
        ptr_address kids[2] = {r.left, r.right};
        new_root = build_inner({}, {&r.div, 1}, {kids, 2}, alloc_hint{rootspan, 1})->address();
    }
    // Shared root: the live tree forked to new_root; drop the live ref on the old
    // root so it survives only on the snapshot's retain (the snapshot reads it intact).
    if (was_shared) w2session().release(root);
    return new_root;
}

}  // namespace psitri::wtree2
