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

namespace psitri::wtree2 {

// Insert result for a slot: InPlace (subtree now at addr) or Split (replace the
// slot with [left, right] separated by divider `div`). A split is ALWAYS binary,
// so no dynamic arrays are needed.
struct ins_result
{
    bool        split = false;
    ptr_address addr  = kNoChild;   // InPlace
    uint32_t    div   = 0;          // Split divider (right group's first word)
    ptr_address left  = kNoChild;   // Split children
    ptr_address right = kNoChild;
};
inline ins_result in_place(ptr_address a) noexcept { return {false, a, 0, kNoChild, kNoChild}; }
inline ins_result split_at(uint32_t d, ptr_address l, ptr_address r) noexcept
{
    return {true, kNoChild, d, l, r};
}

// ── Low-level node builders (SAL alloc + memcpy; no heap) ─────────────
inline void finish_estimator(node2* nd) noexcept
{
    apgm_header* est   = nd->estimator();
    est->segment_count = 0;
    if (nd->num_stems >= kApgmMinStems && apgm_build_adaptive(est, nd->stems(), nd->num_stems) == 0)
        est->segment_count = 0;
}

inline node2* alloc_leaf_fixed(uint16_t n, uint8_t pl, uint8_t sw, uint8_t vl) noexcept
{
    const uint32_t recs = uint32_t(n) * (uint32_t(sw) * 4u + vl);
    node2*         nd =
        w2deref(w2session().alloc<node2>(alloc_hint{}, shape::leaf_fixed, n, pl, recs));
    nd->fx_rem_words = sw;
    nd->fx_val_len   = vl;
    return nd;
}

// ── Generic descent / dispatch ────────────────────────────────────────
inline ins_result insert_rec(node2* nd, std::span<const uint32_t> key,
                             std::span<const uint8_t> val, uint16_t depth) noexcept;

// Rare fallback (collision: same stem different suffix; or a non-uniform-size
// entry that breaks LEAF_FIXED): gather this leaf's entries + the new one into
// the arena and rebuild via bulk_build. Heap-free (arena + SAL). depth is BEFORE
// the leaf's own prefix.
inline ins_result leaf_rebuild_arena(node2* leaf, std::span<const uint32_t> head,
                                     uint16_t depth, std::span<const uint32_t> key,
                                     std::span<const uint8_t> val) noexcept
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
        for (uint16_t j = 0; j < n; ++j)
        {
            const uint8_t* r = leaf->record(j);
            emit(st[j], r, sw, r + size_t(sw) * 4u, vl);
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
            const ptr_address sub = bulk_build({ents, cnt}, depth);
            node2_free(leaf);
            return in_place(sub);
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

    const ptr_address sub = bulk_build({ents, cnt}, depth);
    node2_free(leaf);
    return in_place(sub);   // collision → deeper subtree (pl>0); nests one level (rare)
}

// §7.2 Patricia split: key matches node->prefix[0..j) then diverges. Works for
// inner (trunc = build_inner) and leaf (trunc = leaf re-prefixed) alike.
inline ins_result case2_split(node2* node, std::span<const uint32_t> key,
                              std::span<const uint8_t> val, uint16_t depth, uint16_t j) noexcept
{
    const uint32_t* P          = node->prefix();
    const uint16_t  pl         = node->prefix_len;
    const uint32_t  node_pivot = P[j];
    const uint32_t  key_pivot  = key[depth + j];

    node2* trunc;
    if (node->is_leaf_fixed())
    {
        // Re-prefix the leaf: same stems/records, prefix P[j..pl). (Truncating the
        // FRONT of the prefix doesn't touch stems/records values, only offsets.)
        const uint16_t n = node->num_stems;
        trunc = alloc_leaf_fixed(n, uint8_t(pl - j), node->fx_rem_words, node->fx_val_len);
        std::memcpy(trunc->prefix(), P + j, size_t(pl - j) * 4u);
        std::memcpy(trunc->stems(), node->stems(), size_t(n) * 4u);
        std::memcpy(trunc->records(), node->records(), size_t(n) * node->fx_stride());
        finish_estimator(trunc);
    }
    else  // inner (or varlen leaf, which also has stems/children)
    {
        trunc = build_inner({P + j, size_t(pl - j)}, node->stem_span(), node->child_span());
    }

    leaf_entry e{key, val};
    node2*     nl = build_leaf({&e, 1}, uint16_t(depth + j));

    // Divider = the RIGHT child's pivot (the larger): node_stem_ub routes q==div
    // to the right slot (upper_bound).
    uint32_t    div;
    ptr_address k0, k1;
    if (node_pivot < key_pivot) { div = key_pivot;  k0 = trunc->address(); k1 = nl->address(); }
    else                        { div = node_pivot; k0 = nl->address();    k1 = trunc->address(); }
    ptr_address kids[2] = {k0, k1};
    node2*      small   = build_inner({P, j}, {&div, 1}, {kids, 2});
    node2_free(node);
    return in_place(small->address());
}

// Build a LEAF_FIXED holding merged entries [a,b) of (old leaf + new entry at
// `pos`), with the given prefix. The merged index m maps: m<pos→old[m],
// m==pos→new, m>pos→old[m-1].
inline node2* build_fixed_range(node2* old, uint16_t pos, uint32_t qstem,
                                const uint8_t* qsuffix, std::span<const uint8_t> qval,
                                uint16_t a, uint16_t b, const uint32_t* prefix,
                                uint8_t pl) noexcept
{
    const uint8_t  sw     = old->fx_rem_words, vl = old->fx_val_len;
    const uint16_t stride = old->fx_stride();
    const uint16_t cnt    = uint16_t(b - a);
    node2*         nd     = alloc_leaf_fixed(cnt, pl, sw, vl);
    if (pl) std::memcpy(nd->prefix(), prefix, size_t(pl) * 4u);
    uint32_t* st = nd->stems();
    uint8_t*  rc = nd->records();
    for (uint16_t m = a; m < b; ++m)
    {
        const uint16_t o = uint16_t(m - a);
        if (m == pos)
        {
            st[o] = qstem;
            std::memcpy(rc + size_t(o) * stride, qsuffix, size_t(sw) * 4u);
            std::memcpy(rc + size_t(o) * stride + size_t(sw) * 4u, qval.data(), vl);
        }
        else
        {
            const uint16_t s = uint16_t(m < pos ? m : m - 1);
            st[o] = old->stems()[s];
            std::memcpy(rc + size_t(o) * stride, old->record(s), stride);
        }
    }
    finish_estimator(nd);
    return nd;
}

// Insert a new (distinct) entry into a LEAF_FIXED at sorted index `pos`, handling
// fit (rebuild +1) vs overflow (balanced split). `pl_leaf` = the leaf's prefix
// length (decides Splice vs re-root). depth is AFTER the leaf prefix.
inline ins_result leaf_fixed_add(node2* leaf, uint16_t pos, uint32_t qstem,
                                 const uint8_t* qsuffix, std::span<const uint8_t> qval,
                                 uint16_t depth) noexcept
{
    const uint16_t n  = leaf->num_stems;
    const uint8_t  pl = leaf->prefix_len;
    const uint16_t stride = leaf->fx_stride();
    const uint32_t new_tail = uint32_t(n + 1) * stride;

    if (uint32_t(n + 1) <= kLeafCountCap && new_tail <= kLeafTailBudget)
    {
        // In-place grow: rebuild with one more entry (SAL alloc + memcpy splice).
        node2* nl = build_fixed_range(leaf, pos, qstem, qsuffix, qval, 0, uint16_t(n + 1),
                                      leaf->prefix(), pl);
        node2_free(leaf);
        return in_place(nl->address());
    }

    // Overflow → balanced split at the stem word. The new entry sits at `pos` in
    // the merged sequence of n+1; split index k near the middle.
    const uint16_t total = uint16_t(n + 1);
    const uint16_t k     = uint16_t(total / 2);
    auto merged_stem = [&](uint16_t m) -> uint32_t {
        return m == pos ? qstem : leaf->stems()[m < pos ? m : uint16_t(m - 1)];
    };
    const uint32_t div = merged_stem(k);                 // right group's first word

    // Halves are PREFIXLESS pages at this radix word; the leaf's prefix (if any)
    // moves to a new re-rooted inner above them.
    node2* L = build_fixed_range(leaf, pos, qstem, qsuffix, qval, 0, k, nullptr, 0);
    node2* R = build_fixed_range(leaf, pos, qstem, qsuffix, qval, k, total, nullptr, 0);

    if (pl == 0)
    {
        node2_free(leaf);
        return split_at(div, L->address(), R->address());
    }
    ptr_address kids[2] = {L->address(), R->address()};
    node2*      top     = build_inner({leaf->prefix(), pl}, {&div, 1}, {kids, 2});
    node2_free(leaf);
    return in_place(top->address());
}

// LEAF insert. `gdepth` = depth before the leaf's prefix (for the arena gather /
// re-build, whose keys are reconstructed from `head` = key[0..gdepth)). `sdepth` =
// depth AFTER the leaf's prefix (the stem word position).
inline ins_result leaf_insert(node2* leaf, std::span<const uint32_t> key,
                              std::span<const uint8_t> val, std::span<const uint32_t> head,
                              uint16_t gdepth, uint16_t sdepth) noexcept
{
    if (!leaf->is_leaf_fixed())   // varlen leaf → arena fallback (no insert tests hit this yet)
        return leaf_rebuild_arena(leaf, head, gdepth, key, val);

    const uint32_t q   = key[sdepth];
    const uint16_t pos = node_stem_ub(leaf, q);
    const uint8_t  sw  = leaf->fx_rem_words, vl = leaf->fx_val_len;

    if (pos > 0 && leaf->stems()[pos - 1] == q)
    {
        // Existing stem: duplicate (same suffix → update) or collision.
        uint8_t* rec = leaf->record(uint16_t(pos - 1));
        const bool same = (size_t(sdepth) + 1 + sw == key.size()) &&
                          (sw == 0 || std::memcmp(rec, key.data() + sdepth + 1,
                                                  size_t(sw) * 4u) == 0);
        if (same)
        {
            std::memcpy(rec + size_t(sw) * 4u, val.data(), vl);   // value update in place
            return in_place(leaf->address());
        }
        return leaf_rebuild_arena(leaf, head, gdepth, key, val);   // collision (rare)
    }

    // New entry — must keep the fixed stride, else convert (rare).
    if (uint16_t(key.size() - sdepth - 1) != sw || val.size() != vl)
        return leaf_rebuild_arena(leaf, head, gdepth, key, val);

    return leaf_fixed_add(leaf, pos, q,
                          reinterpret_cast<const uint8_t*>(key.data() + sdepth + 1), val, sdepth);
}

// Splice a child Split into an inner at slot i: children[i]→left, insert
// (div,right) after. Rebuild +1, or Case-1 split when over the stem cap.
inline ins_result inner_splice(node2* inner, uint16_t i, uint32_t div, ptr_address left,
                               ptr_address right) noexcept
{
    const uint16_t  n  = inner->num_stems;
    const uint8_t   pl = inner->prefix_len;
    const uint32_t* P  = inner->prefix();
    const uint32_t*    st = inner->stems();
    const ptr_address* ch = inner->children();

    if (uint32_t(n + 1) <= kInnerStemCap)
    {
        node2*       nd  = nullptr;
        // build inner with n+1 stems
        {
            ptr_address a = w2session().alloc<node2>(alloc_hint{}, shape::inner,
                                                     uint16_t(n + 1), pl);
            nd = w2deref(a);
        }
        if (pl) std::memcpy(nd->prefix(), P, size_t(pl) * 4u);
        uint32_t*    ns = nd->stems();
        ptr_address* nk = nd->children();
        std::memcpy(ns, st, size_t(i) * 4u);
        ns[i] = div;
        std::memcpy(ns + i + 1, st + i, size_t(n - i) * 4u);
        std::memcpy(nk, ch, size_t(i) * sizeof(ptr_address));
        nk[i]     = left;
        nk[i + 1] = right;
        std::memcpy(nk + i + 2, ch + i + 1, size_t(n - i) * sizeof(ptr_address));
        finish_estimator(nd);
        node2_free(inner);
        return in_place(nd->address());
    }

    // Case-1 split: materialise merged (n+1 stems / n+2 children) in the arena.
    scratch_mark mark;
    const uint16_t m  = uint16_t(n + 1);
    uint32_t*      ms = scratch_alloc<uint32_t>(m);
    ptr_address*   mk = scratch_alloc<ptr_address>(uint32_t(m) + 1);
    std::memcpy(ms, st, size_t(i) * 4u);
    ms[i] = div;
    std::memcpy(ms + i + 1, st + i, size_t(n - i) * 4u);
    std::memcpy(mk, ch, size_t(i) * sizeof(ptr_address));
    mk[i] = left; mk[i + 1] = right;
    std::memcpy(mk + i + 2, ch + i + 1, size_t(n - i) * sizeof(ptr_address));

    const uint16_t k    = uint16_t(m / 2);
    const uint32_t pdiv = ms[k];                                   // promoted divider
    node2* Lc = build_inner({}, {ms, k}, {mk, size_t(k) + 1});
    node2* Rc = build_inner({}, {ms + k + 1, size_t(m - k - 1)}, {mk + k + 1, size_t(m - k)});
    if (pl)
    {
        ptr_address kids[2] = {Lc->address(), Rc->address()};
        node2*      top     = build_inner({P, pl}, {&pdiv, 1}, {kids, 2});
        node2_free(inner);
        return in_place(top->address());
    }
    node2_free(inner);
    return split_at(pdiv, Lc->address(), Rc->address());
}

inline ins_result insert_rec(node2* nd, std::span<const uint32_t> key,
                             std::span<const uint8_t> val, uint16_t depth) noexcept
{
    // Prefix match. On divergence: an INNER is a true Patricia split (case2,
    // binary — it's a routing node); a LEAF must RE-PACK (drop/shorten its prefix
    // and add the new key as a stem) so it doesn't degenerate into a binary trie.
    const bool      leaf = nd->is_leaf();
    const uint16_t  pl   = nd->prefix_len;
    const uint32_t* pfx  = nd->prefix();
    for (uint16_t i = 0; i < pl; ++i)
        if (size_t(depth) + i >= key.size() || key[depth + i] != pfx[i])
            return leaf ? leaf_rebuild_arena(nd, key.subspan(0, depth), depth, key, val)
                        : case2_split(nd, key, val, depth, i);
    const uint16_t leaf_depth = depth;            // depth before this node's prefix
    depth = uint16_t(depth + pl);

    if (nd->is_leaf())
        return leaf_insert(nd, key, val, key.subspan(0, leaf_depth), leaf_depth, depth);

    assert(depth < key.size());                   // terminal keys: later phase
    const uint16_t    i = node_stem_ub(nd, key[depth]);
    const ptr_address c = nd->children()[i];
    if (!is_ext_slot(c))
    {
        leaf_entry e{key, val};
        nd->children()[i] = build_leaf({&e, 1}, depth)->address();
        return in_place(nd->address());
    }

    ins_result r = insert_rec(w2deref(c), key, val, depth);
    if (!r.split)
    {
        nd->children()[i] = r.addr;               // in-place pointer update
        return in_place(nd->address());
    }
    return inner_splice(nd, i, r.div, r.left, r.right);
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
    ins_result r = insert_rec(w2deref(root), key, val, 0);
    if (!r.split)
        return r.addr;
    ptr_address kids[2] = {r.left, r.right};
    return build_inner({}, {&r.div, 1}, {kids, 2})->address();   // new root over the split
}

}  // namespace psitri::wtree2
