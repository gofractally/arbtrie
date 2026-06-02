#pragma once
// wtree2/insert.hpp — incremental single-key insert (algo §7), UNIQUE mode.
//
// Phase 4 (integration test + perf). Single-writer, in-place mutation: the
// parent's child pointer is overwritten in place and only the touched leaf is
// rebuilt (no whole-inner clone per insert). MVCC / snapshot COW is a later
// phase; this version assumes the caller owns the tree exclusively.
//
// Strategy: descend by first-word routing to the target leaf, GATHER its entries
// + the new key, and rebuild via bulk_build (which already handles prefix
// absorption, collisions→deeper radix, and packing). The rebuilt subtree is
// spliced back at the parent's slot. On overflow the radix dividers are promoted
// into the parent (flatten, not nest) so the tree stays shallow (§7.1); a prefix
// divergence is a Patricia split (§7.2). Both reuse bulk_build over the gathered
// set, so there is one correctness path, not five.
//
// INCREMENT A (this file, first cut): descent + in-place leaf add (gathered set
// fits one leaf / small collision subtree) + duplicate update + empty-slot
// install + empty tree. Overflow-split promotion (§7.1) and explicit Patricia
// split (§7.2) land next.

#include <psitri/wtree2/node.hpp>

#include <vector>

namespace psitri::wtree2 {

// Owned reconstruction of a leaf's entries (+ room to merge the new key). The
// leaf_entry spans point into key_words / val_bytes, which must outlive use.
struct gathered_entries
{
    std::vector<std::vector<uint32_t>> key_words;
    std::vector<std::vector<uint8_t>>  val_bytes;
    std::vector<leaf_entry>            ents;

    void rebuild_spans() noexcept
    {
        ents.clear();
        ents.reserve(key_words.size());
        for (size_t i = 0; i < key_words.size(); ++i)
            ents.push_back({{key_words[i].data(), key_words[i].size()},
                            {val_bytes[i].data(), val_bytes[i].size()}});
    }
};

// Reconstruct the FULL key of every entry in `leaf` (head ++ prefix ++ stem ++
// suffix), where `head` = the key words consumed by ancestors before this leaf
// (= the descending key's first `depth` words; all leaf entries share them).
inline void gather_leaf(node2* leaf, std::span<const uint32_t> head, uint16_t depth,
                        gathered_entries& out) noexcept
{
    const uint16_t  pl  = leaf->prefix_len;
    const uint32_t* pfx = leaf->prefix();
    const uint32_t* st  = leaf->stems();
    const uint16_t  n   = leaf->num_stems;

    auto emit = [&](uint32_t stem, const uint8_t* suffix, uint16_t sw, const uint8_t* val,
                    uint16_t vl) {
        std::vector<uint32_t> k;
        k.reserve(size_t(depth) + pl + 1 + sw);
        k.insert(k.end(), head.begin(), head.begin() + depth);   // ancestor-consumed words
        k.insert(k.end(), pfx, pfx + pl);                        // leaf Patricia prefix
        k.push_back(stem);                                       // the stem word
        const uint32_t* sw_words = reinterpret_cast<const uint32_t*>(suffix);
        k.insert(k.end(), sw_words, sw_words + sw);              // suffix words
        out.key_words.push_back(std::move(k));
        out.val_bytes.emplace_back(val, val + vl);
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
    else  // LEAF_VARLEN
    {
        const ptr_address* ch = leaf->children();
        for (uint16_t j = 0; j < n; ++j)
        {
            const ptr_address s = ch[j + 1];
            if (!is_local_slot(s)) continue;   // empty / EXT (big values: later phase)
            auto* vr = reinterpret_cast<const value_region*>(leaf->base() + local_tail_off(s));
            emit(st[j], vr->suffix_bytes(), vr->suffix_words, vr->value(), vr->value_len);
        }
    }
}

// Merge the new (key,value) into a gathered set: replace value on exact-key
// match (duplicate → update), else insert keeping ascending key order. Returns
// true if it was a fresh insert (count grew).
inline bool merge_entry(gathered_entries& g, std::span<const uint32_t> key,
                        std::span<const uint8_t> val) noexcept
{
    // find insertion point / duplicate (keys are short; linear is fine for now)
    size_t i = 0;
    for (; i < g.key_words.size(); ++i)
    {
        const auto& k = g.key_words[i];
        if (k.size() == key.size() &&
            std::equal(k.begin(), k.end(), key.begin()))
        {
            g.val_bytes[i].assign(val.begin(), val.end());   // duplicate → update
            return false;
        }
        // ascending order: stop at first key greater than the new one
        if (std::lexicographical_compare(key.begin(), key.end(), k.begin(), k.end()))
            break;
    }
    g.key_words.insert(g.key_words.begin() + i,
                       std::vector<uint32_t>(key.begin(), key.end()));
    g.val_bytes.insert(g.val_bytes.begin() + i,
                       std::vector<uint8_t>(val.begin(), val.end()));
    return true;
}

// Result of a recursive insert into a node's slot:
//   InPlace  — the subtree for this slot is now at `addr` (parent overwrites its
//              child pointer; addr may equal the input node if mutated in place).
//   Splice   — this slot should become `kids` children with `stems` dividers
//              between them (parent merges them into its own arrays at the slot).
struct ins_result
{
    bool                     splice = false;
    ptr_address              addr   = kNoChild;   // InPlace
    std::vector<uint32_t>    stems;               // Splice dividers
    std::vector<ptr_address> kids;                // Splice children (stems.size()+1)
};
inline ins_result in_place(ptr_address a) noexcept { return {false, a, {}, {}}; }

// Max dividers per inner before a Case-1 split (matches bulk builder's cap).
inline constexpr uint16_t kInnerSplitCap = kInnerStemCap;

inline ins_result insert_rec(node2* nd, std::span<const uint32_t> key,
                             std::span<const uint8_t> val, uint16_t depth) noexcept;

// §7.2 Patricia split: new key matches inner->prefix[0..j) but diverges at word j.
inline ins_result case2_split(node2* inner, std::span<const uint32_t> key,
                              std::span<const uint8_t> val, uint16_t depth, uint16_t j) noexcept
{
    const uint32_t* P          = inner->prefix();
    const uint16_t  pl         = inner->prefix_len;
    const uint32_t  node_pivot = P[j];
    const uint32_t  key_pivot  = key[depth + j];

    // trunc = the old inner re-rooted with prefix P[j..pl) (stems-don't-consume:
    // its prefix[0] re-states the pivot word). Same stems/children (refs move).
    node2* trunc = build_inner({P + j, size_t(pl - j)}, inner->stem_span(), inner->child_span());
    // new leaf for the diverging key, entered at depth+j (its prefix re-states key_pivot).
    leaf_entry e{key, val};
    node2*     nl = build_leaf({&e, 1}, uint16_t(depth + j));

    uint32_t    div;
    ptr_address k0, k1;
    if (node_pivot < key_pivot) { div = node_pivot; k0 = trunc->address(); k1 = nl->address(); }
    else                        { div = key_pivot;  k0 = nl->address();    k1 = trunc->address(); }
    ptr_address kids[2]  = {k0, k1};
    node2*      small    = build_inner({P, j}, {&div, 1}, {kids, 2});
    node2_free(inner);   // old shell; its children now belong to trunc
    return in_place(small->address());
}

// Splice a child's Split result into `inner` at slot i; rebuild, or Case-1 split.
inline ins_result splice_into(node2* inner, uint16_t i, ins_result& r) noexcept
{
    const uint16_t              n  = inner->num_stems;
    const uint32_t*             st = inner->stems();
    const ptr_address*          ch = inner->children();
    std::vector<uint32_t>       ns;
    std::vector<ptr_address>    nk;
    ns.reserve(size_t(n) + r.stems.size());
    nk.reserve(size_t(n) + 1 + r.kids.size());
    // children[0..i) + r.kids + children[i+1..n] ; stems[0..i) + r.stems + stems[i..n)
    nk.insert(nk.end(), ch, ch + i);
    nk.insert(nk.end(), r.kids.begin(), r.kids.end());
    nk.insert(nk.end(), ch + i + 1, ch + n + 1);
    ns.insert(ns.end(), st, st + i);
    ns.insert(ns.end(), r.stems.begin(), r.stems.end());
    ns.insert(ns.end(), st + i, st + n);

    const uint16_t m  = uint16_t(ns.size());
    const uint8_t  pl = inner->prefix_len;
    const uint32_t* P = inner->prefix();
    if (m <= kInnerSplitCap)
    {
        node2* ni = build_inner({P, pl}, {ns.data(), m}, {nk.data(), nk.size()});
        node2_free(inner);
        return in_place(ni->address());
    }
    // Case-1 split: promote ns[k]; left/right are PREFIXLESS btree pages at this
    // radix word (the prefix, if any, stays above them).
    const uint16_t k   = uint16_t(m / 2);
    const uint32_t div = ns[k];
    node2* left  = build_inner({}, {ns.data(), k}, {nk.data(), size_t(k) + 1});
    node2* right = build_inner({}, {ns.data() + k + 1, size_t(m - k - 1)},
                               {nk.data() + k + 1, size_t(m - k)});
    if (pl)
    {
        // Prefixed inner = the top of this radix level's btree. It can't promote
        // to the grandparent (which routes a different word), so re-root: a new
        // top carries the prefix + the single promoted divider over left/right.
        ptr_address kids[2] = {left->address(), right->address()};
        node2*      top     = build_inner({P, pl}, {&div, 1}, {kids, 2});
        node2_free(inner);
        return in_place(top->address());
    }
    // Prefixless internal page: promote the divider to the parent btree.
    node2_free(inner);
    ins_result out;
    out.splice = true;
    out.stems  = {div};
    out.kids   = {left->address(), right->address()};
    return out;
}

inline ins_result insert_rec(node2* nd, std::span<const uint32_t> key,
                             std::span<const uint8_t> val, uint16_t depth) noexcept
{
    if (nd->is_leaf())
    {
        // Gather the leaf's entries (full keys from `depth`) and merge the new one.
        gathered_entries g;
        gather_leaf(nd, key, depth, g);
        merge_entry(g, key, val);
        g.rebuild_spans();
        const size_t cnt = g.ents.size();

        // Plan: does the merged set still fit ONE leaf, and is word[depth] distinct
        // across entries (no collision at this radix level)?
        bool     distinct = true;
        uint32_t tail     = 0;
        for (size_t j = 0; j < cnt; ++j)
        {
            const auto& k = g.key_words[j];
            assert(k.size() > depth);            // terminal keys: later phase
            const uint16_t sw = uint16_t(k.size() - depth - 1);
            tail += uint32_t(ucc::round_up_multiple<4>(
                value_region::size_bytes(uint8_t(sw), uint8_t(g.val_bytes[j].size()))));
            if (j && k[depth] == g.key_words[j - 1][depth]) distinct = false;
        }
        const bool fits = cnt <= kLeafCountCap && tail <= kLeafTailBudget;

        if (fits)
        {
            // One leaf (distinct) or a small collision subtree — replace in place.
            const ptr_address sub = bulk_build({g.ents.data(), cnt}, depth);
            node2_free(nd);
            return in_place(sub);
        }

        // Overflow → BALANCED split at word[depth] (two ~equal halves, so each has
        // room to grow — avoids the greedy [cap,1] pack that fragments to singletons).
        size_t k = cnt / 2;
        while (k < cnt && g.key_words[k][depth] == g.key_words[k - 1][depth]) ++k;
        if (k >= cnt)                            // no boundary at/after mid → try before
        {
            k = cnt / 2;
            while (k > 0 && g.key_words[k][depth] == g.key_words[k - 1][depth]) --k;
        }
        if (k == 0 || k >= cnt)                  // all share word[depth] → deep collision
        {
            const ptr_address sub = bulk_build({g.ents.data(), cnt}, depth);
            node2_free(nd);
            return in_place(sub);                // nests one radix level (rare)
        }
        // Divider = the RIGHT group's first word: stem search routes q==div to the
        // right slot (upper_bound), so equal-keyed entries must sit on the right.
        const uint32_t    div = g.key_words[k][depth];
        const ptr_address L   = bulk_build({g.ents.data(), k}, depth);
        const ptr_address R   = bulk_build({g.ents.data() + k, cnt - k}, depth);
        node2_free(nd);
        ins_result out;
        out.splice = true;
        out.stems  = {div};
        out.kids   = {L, R};
        return out;
    }

    // Inner: prefix match (Case 2 on divergence).
    const uint16_t  pl  = nd->prefix_len;
    const uint32_t* pfx = nd->prefix();
    for (uint16_t i = 0; i < pl; ++i)
        if (size_t(depth) + i >= key.size() || key[depth + i] != pfx[i])
            return case2_split(nd, key, val, depth, i);
    depth = uint16_t(depth + pl);

    assert(depth < key.size());   // terminal keys: later phase (fixed-len keys don't hit this)
    const uint16_t i = node_stem_ub(nd, key[depth]);
    const ptr_address c = nd->children()[i];
    if (!is_ext_slot(c))
    {
        leaf_entry e{key, val};
        nd->children()[i] = build_leaf({&e, 1}, depth)->address();
        return in_place(nd->address());
    }

    ins_result r = insert_rec(w2deref(c), key, val, depth);
    if (!r.splice)
    {
        nd->children()[i] = r.addr;              // in-place pointer update
        return in_place(nd->address());
    }
    return splice_into(nd, i, r);
}

// ── insert ────────────────────────────────────────────────────────────
// Returns the (possibly new) root address. UNIQUE mode: mutates slots in place;
// caller holds the lock for the duration.
inline ptr_address insert(ptr_address root, std::span<const uint32_t> key,
                          std::span<const uint8_t> val) noexcept
{
    if (root == kNoChild)
    {
        leaf_entry e{key, val};
        return build_leaf({&e, 1}, 0)->address();
    }
    ins_result r = insert_rec(w2deref(root), key, val, 0);
    if (!r.splice)
        return r.addr;
    // Root split → a fresh top inner over the promoted dividers.
    return build_inner({}, {r.stems.data(), r.stems.size()},
                       {r.kids.data(), r.kids.size()})->address();
}

}  // namespace psitri::wtree2
