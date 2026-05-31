#pragma once

// wtree — radix B-tree using the new leaf_node / inner_node
// architecture and the recursive upsert_result contract.
//
// Tree structure:
//   - leaves: leaf_node (4 KB only, stems + radix-per-stem + value-region)
//   - inner:  inner_node (4 KB → 64 KB, dividers + child pointers)
//   - both share common_header at byte 0 → dispatch via header.type()
//
// upsert pattern (one function per level, same shape):
//
//   upsert_result node_upsert(node*, key) {
//       branch = find_branch(node, key)
//       result = recurse(*branch, key)
//       if (result.right) parent_insert_after_split(node, branch, result.divider, result.right)
//       *branch = result.left
//       // return our own upsert_result describing what happened TO US
//   }
//
// At the top, if root_upsert returns right!=null we allocate a fresh
// inner_node root with [left, divider, right].

#include "leaf_node.hpp"
#include "inner_node.hpp"   // pulls in apply.hpp (op_insert / apply<mode>)
#include "key_codec.hpp"

#include <cassert>
#include <cstdint>
#include <unordered_set>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <span>

namespace psitri::wtree {

class wtree {
public:
    // Stage 2 (SAL): the tree allocates from `session`. `activate()` installs
    // it as the per-thread active session before any op so the node
    // free-functions (alloc/free/child-deref) can reach it.
    wtree(session_t& session, uint16_t value_len) noexcept
        : _session(&session), value_len_(value_len) {
        assert(value_len <= 255);   // 0 ⇒ variable-length mode
    }
    // Non-owning VIEW of an existing tree (shared root) through another session
    // — e.g. a per-thread reader session over a tree built elsewhere. Its
    // destructor does NOT free the tree. The viewing session must read the same
    // allocator's data; concurrent readers each use their own session.
    wtree(session_t& session, uint16_t value_len, ptr_address root) noexcept
        : _session(&session), root_(root), value_len_(value_len), owns_root_(false) {
        assert(value_len <= 255);   // 0 ⇒ variable-length mode
    }
    // Root address of this tree (for opening a reader view from another session).
    ptr_address root() const noexcept { return root_; }

    // Move: transfer the root ownership and neuter the source so its destructor
    // doesn't double-release. Needed so snapshot() can return a wtree by value.
    wtree(wtree&& o) noexcept
        : _session(o._session), root_(o.root_),
          value_len_(o.value_len_), owns_root_(o.owns_root_) {
        o.root_      = kNoChild;
        o.owns_root_ = false;
    }

    // Take a point-in-time MVCC snapshot: retain the current root (bumping its
    // refcount to ≥2) and hand back a wtree that reads it. The extra ref both
    // (a) keeps the old spine alive after the live tree path-copies away from
    // it, and (b) makes the live writer see the root as shared (ref>1), forcing
    // copy-on-write to a new root instead of mutating in place. The returned
    // snapshot OWNS that one retain and releases it on destruction (recursing
    // through destroy()). Read-only by convention — find() only.
    wtree snapshot() const noexcept {
        activate();
        sal::read_lock lock = _session->lock();
        if (root_ != kNoChild) _session->retain(root_);
        wtree snap(*_session, value_len_);   // owning → its dtor release()s the retain
        snap.root_ = root_;
        return snap;
    }

    ~wtree() {
        if (!owns_root_) return;                  // views don't free the shared tree
        activate();
        sal::read_lock lock = _session->lock();   // pin against relocation during teardown
        if (root_ != kNoChild) free_subtree_(root_ptr());
    }
    wtree(const wtree&)            = delete;
    wtree& operator=(const wtree&) = delete;

    // Install this tree's session as the per-thread active session.
    void activate() const noexcept { g_wtree_session = _session; }

    // Public API: byte key (must be a multiple of 4 bytes) + byte value.
    // The thin wrapper byteswap-copies the bytes into a stack-local u32
    // array; everything below this is u32-only.
    bool upsert(byte_key_type key, value_type value) noexcept {
        activate();
        // Per-op read lock (cheap atomic snapshot publish): released when this
        // call returns, so the compactor's reclaim horizon advances after every
        // insert and freed leaves are reclaimed in step. This is the canonical
        // psitri pattern (tree_ops::upsert: `sal::read_lock lock = _session.lock();`).
        sal::read_lock lock = _session->lock();
        // value_len_ == 0 ⇒ variable-length mode (each entry self-describes its
        // length, 0..255); otherwise enforce the tree's fixed width.
        assert(value_len_ == 0 || value.size() == value_len_);
        assert(value.size() <= 255);
        assert(key.size() % 4 == 0);
        assert(key.size() <= kVarlenEncBuf);
        if (root_ == kNoChild) {
            leaf_node* lf = leaf_alloc();
            leaf_init(lf);
            root_ = as_header(lf)->address();
        }
        uint32_t kw[kVarlenEncBuf / 4];
        const uint16_t n_u32 = key_bytes_to_u32_(key, kw);
        return upsert_words_(word_key_type(kw, size_t(n_u32)), value);
    }
    // Debug-find: returns (leaf_ptr, value_ptr) for the path that find takes.
    // leaf_ptr is the LEAF reached; value_ptr is the matched value (or null).
    // Useful for tracking which leaf hosts a given key over time.
    struct find_trace {
        const void* leaf = nullptr;
        const uint8_t* value = nullptr;
        // Path: per inner level, the (inner_ptr, pos, q, stems[pos-1], stems[pos], delta_advance).
        struct step {
            const void* inner;
            uint16_t pos;
            uint16_t num_keys;
            uint32_t q;
            uint32_t stem_prev;   // stems[pos-1] or 0 if pos==0
            uint32_t stem_at;     // stems[pos] or 0 if pos>=num_keys
            uint16_t depth_in;
            bool advanced;
        };
        step path[16];
        uint8_t path_len = 0;
    };
    find_trace find_trace_(byte_key_type key) const noexcept {
        activate();
        sal::read_lock lock = _session->lock();
        assert(key.size() % 4 == 0);
        if (root_ == kNoChild) return {};
        uint32_t kw[kVarlenEncBuf / 4];
        const uint16_t n_u32 = key_bytes_to_u32_(key, kw);
        return node_find_trace_(root_ptr(), word_key_type(kw, size_t(n_u32)), /*depth=*/0);
    }
    find_trace find_trace_varlen(byte_key_type raw) const noexcept {
        uint8_t enc[kVarlenEncBuf];
        psitri::wtree::encode_key(raw.data(), uint16_t(raw.size()), enc);
        return find_trace_(byte_key_type(enc, psitri::wtree::encoded_len(uint16_t(raw.size()))));
    }
    value_type find(byte_key_type key) const noexcept {
        activate();
        sal::read_lock lock = _session->lock();   // per-op snapshot
        assert(key.size() % 4 == 0);
        assert(key.size() <= kVarlenEncBuf);
        if (root_ == kNoChild) return {};
        uint32_t kw[kVarlenEncBuf / 4];
        const uint16_t n_u32 = key_bytes_to_u32_(key, kw);
        return node_find_(root_ptr(), word_key_type(kw, size_t(n_u32)), /*depth=*/0);
    }

    // Remove `key`. Returns true if it was present. Empties tombstone their
    // slot (kNoChild) and free the node; v1 does not merge/collapse underfull
    // nodes — future inserts reuse the freed slots (the sentinel-fill path).
    bool remove(byte_key_type key) noexcept {
        activate();
        sal::read_lock lock = _session->lock();
        assert(key.size() % 4 == 0);
        assert(key.size() <= kVarlenEncBuf);
        if (root_ == kNoChild) return false;
        uint32_t kw[kVarlenEncBuf / 4];
        const uint16_t   n_u32     = key_bytes_to_u32_(key, kw);
        const ptr_address root_addr = root_;
        const rm r = node_remove_(root_ptr(), word_key_type(kw, size_t(n_u32)), /*depth=*/0);
        if (r == rm::not_found) return false;
        if (r == rm::removed_empty) {           // whole tree emptied
            _session->release(root_addr);
            root_ = kNoChild;
            return true;
        }
        // Fold a chain of single-child B-tree inners at the root back down —
        // ultimately to the single leaf the tree started from.
        while (root_ != kNoChild) {
            common_header* rt = root_ptr();
            if (rt->type() != node_type::wtree_inner) break;
            inner_node* ri = as_inner(rt);
            if (ri->prefix_len != 0 || ri->num_keys != 0) break;
            const ptr_address g = inner_child_addr(ri, 0);
            if (g == kNoChild) break;
            root_ = g;
            inner_free_moved(cow_inner(ri));
        }
        return true;
    }
    bool remove_varlen(byte_key_type raw) noexcept {
        uint8_t enc[kVarlenEncBuf];
        psitri::wtree::encode_key(raw.data(), uint16_t(raw.size()), enc);
        return remove(byte_key_type(enc, psitri::wtree::encoded_len(uint16_t(raw.size()))));
    }

    // upsert_varlen: arbitrary-length raw bytes; key_codec encodes them
    // to a mult-of-4 byte representation, then we dispatch to upsert().
    bool upsert_varlen(byte_key_type raw, value_type value) noexcept {
        uint8_t enc[kVarlenEncBuf];
        psitri::wtree::encode_key(raw.data(), uint16_t(raw.size()), enc);
        return upsert(byte_key_type(enc, psitri::wtree::encoded_len(uint16_t(raw.size()))),
                       value);
    }
    value_type find_varlen(byte_key_type raw) const noexcept {
        uint8_t enc[kVarlenEncBuf];
        psitri::wtree::encode_key(raw.data(), uint16_t(raw.size()), enc);
        return find(byte_key_type(enc, psitri::wtree::encoded_len(uint16_t(raw.size()))));
    }

    // Walk the whole tree, verify the delta=1 invariant: every slot
    // [stems[pos-1], stems[pos]) where stems[pos]-stems[pos-1]==1
    // must have a non-leaf child (i.e., a sub-inner that routes by
    // key[depth+1]).  Returns count of violations.
    struct invariant_report {
        size_t delta1_pairs   = 0;
        size_t leaf_violations = 0;   // delta=1 with leaf child
        size_t null_in_delta1  = 0;   // delta=1 with null child (also OK if sentinel)
    };
    invariant_report verify_invariant() const noexcept {
        activate();
        invariant_report r;
        verify_invariant_walk_(root_ptr(), r);
        return r;
    }

    // Full structural invariant check (for tests): every node reached at most
    // once (it's a tree, not a DAG), stems strictly ascending in every node,
    // and each leaf's dead_bytes == tail_alloc_pos - reachable heap. Returns a
    // report; `ok` is true iff all held.
    struct structure_report {
        size_t leaves = 0, inners = 0;
        bool   ok = true;        // conjunction of the below
        bool   sorted = true;    // stems strictly ascending in every node
        bool   tree = true;      // no node reachable from two slots
        bool   dead_ok = true;   // leaf dead_bytes accounting exact
    };
    structure_report check_structure() const noexcept {
        activate();
        sal::read_lock lock = _session->lock();
        structure_report r;
        std::unordered_set<uint32_t> seen;
        if (root_ != kNoChild) structure_walk_(root_ptr(), seen, r);
        return r;
    }
    void structure_walk_(common_header* node, std::unordered_set<uint32_t>& seen,
                         structure_report& r) const noexcept {
        if (!node) return;
        if (!seen.insert(uint32_t(*node->address())).second) { r.tree = r.ok = false; return; }
        if (node->type() == node_type::wtree_leaf) {
            ++r.leaves;
            const leaf_node* lf = as_leaf(node);
            const uint32_t* st = leaf_stems(lf);
            for (uint16_t i = 1; i < lf->num_keys; ++i)
                if (st[i] <= st[i - 1]) { r.sorted = r.ok = false; break; }
            const uint32_t liveb = leaf_live_bytes(lf, value_len_);
            if (uint32_t(lf->dead_bytes) != uint32_t(lf->tail_alloc_pos) - liveb)
                r.dead_ok = r.ok = false;
            return;
        }
        ++r.inners;
        const inner_node* in = as_inner(node);
        const uint32_t* st = inner_stems(in);
        for (uint16_t i = 1; i < in->num_keys; ++i)
            if (st[i] <= st[i - 1]) { r.sorted = r.ok = false; break; }
        for (uint16_t i = 0; i <= in->num_keys; ++i)
            structure_walk_(inner_child(in, i), seen, r);
    }

    // Walks one descent path from root to a leaf, returns level count.
    size_t tree_depth() const noexcept {
        activate();
        sal::read_lock lock = _session->lock();
        size_t d = 0;
        const common_header* n = root_ptr();
        while (n && n->type() == node_type::wtree_inner) {
            ++d;
            n = inner_child(as_inner(n), 0);
        }
        return d + 1;   // +1 for the leaf level
    }

    // Walk the tree counting leaves / inners.  O(N) — call when you
    // want it; we don't maintain a running counter on the hot path.
    struct page_counts { size_t leaves = 0; size_t inners = 0; };
    page_counts count_pages() const noexcept {
        activate();
        sal::read_lock lock = _session->lock();
        page_counts c;
        count_pages_(root_ptr(), c);
        return c;
    }

private:
    static constexpr size_t kVarlenEncBuf = 432;
    // Inner pages dynamically grow (powers of 2 like std::vector).
    // Start small (cap_in_cls=8 = 1 KB), double on overflow up to 512
    // cls (64 KB).  Avoids over-allocation for sparse trees.
    static constexpr uint16_t kInnerInitialCapInCls = 8;     // 1 KB

    session_t*     _session  = nullptr;
    // Root is held ACROSS ops — the compactor can relocate it between ops, so
    // it must be a (stable) ptr_address, re-deref'd per op, not a raw pointer.
    ptr_address    root_ = kNoChild;
    uint16_t       value_len_ = 0;
    bool           owns_root_ = true;   // false for a non-owning reader view

    // Deref the root to a live pointer (nullptr if empty). Within an op the
    // read_lock keeps it valid.
    common_header* root_ptr() const noexcept {
        return root_ == kNoChild ? nullptr : wderef<node>(root_);
    }

    // Free the whole tree by releasing the root; the deferred destroy()
    // cascade reclaims the rest (inner::destroy releases each child, which
    // recurses; leaf::destroy is a no-op). destroy() only READS child
    // addresses, so this is safe even when nodes are synced/read-only —
    // unlike a manual neuter-based walk, whose inner_free_moved writes
    // num_keys=0 into mprotected pages (SIGBUS).
    void free_subtree_(common_header* node) noexcept {
        if (node) _session->release(node->address());
    }

    static void verify_invariant_walk_(const common_header* node, invariant_report& r) noexcept {
        if (!node) return;
        if (node->type() != node_type::wtree_inner) return;
        const auto* in = as_inner(node);
        const uint32_t* stems = inner_stems(in);
        for (uint16_t pos = 1; pos < in->num_keys; ++pos) {
            if (stems[pos] - stems[pos - 1] == 1u) {
                ++r.delta1_pairs;
                common_header* child = inner_child(in, pos);
                if (!child) { ++r.null_in_delta1; continue; }
                if (child->type() == node_type::wtree_leaf) ++r.leaf_violations;
            }
        }
        for (uint16_t i = 0; i <= in->num_keys; ++i) {
            verify_invariant_walk_(inner_child(in, i), r);
        }
    }

    static void count_pages_(const common_header* node, page_counts& c) noexcept {
        if (!node) return;
        switch (node->type()) {
            case node_type::wtree_leaf: ++c.leaves; return;
            case node_type::wtree_inner: {
                ++c.inners;
                const auto* in = as_inner(node);
                for (uint16_t i = 0; i <= in->num_keys; ++i) count_pages_(inner_child(in, i), c);
                return;
            }
            default: assert(false && "count_pages_: unknown node kind");
        }
    }

    // ─── Key prep ─────────────────────────────────────────────────
    // Byteswap-copy a mult-of-4 byte key into a u32 array.  After this,
    // memcmp on the u32 storage bytes gives the same ordering as memcmp
    // on the original key bytes — which lets us compare full keys with
    // a single u32-by-u32 loop (or equivalent memcmp on size_bytes()).
    static uint16_t key_bytes_to_u32_(byte_key_type key, uint32_t* out) noexcept {
        assert(key.size() % 4 == 0);
        const uint16_t whole = uint16_t(key.size() >> 2);
        const uint32_t* src = reinterpret_cast<const uint32_t*>(key.data());
        const uint32_t* const src_end = src + whole;
        for (uint32_t* dst = out; src != src_end; ++src, ++dst) {
            *dst = __builtin_bswap32(*src);
        }
        return whole;
    }

    // ─── upsert_words_: internal entry point with span<u32> key ────
    bool upsert_words_(word_key_type key,
                       value_type value) noexcept {
        upsert_result r = node_upsert_(root_ptr(), key, /*depth=*/0, value);
        // {null,null,0} = drop (heap-tight leaf with no usable structure
        // for promotion).  Surface to caller.
        if (!r.left) return false;
        if (!r.right) {
            root_ = r.left->address();
            return true;
        }
        inner_node* new_root = inner_alloc_sized(kInnerInitialCapInCls, r.left->address());
        inner_init(new_root, kInnerInitialCapInCls);
        if (r.right == kSentinelChild()) {
            // K=1 promotion at root.  We need to encode the full
            // delta=1 pair so descent into the sub-inner advances depth:
            //   stems   = [X, X+1]
            //   children= [null (q<X, empty), new_inner (q∈[X,X+1)), null (q≥X+1, empty)]
            // For root creation, all incoming queries have q≥X (the leaf
            // only ever held first-u32=X), so the q<X slot would only
            // see keys we don't have yet — null is the correct child.
            inner_stems(new_root)[0] = r.prev_divider;   // X
            inner_stems(new_root)[1] = r.divider;        // X+1
            inner_set_child(new_root, 0, nullptr);
            inner_set_child(new_root, 1, r.left);        // the sub-inner
            inner_set_child(new_root, 2, nullptr);
            new_root->num_keys = 2;
        } else {
            // Normal split: [left, divider, right].
            inner_stems(new_root)[0] = r.divider;
            inner_set_child(new_root, 0, r.left);
            inner_set_child(new_root, 1, r.right);
            new_root->num_keys = 1;
        }
        (void)inner_rebuild_corrections(new_root);
        root_ = as_header(new_root)->address();
        return true;
    }

    // ─── find: dispatch on node type ──────────────────────────────
    //
    // Depth-advance invariant: when adjacent parent dividers differ
    // by exactly 1, the slot between them is structurally limited to
    // keys with one specific first-u32 value, so the child below must
    // be in the depth-advanced form (sub-inner routing by key[depth+1]
    // emitted by the K=1 promotion path).  Any normal split that would
    // create a delta=1 pair MUST trigger the same promotion on the
    // affected child to maintain the invariant — that enforcement is
    // not yet wired (TODO; see the K=1 leaf-split path).
    find_trace node_find_trace_(const common_header* node,
                                  word_key_type key,
                                  uint16_t depth) const noexcept {
        find_trace tr;
        while (true) {
            if (node->type() == node_type::wtree_leaf) {
                const auto* lf = as_leaf(node);
                tr.leaf  = static_cast<const void*>(lf);
                tr.value = leaf_find(lf, key, depth, value_len_).data();
                return tr;
            }
            const auto* in = as_inner(node);
            const uint32_t q = (depth < key.size()) ? key[depth] : 0u;
            const uint16_t pos = inner_find_branch(in, q);
            const uint32_t* stems = inner_stems(in);
            const uint16_t depth_in = depth;
            common_header* nxt = inner_child(in, pos);
            bool adv = false;
            // Child-side prefix decides depth-advance now (was: parent's
            // delta=1 stem pair).  Routing invariant: q must equal the
            // child's claimed prefix when the child has one.
            if (nxt && nxt->type() == node_type::wtree_inner) {
                const auto* sub = as_inner(nxt);
                if (sub->prefix_len > 0) {
                    if (q == sub->prefix_data) {
                        depth = uint16_t(depth + sub->prefix_len);
                        adv = true;
                    } else {
                        ++psitri::wtree::g_routing_inconsistency;
                        // Trace continues; caller will see value=null.
                    }
                }
            }
            if (tr.path_len < 16) {
                tr.path[tr.path_len++] = {
                    static_cast<const void*>(in), pos, in->num_keys, q,
                    pos > 0 ? stems[pos - 1] : 0u,
                    pos < in->num_keys ? stems[pos] : 0u,
                    depth_in, adv
                };
            }
            node = inner_child(in, pos);
            if (!node) return tr;
        }
    }
    value_type node_find_(const common_header* node,
                               word_key_type key,
                               uint16_t depth) const noexcept {
        while (true) {
            if (node->type() == node_type::wtree_leaf) {
                return leaf_find(as_leaf(node), key, depth, value_len_);
            }
            const auto* in = as_inner(node);
            const uint32_t q = (depth < key.size()) ? key[depth] : 0u;
            const uint16_t pos = inner_find_branch(in, q);
            node = inner_child(in, pos);
            if (!node) return value_type{};   // sentinel slot (empty); key absent
            __builtin_prefetch(node,                                0, 1);
            __builtin_prefetch(reinterpret_cast<const char*>(node) + kCacheLine, 0, 1);
            // Depth-advance is now decided by the CHILD's self-asserted
            // prefix, not the parent's stem-pair difference.  This
            // eliminates spurious advances caused by accidental delta=1
            // stem pairs (two unrelated keys' first u32s happening to
            // differ by 1).  Routing invariant: if the child claims a
            // prefix, the query's key word at this depth must match.
            if (node->type() == node_type::wtree_inner) {
                const auto* sub = as_inner(node);
                if (sub->prefix_len > 0) {
                    if (q != sub->prefix_data) {
                        // Parent over-routed: q doesn't belong in this
                        // depth-advanced child.  Construction bug — bump
                        // the counter and treat as miss rather than
                        // descend with wrong depth.
                        ++psitri::wtree::g_routing_inconsistency;
                        return value_type{};
                    }
                    depth = uint16_t(depth + sub->prefix_len);
                }
            }
        }
    }

    // ─── choose_safe_split_k_keep_pair_: like _safe_, also keep pos+i intact ─
    //
    // Like choose_safe_split_k_ but ALSO forbids split_k == keep_pos.
    // Used when we've just inserted X at keep_pos and need to insert
    // X+1 at keep_pos+1 — splitting at keep_pos would make X the
    // up_div, separating it from X+1 (loses the delta=1 signal).
    static uint16_t choose_safe_split_k_keep_pair_(const inner_node* in,
                                                     uint16_t want,
                                                     uint16_t keep_pos) noexcept {
        const uint32_t* stems = inner_stems(in);
        const uint16_t K = in->num_keys;
        auto unsafe = [&](uint16_t k) -> bool {
            if (k == 0 || k >= K) return true;
            if (k == keep_pos) return true;          // would split the pair
            if (stems[k] - stems[k - 1] == 1u) return true;
            if (k + 1 < K && stems[k + 1] - stems[k] == 1u) return true;
            return false;
        };
        if (!unsafe(want)) return want;
        for (uint16_t d = 1; d < K; ++d) {
            if (want > d && !unsafe(uint16_t(want - d))) return uint16_t(want - d);
            if (uint16_t(want + d) < K && !unsafe(uint16_t(want + d))) return uint16_t(want + d);
        }
        ++psitri::wtree::g_inner_split_unsafe;
        return want;
    }

    // ─── replace_slot_: atomic multi-stem splice into an inner ────────
    //
    // Replace child[pos] with a sequence of (n_stems + 1) children and
    // n_stems sorted stems.  After the call:
    //
    //   child[pos + i]                = repl_children[i]   for i in [0, n_stems]
    //   stems[pos + i]                = repl_stems[i]      for i in [0, n_stems-1]
    //   child[pos + n_stems + 1 + j]  = old child[pos + 1 + j]   (shifted right)
    //   stems[pos + n_stems + j]      = old stems[pos + j]       (shifted right)
    //
    // The new inner ends up with K + n_stems stems and K + n_stems + 1
    // children.  Picks the smallest cap_in_cls that fits — grows in
    // place when possible, B-tree splits ONCE when the result exceeds
    // the 64 KB max page.  In the split case, both halves are written
    // directly from the merged layout; never goes through the
    // single-insert grow-or-fail interface, so there's no concept of a
    // mid-sequence split or routing.
    //
    // Returns upsert_inplace(in_or_grown) or upsert_split(L, up_div, R).
    upsert_result replace_slot_(inner_node* in,
                                  uint16_t pos,
                                  uint16_t n_stems,
                                  const uint32_t* repl_stems,
                                  common_header* const* repl_children) noexcept {
        const uint16_t K     = in->num_keys;
        const uint16_t K_new = uint16_t(K + n_stems);

        // ── Pick destination cap: smallest cls that fits K_new + 1 children.
        // inner_cap_in_cls_for() clamps to kInnerMaxPageCls, so verify the
        // chosen cls actually has room (else split).
        const uint16_t need_cls = inner_cap_in_cls_for(K_new);
        const bool fits_max     = (K_new <= inner_cap_for(need_cls));

        if (fits_max) {
            // In-place (writable + fits) OR realloc same-id (grow / COW) —
            // BOTH keep `in`'s address, so the caller returns it unchanged and
            // the grandparent skips its COW. The grow/COW remake ctor fuses the
            // copy and the splice in one pass (no copy-then-memmove).
            op_replace_slot op{};
            op.pos           = pos;
            op.n_stems       = n_stems;
            op.repl_stems    = repl_stems;
            op.repl_children = repl_children;
            auto              ref = wsession().get_ref<inner_node>(in->address());
            const ptr_address r   = apply<upsert_mode::unique_upsert>(ref, op);
            return upsert_inplace(wderef<inner_node>(r));
        }

        // ── Split: K_new exceeds max page.  `in` is freed (inner_free_moved)
        // after its children move to L/R → make it writable first.
        in = cow_inner(in);
        const uint32_t* in_st = inner_stems(in);
        auto mst = [&](uint16_t mk) noexcept -> uint32_t {
            if (mk < pos)            return in_st[mk];
            if (mk < pos + n_stems)  return repl_stems[mk - pos];
            return in_st[mk - n_stems];
        };

        // Choose split_k in the
        //   MERGED index space, delta=1-safe.
        auto unsafe = [&](uint16_t k) -> bool {
            if (k == 0 || k >= K_new) return true;
            if (mst(k) - mst(uint16_t(k - 1)) == 1u) return true;
            if (k + 1 < K_new && mst(uint16_t(k + 1)) - mst(k) == 1u) return true;
            return false;
        };
        uint16_t split_k = uint16_t(K_new >> 1);
        if (unsafe(split_k)) {
            bool found = false;
            for (uint16_t d = 1; d < K_new; ++d) {
                if (split_k > d && !unsafe(uint16_t(split_k - d))) {
                    split_k = uint16_t(split_k - d); found = true; break;
                }
                if (uint16_t(split_k + d) < K_new && !unsafe(uint16_t(split_k + d))) {
                    split_k = uint16_t(split_k + d); found = true; break;
                }
            }
            if (!found) ++psitri::wtree::g_inner_split_unsafe;
        }
        const uint32_t up_div = mst(split_k);
        const uint16_t r_n    = uint16_t(K_new - split_k - 1);

        // Allocate L and R sized for their halves.
        const uint16_t L_cls = inner_cap_in_cls_for(split_k);
        const uint16_t R_cls = inner_cap_in_cls_for(r_n);
        inner_node* L = inner_alloc_sized(L_cls, in->address());
        inner_node* R = inner_alloc_sized(R_cls, L->address());
        inner_init(L, L_cls);
        inner_init(R, R_cls);

        // Write each half with bulk memcpy where the merged region maps
        // contiguously to a source range.  The merged layout splits into
        // up to 3 contiguous source spans (orig prefix, repl block, orig
        // tail).  For each half we slice those spans by split_k.
        const uint8_t* src_ch = inner_children_base(in);
        const uint32_t* src_st = inner_stems(in);
        write_half_(L, 0, split_k, pos, n_stems, repl_stems, repl_children,
                     src_st, src_ch);
        write_half_(R, uint16_t(split_k + 1), r_n, pos, n_stems,
                     repl_stems, repl_children, src_st, src_ch);
        (void)inner_rebuild_corrections(L);
        (void)inner_rebuild_corrections(R);
        ++g_inner_btree_split;
        inner_free_moved(in);
        return upsert_split(L, up_div, R);
    }

    // Helper: write `n` stems (= num_keys) + `n+1` children into `dst`
    // by slicing the merged layout starting at merged index `mk_start`.
    // Used by the split path.
    static void write_half_(inner_node* dst,
                              uint16_t mk_start, uint16_t n,
                              uint16_t pos, uint16_t n_stems,
                              const uint32_t* repl_stems,
                              common_header* const* repl_children,
                              const uint32_t* src_st, const uint8_t* src_ch) noexcept {
        // Source mapping (merged → original):
        //   stems:    [0, pos) → src_st[0..pos-1]
        //             [pos, pos+n_stems) → repl_stems[0..n_stems-1]
        //             [pos+n_stems, K_new) → src_st[pos..K-1]
        //   children: [0, pos] → src_ch[0..pos-1] (note: src_ch[pos] DROPPED)
        //             [pos, pos+n_stems] → repl_children[0..n_stems]
        //             [pos+n_stems+1, K_new] → src_ch[pos+1..K]
        uint32_t* dst_st = inner_stems(dst);
        const uint16_t mk_end = uint16_t(mk_start + n);

        // ── Stems
        for (uint16_t mk = mk_start; mk < mk_end; ++mk) {
            uint32_t v;
            if (mk < pos)            v = src_st[mk];
            else if (mk < pos + n_stems) v = repl_stems[mk - pos];
            else                          v = src_st[mk - n_stems];
            dst_st[mk - mk_start] = v;
        }
        // ── Children: merged children index range [mk_start, mk_end].
        // The mapping for child mc:
        //   mc < pos              → src_ch[mc]
        //   pos <= mc <= pos+n_stems → repl_children[mc - pos]
        //   mc > pos+n_stems      → src_ch[mc - n_stems]
        for (uint16_t mc = mk_start; mc <= mk_end; ++mc) {
            ptr_address a;
            if (mc < pos) {
                std::memcpy(&a, src_ch + size_t(mc) * kInnerChildBytes, sizeof(a));
            } else if (mc <= pos + n_stems) {
                common_header* rc = repl_children[mc - pos];
                a = rc ? rc->address() : kNoChild;
            } else {
                std::memcpy(&a, src_ch + size_t(mc - n_stems) * kInnerChildBytes, sizeof(a));
            }
            inner_set_child_addr(dst, uint16_t(mc - mk_start), a);
        }
        dst->num_keys = n;
    }

    // ─── choose_safe_split_k_: avoid splitting through a delta=1 pair ──
    //
    // A split at position k makes stems[k] the up_div (handed to parent)
    // and gives L the last child[k].  If stems[k]-stems[k-1]==1, child[k]
    // is depth-advanced and becomes L's last child (pos==num_keys), where
    // the descent depth-advance check fails (`pos < num_keys`) → the
    // signal is lost.  Same for stems[k+1]-stems[k]==1 putting an
    // orphaned depth-advanced child as R's first child (pos==0).
    //
    // Walk outward from the requested split point to find a position that
    // doesn't touch any delta=1 pair on either side.
    static uint16_t choose_safe_split_k_(const inner_node* in, uint16_t want) noexcept {
        const uint32_t* stems = inner_stems(in);
        const uint16_t K = in->num_keys;
        auto unsafe = [&](uint16_t k) -> bool {
            if (k == 0 || k >= K) return true;
            if (stems[k] - stems[k - 1] == 1u) return true;
            if (k + 1 < K && stems[k + 1] - stems[k] == 1u) return true;
            return false;
        };
        if (!unsafe(want)) return want;
        for (uint16_t d = 1; d < K; ++d) {
            if (want > d && !unsafe(uint16_t(want - d))) return uint16_t(want - d);
            if (uint16_t(want + d) < K && !unsafe(uint16_t(want + d))) return uint16_t(want + d);
        }
        // No safe split exists.  Caller must accept a broken invariant.
        ++psitri::wtree::g_inner_split_unsafe;
        return want;
    }

    // ─── absorb_k1_promote_: insert (X, new_inner) + (X+1, sentinel) ──
    //
    // Handles the in-place / grow / B-tree-split cases atomically for a
    // K=1 promotion absorbed by parent `in` at branch position `pos`.
    // Returns upsert_inplace if the page absorbed the 2 new stems (with
    // possible grow), or upsert_split if a B-tree split was required.
    upsert_result absorb_k1_promote_(inner_node* in, uint16_t pos,
                                       uint32_t X, common_header* new_inner,
                                       uint32_t Xp1) noexcept {
        const uint16_t vl = value_len_;
        // Try the simple path first: kill old leaf, insert both stems.
        inner_set_child(in, pos, nullptr);
        inner_node* g1 = inner_insert_after_split(in, pos, X, new_inner);
        if (g1) {
            // First insert succeeded (possibly after grow).  Try second.
            inner_node* g2 = inner_insert_after_split(g1, uint16_t(pos + 1), Xp1, nullptr);
            if (g2) {
                inner_enforce_invariant_at(g2, pos,               vl);
                inner_enforce_invariant_at(g2, uint16_t(pos + 1), vl);
                inner_enforce_invariant_at(g2, uint16_t(pos + 2), vl);
                return upsert_inplace(g2);
            }
            // First insert grew or stayed; second hit max cap.  B-tree
            // split g1 and place X+1 in the appropriate half.  X is
            // already placed (at pos in g1).
            ++psitri::wtree::g_absorb_k1_second_split;
            // CRITICAL: must split g1 such that X (at pos) and X+1 stay
            // in the SAME half.  If split_k == pos, X becomes parent's
            // up_div and X+1 lands alone in R → delta=1 invariant lost.
            return split_then_insert_one_keep_pair_(g1, uint16_t(pos + 1),
                                                       Xp1, nullptr, pos);
        }
        ++psitri::wtree::g_absorb_k1_first_split;
        // First insert hit max cap with no room.  B-tree split in first,
        // then absorb both X and X+1 into the appropriate half.
        // Note: we already set child[pos]=null above (sentinel slot for
        // [old_stems[pos-1], X) range; the K=1 leaf was already freed by
        // the promote).  That nullification survives the split.
        ++g_inner_btree_split;
        const uint16_t cur_cls = in->cap_in_cls;
        inner_node* L = inner_alloc_sized(cur_cls, in->address());
        inner_node* R = inner_alloc_sized(cur_cls, L->address());
        const uint16_t split_k = choose_safe_split_k_(in, uint16_t(in->num_keys >> 1));
        uint32_t up_div = 0;
        inner_split_at(in, L, R, &up_div, split_k);
        // Map old `pos` to (target half, position in target).
        // L holds stems[0..split_k-1] / children[0..split_k]; so:
        //   pos in [0..split_k]    → target=L, tpos=pos
        //   pos in [split_k+1..K]  → target=R, tpos=pos - split_k - 1
        inner_node* target = (pos <= split_k) ? L : R;
        uint16_t   tpos    = (pos <= split_k) ? pos : uint16_t(pos - split_k - 1);
        inner_node* t1 = inner_insert_after_split(target, tpos, X, new_inner);
        if (!t1) {
            // Shouldn't happen: target is half-size, started fresh.
            assert(false && "split-half full after K=1 promote first insert");
            t1 = target;
        }
        inner_node* t2 = inner_insert_after_split(t1, uint16_t(tpos + 1), Xp1, nullptr);
        if (!t2) {
            assert(false && "split-half full after K=1 promote second insert");
            t2 = t1;
        }
        if (target == L) L = t2; else R = t2;
        inner_enforce_invariant_at(t2, tpos,               vl);
        inner_enforce_invariant_at(t2, uint16_t(tpos + 1), vl);
        inner_enforce_invariant_at(t2, uint16_t(tpos + 2), vl);
        inner_free_moved(in);
        return upsert_split(L, up_div, R);
    }
    // Variant that ALSO preserves the X-at-keep_pos / X+1-at-pos delta=1 pair.
    upsert_result split_then_insert_one_keep_pair_(inner_node* in, uint16_t pos,
                                                      uint32_t divider, common_header* sib,
                                                      uint16_t keep_pos) noexcept {
        const uint16_t vl = value_len_;
        ++g_inner_btree_split;
        const uint16_t cur_cls = in->cap_in_cls;
        inner_node* L = inner_alloc_sized(cur_cls, in->address());
        inner_node* R = inner_alloc_sized(cur_cls, L->address());
        const uint16_t split_k = choose_safe_split_k_keep_pair_(
            in, uint16_t(in->num_keys >> 1), keep_pos);
        uint32_t up_div = 0;
        inner_split_at(in, L, R, &up_div, split_k);
        inner_node* target = (pos <= split_k) ? L : R;
        uint16_t   tpos    = (pos <= split_k) ? pos : uint16_t(pos - split_k - 1);
        inner_node* t = inner_insert_after_split(target, tpos, divider, sib);
        assert(t);
        if (target == L) L = t; else R = t;
        inner_enforce_invariant_at(t, tpos,               vl);
        inner_enforce_invariant_at(t, uint16_t(tpos + 1), vl);
        inner_free_moved(in);
        return upsert_split(L, up_div, R);
    }
    // Helper for the rare "first K=1-promote stem fit, second needed split".
    upsert_result split_then_insert_one_(inner_node* in, uint16_t pos,
                                           uint32_t divider, common_header* sib) noexcept {
        const uint16_t vl = value_len_;
        ++g_inner_btree_split;
        const uint16_t cur_cls = in->cap_in_cls;
        inner_node* L = inner_alloc_sized(cur_cls, in->address());
        inner_node* R = inner_alloc_sized(cur_cls, L->address());
        const uint16_t split_k = choose_safe_split_k_(in, uint16_t(in->num_keys >> 1));
        uint32_t up_div = 0;
        inner_split_at(in, L, R, &up_div, split_k);
        inner_node* target = (pos <= split_k) ? L : R;
        uint16_t   tpos    = (pos <= split_k) ? pos : uint16_t(pos - split_k - 1);
        inner_node* t = inner_insert_after_split(target, tpos, divider, sib);
        assert(t);
        if (target == L) L = t; else R = t;
        inner_enforce_invariant_at(t, tpos,               vl);
        inner_enforce_invariant_at(t, uint16_t(tpos + 1), vl);
        inner_free_moved(in);
        return upsert_split(L, up_div, R);
    }

    // ─── remove: dispatch + recursive ─────────────────────────────
    enum class rm : uint8_t { not_found, removed, removed_empty };

    // A leaf with fewer than this many live heap bytes is a merge candidate.
    static constexpr uint32_t kLeafMergeThreshold = 1024;

    static bool inner_all_sentinel_(const inner_node* in) noexcept {
        const ptr_address* c   = reinterpret_cast<const ptr_address*>(inner_children_base(in));
        const ptr_address* end = c + (in->num_keys + 1);
        for (; c < end; ++c)
            if (*c != kNoChild) return false;
        return true;
    }

    // Merge child[a_pos] and child[a_pos+1] of `in` (both must be leaves) into
    // one, dropping divider stems[a_pos]. COWs `in` on success. Returns true if
    // merged. Skips when either sibling isn't a plain leaf or the combined live
    // data won't fit one page.
    bool attempt_merge_pair_(inner_node*& in, uint16_t a_pos) noexcept {
        if (a_pos >= in->num_keys) return false;                 // need a right sibling
        common_header* ca = inner_child(in, a_pos);
        common_header* cb = inner_child(in, uint16_t(a_pos + 1));
        if (!ca || !cb) return false;
        if (ca->type() != node_type::wtree_leaf || cb->type() != node_type::wtree_leaf)
            return false;
        leaf_node* A = as_leaf(ca);
        leaf_node* B = as_leaf(cb);
        leaf_node* M = leaf_alloc(A->address());
        if (!leaf_merge_(M, A, B, value_len_)) { leaf_free(M); return false; }
        const ptr_address aa = A->address(), ba = B->address();
        in = cow_inner(in);
        inner_set_child(in, a_pos, as_header(M));
        inner_remove_entry(in, a_pos);                           // drop stems[a_pos] + child[a_pos+1]
        (void)inner_rebuild_corrections(in);
        _session->release(aa);
        _session->release(ba);
        return true;
    }

    // After a removal, if child[pos] is an underfull leaf, try to merge it with
    // its right then left leaf sibling.
    void try_merge_leaf_(inner_node*& in, uint16_t pos) noexcept {
        common_header* c = inner_child(in, pos);
        if (!c || c->type() != node_type::wtree_leaf) return;
        const leaf_node* lf = as_leaf(c);
        if (uint32_t(lf->tail_alloc_pos) - lf->dead_bytes >= kLeafMergeThreshold) return;
        if (attempt_merge_pair_(in, pos)) return;               // (pos, pos+1)
        if (pos > 0) attempt_merge_pair_(in, uint16_t(pos - 1)); // (pos-1, pos)
    }

    // If child[pos] is a single-child B-tree (prefix-0) inner — i.e. its own
    // children merged/collapsed down to one — collapse it: replace it with its
    // only grandchild (which is at the SAME depth, since prefix-0 inners don't
    // advance depth) and free the now-redundant inner. COWs `in`. This is how a
    // shrinking tree folds its spine back down toward a single leaf.
    void try_collapse_inner_(inner_node*& in, uint16_t pos) noexcept {
        common_header* c = inner_child(in, pos);
        if (!c || c->type() != node_type::wtree_inner) return;
        inner_node* ci = as_inner(c);
        if (ci->prefix_len != 0 || ci->num_keys != 0) return;   // not single-child / depth-advanced
        const ptr_address g = inner_child_addr(ci, 0);
        if (g == kNoChild) return;
        in = cow_inner(in);
        inner_set_child_addr(in, pos, g);                       // grandchild takes the slot
        inner_free_moved(cow_inner(ci));                        // free the collapsed inner (neuter keeps g)
    }

    rm node_remove_(common_header* node, word_key_type key, uint16_t depth) noexcept {
        if (node->type() == node_type::wtree_leaf) {
            leaf_node* lf = as_leaf(node);
            // Read-only presence check first (handles value + radix), so a miss
            // (or absent key) never wastes a COW.
            if (leaf_find(lf, key, depth, value_len_).data() == nullptr) return rm::not_found;
            leaf_node* w     = cow_leaf(lf);            // mutate → make writable (addr stable)
            bool       empty = false;
            const bool ok    = leaf_remove(w, key, depth, value_len_, &empty);
            (void)ok; assert(ok);                       // find said present
            return empty ? rm::removed_empty : rm::removed;
        }
        auto*          in  = as_inner(node);
        const uint32_t q   = (depth < key.size()) ? key[depth] : 0u;
        const uint16_t pos = inner_find_branch(in, q);
        common_header* child = inner_child(in, pos);
        if (!child) return rm::not_found;               // sentinel → key absent
        uint16_t cdepth = depth;
        if (child->type() == node_type::wtree_inner) {
            const auto* sub = as_inner(child);
            if (sub->prefix_len > 0) {
                if (q != sub->prefix_data) return rm::not_found;   // routing mismatch → absent
                cdepth = uint16_t(depth + sub->prefix_len);
            }
        }
        const ptr_address child_addr = child->address();
        const rm r = node_remove_(child, key, cdepth);
        if (r == rm::not_found) return rm::not_found;
        if (r == rm::removed_empty) {
            in = cow_inner(in);
            _session->release(child_addr);              // free the emptied child subtree
            inner_set_child(in, pos, nullptr);          // tombstone the slot (kNoChild)
            return inner_all_sentinel_(in) ? rm::removed_empty : rm::removed;
        }
        // r == removed, child non-empty: COW kept its address, so `in`'s slot
        // is still valid. Opportunistically merge an underfull leaf child with
        // a sibling (B-tree merge-on-underflow). Merge COWs `in` (address still
        // stable → grandparent still skips); a non-merge leaves the spine
        // untouched.
        try_merge_leaf_(in, pos);
        try_collapse_inner_(in, pos);   // fold a single-child inner back into its child
        return rm::removed;
    }

    // ─── upsert: dispatch + recursive ─────────────────────────────
    upsert_result node_upsert_(common_header* node,
                                word_key_type key,
                                uint16_t depth,
                                value_type value) noexcept {
        if (node->type() == node_type::wtree_leaf) {
            return leaf_upsert(as_leaf(node), key, depth, value);
        }
        auto* in = as_inner(node);
        const uint32_t q = (depth < key.size()) ? key[depth] : 0u;
        const uint16_t pos = inner_find_branch(in, q);
        common_header* child = inner_child(in, pos);
        // Depth-advance is now decided by the CHILD's self-asserted
        // prefix, not the parent's delta=1 detection.  See node_find_
        // for rationale.  Capture the descended-into child's prefix
        // (when it had one) for use by the wrap-in-container path below:
        // the container we'd build to replace this child must itself
        // claim the same prefix.
        bool     delta1            = false;
        uint8_t  child_prefix_len  = 0;
        uint32_t child_prefix_data = 0;
        if (child && child->type() == node_type::wtree_inner) {
            const auto* sub = as_inner(child);
            if (sub->prefix_len > 0) {
                if (q != sub->prefix_data) {
                    ++g_routing_inconsistency;
                    return { nullptr, nullptr, 0, 0 };
                }
                child_prefix_len  = sub->prefix_len;
                child_prefix_data = sub->prefix_data;
                depth = uint16_t(depth + sub->prefix_len);
                delta1 = true;
            }
        }
        if (!child) {
            // Sentinel slot from a prior K=1 split — empty range, no
            // child yet.  Allocate a fresh leaf here and insert.
            leaf_node* lf = leaf_alloc(in->address());   // near parent inner
            leaf_init(lf);
            const bool ok = leaf_insert(lf, key, depth, value);
            if (!ok) {
                // Fresh leaf insert can fail (heap exhaustion on a huge
                // encoded key).  Treat as drop instead of silently leaking.
                leaf_free(lf);
                return { nullptr, nullptr, 0, 0 };
            }
            in = cow_inner(in);   // about to mutate in place → make writable
            inner_set_child(in, pos, as_header(lf));
            return upsert_inplace(in);
        }
        // Prefetch child's first two CLs BEFORE recursing.
        __builtin_prefetch(child,                                              1, 1);
        __builtin_prefetch(reinterpret_cast<const char*>(child) + kCacheLine,  1, 1);
        const ptr_address child_addr = child->address();   // for the no-change skip below
        upsert_result cr = node_upsert_(child, key, depth, value);
        if (!cr.left) return { nullptr, nullptr, 0, 0 };   // drop bubbles up

        // NOTE: `in` is NOT COW'd up front. In unique mode COW/realloc keep a
        // node's address, so when the child is mutated in place (the common,
        // non-splitting case) `in`'s slot still points at it — `in` is
        // unchanged and needs no mutation, so the whole ancestor chain is left
        // shared (no per-insert COW up the spine). Only the branches that
        // actually change `in` (sentinel fill, container wrap, splice/split)
        // COW it locally; copy_on_write keeps `in`'s address, so the grandparent
        // likewise stays untouched.
        const bool is_promote = (cr.right == kSentinelChild());

        // If we descended into a delta=1 (depth-advanced) slot and the
        // child returned a NORMAL split (or 3-way split with extras),
        // the up_div is a key[depth+1] value but our stems are key[depth]
        // values — DON'T bubble up.  Wrap the bubble into a container
        // (a fresh sub-inner) that stays inside this slot.
        //
        // K=1 promote bubble-up is DIFFERENT: the X / X+1 stems are
        // already at the correct depth level (the leaf's stem, which
        // is a key[depth+1] value matching this depth-advanced inner's
        // stem layout).  Absorbing it normally works fine — adding a
        // delta=1 pair within OUR stems creates the proper nested
        // depth-advance for the next u32.  Wrapping in a container
        // here would add a SPURIOUS extra depth advance.
        if (delta1 && !is_promote && cr.right) {
            // Build the full child/stem sequence from cr (left + right +
            // any tail extras), then construct one container holding
            // all of them.  cr.extra->prepend_child is only emitted by
            // K=1 promote paths, so it's not expected here.
            // Max sequence in the wrap path: 4 prepend (n/a here) + 2
            // primary + 4 tail = 10 children / 9 stems.
            common_header* ch_buf[10];
            uint32_t       st_buf[9];
            uint16_t ci = 0, si = 0;
            ch_buf[ci++] = cr.left;
            st_buf[si++] = cr.divider;
            ch_buf[ci++] = cr.right;
            if (cr.extra) {
                assert(cr.extra->n_prepend == 0);   // prepend only with is_promote
                common_header* const* tc = cr.extra->tail_children;
                const uint32_t* td = cr.extra->tail_dividers;
                common_header* const* tc_end = tc + cr.extra->n_tail;
                while (tc < tc_end) {
                    st_buf[si++] = *td++;
                    ch_buf[ci++] = *tc++;
                }
                delete cr.extra;
            }
            inner_node* container = inner_alloc_sized(kInnerInitialCapInCls, in->address());
            inner_init(container, kInnerInitialCapInCls);
            uint32_t* cs = inner_stems(container);
            for (uint16_t i = 0; i < si; ++i) cs[i] = st_buf[i];
            for (uint16_t i = 0; i <= si; ++i) inner_set_child(container, i, ch_buf[i]);
            container->num_keys = si;
            // Self-asserted prefix: this container replaces the
            // depth-advanced sub_inner that previously occupied
            // child[pos].  Inherit its prefix verbatim.
            container->prefix_len  = child_prefix_len;
            container->prefix_data = child_prefix_data;
            (void)inner_rebuild_corrections(container);
            in = cow_inner(in);   // repointing child[pos] → make `in` writable
            inner_set_child(in, pos, as_header(container));
            return upsert_inplace(in);
        }

        if (is_promote) {
            // K=1 promotion at `pos` produces a single slot-replacement:
            // child[pos] is removed and replaced by a sorted sequence of
            // (n_stems + 1) children and n_stems stems.  Layout (PRE:
            // child[pos]=leaf-with-K=1-stem-X, A=stems[pos-1], B=stems[pos]):
            //
            //   no prepend, no tail (n_stems=2):
            //     children = [sentinel-[A,X), new_inner-[X,X+1), sentinel-[X+1,B)]
            //     stems    = [X, X+1]
            //
            //   with prepend (target=R case from leaf_upsert):
            //     children = [prepend_child, sentinel-[prepend_div,X), new_inner,
            //                  sentinel-[X+1,B)]
            //     stems    = [prepend_div, X, X+1]
            //
            //   with tail (target=L case):
            //     children = [sentinel-[A,X), new_inner, sentinel-[X+1,tail_div_0),
            //                  tail_child_0, sentinel-[tail_div_0+?,...) ... ]
            //     stems    = [X, X+1, tail_div_0, tail_div_1, ...]
            //
            // The whole sequence becomes one replace_slot_ call which
            // computes total size, grows in-place if it fits, B-tree
            // splits ONCE if it doesn't.  No intermediate split state.
            const uint8_t n_tail    = cr.extra ? cr.extra->n_tail    : 0;
            const uint8_t n_prepend = cr.extra ? cr.extra->n_prepend : 0;
            const uint16_t n_stems  = uint16_t(2 + n_prepend + n_tail);

            // Build the merged children + stems arrays on the stack.
            // Bound: 2 (K=1) + 4 (max prepend) + 4 (max tail) + 1 = 11
            // children / 10 stems.
            common_header* ch_buf[11];
            uint32_t       st_buf[10];
            common_header** ch_p = ch_buf;
            uint32_t*       st_p = st_buf;
            if (cr.extra && n_prepend > 0) {
                common_header* const* pc = cr.extra->prepend_children;
                const uint32_t*       pd = cr.extra->prepend_dividers;
                common_header* const* pc_end = pc + n_prepend;
                while (pc < pc_end) {
                    *ch_p++ = *pc++;
                    *st_p++ = *pd++;
                }
            }
            // K=1 promote primary: sentinel-[…,X), new_inner-[X,X+1),
            //                       sentinel-[X+1,…)
            *ch_p++ = nullptr;
            *st_p++ = cr.prev_divider;     // X
            *ch_p++ = cr.left;             // new_inner [X, X+1)
            *st_p++ = cr.divider;          // X+1
            *ch_p++ = nullptr;             // sentinel-[X+1, …)
            if (cr.extra && n_tail > 0) {
                common_header* const* tc = cr.extra->tail_children;
                const uint32_t*       td = cr.extra->tail_dividers;
                common_header* const* tc_end = tc + n_tail;
                while (tc < tc_end) {
                    *st_p++ = *td++;
                    *ch_p++ = *tc++;
                }
            }
            assert(size_t(st_p - st_buf) == n_stems);
            assert(size_t(ch_p - ch_buf) == n_stems + 1u);

            delete cr.extra;
            const upsert_result r = replace_slot_(in, pos, n_stems, st_buf, ch_buf);
            // Enforce delta=1 invariants at every position the operation
            // touched: pos, pos+1, ..., pos+n_stems (n_stems+1 positions).
            // The X↔X+1 pair is intentionally delta=1 and the enforcer
            // upgrades the leaf at X+1's slot into an inner where needed.
            // For the split case (r.right != null), the new pieces ended
            // up in either L or R; route enforcement to whichever half
            // each touched position landed in.
            const uint16_t vl = value_len_;
            if (!r.right) {
                inner_node* g = as_inner(r.left);
                for (uint16_t i = 0; i <= n_stems; ++i) {
                    inner_enforce_invariant_at(g, uint16_t(pos + i), vl);
                }
            } else {
                // After the split, merged index `pos + i` lives in:
                //   L if (pos + i) <= split_k_recorded
                //   R if (pos + i) > split_k_recorded
                // replace_slot_ doesn't expose split_k explicitly, but
                // the divider comparison gives the same answer.
                inner_node* L = as_inner(r.left);
                inner_node* R = as_inner(r.right);
                const uint16_t L_n = L->num_keys;
                for (uint16_t i = 0; i <= n_stems; ++i) {
                    const uint16_t mk = uint16_t(pos + i);
                    if (mk < L_n) {
                        inner_enforce_invariant_at(L, mk, vl);
                    } else if (mk == L_n) {
                        // mk landed on the parent's up_div — no in-half
                        // enforcement applies (the up_div will be
                        // checked when the parent absorbs the split).
                    } else {
                        inner_enforce_invariant_at(R, uint16_t(mk - L_n - 1), vl);
                    }
                }
            }
            return r;
        }

        // Normal split / 3-way split / 4-way split (any non-promote
        // result).  Route everything through replace_slot_ so the same
        // primitive handles inplace / grow / split uniformly.
        if (!cr.right && !cr.extra) {
            // Pure inplace at child — left replaced the slot, no new structure.
            // In unique mode the child keeps its address, so `in`'s slot is
            // already correct: skip the COW + write entirely (the win). Only if
            // the child actually relocated do we COW `in` and repoint the slot.
            if (cr.left->address() != child_addr) [[unlikely]] {
                in = cow_inner(in);
                inner_set_child(in, pos, cr.left);
            }
            return upsert_inplace(in);
        }
        // Max sequence: 4 prepend + left + right + 4 tail = 10 children,
        // 9 stems.
        common_header* ch_buf[10];
        uint32_t       st_buf[9];
        uint16_t ci = 0, si = 0;
        ch_buf[ci++] = cr.left;
        if (cr.right) {
            st_buf[si++] = cr.divider;
            ch_buf[ci++] = cr.right;
        }
        if (cr.extra) {
            assert(cr.extra->n_prepend == 0);   // prepend only with is_promote
            common_header* const* tc = cr.extra->tail_children;
            const uint32_t*       td = cr.extra->tail_dividers;
            common_header* const* tc_end = tc + cr.extra->n_tail;
            while (tc < tc_end) {
                st_buf[si++] = *td++;
                ch_buf[ci++] = *tc++;
            }
            delete cr.extra;
        }
        const uint16_t n_stems = si;
        const upsert_result r = replace_slot_(in, pos, n_stems, st_buf, ch_buf);
        // Enforce delta=1 invariant at every touched position.
        const uint16_t vl = value_len_;
        if (!r.right) {
            inner_node* g = as_inner(r.left);
            for (uint16_t i = 0; i <= n_stems; ++i) {
                inner_enforce_invariant_at(g, uint16_t(pos + i), vl);
            }
        } else {
            inner_node* Linner = as_inner(r.left);
            inner_node* Rinner = as_inner(r.right);
            const uint16_t L_n = Linner->num_keys;
            for (uint16_t i = 0; i <= n_stems; ++i) {
                const uint16_t mk = uint16_t(pos + i);
                if (mk < L_n)        inner_enforce_invariant_at(Linner, mk, vl);
                else if (mk == L_n)  { /* parent up_div; not in either half */ }
                else                 inner_enforce_invariant_at(Rinner, uint16_t(mk - L_n - 1), vl);
            }
        }
        return r;
    }
};

}  // namespace psitri::wtree
