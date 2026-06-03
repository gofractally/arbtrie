#pragma once
// wtree/apply.hpp — first-class node-mutation primitive.
//
// One function unifies the in-place / copy-on-write / grow / MVCC-fork
// dispatch that would otherwise be hand-copied at every mutation site.
// It's a wtree UTILITY (policy lives here), built only on SAL primitives
// (smart_ref::will_cow/modify, session::realloc/alloc) + the node contract
// below — keeping sal::smart_ref a clean primitive.
//
// ── The node/Op contract (a (T,Op) pair is "applyable" if it provides) ──
//
//   static uint32_t T::alloc_size(const T& src, Op& op);
//        Plans the mutation INTO `op` (chosen copy strategy op.copy, layout,
//        split-k, …) and returns the resulting allocation size, or 0 if the
//        op cannot be applied (caller must split). `Op&` is a stack work-
//        ticket: this caches the expensive layout once; the ctor/apply read
//        it back (and may write outputs the caller consumes). A second call
//        with an already-planned op is cheap.
//
//   T(uint32_t asize, ptr_address seq-via-allocator, const T& src, Op& op);
//        Remake ctor: build the result from `src` + `op` in ONE pass
//        (copy_fast or copy_rebuild per op.copy, with the edit spliced in).
//        Used by the copy-on-write / grow / fork paths.
//
//   void T::apply(Op& op);
//        In-place edit (same buffer). Used when the node is unique, writable,
//        and the op fits without a rebuild.
//
// (ctor and apply should share one `splice_*` primitive so the edit logic
//  isn't written twice.)

#include "common.hpp"
#include <psitri/upsert_mode.hpp>

namespace psitri::wtree {

// copy_mode lives in common.hpp (shared by nodes + apply).

// apply<mode>(ref, op): mutate the node behind `ref` by `op`, returning the
// resulting node's address (== ref.address() when kept in place / relocated
// same-id; a fresh address on an MVCC fork). Returns null_ptr_address if the
// op doesn't fit (alloc_size==0) — the caller then performs a split.
//
//   unique : keep identity — in-place if (writable & fast & fits), else
//            realloc (same id; fast or rebuild per op.copy / grow).
//   shared : fresh identity — alloc a new object from (src, op); the old
//            stays live for other snapshots (MVCC fork). [also the home for
//            future prune-on-fork strategies.]
template <upsert_mode mode, typename T, typename Op>
inline ptr_address apply(sal::smart_ref<T>& ref, Op& op, alloc_hint hint = {}) noexcept
{
    const uint32_t sz = T::alloc_size(*ref.obj(), op);   // plan once (fills op)
    if (sz == 0) [[unlikely]]
        return kNoChild;                                 // doesn't fit → caller splits

    if constexpr (mode.is_unique())
    {
        const bool inplace = !ref.modify_will_copy()
                          && op.copy == copy_mode::fast
                          && sz <= ref.obj()->size();
        if (inplace) [[likely]]
        {
            ref.modify([&](T* n) { n->apply(op); });     // in-place, same id
            return ref.address();
        }
        // relocate same id (copy-on-write or grow); fast|rebuild per op.copy
        return ref.session()->template realloc<T>(ref, sz, *ref.obj(), op).address();
    }
    else  // shared (or prune) → fresh id
    {
        return ref.session()->template alloc<T>(sz, hint, *ref.obj(), op);
    }
}

}  // namespace psitri::wtree
