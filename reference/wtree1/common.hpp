#pragma once
// wtree/common.hpp — shared types/constants for the wtree node family.
//
// Stage 2 (SAL): wtree nodes are SAL-allocated. Their 12-byte header IS a
// sal::alloc_header (via psitri::node), children are 4-byte ptr_addresses, and
// allocation goes through an allocator_session.
#include <cstddef>
#include <cstdint>
#include <utility>

#include <psitri/node/node.hpp>                 // psitri::node, node_type, ptr_address
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <sal/read_lock.hpp>
#include <sal/smart_ptr.hpp>
#include <ucc/fast_memcpy.hpp>                  // memcpy_aligned_64byte (NEON/AVX512)
#include <ucc/round.hpp>                        // round_up/down_multiple<64>

namespace psitri::wtree {

// Apple Silicon / x86 hardware fetch unit. Node layouts are sized in these.
inline constexpr std::size_t kCacheLine = 128;

// ── SAL type bridge ────────────────────────────────────────────────
using node            = psitri::node;          // 12-byte SAL header base
using node_type       = psitri::node_type;
using ptr_address     = sal::ptr_address;
using ptr_address_seq = sal::ptr_address_seq;
using alloc_hint      = sal::alloc_hint;
using session_t       = sal::allocator_session;

// ── Child / upsert-result sentinels (must not collide with a real addr) ─
// SAL hands out addresses from 0 upward; null is 0xFFFFFFFF.
//   kNoChild           — empty slot stored in a node, and "no split" in
//                        upsert_result.right  (was: nullptr).
//   kSentinelChildAddr — "promote: insert an empty slot at pos+1" marker in
//                        upsert_result.right (was: kSentinelChild()==addr 1).
//                        0xFFFFFFFE is unreachable in practice.
inline constexpr ptr_address kNoChild           = sal::null_ptr_address;
inline constexpr ptr_address kSentinelChildAddr = ptr_address(0xFFFFFFFEu);

// Which copy a mutation plan selected (cached in the Op). fast = active
// 64-aligned head/tail copy (carries the free gap; in-place capable).
// rebuild = walk live structure and drop dead space (stale MVCC entries,
// freed slots) — needs a fresh buffer.
enum class copy_mode : uint8_t { fast = 0, rebuild = 1 };

// Splice target: editing the live buffer in place (dst == src) vs. building
// into a fresh buffer (dst != src, possibly grown). Templated on this so each
// path is branch-free — no runtime src==dst test on the hot path.
enum class splice_mode : uint8_t { in_place = 0, fresh_copy = 1 };

// ── Per-thread active session (unique-mode Stage 2) ─────────────────
// The wtree free-functions (leaf_alloc/inner_alloc_sized/leaf_free/
// inner_free/inner_child) allocate, free, and deref children. Rather than
// thread a session through every signature (which would force rewriting the
// hot algorithm), the tree installs its session here at the top of each
// public op; the free-functions read it. Single-writer / single-thread for
// now. MVCC (Stage 3) will revisit.
inline thread_local session_t* g_wtree_session = nullptr;
inline session_t& wsession() noexcept { return *g_wtree_session; }

// Deref a child ptr_address to a node pointer (unique mode → the mmap pointer
// is stable). Uses get_ptr — the read-path deref: it prefetches the node and
// feeds the read-cache (hot nodes get promoted/grouped into the pinned tier,
// cutting TLB/page misses) but builds NO smart_ref and does NOT retain the
// session (no modify()/release() to back on a read → that overhead would be
// waste). Mutating sites already COW via get_ref().modify() separately.
template <typename T>
inline T* wderef(ptr_address a) noexcept
{
   return wsession().template get_ptr<T>(a);
}

}  // namespace psitri::wtree
