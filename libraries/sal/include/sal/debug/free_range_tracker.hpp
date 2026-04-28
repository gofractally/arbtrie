#pragma once
//
// Debug-only tracker for the global SAL byte address space.
//
// All segments are mapped contiguously (one virtual block, max_segment_count *
// segment_size). Every byte has a unique global offset.
//
// State machine, per-byte:
//   uninit  → live   (mark_alloc — bump allocation, sync_header write, etc.)
//   live    → uninit (mark_free — release/destroy, sync_header accounting)
//   uninit  → live   (mark_alloc — recycled bytes after segment reuse)
//
// Invariants enforced (any violation aborts the process with both tags):
//   * mark_alloc:  the range must not currently overlap any live range.
//   * mark_free:   the range must exactly equal a current live range.
//   * mark_unalloc: same as mark_free (used for compactor rollback paths
//                   where we simply un-record without claiming "freed").
//
// Naturally catches:
//   * double-free        — second mark_free after first removes
//   * free-of-uninit     — mark_free with no matching live entry
//   * partial free       — mark_free with size != live entry size
//   * alloc-into-live    — mark_alloc overlapping a live range
//
// Re-allocation after a free is fine: the free removed the entry, so the
// follow-up alloc finds no overlap.
//
// Segment reuse calls erase_segment_range to drop any stragglers that
// weren't properly freed (and warn). Should normally be a no-op.
//
// Enabled only when -DENABLE_DEEP_INVARIANTS=ON (PSITRI_DEEP_INVARIANTS=1).
// In release builds all entry points compile to no-ops. Single global mutex.

#include <sal/config.hpp>

#if PSITRI_DEEP_INVARIANTS

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>

#include <sal/debug.hpp>

namespace sal::debug {

class free_range_tracker {
 public:
    static free_range_tracker& instance() {
        static free_range_tracker t;
        return t;
    }

    // Expose the mutex so callers can bracket
    //   {meta-counter update + mark_alloc/mark_free + verifier read}
    // as one atomic critical section. Recursive so the inner tracker
    // methods can re-acquire safely.
    std::recursive_mutex& mutex() { return mu_; }

    void set_base(const void* base) {
        (void)base;
        std::lock_guard l(mu_);
        active_ = true;
        SAL_WARN("free_range_tracker: activated (raw-pointer mode)");
    }

    // Allocator installs a verifier — called after every mark_alloc /
    // mark_free with (when_tag, op_tag, obj, size). The verifier looks up
    // the segment containing `obj` and checks the invariant
    //     alloc_pos == freed_space + live_in_seg
    // The first divergence is fatal — pinpoints the offending op.
    using verifier_fn = std::function<void(const char* when, const char* op_tag,
                                           const void* obj, uint32_t size)>;
    void set_verifier(verifier_fn f) {
        std::lock_guard l(mu_);
        verifier_ = std::move(f);
    }

    // Range is being allocated (claims live ownership of these bytes).
    // Aborts if any overlap with current live ranges.
    void mark_alloc(const void* obj, uint32_t size, const char* tag) {
        if (!active_) return;
        alloc_count_.fetch_add(1, std::memory_order_relaxed);
        uintptr_t off = reinterpret_cast<uintptr_t>(obj);
        uintptr_t end = off + size;
        verifier_fn v_copy;
        {
            std::lock_guard l(mu_);
            check_no_overlap("ALLOC-OVER-LIVE", off, end, tag);
            live_.emplace_hint(live_.lower_bound(off), off,
                               range_info{end, tag});
            v_copy = verifier_;
        }
        if (v_copy) v_copy("after_mark_alloc", tag, obj, size);
    }

    // Range is being freed. Must exactly match a current live entry.
    void mark_free(const void* obj, uint32_t size, const char* tag) {
        if (!active_) return;
        free_count_.fetch_add(1, std::memory_order_relaxed);
        uintptr_t off = reinterpret_cast<uintptr_t>(obj);
        uintptr_t end = off + size;
        verifier_fn v_copy;
        {
            std::lock_guard l(mu_);
            auto it = live_.find(off);
            if (it == live_.end()) {
                for (auto& kv : live_) {
                    if (kv.first < end && kv.second.end > off) {
                        SAL_ERROR("FREE-MISALIGNED: free=[{:#x},{:#x}) tag={} overlaps "
                                  "live=[{:#x},{:#x}) prior_tag={}",
                                  off, end, tag, kv.first, kv.second.end,
                                  kv.second.tag);
                        std::abort();
                    }
                }
                SAL_ERROR("FREE-OF-UNINIT: free=[{:#x},{:#x}) tag={} no matching live range",
                          off, end, tag);
                std::abort();
            }
            if (it->second.end != end) {
                SAL_ERROR("FREE-PARTIAL: free=[{:#x},{:#x}) tag={} live=[{:#x},{:#x}) prior_tag={}",
                          off, end, tag, it->first, it->second.end,
                          it->second.tag);
                std::abort();
            }
            live_.erase(it);
            v_copy = verifier_;
        }
        if (v_copy) v_copy("after_mark_free", tag, obj, size);
    }

    // Diagnostic: verify that a live range starting at `obj` has exactly
    // `expected_size`. Called BEFORE `add_freed_space` so we can localize
    // free-time/alloc-time size disagreements (e.g. alloc_header.size()
    // mutated in place between alloc and free) — these would otherwise
    // surface as a generic FREE-PARTIAL inside mark_free with no upstream
    // context.
    void verify_alloc_size(const void* obj, uint32_t expected_size,
                           const char* tag) const {
        if (!active_) return;
        uintptr_t off = reinterpret_cast<uintptr_t>(obj);
        std::lock_guard l(mu_);
        auto it = live_.find(off);
        if (it == live_.end()) {
            // Try to find an enclosing or overlapping live range for context.
            auto up = live_.upper_bound(off);
            const range_info* enclosing = nullptr;
            uintptr_t enclosing_start = 0;
            if (up != live_.begin()) {
                auto p = std::prev(up);
                if (p->second.end > off) {
                    enclosing = &p->second;
                    enclosing_start = p->first;
                }
            }
            if (enclosing) {
                SAL_ERROR("VERIFY-SIZE: free obj=[{:#x},{:#x}) tag={} not a "
                          "live-range start; enclosed by live=[{:#x},{:#x}) "
                          "prior_tag={}",
                          off, off + expected_size, tag, enclosing_start,
                          enclosing->end, enclosing->tag);
            } else {
                SAL_ERROR("VERIFY-SIZE: free obj=[{:#x},{:#x}) tag={} no live "
                          "range starts here (no enclosing range either)",
                          off, off + expected_size, tag);
            }
            std::abort();
        }
        uint32_t live_size = uint32_t(it->second.end - it->first);
        if (live_size != expected_size) {
            SAL_ERROR("VERIFY-SIZE MISMATCH: obj={:#x} free_size={} "
                      "tracked_size={} live_end={:#x} prior_tag={} cur_tag={}",
                      off, expected_size, live_size, it->second.end,
                      it->second.tag, tag);
            std::abort();
        }
    }

    void erase_segment_range(const void* seg_base, uint32_t seg_size) {
        if (!active_) return;
        uintptr_t lo = reinterpret_cast<uintptr_t>(seg_base);
        uintptr_t hi = lo + seg_size;
        std::lock_guard l(mu_);
        auto it = live_.lower_bound(lo);
        while (it != live_.end() && it->first < hi) {
            SAL_WARN("SEGMENT-RESET dropped live=[{:#x},{:#x}) tag={}",
                     it->first, it->second.end, it->second.tag);
            it = live_.erase(it);
        }
    }

    size_t live_count() const {
        std::lock_guard l(mu_);
        return live_.size();
    }

    // Diagnostic: total bytes currently tracked as live (sum of all live
    // ranges). If this disagrees with the segment_meta::freed_space view of
    // "how much has been freed," something is bypassing the tracker.
    uint64_t total_live_bytes() const {
        std::lock_guard l(mu_);
        uint64_t n = 0;
        for (auto& kv : live_) n += kv.second.end - kv.first;
        return n;
    }

    // Sum of live ranges that intersect [lo, hi). Each live range is
    // either entirely inside or entirely outside the half-open range —
    // since allocations can't span segments, we just check containment
    // by start.
    uint64_t live_bytes_in_range(const void* lo, const void* hi) const {
        if (!active_) return 0;
        uintptr_t a = reinterpret_cast<uintptr_t>(lo);
        uintptr_t b = reinterpret_cast<uintptr_t>(hi);
        std::lock_guard l(mu_);
        uint64_t n = 0;
        auto it = live_.lower_bound(a);
        for (; it != live_.end() && it->first < b; ++it)
            n += it->second.end - it->first;
        return n;
    }

    uint64_t alloc_count() const { return alloc_count_.load(std::memory_order_relaxed); }
    uint64_t free_count() const  { return free_count_.load(std::memory_order_relaxed); }

 private:
    free_range_tracker() = default;

    void check_no_overlap(const char* what, uintptr_t off, uintptr_t end,
                          const char* tag) {
        // Caller holds mu_.
        auto it = live_.lower_bound(off);
        if (it != live_.begin()) {
            auto p = std::prev(it);
            if (p->second.end > off) {
                SAL_ERROR("{}: new=[{:#x},{:#x}) tag={} overlaps existing=[{:#x},{:#x}) prior_tag={}",
                          what, off, end, tag, p->first, p->second.end,
                          p->second.tag);
                std::abort();
            }
        }
        if (it != live_.end() && it->first < end) {
            SAL_ERROR("{}: new=[{:#x},{:#x}) tag={} overlaps existing=[{:#x},{:#x}) prior_tag={}",
                      what, off, end, tag, it->first, it->second.end,
                      it->second.tag);
            std::abort();
        }
    }

    struct range_info {
        uintptr_t   end;
        const char* tag;
    };

    bool                              active_ = false;
    // recursive_mutex so callers can hold the lock around the
    //   {alloc_pos / freed_space update + mark_alloc/mark_free}
    // critical section AND have the inner tracker methods re-acquire it.
    // Without this, the verifier observes torn state on concurrent ops.
    mutable std::recursive_mutex      mu_;
    std::map<uintptr_t, range_info>   live_;
    std::atomic<uint64_t>             alloc_count_{0};
    std::atomic<uint64_t>             free_count_{0};
    verifier_fn                       verifier_;
};

#define SAL_TRACK_ALLOC(obj, sz, tag) \
    ::sal::debug::free_range_tracker::instance().mark_alloc(obj, sz, tag)
#define SAL_TRACK_FREE(obj, sz, tag) \
    ::sal::debug::free_range_tracker::instance().mark_free(obj, sz, tag)
#define SAL_TRACK_SEG_RESET(seg, sz) \
    ::sal::debug::free_range_tracker::instance().erase_segment_range(seg, sz)
#define SAL_TRACK_VERIFY_SIZE(obj, sz, tag) \
    ::sal::debug::free_range_tracker::instance().verify_alloc_size(obj, sz, tag)
// Bracket a critical section so {alloc_pos/freed_space update,
// mark_alloc/mark_free, verifier read} are observed atomically.
#define SAL_TRACK_LOCK() \
    std::lock_guard<std::recursive_mutex> _sal_track_lk( \
        ::sal::debug::free_range_tracker::instance().mutex())
#define SAL_TRACK_SET_BASE(base) \
    ::sal::debug::free_range_tracker::instance().set_base(base)

}  // namespace sal::debug

#else  // !PSITRI_DEEP_INVARIANTS

#define SAL_TRACK_ALLOC(obj, sz, tag)  ((void)0)
#define SAL_TRACK_FREE(obj, sz, tag)   ((void)0)
#define SAL_TRACK_SEG_RESET(seg, sz)   ((void)0)
#define SAL_TRACK_SET_BASE(base)       ((void)0)
#define SAL_TRACK_VERIFY_SIZE(obj, sz, tag) ((void)0)
#define SAL_TRACK_LOCK()               ((void)0)

#endif
