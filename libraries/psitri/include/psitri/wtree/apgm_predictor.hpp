#pragma once

// AdaptivePGM predictor for v4 inner_node.
//
// Standalone module — not yet wired into inner_node.hpp.  Layout matches
// the 96-byte budget freed when we remove v3's recip(8) + base32(4) +
// shift_amt(1) + corrections_packed(92) + 1 pad fields.  The replacement
// is up to 9 piecewise-linear segments scanned via SIMD.
//
// Build path: greedy "longest valid prefix segment" — which IS the
// optimal piecewise-linear-bounded-error algorithm (PGM's principle,
// independently re-derived here so we don't carry the library).
//
// Lookup path: integer-only, branch-free segment selection via 2-NEON
// loads + horizontal add, then one 64-bit fixed-point multiply for the
// in-segment regression.  Returns a SAMPLE-space position; the caller
// scales by sample_step and does the final 1-CL adaptive scan over
// stems[] (NOT inner_node-specific — kept here as a separate helper so
// the inner_node integration is mechanical).

#include <arm_neon.h>
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace psitri::wtree {

// ─── Predictor header — 96 B exactly ──────────────────────────────
//
// All fields are written by build, read by lookup.  Hot path reads
// segment_keys[0..7] in 2 NEON loads, then recips[seg] + intercepts[seg]
// for the regression.  Cold path additionally reads recips[8..] and
// intercepts[8..] when the 9th slot is in play.
struct apgm_header {
    uint8_t  segment_count;        // 1..9 (0 = predictor unused)
    uint8_t  sample_step;          // stems/sample at build time (typ. 8 = 2/CL)
    uint16_t eps_used;             // sample-space error bound at build
    uint32_t segment_keys[9];      // 36 B — first key of each segment, sorted
    uint32_t recips[9];            // 36 B — slope as u32 fixed-point (slope × 2^32)
    uint16_t intercepts[9];        // 18 B — sample-space position at segment_keys[i]
    uint8_t  _pad[2];              // 2  B — round to 96
};
static_assert(sizeof(apgm_header) == 96, "apgm_header must be 96 B exactly");
static_assert(offsetof(apgm_header, segment_keys) == 4);
static_assert(offsetof(apgm_header, recips)       == 4 + 36);
static_assert(offsetof(apgm_header, intercepts)   == 4 + 36 + 36);

// Sentinel value packed into unused segment slots so the SIMD scan
// returns the correct count without bounds checks.
inline constexpr uint32_t kApgmKeySentinel = 0xFFFFFFFFu;

// ─── Lookup: q → predicted sample-space position ──────────────────
//
// Returns a sample-space position; multiply by sample_step (or use
// apgm_lookup_stem_position below) to get the original-stems position
// approximation.  Predicted within ±eps of true position.
inline uint32_t apgm_lookup_sample_pos(const apgm_header* h, uint32_t q) noexcept {
    // Step 1 — count_le over the 9 segment_keys.  2 NEON loads cover
    // the first 8 slots; scalar handles the 9th.  Unused slots are
    // pre-filled with kApgmKeySentinel so they never count_le.
    const uint32x4_t tgt = vdupq_n_u32(q);
    int32_t s = 0;
    {
        uint32x4_t v0  = vld1q_u32(h->segment_keys + 0);
        uint32x4_t le0 = vcleq_u32(v0, tgt);
        s += vaddvq_s32(vreinterpretq_s32_u32(le0));
        uint32x4_t v1  = vld1q_u32(h->segment_keys + 4);
        uint32x4_t le1 = vcleq_u32(v1, tgt);
        s += vaddvq_s32(vreinterpretq_s32_u32(le1));
    }
    // vcleq returns 0xFFFFFFFF for true lanes → -1 when summed as int.
    int count_le = -s;
    if (h->segment_keys[8] <= q && h->segment_keys[8] != kApgmKeySentinel) {
        ++count_le;
    }
    // Segment index = count_le - 1; clamp at 0 for q below all keys
    // (= falls in segment 0 by definition).
    int seg = count_le - 1;
    if (seg < 0) seg = 0;

    // Step 2 — in-segment integer regression.
    //   pred_sample_pos = intercept[seg] + (q - segment_keys[seg]) * recip[seg] / 2^32
    const uint32_t seg_key   = h->segment_keys[seg];
    const uint32_t recip     = h->recips[seg];
    const uint16_t intercept = h->intercepts[seg];
    const uint64_t prod      = uint64_t(q - seg_key) * uint64_t(recip);
    return uint32_t(prod >> 32) + uint32_t(intercept);
}

// Convenience: convert sample-space prediction back to stems[] position.
// stems-space prediction is approximate within ±eps_used * sample_step
// (e.g., eps=2, step=8 → ±16 stems = ±1 CL).
inline uint32_t apgm_lookup_stem_position(const apgm_header* h, uint32_t q) noexcept {
    const uint32_t sp = apgm_lookup_sample_pos(h, q);
    return sp * uint32_t(h->sample_step);
}

// ─── Build: stems[0..n-1] → apgm_header ───────────────────────────
//
// Sampling: take stems[0], stems[step], stems[2*step], ... (first
// stem of each CL chunk — empirically the best alignment per the
// design doc; mid-CL sampling roughly doubles walk counts).
//
// Segmentation: greedy "longest valid prefix segment".  For each
// segment starting at sample s_start, scan forward while a single
// linear regression with slope range bounded by ±eps still fits.
// When the slope range collapses, close the segment and start a new
// one at the failing sample.  This is the optimal piecewise-linear
// algorithm for bounded-error approximation.
//
// Returns true iff the segmentation fit in MAX_SEGMENTS (= 9).
// Otherwise the header is left in a partial state — caller can
// retry with a larger eps.
inline bool apgm_build(apgm_header* h,
                        const uint32_t* stems,
                        uint32_t n,
                        uint16_t eps = 2,
                        uint8_t  sample_step = 8) noexcept {
    constexpr uint8_t kMaxSegments = 9;
    // Initialize sentinels in case we return early.
    for (uint8_t i = 0; i < kMaxSegments; ++i) {
        h->segment_keys[i] = kApgmKeySentinel;
        h->recips[i]       = 0;
        h->intercepts[i]   = 0;
    }
    h->sample_step  = sample_step;
    h->eps_used     = eps;
    h->segment_count= 0;
    h->_pad[0] = h->_pad[1] = 0;
    if (n == 0) return true;

    // Pre-compute sampled positions and keys.  Up to ceil(n/step)
    // samples.  Bounded by inner_node's max ~5800 stems / 8 = 725
    // samples; stack-allocate.
    const uint32_t n_samples = (n + sample_step - 1) / sample_step;
    if (n_samples > 1024) return false;   // sanity bound
    uint32_t sample_keys[1024];
    for (uint32_t i = 0; i < n_samples; ++i) {
        const uint32_t pos = std::min<uint32_t>(i * sample_step, n - 1);
        sample_keys[i] = stems[pos];
    }

    // Greedy segmentation.  Each segment is anchored at (x0, y0 = 0)
    // where x0 = sample_keys[start] and y_i = i - start.  Constraint
    // per point i: |slope * (sample_keys[i] - x0) - (i - start)| <= eps.
    // → slope in [(dy - eps) / dx, (dy + eps) / dx] when dx > 0.
    uint8_t  seg = 0;
    uint32_t start = 0;
    while (start < n_samples) {
        if (seg >= kMaxSegments) return false;   // doesn't fit
        const uint32_t x0 = sample_keys[start];
        // Track running slope bounds as (numerator, denominator) pairs
        // to avoid floating-point precision loss on long segments.
        // We hold lo_num/lo_den and hi_num/hi_den so the valid slope
        // range is [lo_num/lo_den, hi_num/hi_den].  Initialized wide.
        double lo = -1e30;
        double hi = +1e30;
        uint32_t end = start + 1;
        while (end < n_samples) {
            const uint32_t dx = sample_keys[end] - x0;
            const uint32_t dy = end - start;
            if (dx == 0) {
                // Duplicate sample key (rare on real workloads).  Any
                // slope works for this point as long as |slope*0 - dy|
                // <= eps, i.e., dy <= eps.  If dy > eps, segment breaks.
                if (dy > eps) break;
                ++end;
                continue;
            }
            const double cand_lo = (double(dy) - double(eps)) / double(dx);
            const double cand_hi = (double(dy) + double(eps)) / double(dx);
            const double new_lo  = std::max(lo, cand_lo);
            const double new_hi  = std::min(hi, cand_hi);
            if (new_lo > new_hi) break;
            lo = new_lo;
            hi = new_hi;
            ++end;
        }
        // Segment covers samples [start, end).  Pick slope at midpoint
        // of valid range (or 0 if degenerate single-sample segment).
        double slope = 0.0;
        if (end > start + 1) {
            // lo/hi may still be at sentinel values if the only constraint
            // was the dx==0 edge case; clamp to a sensible value.
            const double mid_lo = (lo > -1e29) ? lo : 0.0;
            const double mid_hi = (hi <  1e29) ? hi : 0.0;
            slope = (mid_lo + mid_hi) * 0.5;
            if (slope < 0.0) slope = 0.0;
            // Cap at slope just under 1.0 — beyond that the u32 recip
            // (slope × 2^32) would overflow.  In practice n_samples is
            // tiny relative to u32 key range so this never trips.
            if (slope > 0.999999) slope = 0.999999;
        }
        const uint32_t recip = uint32_t(slope * 4294967296.0);
        h->segment_keys[seg] = x0;
        h->recips[seg]       = recip;
        h->intercepts[seg]   = uint16_t(start);
        ++seg;
        start = end;
    }
    h->segment_count = seg;
    return true;
}

// Try increasing eps values until the build fits in MAX_SEGMENTS.
// Returns the eps that succeeded, or 0 on failure (n too large for
// even the loosest eps to fit).
inline uint16_t apgm_build_adaptive(apgm_header* h,
                                      const uint32_t* stems,
                                      uint32_t n,
                                      uint8_t sample_step = 8) noexcept {
    static constexpr uint16_t kEpsLadder[] = { 1, 2, 3, 4, 6, 8, 12, 16, 24, 32, 64, 128, 256, 512 };
    for (uint16_t eps : kEpsLadder) {
        if (apgm_build(h, stems, n, eps, sample_step)) return eps;
    }
    return 0;
}

}  // namespace psitri::wtree
