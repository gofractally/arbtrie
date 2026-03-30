/**
 * Benchmark: leaf node header insertion — seven approaches.
 *
 * The leaf header layout (N branches, C clines):
 *   kh[0..N-1]   uint8   key hash      (1 byte each)
 *   ko[0..N-1]   uint16  key offset    (2 bytes each)
 *   vo[0..N-1]   uint16  value_branch  (2 bytes each)
 *   cl[0..C-1]   uint32  cline addr    (4 bytes each)
 *   total = 5*N + 4*C bytes
 *
 * Inserting at branch position `bn` opens three gaps.
 *
 * Approaches:
 *   memmove3        — current baseline: three memmoves
 *   vpexpand        — per-array vpexpandb/vpexpandw, masks computed per call
 *   vpexpand_lut    — same but masks from a precomputed table (eliminates mask computation)
 *   full_zmm        — load entire kh+ko+vo into ≤2 ZMMs, single vpermt2b per ZMM,
 *                     perm vector computed analytically (no table)
 *   full_zmm_lut    — same but perm vector from a precomputed table (table fits in L1 for N≤12)
 *   vpermt2b_scalar — (baseline) scalar gather using precomputed perm16 table
 *   single_zmm      — pure single-ZMM vpermt2b for tiny headers (≤64 bytes total)
 *
 * Build:  cmake --build build/release --target leaf-insert-benchmark
 * Run:    ./build/release/bin/leaf-insert-benchmark
 */

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>

#if defined(__AVX512VBMI2__) || defined(__AVX512VBMI__)
#include <immintrin.h>
#endif

// ── helpers ───────────────────────────────────────────────────────────────────

static std::mt19937 rng(0xDEADBEEF);
static volatile uint64_t g_sink = 0;

template <typename F>
double bench_ns(F&& f, int reps = 5'000'000)
{
   for (int i = 0; i < 200'000; ++i) f();
   auto t0 = std::chrono::high_resolution_clock::now();
   for (int i = 0; i < reps; ++i) f();
   auto t1 = std::chrono::high_resolution_clock::now();
   return std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count() / double(reps);
}

static inline uint64_t mask64(int n) { return n < 64 ? ((uint64_t(1) << n) - 1) : ~uint64_t(0); }
static inline uint32_t mask32(int n) { return n < 32 ? ((uint32_t(1) << n) - 1) : ~uint32_t(0); }

// ── mock header buffer ────────────────────────────────────────────────────────

static constexpr int MAX_N = 255;
static constexpr int MAX_C = 16;
static constexpr int BUF   = 2048;

struct Header
{
   alignas(64) uint8_t buf[BUF];
   int N;
   int C;

   uint8_t*  kh() noexcept { return buf; }
   uint16_t* ko() noexcept { return reinterpret_cast<uint16_t*>(buf + N); }
   uint16_t* vo() noexcept { return reinterpret_cast<uint16_t*>(buf + 3 * N); }
   uint32_t* cl() noexcept { return reinterpret_cast<uint32_t*>(buf + 5 * N); }

   void fill_random() { for (int i = 0; i < BUF; ++i) buf[i] = static_cast<uint8_t>(rng()); }
};

// ── reference ────────────────────────────────────────────────────────────────

static void insert_reference(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   uint8_t  kh[MAX_N]; uint16_t ko[MAX_N], vo[MAX_N]; uint32_t cl[MAX_C];
   std::memcpy(kh, h.kh(), N);
   std::memcpy(ko, h.ko(), N * 2);
   std::memcpy(vo, h.vo(), N * 2);
   std::memcpy(cl, h.cl(), C * 4);
   h.N = N + 1;
   uint8_t*  kh2 = h.buf;
   uint16_t* ko2 = reinterpret_cast<uint16_t*>(h.buf + h.N);
   uint16_t* vo2 = reinterpret_cast<uint16_t*>(h.buf + 3 * h.N);
   uint32_t* cl2 = reinterpret_cast<uint32_t*>(h.buf + 5 * h.N);
   for (int i = 0; i < bn; ++i) kh2[i]     = kh[i]; kh2[bn] = 0;
   for (int i = bn; i < N; ++i) kh2[i + 1] = kh[i];
   for (int i = 0; i < bn; ++i) ko2[i]     = ko[i]; ko2[bn] = 0;
   for (int i = bn; i < N; ++i) ko2[i + 1] = ko[i];
   for (int i = 0; i < bn; ++i) vo2[i]     = vo[i]; vo2[bn] = 0;
   for (int i = bn; i < N; ++i) vo2[i + 1] = vo[i];
   std::memcpy(cl2, cl, C * 4);
}

// ── approach 1: memmove baseline ─────────────────────────────────────────────

static void insert_memmove(Header& h, int bn) noexcept
{
   constexpr int move_size = 5;
   const int tail = h.N - bn;
   std::memmove(reinterpret_cast<char*>(h.vo() + bn) + move_size,
                h.vo() + bn, tail * 2 + h.C * 4);
   std::memmove(reinterpret_cast<char*>(h.ko() + bn) + move_size - 2,
                h.ko() + bn, tail * 2 + bn * 2);
   std::memmove(reinterpret_cast<char*>(h.kh() + bn) + 1,
                h.kh() + bn, tail + bn * 2);
   h.N++;
   h.kh()[bn] = 0; h.ko()[bn] = 0; h.vo()[bn] = 0;
}

#if defined(__AVX512VBMI__)

// ── approach 2: vpexpand, masks computed per call ─────────────────────────────

__attribute__((target("avx512vbmi2,avx512bw")))
static void insert_vpexpand(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   if (N > 31) { insert_memmove(h, bn); return; }

   const uint8_t*  kh_src = h.buf;
   const uint16_t* ko_src = reinterpret_cast<const uint16_t*>(h.buf + N);
   const uint16_t* vo_src = reinterpret_cast<const uint16_t*>(h.buf + 3 * N);
   const uint8_t*  cl_src = h.buf + 5 * N;

   uint64_t kh_exp = mask64(N + 1) & ~(uint64_t(1) << bn);
   uint32_t kv_exp = mask32(N + 1) & ~(uint32_t(1) << bn);

   __m512i kh_v  = _mm512_maskz_loadu_epi8 (mask64(N), kh_src);
   __m512i ko_v  = _mm512_maskz_loadu_epi16(mask32(N), ko_src);
   __m512i vo_v  = _mm512_maskz_loadu_epi16(mask32(N), vo_src);
   int     cl_b  = C * 4;
   __m512i cl_v  = cl_b > 0 ? _mm512_maskz_loadu_epi8(mask64(cl_b), cl_src)
                             : _mm512_setzero_si512();

   __m512i kh_out = _mm512_maskz_expand_epi8 (kh_exp, kh_v);
   __m512i ko_out = _mm512_maskz_expand_epi16(kv_exp, ko_v);
   __m512i vo_out = _mm512_maskz_expand_epi16(kv_exp, vo_v);

   if (cl_b > 0) _mm512_mask_storeu_epi8(h.buf + 5*N+5,   mask64(cl_b),   cl_v);
   _mm512_mask_storeu_epi16(reinterpret_cast<uint16_t*>(h.buf + 3*N+3), mask32(N+1), vo_out);
   _mm512_mask_storeu_epi16(reinterpret_cast<uint16_t*>(h.buf + N+1),   mask32(N+1), ko_out);
   _mm512_mask_storeu_epi8 (h.buf,                                       mask64(N+1), kh_out);
   h.N++;
}

// ── approach 3: vpexpand with precomputed mask table ─────────────────────────
//
// Precompute all (kh_mask, kv_mask) pairs for N ≤ 31, bn ≤ N.
// Table is 32×33×12 bytes ≈ 12 KB — fits in L1 cache.
// Eliminates the mask computation (mask64/mask32 + ~) from the hot path.

struct VExpandMasks { uint64_t kh; uint32_t kv; };

static VExpandMasks g_expand_lut[32][33];  // [N][bn], N in [0,31], bn in [0,N]

static void init_expand_lut()
{
   for (int N = 0; N <= 31; ++N)
      for (int bn = 0; bn <= N; ++bn)
      {
         g_expand_lut[N][bn].kh = mask64(N + 1) & ~(uint64_t(1) << bn);
         g_expand_lut[N][bn].kv = mask32(N + 1) & ~(uint32_t(1) << bn);
      }
}

__attribute__((target("avx512vbmi2,avx512bw")))
static void insert_vpexpand_lut(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   if (N > 31) { insert_memmove(h, bn); return; }

   const auto& m = g_expand_lut[N][bn];

   const uint8_t*  kh_src = h.buf;
   const uint16_t* ko_src = reinterpret_cast<const uint16_t*>(h.buf + N);
   const uint16_t* vo_src = reinterpret_cast<const uint16_t*>(h.buf + 3 * N);
   const uint8_t*  cl_src = h.buf + 5 * N;

   __m512i kh_v  = _mm512_maskz_loadu_epi8 (mask64(N), kh_src);
   __m512i ko_v  = _mm512_maskz_loadu_epi16(mask32(N), ko_src);
   __m512i vo_v  = _mm512_maskz_loadu_epi16(mask32(N), vo_src);
   int     cl_b  = C * 4;
   __m512i cl_v  = cl_b > 0 ? _mm512_maskz_loadu_epi8(mask64(cl_b), cl_src)
                             : _mm512_setzero_si512();

   __m512i kh_out = _mm512_maskz_expand_epi8 (m.kh, kh_v);
   __m512i ko_out = _mm512_maskz_expand_epi16(m.kv, ko_v);
   __m512i vo_out = _mm512_maskz_expand_epi16(m.kv, vo_v);

   if (cl_b > 0) _mm512_mask_storeu_epi8(h.buf + 5*N+5,   mask64(cl_b),   cl_v);
   _mm512_mask_storeu_epi16(reinterpret_cast<uint16_t*>(h.buf + 3*N+3), mask32(N+1), vo_out);
   _mm512_mask_storeu_epi16(reinterpret_cast<uint16_t*>(h.buf + N+1),   mask32(N+1), ko_out);
   _mm512_mask_storeu_epi8 (h.buf,                                       mask64(N+1), kh_out);
   h.N++;
}

// ── helper: build a 64-byte vpermt2b index for one output chunk ───────────────
//
// Generates the permutation index for output bytes [out_off, out_off+64).
// Each output byte selects from a 128-byte "window" of source bytes starting at
// src_base: idx < 64 → source[src_base + idx], idx >= 64 → source[src_base+64 + idx-64].
// Holes (new kh/ko/vo) are set to 0xFF → selects from zero register.
//
// Source layout (OLD, N elements):
//   kh: [0, N)
//   ko: [N, 3N)   (byte offsets of uint16 array)
//   vo: [3N, 5N)
//   (clines not included — handled separately)

// Builds one 64-byte vpermt2b permutation index for output bytes [out_off, out_off+64).
//
// vpermt2b(idx, lo, hi): idx[i] < 64 selects lo[idx[i]], idx[i] >= 64 selects hi[idx[i]-64].
// We always set lo = src[0] (source bytes 0..63) and hi = src[1] (source bytes 64..127).
// So perm index = source byte index directly (< 64 → from lo, 64..127 → from hi).
// Constraint: source byte indices must be < 128 (true for N ≤ 24, src_bytes ≤ 120).
// Holes use 0xFF → selects hi[191] which is always 0 from the zero register.
static void build_perm_chunk(uint8_t* perm64, int N, int bn, int out_off)
{
   const int kh_hole   = bn;
   const int ko_hole_0 = N + 1 + 2 * bn;
   const int ko_hole_1 = ko_hole_0 + 1;
   const int vo_hole_0 = 3 * (N + 1) + 2 * bn;
   const int vo_hole_1 = vo_hole_0 + 1;

   for (int lane = 0; lane < 64; ++lane)
   {
      int  out_i   = out_off + lane;
      bool is_hole = (out_i == kh_hole
                   || out_i == ko_hole_0 || out_i == ko_hole_1
                   || out_i == vo_hole_0 || out_i == vo_hole_1);

      if (is_hole) { perm64[lane] = 0xFF; continue; }

      int src_off;
      if      (out_i < kh_hole)   src_off = 0;
      else if (out_i < ko_hole_0) src_off = 1;
      else if (out_i < vo_hole_0) src_off = 3;
      else                        src_off = 5;

      int src_i = out_i - src_off;
      assert(src_i >= 0 && src_i < 128);
      perm64[lane] = uint8_t(src_i);
   }
}

// ── approach 4: full_zmm — multi-ZMM cover of kh+ko+vo, analytical perm ──────
//
// Load the entire kh+ko+vo region (5*N bytes) into ≤3 source ZMMs.
// For each 64-byte output chunk, generate a vpermt2b permutation index analytically
// (build_perm_chunk above), apply vpermt2b(perm, src_lo, src_hi), store.
// Clines are shifted separately with a single ZMM load/store.
//
// The permutation is generated at call time (no table). This isolates the cost of
// build_perm_chunk from the table-lookup alternative (full_zmm_lut).

__attribute__((target("avx512vbmi,avx512bw")))
static void insert_full_zmm(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   // vpermt2b covers at most 2 source ZMMs (128 bytes). Limit to src_bytes ≤ 128.
   if (5 * N > 128) { insert_memmove(h, bn); return; }

   const int src_bytes = 5 * N;        // kh+ko+vo bytes in old layout
   const int dst_bytes = 5 * (N + 1);  // kh+ko+vo bytes in new layout
   const int n_out_zmm = (dst_bytes + 63) / 64;

   // Load source ZMMs (covering src_bytes)
   __m512i src[3];
   int n_src_zmm = (src_bytes + 63) / 64;
   for (int z = 0; z < n_src_zmm; ++z)
   {
      int off  = z * 64;
      int rem  = src_bytes - off;
      src[z]   = rem >= 64 ? _mm512_loadu_si512(h.buf + off)
                           : _mm512_maskz_loadu_epi8(mask64(rem), h.buf + off);
   }

   // Clines: load from old position before we touch the buffer
   int     cl_bytes = C * 4;
   __m512i cl_v     = cl_bytes > 0
                    ? _mm512_maskz_loadu_epi8(mask64(cl_bytes), h.buf + 5 * N)
                    : _mm512_setzero_si512();

   // For each output ZMM, generate perm analytically and apply vpermt2b.
   // Source window for output ZMM z starts at src_base = z*64 (two adjacent ZMMs).
   alignas(64) uint8_t perm64[64];

   // lo/hi are always src[0] and src[1] — perm index encodes which ZMM via bit 6.
   __m512i src_lo = src[0];
   __m512i src_hi = n_src_zmm > 1 ? src[1] : _mm512_setzero_si512();

   for (int z = 0; z < n_out_zmm; ++z)
   {
      build_perm_chunk(perm64, N, bn, z * 64);

      __m512i idx = _mm512_loadu_si512(perm64);
      __m512i out = _mm512_permutex2var_epi8(src_lo, idx, src_hi);

      int dst_off = z * 64;
      int dst_rem = dst_bytes - dst_off;
      if (dst_rem >= 64)
         _mm512_storeu_si512(h.buf + dst_off, out);
      else
         _mm512_mask_storeu_epi8(h.buf + dst_off, mask64(dst_rem), out);
   }

   // Clines: store shifted +5 bytes from old position (which is 5*(N+1) in new layout)
   if (cl_bytes > 0)
      _mm512_mask_storeu_epi8(h.buf + 5 * (N + 1), mask64(cl_bytes), cl_v);

   h.N++;
}

// ── approach 5: full_zmm_lut — same but with precomputed permutation table ────
//
// Precompute all permutation ZMMs for (N, bn) pairs with N ≤ LUT_N.
// Table size: (LUT_N+1)^2 / 2 * n_out_zmm * 64 bytes.
//
// LUT_N=12: src_bytes=60, dst_bytes=65, n_out_zmm=2.
//   Pairs: sum(1..13) = 91. Table: 91 * 2 * 64 = 11.6 KB  → fits in L1.
//
// LUT_N=24: src_bytes=120, dst_bytes=125, n_out_zmm=2.
//   Pairs: sum(1..25) = 325. Table: 325 * 2 * 64 = 41.6 KB → L2.
//
// ── approach 7: full_zmm_lut2 — no-mask variant, clines folded into perm ────
//
// Nodes are always 64-byte multiples, so we never need masked loads/stores.
// The permutation table covers kh+ko+vo+cl in one pass:
//   output bytes >= 5*(N+1) are clines: src = out_i - 5 (shift-right-by-5 rule).
//   output bytes in kh/ko/vo holes: 0xFF → zeroed by vpermt2b.
//
// Always executes exactly 8 ops: 2 aligned loads + 2 LUT loads +
//   2 vpermt2b + 2 aligned stores.  No masking, no clines special-case.
//
// Constraint: 5*(N+1) + 4*C ≤ 128 (all output bytes within 2 ZMMs).
//   For C=8:  N ≤ 18.   For C=0: N ≤ 24.
//   All source clines bytes also ≤ 127 (implied).
//   Garbage in tail bytes (positions > dst_bytes) is acceptable.

static constexpr int LUT_N = 24;

// Layout: g_perm_lut[N][bn][zmm_idx][64 bytes]
// We flatten [N][bn] → linear index to avoid wasted space for bn > N.
// For simplicity, use a full 2D array with bn dimension = LUT_N+1.
struct PermZmm { alignas(64) uint8_t p[2][64]; int n_out; };
static PermZmm g_perm_lut[LUT_N + 1][LUT_N + 1];

static void init_perm_lut()
{
   for (int N = 0; N <= LUT_N; ++N)
      for (int bn = 0; bn <= N; ++bn)
      {
         PermZmm& e = g_perm_lut[N][bn];
         e.n_out    = (5 * (N + 1) + 63) / 64;
         for (int z = 0; z < e.n_out && z < 2; ++z)
            build_perm_chunk(e.p[z], N, bn, z * 64);
      }
}

// ── perm table v2: always 2 ZMMs, includes clines, no masking needed ─────────

// Build one 64-byte vpermt2b index for output bytes [out_off, out_off+64).
// For output positions >= cl_out (= 5*(N+1)): clines rule: src = out_i - 5.
// For holes in kh/ko/vo: 0xFF (zeroed by vpermt2b).
// Constraint: all src indices < 128 — guaranteed when 5*(N+1)+4*C ≤ 128.
// (not used directly — see build_perm_chunk_v2_masked below)

// Wrapper that also returns the keep_mask (bit=1 for non-hole lanes).
static uint64_t build_perm_chunk_v2_masked(uint8_t* perm64, int N, int bn, int out_off)
{
   const int kh_hole   = bn;
   const int ko_hole_0 = N + 1 + 2 * bn;
   const int ko_hole_1 = ko_hole_0 + 1;
   const int vo_hole_0 = 3 * (N + 1) + 2 * bn;
   const int vo_hole_1 = vo_hole_0 + 1;
   const int cl_out    = 5 * (N + 1);
   uint64_t  keep      = 0;

   for (int lane = 0; lane < 64; ++lane)
   {
      int out_i = out_off + lane;
      if (out_i >= cl_out)
      {
         perm64[lane] = uint8_t(out_i - 5);
         keep |= uint64_t(1) << lane;
         continue;
      }
      bool is_hole = (out_i == kh_hole
                   || out_i == ko_hole_0 || out_i == ko_hole_1
                   || out_i == vo_hole_0 || out_i == vo_hole_1);
      if (is_hole) { perm64[lane] = 0; continue; }  // mask zeroes output

      int src_off;
      if      (out_i < kh_hole)   src_off = 0;
      else if (out_i < ko_hole_0) src_off = 1;
      else if (out_i < vo_hole_0) src_off = 3;
      else                        src_off = 5;
      perm64[lane] = uint8_t(out_i - src_off);
      keep |= uint64_t(1) << lane;
   }
   return keep;
}

struct PermZmm2
{
   alignas(64) uint8_t p[2][64];  // permutation indices
   uint64_t            keep[2];   // bit=1 at non-hole output positions (for maskz)
};
static PermZmm2 g_perm_lut2[LUT_N + 1][LUT_N + 1];

static void init_perm_lut2()
{
   for (int N = 0; N <= LUT_N; ++N)
      for (int bn = 0; bn <= N; ++bn)
      {
         PermZmm2& e = g_perm_lut2[N][bn];
         e.keep[0]   = build_perm_chunk_v2_masked(e.p[0], N, bn,  0);
         e.keep[1]   = build_perm_chunk_v2_masked(e.p[1], N, bn, 64);
      }
}

__attribute__((target("avx512vbmi,avx512bw")))
static void insert_full_zmm_lut(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   if (N > LUT_N) { insert_memmove(h, bn); return; }

   const PermZmm& e        = g_perm_lut[N][bn];
   const int      src_bytes = 5 * N;
   const int      dst_bytes = 5 * (N + 1);

   // Load source ZMMs
   int     n_src  = (src_bytes + 63) / 64;
   __m512i src[2] = {};
   for (int z = 0; z < n_src && z < 2; ++z)
   {
      int off = z * 64, rem = src_bytes - off;
      src[z]  = rem >= 64 ? _mm512_loadu_si512(h.buf + off)
                          : _mm512_maskz_loadu_epi8(mask64(rem), h.buf + off);
   }

   // Clines (load before overwriting buffer)
   int     cl_bytes = C * 4;
   __m512i cl_v     = cl_bytes > 0
                    ? _mm512_maskz_loadu_epi8(mask64(cl_bytes), h.buf + 5 * N)
                    : _mm512_setzero_si512();

   // lo/hi are always src[0]/src[1] — same pair for all output ZMMs.
   __m512i src_lo = src[0];
   __m512i src_hi = n_src > 1 ? src[1] : _mm512_setzero_si512();

   // Apply precomputed permutations
   for (int z = 0; z < e.n_out && z < 2; ++z)
   {
      __m512i idx = _mm512_load_si512(e.p[z]);  // aligned load from table
      __m512i out = _mm512_permutex2var_epi8(src_lo, idx, src_hi);

      int off = z * 64, rem = dst_bytes - off;
      if (rem >= 64) _mm512_storeu_si512(h.buf + off, out);
      else           _mm512_mask_storeu_epi8(h.buf + off, mask64(rem), out);
   }

   // Clines shifted to new position
   if (cl_bytes > 0)
      _mm512_mask_storeu_epi8(h.buf + 5 * (N + 1), mask64(cl_bytes), cl_v);

   h.N++;
}

// ── approach 6: single_zmm — for tiny headers that fit entirely in 64 bytes ───
//
// When 5*N + 4*C ≤ 64 (whole header fits in one ZMM), the permutation covers
// both the kh+ko+vo region AND clines in one instruction.
// Uses a precomputed 64-byte permutation that includes clines.
// Table is tiny: for 5*N+4*C ≤ 64, N ≤ C=0: N≤12, C=8: N≤6, C=16: N≤3.
// We just build the perm at call time (scalar loop, but only 64 iterations).

__attribute__((target("avx512vbmi,avx512bw")))
static void insert_single_zmm(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   const int src_bytes = 5 * N + 4 * C;
   const int dst_bytes = 5 * (N + 1) + 4 * C;

   if (src_bytes > 64 || dst_bytes > 64) { insert_memmove(h, bn); return; }

   // Build full perm (includes clines) covering OLD layout → NEW layout.
   // Uses int16_t to avoid the overflow/sentinel conflict for large indices.
   alignas(64) uint8_t perm8[64] = {};

   // Same logic as build_perm_chunk but also handles clines in the output.
   const int kh_hole   = bn;
   const int ko_hole_0 = N + 1 + 2 * bn;
   const int ko_hole_1 = ko_hole_0 + 1;
   const int vo_hole_0 = 3 * (N + 1) + 2 * bn;
   const int vo_hole_1 = vo_hole_0 + 1;
   const int cl_old    = 5 * N;      // clines start in OLD layout
   const int cl_new    = 5 * (N + 1); // clines start in NEW layout

   for (int out_i = 0; out_i < dst_bytes; ++out_i)
   {
      bool is_hole = (out_i == kh_hole
                  || out_i == ko_hole_0 || out_i == ko_hole_1
                  || out_i == vo_hole_0 || out_i == vo_hole_1);
      if (is_hole) { perm8[out_i] = 0xFF; continue; }

      int src_off;
      if      (out_i < kh_hole)   src_off = 0;
      else if (out_i < ko_hole_0) src_off = 1;
      else if (out_i < vo_hole_0) src_off = 3;
      else if (out_i < cl_new)    src_off = 5;
      else                        src_off = 5;  // clines: out_i - 5 = cl_old + (out_i - cl_new)

      perm8[out_i] = uint8_t(out_i - src_off);
   }

   __m512i src  = _mm512_maskz_loadu_epi8(mask64(src_bytes), h.buf);
   __m512i idx  = _mm512_loadu_si512(perm8);
   __m512i zero = _mm512_setzero_si512();
   __m512i out  = _mm512_permutex2var_epi8(src, idx, zero);
   _mm512_mask_storeu_epi8(h.buf, mask64(dst_bytes), out);

   h.N++;
}

// ── approach 7: full_zmm_lut2 — no-mask, clines folded, fixed 8-op path ─────

__attribute__((target("avx512vbmi,avx512bw")))
static void insert_full_zmm_lut2(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   // Fast path: all output bytes fit in 2 ZMMs; all source bytes also fit (implied).
   if (N > LUT_N || 5 * (N + 1) + 4 * C > 128) { insert_memmove(h, bn); return; }

   const PermZmm2& e = g_perm_lut2[N][bn];

   // Load 2 full aligned ZMMs — node is a 64-byte multiple, so always safe.
   __m512i src_lo = _mm512_load_si512(h.buf);
   __m512i src_hi = _mm512_load_si512(h.buf + 64);

   // Apply perm covering kh+ko+vo AND clines in one pass.
   // maskz_permutex2var: zeros output lanes where keep bit=0 (the 5 hole positions).
   __m512i out0 = _mm512_maskz_permutex2var_epi8(e.keep[0], src_lo, _mm512_load_si512(e.p[0]), src_hi);
   __m512i out1 = _mm512_maskz_permutex2var_epi8(e.keep[1], src_lo, _mm512_load_si512(e.p[1]), src_hi);

   // Store 2 full aligned ZMMs — garbage in tail bytes beyond dst_bytes is acceptable.
   _mm512_store_si512(h.buf,      out0);
   _mm512_store_si512(h.buf + 64, out1);

   h.N++;
}

// ── approach 8: sliding_lut — sliding source window, arbitrary N ─────────────
//
// Generalizes full_zmm_lut2 to arbitrary N by processing 64-byte output chunks
// with a sliding source pair, working in reverse order for safe in-place edits.
//
// Source pair for chunk z:
//   z=0,1 : (buf[0], buf[64])   — pre-loaded before any stores
//   z≥2   : (buf[(z-1)*64], buf[z*64])  — loaded fresh in reverse order
//
// Chunk type:
//   "complex" (z < n_complex): contains ≥1 hole → LUT perm + maskz zeroing
//   "shift-by-5" (z ≥ n_complex): no holes → fixed perm g_shift5_perm[l]=l+59
//
// n_complex = floor(vo_hole_1 / 64) + 1  where vo_hole_1 = 3*(N+1)+2*bn+1
// n_out     = ceil((5*(N+1)+4*C) / 64)    — only header bytes, no free/alloc area
//
// LUT3_N=50, LUT3_MAX_COMPLEX=4 (max n_complex for N≤50).
// LUT: g_chunk_perms ≈ 654 KB, g_chunk_keep ≈ 82 KB.

static constexpr int LUT3_N           = 50;
static constexpr int LUT3_MAX_COMPLEX = 4;  // floor((5*50+4)/64)+1 = 4

static uint8_t g_shift5_perm[64] __attribute__((aligned(64)));
static uint8_t g_chunk_perms[LUT3_N + 1][LUT3_N + 1][LUT3_MAX_COMPLEX][64] __attribute__((aligned(64)));
static uint64_t            g_chunk_keep[LUT3_N + 1][LUT3_N + 1][LUT3_MAX_COMPLEX];

// Build one 64-byte vpermt2b index for output bytes [out_off, out_off+64).
// src_base: absolute byte address of the lo source ZMM (0 for z=0,1; (z-1)*64 for z≥2).
// perm index = abs_source_byte - src_base, in [0,127].
static void build_chunk_perm(uint8_t* perm64, uint64_t* keep,
                              int N, int bn, int out_off, int src_base)
{
   const int kh_hole   = bn;
   const int ko_hole_0 = N + 1 + 2 * bn;
   const int ko_hole_1 = ko_hole_0 + 1;
   const int vo_hole_0 = 3 * (N + 1) + 2 * bn;
   const int vo_hole_1 = vo_hole_0 + 1;
   const int cl_out    = 5 * (N + 1);
   uint64_t  k         = 0;

   for (int lane = 0; lane < 64; ++lane)
   {
      int out_i = out_off + lane;
      if (out_i >= cl_out)
      {
         perm64[lane] = uint8_t(out_i - 5 - src_base);
         k |= uint64_t(1) << lane;
         continue;
      }
      bool is_hole = (out_i == kh_hole || out_i == ko_hole_0 || out_i == ko_hole_1 ||
                      out_i == vo_hole_0 || out_i == vo_hole_1);
      if (is_hole) { perm64[lane] = 0; continue; }
      int off;
      if      (out_i < kh_hole)   off = 0;
      else if (out_i < ko_hole_0) off = 1;
      else if (out_i < vo_hole_0) off = 3;
      else                        off = 5;
      perm64[lane] = uint8_t(out_i - off - src_base);
      k |= uint64_t(1) << lane;
   }
   *keep = k;
}

static void init_chunk_lut3()
{
   for (int l = 0; l < 64; ++l) g_shift5_perm[l] = uint8_t(l + 59);

   for (int N = 0; N <= LUT3_N; ++N)
      for (int bn = 0; bn <= N; ++bn)
      {
         int n_complex = (3 * (N + 1) + 2 * bn + 1) / 64 + 1;
         for (int z = 0; z < n_complex && z < LUT3_MAX_COMPLEX; ++z)
         {
            int src_base = (z <= 1) ? 0 : (z - 1) * 64;
            build_chunk_perm(g_chunk_perms[N][bn][z], &g_chunk_keep[N][bn][z],
                             N, bn, z * 64, src_base);
         }
      }
}

__attribute__((target("avx512vbmi,avx512bw")))
static void insert_sliding_lut(Header& h, int bn) noexcept
{
   const int N = h.N, C = h.C;
   if (N > LUT3_N) { insert_memmove(h, bn); return; }

   const int n_complex = (3 * (N + 1) + 2 * bn + 1) / 64 + 1;
   if (n_complex > LUT3_MAX_COMPLEX) { insert_memmove(h, bn); return; }

   const int n_out = (5 * (N + 1) + 4 * C + 63) / 64;

   // Pre-load z=0,1 source pair — must happen before z=1 store overwrites buf+64.
   const __m512i lo0    = _mm512_load_si512(h.buf);
   const __m512i hi0    = _mm512_load_si512(h.buf + 64);
   const __m512i shift5 = _mm512_load_si512(g_shift5_perm);

   // Shift-by-5 chunks (z ≥ n_complex, z ≥ 2) — reverse order.
   for (int z = n_out - 1; z >= n_complex && z >= 2; --z)
   {
      const int     sb = (z - 1) * 64;
      const __m512i lo = _mm512_load_si512(h.buf + sb);
      const __m512i hi = _mm512_load_si512(h.buf + sb + 64);
      _mm512_store_si512(h.buf + z * 64, _mm512_permutex2var_epi8(lo, shift5, hi));
   }

   // Complex LUT chunks (z < n_complex, z ≥ 2) — reverse order.
   for (int z = (n_complex - 1 < n_out - 1 ? n_complex - 1 : n_out - 1); z >= 2; --z)
   {
      const int     sb  = (z - 1) * 64;
      const __m512i lo  = _mm512_load_si512(h.buf + sb);
      const __m512i hi  = _mm512_load_si512(h.buf + sb + 64);
      const __m512i idx = _mm512_load_si512(g_chunk_perms[N][bn][z]);
      _mm512_store_si512(h.buf + z * 64,
          _mm512_maskz_permutex2var_epi8(g_chunk_keep[N][bn][z], lo, idx, hi));
   }

   // z=1 — uses pre-loaded lo0/hi0.
   if (n_out >= 2)
   {
      const __m512i out1 =
          n_complex <= 1
              ? _mm512_permutex2var_epi8(lo0, shift5, hi0)
              : _mm512_maskz_permutex2var_epi8(
                    g_chunk_keep[N][bn][1], lo0,
                    _mm512_load_si512(g_chunk_perms[N][bn][1]), hi0);
      _mm512_store_si512(h.buf + 64, out1);
   }

   // z=0 — always complex LUT, uses pre-loaded lo0/hi0.
   _mm512_store_si512(h.buf,
       _mm512_maskz_permutex2var_epi8(
           g_chunk_keep[N][bn][0], lo0,
           _mm512_load_si512(g_chunk_perms[N][bn][0]), hi0));

   h.N++;
}

#endif  // __AVX512VBMI__

// ── correctness ───────────────────────────────────────────────────────────────

static int g_failures = 0;

template <typename Fn>
static void verify_all(const char* name, Fn&& fn)
{
   int fails = 0;
   for (int N : {0, 1, 4, 8, 12, 15, 16, 24, 25, 31, 32, 63, 64, 100, 200})
      for (int C : {0, 4, 8, 16})
         for (int frac : {0, 1, 2, 3, 4})
         {
            int bn = (N * frac) / 4;
            if ((N + 1) * 5 + C * 4 + 64 > BUF) continue;

            Header ref, tst;
            ref.N = tst.N = N; ref.C = tst.C = C;
            ref.fill_random();
            std::memcpy(tst.buf, ref.buf, BUF);

            insert_reference(ref, bn);
            fn(tst, bn);

            int len = ref.N * 5 + ref.C * 4;
            bool ok = (ref.N == tst.N) && (std::memcmp(ref.buf, tst.buf, len) == 0);
            if (!ok)
            {
               if (fails < 2)
               {
                  printf("    FAIL N=%-3d C=%-2d bn=%-3d\n", N, C, bn);
                  for (int i = 0; i < len; ++i)
                     if (ref.buf[i] != tst.buf[i])
                     { printf("      byte[%d]: ref=0x%02x tst=0x%02x\n", i, ref.buf[i], tst.buf[i]); break; }
               }
               ++fails; ++g_failures;
            }
         }
   printf("  %-16s  %s\n", name, fails ? "FAIL" : "OK");
}

// ── benchmark ─────────────────────────────────────────────────────────────────

template <typename Fn>
static void bench_case(const char* name, Fn&& fn, int N, int C, int bn)
{
   Header h;
   h.N = N; h.C = C;
   h.fill_random();
   double ns = bench_ns([&] { h.N = N; fn(h, bn); g_sink ^= h.buf[0]; });
   printf("  %-18s  N=%-3d bn=%-3d  %6.2f ns\n", name, N, bn, ns);
}

int main()
{
   printf("\n=== leaf insert header benchmark ===\n");
#if defined(__AVX512VBMI__)
   printf("SIMD: AVX512VBMI");
#if defined(__AVX512VBMI2__)
   printf(" AVX512VBMI2");
#endif
   printf("\n");
#else
   printf("SIMD: none (only memmove3 will run)\n");
#endif

   // Build lookup tables
#if defined(__AVX512VBMI__)
   init_expand_lut();
   init_perm_lut();
   init_perm_lut2();
   init_chunk_lut3();
   printf("Tables: expand_lut=%zu B  perm_lut=%zu B  perm_lut2=%zu B  chunk_lut3=%zu B\n",
          sizeof(g_expand_lut), sizeof(g_perm_lut), sizeof(g_perm_lut2),
          sizeof(g_chunk_perms) + sizeof(g_chunk_keep));
#endif

   printf("\n--- correctness ---\n");
   verify_all("memmove3",       insert_memmove);
#if defined(__AVX512VBMI__)
   verify_all("vpexpand",       insert_vpexpand);
   verify_all("vpexpand_lut",   insert_vpexpand_lut);
   verify_all("full_zmm",       insert_full_zmm);
   verify_all("full_zmm_lut",   insert_full_zmm_lut);
   verify_all("single_zmm",     insert_single_zmm);
   verify_all("full_zmm_lut2",  insert_full_zmm_lut2);
   verify_all("sliding_lut",    insert_sliding_lut);
#endif

   if (g_failures) { printf("\n%d failure(s) — aborting.\n\n", g_failures); return 1; }

   constexpr int C = 8;
   struct TC { int N, bn; const char* note; };
   const TC cases[] = {
      {4,   2,   "tiny"},
      {8,   0,   "small-front"},
      {8,   4,   "small-mid"},
      {8,   7,   "small-back"},
      {12,  6,   "lut-boundary"},
      {16,  8,   "mid"},
      {24,  12,  "lut-N=LUT_N"},
      {25,  12,  "lut-fallback"},
      {31,  15,  "expand-boundary"},
      {32,  16,  "expand-fallback"},
      {48,  24,  "large"},
      {64,  32,  "large"},
      {128, 64,  "very-large"},
   };

   printf("\n--- benchmarks (ns/call, -O3 -march=native, C=%d) ---\n\n", C);

   auto run_sweep = [&](const char* label, auto fn)
   {
      printf("  [%s]\n", label);
      for (auto& tc : cases)
         bench_case(label, fn, tc.N, C, tc.bn);
      printf("\n");
   };

   run_sweep("memmove3",     insert_memmove);
#if defined(__AVX512VBMI__)
   run_sweep("vpexpand",     insert_vpexpand);
   run_sweep("vpexpand_lut", insert_vpexpand_lut);
   run_sweep("full_zmm",     insert_full_zmm);
   run_sweep("full_zmm_lut", insert_full_zmm_lut);
   run_sweep("single_zmm",   insert_single_zmm);
   run_sweep("full_zmm_lut2", insert_full_zmm_lut2);
   run_sweep("sliding_lut",   insert_sliding_lut);
#endif

   return 0;
}
