/**
 * SIMD correctness verification and performance benchmark.
 *
 * Each test:
 *   1. Checks the SIMD dispatch result equals the scalar reference.
 *   2. Benchmarks "dispatch (SIMD path)" vs "scalar" and reports speedup.
 *
 * On x86-64: dispatches to SSE2 paths (always on), SSE4.1/SSSE3 with -march=native.
 * On ARM:    dispatches to NEON paths.
 *
 * Build with -DENABLE_NATIVE_ARCH=ON for full platform SIMD coverage.
 */

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <random>
#include <vector>

#include <psitri/node/inner_node_util.hpp>
#include <psitri/util.hpp>
#include <sal/min_index.hpp>
#include <ucc/lower_bound.hpp>

// ── helpers ───────────────────────────────────────────────────────────────────

static std::mt19937 rng(0xDEADBEEF);

static volatile uint64_t g_sink = 0;
#define SINK(x) (g_sink ^= static_cast<uint64_t>(x))

// Returns nanoseconds per call.
template <typename F>
double bench_ns(F&& f, int reps = 4'000'000)
{
   for (int i = 0; i < 100'000; ++i) f();  // warmup
   auto t0 = std::chrono::high_resolution_clock::now();
   for (int i = 0; i < reps; ++i) f();
   auto t1 = std::chrono::high_resolution_clock::now();
   return std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count() /
          static_cast<double>(reps);
}

static void print_result(const char* name, double scalar_ns, double simd_ns)
{
   const char* platform =
#if defined(__ARM_NEON)
       "NEON";
#elif defined(__SSE4_1__) && defined(__SSSE3__)
       "SSE4.1+SSSE3";
#elif defined(__SSE2__)
       "SSE2";
#else
       "scalar-only";
#endif
   printf("  %-50s  scalar %5.2f ns   %-13s %5.2f ns   speedup %.2fx\n",
          name, scalar_ns, platform, simd_ns, scalar_ns / simd_ns);
}

// ── lower_bound (ucc) ─────────────────────────────────────────────────────────

static void test_lower_bound_ucc()
{
   alignas(16) uint8_t buf[512];
   int failures = 0;
   for (int sz = 0; sz <= 255; ++sz)
   {
      for (int i = 0; i < sz; ++i)
         buf[i] = static_cast<uint8_t>(rng() & 0xFF);
      std::sort(buf, buf + sz);
      for (int v = 0; v <= 255; ++v)
      {
         auto expected = ucc::lower_bound_scalar(buf, sz, static_cast<uint8_t>(v));
         auto got      = ucc::lower_bound(buf, sz, static_cast<uint8_t>(v));
         if (expected != got)
            ++failures;
      }
   }
   printf("  ucc::lower_bound              correctness %s  (65536 cases)\n",
          failures ? "FAIL" : "OK");
   assert(failures == 0);
}

static void bench_lower_bound_ucc()
{
   alignas(16) uint8_t buf[512];
   constexpr int SZ = 64;
   for (int i = 0; i < SZ; ++i)
      buf[i] = static_cast<uint8_t>(i * 3 + 1);
   uint8_t val = 97;

   double s = bench_ns([&] { SINK(ucc::lower_bound_scalar(buf, SZ, val)); });
   double f = bench_ns([&] { SINK(ucc::lower_bound(buf, SZ, val)); });
   print_result("ucc::lower_bound (sorted, size=64)", s, f);
}

// ── lower_bound_padded (ucc) ──────────────────────────────────────────────────

static void test_lower_bound_padded()
{
   alignas(16) uint8_t buf[512];
   memset(buf, 0xFF, sizeof(buf));
   int failures = 0;
   for (int sz = 0; sz <= 240; ++sz)
   {
      for (int i = 0; i < sz; ++i)
         buf[i] = static_cast<uint8_t>(rng() & 0xFF);
      std::sort(buf, buf + sz);
      for (int v = 0; v <= 255; ++v)
      {
         auto expected = ucc::lower_bound_scalar(buf, sz, static_cast<uint8_t>(v));
         auto got      = ucc::lower_bound_padded(buf, sz, static_cast<uint8_t>(v));
         if (expected != got)
            ++failures;
      }
   }
   printf("  ucc::lower_bound_padded       correctness %s  (61440 cases)\n",
          failures ? "FAIL" : "OK");
   assert(failures == 0);
}

static void bench_lower_bound_padded()
{
   alignas(16) uint8_t buf[512];
   memset(buf, 0xFF, sizeof(buf));
   constexpr int SZ = 64;
   for (int i = 0; i < SZ; ++i)
      buf[i] = static_cast<uint8_t>(i * 3 + 1);
   uint8_t val = 97;

   double s = bench_ns([&] { SINK(ucc::lower_bound_scalar(buf, SZ, val)); });
   double f = bench_ns([&] { SINK(ucc::lower_bound_padded(buf, SZ, val)); });
   print_result("ucc::lower_bound_padded (sorted, size=64)", s, f);
}

// ── lower_bound (psitri) ──────────────────────────────────────────────────────

static void test_lower_bound_psitri()
{
   alignas(16) uint8_t buf[512];
   int failures = 0;
   for (int sz = 0; sz <= 255; ++sz)
   {
      for (int i = 0; i < sz; ++i)
         buf[i] = static_cast<uint8_t>(rng() & 0xFF);
      std::sort(buf, buf + sz);
      for (int v = 0; v <= 255; ++v)
      {
         auto expected = psitri::lower_bound_scalar(buf, sz, static_cast<uint8_t>(v));
         auto got      = psitri::lower_bound(buf, sz, static_cast<uint8_t>(v));
         if (expected != got)
            ++failures;
      }
   }
   printf("  psitri::lower_bound           correctness %s  (65536 cases)\n",
          failures ? "FAIL" : "OK");
   assert(failures == 0);
}

static void bench_lower_bound_psitri()
{
   alignas(16) uint8_t buf[512];
   constexpr int SZ = 64;
   for (int i = 0; i < SZ; ++i)
      buf[i] = static_cast<uint8_t>(i * 3 + 1);
   uint8_t val = 97;

   double s = bench_ns([&] { SINK(psitri::lower_bound_scalar(buf, SZ, val)); });
   double f = bench_ns([&] { SINK(psitri::lower_bound(buf, SZ, val)); });
   print_result("psitri::lower_bound (sorted, size=64)", s, f);
}

// ── find_u32x16 ───────────────────────────────────────────────────────────────

static void test_find_u32x16()
{
   alignas(16) uint32_t arr[32] = {};  // 16 meaningful + 16 padding
   int failures = 0;
   for (int sz = 1; sz <= 16; ++sz)
   {
      for (int i = 0; i < 16; ++i)
         arr[i] = static_cast<uint32_t>(i * 7 + 13);
      for (int pos = 0; pos < sz; ++pos)
      {
         uint32_t target   = arr[pos];
         auto     expected = ucc::find_u32x16_scalar_unrolled(arr, sz, target);
         auto     got      = ucc::find_u32x16(arr, sz, target);
         if (expected != got) ++failures;
      }
      auto expected = ucc::find_u32x16_scalar_unrolled(arr, sz, 0xDEAD);
      auto got      = ucc::find_u32x16(arr, sz, 0xDEAD);
      if (expected != got) ++failures;
   }
   printf("  ucc::find_u32x16              correctness %s  (sizes 1-16, all positions)\n",
          failures ? "FAIL" : "OK");
   assert(failures == 0);
}

static void bench_find_u32x16()
{
   alignas(16) uint32_t arr[32] = {};
   for (int i = 0; i < 16; ++i)
      arr[i] = static_cast<uint32_t>(i * 7 + 1);
   uint32_t target = arr[15];  // worst case

   // Explicitly compare scalar_unrolled vs the best available SIMD path.
   double s = bench_ns([&] { SINK(ucc::find_u32x16_scalar_unrolled(arr, 16, target)); });
#if defined(__AVX512F__)
   double f = bench_ns([&] { SINK(ucc::find_u32x16_avx512(arr, 16, target)); });
   print_result("ucc::find_u32x16 (size=16, worst-case, scalar vs AVX-512)", s, f);
#elif defined(__SSE2__)
   double f = bench_ns([&] { SINK(ucc::find_u32x16_sse2(arr, 16, target)); });
   print_result("ucc::find_u32x16 (size=16, worst-case, scalar vs SSE2)", s, f);
#else
   double f = bench_ns([&] { SINK(ucc::find_u32x16(arr, 16, target)); });
   print_result("ucc::find_u32x16 (size=16, worst-case)", s, f);
#endif
}

// ── create_nth_set_bit_table ──────────────────────────────────────────────────

static void test_create_nth_set_bit_table()
{
   int failures = 0;
   for (int trial = 0; trial < 10000; ++trial)
   {
      std::array<uint8_t, 16> input;
      for (auto& b : input) b = static_cast<uint8_t>(rng() & 0xFF);
      auto expected = psitri::create_nth_set_bit_table_scalar(input);
      auto got      = psitri::create_nth_set_bit_table(input);
      if (expected != got) ++failures;
   }
   printf("  psitri::create_nth_set_bit_table  correctness %s  (10000 random inputs)\n",
          failures ? "FAIL" : "OK");
   assert(failures == 0);
}

static void bench_create_nth_set_bit_table()
{
   // This function is tiny (~0.1 ns). Batch 64 calls to get measurable time,
   // then explicitly call scalar vs SSE2 to avoid measuring the same dispatch path.
   std::array<std::array<uint8_t, 16>, 64> inputs;
   for (auto& inp : inputs)
      for (auto& b : inp) b = static_cast<uint8_t>(rng() & 0xFF);

#if defined(__SSE2__)
   double s = bench_ns(
       [&] {
          for (auto& inp : inputs) SINK(psitri::create_nth_set_bit_table_scalar(inp)[0]);
       },
       200'000);
   double f = bench_ns(
       [&] {
          for (auto& inp : inputs) SINK(psitri::create_nth_set_bit_table_sse2(inp)[0]);
       },
       200'000);
   print_result("psitri::create_nth_set_bit_table (x64 batch, scalar vs SSE2)", s / 64, f / 64);
#else
   double s = bench_ns([&] { SINK(psitri::create_nth_set_bit_table_scalar(inputs[0])[0]); });
   double f = bench_ns([&] { SINK(psitri::create_nth_set_bit_table(inputs[0])[0]); });
   print_result("psitri::create_nth_set_bit_table", s, f);
#endif
}

// ── find_min_index ────────────────────────────────────────────────────────────

static void test_find_min_index()
{
   alignas(64) uint16_t arr32[32];
   alignas(64) uint16_t arr64[64];
   int failures = 0;
   for (int trial = 0; trial < 100000; ++trial)
   {
      for (auto& v : arr32) v = static_cast<uint16_t>(rng() & 0xFFFF);
      for (auto& v : arr64) v = static_cast<uint16_t>(rng() & 0xFFFF);
      // Both indices must point to the same minimum value (ties allowed at different indices)
      if (arr32[sal::find_min_index32_tournament(arr32)] !=
          arr32[sal::find_min_index_32(arr32)])
         ++failures;
      if (arr64[sal::find_min_index64_tournament(arr64)] !=
          arr64[sal::find_min_index_64(arr64)])
         ++failures;
   }
   printf("  sal::find_min_index 32+64     correctness %s  (100000 arrays each)\n",
          failures ? "FAIL" : "OK");
   assert(failures == 0);
}

// Naive linear scan — reference baseline showing what we'd have without optimization.
static int find_min_index_naive(const uint16_t* v, int n)
{
   int best = 0;
   for (int i = 1; i < n; ++i)
      if (v[i] < v[best]) best = i;
   return best;
}

static void bench_find_min_index()
{
   alignas(64) uint16_t arr32[32];
   alignas(64) uint16_t arr64[64];
   for (auto& v : arr32) v = static_cast<uint16_t>(rng() & 0xFFFF);
   for (auto& v : arr64) v = static_cast<uint16_t>(rng() & 0xFFFF);

   // On x86: find_min_index_32/64 dispatch to the tournament (no SSE path — SSE4.1 was
   // benchmarked and found slower than tournament due to _mm_extract_epi16 latency chains).
   // Compare naive linear scan vs tournament so the baseline reflects real savings.
   {
      double s = bench_ns([&] { SINK(find_min_index_naive(arr32, 32)); });
      double f = bench_ns([&] { SINK(sal::find_min_index32_tournament(arr32)); });
      print_result("sal::find_min_index_32 (naive scan vs tournament)", s, f);
   }
   {
      double s = bench_ns([&] { SINK(find_min_index_naive(arr64, 64)); });
      double f = bench_ns([&] { SINK(sal::find_min_index64_tournament(arr64)); });
      print_result("sal::find_min_index_64 (naive scan vs tournament)", s, f);
   }
}

// ── copy_branches_and_update_cline_index ─────────────────────────────────────

static void test_copy_branches()
{
   alignas(16) uint8_t inp_buf[256] = {};
   alignas(16) uint8_t out_scalar[256] = {};
   alignas(16) uint8_t out_simd[256] = {};
   uint8_t* inp = inp_buf;

   std::array<uint8_t, 16> lut;
   int                     failures = 0;
   for (int trial = 0; trial < 1000; ++trial)
   {
      for (auto& v : lut) v = static_cast<uint8_t>(rng() % 16);
      for (int i = 0; i < 256; ++i) inp[i] = static_cast<uint8_t>(rng() & 0xFF);
      for (int N = 1; N <= 128; ++N)
      {
         memset(out_scalar + 16, 0, 128);
         memset(out_simd + 16, 0, 128);
         psitri::copy_branches_and_update_cline_index_scalar(inp, out_scalar, N, lut);
         psitri::copy_branches_and_update_cline_index(
             reinterpret_cast<psitri::branch*>(inp),
             reinterpret_cast<psitri::branch*>(out_simd), N, lut);
         if (memcmp(out_scalar, out_simd, N) != 0)
            ++failures;
      }
   }
   printf("  psitri::copy_branches         correctness %s  (N=1..128, 1000 trials)\n",
          failures ? "FAIL" : "OK");
   assert(failures == 0);
}

static void bench_copy_branches()
{
   alignas(16) uint8_t inp_buf[256] = {};
   alignas(16) uint8_t out[256] = {};
   uint8_t* inp = inp_buf;
   for (int i = 0; i < 128; ++i) inp[i] = static_cast<uint8_t>(rng() & 0xFF);
   std::array<uint8_t, 16> lut;
   for (auto& v : lut) v = static_cast<uint8_t>(rng() % 16);

   for (int N : {16, 64, 128})
   {
      double s = bench_ns([&] {
         psitri::copy_branches_and_update_cline_index_scalar(inp, out, N, lut);
         SINK(out[0]);
      });
      double f = bench_ns([&] {
         psitri::copy_branches_and_update_cline_index(
             reinterpret_cast<psitri::branch*>(inp),
             reinterpret_cast<psitri::branch*>(out), N, lut);
         SINK(out[0]);
      });
      char name[64];
      snprintf(name, sizeof(name), "psitri::copy_branches (N=%d)", N);
      print_result(name, s, f);
   }
}

// ── main ──────────────────────────────────────────────────────────────────────

int main()
{
   printf("\n=== SIMD Correctness + Performance Report");
#if defined(__ARM_NEON)
   printf(" [ARM NEON]");
#elif defined(__SSE4_1__) && defined(__SSSE3__)
   printf(" [x86 SSE2 + SSE4.1 + SSSE3]");
#elif defined(__SSE2__)
   printf(" [x86 SSE2 only — rebuild with -DENABLE_NATIVE_ARCH=ON for SSE4.1/SSSE3]");
#else
   printf(" [no SIMD — scalar only]");
#endif
   printf(" ===\n\n");

   printf("Correctness checks:\n");
   test_lower_bound_ucc();
   test_lower_bound_padded();
   test_lower_bound_psitri();
   test_find_u32x16();
   test_create_nth_set_bit_table();
   test_find_min_index();
   test_copy_branches();

   printf("\nBenchmarks (SIMD dispatch vs scalar, all data in L1 cache):\n");
   bench_lower_bound_ucc();
   bench_lower_bound_padded();
   bench_lower_bound_psitri();
   bench_find_u32x16();
   bench_create_nth_set_bit_table();
   bench_find_min_index();
   bench_copy_branches();

   printf("\nAll correctness checks passed.\n\n");
   if (g_sink == 0xDEADBEEF) printf("(magic sink)\n");
   return 0;
}
