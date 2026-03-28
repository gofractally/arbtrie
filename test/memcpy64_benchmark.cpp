// Benchmark: fastest memcpy for 64-byte-aligned, 64-byte-multiple sizes.
// Compares glibc memcpy vs manual AVX-512 vmovdqa64 loops for typical node
// copy sizes (64-512 bytes, hot in L1/L2 cache).
//
// Build: compiled as part of the test/ CMake target with -O3 -march=native.

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <array>

#if defined(__AVX512F__)
#include <immintrin.h>
#endif

// ── implementations ──────────────────────────────────────────────────────────

static void copy_memcpy(void* dst, const void* src, size_t n) {
    memcpy(dst, src, n);
}

#if defined(__AVX512F__)
__attribute__((target("avx512f")))
static void copy_avx512_1zmm(void* dst, const void* src, size_t n) {
    // Single ZMM per iteration — simplest loop, 64 bytes/iter.
    const __m512i* s = reinterpret_cast<const __m512i*>(src);
          __m512i* d = reinterpret_cast<      __m512i*>(dst);
    for (size_t i = 0; i < n / 64; ++i)
        _mm512_store_si512(d + i, _mm512_load_si512(s + i));
}

__attribute__((target("avx512f")))
static void copy_avx512_4zmm(void* dst, const void* src, size_t n) {
    // 4 ZMMs per iteration (256 bytes/iter) for better instruction-level parallelism.
    const __m512i* s = reinterpret_cast<const __m512i*>(src);
          __m512i* d = reinterpret_cast<      __m512i*>(dst);
    size_t chunks = n / 64;
    size_t i = 0;
    for (; i + 4 <= chunks; i += 4) {
        __m512i v0 = _mm512_load_si512(s + i + 0);
        __m512i v1 = _mm512_load_si512(s + i + 1);
        __m512i v2 = _mm512_load_si512(s + i + 2);
        __m512i v3 = _mm512_load_si512(s + i + 3);
        _mm512_store_si512(d + i + 0, v0);
        _mm512_store_si512(d + i + 1, v1);
        _mm512_store_si512(d + i + 2, v2);
        _mm512_store_si512(d + i + 3, v3);
    }
    for (; i < chunks; ++i)
        _mm512_store_si512(d + i, _mm512_load_si512(s + i));
}

__attribute__((target("avx512f")))
static void copy_avx512_2zmm(void* dst, const void* src, size_t n) {
    // 2 ZMMs per iteration (128 bytes/iter).
    const __m512i* s = reinterpret_cast<const __m512i*>(src);
          __m512i* d = reinterpret_cast<      __m512i*>(dst);
    size_t chunks = n / 64;
    size_t i = 0;
    for (; i + 2 <= chunks; i += 2) {
        __m512i v0 = _mm512_load_si512(s + i + 0);
        __m512i v1 = _mm512_load_si512(s + i + 1);
        _mm512_store_si512(d + i + 0, v0);
        _mm512_store_si512(d + i + 1, v1);
    }
    if (i < chunks)
        _mm512_store_si512(d + i, _mm512_load_si512(s + i));
}
#endif  // __AVX512F__

// ── benchmark harness ────────────────────────────────────────────────────────

static const int ITERS = 2000000;

alignas(64) static uint8_t src_buf[512];
alignas(64) static uint8_t dst_buf[512];

using CopyFn = void(*)(void*, const void*, size_t);

static double bench(const char* label, CopyFn fn, size_t n) {
    // Warm up
    for (int i = 0; i < 10000; ++i) fn(dst_buf, src_buf, n);

    auto t0 = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < ITERS; ++i) {
        fn(dst_buf, src_buf, n);
        // Prevent the compiler from eliminating the store by reading back one byte
        asm volatile("" : : "m"(dst_buf[0]) : "memory");
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / ITERS;
    printf("  %-22s  n=%3zu  %5.2f ns\n", label, n, ns);
    return ns;
}

int main() {
    // Initialize buffers
    for (int i = 0; i < 512; ++i) src_buf[i] = (uint8_t)i;

    printf("=== memcpy benchmark: 64-byte-aligned, 64-byte-multiple sizes ===\n");
    printf("    (%d iterations per case, data hot in L1 cache)\n\n", ITERS);

    static const size_t sizes[] = {64, 128, 192, 256, 320, 384, 448, 512};

    for (size_t n : sizes) {
        printf("--- %zu bytes ---\n", n);
        bench("memcpy",          copy_memcpy,    n);
#if defined(__AVX512F__)
        bench("avx512_1zmm",     copy_avx512_1zmm, n);
        bench("avx512_2zmm",     copy_avx512_2zmm, n);
        bench("avx512_4zmm",     copy_avx512_4zmm, n);
#else
        printf("  (AVX-512 not available)\n");
#endif
        printf("\n");
    }
    return 0;
}
