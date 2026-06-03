// bench_wtree_sal — Stage-2 SAL-backed wtree bench. Same 24-B splitmix
// workload as bench_wtree.cpp (the malloc Stage-1 port), but the tree
// allocates nodes from a sal::allocator_session (ptr_address children,
// unique mode). Run both and diff: the delta is the pure allocator-swap
// cost (LTO tax cancels — both built in build-release).

#include <psitri/wtree/wtree.hpp>

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <sal/read_lock.hpp>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <filesystem>
#include <random>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

static double now_s() {
    using namespace std::chrono;
    return duration<double>(steady_clock::now().time_since_epoch()).count();
}

static inline uint64_t splitmix64(uint64_t x) noexcept {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

// 24-byte deterministic key from index i (3 u64 hashes). Same as bench_wtree.
static inline void make_key24(uint64_t i, uint8_t out[24]) noexcept {
    const uint64_t h0 = splitmix64(i ^ 0xA1B2C3D4ULL);
    const uint64_t h1 = splitmix64(i ^ 0xDEADBEEFULL);
    const uint64_t h2 = splitmix64(i ^ 0x12345678ULL);
    std::memcpy(out +  0, &h0, 8);
    std::memcpy(out +  8, &h1, 8);
    std::memcpy(out + 16, &h2, 8);
}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    const uint64_t N = (argc > 1) ? std::stoull(argv[1]) : 20'000'000ULL;

    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path dbdir =
        std::filesystem::temp_directory_path() /
        ("bench_wtree_sal-" + std::to_string(ts) + "-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dbdir);

    std::printf("=== bench_wtree_sal: N=%llu (24-B splitmix keys) dbdir=%s ===\n",
                (unsigned long long)N, dbdir.c_str());

    // argv[2]=="nocompact" → don't start SAL background threads (compactor).
    const bool start_threads = !(argc > 2 && std::string(argv[2]) == "nocompact");
    sal::allocator salloc(dbdir, sal::runtime_config{}, start_threads);
    std::printf("=== SAL background threads (compactor): %s ===\n",
                start_threads ? "ON" : "OFF");
    salloc.register_type_ops<psitri::wtree::leaf_node>();
    salloc.register_type_ops<psitri::wtree::inner_node>();
    auto ses = salloc.get_session();

    psitri::wtree::wtree t(*ses, 8);

    // ─── Build ────────────────────────────────────────────────
    // No outer lock: wtree::upsert() takes a per-op read_lock internally
    // (canonical psitri pattern), released after every insert so the
    // compactor's reclaim horizon advances continuously and freed leaves
    // are reclaimed in step (bounded memory).
    {
        const double tb0 = now_s();
        const uint64_t STEP = 1'000'000ULL;   // report instantaneous rate per 1M
        uint64_t next_step = STEP;
        double   seg_t0    = tb0;              // start time of the current 1M segment
        uint64_t shape_at  = 10'000'000ULL;    // count_pages (O(tree)) only at decades
        for (uint64_t i = 0; i < N; ++i) {
            uint8_t key[24];
            make_key24(i, key);
            const uint64_t v = i + 1;
            t.upsert(std::span<const uint8_t>(key, 24),
                     std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&v), 8));
            if (i + 1 == next_step) {
                const double now = now_s();
                const double seg_ns = (now - seg_t0) * 1e9 / double(STEP);     // last 1M
                const double cum_ns = (now - tb0)   * 1e9 / double(i + 1);     // cumulative
                if (i + 1 == shape_at) {
                    // Decade milestone: also report tree shape (O(tree) scan).
                    const auto pc = t.count_pages();
                    std::printf("[build] %4lluM  last1M=%.0f  cum=%.0f ns/op  | %zu leaves + %zu inners\n",
                                (unsigned long long)((i + 1) / 1'000'000ULL), seg_ns, cum_ns,
                                pc.leaves, pc.inners);
                    shape_at = (i + 1) * 10;
                } else {
                    std::printf("[build] %4lluM  last1M=%.0f  cum=%.0f ns/op\n",
                                (unsigned long long)((i + 1) / 1'000'000ULL), seg_ns, cum_ns);
                }
                seg_t0    = now_s();   // exclude the print/count from the next segment
                next_step += STEP;
            }
        }
        const double tb1 = now_s();
        const auto pc = t.count_pages();
        std::printf("[build] %llu inserts in %.2fs (%.2f Mops/s, %.0f ns/insert)\n",
                    (unsigned long long)N, tb1 - tb0,
                    double(N) / (tb1 - tb0) / 1e6,
                    (tb1 - tb0) * 1e9 / double(N));
        std::printf("[build] %zu leaves + %zu inners (%zu pages) depth=%zu\n",
                    pc.leaves, pc.inners, pc.leaves + pc.inners, t.tree_depth());
        // Leak/balance check: every transient alloc from splits/relocates must
        // have been freed, so live SAL objects == current tree node count.
        const uint64_t live   = ses->get_total_allocated_objects();
        const uint64_t nodes  = uint64_t(pc.leaves) + uint64_t(pc.inners);
        std::printf("[mem]   live SAL objects=%llu  tree nodes=%llu  -> %s\n",
                    (unsigned long long)live, (unsigned long long)nodes,
                    (live == nodes) ? "BALANCED" : "*** LEAK/IMBALANCE ***");
    }

    // ─── Verify all (correctness) ─────────────────────────────
    {
        size_t miss = 0, mismatch = 0;
        const double tv0 = now_s();
        for (uint64_t i = 0; i < N; ++i) {
            uint8_t key[24];
            make_key24(i, key);
            const auto v = t.find(std::span<const uint8_t>(key, 24));
            if (v.empty()) { ++miss; continue; }
            uint64_t got;
            std::memcpy(&got, v.data(), 8);
            if (got != i + 1) ++mismatch;
        }
        const double tv1 = now_s();
        std::printf("[verify] %llu lookups in %.2fs (%.0f ns/lookup), miss=%zu mismatch=%zu\n",
                    (unsigned long long)N, tv1 - tv0,
                    (tv1 - tv0) * 1e9 / double(N), miss, mismatch);
    }

    // ─── Hot bench: 1024 keys × 4096 iters (L1-resident) ──────
    {
        const size_t HOT_N = 1024;
        const size_t ITERS = 4096;
        std::vector<uint64_t> hot_idx(HOT_N);
        std::mt19937_64 r(0x1234);
        std::uniform_int_distribution<uint64_t> dist(0, N - 1);
        for (auto& x : hot_idx) x = dist(r);
        uint64_t sink = 0;
        for (auto i : hot_idx) {   // warm
            uint8_t key[24]; make_key24(i, key);
            sink ^= reinterpret_cast<uintptr_t>(
                t.find(std::span<const uint8_t>(key, 24)).data());
        }
        const double th0 = now_s();
        for (size_t it = 0; it < ITERS; ++it) {
            for (auto i : hot_idx) {
                uint8_t key[24]; make_key24(i, key);
                sink ^= reinterpret_cast<uintptr_t>(
                    t.find(std::span<const uint8_t>(key, 24)).data());
            }
        }
        const double th1 = now_s();
        asm volatile("" : : "r"(sink));
        std::printf("[hot]   %.2f ns/lookup\n",
                    (th1 - th0) * 1e9 / double(HOT_N * ITERS));
    }

    // ─── Cold bench: 1M random keys after a 256 MB cache evict ─
    {
        const size_t COLD_N = 1'000'000;
        std::vector<uint64_t> cold_idx(COLD_N);
        std::mt19937_64 r(0x5678);
        std::uniform_int_distribution<uint64_t> dist(0, N - 1);
        for (auto& x : cold_idx) x = dist(r);
        std::vector<uint8_t> evict(256 * 1024 * 1024, 0);
        volatile uint8_t es = 0;
        for (size_t i = 0; i < evict.size(); i += 64) es ^= evict[i];
        (void)es;
        uint64_t sink = 0;
        const double tc0 = now_s();
        for (auto i : cold_idx) {
            uint8_t key[24]; make_key24(i, key);
            sink ^= reinterpret_cast<uintptr_t>(
                t.find(std::span<const uint8_t>(key, 24)).data());
        }
        const double tc1 = now_s();
        asm volatile("" : : "r"(sink));
        std::printf("[cold]  %.2f ns/lookup\n",
                    (tc1 - tc0) * 1e9 / double(COLD_N));
    }

    return 0;
}
