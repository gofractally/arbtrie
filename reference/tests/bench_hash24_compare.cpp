// bench_hash24_compare — apples-to-apples vs libwtree hash24_compare_bench.
//
// Same 24-B splitmix workload, 8-B values, single-threaded, line-buffered
// stdout so the run can be tee'd to a file the user tails. Spec: N inserts
// (default 1M) followed by QN random queries from inserted set (default 100k).
//
// psitri wtree2 keys are u32 words; 24 B = 6 u32 words. Insert + find go through
// the SAL allocator with a temp-dir-backed session (matches bench_wtree2.cpp).

#include <psitri/wtree2/node.hpp>
#include <psitri/wtree2/insert.hpp>

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <sal/read_lock.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <unistd.h>

static inline uint64_t splitmix64(uint64_t x) noexcept {
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

static inline void make_key24(uint64_t i, uint8_t out[24]) noexcept {
    const uint64_t h0 = splitmix64(i ^ 0xA1B2C3D4ULL);
    const uint64_t h1 = splitmix64(i ^ 0xDEADBEEFULL);
    const uint64_t h2 = splitmix64(i ^ 0x12345678ULL);
    std::memcpy(out + 0,  &h0, 8);
    std::memcpy(out + 8,  &h1, 8);
    std::memcpy(out + 16, &h2, 8);
}

int main(int argc, char** argv) {
    setvbuf(stdout, nullptr, _IOLBF, 0);
    namespace w2 = psitri::wtree2;

    const uint64_t N  = (argc > 1) ? std::strtoull(argv[1], nullptr, 10) : 1'000'000ULL;
    const uint64_t QN = (argc > 2) ? std::strtoull(argv[2], nullptr, 10) :   100'000ULL;
    const uint16_t KW = 6;  // 24 B = 6 u32 words

    using clk = std::chrono::steady_clock;
    using std::chrono::duration;

    std::printf("=== psitri-wtree2 N=%llu insert + %llu rand-query, 24-B splitmix ===\n",
                (unsigned long long)N, (unsigned long long)QN);

    // sal session — temp dir, background compactor enabled (production default)
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path dbdir =
        std::filesystem::temp_directory_path() /
        ("hash24_cmp-" + std::to_string(ts) + "-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dbdir);

    sal::allocator salloc(dbdir, sal::runtime_config{}, /*threads=*/true);
    salloc.register_type_ops<w2::node2>();
    auto ses             = salloc.get_session();
    w2::g_wtree2_session = ses.get();

    w2::ptr_address root = w2::kNoChild;

    // ── insert ──
    const auto t0 = clk::now();
    for (uint64_t i = 0; i < N; ++i) {
        std::array<uint32_t, 6> k{};
        make_key24(i, reinterpret_cast<uint8_t*>(k.data()));
        const uint64_t v = splitmix64(i);
        sal::read_lock lk = ses->lock();
        root = w2::insert(root, {k.data(), KW},
                          {reinterpret_cast<const uint8_t*>(&v), 8});
    }
    const auto t1 = clk::now();
    const double ins_ns = duration<double, std::nano>(t1 - t0).count() / double(N);
    std::printf("[psitri]   insert: %llu keys in %.2fs (%.0f ns/key, %.2f Mops/s)\n",
                (unsigned long long)N,
                duration<double>(t1 - t0).count(),
                ins_ns,
                1e9 / ins_ns / 1e6);

    // ── 100k random queries from inserted set [0,N) ──
    uint64_t miss = 0, mism = 0;
    const auto t2 = clk::now();
    {
        sal::read_lock lk = ses->lock();
        w2::node2* r = w2::w2deref(root);
        std::array<uint32_t, 6> k{};
        for (uint64_t q = 0; q < QN; ++q) {
            const uint64_t idx = splitmix64(q ^ 0x5151515151515151ULL) % N;
            std::memset(k.data(), 0, sizeof(k));
            make_key24(idx, reinterpret_cast<uint8_t*>(k.data()));
            auto v = w2::find_value(r, {k.data(), KW});
            if (v.data() == nullptr) { ++miss; continue; }
            uint64_t got = 0;
            if (v.size() == 8) std::memcpy(&got, v.data(), 8);
            if (v.size() != 8 || got != splitmix64(idx)) ++mism;
        }
    }
    const auto t3 = clk::now();
    const double q_ns = duration<double, std::nano>(t3 - t2).count() / double(QN);
    std::printf("[psitri]   query:  %llu lookups in %.4fs (%.0f ns/lookup, miss=%llu mismatch=%llu)\n",
                (unsigned long long)QN,
                duration<double>(t3 - t2).count(),
                q_ns,
                (unsigned long long)miss,
                (unsigned long long)mism);

    std::printf("[psitri]   SUMMARY  insert_ns=%.0f  query_ns=%.0f\n", ins_ns, q_ns);

    std::filesystem::remove_all(dbdir);
    return (miss || mism) ? 1 : 0;
}
