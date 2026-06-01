// bench_wtree2 — first benchmark of the wtree2 rewrite.
//
// NOTE: "build" here is the RECURSIVE BULK CONSTRUCTOR (sort + construct), not
// incremental random insert (that's Phase 4, in progress). The FIND number is
// the real read-path benchmark over the new node2 structure at scale.
// Same 24-B splitmix workload as bench_wtree_sal for comparability.

#include <psitri/wtree2/node.hpp>

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <sal/read_lock.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <random>
#include <span>
#include <string>
#include <unistd.h>
#include <vector>

static double now_s()
{
    using namespace std::chrono;
    return duration<double>(steady_clock::now().time_since_epoch()).count();
}
static inline uint64_t splitmix64(uint64_t x) noexcept
{
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}
static inline void make_key24(uint64_t i, uint8_t out[24]) noexcept
{
    const uint64_t h0 = splitmix64(i ^ 0xA1B2C3D4ULL);
    const uint64_t h1 = splitmix64(i ^ 0xDEADBEEFULL);
    const uint64_t h2 = splitmix64(i ^ 0x12345678ULL);
    std::memcpy(out + 0, &h0, 8);
    std::memcpy(out + 8, &h1, 8);
    std::memcpy(out + 16, &h2, 8);
}

int main(int argc, char** argv)
{
    setvbuf(stdout, nullptr, _IOLBF, 0);
    namespace w2          = psitri::wtree2;
    const uint64_t N      = (argc > 1) ? std::stoull(argv[1]) : 10'000'000ULL;
    const bool     threads = !(argc > 2 && std::string(argv[2]) == "nocompact");

    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path dbdir =
        std::filesystem::temp_directory_path() /
        ("bench_wtree2-" + std::to_string(ts) + "-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dbdir);
    std::printf("=== bench_wtree2: N=%llu (24-B splitmix keys, 8-B values) ===\n",
                (unsigned long long)N);

    sal::allocator salloc(dbdir, sal::runtime_config{}, threads);
    salloc.register_type_ops<w2::node2>();
    auto ses                  = salloc.get_session();
    w2::g_wtree2_session      = ses.get();

    // ─── Generate + sort the entry stream ─────────────────────
    struct Ent { std::array<uint32_t, 6> key; uint64_t val; };
    std::printf("[gen]   generating %llu entries...\n", (unsigned long long)N);
    std::vector<Ent> ents(N);
    for (uint64_t i = 0; i < N; ++i)
    {
        uint8_t k[24]; make_key24(i, k);
        std::memcpy(ents[i].key.data(), k, 24);
        ents[i].val = i + 1;
    }
    double t0 = now_s();
    std::sort(ents.begin(), ents.end(), [](const Ent& a, const Ent& b) { return a.key < b.key; });
    ents.erase(std::unique(ents.begin(), ents.end(),
                           [](const Ent& a, const Ent& b) { return a.key == b.key; }),
               ents.end());
    const uint64_t M = ents.size();
    std::printf("[sort]  %llu unique in %.2fs\n", (unsigned long long)M, now_s() - t0);

    std::vector<w2::leaf_entry> le;
    le.reserve(M);
    for (auto& e : ents)
        le.push_back({{e.key.data(), 6}, {reinterpret_cast<const uint8_t*>(&e.val), 8}});

    // ─── Bulk build ───────────────────────────────────────────
    t0 = now_s();
    w2::ptr_address root;
    { sal::read_lock lk = ses->lock(); root = w2::bulk_build({le.data(), M}, 0); }
    const double build_s = now_s() - t0;
    std::printf("[build] bulk-build %llu in %.2fs (%.2f Mops/s, %.0f ns/key)\n",
                (unsigned long long)M, build_s, M / build_s / 1e6, build_s / M * 1e9);

    // ─── Find ─────────────────────────────────────────────────
    t0 = now_s();
    size_t miss = 0, mism = 0;
    {
        sal::read_lock lk = ses->lock();
        w2::node2*     r  = w2::w2deref(root);
        for (auto& e : ents)
        {
            auto v = w2::find_value(r, {e.key.data(), 6});
            if (v.data() == nullptr) {
                if (miss < 8)
                    std::printf("[miss]  key = %08x %08x %08x %08x %08x %08x\n",
                                e.key[0], e.key[1], e.key[2], e.key[3], e.key[4], e.key[5]);
                ++miss; continue;
            }
            uint64_t got = 0; std::memcpy(&got, v.data(), 8);
            if (v.size() != 8 || got != e.val) ++mism;
        }
    }
    const double find_s = now_s() - t0;
    std::printf("[find/seq] %llu lookups in %.2fs (%.0f ns/lookup), miss=%zu mismatch=%zu\n",
                (unsigned long long)M, find_s, find_s / M * 1e9, miss, mism);

    // Random-order find — the fair comparison (the seq pass above benefits from
    // sorted-order tree locality; this hits the tree in scrambled order).
    std::vector<uint32_t> order(M);
    for (uint32_t i = 0; i < M; ++i) order[i] = i;
    std::mt19937_64 rng(12345);
    std::shuffle(order.begin(), order.end(), rng);
    t0 = now_s();
    size_t miss2 = 0;
    {
        sal::read_lock lk = ses->lock();
        w2::node2*     r  = w2::w2deref(root);
        for (uint32_t idx : order)
            if (w2::find_value(r, {ents[idx].key.data(), 6}).data() == nullptr) ++miss2;
    }
    const double rfind_s = now_s() - t0;
    std::printf("[find/rnd] %llu lookups in %.2fs (%.0f ns/lookup), miss=%zu\n",
                (unsigned long long)M, rfind_s, rfind_s / M * 1e9, miss2);

    std::filesystem::remove_all(dbdir);
    return (miss || mism) ? 1 : 0;
}
