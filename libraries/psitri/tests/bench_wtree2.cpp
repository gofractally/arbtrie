// bench_wtree2 — first benchmark of the wtree2 rewrite.
//
// NOTE: "build" here is the RECURSIVE BULK CONSTRUCTOR (sort + construct), not
// incremental random insert (that's Phase 4, in progress). The FIND number is
// the real read-path benchmark over the new node2 structure at scale.
// Same 24-B splitmix workload as bench_wtree_sal for comparability.

#include <psitri/wtree2/node.hpp>
#include <psitri/wtree2/insert.hpp>

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
#include <span>
#include <string>
#include <thread>
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
    const std::string mode = (argc > 2) ? std::string(argv[2]) : std::string();
    const bool     insert_mode = (mode == "insert");
    // Compactor ON by default (insert needs it to reclaim per-op garbage). In-place
    // mutation is safe under the per-insert read_lock (it pins nodes during the op).
    const bool     threads = (mode != "nocompact");

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

    // ─── Build: bulk constructor OR incremental insert ────────
    t0 = now_s();
    w2::ptr_address root;
    if (insert_mode)
    {
        // Lock per insert so the background compactor can reclaim the garbage each
        // insert produces (freed leaves/inners) — holding one lock for the whole
        // run would starve reclamation and exhaust segment space.
        root = w2::kNoChild;
        for (uint64_t i = 0; i < N; ++i)   // random (hash) order, value i+1
        {
            std::array<uint32_t, 6> k;
            make_key24(i, reinterpret_cast<uint8_t*>(k.data()));
            uint64_t       v  = i + 1;
            sal::read_lock lk = ses->lock();
            root = w2::insert(root, {k.data(), 6},
                              {reinterpret_cast<const uint8_t*>(&v), 8});
        }
    }
    else
    {
        sal::read_lock lk = ses->lock();
        root = w2::bulk_build({le.data(), M}, 0);
    }
    const double   build_s = now_s() - t0;
    const uint64_t built   = insert_mode ? N : M;
    std::printf("[%s] %llu in %.2fs (%.2f Mops/s, %.0f ns/key)\n",
                insert_mode ? "insert" : "build ",
                (unsigned long long)built, build_s, built / build_s / 1e6,
                build_s / built * 1e9);

    // Let the background compactor settle (it contracts roomy 4 KB leaves to their
    // exact occupancy once they stop changing) so find measures steady state.
    if (insert_mode && threads)
    {
        std::printf("[settle] letting compactor contract leaves...\n");
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    // ─── Structure stats ──────────────────────────────────────
    {
        sal::read_lock lk = ses->lock();
        struct St { uint64_t leaves=0, inners=0, leaf_entries=0, leaf_bytes=0, inner_fan=0,
                    inner_pfx=0, fan_le3=0, fan_ge256=0; uint16_t maxd=0, maxfan=0; } st;
        auto walk = [&](auto&& self, w2::ptr_address a, uint16_t d) -> void {
            w2::node2* nd = w2::w2deref(a);
            if (nd->is_leaf()) {
                ++st.leaves; st.leaf_entries += nd->num_stems; st.leaf_bytes += nd->size();
                if (d > st.maxd) st.maxd = d;
                return;
            }
            ++st.inners;
            const uint16_t fan = nd->n_slots();
            st.inner_fan += fan;
            if (nd->prefix_len) ++st.inner_pfx;
            if (fan <= 3) ++st.fan_le3;
            if (fan >= 256) ++st.fan_ge256;
            if (fan > st.maxfan) st.maxfan = fan;
            for (auto c : nd->child_span())
                if (w2::is_ext_slot(c)) self(self, c, uint16_t(d + 1));
        };
        walk(walk, root, 1);
        std::printf("[shape] %llu leaves + %llu inners, depth=%u, avg leaf=%.1f entries, "
                    "leaf bytes/entry=%.1f\n",
                    (unsigned long long)st.leaves, (unsigned long long)st.inners, st.maxd,
                    st.leaves ? double(st.leaf_entries) / st.leaves : 0.0,
                    st.leaf_entries ? double(st.leaf_bytes) / st.leaf_entries : 0.0);
        std::printf("[inner] avg fanout=%.1f max=%u | with-prefix=%llu | fanout<=3: %llu  >=256: %llu\n",
                    st.inners ? double(st.inner_fan) / st.inners : 0.0, st.maxfan,
                    (unsigned long long)st.inner_pfx, (unsigned long long)st.fan_le3,
                    (unsigned long long)st.fan_ge256);
    }

    // The tree now owns copies of every key+value (build_leaf memcpy'd them), so
    // the generator arrays are dead weight. Drop them BEFORE timing find so there
    // is no large array to accidentally touch — the probe key for index i is just
    // make_key24(i) and its expected value is i+1 (the splitmix hashes are 192-bit,
    // so for these N there are no collisions: M == N and i↦key↦value is a bijection).
    const bool bijective = (M == N);
    ents.clear(); ents.shrink_to_fit();
    le.clear();   le.shrink_to_fit();
    if (!bijective)
        std::printf("[warn]  %llu hash collisions — value check disabled\n",
                    (unsigned long long)(N - M));

    // ─── Find (sequential index) ──────────────────────────────
    // Key + expected value are regenerated from the loop counter — no value array.
    t0 = now_s();
    size_t miss = 0, mism = 0;
    {
        sal::read_lock lk = ses->lock();
        w2::node2*     r  = w2::w2deref(root);
        std::array<uint32_t, 6> key;
        for (uint64_t i = 0; i < N; ++i)
        {
            make_key24(i, reinterpret_cast<uint8_t*>(key.data()));
            auto v = w2::find_value(r, {key.data(), 6});
            if (v.data() == nullptr)         { ++miss; continue; }
            uint64_t got = 0; std::memcpy(&got, v.data(), 8);
            if (v.size() != 8 || (bijective && got != i + 1)) ++mism;
        }
    }
    const double find_s = now_s() - t0;
    std::printf("[find/seq] %llu lookups in %.2fs (%.0f ns/lookup), miss=%zu mismatch=%zu\n",
                (unsigned long long)N, find_s, find_s / N * 1e9, miss, mism);

    // ─── Find (random index) ──────────────────────────────────
    // r = splitmix64(k) % N selects a pseudo-random EXISTING key on the fly (with
    // replacement) — scrambled tree access with no order[]/probe[] array at all.
    t0 = now_s();
    size_t miss2 = 0, mism2 = 0;
    {
        sal::read_lock lk = ses->lock();
        w2::node2*     r  = w2::w2deref(root);
        std::array<uint32_t, 6> key;
        for (uint64_t k = 0; k < N; ++k)
        {
            const uint64_t idx = splitmix64(k ^ 0x5151515151515151ULL) % N;
            make_key24(idx, reinterpret_cast<uint8_t*>(key.data()));
            auto v = w2::find_value(r, {key.data(), 6});
            if (v.data() == nullptr)         { ++miss2; continue; }
            uint64_t got = 0; std::memcpy(&got, v.data(), 8);
            if (v.size() != 8 || (bijective && got != idx + 1)) ++mism2;
        }
    }
    const double rfind_s = now_s() - t0;
    std::printf("[find/rnd] %llu lookups in %.2fs (%.0f ns/lookup), miss=%zu mismatch=%zu\n",
                (unsigned long long)N, rfind_s, rfind_s / N * 1e9, miss2, mism2);

    std::filesystem::remove_all(dbdir);
    return (miss || mism || miss2 || mism2) ? 1 : 0;
}
