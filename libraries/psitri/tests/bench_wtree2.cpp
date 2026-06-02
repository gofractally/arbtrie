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

// ─── Experimental GRID builder (doc/implicit_dividers.md §13) ─────────
// Standalone, u32-only: partitions the sorted entries by byte-radix CELL so the
// inner dividers are an exact constant-stride power-of-2 grid (boundaries, not
// first-keys). Production w2::bulk_build is left untouched (clean rollback).
// `shift` = the current level's cell shift (24 → top byte, then 16, 8, 0).
namespace w2bench = psitri::wtree2;
static w2bench::ptr_address build_grid(std::span<const w2bench::leaf_entry> E, int shift)
{
    namespace w2 = psitri::wtree2;
    if (E.size() <= w2::kLeafCountCap || shift < 0)
        return w2::build_leaf(E, 0)->address();
    const uint32_t s      = uint32_t(shift);
    const uint32_t himask = (s + 8 >= 32) ? 0u : ~((1u << (s + 8)) - 1u);
    const uint32_t baseR  = E.front().key[0] & himask;            // common high bits
    const uint32_t clo    = (E.front().key[0] >> s) & 0xFFu;
    const uint32_t chi    = (E.back().key[0]  >> s) & 0xFFu;
    w2::ptr_address kids[256];
    uint32_t        divs[256];
    int             nk = 0;
    size_t          i  = 0;
    for (uint32_t c = clo; c <= chi; ++c)                          // dense cells [clo,chi]
    {
        size_t j = i;
        while (j < E.size() && ((E[j].key[0] >> s) & 0xFFu) == c) ++j;
        kids[nk] = (j > i) ? build_grid(E.subspan(i, j - i), shift - 8) : w2::kNoChild;
        if (c < chi) divs[nk] = baseR + (c + 1) * (1u << s);       // boundary → constant stride
        ++nk;
        i = j;
    }
    return w2::build_inner({}, {divs, size_t(nk - 1)}, {kids, size_t(nk)})->address();
}

int main(int argc, char** argv)
{
    setvbuf(stdout, nullptr, _IOLBF, 0);
    namespace w2          = psitri::wtree2;
    const uint64_t N      = (argc > 1) ? std::stoull(argv[1]) : 10'000'000ULL;
    bool insert_mode = false, nocompact = false, u32_mode = false, grid_mode = false,
         stream_mode = false;
    for (int a = 2; a < argc; ++a)
    {
        std::string s(argv[a]);
        if (s == "insert")    insert_mode = true;
        if (s == "nocompact") nocompact   = true;
        if (s == "u32")       u32_mode    = true;   // 1-word (4-B) keys instead of 24-B
        if (s == "grid")      grid_mode   = true;   // experimental grid builder (implies u32)
        if (s == "stream")    stream_mode = true;   // stateless incremental upsert + commit/1M
        if (s == "lu16")      w2::g_leaf_u16 = true; // u16 range-encoded leaves (Tiny16/Medium16)
    }
    if (grid_mode) u32_mode = true;                 // grid builder is u32-only
    const uint16_t KW = u32_mode ? 1 : 6;           // key words
    // Compactor ON by default (insert needs it to reclaim per-op garbage). In-place
    // mutation is safe under the per-insert read_lock (it pins nodes during the op).
    const bool     threads = !nocompact;
    auto mk = [&](uint64_t i, uint32_t* out) {
        if (u32_mode) { out[0] = uint32_t(splitmix64(i ^ 0xC0FFEEULL)); return; }
        make_key24(i, reinterpret_cast<uint8_t*>(out));
    };

    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path dbdir =
        std::filesystem::temp_directory_path() /
        ("bench_wtree2-" + std::to_string(ts) + "-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dbdir);
    std::printf("=== bench_wtree2: N=%llu (%u-B splitmix keys, 8-B values) ===\n",
                (unsigned long long)N, unsigned(KW) * 4u);

    sal::allocator salloc(dbdir, sal::runtime_config{}, threads);
    salloc.register_type_ops<w2::node2>();
    auto ses                  = salloc.get_session();
    w2::g_wtree2_session      = ses.get();

    // ─── Stateless INCREMENTAL upsert, STAGED (no generator arrays) ────────
    // Per round: insert STEP, then query QSTEP RANDOM keys from the inserted-so-far
    // set, print per-round insert/query ns — line-buffered so `tail -f` shows perf
    // over time (watch for insert/query ns climbing = compaction falling behind or
    // the tree degrading). read_lock PER OP (psitri pattern); sync::none = finalize
    // segments for compaction WITHOUT the msync syscall (fastest for this workload).
    if (stream_mode)
    {
        if (N > (1ull << 32)) { std::printf("stream: N must be <= 2^32\n"); return 2; }
        std::printf("=== stream (staged INCREMENTAL, sync::none): N=%llu u32, 8-B val, lu16=%d ===\n",
                    (unsigned long long)N, int(w2::g_leaf_u16));
        std::printf("   round      inserted   insert ns/key   query ns   miss\n");
        auto scramble = [N](uint64_t c) -> uint64_t {     // bijection over [0,N)
            return uint64_t(static_cast<__uint128_t>(c) * 2654435761ull % N);
        };
        const uint64_t  STEP  = 10'000'000;       // commit (sync) + status row every 10M
        const uint64_t  QSTEP = 100'000;
        w2::ptr_address root  = w2::kNoChild;
        size_t          tot_miss = 0;
        const double    t_all = now_s();
        for (uint64_t pbase = 0; pbase < N; pbase += STEP)
        {
            const uint64_t pend = std::min(pbase + STEP, N);
            const double   ti   = now_s();
            for (uint64_t c = pbase; c < pend; ++c)
            {
                const uint64_t i = scramble(c);
                const uint32_t k = uint32_t(i);
                uint64_t       v = splitmix64(i);
                { sal::read_lock lk = ses->lock();
                  root = w2::insert(root, {&k, 1}, {reinterpret_cast<const uint8_t*>(&v), 8}); }
            }
            ses->sync(sal::sync_type::none);          // commit every 10M
            const double ins_ns = (now_s() - ti) / double(pend - pbase) * 1e9;
            // query QSTEP random keys from the inserted-so-far set [0,pend)
            size_t       miss = 0;
            const double tq   = now_s();
            {
                sal::read_lock lk = ses->lock();
                w2::node2*     r  = w2::w2deref(root);
                for (uint64_t q = 0; q < QSTEP; ++q)
                {
                    const uint64_t c2 = splitmix64(q ^ (pbase * 0x9E37ULL + 1)) % pend;
                    const uint64_t i2 = scramble(c2);
                    const uint32_t k2 = uint32_t(i2);
                    auto v = w2::find_value(r, {&k2, 1});
                    uint64_t g = 0;
                    if (v.data()) std::memcpy(&g, v.data(), 8);
                    if (!v.data() || g != splitmix64(i2)) ++miss;
                }
            }
            const double q_ns = (now_s() - tq) / double(QSTEP) * 1e9;
            tot_miss += miss;
            std::printf("   %4lluM   %11llu   %12.0f   %8.0f   %4zu\n",
                        (unsigned long long)(pend / 1'000'000), (unsigned long long)pend,
                        ins_ns, q_ns, miss);
        }
        std::printf("=== total %.1fs, miss=%zu ===\n", now_s() - t_all, tot_miss);
        std::filesystem::remove_all(dbdir);
        return tot_miss ? 1 : 0;
    }

    // ─── Generate + sort the entry stream ─────────────────────
    struct Ent { std::array<uint32_t, 6> key; uint64_t val; };
    std::printf("[gen]   generating %llu entries...\n", (unsigned long long)N);
    std::vector<Ent> ents(N);
    for (uint64_t i = 0; i < N; ++i)
    {
        ents[i].key = {};               // zero unused words so the array compare works
        mk(i, ents[i].key.data());
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
        le.push_back({{e.key.data(), KW}, {reinterpret_cast<const uint8_t*>(&e.val), 8}});

    // ─── Build: bulk constructor OR incremental insert ────────
    w2::g_inner_total = 0; w2::g_inner_grid = 0;   // grid-detection counters (all modes)
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
            mk(i, k.data());
            uint64_t       v  = i + 1;
            sal::read_lock lk = ses->lock();
            root = w2::insert(root, {k.data(), KW},
                              {reinterpret_cast<const uint8_t*>(&v), 8});
        }
    }
    else if (grid_mode)
    {
        sal::read_lock lk = ses->lock();
        root = build_grid({le.data(), M}, 24);
    }
    else
    {
        sal::read_lock lk = ses->lock();
        root = w2::bulk_build({le.data(), M}, 0);
    }
    const double   build_s = now_s() - t0;
    const uint64_t built   = insert_mode ? N : M;
    std::printf("[%s] %llu in %.2fs (%.2f Mops/s, %.0f ns/key)\n",
                insert_mode ? "insert" : (grid_mode ? "grid  " : "build "),
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
        std::printf("[grid]  %llu/%llu inners are exact power-of-2 grids (%.0f%%)\n",
                    (unsigned long long)w2::g_inner_grid,
                    (unsigned long long)w2::g_inner_total,
                    w2::g_inner_total ? 100.0 * double(w2::g_inner_grid) / double(w2::g_inner_total) : 0.0);
    }

    // ─── Find (sorted / tree order — HOT, compute-bound) ──────────
    // Probe keys in sorted order (le is sorted) so the inners stay cache-hot and
    // the leaves stream under the HW prefetcher → cold-cl latency is hidden and the
    // per-lookup CPU work (route) is the bottleneck. This is where shift-routing
    // (one shift) should beat the apgm path (9-SIMD select + correction walk). The
    // sorted key-array read is sequential (streamed), NOT the random-320MB artifact.
    {
        sal::read_lock lk = ses->lock();
        w2::node2*     r  = w2::w2deref(root);
        double         th = now_s();
        size_t         missh = 0;
        for (uint64_t i = 0; i < M; ++i)
            if (w2::find_value(r, le[i].key).data() == nullptr) ++missh;
        const double hs = now_s() - th;
        std::printf("[find/hot] %llu sorted-order lookups in %.2fs (%.0f ns/lookup), miss=%zu\n",
                    (unsigned long long)M, hs, hs / M * 1e9, missh);
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
            mk(i, key.data());
            auto v = w2::find_value(r, {key.data(), KW});
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
            mk(idx, key.data());
            auto v = w2::find_value(r, {key.data(), KW});
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
