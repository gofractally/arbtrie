// bench_arbtrie_vs_wtree — apples-to-apples baseline: the long-stable arbtrie
// engine vs the new wtree, SAME data, SAME query order.
//
//   bench_arbtrie_vs_wtree [N=10000000]
//
// Workload (both engines): N unique 24-byte splitmix keys, 8-byte values.
//   INSERT phase: insert all N (timed).
//   QUERY  phase: look up all N in a FIXED random order (same seed for both),
//                 verifying each value (timed).
//
// Fairness notes (be honest about what differs):
//   * arbtrie = the production core engine: database + write_session + ONE
//     transaction for the whole insert batch (the canonical kv_bench insert
//     methodology), MVCC/COW, query via write-session root cursor.
//   * wtree   = the raw radix B-tree on SAL (unique mode; NO MVCC/transactions
//     yet), upsert/find with a per-op read_lock.
//   Both: sync_mode = none, single-threaded, background compactor ON.
// So this measures the tree algorithm + node layout + each engine's intrinsic
// per-op overhead — the baseline wtree must beat (and the gap wtree closes by
// not yet carrying MVCC).

#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/write_session_impl.hpp>

#include <psitri/wtree/wtree.hpp>

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
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

struct result { double ins_ns, qry_ns; uint64_t miss; };

// Build a fixed random permutation-ish query order (same for both engines).
static std::vector<uint64_t> query_order(uint64_t N)
{
    std::vector<uint64_t> q(N);
    uint64_t r = 0x9E3779B9ULL;
    for (uint64_t i = 0; i < N; ++i) { r = splitmix64(r); q[i] = r % N; }
    return q;
}

static result bench_arbtrie(const std::filesystem::path& dir, uint64_t N,
                            const std::vector<uint64_t>& qorder)
{
    psitri::runtime_config cfg;
    cfg.sync_mode = sal::sync_type::none;
    auto db  = psitri::database::create(dir.string(), cfg);
    auto ses = db->start_write_session();

    // INSERT — one transaction for the whole batch (kv_bench methodology).
    const double t0 = now_s();
    {
        auto tx = ses->start_transaction(0);
        uint8_t key[24];
        for (uint64_t i = 0; i < N; ++i)
        {
            make_key24(i, key);
            const uint64_t v = i + 1;
            tx.insert(psitri::key_view(reinterpret_cast<const char*>(key), 24),
                      psitri::value_view(reinterpret_cast<const char*>(&v), 8));
        }
        tx.commit();
    }
    const double ins_ns = (now_s() - t0) * 1e9 / double(N);

    // QUERY — fixed random order, verify each value.
    auto         root = ses->get_root(0);
    psitri::cursor cur(root);
    std::string  out;
    uint64_t     miss = 0;
    const double q0 = now_s();
    for (uint64_t j = 0; j < N; ++j)
    {
        const uint64_t idx = qorder[j];
        uint8_t        key[24]; make_key24(idx, key);
        const int32_t  rc = cur.get(psitri::key_view(reinterpret_cast<const char*>(key), 24), &out);
        if (rc < 0 || out.size() != 8) { ++miss; continue; }
        uint64_t got; std::memcpy(&got, out.data(), 8);
        if (got != idx + 1) ++miss;
    }
    const double qry_ns = (now_s() - q0) * 1e9 / double(N);
    return {ins_ns, qry_ns, miss};
}

static result bench_wtree(const std::filesystem::path& dir, uint64_t N,
                          const std::vector<uint64_t>& qorder)
{
    sal::allocator salloc(dir, sal::runtime_config{});
    salloc.register_type_ops<psitri::wtree::leaf_node>();
    salloc.register_type_ops<psitri::wtree::inner_node>();
    auto ses = salloc.get_session();
    psitri::wtree::wtree t(*ses, 8);

    // INSERT
    const double t0 = now_s();
    {
        uint8_t key[24];
        for (uint64_t i = 0; i < N; ++i)
        {
            make_key24(i, key);
            const uint64_t v = i + 1;
            t.upsert(std::span<const uint8_t>(key, 24),
                     std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&v), 8));
        }
    }
    const double ins_ns = (now_s() - t0) * 1e9 / double(N);

    // QUERY — same fixed random order, verify.
    uint64_t     miss = 0;
    const double q0 = now_s();
    for (uint64_t j = 0; j < N; ++j)
    {
        const uint64_t idx = qorder[j];
        uint8_t        key[24]; make_key24(idx, key);
        auto           vv = t.find(std::span<const uint8_t>(key, 24));
        if (vv.size() != 8) { ++miss; continue; }
        uint64_t got; std::memcpy(&got, vv.data(), 8);
        if (got != idx + 1) ++miss;
    }
    const double qry_ns = (now_s() - q0) * 1e9 / double(N);
    return {ins_ns, qry_ns, miss};
}

int main(int argc, char** argv)
{
    setvbuf(stdout, nullptr, _IOLBF, 0);
    const uint64_t N = (argc > 1) ? std::stoull(argv[1]) : 10'000'000ULL;
    const auto base = std::filesystem::temp_directory_path() /
                      ("arb_vs_wtree-" + std::to_string(::getpid()));
    std::filesystem::create_directories(base);

    std::printf("=== arbtrie vs wtree — N=%llu, 24-B keys, 8-B values, same data+query order ===\n",
                (unsigned long long)N);
    const auto qorder = query_order(N);

    result arb, wt;
    { auto d = base / "arb"; std::filesystem::create_directories(d); arb = bench_arbtrie(d, N, qorder); }
    { auto d = base / "wt";  std::filesystem::create_directories(d); wt  = bench_wtree(d, N, qorder); }

    std::printf("\n%-10s %14s %14s %14s %14s %8s\n",
                "engine", "insert ns/op", "insert M/s", "query ns/op", "query M/s", "miss");
    auto row = [&](const char* nm, const result& r) {
        std::printf("%-10s %14.1f %14.2f %14.1f %14.2f %8llu\n", nm,
                    r.ins_ns, 1e3 / r.ins_ns, r.qry_ns, 1e3 / r.qry_ns,
                    (unsigned long long)r.miss);
    };
    row("arbtrie", arb);
    row("wtree", wt);
    std::printf("\nwtree speedup:  insert %.2fx   query %.2fx\n",
                arb.ins_ns / wt.ins_ns, arb.qry_ns / wt.qry_ns);

    std::error_code ec;
    std::filesystem::remove_all(base, ec);
    return (arb.miss == 0 && wt.miss == 0) ? 0 : 1;
}
