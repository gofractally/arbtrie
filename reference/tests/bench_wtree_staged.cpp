// bench_wtree_staged — interleaved 1M-insert / 1M-query rounds up to N.
//
// Each round: insert the next STEP keys, then query STEP random keys drawn
// from the whole inserted set. The read phases generate no garbage, giving
// the background compactor a chance to drain/reclaim between insert bursts —
// so we can watch whether RSS stays bounded, and get an insert-rate AND
// query-rate curve the entire way to 1B.
//
// Per-round line:
//   [N=   5M] ins=420  q=310 ns/op | rss=2.1GB live=70K pend=0 miss=0

#include <psitri/wtree/wtree.hpp>

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>
#include <sal/read_lock.hpp>

#include <mach/mach.h>

#include <algorithm>
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

static double now_s()
{
    using namespace std::chrono;
    return duration<double>(steady_clock::now().time_since_epoch()).count();
}

static double rss_gb()
{
    mach_task_basic_info_data_t info;
    mach_msg_type_number_t      cnt = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                  reinterpret_cast<task_info_t>(&info), &cnt) != KERN_SUCCESS)
        return 0.0;
    return double(info.resident_size) / (1024.0 * 1024.0 * 1024.0);
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
    const uint64_t N    = (argc > 1) ? std::stoull(argv[1]) : 1'000'000'000ULL;
    const uint64_t STEP = (argc > 2) ? std::stoull(argv[2]) : 1'000'000ULL;

    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path dbdir =
        std::filesystem::temp_directory_path() /
        ("bench_wtree_staged-" + std::to_string(ts) + "-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dbdir);

    std::printf("=== bench_wtree_staged: N=%llu step=%llu (insert/query rounds) ===\n",
                (unsigned long long)N, (unsigned long long)STEP);

    // argv[3] = compaction "unused" threshold in MB (default 16 = 50% of a
    // 32MB segment). Our split pattern frees <50% per segment, so the default
    // never recycles them — lower it to recycle at a smaller garbage fraction.
    const uint32_t thresh_mb = (argc > 3) ? uint32_t(std::stoul(argv[3])) : 16u;
    sal::runtime_config cfg;
    cfg.compact_unpinned_unused_threshold_mb = uint8_t(thresh_mb);
    cfg.compact_pinned_unused_threshold_mb   = uint8_t(std::min<uint32_t>(thresh_mb, 4u));
    std::printf("=== compaction unused-threshold: %u MB (of 32 MB segment = %.0f%%) ===\n",
                thresh_mb, 100.0 * thresh_mb / 32.0);
    sal::allocator salloc(dbdir, cfg);
    salloc.register_type_ops<psitri::wtree::leaf_node>();
    salloc.register_type_ops<psitri::wtree::inner_node>();
    auto ses = salloc.get_session();

    const bool do_sync = !(argc > 4 && std::string(argv[4]) == "0");
    std::printf("=== per-round sync (msync_async): %s ===\n", do_sync ? "ON" : "OFF");

    psitri::wtree::wtree t(*ses, 8);

    for (uint64_t base = 0; base < N; base += STEP)
    {
        const uint64_t end = std::min(base + STEP, N);

        // ── insert STEP new keys ──────────────────────────────
        const double ti0 = now_s();
        for (uint64_t i = base; i < end; ++i)
        {
            uint8_t        key[24];
            make_key24(i, key);
            const uint64_t v = i + 1;
            t.upsert(std::span<const uint8_t>(key, 24),
                     std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&v), 8));
        }
        const double ti1     = now_s();
        const double ins_ns  = (ti1 - ti0) * 1e9 / double(end - base);

        // Sync finalizes + marks full segments read-only → compactable.
        // Without it segments never become eligible (recyc_q stays 0) and
        // freed-leaf space is never reclaimed. msync_async is non-blocking.
        // argv[4]=0 disables (to show the difference).
        if (do_sync)
            ses->sync(sal::sync_type::msync_async);

        // ── query STEP random keys from the whole inserted set ─
        const uint64_t total = end;
        std::mt19937_64 r(0x5151ULL ^ base);
        std::uniform_int_distribution<uint64_t> dist(0, total - 1);
        uint64_t miss = 0, mismatch = 0, sink = 0;
        const double tq0 = now_s();
        for (uint64_t q = 0; q < STEP; ++q)
        {
            const uint64_t idx = dist(r);
            uint8_t        key[24];
            make_key24(idx, key);
            const auto vv = t.find(std::span<const uint8_t>(key, 24));
            if (vv.empty()) { ++miss; continue; }
            uint64_t got;
            std::memcpy(&got, vv.data(), 8);
            if (got != idx + 1) ++mismatch;
            sink ^= got;
        }
        const double tq1    = now_s();
        const double q_ns   = (tq1 - tq0) * 1e9 / double(STEP);
        asm volatile("" : : "r"(sink));

        // SAL ground-truth segment accounting (not RSS): total segments
        // allocated, live object bytes, and freed-but-not-recycled bytes.
        const auto   d         = salloc.dump();
        const double seg_gb    = double(d.total_segments) * 32.0 / 1024.0;     // 32MB segs
        const double live_gb   = double(d.total_read_bytes) / 1073741824.0;
        const double free_gb   = double(d.total_free_space) / 1073741824.0;

        std::printf("[N=%5lluM] ins=%4.0f q=%4.0f | rss=%5.1f segGB=%5.1f freeGB=%4.1f | "
                    "segs=%llu recyc_q=%llu freerel=%lld pend=%llu miss=%llu\n",
                    (unsigned long long)(total / 1'000'000ULL), ins_ns, q_ns, rss_gb(),
                    seg_gb, free_gb,
                    (unsigned long long)d.total_segments,
                    (unsigned long long)d.recycled_queue_depth,
                    (long long)d.free_release_count,
                    (unsigned long long)salloc.total_pending_releases(),
                    (unsigned long long)miss);
        (void)mismatch; (void)live_gb;
    }

    std::error_code ec;
    std::filesystem::remove_all(dbdir, ec);
    return 0;
}
