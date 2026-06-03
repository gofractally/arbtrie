// bench_sal_dump_race — fast reproducer for the allocator::dump() SIGBUS race.
//
// ROOT CAUSE: block_allocator::alloc() does _num_blocks.fetch_add(1) BEFORE
// reserve() grows the backing file (ftruncate+mmap) and publishes _file_size.
// So num_blocks() transiently counts a segment whose file backing is NOT yet
// committed. allocator::dump() iterates [0, num_blocks()) and dereferences each
// segment's in-band footer (is_finalized()/is_read_only() at offset
// segment_size-64). Reading that beyond-EOF mapped page faults (Darwin SIGBUS,
// EXC_BAD_ACCESS code=2).
//
// This bench maximizes the overlap: one thread inserts hard (constant segment
// growth) while a tight background thread hammers dump(). On the unfixed code
// it SIGBUSes within seconds; with the loop clamped to committed blocks it runs
// to completion.
//
//   bench_sal_dump_race [seconds=20]

#include <psitri/wtree/wtree.hpp>

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <span>
#include <string>
#include <thread>
#include <unistd.h>

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
    const double SECONDS = (argc > 1) ? std::stod(argv[1]) : 20.0;

    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    std::filesystem::path dbdir =
        std::filesystem::temp_directory_path() /
        ("bench_sal_dump_race-" + std::to_string(ts) + "-" + std::to_string(::getpid()));
    std::filesystem::create_directories(dbdir);

    sal::runtime_config cfg;
    cfg.compact_unpinned_unused_threshold_mb = 2;   // keep the compactor busy too
    cfg.compact_pinned_unused_threshold_mb   = 2;
    sal::allocator salloc(dbdir, cfg);
    salloc.register_type_ops<psitri::wtree::leaf_node>();
    salloc.register_type_ops<psitri::wtree::inner_node>();
    auto ses = salloc.get_session();

    std::printf("=== bench_sal_dump_race: %.0fs (insert thread + dump-hammer thread) ===\n",
                SECONDS);

    std::atomic<bool>     stop{false};
    std::atomic<uint64_t> dumps{0};

    // dump-hammer: tight loop calling dump() while segments grow underneath it.
    std::thread hammer([&] {
        while (!stop.load(std::memory_order_relaxed)) {
            const auto d = salloc.dump();
            asm volatile("" : : "r"(&d));
            dumps.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // insert: constant segment growth.
    psitri::wtree::wtree t(*ses, 8);
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t   i  = 0;
    while (true) {
        for (int b = 0; b < 50'000; ++b, ++i) {
            uint8_t key[24]; make_key24(i, key);
            const uint64_t v = i + 1;
            t.upsert(std::span<const uint8_t>(key, 24),
                     std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&v), 8));
        }
        ses->sync(sal::sync_type::msync_async);
        const double el = std::chrono::duration<double>(
                              std::chrono::steady_clock::now() - t0).count();
        if ((i % 1'000'000) < 50'000)
            std::printf("  inserted %lluM, dumps=%llu, %.1fs\n",
                        (unsigned long long)(i / 1'000'000),
                        (unsigned long long)dumps.load(), el);
        if (el >= SECONDS) break;
    }

    stop.store(true, std::memory_order_relaxed);
    hammer.join();
    std::printf("=== SURVIVED: %llu inserts, %llu dumps, no SIGBUS ===\n",
                (unsigned long long)i, (unsigned long long)dumps.load());
    std::error_code ec;
    std::filesystem::remove_all(dbdir, ec);
    return 0;
}
