/**
 * cow_amort_bench.cpp
 *
 * Measures psitri COW amortization cost vs batch size and key pattern,
 * with std::map as an in-memory reference.
 *
 * Usage: cow-amort-bench [prepop_count [insert_count]]
 *   Default: prepop=1M insert=1M
 *
 * Key patterns:
 *   sequential  — big-endian uint64 ascending (always hits rightmost path)
 *   random      — bijective hash permutation of same range (no extra allocation)
 */

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory_resource>
#include <vector>

#include <absl/container/btree_map.h>

#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>

using Clock = std::chrono::high_resolution_clock;
using namespace psitri;

static constexpr uint32_t VALUE_SIZE = 8;
static const char*        DB_PATH    = "/tmp/cow_amort_bench_db";
static uint8_t            g_val_buf[VALUE_SIZE];

static void encode_be(uint64_t v, uint8_t out[8])
{
    out[0]=(v>>56)&0xFF; out[1]=(v>>48)&0xFF; out[2]=(v>>40)&0xFF; out[3]=(v>>32)&0xFF;
    out[4]=(v>>24)&0xFF; out[5]=(v>>16)&0xFF; out[6]=(v>> 8)&0xFF; out[7]=(v>> 0)&0xFF;
}

static uint64_t bijective_hash(uint64_t x)
{
    x ^= x >> 30; x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27; x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

static double elapsed_sec(Clock::time_point t0)
{
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

enum class Pattern { Sequential, Random };

static uint64_t gen_key(uint64_t prepop, uint64_t i, Pattern pat)
{
    uint64_t base = prepop + i;
    return pat == Pattern::Sequential ? base : bijective_hash(base);
}

// ── std::map reference ────────────────────────────────────────────────────────

static double run_stdmap(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    std::map<uint64_t, uint64_t> m;
    for (uint64_t i = 0; i < prepop; ++i)
        m.emplace(i, 0xABABABABABABABABULL);

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
        m[gen_key(prepop, i, pat)] = 0xABABABABABABABABULL;
    return elapsed_sec(t0);
}

static double run_pmrmap(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    // monotonic_buffer_resource: bump-allocates in geometrically growing chunks,
    // never frees individual nodes — all released at once when pool is destroyed.
    std::pmr::monotonic_buffer_resource pool;
    std::pmr::map<uint64_t, uint64_t> m{&pool};
    for (uint64_t i = 0; i < prepop; ++i)
        m.emplace(i, 0xABABABABABABABABULL);

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
        m[gen_key(prepop, i, pat)] = 0xABABABABABABABABULL;
    return elapsed_sec(t0);
    // pool destructor frees all node memory in one shot
}

static double run_absl_btree(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    absl::btree_map<uint64_t, uint64_t> m;
    for (uint64_t i = 0; i < prepop; ++i)
        m.emplace(i, 0xABABABABABABABABULL);

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
        m[gen_key(prepop, i, pat)] = 0xABABABABABABABABULL;
    return elapsed_sec(t0);
}

static double run_pmr_absl_btree(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    using PmrBtree = absl::btree_map<uint64_t, uint64_t, std::less<uint64_t>,
                                     std::pmr::polymorphic_allocator<std::pair<const uint64_t, uint64_t>>>;
    std::pmr::monotonic_buffer_resource pool;
    PmrBtree m{&pool};
    for (uint64_t i = 0; i < prepop; ++i)
        m.emplace(i, 0xABABABABABABABABULL);

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
        m[gen_key(prepop, i, pat)] = 0xABABABABABABABABULL;
    return elapsed_sec(t0);
}

// ── psitri ────────────────────────────────────────────────────────────────────

static std::shared_ptr<database> make_db()
{
    std::filesystem::remove_all(DB_PATH);
    psitri::runtime_config cfg;
    cfg.sync_mode = sal::sync_type::none;
    return database::create(DB_PATH, cfg);
}

static void prepopulate(database& db, uint64_t count)
{
    auto ses = db.start_write_session();
    const uint64_t CHUNK = 1'000'000;
    uint8_t kbuf[8];
    for (uint64_t base = 0; base < count; base += CHUNK)
    {
        uint64_t end = std::min(base + CHUNK, count);
        auto tx = ses->start_transaction(0);
        for (uint64_t i = base; i < end; ++i)
        {
            encode_be(i, kbuf);
            tx.upsert(key_view((char*)kbuf, 8), value_view((char*)g_val_buf, VALUE_SIZE));
        }
        tx.commit();
    }
    ses.reset();
    db.compact_and_truncate();
}

static double run_psitri(database& db, uint64_t prepop, uint64_t insert_count,
                         uint64_t num_batches, Pattern pat)
{
    auto     ses        = db.start_write_session();
    uint64_t batch_size = insert_count / num_batches;
    uint8_t  kbuf[8];

    auto t0 = Clock::now();
    for (uint64_t b = 0; b < num_batches; ++b)
    {
        auto     tx    = ses->start_transaction(0);
        uint64_t start = b * batch_size;
        uint64_t end   = start + batch_size;
        for (uint64_t i = start; i < end; ++i)
        {
            encode_be(gen_key(prepop, i, pat), kbuf);
            tx.upsert(key_view((char*)kbuf, 8), value_view((char*)g_val_buf, VALUE_SIZE));
        }
        tx.commit();
    }
    return elapsed_sec(t0);
}

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
    uint64_t prepop_count = 1'000'000;
    uint64_t insert_count = 1'000'000;
    if (argc >= 2) prepop_count = std::stoull(argv[1]);
    if (argc >= 3) insert_count = std::stoull(argv[2]);

    memset(g_val_buf, 0xAB, VALUE_SIZE);

    printf("=== Insert Benchmark: psitri vs std::map ===\n");
    printf("  Pre-populated keys : %llu\n", (unsigned long long)prepop_count);
    printf("  Inserted keys      : %llu\n", (unsigned long long)insert_count);
    printf("  Value size         : %u bytes\n\n", VALUE_SIZE);

    // ── std::map reference (no batch concept) ─────────────────────────────────
    fprintf(stderr, "std::map sequential...\n"); fflush(stderr);
    double map_seq_sec = run_stdmap(prepop_count, insert_count, Pattern::Sequential);
    fprintf(stderr, "std::map random...\n"); fflush(stderr);
    double map_rnd_sec = run_stdmap(prepop_count, insert_count, Pattern::Random);
    fprintf(stderr, "pmr::map sequential...\n"); fflush(stderr);
    double pmr_seq_sec = run_pmrmap(prepop_count, insert_count, Pattern::Sequential);
    fprintf(stderr, "pmr::map random...\n"); fflush(stderr);
    double pmr_rnd_sec = run_pmrmap(prepop_count, insert_count, Pattern::Random);
    fprintf(stderr, "absl::btree_map sequential...\n"); fflush(stderr);
    double absl_seq_sec = run_absl_btree(prepop_count, insert_count, Pattern::Sequential);
    fprintf(stderr, "absl::btree_map random...\n"); fflush(stderr);
    double absl_rnd_sec = run_absl_btree(prepop_count, insert_count, Pattern::Random);
    fprintf(stderr, "pmr absl::btree_map sequential...\n"); fflush(stderr);
    double pabsl_seq_sec = run_pmr_absl_btree(prepop_count, insert_count, Pattern::Sequential);
    fprintf(stderr, "pmr absl::btree_map random...\n"); fflush(stderr);
    double pabsl_rnd_sec = run_pmr_absl_btree(prepop_count, insert_count, Pattern::Random);

    double map_seq_ns   = map_seq_sec   * 1e9 / insert_count;
    double map_rnd_ns   = map_rnd_sec   * 1e9 / insert_count;
    double pmr_seq_ns   = pmr_seq_sec   * 1e9 / insert_count;
    double pmr_rnd_ns   = pmr_rnd_sec   * 1e9 / insert_count;
    double absl_seq_ns  = absl_seq_sec  * 1e9 / insert_count;
    double absl_rnd_ns  = absl_rnd_sec  * 1e9 / insert_count;
    double pabsl_seq_ns = pabsl_seq_sec * 1e9 / insert_count;
    double pabsl_rnd_ns = pabsl_rnd_sec * 1e9 / insert_count;

    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "structure", "seq ops/s", "seq ns/op", "rnd ops/s", "rnd ns/op");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "------------------------", "---------", "---------", "---------", "---------");
    printf("%-24s  %12.0f  %12.1f  %12.0f  %12.1f\n", "std::map",
           insert_count / map_seq_sec, map_seq_ns,
           insert_count / map_rnd_sec, map_rnd_ns);
    printf("%-24s  %12.0f  %12.1f  %12.0f  %12.1f\n", "pmr::map(monotonic)",
           insert_count / pmr_seq_sec, pmr_seq_ns,
           insert_count / pmr_rnd_sec, pmr_rnd_ns);
    printf("%-24s  %12.0f  %12.1f  %12.0f  %12.1f\n", "absl::btree_map",
           insert_count / absl_seq_sec, absl_seq_ns,
           insert_count / absl_rnd_sec, absl_rnd_ns);
    printf("%-24s  %12.0f  %12.1f  %12.0f  %12.1f\n", "pmr absl::btree_map",
           insert_count / pabsl_seq_sec, pabsl_seq_ns,
           insert_count / pabsl_rnd_sec, pabsl_rnd_ns);
    printf("\n");

    // ── psitri by batch size ──────────────────────────────────────────────────
    printf("%-14s  %12s  %12s  %12s  %12s  %12s  %12s\n",
           "psitri batch", "seq ops/s", "seq ns/op", "vs pabsl",
           "rnd ops/s", "rnd ns/op", "vs pabsl");
    printf("%-14s  %12s  %12s  %12s  %12s  %12s  %12s\n",
           "------------", "---------", "---------", "--------",
           "---------", "---------", "--------");

    std::vector<uint64_t> batch_counts;
    for (uint64_t nb = insert_count; nb >= 1; nb /= 10)
        batch_counts.push_back(nb);

    for (uint64_t num_batches : batch_counts)
    {
        uint64_t batch_size = insert_count / num_batches;

        fprintf(stderr, "[batch=%llu] seq...", (unsigned long long)batch_size); fflush(stderr);
        auto db_seq = make_db();
        prepopulate(*db_seq, prepop_count);
        double seq_sec = run_psitri(*db_seq, prepop_count, insert_count, num_batches, Pattern::Sequential);
        db_seq.reset();
        std::filesystem::remove_all(DB_PATH);

        fprintf(stderr, " rnd...\n"); fflush(stderr);
        auto db_rnd = make_db();
        prepopulate(*db_rnd, prepop_count);
        double rnd_sec = run_psitri(*db_rnd, prepop_count, insert_count, num_batches, Pattern::Random);
        db_rnd.reset();
        std::filesystem::remove_all(DB_PATH);

        double seq_ns = seq_sec * 1e9 / insert_count;
        double rnd_ns = rnd_sec * 1e9 / insert_count;

        printf("%-14llu  %12.0f  %12.1f  %11.1fx  %12.0f  %12.1f  %11.1fx\n",
               (unsigned long long)batch_size,
               insert_count / seq_sec, seq_ns, pabsl_seq_ns / seq_ns,
               insert_count / rnd_sec, rnd_ns, pabsl_rnd_ns / rnd_ns);
        fflush(stdout);
    }
    return 0;
}
