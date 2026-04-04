/**
 * cow_amort_bench.cpp
 *
 * Measures psitri COW amortization cost vs batch size and key pattern,
 * with std::map, absl::btree_map, and libart ART as in-memory references.
 *
 * Usage: cow-amort-bench [prepop_count [insert_count]]
 *   Default: prepop=1M insert=1M
 *
 * Key patterns:
 *   sequential  — big-endian uint64 ascending (always hits rightmost path)
 *   random      — bijective hash permutation of same range (no extra allocation)
 *
 * Tests both uint64 keys (fixed-size) and variable-length string keys
 * to evaluate ART's advantage with prefix-heavy key workloads.
 */

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

#include <absl/container/btree_map.h>

#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>

extern "C" {
#include "art.h"
}

#include <art/art_map.hpp>

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

// Generate a variable-length string key like "/db/table/row/col/<number>"
// This simulates prefix-heavy keys typical of WAL workloads
static void gen_string_key(uint64_t prepop, uint64_t i, Pattern pat, char* buf, int* len)
{
    uint64_t v = gen_key(prepop, i, pat);
    *len = snprintf(buf, 64, "/db/table/row/col/%016llx", (unsigned long long)v);
}

// ── uint64 key benchmarks ────────────────────────────────────────────────────

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
    std::pmr::monotonic_buffer_resource pool;
    std::pmr::map<uint64_t, uint64_t> m{&pool};
    for (uint64_t i = 0; i < prepop; ++i)
        m.emplace(i, 0xABABABABABABABABULL);

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
        m[gen_key(prepop, i, pat)] = 0xABABABABABABABABULL;
    return elapsed_sec(t0);
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

static double run_art(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    art_tree t;
    art_tree_init(&t);
    uint8_t kbuf[8];
    for (uint64_t i = 0; i < prepop; ++i)
    {
        encode_be(i, kbuf);
        art_insert(&t, kbuf, 8, (void*)0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        encode_be(gen_key(prepop, i, pat), kbuf);
        art_insert(&t, kbuf, 8, (void*)0xABABABABABABABABULL);
    }
    double sec = elapsed_sec(t0);
    art_tree_destroy(&t);
    return sec;
}

// ── string key benchmarks ────────────────────────────────────────────────────

static double run_stdmap_str(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    std::map<std::string, uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < prepop; ++i)
    {
        gen_string_key(0, i, Pattern::Sequential, kbuf, &klen);
        m.emplace(std::string(kbuf, klen), 0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        gen_string_key(prepop, i, pat, kbuf, &klen);
        m[std::string(kbuf, klen)] = 0xABABABABABABABABULL;
    }
    return elapsed_sec(t0);
}

static double run_pmrmap_str(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    std::pmr::monotonic_buffer_resource pool;
    std::pmr::map<std::pmr::string, uint64_t> m{&pool};
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < prepop; ++i)
    {
        gen_string_key(0, i, Pattern::Sequential, kbuf, &klen);
        m.emplace(std::pmr::string(kbuf, klen, &pool), 0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        gen_string_key(prepop, i, pat, kbuf, &klen);
        m[std::pmr::string(kbuf, klen, &pool)] = 0xABABABABABABABABULL;
    }
    return elapsed_sec(t0);
}

static double run_absl_btree_str(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    absl::btree_map<std::string, uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < prepop; ++i)
    {
        gen_string_key(0, i, Pattern::Sequential, kbuf, &klen);
        m.emplace(std::string(kbuf, klen), 0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        gen_string_key(prepop, i, pat, kbuf, &klen);
        m[std::string(kbuf, klen)] = 0xABABABABABABABABULL;
    }
    return elapsed_sec(t0);
}

static double run_pmr_absl_btree_str(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    using PmrBtree = absl::btree_map<std::pmr::string, uint64_t, std::less<std::pmr::string>,
                                     std::pmr::polymorphic_allocator<std::pair<const std::pmr::string, uint64_t>>>;
    std::pmr::monotonic_buffer_resource pool;
    PmrBtree m{&pool};
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < prepop; ++i)
    {
        gen_string_key(0, i, Pattern::Sequential, kbuf, &klen);
        m.emplace(std::pmr::string(kbuf, klen, &pool), 0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        gen_string_key(prepop, i, pat, kbuf, &klen);
        m[std::pmr::string(kbuf, klen, &pool)] = 0xABABABABABABABABULL;
    }
    return elapsed_sec(t0);
}

static double run_art_str(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    art_tree t;
    art_tree_init(&t);
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < prepop; ++i)
    {
        gen_string_key(0, i, Pattern::Sequential, kbuf, &klen);
        art_insert(&t, (const unsigned char*)kbuf, klen, (void*)0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        gen_string_key(prepop, i, pat, kbuf, &klen);
        art_insert(&t, (const unsigned char*)kbuf, klen, (void*)0xABABABABABABABABULL);
    }
    double sec = elapsed_sec(t0);
    art_tree_destroy(&t);
    return sec;
}

static double run_artmap_str(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    art::art_map<uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < prepop; ++i)
    {
        gen_string_key(0, i, Pattern::Sequential, kbuf, &klen);
        m.upsert(std::string_view(kbuf, klen), 0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        gen_string_key(prepop, i, pat, kbuf, &klen);
        m.upsert(std::string_view(kbuf, klen), 0xABABABABABABABABULL);
    }
    return elapsed_sec(t0);
}

static double run_artmap(uint64_t prepop, uint64_t insert_count, Pattern pat)
{
    art::art_map<uint64_t> m;
    uint8_t kbuf[8];
    for (uint64_t i = 0; i < prepop; ++i)
    {
        encode_be(i, kbuf);
        m.upsert(std::string_view((char*)kbuf, 8), 0xABABABABABABABABULL);
    }

    auto t0 = Clock::now();
    for (uint64_t i = 0; i < insert_count; ++i)
    {
        encode_be(gen_key(prepop, i, pat), kbuf);
        m.upsert(std::string_view((char*)kbuf, 8), 0xABABABABABABABABULL);
    }
    return elapsed_sec(t0);
}

// ── search benchmarks ────────────────────────────────────────────────────────

static double run_search_stdmap(uint64_t count, Pattern pat)
{
    std::map<uint64_t, uint64_t> m;
    for (uint64_t i = 0; i < count; ++i)
        m[gen_key(0, i, pat)] = 0xABABABABABABABABULL;

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        auto it = m.find(gen_key(0, i, pat));
        if (it != m.end()) sink = it->second;
    }
    return elapsed_sec(t0);
}

static double run_search_absl(uint64_t count, Pattern pat)
{
    absl::btree_map<uint64_t, uint64_t> m;
    for (uint64_t i = 0; i < count; ++i)
        m[gen_key(0, i, pat)] = 0xABABABABABABABABULL;

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        auto it = m.find(gen_key(0, i, pat));
        if (it != m.end()) sink = it->second;
    }
    return elapsed_sec(t0);
}

static double run_search_art(uint64_t count, Pattern pat)
{
    art_tree t;
    art_tree_init(&t);
    uint8_t kbuf[8];
    for (uint64_t i = 0; i < count; ++i)
    {
        encode_be(gen_key(0, i, pat), kbuf);
        art_insert(&t, kbuf, 8, (void*)0xABABABABABABABABULL);
    }

    volatile void* sink = nullptr;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        encode_be(gen_key(0, i, pat), kbuf);
        sink = art_search(&t, kbuf, 8);
    }
    double sec = elapsed_sec(t0);
    art_tree_destroy(&t);
    return sec;
}

static double run_search_art_str(uint64_t count, Pattern pat)
{
    art_tree t;
    art_tree_init(&t);
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        art_insert(&t, (const unsigned char*)kbuf, klen, (void*)0xABABABABABABABABULL);
    }

    volatile void* sink = nullptr;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        sink = art_search(&t, (const unsigned char*)kbuf, klen);
    }
    double sec = elapsed_sec(t0);
    art_tree_destroy(&t);
    return sec;
}

static double run_search_absl_str(uint64_t count, Pattern pat)
{
    absl::btree_map<std::string, uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        m[std::string(kbuf, klen)] = 0xABABABABABABABABULL;
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        auto it = m.find(std::string(kbuf, klen));
        if (it != m.end()) sink = it->second;
    }
    return elapsed_sec(t0);
}

static double run_search_stdmap_str(uint64_t count, Pattern pat)
{
    std::map<std::string, uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        m[std::string(kbuf, klen)] = 0xABABABABABABABABULL;
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        auto it = m.find(std::string(kbuf, klen));
        if (it != m.end()) sink = it->second;
    }
    return elapsed_sec(t0);
}

// ── ordered iteration benchmark ──────────────────────────────────────────────

static double run_iter_stdmap_str(uint64_t count, Pattern pat)
{
    std::map<std::string, uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        m[std::string(kbuf, klen)] = 0xABABABABABABABABULL;
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (auto& [k,v] : m) sink = v;
    return elapsed_sec(t0);
}

static double run_iter_absl_str(uint64_t count, Pattern pat)
{
    absl::btree_map<std::string, uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        m[std::string(kbuf, klen)] = 0xABABABABABABABABULL;
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (auto& [k,v] : m) sink = v;
    return elapsed_sec(t0);
}

static int art_iter_callback(void *data, const unsigned char *key, uint32_t key_len, void *value)
{
    (void)key; (void)key_len;
    *((volatile uint64_t*)data) = (uint64_t)(uintptr_t)value;
    return 0;
}

static double run_iter_art_str(uint64_t count, Pattern pat)
{
    art_tree t;
    art_tree_init(&t);
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        art_insert(&t, (const unsigned char*)kbuf, klen, (void*)0xABABABABABABABABULL);
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    art_iter(&t, art_iter_callback, (void*)&sink);
    double sec = elapsed_sec(t0);
    art_tree_destroy(&t);
    return sec;
}

static double run_search_artmap(uint64_t count, Pattern pat)
{
    art::art_map<uint64_t> m;
    uint8_t kbuf[8];
    for (uint64_t i = 0; i < count; ++i)
    {
        encode_be(gen_key(0, i, pat), kbuf);
        m.upsert(std::string_view((char*)kbuf, 8), 0xABABABABABABABABULL);
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        encode_be(gen_key(0, i, pat), kbuf);
        auto* v = m.get(std::string_view((char*)kbuf, 8));
        if (v) sink = *v;
    }
    return elapsed_sec(t0);
}

static double run_search_artmap_str(uint64_t count, Pattern pat)
{
    art::art_map<uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        m.upsert(std::string_view(kbuf, klen), 0xABABABABABABABABULL);
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        auto* v = m.get(std::string_view(kbuf, klen));
        if (v) sink = *v;
    }
    return elapsed_sec(t0);
}

static double run_iter_artmap_str(uint64_t count, Pattern pat)
{
    art::art_map<uint64_t> m;
    char kbuf[64]; int klen;
    for (uint64_t i = 0; i < count; ++i)
    {
        gen_string_key(0, i, pat, kbuf, &klen);
        m.upsert(std::string_view(kbuf, klen), 0xABABABABABABABABULL);
    }

    volatile uint64_t sink = 0;
    auto t0 = Clock::now();
    for (auto it = m.begin(); it != m.end(); ++it)
        sink = it.value();
    return elapsed_sec(t0);
}

// ── psitri ───────────────────────────────────────────────────────────────────

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

// ── print helpers ────────────────────────────────────────────────────────────

struct BenchResult { double seq_sec; double rnd_sec; };

static void print_row(const char* name, uint64_t count, BenchResult r)
{
    double seq_ns = r.seq_sec * 1e9 / count;
    double rnd_ns = r.rnd_sec * 1e9 / count;
    printf("%-24s  %12.0f  %12.1f  %12.0f  %12.1f\n",
           name,
           count / r.seq_sec, seq_ns,
           count / r.rnd_sec, rnd_ns);
}

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv)
{
    uint64_t prepop_count = 1'000'000;
    uint64_t insert_count = 1'000'000;
    if (argc >= 2) prepop_count = std::stoull(argv[1]);
    if (argc >= 3) insert_count = std::stoull(argv[2]);

    memset(g_val_buf, 0xAB, VALUE_SIZE);

    printf("=== Insert Benchmark: ART vs btree vs std::map ===\n");
    printf("  Pre-populated keys : %llu\n", (unsigned long long)prepop_count);
    printf("  Inserted keys      : %llu\n", (unsigned long long)insert_count);
    printf("  Value size         : %u bytes\n\n", VALUE_SIZE);

    // ── SECTION 1: uint64 key inserts ────────────────────────────────────────
    printf("── uint64 key inserts ──────────────────────────────────────────────\n");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "structure", "seq ops/s", "seq ns/op", "rnd ops/s", "rnd ns/op");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "------------------------", "---------", "---------", "---------", "---------");

    auto bench_insert = [&](const char* name, auto fn) {
        fprintf(stderr, "%s sequential...\n", name); fflush(stderr);
        double seq = fn(prepop_count, insert_count, Pattern::Sequential);
        fprintf(stderr, "%s random...\n", name); fflush(stderr);
        double rnd = fn(prepop_count, insert_count, Pattern::Random);
        BenchResult r{seq, rnd};
        print_row(name, insert_count, r);
        return r;
    };

    bench_insert("std::map", run_stdmap);
    bench_insert("pmr::map(monotonic)", run_pmrmap);
    bench_insert("absl::btree_map", run_absl_btree);
    auto pabsl = bench_insert("pmr absl::btree_map", run_pmr_absl_btree);
    bench_insert("libart ART", run_art);
    bench_insert("art_map", run_artmap);
    printf("\n");

    // ── SECTION 2: string key inserts ────────────────────────────────────────
    printf("── string key inserts (/db/table/row/col/<hex>) ────────────────────\n");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "structure", "seq ops/s", "seq ns/op", "rnd ops/s", "rnd ns/op");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "------------------------", "---------", "---------", "---------", "---------");

    bench_insert("std::map<string>", run_stdmap_str);
    bench_insert("pmr::map<string>", run_pmrmap_str);
    bench_insert("absl::btree<string>", run_absl_btree_str);
    bench_insert("pmr absl::btree<str>", run_pmr_absl_btree_str);
    bench_insert("libart ART (string)", run_art_str);
    bench_insert("art_map (string)", run_artmap_str);
    printf("\n");

    // ── SECTION 3: point lookup (search) ─────────────────────────────────────
    uint64_t search_count = insert_count;
    printf("── point lookup (uint64 keys, %llu items) ─────────────────────────\n",
           (unsigned long long)search_count);
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "structure", "seq ops/s", "seq ns/op", "rnd ops/s", "rnd ns/op");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "------------------------", "---------", "---------", "---------", "---------");

    auto bench_search = [&](const char* name, auto fn) {
        fprintf(stderr, "%s search seq...\n", name); fflush(stderr);
        double seq = fn(search_count, Pattern::Sequential);
        fprintf(stderr, "%s search rnd...\n", name); fflush(stderr);
        double rnd = fn(search_count, Pattern::Random);
        BenchResult r{seq, rnd};
        print_row(name, search_count, r);
    };

    bench_search("std::map", run_search_stdmap);
    bench_search("absl::btree_map", run_search_absl);
    bench_search("libart ART", run_search_art);
    bench_search("art_map", run_search_artmap);
    printf("\n");

    // ── SECTION 4: string key point lookup ───────────────────────────────────
    printf("── point lookup (string keys, %llu items) ─────────────────────────\n",
           (unsigned long long)search_count);
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "structure", "seq ops/s", "seq ns/op", "rnd ops/s", "rnd ns/op");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "------------------------", "---------", "---------", "---------", "---------");

    bench_search("std::map<string>", run_search_stdmap_str);
    bench_search("absl::btree<string>", run_search_absl_str);
    bench_search("libart ART (string)", run_search_art_str);
    bench_search("art_map (string)", run_search_artmap_str);
    printf("\n");

    // ── SECTION 5: ordered iteration (string keys) ───────────────────────────
    printf("── ordered iteration (string keys, %llu items) ────────────────────\n",
           (unsigned long long)search_count);
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "structure", "seq ops/s", "seq ns/op", "rnd ops/s", "rnd ns/op");
    printf("%-24s  %12s  %12s  %12s  %12s\n",
           "------------------------", "---------", "---------", "---------", "---------");

    // For iteration, "seq" and "rnd" refer to the insertion pattern
    // (the iteration itself is always in sorted order)
    bench_search("std::map<string>", run_iter_stdmap_str);
    bench_search("absl::btree<string>", run_iter_absl_str);
    bench_search("libart ART (string)", run_iter_art_str);
    bench_search("art_map (string)", run_iter_artmap_str);
    printf("\n");

    // ── SECTION 6: psitri by batch size ──────────────────────────────────────
    double pabsl_seq_ns = pabsl.seq_sec * 1e9 / insert_count;
    double pabsl_rnd_ns = pabsl.rnd_sec * 1e9 / insert_count;

    printf("── psitri COW by batch size ────────────────────────────────────────\n");
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
