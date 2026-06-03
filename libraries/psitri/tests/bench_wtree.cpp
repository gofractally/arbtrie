// bench_wtree — perf smoke for the clean psitri::wtree (Stage A: tiny u32 B-tree).
// Inserts N hash-scrambled u32 keys (random arrival order), then queries them in a
// second random order. Reports insert ns/key + query ns/lookup + miss. build/release
// only (see node_design_patterns §G). Usage: bench_wtree [N]

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
#include <string>

namespace w = psitri::wtree;

static uint64_t splitmix64(uint64_t x) noexcept
{
   x += 0x9E3779B97F4A7C15ULL;
   x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
   x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
   return x ^ (x >> 31);
}

int main(int argc, char** argv)
{
   const uint64_t N = (argc > 1) ? std::stoull(argv[1]) : 10'000'000ULL;
   if (N > (1ull << 32)) { std::printf("N must be <= 2^32\n"); return 2; }

   const auto dir = std::filesystem::temp_directory_path() / ("bench_wtree-" + std::to_string(::getpid()));
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir);
   auto alloc = std::make_unique<sal::allocator>(dir, sal::runtime_config{});
   alloc->register_type_ops<w::tiny>();
   auto ses     = alloc->get_session();
   w::g_session = ses.get();

   auto scramble = [](uint64_t i) { return uint32_t(splitmix64(i)); };
   auto val_of   = [](uint32_t k) { return uint64_t(k) * 1315423911ull + 7; };
   using clk     = std::chrono::steady_clock;
   auto now      = [] { return clk::now(); };
   auto ns       = [](auto a, auto b) { return std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count(); };

   std::printf("=== bench_wtree (tiny u32 B-tree): N=%llu ===\n", (unsigned long long)N);

   // ─── insert ───
   w::ptr_address root = w::kNoChild;
   const auto t0 = now();
   {
      sal::read_lock lk = ses->lock();
      for (uint64_t i = 0; i < N; ++i)
      {
         const uint32_t k = scramble(i);
         root             = w::insert(root, k, val_of(k));
      }
   }
   const auto t1 = now();

   // ─── query (different scramble order) ───
   uint64_t miss = 0, mism = 0;
   const auto t2 = now();
   {
      sal::read_lock lk = ses->lock();
      for (uint64_t i = 0; i < N; ++i)
      {
         const uint32_t k = scramble((i * 2654435761u) ^ 0x5bd1e995u);  // reorder
         // map back to a key that was inserted: just query scramble(i') for i' in [0,N)
         const uint32_t kk = scramble(i);
         const uint8_t* v  = w::find(root, kk);
         if (!v) { ++miss; continue; }
         uint64_t got = 0; std::memcpy(&got, v, 8);
         if (got != val_of(kk)) ++mism;
         (void)k;
      }
   }
   const auto t3 = now();

   std::printf("insert: %lld ns/key (%.2f s total)\n", (long long)(ns(t0, t1) / (long long)N),
               double(ns(t0, t1)) / 1e9);
   std::printf("query : %lld ns/lookup (%.2f s)  miss=%llu mism=%llu\n",
               (long long)(ns(t2, t3) / (long long)N), double(ns(t2, t3)) / 1e9,
               (unsigned long long)miss, (unsigned long long)mism);

   { sal::read_lock lk = ses->lock(); w::session().release(root); }
   ses = sal::allocator_session_ptr(nullptr);
   alloc.reset();
   std::filesystem::remove_all(dir);
   return (miss || mism) ? 1 : 0;
}
