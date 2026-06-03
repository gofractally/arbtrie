// wtree_tests — small correctness + leak/refcount tests for psitri::wtree.
//
// Mirrors the psitri leak-check idiom (fuzz_tests.cpp): after dropping the
// tree, wait for the compactor to drain pending releases, then REQUIRE that
// the session reports ZERO live SAL objects. A leak (missing release) leaves
// live objects > 0; an over-release would corrupt and trip the value checks.

#include <catch2/catch_all.hpp>

#include <psitri/wtree/wtree.hpp>
#include <psitri/wtree2/node.hpp>   // wtree2 rewrite (Phase 1: node record + layout)
#include <psitri/wtree2/insert.hpp> // wtree2 incremental insert (Phase 4)

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

using psitri::wtree::wtree;

namespace
{
   uint64_t splitmix64(uint64_t x) noexcept
   {
      x += 0x9E3779B97F4A7C15ULL;
      x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
      x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
      return x ^ (x >> 31);
   }

   // Deterministic 24-byte key from index i (3 u64 hashes).
   void make_key24(uint64_t i, uint8_t out[24]) noexcept
   {
      const uint64_t h0 = splitmix64(i ^ 0xA1B2C3D4ULL);
      const uint64_t h1 = splitmix64(i ^ 0xDEADBEEFULL);
      const uint64_t h2 = splitmix64(i ^ 0x12345678ULL);
      std::memcpy(out + 0, &h0, 8);
      std::memcpy(out + 8, &h1, 8);
      std::memcpy(out + 16, &h2, 8);
   }

   std::span<const uint8_t> kview(const uint8_t* k, size_t n)
   {
      return std::span<const uint8_t>(k, n);
   }
   std::span<const uint8_t> vview(const uint64_t& v)
   {
      return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&v), 8);
   }

   // RAII tmp dir + sal::allocator + session, with the wtree node types
   // registered. Background threads (compactor) ON by default.
   struct sal_fixture
   {
      std::filesystem::path        dir;
      std::unique_ptr<sal::allocator> alloc;
      sal::allocator_session_ptr   ses;

      explicit sal_fixture(const std::string& name)
          : dir(std::filesystem::temp_directory_path() /
                ("wtree_tests-" + name + "-" + std::to_string(::getpid()))),
            ses(nullptr)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir);
         alloc = std::make_unique<sal::allocator>(dir, sal::runtime_config{});
         alloc->register_type_ops<psitri::wtree::leaf_node>();
         alloc->register_type_ops<psitri::wtree::inner_node>();
         ses = alloc->get_session();
      }
      ~sal_fixture()
      {
         ses = sal::allocator_session_ptr(nullptr);
         alloc.reset();
         std::error_code ec;
         std::filesystem::remove_all(dir, ec);
      }

      // Poll until the compactor has drained all pending releases (mirrors
      // database::wait_for_compactor, which my raw-allocator setup lacks).
      void wait_for_compactor(int timeout_ms = 10000)
      {
         using namespace std::chrono;
         auto deadline = steady_clock::now() + milliseconds(timeout_ms);
         while (steady_clock::now() < deadline)
         {
            if (alloc->total_pending_releases() == 0)
            {
               std::this_thread::sleep_for(milliseconds(50));
               if (alloc->total_pending_releases() == 0)
                  return;
            }
            std::this_thread::sleep_for(milliseconds(10));
         }
      }

      uint64_t live_objects() const { return ses->get_total_allocated_objects(); }
   };
}  // namespace

// ── Correctness ────────────────────────────────────────────────────

TEST_CASE("wtree empty find misses", "[wtree]")
{
   sal_fixture fx("empty");
   wtree       t(*fx.ses, 8);
   uint8_t     k[24];
   make_key24(0, k);
   REQUIRE(t.find(kview(k, 24)).empty());
}

TEST_CASE("wtree single insert + find", "[wtree]")
{
   sal_fixture fx("single");
   wtree       t(*fx.ses, 8);
   uint8_t     k[24];
   make_key24(42, k);
   const uint64_t v = 0xCAFEBABEull;
   t.upsert(kview(k, 24), vview(v));
   auto got = t.find(kview(k, 24));
   REQUIRE(got.size() == 8);
   uint64_t out;
   std::memcpy(&out, got.data(), 8);
   REQUIRE(out == v);
}

TEST_CASE("wtree insert N round-trips every key", "[wtree]")
{
   const uint64_t N = GENERATE(uint64_t(100), uint64_t(100'000), uint64_t(1'000'000));
   sal_fixture    fx("roundtrip-" + std::to_string(N));
   wtree          t(*fx.ses, 8);

   for (uint64_t i = 0; i < N; ++i)
   {
      uint8_t k[24];
      make_key24(i, k);
      const uint64_t v = i + 1;
      t.upsert(kview(k, 24), vview(v));
   }

   size_t miss = 0, mismatch = 0;
   for (uint64_t i = 0; i < N; ++i)
   {
      uint8_t k[24];
      make_key24(i, k);
      auto got = t.find(kview(k, 24));
      if (got.empty()) { ++miss; continue; }
      uint64_t out;
      std::memcpy(&out, got.data(), 8);
      if (out != i + 1) ++mismatch;
   }
   REQUIRE(miss == 0);
   REQUIRE(mismatch == 0);

   // live SAL objects must equal the reachable tree (no orphans). Drain
   // the compactor first so in-flight (queued) releases don't inflate the
   // count — get_total_allocated_objects() counts CBs still pending release.
   fx.wait_for_compactor();
   auto pc = t.count_pages();
   REQUIRE(fx.live_objects() == uint64_t(pc.leaves) + uint64_t(pc.inners));
}

TEST_CASE("wtree update existing key replaces value, no growth", "[wtree]")
{
   sal_fixture fx("update");
   wtree       t(*fx.ses, 8);
   const uint64_t N = 10'000;
   for (uint64_t i = 0; i < N; ++i)
   {
      uint8_t k[24]; make_key24(i, k);
      const uint64_t v = i + 1;
      t.upsert(kview(k, 24), vview(v));
   }
   const auto pc_before = t.count_pages();

   // Rewrite every key with a new value.
   for (uint64_t i = 0; i < N; ++i)
   {
      uint8_t k[24]; make_key24(i, k);
      const uint64_t v = i + 1000000;
      t.upsert(kview(k, 24), vview(v));
   }
   const auto pc_after = t.count_pages();
   REQUIRE(pc_after.leaves == pc_before.leaves);   // same key set → same shape

   for (uint64_t i = 0; i < N; ++i)
   {
      uint8_t k[24]; make_key24(i, k);
      auto got = t.find(kview(k, 24));
      REQUIRE(got.size() == 8);
      uint64_t out; std::memcpy(&out, got.data(), 8);
      REQUIRE(out == i + 1000000);
   }
}

// ── apply()/op_insert: templated splice (in-place + COW) ───────────
TEST_CASE("wtree inner op_insert splice", "[wtree][apply]")
{
   namespace pw = psitri::wtree;
   sal_fixture fx("op_insert");
   pw::g_wtree_session = fx.ses.get();   // the node free-fns read this

   const bool cow = GENERATE(false, true);   // exercise in-place AND copy-on-write
   INFO("cow=" << cow);

   // Build a synthetic inner: stems 10,20,30,40 ; children addr 100..104.
   pw::inner_node* in = pw::inner_alloc_sized(8);
   in->num_keys = 4;
   uint32_t* st = pw::inner_stems(in);
   for (uint16_t i = 0; i < 4; ++i) st[i] = uint32_t((i + 1) * 10);
   for (uint16_t i = 0; i < 5; ++i)
      pw::inner_set_child_addr(in, i, pw::ptr_address(100 + i));
   const pw::ptr_address addr = in->address();

   if (cow)
      fx.ses->sync(sal::sync_type::msync_async);   // make the segment read-only → forces COW

   // Insert divider=25 at pos=2 (child at children[3]) = ptr_address(999).
   pw::op_insert op{};
   op.pos = 2; op.divider = 25; op.child = pw::ptr_address(999);
   auto        ref = fx.ses->get_ref<pw::inner_node>(addr);
   pw::ptr_address r = pw::apply<psitri::upsert_mode::unique_upsert>(ref, op);
   REQUIRE(r != pw::kNoChild);
   pw::inner_node* res = pw::wderef<pw::inner_node>(r);

   REQUIRE(res->num_keys == 5);
   const uint32_t  want_stems[5]    = {10, 20, 25, 30, 40};
   const uint32_t  want_children[6] = {100, 101, 102, 999, 103, 104};
   const uint32_t* rs = pw::inner_stems(res);
   for (uint16_t i = 0; i < 5; ++i) REQUIRE(rs[i] == want_stems[i]);
   for (uint16_t i = 0; i < 6; ++i)
      REQUIRE(*pw::inner_child_addr(res, i) == want_children[i]);
}

// ── dead_bytes counter accuracy (vs reachable-heap walk) ───────────
TEST_CASE("wtree leaf dead_bytes tracks abandoned heap", "[wtree][dead]")
{
   namespace pw = psitri::wtree;
   sal_fixture fx("dead");
   pw::g_wtree_session = fx.ses.get();

   // Invariant: dead_bytes == tail_alloc_pos - reachable-heap bytes.
   // The counter must always equal tail_alloc_pos minus the reachable heap.
   auto check = [&](pw::leaf_node* lf) {
      const uint32_t live = pw::leaf_live_bytes(lf, 8);
      REQUIRE(uint32_t(lf->tail_alloc_pos) >= live);
      REQUIRE(uint32_t(lf->dead_bytes) == uint32_t(lf->tail_alloc_pos) - live);
   };

   auto make_words = [](uint32_t i, uint32_t w[4]) {
      switch (i % 3)
      {
         case 0:  w[0] = 0xAAAA; w[1] = i;      w[2] = 7;      w[3] = 9; break;  // share w0
         case 1:  w[0] = 0xBBBB; w[1] = 0xCCCC; w[2] = i;      w[3] = 5; break;  // share w0,w1
         default: w[0] = 0xBBBB; w[1] = 0xCCCC; w[2] = 8;      w[3] = i; break;  // share w0,w1,w2 (path)
      }
   };

   pw::leaf_node* lf = pw::leaf_alloc();
   pw::leaf_init(lf);
   REQUIRE(lf->dead_bytes == 0);

   uint32_t inserted = 0;
   for (uint32_t i = 0; i < 300; ++i)
   {
      uint32_t w[4]; make_words(i, w);
      const uint64_t v = i;
      if (!pw::leaf_insert(lf, pw::word_key_type(w, 4), 0, vview(v))) break;
      ++inserted;
      check(lf);
   }
   REQUIRE(inserted > 50);              // exercised real collisions/radix growth
   REQUIRE(lf->dead_bytes > 0);         // abandonment did happen

   // All built keys must read back (deep-radix correctness).
   for (uint32_t i = 0; i < inserted; ++i)
   {
      uint32_t w[4]; make_words(i, w);
      auto got = pw::leaf_find(lf, pw::word_key_type(w, 4), 0, 8);
      REQUIRE(got.data() != nullptr);
      uint64_t out; std::memcpy(&out, got.data(), 8);
      REQUIRE(out == uint64_t(i));
   }
}

// Deep-radix in-place value update must not corrupt the leaf.
TEST_CASE("wtree deep-radix value update", "[wtree][radix-update]")
{
   namespace pw = psitri::wtree;
   sal_fixture fx("radix-update");
   pw::g_wtree_session = fx.ses.get();
   pw::leaf_node* lf = pw::leaf_alloc();
   pw::leaf_init(lf);

   auto key_of = [](uint32_t i, uint32_t w[4]) { w[0]=0xAAAA; w[1]=i; w[2]=7; w[3]=9; };
   auto find_v = [&](uint32_t i) -> uint64_t {
      uint32_t w[4]; key_of(i, w);
      auto got = pw::leaf_find(lf, pw::word_key_type(w, 4), 0, 8);
      REQUIRE(got.data() != nullptr);
      uint64_t out; std::memcpy(&out, got.data(), 8); return out;
   };

   // Build colliding keys (shared first word → deep in-leaf radix), tolerate full.
   uint32_t n = 0;
   for (; n < 200; ++n)
   {
      uint32_t w[4]; key_of(n, w);
      const uint64_t v = n;
      if (!pw::leaf_insert(lf, pw::word_key_type(w, 4), 0, vview(v))) break;
   }
   REQUIRE(n > 20);
   for (uint32_t i = 0; i < n; ++i) REQUIRE(find_v(i) == i);   // build correct

   // Update every key in place (new value), verifying ALL keys after each
   // update — a corruption shows up as a neighbour reading wrong.
   for (uint32_t i = 0; i < n; ++i)
   {
      uint32_t w[4]; key_of(i, w);
      const uint64_t v = uint64_t(i) + 1'000'000;
      REQUIRE(pw::leaf_insert(lf, pw::word_key_type(w, 4), 0, vview(v)));
      for (uint32_t j = 0; j < n; ++j)
      {
         const uint64_t want = (j <= i) ? (uint64_t(j) + 1'000'000) : uint64_t(j);
         INFO("after updating i=" << i << " key j=" << j);
         REQUIRE(find_v(j) == want);
      }
   }
}

// Deep-radix (multi-family, path-compressed) updates must keep find correct
// AND the dead_bytes invariant intact. Build WELL UNDER full so no failed
// (partial-mutation) insert is involved.
TEST_CASE("wtree deep-radix update correctness", "[wtree][radix-update]")
{
   namespace pw = psitri::wtree;
   sal_fixture fx("deep-upd");
   pw::g_wtree_session = fx.ses.get();
   pw::leaf_node* lf = pw::leaf_alloc();
   pw::leaf_init(lf);

   auto key_of = [](uint32_t i, uint32_t w[4]) {
      switch (i % 3) {
         case 0:  w[0]=0xAAAA; w[1]=i;      w[2]=7; w[3]=9; break;
         case 1:  w[0]=0xBBBB; w[1]=0xCCCC; w[2]=i; w[3]=5; break;
         default: w[0]=0xBBBB; w[1]=0xCCCC; w[2]=8; w[3]=i; break;   // path-compressed
      }
   };
   auto find_v = [&](uint32_t i) -> uint64_t {
      uint32_t w[4]; key_of(i, w);
      auto got = pw::leaf_find(lf, pw::word_key_type(w, 4), 0, 8);
      REQUIRE(got.data() != nullptr);
      uint64_t out; std::memcpy(&out, got.data(), 8); return out;
   };
   auto inv_ok = [&] {
      const uint32_t live = pw::leaf_live_bytes(lf, 8);
      return uint32_t(lf->dead_bytes) == uint32_t(lf->tail_alloc_pos) - live;
   };

   const uint32_t N = 24;   // deep radix, comfortably under a full 4 KB leaf
   for (uint32_t i = 0; i < N; ++i) {
      uint32_t w[4]; key_of(i, w);
      REQUIRE(pw::leaf_insert(lf, pw::word_key_type(w, 4), 0, vview(uint64_t(i))));
      REQUIRE(inv_ok());
   }
   for (uint32_t i = 0; i < N; ++i) REQUIRE(find_v(i) == i);

   // Update every key in place; verify ALL keys + the invariant after each.
   for (uint32_t i = 0; i < N; ++i) {
      uint32_t w[4]; key_of(i, w);
      REQUIRE(pw::leaf_insert(lf, pw::word_key_type(w, 4), 0, vview(uint64_t(i) + 1'000'000)));
      for (uint32_t j = 0; j < N; ++j) {
         INFO("after update i=" << i << " key j=" << j);
         REQUIRE(find_v(j) == ((j <= i) ? uint64_t(j) + 1'000'000 : uint64_t(j)));
      }
      INFO("invariant after update i=" << i);
      REQUIRE(inv_ok());
   }
}

// Force INNER-node splits (rare until a tree is huge): many keys sharing the
// first word collapse onto one root child, whose inner overflows max page and
// must B-tree-split repeatedly. Exercises replace_slot_'s split path +
// write_half_ + bubble-up at unit scale.
TEST_CASE("wtree inner split under one prefix", "[wtree][inner-split]")
{
   const bool sync = GENERATE(false, true);
   INFO("sync=" << sync);
   sal_fixture fx(std::string("inner-split-") + (sync ? "s" : "n"));
   wtree       t(*fx.ses, 8);

   // ~8175 children max per inner, ~150 keys/leaf → need >1.2M keys under one
   // prefix to actually B-tree-split the inner (and bubble up to split root).
   const uint32_t N = 1'500'000;
   auto mk = [](uint32_t i, uint8_t k[16]) {
      const uint32_t w0 = 0x11111111u;   // constant → one root child
      std::memcpy(k + 0, &w0, 4);
      std::memcpy(k + 4, &i, 4);         // distinct → wide inner that overflows
      std::memcpy(k + 8, &i, 4);
      std::memcpy(k + 12, &i, 4);
   };
   for (uint32_t i = 0; i < N; ++i)
   {
      uint8_t k[16]; mk(i, k);
      const uint64_t v = i;
      t.upsert(kview(k, 16), vview(v));
      if (sync && (i % 4096 == 0)) fx.ses->sync(sal::sync_type::msync_async);
   }
   size_t miss = 0, mismatch = 0;
   for (uint32_t i = 0; i < N; ++i)
   {
      uint8_t k[16]; mk(i, k);
      auto got = t.find(kview(k, 16));
      if (got.empty()) { ++miss; continue; }
      uint64_t out; std::memcpy(&out, got.data(), 8);
      if (out != i) ++mismatch;
   }
   REQUIRE(miss == 0);
   REQUIRE(mismatch == 0);
}

// ── remove: value + radix, tombstone+reuse, leak, not-found ────────
TEST_CASE("wtree remove", "[wtree][remove]")
{
   const bool sync = GENERATE(false, true);
   INFO("sync=" << sync);
   sal_fixture fx(std::string("remove-") + (sync ? "s" : "n"));
   wtree       t(*fx.ses, 8);

   const uint32_t N = 60'000;
   // Cluster: w0 group (wide inners), w1 collides within a leaf (→ in-leaf
   // radix), w2/w3 distinct → exercises both value_region and radix removal.
   auto mk = [](uint32_t i, uint8_t k[16]) {
      const uint32_t w0 = i % 64, w1 = i % 2048, w2 = i, w3 = i;
      std::memcpy(k + 0, &w0, 4); std::memcpy(k + 4, &w1, 4);
      std::memcpy(k + 8, &w2, 4); std::memcpy(k + 12, &w3, 4);
   };

   for (uint32_t i = 0; i < N; ++i) {
      uint8_t k[16]; mk(i, k);
      t.upsert(kview(k, 16), vview(uint64_t(i)));
      if (sync && (i % 2048 == 0)) fx.ses->sync(sal::sync_type::msync_async);
   }
   // Remove evens; verify evens absent, odds intact.
   for (uint32_t i = 0; i < N; i += 2) {
      uint8_t k[16]; mk(i, k);
      REQUIRE(t.remove(kview(k, 16)));
      if (sync && (i % 4096 == 0)) fx.ses->sync(sal::sync_type::msync_async);
   }
   for (uint32_t i = 0; i < N; ++i) {
      uint8_t k[16]; mk(i, k);
      auto got = t.find(kview(k, 16));
      if (i % 2 == 0) { REQUIRE(got.empty()); }
      else { REQUIRE(got.size() == 8); uint64_t o; std::memcpy(&o, got.data(), 8); REQUIRE(o == i); }
   }
   // Double-remove an already-removed key → false.
   { uint8_t k[16]; mk(0, k); REQUIRE_FALSE(t.remove(kview(k, 16))); }

   // Remove the odds → tree empty.
   for (uint32_t i = 1; i < N; i += 2) { uint8_t k[16]; mk(i, k); REQUIRE(t.remove(kview(k, 16))); }
   for (uint32_t i = 0; i < N; ++i) { uint8_t k[16]; mk(i, k); REQUIRE(t.find(kview(k, 16)).empty()); }

   // Re-insert everything (reuses tombstoned slots) → all present.
   for (uint32_t i = 0; i < N; ++i) { uint8_t k[16]; mk(i, k); t.upsert(kview(k, 16), vview(uint64_t(i) + 7)); }
   for (uint32_t i = 0; i < N; ++i) {
      uint8_t k[16]; mk(i, k);
      auto got = t.find(kview(k, 16));
      REQUIRE(got.size() == 8); uint64_t o; std::memcpy(&o, got.data(), 8); REQUIRE(o == uint64_t(i) + 7);
   }

   fx.wait_for_compactor();
   auto pc = t.count_pages();
   REQUIRE(fx.live_objects() == uint64_t(pc.leaves) + uint64_t(pc.inners));
}

// remove must MERGE underfull leaves: after deleting most keys, the leaf
// count should collapse (not just tombstone empties).
TEST_CASE("wtree remove merges underfull leaves", "[wtree][remove][merge]")
{
   sal_fixture fx("merge");
   wtree       t(*fx.ses, 8);
   const uint32_t N = 50'000;
   auto mk = [](uint32_t i, uint8_t k[16]) {
      const uint32_t c = 0x55555555u;       // shared tail → keys differ only in word0
      std::memcpy(k + 0, &i, 4); std::memcpy(k + 4, &c, 4);
      std::memcpy(k + 8, &c, 4); std::memcpy(k + 12, &c, 4);
   };
   for (uint32_t i = 0; i < N; ++i) { uint8_t k[16]; mk(i, k); t.upsert(kview(k, 16), vview(uint64_t(i))); }
   const size_t leaves_full = t.count_pages().leaves;

   // Delete 90% (keep every 10th).
   for (uint32_t i = 0; i < N; ++i)
      if (i % 10 != 0) { uint8_t k[16]; mk(i, k); REQUIRE(t.remove(kview(k, 16))); }

   const size_t leaves_sparse = t.count_pages().leaves;
   INFO("leaves_full=" << leaves_full << " leaves_sparse=" << leaves_sparse);
   REQUIRE(leaves_full > 10);
   REQUIRE(leaves_sparse * 2 < leaves_full);   // merge collapsed the underfull leaves

   // Survivors still correct.
   for (uint32_t i = 0; i < N; i += 10) {
      uint8_t k[16]; mk(i, k);
      auto got = t.find(kview(k, 16));
      REQUIRE(got.size() == 8); uint64_t o; std::memcpy(&o, got.data(), 8); REQUIRE(o == i);
   }
   fx.wait_for_compactor();
   auto pc = t.count_pages();
   REQUIRE(fx.live_objects() == uint64_t(pc.leaves) + uint64_t(pc.inners));
}

// Tree must shrink back toward a single leaf as it depopulates: removing
// all but one key collapses the inner spine (leaf merge + single-child inner
// collapse); removing the last key empties the tree.
TEST_CASE("wtree shrinks to a single leaf", "[wtree][shrink]")
{
   const bool sync = GENERATE(false, true);
   INFO("sync=" << sync);
   sal_fixture fx(std::string("shrink-") + (sync ? "s" : "n"));
   wtree       t(*fx.ses, 8);
   const uint32_t N = 30'000;
   auto mk = [](uint32_t i, uint8_t k[16]) {
      const uint32_t c = 0x9abcdef0u;
      std::memcpy(k + 0, &i, 4); std::memcpy(k + 4, &c, 4);
      std::memcpy(k + 8, &c, 4); std::memcpy(k + 12, &c, 4);
   };
   for (uint32_t i = 0; i < N; ++i) { uint8_t k[16]; mk(i, k); t.upsert(kview(k, 16), vview(uint64_t(i))); }
   REQUIRE(t.count_pages().inners > 0);   // grew into a multi-level tree

   // Remove all but key 0.
   for (uint32_t i = 1; i < N; ++i) {
      uint8_t k[16]; mk(i, k); REQUIRE(t.remove(kview(k, 16)));
      if (sync && (i % 2048 == 0)) fx.ses->sync(sal::sync_type::msync_async);
   }
   auto pc = t.count_pages();
   INFO("after bulk delete: leaves=" << pc.leaves << " inners=" << pc.inners);
   REQUIRE(pc.inners == 0);   // collapsed back to a single leaf
   REQUIRE(pc.leaves == 1);
   { uint8_t k[16]; mk(0, k); auto g = t.find(kview(k, 16)); REQUIRE(g.size() == 8);
     uint64_t o; std::memcpy(&o, g.data(), 8); REQUIRE(o == 0); }

   // Remove the last key → empty tree.
   { uint8_t k[16]; mk(0, k); REQUIRE(t.remove(kview(k, 16))); }
   { uint8_t k[16]; mk(0, k); REQUIRE(t.find(kview(k, 16)).empty()); }
   fx.wait_for_compactor();
   REQUIRE(t.count_pages().leaves == 0);
   REQUIRE(fx.live_objects() == 0);

   // Re-insert one → single leaf, single value (initial state).
   { uint8_t k[16]; mk(42, k); t.upsert(kview(k, 16), vview(uint64_t(42))); }
   auto pc2 = t.count_pages();
   REQUIRE(pc2.leaves == 1);
   REQUIRE(pc2.inners == 0);
}

// ── Randomized fuzz/stress: insert/update + random sync + teardown ──
// Mixes new inserts and in-place updates over CLUSTERED keys (shared prefix
// words → radix collisions, promotes, leaf/inner splits), with random msync
// (exercises COW of read-only segments + compaction + variable-size leaves),
// spot-checks against a model, then full-verifies + leak-checks. Multi-seed.
// This is the class of coverage that caught the teardown SIGBUS.
TEST_CASE("wtree fuzz insert/update/sync", "[wtree][fuzz]")
{
   const uint64_t seed = GENERATE(uint64_t(1), uint64_t(7), uint64_t(1337));
   INFO("seed=" << seed);
   sal_fixture fx("fuzz-" + std::to_string(seed));
   wtree       t(*fx.ses, 8);

   using K = std::array<uint8_t, 16>;
   std::map<K, uint64_t> model;
   std::vector<K>        pool;
   uint64_t              rng = seed * 0x9E3779B97F4A7C15ULL + 1;
   auto next = [&]() { rng = splitmix64(rng); return rng; };

   const uint32_t OPS = 200'000;
   const uint32_t G   = 48;       // prefix groups → clustering across leaves/inners
   for (uint32_t op = 0; op < OPS; ++op)
   {
      const uint32_t roll = uint32_t(next() % 100);
      if (!pool.empty() && roll < 30)            // 30% REMOVE a pooled key
      {
         const K& k = pool[next() % pool.size()];
         const bool present = model.count(k) != 0;
         const bool removed = t.remove(kview(k.data(), 16));
         REQUIRE(removed == present);            // remove iff currently present
         model.erase(k);
      }
      else if (!pool.empty() && roll < 60)       // 30% in-place UPDATE of a live key
      {
         const K& k = pool[next() % pool.size()];
         const uint64_t v = next();
         t.upsert(kview(k.data(), 16), vview(v));
         model[k] = v;                           // re-inserts if it had been removed
      }
      else                                       // 40% INSERT a fresh key
      {
         K k;
         const uint32_t w0 = uint32_t(next() % G);
         const uint32_t w1 = uint32_t(next() % 8192);
         const uint32_t w2 = uint32_t(next());
         const uint32_t w3 = uint32_t(next());
         std::memcpy(k.data() + 0, &w0, 4); std::memcpy(k.data() + 4, &w1, 4);
         std::memcpy(k.data() + 8, &w2, 4); std::memcpy(k.data() + 12, &w3, 4);
         const uint64_t v = next();
         t.upsert(kview(k.data(), 16), vview(v));
         model[k] = v;
         pool.push_back(k);
      }

      if ((next() % 64) == 0) fx.ses->sync(sal::sync_type::msync_async);

      if ((op % 1009) == 0 && !pool.empty())   // spot-check a pooled key (live or removed)
      {
         const K& sk = pool[next() % pool.size()];
         auto got = t.find(kview(sk.data(), 16));
         auto it  = model.find(sk);
         if (it == model.end()) { REQUIRE(got.empty()); }
         else { REQUIRE(got.size() == 8); uint64_t out; std::memcpy(&out, got.data(), 8);
                REQUIRE(out == it->second); }
      }
   }

   // Full verification: every live key present+correct.
   size_t miss = 0, mismatch = 0;
   for (const auto& [k, v] : model)
   {
      auto got = t.find(kview(k.data(), 16));
      if (got.empty()) { ++miss; continue; }
      uint64_t out; std::memcpy(&out, got.data(), 8);
      if (out != v) ++mismatch;
   }
   // ...and every removed key (pooled but not live) absent.
   size_t ghost = 0;
   for (const K& k : pool)
      if (!model.count(k) && !t.find(kview(k.data(), 16)).empty()) ++ghost;
   INFO("seed=" << seed << " live=" << model.size() << " pooled=" << pool.size());
   REQUIRE(miss == 0);
   REQUIRE(mismatch == 0);
   REQUIRE(ghost == 0);

   // Reachable == live (no orphan/leak) before teardown.
   fx.wait_for_compactor();
   auto pc = t.count_pages();
   REQUIRE(fx.live_objects() == uint64_t(pc.leaves) + uint64_t(pc.inners));
   // (tree teardown via ~wtree exercises the release-cascade on synced nodes)
}

// ── "zip": grow (add 2 / remove 1) then shrink (remove 2 / add 1) over many
// cycles, sweeping every structural transition up and down, checking the
// tree's invariants after EVERY operation. Invariants:
//   (1) correctness   — the just-touched key reads back right (present/absent)
//   (2) tree shape    — no node reachable twice; stems strictly ascending in
//                       every node; dead_bytes == tail - reachable heap
//   (3) delta=1       — depth-advanced (delta=1) slots have non-leaf children
//   (4) model match   — periodic full sweep: every live key present+correct
//   (5) shrink        — drained to few keys ⇒ a single leaf (no inners)
//   (6) no leak       — at the end, live SAL objects == 0 after drop
TEST_CASE("wtree zip up/down invariants", "[wtree][zip]")
{
   const uint64_t seed = GENERATE(uint64_t(1), uint64_t(0xC0FFEE));
   INFO("seed=" << seed);
   sal_fixture fx("zip-" + std::to_string(seed));
   wtree       t(*fx.ses, 8);

   auto key16 = [&](uint32_t idx, uint8_t k[16]) {
      const uint64_t a = splitmix64(uint64_t(idx) ^ (seed * 0x100000001B3ULL));
      const uint64_t b = splitmix64(a ^ 0x5bd1e995ULL);
      std::memcpy(k, &a, 8); std::memcpy(k + 8, &b, 8);
   };
   std::set<uint32_t> live;
   uint32_t           next = 0;
   bool               sync_on = false;

   auto check = [&]() {
      auto sr = t.check_structure();
      REQUIRE(sr.tree);        // not a DAG
      REQUIRE(sr.sorted);      // stems ordered everywhere
      REQUIRE(sr.dead_ok);     // dead_bytes accounting exact
      auto ir = t.verify_invariant();
      REQUIRE(ir.leaf_violations == 0);   // delta=1 slots are depth-advanced
   };
   auto ins = [&]() {
      const uint32_t i = next++;
      uint8_t k[16]; key16(i, k);
      t.upsert(kview(k, 16), vview(uint64_t(i)));
      live.insert(i);
      auto g = t.find(kview(k, 16));
      REQUIRE(g.size() == 8); uint64_t o; std::memcpy(&o, g.data(), 8); REQUIRE(o == i);
      check();
   };
   auto rem = [&]() {
      if (live.empty()) return;
      const uint32_t i = *live.begin();
      uint8_t k[16]; key16(i, k);
      REQUIRE(t.remove(kview(k, 16)));
      live.erase(i);
      REQUIRE(t.find(kview(k, 16)).empty());
      check();
   };
   auto full_verify = [&]() {
      for (uint32_t i : live) {
         uint8_t k[16]; key16(i, k);
         auto g = t.find(kview(k, 16));
         REQUIRE(g.size() == 8); uint64_t o; std::memcpy(&o, g.data(), 8); REQUIRE(o == i);
      }
   };

   const uint32_t TARGET = 2000;
   for (int sweep = 0; sweep < 4; ++sweep)
   {
      sync_on = (sweep % 2 == 1);
      // UP: add 2, remove 1  (net +1) until TARGET
      uint32_t op = 0;
      while (live.size() < TARGET) {
         ins(); ins(); rem();
         if (sync_on && (++op % 97 == 0)) fx.ses->sync(sal::sync_type::msync_async);
         if (op % 200 == 0) full_verify();
      }
      // DOWN: remove 2, add 1  (net -1) until nearly empty, then drain
      while (live.size() > 4) {
         rem(); rem(); ins();
         if (sync_on && (++op % 97 == 0)) fx.ses->sync(sal::sync_type::msync_async);
         if (op % 200 == 0) full_verify();
      }
      while (!live.empty()) rem();
      // (5) drained ⇒ collapsed to a single leaf (or empty)
      auto pc = t.count_pages();
      REQUIRE(pc.inners == 0);
      REQUIRE(pc.leaves <= 1);
      full_verify();   // (live is empty here — trivially true; also checks no ghosts indirectly)
   }

   // (6) no leak after drop.
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == 0);
}

// ── Leak / refcount checks (the point of this file) ────────────────

TEST_CASE("wtree no leak after drop", "[wtree][leak]")
{
   const uint64_t N = GENERATE(uint64_t(1'000), uint64_t(200'000), uint64_t(1'000'000));
   sal_fixture    fx("leak-" + std::to_string(N));

   {
      wtree t(*fx.ses, 8);
      for (uint64_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         const uint64_t v = i + 1;
         t.upsert(kview(k, 24), vview(v));
      }
      // sanity: tree built and balanced before drop (drain pending first)
      fx.wait_for_compactor();
      auto pc = t.count_pages();
      REQUIRE(fx.live_objects() == uint64_t(pc.leaves) + uint64_t(pc.inners));
   }  // wtree dtor releases the whole tree

   fx.wait_for_compactor();
   INFO("N=" << N << " still-live objects=" << fx.live_objects());
   REQUIRE(fx.live_objects() == 0);
}

// ── Varlen / prefix-key correctness (the terminal_off concern) ─────
//
// encode_key appends a terminator chunk to every key, so NO encoded key is a
// word-prefix of another — the terminal_off gap is unreachable through the
// varlen API. This test proves it: a key set built entirely from prefix chains
// of random base strings must ALL be storable (zero rejects) and round-trip,
// and updates/removes must never corrupt survivors.
TEST_CASE("wtree varlen prefix-chain keys round-trip + no corruption",
          "[wtree][varlen][correctness]")
{
   sal_fixture                     fx("varlen-prefix");
   wtree                           t(*fx.ses, 8);
   std::map<std::string, uint64_t> model;

   auto val_for = [](const std::string& k) {
      uint64_t h = 0xABCDEFull;
      for (unsigned char c : k) h = splitmix64(h ^ c);
      return h | 1ull;
   };
   auto raw = [](const std::string& k) {
      return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size());
   };
   auto read_v = [](std::span<const uint8_t> vv) {
      uint64_t out = 0; if (vv.size() == 8) std::memcpy(&out, vv.data(), 8); return out;
   };

   // Adversarial set: every prefix of many random base strings, so countless
   // keys are byte-prefixes of others. Plus the empty key.
   std::vector<std::string> keys;
   keys.push_back("");
   uint64_t rng = 0x1234ull;
   for (int base = 0; base < 60; ++base) {
      const int   L = 1 + int((rng = splitmix64(rng)) % 40);
      std::string s;
      for (int j = 0; j < L; ++j) { rng = splitmix64(rng); s.push_back(char(rng & 0xFF)); }
      for (int p = 1; p <= L; ++p) keys.push_back(s.substr(0, size_t(p)));   // ALL prefixes
   }
   std::sort(keys.begin(), keys.end());
   keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

   uint64_t rejected = 0;
   for (const auto& k : keys) {
      const uint64_t v  = val_for(k);
      const bool     ok = t.upsert_varlen(raw(k), vview(v));
      if (!ok) { ++rejected; continue; }
      model[k] = v;
      REQUIRE(read_v(t.find_varlen(raw(k))) == v);   // accepted ⇒ immediately findable
   }
   INFO("varlen rejected=" << rejected << " of " << keys.size());
   REQUIRE(rejected == 0);                            // varlen prefixes are ALWAYS storable

   auto full_sweep = [&] {
      for (const auto& [k, v] : model) REQUIRE(read_v(t.find_varlen(raw(k))) == v);
   };
   full_sweep();

   // Updates: rewrite ~half; survivors must stay intact.
   for (auto& [k, v] : model) {
      rng = splitmix64(rng);
      if (rng & 1) { v ^= 0xF00Dull; REQUIRE(t.upsert_varlen(raw(k), vview(v))); }
   }
   full_sweep();

   // Removes: delete ~a third; verify gone, survivors intact, no ghosts.
   std::vector<std::string> rm;
   for (const auto& [k, v] : model) { rng = splitmix64(rng); if (rng % 3 == 0) rm.push_back(k); }
   for (const auto& k : rm) { REQUIRE(t.remove_varlen(raw(k))); model.erase(k); }
   for (const auto& k : rm) REQUIRE(t.find_varlen(raw(k)).empty());
   full_sweep();

   fx.wait_for_compactor();
}

// Raw upsert() (no terminator encoding) CAN see word-prefix keys, e.g. [w1] and
// [w1,w2]. That hits the terminal_off gap. This test pins the SAFETY contract
// regardless of whether the gap is closed: an accepted key is always findable
// (no silent loss), a rejected key is never stored, and NO upsert — accepted or
// rejected — corrupts a previously-stored key.
TEST_CASE("wtree raw word-prefix keys are honest (no silent loss/corruption)",
          "[wtree][raw][correctness]")
{
   sal_fixture                     fx("raw-wordprefix");
   wtree                           t(*fx.ses, 8);
   std::map<std::string, uint64_t> model;   // key = raw bytes (multiple of 4)

   auto raw = [](const std::string& k) {
      return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size());
   };
   auto read_v = [](std::span<const uint8_t> vv) {
      uint64_t out = 0; if (vv.size() == 8) std::memcpy(&out, vv.data(), 8); return out;
   };

   // Groups of keys sharing leading words: lengths 1..maxw words, each a
   // word-prefix of the next.
   std::vector<std::string> keys;
   uint64_t                 rng = 0xBEEFull;
   for (int g = 0; g < 80; ++g) {
      const int             maxw = 1 + int((rng = splitmix64(rng)) % 5);
      std::vector<uint32_t> words;
      for (int w = 0; w < maxw; ++w) { rng = splitmix64(rng); words.push_back(uint32_t(rng)); }
      for (int n = 1; n <= maxw; ++n)
         keys.emplace_back(reinterpret_cast<const char*>(words.data()), size_t(n) * 4);
   }
   std::sort(keys.begin(), keys.end());
   keys.erase(std::unique(keys.begin(), keys.end()), keys.end());

   uint64_t accepted = 0, rejected = 0;
   for (size_t i = 0; i < keys.size(); ++i) {
      const std::string& k  = keys[i];
      const uint64_t     v  = splitmix64(i) | 1ull;
      const bool         ok = t.upsert(raw(k), vview(v));
      if (ok) { model[k] = v; ++accepted; REQUIRE(read_v(t.find(raw(k))) == v); }  // no silent loss
      else    { ++rejected;               REQUIRE(t.find(raw(k)).empty()); }       // not stored
      for (const auto& [mk, mv] : model)  REQUIRE(read_v(t.find(raw(mk))) == mv);  // no corruption
   }
   INFO("raw word-prefix: accepted=" << accepted << " rejected=" << rejected
        << " of " << keys.size());
   for (const auto& [mk, mv] : model) REQUIRE(read_v(t.find(raw(mk))) == mv);

   fx.wait_for_compactor();
}

// Force the terminal_off gap: a FAT collision group on word[0]=W (enough keys
// to overflow the leaf and force a K=1 promotion routed by word[1]) that ALSO
// contains the terminal key [W] (no word[1]). The promotion can't represent the
// terminal entry, so the engine must either store everything anyway or honestly
// reject — never silently lose or corrupt. Runs with the terminal key inserted
// first AND last.
TEST_CASE("wtree raw terminal-in-fat-collision is honest", "[wtree][raw][correctness][terminal]")
{
   const bool terminal_first = GENERATE(true, false);
   sal_fixture                     fx(terminal_first ? "term-first" : "term-last");
   wtree                           t(*fx.ses, 8);
   std::map<std::string, uint64_t> model;

   auto raw = [](const std::string& k) {
      return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size());
   };
   auto read_v = [](std::span<const uint8_t> vv) {
      uint64_t out = 0; if (vv.size() == 8) std::memcpy(&out, vv.data(), 8); return out;
   };
   auto key2 = [](uint32_t w0, uint32_t w1) {
      uint32_t ws[2] = {w0, w1};
      return std::string(reinterpret_cast<const char*>(ws), 8);
   };
   auto key1 = [](uint32_t w0) {
      return std::string(reinterpret_cast<const char*>(&w0), 4);
   };

   const uint32_t W = 0xA5A5A5A5u;            // the shared leading word
   const std::string term = key1(W);          // terminal key [W] (no word[1])
   const int N = 4000;                        // enough to overflow + force promotion

   uint64_t accepted = 0, rejected = 0;
   auto try_put = [&](const std::string& k, uint64_t v) {
      const bool ok = t.upsert(raw(k), vview(v));
      if (ok) { model[k] = v; ++accepted; REQUIRE(read_v(t.find(raw(k))) == v); }
      else    { ++rejected;               REQUIRE(t.find(raw(k)).empty()); }
   };

   if (terminal_first) try_put(term, 0xC0DEull);
   for (int i = 0; i < N; ++i) try_put(key2(W, uint32_t(i) * 2654435761u), uint64_t(i) + 1);
   if (!terminal_first) try_put(term, 0x7E12ull);

   // Whatever was accepted must be exactly retrievable; nothing corrupted.
   for (const auto& [mk, mv] : model) REQUIRE(read_v(t.find(raw(mk))) == mv);
   INFO("terminal_first=" << terminal_first << " accepted=" << accepted
        << " rejected=" << rejected);
   // The terminal key's fate is deterministic per ordering; assert it is handled
   // honestly (present iff accepted), never a phantom.
   const bool term_findable = !t.find(raw(term)).empty();
   REQUIRE(term_findable == (model.count(term) != 0));

   fx.wait_for_compactor();
}

// ── Variable-length values ─────────────────────────────────────────
//
// value_region v2 stores a per-entry length, so the SAME key can be
// re-upserted with values of different sizes (the slot is re-homed to a
// fresh region on a size change, the old one marked dead).  These tests
// pin down (a) read-back correctness across resizes and (b) the leaf
// dead-byte accounting invariant under varlen churn.

namespace
{
   // Deterministic value of `n` bytes from a seed (distinct per (seed,len)).
   std::vector<uint8_t> mkval(uint8_t seed, size_t n)
   {
      std::vector<uint8_t> v(n);
      for (size_t i = 0; i < n; ++i)
         v[i] = uint8_t(seed * 31u + i * 7u + 1u);
      return v;
   }
   std::span<const uint8_t> sp(const std::vector<uint8_t>& v)
   {
      return std::span<const uint8_t>(v.data(), v.size());
   }
}  // namespace

TEST_CASE("wtree upsert same key with different value sizes", "[wtree][varlen-value]")
{
   sal_fixture fx("varlen-resize");
   wtree       t(*fx.ses, /*value_len=*/0);   // 0 ⇒ variable-length mode

   uint8_t k[24];
   make_key24(42, k);

   // Re-upsert the SAME key through a sequence of sizes, verifying each
   // read-back exactly (length + bytes).  Includes grow, shrink, and the
   // 0..255 extremes.
   const std::vector<size_t> sizes = {1, 12, 64, 1, 255, 8, 0, 200, 3};
   uint8_t seed = 0;
   for (size_t n : sizes)
   {
      const auto v = mkval(++seed, n);
      REQUIRE(t.upsert(kview(k, 24), sp(v)));
      auto got = t.find(kview(k, 24));
      INFO("size=" << n);
      REQUIRE(got.data() != nullptr);                 // present (even at len 0)
      REQUIRE(got.size() == n);
      REQUIRE(std::equal(got.begin(), got.end(), v.begin(), v.end()));
   }

   // Remove and confirm it is gone; a second remove is a no-op.
   REQUIRE(t.remove(kview(k, 24)));
   REQUIRE(t.find(kview(k, 24)).data() == nullptr);
   REQUIRE_FALSE(t.remove(kview(k, 24)));

   // Many distinct keys, each carrying an independent varlen value; then
   // resize a subset and re-verify all (no cross-key corruption).
   constexpr uint32_t M = 300;
   std::map<uint32_t, std::vector<uint8_t>> model;
   for (uint32_t i = 0; i < M; ++i)
   {
      uint8_t ki[24]; make_key24(1000 + i, ki);
      auto v = mkval(uint8_t(i), size_t(i % 64));     // sizes 0..63
      REQUIRE(t.upsert(kview(ki, 24), sp(v)));
      model[i] = std::move(v);
   }
   auto verify_all = [&] {
      for (const auto& [i, v] : model)
      {
         uint8_t ki[24]; make_key24(1000 + i, ki);
         auto got = t.find(kview(ki, 24));
         INFO("key i=" << i);
         REQUIRE(got.size() == v.size());
         REQUIRE(std::equal(got.begin(), got.end(), v.begin(), v.end()));
      }
   };
   verify_all();
   // Resize every 7th key to a different length (grow some, shrink others).
   for (uint32_t i = 0; i < M; i += 7)
   {
      uint8_t ki[24]; make_key24(1000 + i, ki);
      auto v = mkval(uint8_t(i + 128), size_t((i * 5 + 17) % 200));
      REQUIRE(t.upsert(kview(ki, 24), sp(v)));
      model[i] = std::move(v);
   }
   verify_all();

   fx.wait_for_compactor();
}

TEST_CASE("wtree varlen leaf dead-byte accounting", "[wtree][varlen-value][leaf-accounting]")
{
   namespace pw = psitri::wtree;
   sal_fixture fx("varlen-accounting");
   pw::g_wtree_session = fx.ses.get();
   pw::leaf_node* lf = pw::leaf_alloc();
   pw::leaf_init(lf);

   // dead_bytes must always equal tail_alloc_pos - live (value_len param to
   // leaf_live_bytes is ignored now — each region self-describes its length).
   auto inv_ok = [&] {
      const uint32_t live = pw::leaf_live_bytes(lf, /*ignored=*/0);
      return uint32_t(lf->dead_bytes) == uint32_t(lf->tail_alloc_pos) - live;
   };
   auto W = [](uint32_t a, uint32_t b, uint32_t c, uint32_t d, uint32_t w[4]) {
      w[0] = a; w[1] = b; w[2] = c; w[3] = d;
   };

   // Single key, churned through many sizes — exercises the in-place vs
   // re-home overwrite paths and the dead accounting after each.
   uint32_t w[4]; W(7, 11, 13, 17, w);
   const std::vector<size_t> sizes = {4, 40, 4, 120, 1, 255, 8, 0, 64};
   uint8_t seed = 0;
   for (size_t n : sizes)
   {
      const auto v = mkval(++seed, n);
      REQUIRE(pw::leaf_insert(lf, pw::word_key_type(w, 4), 0, sp(v)));
      auto got = pw::leaf_find(lf, pw::word_key_type(w, 4), 0, 0);
      INFO("single-key size=" << n);
      REQUIRE(got.size() == n);
      REQUIRE(std::equal(got.begin(), got.end(), v.begin(), v.end()));
      REQUIRE(inv_ok());
   }

   // Multiple colliding/non-colliding keys with mixed sizes; update + remove
   // while holding the invariant.  Keys share leading words to force in-leaf
   // radix collisions (the variable-length collision-split paths).
   // Modest population that stays well under the 4 KB leaf even with resize
   // churn (a raw leaf has no split/compaction fallback, and dead bytes are
   // never reclaimed here — a mid-collision leaf_insert that runs out of heap
   // legitimately orphans its partial allocation, which would break the
   // accounting invariant; the real wtree avoids that by splitting).
   std::map<uint32_t, std::vector<uint8_t>> model;
   auto key_of = [&](uint32_t i, uint32_t ww[4]) { W(7, 11, 13, 100 + i, ww); };
   const uint32_t N = 18;
   for (uint32_t i = 0; i < N; ++i)
   {
      uint32_t ww[4]; key_of(i, ww);
      auto v = mkval(uint8_t(i), size_t((i * 5) % 32));
      REQUIRE(pw::leaf_insert(lf, pw::word_key_type(ww, 4), 0, sp(v)));
      model[i] = std::move(v);
      REQUIRE(inv_ok());
   }
   auto verify_all = [&] {
      for (const auto& [i, v] : model)
      {
         uint32_t ww[4]; key_of(i, ww);
         auto got = pw::leaf_find(lf, pw::word_key_type(ww, 4), 0, 0);
         INFO("multi-key i=" << i);
         REQUIRE(got.size() == v.size());
         REQUIRE(std::equal(got.begin(), got.end(), v.begin(), v.end()));
      }
   };
   verify_all();
   REQUIRE(inv_ok());
   // Resize a subset (grow some, shrink others); invariant holds after each.
   for (uint32_t i = 1; i < N; i += 3)
   {
      uint32_t ww[4]; key_of(i, ww);
      auto v = mkval(uint8_t(i + 200), size_t((i * 3 + 7) % 40));
      REQUIRE(pw::leaf_insert(lf, pw::word_key_type(ww, 4), 0, sp(v)));
      model[i] = std::move(v);
      REQUIRE(inv_ok());
   }
   verify_all();
   // Remove the even-indexed survivors; the rest stay correct, invariant holds.
   std::vector<uint32_t> removed;
   for (const auto& [i, v] : model) if (i % 2 == 0) removed.push_back(i);
   for (uint32_t i : removed)
   {
      uint32_t ww[4]; key_of(i, ww);
      bool empty = false;
      REQUIRE(pw::leaf_remove(lf, pw::word_key_type(ww, 4), 0, 0, &empty));
      model.erase(i);
      REQUIRE(inv_ok());
   }
   verify_all();
   for (uint32_t i : removed)
   {
      uint32_t ww[4]; key_of(i, ww);
      REQUIRE(pw::leaf_find(lf, pw::word_key_type(ww, 4), 0, 0).data() == nullptr);
   }

   pw::leaf_free(lf);
}

// ── Node lifecycle over out-of-line value refs (big / subtree) ──────
//
// No write path produces big/subtree values yet, so these synthesize one by
// hand: insert a 4-byte inline value, then patch its value_region's type to
// `big` and write a live child node's ptr_address into the value bytes. That
// exercises the destroy / visit_children / retain_children walk for real
// (release the child on free, report it to the compactor, retain it at a share
// point) — the foundation big values and subtrees will sit on.

TEST_CASE("wtree leaf lifecycle releases big/subtree refs", "[wtree][lifecycle]")
{
   namespace pw = psitri::wtree;
   sal_fixture fx("lifecycle");
   pw::g_wtree_session = fx.ses.get();

   // ── single-key big value: visit, retain, destroy ──
   {
      const uint64_t base = fx.live_objects();
      pw::leaf_node* parent = pw::leaf_alloc();
      pw::leaf_init(parent);
      uint32_t w[1]  = {0xABCD};
      uint8_t  v4[4] = {1, 2, 3, 4};
      REQUIRE(pw::leaf_insert(parent, pw::word_key_type(w, 1), 0,
                              std::span<const uint8_t>(v4, 4)));
      pw::leaf_node* child = pw::leaf_alloc();   // ref 1, absorbed by the big slot
      pw::leaf_init(child);
      const sal::ptr_address child_addr = child->address();

      // Patch the only (swords==0) value_region into a big → child.
      const uint16_t off = pw::leaf_offs(parent)[0];
      REQUIRE(!pw::offs_is_radix(off));
      auto* vr = reinterpret_cast<pw::value_region*>(pw::leaf_at(parent, off));
      REQUIRE(vr->suffix_words == 0);
      REQUIRE(vr->value_len == 4);
      vr->set_type(pw::value_region::big);
      std::memcpy(vr->value(), &child_addr, sizeof(child_addr));

      // visit_children reports exactly the child.
      std::vector<sal::ptr_address> seen;
      parent->visit_children([&](sal::ptr_address a) { seen.push_back(a); });
      REQUIRE(seen.size() == 1);
      REQUIRE(seen[0] == child_addr);

      // retain_children bumps the child so it survives the parent's destroy.
      parent->retain_children(fx.ses);   // child ref 1 → 2
      pw::leaf_free(parent);             // destroy → release child (2 → 1)
      fx.ses->release(child_addr);       // drop our retain (1 → 0) → freed
      fx.wait_for_compactor();
      REQUIRE(fx.live_objects() == base);
   }

   // ── big value buried in an in-leaf radix: recursion + destroy ──
   {
      const uint64_t base = fx.live_objects();
      pw::leaf_node* parent = pw::leaf_alloc();
      pw::leaf_init(parent);
      auto key_of = [](uint32_t i, uint32_t w[4]) { w[0]=5; w[1]=6; w[2]=7; w[3]=100+i; };
      for (uint32_t i = 0; i < 3; ++i)   // ≥2 colliding keys → shared-prefix radix
      {
         uint32_t w[4]; key_of(i, w);
         uint8_t  v4[4] = {uint8_t(i), 0, 0, 0};
         REQUIRE(pw::leaf_insert(parent, pw::word_key_type(w, 4), 0,
                                 std::span<const uint8_t>(v4, 4)));
      }
      pw::leaf_node* child = pw::leaf_alloc();
      pw::leaf_init(child);
      const sal::ptr_address child_addr = child->address();

      // Patch key i=1's deep value_region (swords==0 after collision) to big.
      uint32_t w1[4]; key_of(1, w1);
      auto got = pw::leaf_find(parent, pw::word_key_type(w1, 4), 0, 0);
      REQUIRE(got.size() == 4);
      auto* vr = reinterpret_cast<pw::value_region*>(
          const_cast<uint8_t*>(got.data()) - pw::value_region::kHeaderBytes);
      REQUIRE(vr->suffix_words == 0);
      vr->set_type(pw::value_region::big);
      std::memcpy(vr->value(), &child_addr, sizeof(child_addr));

      std::vector<sal::ptr_address> seen;
      parent->visit_children([&](sal::ptr_address a) { seen.push_back(a); });
      REQUIRE(seen.size() == 1);
      REQUIRE(seen[0] == child_addr);

      // destroy (via free) must recurse the radix and release the child.
      pw::leaf_free(parent);
      fx.wait_for_compactor();
      REQUIRE(fx.live_objects() == base);
   }
}

// ── Fork-clone primitive (foundation for shared-mode COW-to-root) ───
//
// clone_leaf_fresh / clone_inner_fresh fork a node to a NEW ptr_address with
// copied content (vs cow_*, which preserve the address via a CB move). This is
// the primitive shared-mode upsert uses so a snapshot's old node survives. Here
// we prove the leaf fork: new address, identical reads, and INDEPENDENT storage
// (mutating the clone doesn't touch the original).

TEST_CASE("wtree clone_leaf_fresh forks to an independent copy", "[wtree][clone]")
{
   namespace pw = psitri::wtree;
   sal_fixture fx("clone-leaf");
   pw::g_wtree_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   pw::leaf_node* src = pw::leaf_alloc();
   pw::leaf_init(src);
   std::map<uint32_t, uint64_t> model;
   for (uint32_t i = 0; i < 12; ++i)
   {
      uint32_t w[2] = {0x100u + i, i * 7 + 1};
      uint64_t v    = uint64_t(i) * 1000 + 1;
      REQUIRE(pw::leaf_insert(src, pw::word_key_type(w, 2), 0, vview(v)));
      model[i] = v;
   }

   pw::leaf_node* dst = pw::clone_leaf_fresh(src);
   REQUIRE(dst->address() != src->address());     // forked → its own control block
   REQUIRE(dst->num_keys == src->num_keys);

   // Every key reads back identically from the clone.
   for (auto& [i, v] : model)
   {
      uint32_t w[2] = {0x100u + i, i * 7 + 1};
      auto g = pw::leaf_find(dst, pw::word_key_type(w, 2), 0, 0);
      REQUIRE(g.size() == 8);
      uint64_t out; std::memcpy(&out, g.data(), 8);
      REQUIRE(out == v);
   }

   // Mutate the clone; the original must be untouched (independent storage —
   // the whole point of fork vs the address-preserving cow_leaf).
   {
      uint32_t w[2] = {0x100u, 1};   // key i=0
      uint64_t nv   = 999999;
      REQUIRE(pw::leaf_insert(dst, pw::word_key_type(w, 2), 0, vview(nv)));
      auto gs = pw::leaf_find(src, pw::word_key_type(w, 2), 0, 0);
      auto gd = pw::leaf_find(dst, pw::word_key_type(w, 2), 0, 0);
      uint64_t os, od; std::memcpy(&os, gs.data(), 8); std::memcpy(&od, gd.data(), 8);
      REQUIRE(os == model[0]);   // original unchanged
      REQUIRE(od == 999999);     // clone changed
   }

   // Inline values own no out-of-line refs → destroy releases nothing; free both.
   pw::leaf_free(src);
   pw::leaf_free(dst);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── Snapshot infrastructure (retain root → read view) ──────────────
//
// snapshot() retains the root and returns a wtree that reads it. Here we verify
// the retain/release balance and that the snapshot reads the same data — WITHOUT
// writing to the live tree between (snapshot isolation UNDER concurrent writes
// needs shared-mode COW, the next step). A read-only snapshot must add no net
// live objects once dropped.

TEST_CASE("wtree snapshot reads and balances refcounts", "[wtree][snapshot]")
{
   sal_fixture fx("snapshot-basic");
   const uint64_t base = fx.live_objects();
   auto read_v = [](std::span<const uint8_t> vv) {
      uint64_t out = 0; if (vv.size() == 8) std::memcpy(&out, vv.data(), 8); return out;
   };
   {
      wtree t(*fx.ses, 8);
      std::map<uint32_t, uint64_t> model;
      for (uint32_t i = 0; i < 200; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         uint64_t v = uint64_t(i) * 1000 + 7;
         REQUIRE(t.upsert(kview(k, 24), vview(v)));
         model[i] = v;
      }
      {
         wtree snap = t.snapshot();   // retains root (ref ≥ 2)
         for (auto& [i, v] : model)
         {
            uint8_t k[24]; make_key24(i, k);
            REQUIRE(read_v(snap.find(kview(k, 24))) == v);   // snapshot reads all
            REQUIRE(read_v(t.find(kview(k, 24)))    == v);   // live reads all
         }
      }   // snapshot dropped → releases its retain
      // Live tree is intact and still readable after the snapshot is gone.
      for (auto& [i, v] : model)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(read_v(t.find(kview(k, 24))) == v);
      }
   }   // live tree dropped → frees everything
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── Snapshot isolation UNDER concurrent writes (shared-mode COW) ────
//
// The real test: take a snapshot, then heavily mutate the live tree (overwrite
// every key + insert as many new ones, forcing shared-mode splits up a
// multi-level spine). The snapshot must stay frozen at the pre-write state
// (old values, new keys invisible) while the live tree sees the new world —
// and refcounts must balance so nothing leaks once both drop.

TEST_CASE("wtree snapshot isolation under writes", "[wtree][snapshot][shared]")
{
   sal_fixture fx("snapshot-isolation");
   const uint64_t base = fx.live_objects();
   auto read_v = [](std::span<const uint8_t> vv) {
      uint64_t out = 0; if (vv.size() == 8) std::memcpy(&out, vv.data(), 8); return out;
   };
   {
      wtree          t(*fx.ses, 8);
      const uint32_t N = 2000;   // multi-level tree (inner nodes + many leaves)
      for (uint32_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(t.upsert(kview(k, 24), vview(uint64_t(i) * 10 + 1)));
      }

      wtree snap = t.snapshot();   // ref>1 on root → subsequent writes fork-to-root

      // Overwrite every original key with a new value, and insert N new keys
      // (grows the tree → exercises shared-mode splits on the cloned spine).
      for (uint32_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(t.upsert(kview(k, 24), vview(uint64_t(i) * 10 + 7'000'000)));
      }
      for (uint32_t i = N; i < 2 * N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(t.upsert(kview(k, 24), vview(uint64_t(i) * 10 + 1)));
      }

      // Snapshot is FROZEN at the pre-write state.
      for (uint32_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(read_v(snap.find(kview(k, 24))) == uint64_t(i) * 10 + 1);   // old values
      }
      for (uint32_t i = N; i < 2 * N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(snap.find(kview(k, 24)).data() == nullptr);                 // new keys invisible
      }

      // Live tree sees the new world: overwritten values + the new keys.
      for (uint32_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(read_v(t.find(kview(k, 24))) == uint64_t(i) * 10 + 7'000'000);
      }
      for (uint32_t i = N; i < 2 * N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(read_v(t.find(kview(k, 24))) == uint64_t(i) * 10 + 1);
      }
   }   // snap destroyed first (releases old spine), then t (releases new spine)
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── Snapshot isolation under REMOVES (shared-mode remove via node_modify_) ──
//
// Remove now shares the mode-templated descent with upsert, so removing a key
// while a snapshot is held forks to a new root instead of mutating shared nodes
// in place (the previously-latent corruption). The snapshot must keep the
// removed keys; the live tree must not see them; refcounts must balance.

TEST_CASE("wtree snapshot isolation under removes", "[wtree][snapshot][shared][remove]")
{
   sal_fixture fx("snapshot-remove");
   const uint64_t base = fx.live_objects();
   auto read_v = [](std::span<const uint8_t> vv) {
      uint64_t out = 0; if (vv.size() == 8) std::memcpy(&out, vv.data(), 8); return out;
   };
   {
      wtree          t(*fx.ses, 8);
      const uint32_t N = 2000;
      for (uint32_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(t.upsert(kview(k, 24), vview(uint64_t(i) * 10 + 1)));
      }

      wtree snap = t.snapshot();   // freeze the full set

      // Remove every even key from the LIVE tree (forks the spine behind the
      // snapshot; exercises shared-mode leaf remove + slot-drop + the
      // address-bubbling unwind).
      for (uint32_t i = 0; i < N; i += 2)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(t.remove(kview(k, 24)));
         REQUIRE_FALSE(t.remove(kview(k, 24)));   // second remove is a miss
      }

      // Snapshot is FROZEN with ALL keys still present at their old values.
      for (uint32_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         REQUIRE(read_v(snap.find(kview(k, 24))) == uint64_t(i) * 10 + 1);
      }
      // Live tree: evens gone, odds intact.
      for (uint32_t i = 0; i < N; ++i)
      {
         uint8_t k[24]; make_key24(i, k);
         auto g = t.find(kview(k, 24));
         if (i % 2 == 0) REQUIRE(g.data() == nullptr);
         else            REQUIRE(read_v(g) == uint64_t(i) * 10 + 1);
      }
   }   // snap (old full set) then t (odds) drop
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 rewrite — Phase 1: node record + region layout + sizing ──────
//
// The header's static_asserts verify the fixed-header layout at compile time
// (this TU including the header IS that check). Here we verify the runtime
// region math: cl-aligned (128 B) offsets, monotonic regions, and size_for()
// matching an instance's total_bytes().

TEST_CASE("wtree2 node layout + sizing", "[wtree2][layout]")
{
   namespace w2 = psitri::wtree2;
   constexpr std::size_t CL = w2::kCacheLine;   // 128

   auto check = [&](uint16_t n, uint8_t pfx) {
      // size_for must equal what an instance with these dims reports, and must
      // be cacheline-aligned with strictly-ordered, cl-aligned regions.
      const uint32_t sz = w2::node2::size_for(n, pfx);
      REQUIRE(sz % CL == 0);

      // Stamp a buffer as a node2 and check its accessors agree with size_for.
      alignas(CL) static thread_local uint8_t buf[1 << 20];
      REQUIRE(sz <= sizeof(buf));
      auto* nd = reinterpret_cast<w2::node2*>(buf);
      nd->num_stems  = n;
      nd->prefix_len = pfx;
      nd->shape_     = uint8_t(w2::shape::inner);
      nd->flags      = 0;

      REQUIRE(nd->stems_off()    % CL == 0);
      REQUIRE(nd->children_off() % CL == 0);
      REQUIRE(nd->stems_off()    >= w2::node2::kFixedHeaderBytes + uint32_t(pfx) * 4u);
      REQUIRE(nd->children_off() >= nd->stems_off() + uint32_t(n) * 4u);
      REQUIRE(nd->total_bytes()  == sz);
      // n_slots = n+1 children
      REQUIRE(nd->n_slots() == uint16_t(n + 1));
   };

   check(0, 0);
   check(1, 0);
   check(15, 1);
   check(16, 1);     // stems cross into a 2nd region cl
   check(255, 3);
   check(2000, 8);
   check(6000, 0);   // ~max inner fanout
}

// ── wtree2 — Phase 2: bulk-inner constructor + prefix absorption + search ──

TEST_CASE("wtree2 prefix absorption + stem search", "[wtree2][build]")
{
   namespace w2 = psitri::wtree2;

   // longest_common_prefix over sorted key-word spans = LCP(first, last).
   {
      uint32_t a[] = {1, 2, 3}, b[] = {1, 2, 9};
      REQUIRE(w2::longest_common_prefix({a, 3}, {b, 3}) == 2);
      uint32_t c[] = {5}, d[] = {7};
      REQUIRE(w2::longest_common_prefix({c, 1}, {d, 1}) == 0);
      uint32_t e[] = {1, 2}, f[] = {1, 2};
      REQUIRE(w2::longest_common_prefix({e, 2}, {f, 2}) == 2);
   }

   // stem_upper_bound: slots[0] for q<stems[0]; q==stem routes right.
   {
      uint32_t st[] = {100, 200};
      REQUIRE(w2::stem_upper_bound(st, 2, 50)  == 0);
      REQUIRE(w2::stem_upper_bound(st, 2, 100) == 1);   // == divider → right side
      REQUIRE(w2::stem_upper_bound(st, 2, 150) == 1);
      REQUIRE(w2::stem_upper_bound(st, 2, 200) == 2);
      REQUIRE(w2::stem_upper_bound(st, 2, 300) == 2);
   }
}

TEST_CASE("wtree2 build_inner materializes + routes", "[wtree2][build]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-build");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   // Three child node2s (leaf-shaped, empty) to point at.
   auto mk_child = [&] {
      return w2::w2session().alloc<w2::node2>(w2::alloc_hint{}, w2::shape::leaf,
                                              /*n_stems=*/0, /*prefix_len=*/0);
   };
   const w2::ptr_address cA = mk_child(), cB = mk_child(), cC = mk_child();

   uint32_t       prefix[]   = {0xAAAAu};
   uint32_t       stems[]    = {100u, 200u};
   w2::ptr_address children[] = {cA, cB, cC};
   w2::node2* in = w2::build_inner({prefix, 1}, {stems, 2}, {children, 3});

   // Structure read-back.
   REQUIRE(in->is_inner());
   REQUIRE(in->num_stems == 2);
   REQUIRE(in->prefix_len == 1);
   REQUIRE(in->prefix()[0] == 0xAAAAu);
   // Read logical stems via the form-agnostic accessor: an inner may store them
   // grid (base+shift, no region) or u16-range-encoded, so a raw stems() u32 read
   // would see the compressed bytes. (A typed-CRTP inner would make that a compile
   // error rather than a wrong read — see node_design_patterns 3-tier CRTP.)
   uint32_t logical_stems[2];
   w2::read_stems(in, logical_stems);
   REQUIRE(logical_stems[0] == 100u);
   REQUIRE(logical_stems[1] == 200u);
   REQUIRE(in->children()[0] == cA);
   REQUIRE(in->children()[1] == cB);
   REQUIRE(in->children()[2] == cC);
   REQUIRE(in->total_bytes() == w2::node2::size_for(2, 1));

   // Routing matches stem_upper_bound semantics. Use the form-agnostic node_stem_ub
   // (grid / u16 / apgm / explicit) — a raw stems() u32 read would see compressed bytes.
   auto route = [&](uint32_t q) { return in->children()[w2::node_stem_ub(in, q)]; };
   REQUIRE(route(50)  == cA);
   REQUIRE(route(100) == cB);
   REQUIRE(route(150) == cB);
   REQUIRE(route(200) == cC);
   REQUIRE(route(999) == cC);

   // release_node recurses via destroy() → frees the inner AND its EXT children.
   w2::release_node(in);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — APGM predictor on a wide inner (algo §6) ──────────────────
// A wide inner must build an estimator (segment_count != 0) and node_stem_ub
// (predictor-seeded) must return the EXACT same slot as binary search for every
// query class: below-first, exact-stem, between-stems, above-last.
TEST_CASE("wtree2 inner predictor matches binary search", "[wtree2][predictor]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-predictor");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   // Build a wide inner over N stems. Use a non-uniform (quadratic) key
   // distribution so the predictor must lay down multiple segments, not a single
   // perfect line — the correction walk is what we're really exercising.
   constexpr uint16_t N = 1000;
   std::vector<uint32_t>       stems(N);
   std::vector<w2::ptr_address> kids;
   kids.reserve(N + 1);
   for (uint16_t i = 0; i < N; ++i)
      stems[i] = 1000u + uint32_t(i) * uint32_t(i) * 3u;   // strictly increasing, curved
   auto mk_child = [&] {
      // An empty leaf (num_stems==0) still has n_slots()==1, and a varlen leaf has
      // child refs → destroy() walks slot 0. The header-only ctor leaves it
      // UNINITIALIZED, so it must be set to kNoChild (NOT left as 0 — ptr_address(0)
      // is a real allocatable id, so an uninitialized 0 slot would release id 0 on
      // teardown). See [[sal_ptr_address_zero]].
      auto a = w2::w2session().alloc<w2::node2>(w2::alloc_hint{}, w2::shape::leaf, 0, 0);
      w2::w2deref(a)->children()[0] = w2::kNoChild;
      return a;
   };
   for (uint16_t i = 0; i <= N; ++i) kids.push_back(mk_child());

   w2::node2* in = w2::build_inner({}, {stems.data(), N}, {kids.data(), size_t(N) + 1});

   // A wide, well-behaved inner must have a usable estimator.
   REQUIRE(in->estimator()->segment_count != 0);

   // Exhaustive query sweep: for each stem probe q-1, q, q+1, plus the extremes.
   auto check_q = [&](uint32_t q) {
      const uint16_t want = w2::stem_upper_bound(in->stems(), in->num_stems, q);
      const uint16_t got  = w2::node_stem_ub(in, q);
      REQUIRE(got == want);
   };
   check_q(0);
   check_q(1);
   for (uint16_t i = 0; i < N; ++i)
   {
      const uint32_t s = stems[i];
      if (s > 0) check_q(s - 1);
      check_q(s);
      check_q(s + 1);
   }
   check_q(0xFFFFFFFFu);

   w2::release_node(in);   // recurses via destroy() → frees the children too
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — implicit-divider grid: detect, route-without-stems, downgrade ──
// A constant-stride power-of-2 inner is stored as base+shift (segment_count ==
// kBinarySentinel) and routed by a single shift — the stems line is never read on
// the find path. When a leaf below splits and pushes a divider that does NOT fit the
// grid, the node must DOWNGRADE: regenerate explicit stems (with the new divider
// spliced in) and route by stem search. These tests pin both halves.
TEST_CASE("wtree2 grid inner detect + route + downgrade", "[wtree2][grid]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-grid");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base_objs = fx.live_objects();

   auto mk_child = [&] {
      // An empty leaf (num_stems==0) still has n_slots()==1, and a varlen leaf has
      // child refs → destroy() walks slot 0. The header-only ctor leaves it
      // UNINITIALIZED, so it must be set to kNoChild (NOT left as 0 — ptr_address(0)
      // is a real allocatable id, so an uninitialized 0 slot would release id 0 on
      // teardown). See [[sal_ptr_address_zero]].
      auto a = w2::w2session().alloc<w2::node2>(w2::alloc_hint{}, w2::shape::leaf, 0, 0);
      w2::w2deref(a)->children()[0] = w2::kNoChild;
      return a;
   };

   // A constant-stride power-of-2 grid: stems[i] = BASE + (i+1)*STRIDE.
   constexpr uint16_t N      = 8;
   constexpr uint32_t BASE   = 1024;
   constexpr uint32_t STRIDE = 16;          // 2^4
   std::vector<uint32_t>        gstems(N);
   std::vector<w2::ptr_address> gkids;
   for (uint16_t i = 0; i < N; ++i) gstems[i] = BASE + uint32_t(i + 1) * STRIDE;
   for (uint16_t i = 0; i <= N; ++i) gkids.push_back(mk_child());

   w2::node2* grid = w2::build_inner({}, {gstems.data(), N}, {gkids.data(), size_t(N) + 1});

   // (1) Detected as an exact grid → routes by shift, EXACT vs reference stem search.
   REQUIRE(grid->estimator()->segment_count == w2::kBinarySentinel);
   auto sweep = [&](w2::node2* nd, const std::vector<uint32_t>& ref) {
      auto check = [&](uint32_t q) {
         const uint16_t want = w2::stem_upper_bound(ref.data(), uint16_t(ref.size()), q);
         REQUIRE(w2::node_stem_ub(nd, q) == want);
      };
      check(0);
      for (uint32_t s : ref) { if (s) check(s - 1); check(s); check(s + 1); }
      check(0xFFFFFFFFu);
   };
   sweep(grid, gstems);

   // (2) A leaf below splits at slot `pos`, pushing divider `div` that is NOT on the
   // grid (falls strictly between two grid points → breaks constant stride). Build
   // the post-split node through the absorb op-ctor (replaces slot pos with two
   // children separated by div) and require it downgraded to explicit + routes right.
   constexpr uint16_t pos = 3;
   const uint32_t     div = gstems[pos] - STRIDE / 2;          // mid-cell → off-grid
   w2::ptr_address    ins_kids[2] = {mk_child(), mk_child()};
   const w2::op_inner_absorb op{pos, 1, &div, ins_kids};
   w2::node2* down =
       w2::w2deref(w2::w2session().alloc<w2::node2>(w2::alloc_hint{}, *grid, op));

   REQUIRE(down->num_stems == N + 1);
   REQUIRE(down->estimator()->segment_count != w2::kBinarySentinel);   // downgraded

   std::vector<uint32_t> ref = gstems;            // expected merged stems
   ref.insert(ref.begin() + pos, div);
   sweep(down, ref);

   // Teardown mirrors the real absorb transfer: `down` cloned grid's children
   // (unretained), so `grid` is released-MOVED (neuter → releases none of them) while
   // `down` owns + deep-releases them. The slot-`pos` child was "split away" (replaced
   // by ins_kids in `down`), so release it explicitly like a self-freeing split child.
   w2::release_node(down);
   w2::release_node_moved(grid);
   w2::release_subtree(gkids[pos]);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base_objs);
}

// ── wtree2 — lower_bound (floor-or-above) vs std::lower_bound reference ─────
// Many distinct u32 keys (multi-leaf, so the inner back-track path is exercised);
// probe every key ±1 plus the extremes and require the returned value matches the
// smallest key ≥ q (or absent when q exceeds the max).
TEST_CASE("wtree2 lower_bound floor-or-above", "[wtree2][lowerbound]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-lb");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base_objs = fx.live_objects();

   auto splitmix = [](uint64_t x) {
      x += 0x9E3779B97F4A7C15ULL;
      x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
      x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
      return uint32_t(x ^ (x >> 31));
   };
   constexpr int         N = 3000;
   std::vector<uint32_t> sorted;
   sorted.reserve(N);
   for (int i = 0; i < N; ++i) sorted.push_back(splitmix(i));
   std::sort(sorted.begin(), sorted.end());
   sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());

   std::vector<uint32_t> ins = sorted;                    // insert in shuffled order
   std::shuffle(ins.begin(), ins.end(), std::mt19937_64(7));

   w2::ptr_address root = w2::kNoChild;
   {
      sal::read_lock lk = fx.ses->lock();
      for (uint32_t k : ins)
      {
         const uint64_t v = k;
         root = w2::insert(root, {&k, 1}, {reinterpret_cast<const uint8_t*>(&v), 8});
      }
   }
   {
      sal::read_lock lk = fx.ses->lock();
      w2::node2*     r  = w2::w2deref(root);
      auto lb = [&](uint32_t q) -> int64_t {
         auto v = w2::lower_bound_value(r, {&q, 1});
         if (!v.data()) return -1;
         uint64_t g = 0;
         std::memcpy(&g, v.data(), 8);
         return int64_t(g);
      };
      auto expect = [&](uint32_t q) -> int64_t {
         auto it = std::lower_bound(sorted.begin(), sorted.end(), q);
         return it == sorted.end() ? -1 : int64_t(*it);
      };
      for (uint32_t k : sorted)
      {
         REQUIRE(lb(k) == expect(k));
         if (k > 0) REQUIRE(lb(k - 1) == expect(k - 1));
         if (k < 0xFFFFFFFFu) REQUIRE(lb(k + 1) == expect(k + 1));
      }
      REQUIRE(lb(0) == expect(0));
      REQUIRE(lb(0xFFFFFFFFu) == expect(0xFFFFFFFFu));
   }

   w2::release_subtree(root);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base_objs);
}

// ── wtree2 — system-dictionary load + query (mirrors tree_context_tests) ───
// Loads /usr/share/dict/words, inserts every word (value = an 8-B hash of the word),
// then queries every word in FILE order and in RANDOM order. Reports words/sec +
// ns/lookup so the numbers line up against the psitri tree_context dictionary test.
// Variable-length keys exercise the Patricia + varlen-leaf path. Skips if absent.
TEST_CASE("wtree2 system dictionary load + query", "[wtree2][dict]")
{
   namespace w2 = psitri::wtree2;

   std::vector<std::string> words;
   {
      std::ifstream f("/usr/share/dict/words");
      if (!f) { WARN("no /usr/share/dict/words — skipping"); return; }
      for (std::string w; f >> w;) words.push_back(w);
   }
   if (words.size() < 1000) { WARN("dictionary too small — skipping"); return; }
   const size_t N = words.size();

   // Pack a word + NUL terminator into u32 words (injective: distinct strings → distinct
   // word-sequences; the NUL disambiguates the zero padding of the last word).
   auto pack = [](const std::string& s) {
      std::vector<uint32_t> k((s.size() + 1 + 3) / 4, 0u);
      std::memcpy(k.data(), s.data(), s.size());
      return k;
   };
   auto hash8 = [](const std::string& s) {
      uint64_t h = 1469598103934665603ull;
      for (unsigned char c : s) { h ^= c; h *= 1099511628211ull; }
      return h ? h : 1ull;
   };
   std::vector<std::vector<uint32_t>> keys(N);
   for (size_t i = 0; i < N; ++i) keys[i] = pack(words[i]);

   sal_fixture fx("wtree2-dict");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base_objs = fx.live_objects();

   auto now = [] { return std::chrono::high_resolution_clock::now(); };
   auto ns  = [](auto a, auto b) {
      return std::chrono::duration_cast<std::chrono::nanoseconds>(b - a).count();
   };

   w2::g_inner_total = 0;
   w2::g_inner_grid  = 0;
   w2::ptr_address root = w2::kNoChild;
   {
      sal::read_lock lk = fx.ses->lock();
      const auto t0 = now();
      for (size_t i = 0; i < N; ++i)
      {
         const uint64_t v = hash8(words[i]);
         root = w2::insert(root, {keys[i].data(), uint16_t(keys[i].size())},
                           {reinterpret_cast<const uint8_t*>(&v), 8});
      }
      const auto t1 = now();
      std::printf("[wtree2][dict] inners built=%llu grids=%llu\n",
                  (unsigned long long)w2::g_inner_total, (unsigned long long)w2::g_inner_grid);
      std::printf("[wtree2][dict] insert %zu words in %lld ms (%.2f M words/s)\n",
                  N, (long long)(ns(t0, t1) / 1000000),
                  double(N) / (double(ns(t0, t1)) / 1e9) / 1e6);
      std::fflush(stdout);
   }

   auto query = [&](const std::vector<uint32_t>& order, const char* label) {
      sal::read_lock lk = fx.ses->lock();
      w2::node2*     r  = w2::w2deref(root);
      size_t         miss = 0, mism = 0;
      const auto     t0 = now();
      for (uint32_t idx : order)
      {
         auto v = w2::find_value(r, {keys[idx].data(), uint16_t(keys[idx].size())});
         if (!v.data()) { ++miss; continue; }
         uint64_t got = 0;
         std::memcpy(&got, v.data(), 8);
         if (got != hash8(words[idx])) ++mism;
      }
      const auto t1 = now();
      std::printf("[wtree2][dict] query/%s %zu in %lld ms (%lld ns/lookup) miss=%zu mism=%zu\n",
                  label, order.size(), (long long)(ns(t0, t1) / 1000000),
                  (long long)(ns(t0, t1) / (long long)order.size()), miss, mism);
      std::fflush(stdout);
      REQUIRE(miss == 0);
      REQUIRE(mism == 0);
   };

   // lower_bound (floor-or-above) — the apples-to-apples op vs tree_context's cursor
   // lower_bound. Every dict key is present, so lb(key) returns that key's value.
   auto query_lb = [&](const std::vector<uint32_t>& order, const char* label) {
      sal::read_lock lk = fx.ses->lock();
      w2::node2*     r  = w2::w2deref(root);
      size_t         miss = 0, mism = 0;
      const auto     t0 = now();
      for (uint32_t idx : order)
      {
         auto v = w2::lower_bound_value(r, {keys[idx].data(), uint16_t(keys[idx].size())});
         if (!v.data()) { ++miss; continue; }
         uint64_t got = 0;
         std::memcpy(&got, v.data(), 8);
         if (got != hash8(words[idx])) ++mism;
      }
      const auto t1 = now();
      std::printf("[wtree2][dict] lbound/%s %zu in %lld ms (%lld ns/lookup) miss=%zu mism=%zu\n",
                  label, order.size(), (long long)(ns(t0, t1) / 1000000),
                  (long long)(ns(t0, t1) / (long long)order.size()), miss, mism);
      std::fflush(stdout);
      REQUIRE(miss == 0);
      REQUIRE(mism == 0);
   };

   std::vector<uint32_t> inorder(N);
   for (size_t i = 0; i < N; ++i) inorder[i] = uint32_t(i);
   query(inorder, "inorder");
   query_lb(inorder, "inorder");

   std::vector<uint32_t> rnd = inorder;
   std::shuffle(rnd.begin(), rnd.end(), std::mt19937_64(0x9E3779B9ull));
   query(rnd, "random ");
   query_lb(rnd, "random ");

   // Refcount-clean teardown: release_subtree recurses and every node is freed.
   w2::release_subtree(root);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base_objs);
}

// ── wtree2 — slot encoding (EXT / LOCAL / named via reserved range) ─────

TEST_CASE("wtree2 slot encoding", "[wtree2][slot]")
{
   namespace w2 = psitri::wtree2;

   // EXT — real allocatable addresses.
   REQUIRE(w2::is_ext_slot(w2::ptr_address(0)));
   REQUIRE(w2::is_ext_slot(w2::ptr_address(1'000'000)));
   REQUIRE_FALSE(w2::is_local_slot(w2::ptr_address(1'000'000)));

   // Named sentinels — neither EXT nor LOCAL.
   REQUIRE_FALSE(w2::is_ext_slot(w2::kNoChild));
   REQUIRE_FALSE(w2::is_local_slot(w2::kNoChild));

   // LOCAL — round-trips a 4-byte-aligned tail offset; never EXT; reserved
   // (so the SAL allocator never hands it out as a real address).
   for (uint32_t off : {0u, 4u, 64u, 4092u, w2::kMaxLocalTailOff})
   {
      auto s = w2::make_local_slot(off);
      REQUIRE(w2::is_local_slot(s));
      REQUIRE_FALSE(w2::is_ext_slot(s));
      REQUIRE(w2::local_tail_off(s) == off);
      REQUIRE(sal::is_reserved_ptr_address(s));
   }
   REQUIRE(w2::kMaxLocalTailOff >= 4096u);   // covers a full 4 KB leaf tail
}

// ── wtree2 — Phase 3a: find descent (routing) ──────────────────────────

TEST_CASE("wtree2 find descent routes + rejects out-of-range", "[wtree2][find]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-find");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   // Three leaves, each self-asserting a 1-word prefix for its range.
   auto mk_leaf = [&](uint32_t pfx_word) {
      auto a = w2::w2session().alloc<w2::node2>(w2::alloc_hint{}, w2::shape::leaf,
                                                /*n_stems=*/0, /*prefix_len=*/1);
      auto* nd = w2::w2deref(a);
      nd->prefix()[0] = pfx_word;
      return a;
   };
   const w2::ptr_address lA = mk_leaf(50), lB = mk_leaf(150), lC = mk_leaf(250);

   uint32_t        stems[]    = {100u, 200u};
   w2::ptr_address children[] = {lA, lB, lC};
   w2::node2*      root = w2::build_inner({}, {stems, 2}, {children, 3});

   auto reaches = [&](std::initializer_list<uint32_t> k, w2::ptr_address want) {
      std::vector<uint32_t> kv(k);
      w2::node2* leaf = w2::find_leaf(root, {kv.data(), kv.size()});
      return leaf && leaf->address() == want;
   };

   REQUIRE(reaches({50},  lA));
   REQUIRE(reaches({150}, lB));
   REQUIRE(reaches({250}, lC));
   REQUIRE(reaches({150, 999}, lB));   // extra word is for leaf-internal lookup

   // Out-of-range: 175 routes to lB's slot, but lB's prefix (150) rejects it →
   // absent (the no-sentinel "child rejects" model, algo §7.3).
   { std::vector<uint32_t> k{175}; REQUIRE(w2::find_leaf(root, {k.data(), k.size()}) == nullptr); }
   // Key shorter than a matched prefix → absent.
   { std::vector<uint32_t> k{}; REQUIRE(w2::find_leaf(root, {k.data(), k.size()}) == nullptr); }

   w2::release_node(root);   // recurses via destroy() → frees lA/lB/lC
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — Phase 3b: leaf value model + end-to-end find ──────────────

TEST_CASE("wtree2 leaf build + find (end-to-end DIRECT)", "[wtree2][find]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-leaf");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   auto vspan = [](const std::vector<uint8_t>& v) { return std::span<const uint8_t>(v.data(), v.size()); };
   auto kspan = [](const std::vector<uint32_t>& k) { return std::span<const uint32_t>(k.data(), k.size()); };
   auto rd    = [](std::span<const uint8_t> s) { return std::vector<uint8_t>(s.begin(), s.end()); };

   // ── Single leaf: distinct first-words, multi-word keys ──
   {
      std::vector<uint32_t> kA{10, 5}, kB{20, 7}, kC{30, 9};
      std::vector<uint8_t>  vA{1, 2, 3}, vB{4}, vC{5, 6};
      w2::leaf_entry ents[] = {{kspan(kA), vspan(vA)}, {kspan(kB), vspan(vB)}, {kspan(kC), vspan(vC)}};
      w2::node2* leaf = w2::build_leaf({ents, 3}, 0);
      REQUIRE(leaf->is_leaf());
      REQUIRE(leaf->num_stems == 3);
      REQUIRE(rd(w2::find_value(leaf, kspan(kA))) == vA);
      REQUIRE(rd(w2::find_value(leaf, kspan(kB))) == vB);
      REQUIRE(rd(w2::find_value(leaf, kspan(kC))) == vC);
      std::vector<uint32_t> m1{20, 8}, m2{25, 7}, m3{10};
      REQUIRE(w2::find_value(leaf, kspan(m1)).data() == nullptr);   // suffix mismatch
      REQUIRE(w2::find_value(leaf, kspan(m2)).data() == nullptr);   // not a stem
      REQUIRE(w2::find_value(leaf, kspan(m3)).data() == nullptr);   // suffix length
      w2::release_node(leaf);
   }

   // ── 2-level: inner routing over two leaves ──
   {
      std::vector<uint32_t> a1{50, 1}, a2{60, 2}, b1{150, 3}, b2{160, 4};
      std::vector<uint8_t>  v1{11}, v2{22}, v3{33}, v4{44};
      w2::leaf_entry la[] = {{kspan(a1), vspan(v1)}, {kspan(a2), vspan(v2)}};
      w2::leaf_entry lb[] = {{kspan(b1), vspan(v3)}, {kspan(b2), vspan(v4)}};
      w2::node2* leafA = w2::build_leaf({la, 2}, 0);
      w2::node2* leafB = w2::build_leaf({lb, 2}, 0);
      uint32_t        stems[] = {100u};
      w2::ptr_address kids[]  = {leafA->address(), leafB->address()};
      w2::node2* root = w2::build_inner({}, {stems, 1}, {kids, 2});

      REQUIRE(rd(w2::find_value(root, kspan(a1))) == v1);
      REQUIRE(rd(w2::find_value(root, kspan(a2))) == v2);
      REQUIRE(rd(w2::find_value(root, kspan(b1))) == v3);
      REQUIRE(rd(w2::find_value(root, kspan(b2))) == v4);
      std::vector<uint32_t> m{55, 9};   // routes to leafA, not a stem there
      REQUIRE(w2::find_value(root, kspan(m)).data() == nullptr);

      w2::release_node(root);   // recurses via destroy() → frees leafA/leafB
   }
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — LEAF_FIXED: uniform sizes pick the SoA path ───────────────
TEST_CASE("wtree2 leaf_fixed build + find", "[wtree2][find]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-leaffixed");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   auto rd = [](std::span<const uint8_t> s) { return std::vector<uint8_t>(s.begin(), s.end()); };

   // Uniform: every key 3 words, every value 4 bytes → LEAF_FIXED (stride 12).
   std::vector<std::array<uint32_t, 3>> keys = {
       {10, 1, 2}, {20, 3, 4}, {30, 5, 6}, {40, 7, 8}};
   std::vector<std::array<uint8_t, 4>>  vals = {
       {1, 1, 1, 1}, {2, 2, 2, 2}, {3, 3, 3, 3}, {4, 4, 4, 4}};
   std::vector<w2::leaf_entry> ents;
   for (size_t i = 0; i < keys.size(); ++i)
      ents.push_back({{keys[i].data(), 3}, {vals[i].data(), 4}});

   w2::node2* leaf = w2::build_leaf({ents.data(), ents.size()}, 0);
   REQUIRE(leaf->is_leaf());
   REQUIRE(leaf->is_leaf_fixed());                // uniform → fixed path
   REQUIRE(leaf->num_stems == 4);
   REQUIRE(leaf->fx_rem_words == 2);              // 3 key words - 1 stem word
   REQUIRE(leaf->fx_val_len == 4);

   for (size_t i = 0; i < keys.size(); ++i)
      REQUIRE(rd(w2::find_value(leaf, {keys[i].data(), 3})) ==
              std::vector<uint8_t>(vals[i].begin(), vals[i].end()));

   // Misses: wrong stem, wrong suffix, wrong length.
   std::array<uint32_t, 3> miss_stem{25, 3, 4};
   std::array<uint32_t, 3> miss_suf {20, 9, 9};
   std::array<uint32_t, 2> miss_len {20, 3};
   REQUIRE(w2::find_value(leaf, {miss_stem.data(), 3}).data() == nullptr);
   REQUIRE(w2::find_value(leaf, {miss_suf.data(), 3}).data()  == nullptr);
   REQUIRE(w2::find_value(leaf, {miss_len.data(), 2}).data()  == nullptr);

   w2::release_node(leaf);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — LEAF_FIXED_U16 (Tiny16/Medium16) node type, on its own ────────
// Build the u16 range-encoded leaf directly, verify it is transparently
// substitutable for a u32 leaf_fixed (same keys → same find results, same
// misses), and that runtime detection (g_leaf_u16) produces it from build_leaf.
TEST_CASE("wtree2 leaf_fixed_u16 build + find (substitutable)", "[wtree2][find][u16]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-u16");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base_objs = fx.live_objects();
   auto rd = [](std::span<const uint8_t> s) { return std::vector<uint8_t>(s.begin(), s.end()); };

   // Single-word u32 keys in a tight (<2^16) window → u16-encodable; 8-byte values.
   const uint32_t            B    = 1'000'000;
   std::vector<uint32_t>     keys = {B + 0, B + 7, B + 100, B + 5000, B + 60000};
   std::vector<std::array<uint8_t, 8>> vals;
   for (size_t i = 0; i < keys.size(); ++i) vals.push_back({uint8_t(i + 1), 2, 3, 4, 5, 6, 7, 8});
   std::vector<w2::leaf_entry> ents;
   for (size_t i = 0; i < keys.size(); ++i)
      ents.push_back({{&keys[i], 1}, {vals[i].data(), 8}});

   // ── build the node directly ──
   w2::node2* u16 = w2::build_leaf_u16({ents.data(), ents.size()}, 0, 0, 0, 0, 8, {});
   REQUIRE(u16->is_leaf());
   REQUIRE(u16->is_leaf_u16());
   REQUIRE_FALSE(u16->is_leaf_fixed());
   REQUIRE(u16->num_stems == 5);
   REQUIRE(u16->fx_base() == B);                         // range base
   REQUIRE(u16->fx_val_len == 8);

   // ── reference u32 leaf with the same entries ──
   w2::node2* u32 = w2::build_leaf_fixed({ents.data(), ents.size()}, 0, 0, 0, 0, 8, {});
   REQUIRE(u32->is_leaf_fixed());

   // Substitutable: same keys → identical values from both nodes.
   for (size_t i = 0; i < keys.size(); ++i)
   {
      auto a = rd(w2::find_value(u16, {&keys[i], 1}));
      auto b = rd(w2::find_value(u32, {&keys[i], 1}));
      REQUIRE(a == std::vector<uint8_t>(vals[i].begin(), vals[i].end()));
      REQUIRE(a == b);
   }
   // Misses: in-window absent stem, below window, above window — all absent in both.
   for (uint32_t mq : {B + 50u, B - 1u, B + 70000u})
   {
      REQUIRE(w2::find_value(u16, {&mq, 1}).data() == nullptr);
      REQUIRE(w2::find_value(u32, {&mq, 1}).data() == nullptr);
   }

   // ── runtime detection via build_leaf when enabled ──
   w2::g_leaf_u16 = true;
   w2::node2* det = w2::build_leaf({ents.data(), ents.size()}, 0);
   REQUIRE(det->is_leaf_u16());                          // range fits → u16 chosen
   w2::g_leaf_u16 = false;
   w2::node2* det32 = w2::build_leaf({ents.data(), ents.size()}, 0);
   REQUIRE(det32->is_leaf_fixed());                      // toggle off → u32
   REQUIRE(rd(w2::find_value(det, {&keys[2], 1})) == rd(w2::find_value(det32, {&keys[2], 1})));

   w2::release_node(u16); w2::release_node(u32); w2::release_node(det); w2::release_node(det32);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base_objs);
}

// ── wtree2 — MVCC snapshot isolation via shared-mode COW ───────────────────
// Retain the root (a snapshot) so subsequent upserts see ref>1 and FORK the path
// (unique→shared flip). Verify the snapshot keeps the old keys unchanged while the
// live tree gets the new ones, and that refcount-aware teardown leaves no leak.
TEST_CASE("wtree2 snapshot shared-mode COW isolation", "[wtree2][insert][snapshot]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-snap");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base_objs = fx.live_objects();

   auto val_of = [](uint32_t x) { return uint64_t(x) * 1315423911ull + 1; };
   auto put = [&](w2::ptr_address r, uint32_t x) {
      uint64_t       v  = val_of(x);
      sal::read_lock lk = fx.ses->lock();
      return w2::insert(r, {&x, 1}, {reinterpret_cast<const uint8_t*>(&v), 8});
   };
   auto get = [&](w2::node2* r, uint32_t x) -> int64_t {     // -1 = absent
      sal::read_lock lk = fx.ses->lock();
      auto v = w2::find_value(r, {&x, 1});
      if (!v.data()) return -1;
      uint64_t g; std::memcpy(&g, v.data(), 8);
      return int64_t(g);
   };

   const uint32_t M0 = 2000, ADD = 500;           // [0,M0) snapshotted; [M0,M0+ADD) live-only
   w2::ptr_address root = w2::kNoChild;
   for (uint32_t x = 0; x < M0; ++x) root = put(root, x);

   { sal::read_lock lk = fx.ses->lock(); fx.ses->retain(root); }   // snapshot: ref → 2
   const w2::ptr_address snap = root;

   for (uint32_t x = M0; x < M0 + ADD; ++x) root = put(root, x);   // forks the path (shared)
   REQUIRE(root != snap);                                          // live forked to a new root

   {
      sal::read_lock lk = fx.ses->lock();
      w2::node2* snr = w2::w2deref(snap);
      w2::node2* lvr = w2::w2deref(root);
      for (uint32_t x = 0; x < M0; ++x)            // original keys: in BOTH, unchanged
      {
         REQUIRE(get(snr, x) == int64_t(val_of(x)));
         REQUIRE(get(lvr, x) == int64_t(val_of(x)));
      }
      for (uint32_t x = M0; x < M0 + ADD; ++x)     // new keys: live only, snapshot isolated
      {
         REQUIRE(get(snr, x) == -1);
         REQUIRE(get(lvr, x) == int64_t(val_of(x)));
      }
   }

   { sal::read_lock lk = fx.ses->lock();           // refcount-aware teardown of both trees
     w2::release_subtree(root); w2::release_subtree(snap); }
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base_objs);
}

// ── wtree2 — incremental insert, Increment A (in-place leaf + duplicate) ───
// Keys share a 5-word prefix and vary in the 6th word, so every key routes to a
// single leaf (no Patricia split, no overflow) — exercising descent, gathered
// rebuild, in-place growth, duplicate update, and miss.
TEST_CASE("wtree2 insert leaf in-place + duplicate", "[wtree2][insert]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-insert-a");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   constexpr uint16_t N = 50;
   auto mk_key = [](uint16_t i) {
      return std::array<uint32_t, 6>{7, 7, 7, 7, 7, uint32_t(i) * 3u + 1u};
   };

   w2::ptr_address root = w2::kNoChild;
   {
      sal::read_lock lk = fx.ses->lock();
      for (uint16_t i = 0; i < N; ++i)
      {
         auto    k = mk_key(i);
         uint8_t v = uint8_t(i);
         root = w2::insert(root, {k.data(), 6}, {&v, 1});
      }
   }

   auto find1 = [&](std::array<uint32_t, 6> k) {
      sal::read_lock lk = fx.ses->lock();
      return w2::find_value(w2::w2deref(root), {k.data(), 6});
   };

   // All present with the right value.
   for (uint16_t i = 0; i < N; ++i)
   {
      auto v = find1(mk_key(i));
      REQUIRE(v.size() == 1);
      REQUIRE(v[0] == uint8_t(i));
   }
   // Duplicate → update in place.
   {
      sal::read_lock lk = fx.ses->lock();
      auto    k = mk_key(1);
      uint8_t v = 99;
      root = w2::insert(root, {k.data(), 6}, {&v, 1});
   }
   {
      auto v = find1(mk_key(1));
      REQUIRE(v.size() == 1);
      REQUIRE(v[0] == 99);
   }
   // Miss.
   REQUIRE(find1({7, 7, 7, 7, 7, 99999}).data() == nullptr);

   {
      sal::read_lock lk = fx.ses->lock();
      w2::release_subtree(w2::w2deref(root));
   }
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — adaptive leaf: uniform→FIXED, size change UPGRADES to dynamic ──
// A leaf of equal-sized values is LEAF_FIXED (strided, fast read). Inserting a
// differently-sized value must upgrade the leaf to the dynamic (heap+slot) form
// and keep every entry (old uniform + new odd-sized) correct.
TEST_CASE("wtree2 leaf upgrades fixed->dynamic on size change", "[wtree2][insert]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-upgrade");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   auto mk = [](uint32_t last) { return std::array<uint32_t, 6>{9, 9, 9, 9, 9, last}; };
   auto rd = [](std::span<const uint8_t> s) { return std::vector<uint8_t>(s.begin(), s.end()); };

   constexpr uint16_t N = 30;
   w2::ptr_address    root = w2::kNoChild;
   {
      sal::read_lock lk = fx.ses->lock();
      for (uint16_t i = 0; i < N; ++i)                 // uniform 8-byte values → FIXED
      {
         auto     k = mk(uint32_t(i) * 2u + 1u);
         uint64_t v = 0x1000u + i;
         root = w2::insert(root, {k.data(), 6}, {reinterpret_cast<const uint8_t*>(&v), 8});
      }
   }
   REQUIRE(w2::w2deref(root)->is_leaf_fixed());        // uniform → fixed form

   // Insert a 3-byte value into the same leaf → must upgrade to dynamic.
   {
      sal::read_lock lk = fx.ses->lock();
      auto    k = mk(777);
      uint8_t v3[3] = {0xAA, 0xBB, 0xCC};
      root = w2::insert(root, {k.data(), 6}, {v3, 3});
   }
   REQUIRE(!w2::w2deref(root)->is_leaf_fixed());       // upgraded to dynamic (varlen)

   // All originals (8 B) still correct, plus the odd 3-B entry.
   {
      sal::read_lock lk = fx.ses->lock();
      w2::node2*     r  = w2::w2deref(root);
      for (uint16_t i = 0; i < N; ++i)
      {
         auto k = mk(uint32_t(i) * 2u + 1u);
         auto v = w2::find_value(r, {k.data(), 6});
         REQUIRE(v.size() == 8);
         uint64_t got = 0; std::memcpy(&got, v.data(), 8);
         REQUIRE(got == 0x1000u + i);
      }
      auto k = mk(777);
      REQUIRE(rd(w2::find_value(r, {k.data(), 6})) == std::vector<uint8_t>{0xAA, 0xBB, 0xCC});
   }
   {
      sal::read_lock lk = fx.ses->lock();
      w2::release_subtree(w2::w2deref(root));
   }
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — incremental insert integration (random keys, all cases) ───
// Inserts random 24-B keys one at a time into an empty tree, exercising Patricia
// split (§7.2), overflow fanout split (§7.1), and collisions. Verifies every key
// is findable with the right value, updates are honored, and absent keys miss.
TEST_CASE("wtree2 insert random integration", "[wtree2][insert]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-insert-rnd");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   auto splitmix = [](uint64_t x) {
      x += 0x9E3779B97F4A7C15ULL;
      x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
      x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
      return x ^ (x >> 31);
   };
   auto mk_key = [&](uint64_t i) {
      std::array<uint32_t, 6> k;
      uint64_t h0 = splitmix(i ^ 0xA1B2C3D4ULL), h1 = splitmix(i ^ 0xDEADBEEFULL),
               h2 = splitmix(i ^ 0x12345678ULL);
      std::memcpy(k.data() + 0, &h0, 8);
      std::memcpy(k.data() + 2, &h1, 8);
      std::memcpy(k.data() + 4, &h2, 8);
      return k;
   };

   constexpr uint64_t N = 40000;
   w2::ptr_address    root = w2::kNoChild;
   {
      sal::read_lock lk = fx.ses->lock();
      for (uint64_t i = 0; i < N; ++i)
      {
         auto     k = mk_key(i);
         uint64_t v = i + 1;
         root = w2::insert(root, {k.data(), 6},
                           {reinterpret_cast<const uint8_t*>(&v), 8});
      }
   }

   {
      sal::read_lock lk   = fx.ses->lock();
      w2::node2*     r    = w2::w2deref(root);
      size_t         miss = 0, mism = 0;
      for (uint64_t i = 0; i < N; ++i)
      {
         auto k = mk_key(i);
         auto v = w2::find_value(r, {k.data(), 6});
         if (v.data() == nullptr) { ++miss; continue; }
         uint64_t got = 0;
         std::memcpy(&got, v.data(), 8);
         if (v.size() != 8 || got != i + 1) ++mism;
      }
      REQUIRE(miss == 0);
      REQUIRE(mism == 0);
      // absent key
      auto k = mk_key(N + 12345);
      REQUIRE(w2::find_value(r, {k.data(), 6}).data() == nullptr);
   }

   // Update a sample of keys, re-find.
   {
      sal::read_lock lk = fx.ses->lock();
      for (uint64_t i = 0; i < N; i += 1000)
      {
         auto     k = mk_key(i);
         uint64_t v = 0xF00D0000ULL + i;
         root = w2::insert(root, {k.data(), 6},
                           {reinterpret_cast<const uint8_t*>(&v), 8});
      }
      w2::node2* r = w2::w2deref(root);
      for (uint64_t i = 0; i < N; i += 1000)
      {
         auto     k   = mk_key(i);
         auto     v   = w2::find_value(r, {k.data(), 6});
         uint64_t got = 0;
         std::memcpy(&got, v.data(), 8);
         REQUIRE(got == 0xF00D0000ULL + i);
      }
   }

   {
      sal::read_lock lk = fx.ses->lock();
      w2::release_subtree(w2::w2deref(root));
   }
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

// ── wtree2 — recursive bulk_build correctness at scale ─────────────────

TEST_CASE("wtree2 bulk_build + find correctness (100K)", "[wtree2][bulk]")
{
   namespace w2 = psitri::wtree2;
   sal_fixture fx("wtree2-bulk");
   fx.alloc->register_type_ops<w2::node2>();
   w2::g_wtree2_session = fx.ses.get();
   const uint64_t base = fx.live_objects();

   struct Ent { std::array<uint32_t, 6> key; uint64_t val; };
   const uint32_t N = 100'000;
   std::vector<Ent> ents(N);
   for (uint32_t i = 0; i < N; ++i)
   {
      uint8_t k[24]; make_key24(i, k);
      std::memcpy(ents[i].key.data(), k, 24);
      ents[i].val = uint64_t(i) * 10 + 1;
   }
   std::sort(ents.begin(), ents.end(), [](const Ent& a, const Ent& b) { return a.key < b.key; });
   ents.erase(std::unique(ents.begin(), ents.end(),
                          [](const Ent& a, const Ent& b) { return a.key == b.key; }),
              ents.end());

   std::vector<w2::leaf_entry> le;
   le.reserve(ents.size());
   for (auto& e : ents)
      le.push_back({{e.key.data(), 6},
                    {reinterpret_cast<const uint8_t*>(&e.val), 8}});

   w2::node2* root = w2::w2deref(w2::bulk_build({le.data(), le.size()}, 0));

   size_t miss = 0, mism = 0;
   for (auto& e : ents)
   {
      auto v = w2::find_value(root, {e.key.data(), 6});
      if (v.data() == nullptr) { ++miss; continue; }
      uint64_t got = 0; std::memcpy(&got, v.data(), std::min<size_t>(v.size(), 8));
      if (v.size() != 8 || got != e.val) ++mism;
   }
   INFO("N=" << ents.size() << " miss=" << miss << " mism=" << mism);
   REQUIRE(miss == 0);
   REQUIRE(mism == 0);

   w2::release_subtree(root);
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

TEST_CASE("wtree no leak across repeated build/destroy", "[wtree][leak]")
{
   sal_fixture fx("leak-repeat");
   for (int round = 0; round < 5; ++round)
   {
      {
         wtree t(*fx.ses, 8);
         for (uint64_t i = 0; i < 50'000; ++i)
         {
            uint8_t k[24]; make_key24(i + uint64_t(round) * 1'000'000, k);
            const uint64_t v = i + 1;
            t.upsert(kview(k, 24), vview(v));
         }
      }
      fx.wait_for_compactor();
      INFO("round=" << round << " still-live=" << fx.live_objects());
      REQUIRE(fx.live_objects() == 0);
   }
}
