// wtree_tests — the one psitri::wtree engine (clean rewrite, branch wtree-rewrite).
// Stage A: typed-node foundation — single `tiny` form (u32), leaf build + point find.
// Inner routing, the tier×width matrix, splits, and the varlen value model land in
// later stages. Leak idiom: drop the tree, drain the compactor, REQUIRE 0 live objects.

#include <catch2/catch_all.hpp>

#include <psitri/wtree/wtree.hpp>

#include <sal/allocator.hpp>
#include <sal/allocator_impl.hpp>
#include <sal/allocator_session.hpp>
#include <sal/allocator_session_impl.hpp>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <vector>

namespace w = psitri::wtree;

namespace
{
   struct sal_fixture
   {
      std::filesystem::path           dir;
      std::unique_ptr<sal::allocator> alloc;
      sal::allocator_session_ptr      ses;

      explicit sal_fixture(const std::string& name)
          : dir(std::filesystem::temp_directory_path() /
                ("wtree_tests-" + name + "-" + std::to_string(::getpid()))),
            ses(nullptr)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir);
         alloc = std::make_unique<sal::allocator>(dir, sal::runtime_config{});
         alloc->register_type_ops<w::tiny>();
         ses              = alloc->get_session();
         w::g_session = ses.get();
      }
      ~sal_fixture()
      {
         ses = sal::allocator_session_ptr(nullptr);
         alloc.reset();
         std::error_code ec;
         std::filesystem::remove_all(dir, ec);
      }
      uint64_t live_objects() const { return ses->get_total_allocated_objects(); }

      // Poll until the compactor has drained pending (deferred) releases.
      void wait_for_compactor(int timeout_ms = 10000)
      {
         using namespace std::chrono;
         auto deadline = steady_clock::now() + milliseconds(timeout_ms);
         while (steady_clock::now() < deadline)
         {
            if (alloc->total_pending_releases() == 0)
            {
               std::this_thread::sleep_for(milliseconds(20));
               if (alloc->total_pending_releases() == 0) return;
            }
            std::this_thread::sleep_for(milliseconds(5));
         }
      }
   };
}  // namespace

TEST_CASE("wtree tiny leaf build + point find", "[wtree][tiny]")
{
   sal_fixture fx("tiny");
   const uint64_t base = fx.live_objects();

   std::vector<uint32_t> keys = {10, 20, 30, 40, 50};
   std::vector<uint64_t> vals = {100, 200, 300, 400, 500};

   w::ptr_address root;
   {
      sal::read_lock lk = fx.ses->lock();
      root              = w::build_tiny_leaf(keys, vals);
   }
   w::tiny* nd = w::deref<w::tiny>(root);

   REQUIRE(nd->is_leaf());
   REQUIRE(nd->num_stems() == 5);

   for (size_t i = 0; i < keys.size(); ++i)
   {
      const uint8_t* v = nd->find_value(keys[i]);
      REQUIRE(v != nullptr);
      uint64_t got = 0;
      std::memcpy(&got, v, 8);
      REQUIRE(got == vals[i]);
   }
   // misses: below, between, above
   REQUIRE(nd->find_value(5) == nullptr);
   REQUIRE(nd->find_value(25) == nullptr);
   REQUIRE(nd->find_value(99) == nullptr);

   {
      sal::read_lock lk = fx.ses->lock();
      w::session().release(root);
   }
   fx.wait_for_compactor();                 // release is deferred to the compactor
   REQUIRE(fx.live_objects() == base);
}

TEST_CASE("wtree two-level descent + routing", "[wtree][tiny][descent]")
{
   sal_fixture fx("descent");
   const uint64_t base = fx.live_objects();

   // Two leaves split at divider 100: L = [10,20,30], R = [100,200,300].
   std::vector<uint32_t> lk = {10, 20, 30}, rk = {100, 200, 300};
   std::vector<uint64_t> lv = {1, 2, 3}, rv = {11, 22, 33};
   w::ptr_address root;
   {
      sal::read_lock lk_ = fx.ses->lock();
      w::ptr_address L = w::build_tiny_leaf(lk, lv);
      w::ptr_address R = w::build_tiny_leaf(rk, rv);
      uint32_t          div  = 100;                 // R's first key
      w::ptr_address    kids[2] = {L, R};
      root = w::build_tiny_inner(/*height*/ 1, {&div, 1}, {kids, 2});
   }
   w::node_base* rn = w::deref<w::node_base>(root);
   REQUIRE(rn->is_inner());
   REQUIRE(rn->num_stems() == 1);

   auto check = [&](uint32_t k, uint64_t want) {
      const uint8_t* v = w::find(root, k);
      REQUIRE(v != nullptr);
      uint64_t got = 0; std::memcpy(&got, v, 8);
      REQUIRE(got == want);
   };
   check(10, 1); check(30, 3); check(100, 11); check(300, 33);   // both halves route correctly
   REQUIRE(w::find(root, 50) == nullptr);    // gap in L's range
   REQUIRE(w::find(root, 5)  == nullptr);    // below all
   REQUIRE(w::find(root, 999) == nullptr);   // above all

   {
      sal::read_lock lk_ = fx.ses->lock();
      w::session().release(root);            // recursive: releases the two leaves too
   }
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}

TEST_CASE("wtree incremental insert: splits + multi-level round-trip", "[wtree][tiny][insert]")
{
   sal_fixture fx("insert");
   const uint64_t base = fx.live_objects();

   // Hash-scrambled keys so inserts arrive in random order and force leaf + inner
   // splits (kTinyCap=256, so >256 keys → multiple leaves → an inner; >256*256 → 2
   // levels — we use 5000 to get a few levels).
   const uint32_t N = 5000;
   auto scramble = [](uint32_t i) { return uint32_t(0x9E3779B1u * (i + 1)); };
   auto val_of   = [](uint32_t k) { return uint64_t(k) * 1315423911ull + 7; };

   w::ptr_address root = w::kNoChild;
   {
      sal::read_lock lk = fx.ses->lock();
      for (uint32_t i = 0; i < N; ++i)
      {
         const uint32_t k = scramble(i);
         root             = w::insert(root, k, val_of(k));
      }
   }
   // Every key present with the right value; a deeper tree formed.
   w::node_base* rn = w::deref<w::node_base>(root);
   REQUIRE(rn->is_inner());
   size_t miss = 0, mism = 0;
   {
      sal::read_lock lk = fx.ses->lock();
      for (uint32_t i = 0; i < N; ++i)
      {
         const uint32_t k = scramble(i);
         const uint8_t* v = w::find(root, k);
         if (!v) { ++miss; continue; }
         uint64_t got = 0; std::memcpy(&got, v, 8);
         if (got != val_of(k)) ++mism;
      }
      // a few keys that were never inserted must miss
      REQUIRE(w::find(root, 0xDEADBEEFu) == nullptr);
      REQUIRE(w::find(root, 0xFFFFFFFEu) == nullptr);
   }
   REQUIRE(miss == 0);
   REQUIRE(mism == 0);

   {
      sal::read_lock lk = fx.ses->lock();
      w::session().release(root);
   }
   fx.wait_for_compactor();
   REQUIRE(fx.live_objects() == base);
}
