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
      const uint8_t* v = w::find_in_tiny_leaf(nd, keys[i]);
      REQUIRE(v != nullptr);
      uint64_t got = 0;
      std::memcpy(&got, v, 8);
      REQUIRE(got == vals[i]);
   }
   // misses: below, between, above
   REQUIRE(w::find_in_tiny_leaf(nd, 5) == nullptr);
   REQUIRE(w::find_in_tiny_leaf(nd, 25) == nullptr);
   REQUIRE(w::find_in_tiny_leaf(nd, 99) == nullptr);

   {
      sal::read_lock lk = fx.ses->lock();
      w::session().release(root);
   }
   fx.wait_for_compactor();                 // release is deferred to the compactor
   REQUIRE(fx.live_objects() == base);
}
