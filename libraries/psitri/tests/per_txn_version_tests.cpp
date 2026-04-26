// Per-txn version + value_node coalescing tests.
//
// Validates that try_mvcc_upsert / try_mvcc_remove collapse repeated writes
// to the same key under a fixed version into a single chain entry instead
// of growing the chain linearly. This is the building block that makes
// per-txn versioning viable: a transaction that writes the same key 100
// times produces 1 chain entry, not 100.

#include <catch2/catch_all.hpp>
#include <chrono>
#include <psitri/cursor.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <sal/sal.hpp>
#include <unistd.h>

using namespace psitri;
using sal::alloc_header;
using sal::smart_ptr;

namespace
{
   struct ptv_db
   {
      std::filesystem::path           dir;
      std::unique_ptr<sal::allocator> alloc;
      sal::allocator_session_ptr      ses{nullptr};

      ptv_db()
      {
         auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
         dir     = std::filesystem::temp_directory_path() /
               ("psitri_ptv_" + std::to_string(getpid()) + "_" +
                std::to_string(ts));
         std::filesystem::remove_all(dir);
         alloc = std::make_unique<sal::allocator>(dir, sal::runtime_config());
         sal::register_type_vtable<leaf_node>();
         sal::register_type_vtable<inner_prefix_node>();
         sal::register_type_vtable<inner_node>();
         sal::register_type_vtable<value_node>();
         ses = alloc->get_session();
      }
      ~ptv_db() { std::filesystem::remove_all(dir); }

      smart_ptr<alloc_header> root()
      {
         return ses->get_root<>(sal::root_object_number(0));
      }
      void set_root(smart_ptr<alloc_header> r)
      {
         ses->set_root(sal::root_object_number(0), std::move(r), sal::sync_type::none);
      }
   };

   key_view   to_kv(const std::string& s) { return key_view(s.data(), s.size()); }
   value_view to_vv(const std::string& s) { return value_view(s.data(), s.size()); }

   // Total version-chain entries across all value_nodes in the tree.
   // For these tests we only have one key, so this equals that key's
   // chain length.
   uint64_t total_chain_entries(tree_context& ctx)
   {
      auto s = ctx.get_stats();
      return s.total_version_entries;
   }

   uint64_t value_node_count(tree_context& ctx)
   {
      auto s = ctx.get_stats();
      return s.value_nodes;
   }
}  // namespace

TEST_CASE("per-txn version: repeated upserts at same version coalesce",
          "[per_txn][coalesce]")
{
   ptv_db db;

   // Seed the tree so key1 has an inline value at version 0.
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key1"), value_type(to_vv("v0")));
      db.set_root(ctx.take_root());
   }

   // First MVCC upsert at version 5 — promotes inline → value_node with
   // 2 entries: (0, "v0"), (5, "v5a")
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_current_epoch(0);
      bool ok = ctx.try_mvcc_upsert(to_kv("key1"), value_type(to_vv("v5a")), 5);
      REQUIRE(ok);
      db.set_root(ctx.take_root());
   }
   {
      tree_context ctx(db.root());
      CHECK(value_node_count(ctx) == 1);
      CHECK(total_chain_entries(ctx) == 2);
   }

   // Second upsert at SAME version 5 — coalesces with the top entry.
   // Chain stays at 2 entries: (0, "v0"), (5, "v5b").
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_current_epoch(0);
      bool ok = ctx.try_mvcc_upsert(to_kv("key1"), value_type(to_vv("v5b")), 5);
      REQUIRE(ok);
      db.set_root(ctx.take_root());
   }
   {
      tree_context ctx(db.root());
      CHECK(value_node_count(ctx) == 1);
      CHECK(total_chain_entries(ctx) == 2);
      cursor c(ctx.get_root());
      REQUIRE(c.seek(to_kv("key1")));
      auto v = c.value<std::string>();
      REQUIRE(v.has_value());
      CHECK(*v == "v5b");
   }

   // Third upsert at version 6 — new version, appends. Chain now 3 entries.
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_current_epoch(0);
      bool ok = ctx.try_mvcc_upsert(to_kv("key1"), value_type(to_vv("v6")), 6);
      REQUIRE(ok);
      db.set_root(ctx.take_root());
   }
   {
      tree_context ctx(db.root());
      CHECK(value_node_count(ctx) == 1);
      CHECK(total_chain_entries(ctx) == 3);
   }
}

TEST_CASE("per-txn version: 100 writes at same version produce 1 chain entry",
          "[per_txn][coalesce]")
{
   ptv_db db;

   // Seed key1 with an inline value, then do one upsert at v=42 to promote
   // to a value_node (2 entries).
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key1"), value_type(to_vv("v0")));
      db.set_root(ctx.take_root());
   }
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_current_epoch(0);
      bool ok =
          ctx.try_mvcc_upsert(to_kv("key1"), value_type(to_vv("first")), 42);
      REQUIRE(ok);
      db.set_root(ctx.take_root());
   }

   // 99 more upserts at the same version 42. Each should coalesce with the
   // previous one. Chain length stays at 2.
   for (int i = 1; i < 100; ++i)
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_current_epoch(0);
      char val[16];
      std::snprintf(val, sizeof(val), "iter_%d", i);
      bool ok = ctx.try_mvcc_upsert(
          to_kv("key1"), value_type(value_view(val, std::strlen(val))), 42);
      REQUIRE(ok);
      db.set_root(ctx.take_root());
   }

   // Final state: chain still 2 entries (initial v=0 + coalesced v=42).
   {
      tree_context ctx(db.root());
      CHECK(value_node_count(ctx) == 1);
      CHECK(total_chain_entries(ctx) == 2);
      cursor c(ctx.get_root());
      REQUIRE(c.seek(to_kv("key1")));
      auto v = c.value<std::string>();
      REQUIRE(v.has_value());
      // The last write wins — should be "iter_99".
      CHECK(*v == "iter_99");
   }
}

TEST_CASE("per-txn version: tombstone coalesces too",
          "[per_txn][coalesce]")
{
   ptv_db db;

   // Seed and promote to value_node with 2 entries.
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key1"), value_type(to_vv("v0")));
      db.set_root(ctx.take_root());
   }
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_current_epoch(0);
      ctx.try_mvcc_upsert(to_kv("key1"), value_type(to_vv("v7")), 7);
      db.set_root(ctx.take_root());
   }

   // Remove at version 7 — coalesces with the v=7 data entry, replacing it
   // with a tombstone. Chain stays at 2 entries: (0, v0), (7, tombstone).
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_current_epoch(0);
      bool ok = ctx.try_mvcc_remove(to_kv("key1"), 7);
      REQUIRE(ok);
      db.set_root(ctx.take_root());
   }
   {
      tree_context ctx(db.root());
      CHECK(value_node_count(ctx) == 1);
      CHECK(total_chain_entries(ctx) == 2);
      cursor c(ctx.get_root());
      // After tombstone, seeking returns false (key is dead at latest version).
      bool found = c.seek(to_kv("key1"));
      if (found)
      {
         auto v = c.value<std::string>();
         CHECK(!v.has_value());
      }
   }
}
