// Per-txn version + value_node coalescing tests.
//
// Validates that try_upsert_at_version / try_remove_at_version collapse repeated writes
// to the same key under a fixed version into a single chain entry instead
// of growing the chain linearly. This is the building block that makes
// per-txn versioning viable: a transaction that writes the same key 100
// times produces 1 chain entry, not 100.

#include <unistd.h>
#include <catch2/catch_all.hpp>
#include <chrono>
#include <psitri/cursor.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <sal/sal.hpp>

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
                   ("psitri_ptv_" + std::to_string(getpid()) + "_" + std::to_string(ts));
         std::filesystem::remove_all(dir);
         alloc = std::make_unique<sal::allocator>(dir, sal::runtime_config());
         detail::register_node_types(*alloc);
         ses = alloc->get_session();
      }
      ~ptv_db() { std::filesystem::remove_all(dir); }

      smart_ptr<alloc_header> root() { return ses->get_root<>(sal::root_object_number(0)); }
      void                    set_root(smart_ptr<alloc_header> r)
      {
         ses->set_root(sal::root_object_number(0), std::move(r), sal::sync_type::none);
      }
   };

   key_view to_kv(const std::string& s)
   {
      return key_view(s.data(), s.size());
   }
   value_view to_vv(const std::string& s)
   {
      return value_view(s.data(), s.size());
   }

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

TEST_CASE("per-txn version: repeated upserts at same version coalesce", "[per_txn][coalesce]")
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
      ctx.set_epoch_base(0);
      bool ok = ctx.try_upsert_at_version(to_kv("key1"), value_type(to_vv("v5a")), 5);
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
      ctx.set_epoch_base(0);
      bool ok = ctx.try_upsert_at_version(to_kv("key1"), value_type(to_vv("v5b")), 5);
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
      ctx.set_epoch_base(0);
      bool ok = ctx.try_upsert_at_version(to_kv("key1"), value_type(to_vv("v6")), 6);
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
      ctx.set_epoch_base(0);
      bool ok = ctx.try_upsert_at_version(to_kv("key1"), value_type(to_vv("first")), 42);
      REQUIRE(ok);
      db.set_root(ctx.take_root());
   }

   // 99 more upserts at the same version 42. Each should coalesce with the
   // previous one. Chain length stays at 2.
   for (int i = 1; i < 100; ++i)
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_epoch_base(0);
      char val[16];
      std::snprintf(val, sizeof(val), "iter_%d", i);
      bool ok =
          ctx.try_upsert_at_version(to_kv("key1"), value_type(value_view(val, std::strlen(val))), 42);
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

TEST_CASE("per-txn version: tombstone coalesces too", "[per_txn][coalesce]")
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
      ctx.set_epoch_base(0);
      ctx.try_upsert_at_version(to_kv("key1"), value_type(to_vv("v7")), 7);
      db.set_root(ctx.take_root());
   }

   // Remove at version 7 — coalesces with the v=7 data entry, replacing it
   // with a tombstone. Chain stays at 2 entries: (0, v0), (7, tombstone).
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(nullptr);
      ctx.set_epoch_base(0);
      bool ok = ctx.try_remove_at_version(to_kv("key1"), 7);
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

TEST_CASE("per-txn version: cursor skips value_node entries newer than its version",
          "[per_txn][cursor]")
{
   ptv_db db;

   const std::string future_v5 = "future-v5";
   const std::string future_v6 = "future-v6";

   {
      auto vn_addr =
          db.ses->alloc<value_node>(uint64_t(5), to_vv(future_v5), uint64_t(6), to_vv(future_v6));

      tree_context ctx(db.root());
      ctx.insert(to_kv("future"), value_type::make_value_node(vn_addr));
      ctx.insert(to_kv("later"), value_type(to_vv("visible")));
      db.set_root(ctx.take_root());
   }

   auto root = db.root();

   cursor before(root, 4);
   CHECK_FALSE(before.seek(to_kv("future")));
   REQUIRE(before.lower_bound(to_kv("future")));
   CHECK(std::string(before.key().data(), before.key().size()) == "later");

   std::string buf;
   CHECK(before.get(to_kv("future"), &buf) == cursor::value_not_found);

   bool called = false;
   CHECK_FALSE(before.get(to_kv("future"), [&](value_view) { called = true; }));
   CHECK_FALSE(called);

   cursor at_v5(root, 5);
   REQUIRE(at_v5.seek(to_kv("future")));
   auto visible = at_v5.value<std::string>();
   REQUIRE(visible.has_value());
   CHECK(*visible == future_v5);
}

// ════════════════════════════════════════════════════════════════════
// End-to-end: Phase A per-txn version flow through the public API
// ════════════════════════════════════════════════════════════════════

namespace
{
   struct ptv_pubapi_db
   {
      std::filesystem::path          dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      ptv_pubapi_db()
      {
         auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
         dir     = std::filesystem::temp_directory_path() /
                   ("psitri_ptv_pub_" + std::to_string(getpid()) + "_" + std::to_string(ts));
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir / "data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }
      ~ptv_pubapi_db() { std::filesystem::remove_all(dir); }

      uint64_t chain_length_of(uint32_t root_index, key_view key)
      {
         auto root = ses->get_root(root_index);
         if (!root)
            return 0;
         tree_context ctx(root);
         auto         s = ctx.get_stats();
         // Single-key trees: total_version_entries == this key's chain length
         return s.total_version_entries;
      }

      uint64_t global_version() const { return db->dump().total_segments; /* unused */ }
   };
}  // namespace

TEST_CASE("Phase A: unique transaction updates collapse to one current value",
          "[per_txn][phaseA][unique]")
{
   ptv_pubapi_db t;

   // Use values >64 bytes so make_value forces a value_node allocation.
   // (Smaller values stay inline in the leaf and never form a chain.)
   auto big = [](char c, int idx)
   {
      std::string s(80, c);
      char        buf[32];
      std::snprintf(buf, sizeof(buf), "_%03d", idx);
      s.append(buf);
      return s;
   };

   // First txn: insert "k" with initial value. Forces value_node (1 entry).
   {
      auto tx = t.ses->start_transaction(0);
      auto v0 = big('A', 0);
      tx.upsert(to_kv("k"), value_view(v0.data(), v0.size()));
      tx.commit();
   }
   CHECK(t.chain_length_of(0, to_kv("k")) == 1);

   // Second txn: 100 updates to the same key. The transaction owns the path
   // to the leaf, so older readers are on older roots. The hot leaf should
   // keep only the current value for this key instead of accumulating a
   // private version chain.
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 100; ++i)
      {
         auto v = big('B', i);
         tx.upsert(to_kv("k"), value_view(v.data(), v.size()));
      }
      tx.commit();
   }

   // Exactly 1 chain entry: the latest current value in the unique leaf.
   auto chain = t.chain_length_of(0, to_kv("k"));
   INFO("chain entries after 100 same-txn updates: " << chain);
   CHECK(chain == 1);

   // Verify the latest value is the last one written.
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   auto expected = big('B', 99);
   CHECK(*v == expected);
}

TEST_CASE("Phase A: unique transaction update collapses prior MVCC history",
          "[per_txn][phaseA][unique]")
{
   ptv_pubapi_db t;

   auto big = [](char c, int idx)
   {
      std::string s(80, c);
      char        buf[32];
      std::snprintf(buf, sizeof(buf), "_%03d", idx);
      s.append(buf);
      return s;
   };

   {
      auto tx = t.ses->start_transaction(0);
      auto v0 = big('A', 0);
      tx.upsert(to_kv("k"), value_view(v0.data(), v0.size()));
      tx.commit();
   }
   CHECK(t.chain_length_of(0, to_kv("k")) == 1);

   auto mvcc_value = big('M', 1);
   t.ses->upsert(0, to_kv("k"), value_view(mvcc_value.data(), mvcc_value.size()));
   REQUIRE(t.chain_length_of(0, to_kv("k")) == 2);

   auto final_value = big('U', 2);
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), value_view(final_value.data(), final_value.size()));
      tx.commit();
   }

   auto chain = t.chain_length_of(0, to_kv("k"));
   INFO("chain entries after unique update of prior MVCC history: " << chain);
   CHECK(chain == 1);

   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   CHECK(*v == final_value);
}

TEST_CASE("Phase A: shared leaf COW normalizes value history during copy",
          "[per_txn][phaseA][cow][leaf_rewrite]")
{
   ptv_db db;

   auto big = [](char c)
   {
      std::string s(80, c);
      return s;
   };

   {
      tree_context ctx(db.root());
      auto         seed = big('A');
      ctx.insert(to_kv("a"), value_type(to_vv(seed)));
      db.set_root(ctx.take_root());
   }

   {
      tree_context ctx(db.root());
      ctx.set_root_version(2);
      REQUIRE(ctx.try_upsert_at_version(to_kv("a"), value_type(to_vv(big('B'))), 2));
      db.set_root(ctx.take_root());
   }

   {
      tree_context ctx(db.root());
      CHECK(total_chain_entries(ctx) == 2);
   }

   auto old_snapshot = db.root();

   {
      tree_context ctx(db.root());
      ctx.set_epoch_base(0);
      ctx.set_root_version(3);
      ctx.upsert(to_kv("b"), value_type(to_vv("small")));
      db.set_root(ctx.take_root());
   }

   {
      tree_context ctx(db.root());
      CHECK(total_chain_entries(ctx) == 1);
   }

   cursor old_cur(old_snapshot, 2);
   REQUIRE(old_cur.seek(to_kv("a")));
   auto old_val = old_cur.value<std::string>();
   REQUIRE(old_val.has_value());
   CHECK(*old_val == big('B'));
   CHECK_FALSE(old_cur.seek(to_kv("b")));

   auto latest_root = db.root();
   cursor latest(latest_root);
   REQUIRE(latest.seek(to_kv("a")));
   auto latest_a = latest.value<std::string>();
   REQUIRE(latest_a.has_value());
   CHECK(*latest_a == big('B'));
   REQUIRE(latest.seek(to_kv("b")));
   CHECK(latest.value<std::string>().value_or("") == "small");
}

TEST_CASE("Phase A: unique transaction update demotes value_node history to inline",
          "[per_txn][phaseA][unique]")
{
   ptv_pubapi_db t;

   std::string seed(80, 'S');
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), value_view(seed.data(), seed.size()));
      tx.commit();
   }
   CHECK(t.chain_length_of(0, to_kv("k")) == 1);

   std::string mvcc_value(80, 'M');
   t.ses->upsert(0, to_kv("k"), value_view(mvcc_value.data(), mvcc_value.size()));
   REQUIRE(t.chain_length_of(0, to_kv("k")) == 2);

   const std::string inline_value = "fits-inline";
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), to_vv(inline_value));
      tx.commit();
   }

   auto chain = t.chain_length_of(0, to_kv("k"));
   INFO("chain entries after demoting unique value_node history: " << chain);
   CHECK(chain == 0);

   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   CHECK(*v == inline_value);
}

TEST_CASE("Phase A: upsert_at_version new keys stay hidden from older snapshots",
          "[per_txn][phaseA][snapshot]")
{
   ptv_pubapi_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("base"), to_vv("seed"));
      tx.commit();
   }

   auto old_root = t.ses->get_root(0);
   REQUIRE(old_root);
   cursor old_info_cursor(old_root);
   auto   base_info = old_info_cursor.get_key_info(to_kv("base"));
   REQUIRE(base_info.leaf_addr != sal::null_ptr_address);

   const std::string value = "new-value";
   t.ses->upsert(0, to_kv("new"), to_vv(value));

   cursor old_cursor(old_root);
   CHECK_FALSE(old_cursor.seek(to_kv("new")));

   std::string old_buf;
   CHECK(old_cursor.get(to_kv("new"), &old_buf) == cursor::value_not_found);

   auto   latest_root = t.ses->get_root(0);
   cursor latest_cursor(latest_root);
   REQUIRE(latest_cursor.seek(to_kv("new")));
   auto new_info = latest_cursor.get_key_info(to_kv("new"));
   CHECK(new_info.leaf_addr == base_info.leaf_addr);
   auto latest = latest_cursor.value<std::string>();
   REQUIRE(latest.has_value());
   CHECK(*latest == value);
}

TEST_CASE("Phase A: branch creation version survives inline promotion",
          "[per_txn][phaseA][snapshot]")
{
   ptv_pubapi_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("base"), to_vv("seed"));
      tx.commit();
   }

   auto before_new_root = t.ses->get_root(0);
   REQUIRE(before_new_root);

   const std::string first     = "first-inline-value";
   auto              first_ver = t.ses->upsert(0, to_kv("new"), to_vv(first));

   auto after_new_root = t.ses->get_root(0);
   REQUIRE(after_new_root);
   cursor after_new_info_cursor(after_new_root);
   auto   branch_info = after_new_info_cursor.get_key_info(to_kv("new"));
   REQUIRE(branch_info.leaf_addr != sal::null_ptr_address);
   {
      auto leaf = after_new_root.session()->get_ref<leaf_node>(branch_info.leaf_addr);
      auto bn   = leaf->get(to_kv("new"));
      REQUIRE(bn != leaf->num_branches());
      CHECK(leaf->get_version(bn) == first_ver);
   }

   const std::string second(96, 'B');
   auto              second_ver = t.ses->upsert(0, to_kv("new"), to_vv(second));
   REQUIRE(second_ver > first_ver);

   cursor before_new(before_new_root);
   CHECK_FALSE(before_new.seek(to_kv("new")));

   cursor at_creation(after_new_root, first_ver);
   REQUIRE(at_creation.seek(to_kv("new")));
   auto created_value = at_creation.value<std::string>();
   REQUIRE(created_value.has_value());
   CHECK(*created_value == first);

   auto   latest_root = t.ses->get_root(0);
   cursor latest(latest_root);
   REQUIRE(latest.seek(to_kv("new")));
   auto latest_info = latest.get_key_info(to_kv("new"));
   CHECK(latest_info.leaf_addr == branch_info.leaf_addr);
   {
      auto leaf = latest_root.session()->get_ref<leaf_node>(latest_info.leaf_addr);
      auto bn   = leaf->get(to_kv("new"));
      REQUIRE(bn != leaf->num_branches());
      CHECK(leaf->get_version(bn) == first_ver);
   }
   auto latest_value = latest.value<std::string>();
   REQUIRE(latest_value.has_value());
   CHECK(*latest_value == second);
}

// ════════════════════════════════════════════════════════════════════
// Phase B: lazy version allocation for expect_failure
// ════════════════════════════════════════════════════════════════════

namespace
{
   uint64_t global_version_of(database& db)
   {
      // Read the database's global_version atomic via the public stats
      // path. There's no direct accessor, so we infer it from a no-op
      // read_session start (which doesn't bump). Instead use the stats
      // dump's running counter — actually, simplest is to start a
      // throwaway expect_success txn, observe ver, abort, and convert.
      //
      // Cleanest: the tx's primary().get_stats().total_version_entries
      // doesn't directly tell us the global. Use a sentinel txn to
      // sample by allocating a ver and inspecting it.
      auto     ws   = db.start_write_session();
      auto     tx   = ws->start_transaction(0, tx_mode::expect_success);
      auto     root = tx.primary().read_cursor().get_root();
      uint64_t ver  = 0;
      if (root.ver() != sal::null_ptr_address)
         ver = root.session()->read_custom_cb(root.ver());
      tx.abort();
      return ver;
   }
}  // namespace

TEST_CASE("Phase B: expect_failure with no writes does not bump global_version",
          "[per_txn][phaseB][lazy]")
{
   ptv_pubapi_db t;

   auto before = global_version_of(*t.db);
   for (int i = 0; i < 100; ++i)
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.abort();
   }
   auto after = global_version_of(*t.db);

   // Each global_version_of() bumps once (it starts a sentinel
   // expect_success txn). 100 expect_failure aborts in between should
   // bump zero times. So delta == 1 (the second sentinel call).
   INFO("global_version delta: " << (after - before));
   CHECK(after - before == 1);
}

TEST_CASE("Phase B: expect_failure with reads only does not bump global_version",
          "[per_txn][phaseB][lazy]")
{
   ptv_pubapi_db t;

   // Seed a key so reads have something to find.
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), to_vv("seed"));
      tx.commit();
   }

   auto before = global_version_of(*t.db);
   for (int i = 0; i < 50; ++i)
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      auto v  = tx.get<std::string>(to_kv("k"));
      REQUIRE(v.has_value());
      tx.abort();
   }
   auto after = global_version_of(*t.db);

   INFO("global_version delta after read-only aborts: " << (after - before));
   CHECK(after - before == 1);  // just the sentinel, no per-txn bump
}

TEST_CASE("Phase B: expect_failure with writes commits and bumps once", "[per_txn][phaseB][lazy]")
{
   ptv_pubapi_db t;

   auto before = global_version_of(*t.db);
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.upsert(to_kv("a"), to_vv("1"));
      tx.upsert(to_kv("b"), to_vv("2"));
      tx.commit();
   }
   auto after = global_version_of(*t.db);

   // 2 sentinels + 1 actual commit = 3 bumps total.
   // Delta from sentinel-to-sentinel = (sentinel + actual + sentinel) -
   // (sentinel) = sentinel + actual + sentinel - sentinel = sentinel +
   // actual = 2. Wait let me think again...
   //
   // before: sentinel1 fires, ver = N+1.
   // tx commit: ver = N+2.
   // after: sentinel2 fires, ver = N+3. Reads N+3.
   // delta = N+3 - N+1 = 2.
   CHECK(after - before == 2);

   // Verify writes are persisted.
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("a")));
   CHECK(c.value<std::string>().value_or("") == "1");
}

TEST_CASE("Phase B: 1000 start+abort cycles in expect_failure produce zero delta",
          "[per_txn][phaseB][lazy]")
{
   ptv_pubapi_db t;

   auto before = global_version_of(*t.db);
   for (int i = 0; i < 1000; ++i)
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.abort();
   }
   auto after = global_version_of(*t.db);

   // Just the 2 sentinel calls. 1000 aborts contribute nothing.
   CHECK(after - before == 1);
}

// ════════════════════════════════════════════════════════════════════
// Phase C: abort releases the version
// ════════════════════════════════════════════════════════════════════

TEST_CASE("Phase C: aborted expect_success txn registers ver as dead", "[per_txn][phaseC][abort]")
{
   ptv_pubapi_db t;

   // Seed so the slot has a published ver to compare against.
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), to_vv("v0"));
      tx.commit();
   }

   // Sample dead_versions before. Aborted versions should appear in
   // the snapshot after abort.
   t.db->wait_for_compactor(std::chrono::milliseconds(500));
   auto     dead_before       = t.db->dead_versions().load_snapshot();
   uint64_t dead_count_before = 0;
   if (dead_before)
      dead_count_before = dead_before->num_ranges();

   // Run an expect_success txn that does some work then aborts.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_success);
      tx.upsert(to_kv("k2"), to_vv("v_aborted"));
      tx.abort();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(500));
   auto     dead_after       = t.db->dead_versions().load_snapshot();
   uint64_t dead_count_after = 0;
   if (dead_after)
      dead_count_after = dead_after->num_ranges();

   INFO("dead version count before/after abort: " << dead_count_before << " / "
                                                  << dead_count_after);
   CHECK(dead_count_after > dead_count_before);

   // Verify the aborted write didn't make it.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      auto v  = tx.get<std::string>(to_kv("k2"));
      CHECK(!v.has_value());
      tx.abort();
   }
}

TEST_CASE("Phase C: aborted expect_success txn with many mutations leaves no leaks",
          "[per_txn][phaseC][abort]")
{
   ptv_pubapi_db t;

   // Seed a known baseline.
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("baseline"), to_vv("seed"));
      tx.commit();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(2000));
   auto baseline_alloc = t.ses->get_total_allocated_objects();

   // Run a fat expect_success txn doing 1000 mutations, then abort.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_success);
      for (int i = 0; i < 1000; ++i)
      {
         char key[32];
         std::snprintf(key, sizeof(key), "ephemeral_%04d", i);
         tx.upsert(key_view(key, std::strlen(key)), to_vv("ephemeral"));
      }
      tx.abort();
   }

   // Compactor drains the released pages.
   t.db->wait_for_compactor(std::chrono::milliseconds(5000));

   auto post_abort_alloc = t.ses->get_total_allocated_objects();
   INFO("baseline=" << baseline_alloc << " post_abort=" << post_abort_alloc);
   // Allocated count should return to baseline (or very close — defrag
   // may leave slack). Allow a small slop for compactor lag.
   CHECK(post_abort_alloc <= baseline_alloc + 2);

   // Verify the aborted writes didn't persist.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      auto v  = tx.get<std::string>(to_kv("ephemeral_0500"));
      CHECK(!v.has_value());
      tx.abort();
   }
}

TEST_CASE("Phase C: expect_failure aborted after forced flush releases version",
          "[per_txn][phaseC][abort]")
{
   ptv_pubapi_db t;

   // Seed with enough keys to cross the tombstone_threshold (256) so a
   // remove_range forces a buffer flush in expect_failure.
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 500; ++i)
      {
         char key[32];
         std::snprintf(key, sizeof(key), "k%05d", i);
         tx.upsert(key_view(key, std::strlen(key)), to_vv("v"));
      }
      tx.commit();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(2000));
   auto     dead_before       = t.db->dead_versions().load_snapshot();
   uint64_t dead_count_before = dead_before ? dead_before->num_ranges() : 0;

   // expect_failure txn that triggers a forced flush via remove_range,
   // then aborts. Should still release the lazily-allocated ver.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.remove_range(to_kv("k00000"), to_kv("k99999"));
      tx.abort();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(500));
   auto     dead_after       = t.db->dead_versions().load_snapshot();
   uint64_t dead_count_after = dead_after ? dead_after->num_ranges() : 0;

   INFO("dead version count before/after expect_failure abort: " << dead_count_before << " / "
                                                                 << dead_count_after);
   CHECK(dead_count_after > dead_count_before);

   // Verify the removes were rolled back.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      auto v  = tx.get<std::string>(to_kv("k00100"));
      CHECK(v.has_value());
      tx.abort();
   }
}

// ════════════════════════════════════════════════════════════════════
// Phase D: in-place coalesce fast paths
// ════════════════════════════════════════════════════════════════════

TEST_CASE("Phase D: 10k hot-key updates allocate ≤ 5 new objects (perf gate)",
          "[per_txn][phaseD][in_place][perf]")
{
   ptv_pubapi_db t;

   // Seed with a value_node-forcing payload (>64 bytes).
   std::string fixed_size(80, 'X');
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), value_view(fixed_size.data(), fixed_size.size()));
      tx.commit();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(2000));
   auto baseline_alloc = t.ses->get_total_allocated_objects();

   // 10000 same-size updates in one expect_success txn. Once the transaction
   // owns the path, the existing value_node is a single current value and the
   // same-size writes overwrite that slot in place.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_success);
      for (int i = 0; i < 10000; ++i)
      {
         std::string val(80, 'A' + (i % 26));
         tx.upsert(to_kv("k"), value_view(val.data(), val.size()));
      }
      tx.commit();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(5000));
   auto post_alloc = t.ses->get_total_allocated_objects();

   INFO("baseline=" << baseline_alloc << " post_10k_updates=" << post_alloc
                    << " delta=" << (post_alloc - baseline_alloc));
   // Expectation: a small handful of new allocations (the new ver CB +
   // possibly a leaf cline shift), but NOT 10000 of them. Without Phase
   // D's in-place memcpy we'd see allocator growth proportional to the
   // update count.
   CHECK(post_alloc - baseline_alloc <= 5);

   // Verify the latest value is what we expect.
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   std::string expected(80, 'A' + (9999 % 26));
   CHECK(*v == expected);
}

TEST_CASE("Phase D: smaller-size updates demote once they fit inline", "[per_txn][phaseD][in_place]")
{
   ptv_pubapi_db t;

   // Seed with an 80-byte value (forces value_node).
   std::string seed(80, 'S');
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), value_view(seed.data(), seed.size()));
      tx.commit();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(2000));
   auto baseline_alloc = t.ses->get_total_allocated_objects();

   // Update with progressively smaller values. The >64 byte values reuse the
   // value_node slot; once the current value fits inline, collapse demotes it
   // back into the leaf.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_success);
      for (int sz = 70; sz >= 1; --sz)
      {
         std::string val(sz, 'B');
         tx.upsert(to_kv("k"), value_view(val.data(), val.size()));
      }
      tx.commit();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(5000));
   auto post_alloc = t.ses->get_total_allocated_objects();

   INFO("baseline=" << baseline_alloc << " post_smaller_updates=" << post_alloc);
   CHECK(post_alloc <= baseline_alloc + 5);

   // Final value: the last (1-byte) update.
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   CHECK(v->size() == 1);
   CHECK(*v == "B");
   CHECK(t.chain_length_of(0, to_kv("k")) == 0);
}

TEST_CASE("Phase D: shared leaf rewrite skips flat value_nodes", "[per_txn][phaseD][flat]")
{
   ptv_db t;

   // Values larger than value_node::max_inline_entry_size use flat value_node
   // storage: they have one implicit current value and no explicit version
   // entries. Leaf rewrite pruning must not ask them for entry version 0.
   std::string huge(value_node::max_inline_entry_size + 1, 'F');
   {
      tree_context ctx(t.root());
      ctx.insert(to_kv("flat"), value_type(to_vv(huge)));
      t.set_root(ctx.take_root());
   }

   {
      tree_context ctx(t.root());
      auto old_size =
          ctx.upsert<upsert_mode::shared_insert>(to_kv("next"), value_type(to_vv("small")));
      CHECK(old_size == -1);
      t.set_root(ctx.take_root());
   }

   tree_context ctx(t.root());
   cursor       c(ctx.get_root());
   REQUIRE(c.seek(to_kv("flat")));
   auto flat = c.value<std::string>();
   REQUIRE(flat.has_value());
   CHECK(flat->size() == huge.size());
   REQUIRE(c.seek(to_kv("next")));
   auto next = c.value<std::string>();
   REQUIRE(next.has_value());
   CHECK(*next == "small");
}

TEST_CASE("Phase A: cross-txn updates leave the latest value visible", "[per_txn][phaseA]")
{
   // Across-txn chain semantics depend on the COW prune behavior in
   // shared-mode update — currently the COW path strips multi-version
   // value_nodes back to inline form, so the chain length isn't a stable
   // invariant across txns. What MUST hold: each txn's writes are
   // visible after commit, regardless of whether the chain was preserved.
   ptv_pubapi_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert(to_kv("k"), to_vv("v0"));
      tx.commit();
   }

   for (int i = 0; i < 5; ++i)
   {
      auto tx = t.ses->start_transaction(0);
      char val[32];
      std::snprintf(val, sizeof(val), "v_txn_%d", i);
      tx.upsert(to_kv("k"), value_view(val, std::strlen(val)));
      tx.commit();
   }

   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   CHECK(*v == "v_txn_4");
}
