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
               ("psitri_ptv_pub_" + std::to_string(getpid()) + "_" +
                std::to_string(ts));
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
}

TEST_CASE("Phase A: 100 updates to one key in one txn coalesce to ≤ 2 chain entries",
          "[per_txn][phaseA][coalesce]")
{
   ptv_pubapi_db t;

   // Use values >64 bytes so make_value forces a value_node allocation.
   // (Smaller values stay inline in the leaf and never form a chain.)
   auto big = [](char c, int idx) {
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

   // Second txn: 100 updates to the same key. With per-txn version
   // threading, the first update appends to the chain (2 entries: committed
   // v0 + this txn's first iter). Subsequent 99 updates coalesce because
   // they all share the same txn version. Chain stays at exactly 2.
   {
      auto tx = t.ses->start_transaction(0);
      for (int i = 0; i < 100; ++i)
      {
         auto v = big('B', i);
         tx.upsert(to_kv("k"), value_view(v.data(), v.size()));
      }
      tx.commit();
   }

   // Exactly 2 chain entries: committed v0 + this txn's coalesced top.
   auto chain = t.chain_length_of(0, to_kv("k"));
   INFO("chain entries after 100 same-txn updates: " << chain);
   CHECK(chain == 2);

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
      auto ws       = db.start_write_session();
      auto tx       = ws->start_transaction(0, tx_mode::expect_success);
      auto root     = tx.primary().read_cursor().get_root();
      uint64_t ver  = 0;
      if (root.ver() != sal::null_ptr_address)
         ver = root.session()->read_custom_cb(root.ver());
      tx.abort();
      return ver;
   }
}

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

TEST_CASE("Phase B: expect_failure with writes commits and bumps once",
          "[per_txn][phaseB][lazy]")
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

TEST_CASE("Phase C: aborted expect_success txn registers ver as dead",
          "[per_txn][phaseC][abort]")
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
   auto dead_before = t.db->dead_versions().load_snapshot();
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
   auto dead_after = t.db->dead_versions().load_snapshot();
   uint64_t dead_count_after = 0;
   if (dead_after)
      dead_count_after = dead_after->num_ranges();

   INFO("dead version count before/after abort: "
        << dead_count_before << " / " << dead_count_after);
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
   auto dead_before = t.db->dead_versions().load_snapshot();
   uint64_t dead_count_before = dead_before ? dead_before->num_ranges() : 0;

   // expect_failure txn that triggers a forced flush via remove_range,
   // then aborts. Should still release the lazily-allocated ver.
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
      tx.remove_range(to_kv("k00000"), to_kv("k99999"));
      tx.abort();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(500));
   auto dead_after = t.db->dead_versions().load_snapshot();
   uint64_t dead_count_after = dead_after ? dead_after->num_ranges() : 0;

   INFO("dead version count before/after expect_failure abort: "
        << dead_count_before << " / " << dead_count_after);
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

TEST_CASE("Phase D: same-size hot-key updates allocate zero new value_nodes",
          "[per_txn][phaseD][in_place]")
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

   // 1000 same-size updates in one expect_success txn. Phase D's
   // try_coalesce_in_place fires every iteration after the first promote,
   // so allocations stay bounded (1 leaf realloc + 1 chain promote, no
   // per-iter VN allocs).
   {
      auto tx = t.ses->start_transaction(0, tx_mode::expect_success);
      for (int i = 0; i < 1000; ++i)
      {
         std::string val(80, 'A' + (i % 26));
         tx.upsert(to_kv("k"), value_view(val.data(), val.size()));
      }
      tx.commit();
   }

   t.db->wait_for_compactor(std::chrono::milliseconds(5000));
   auto post_alloc = t.ses->get_total_allocated_objects();

   INFO("baseline=" << baseline_alloc << " post_1000_updates="
                    << post_alloc << " delta=" << (post_alloc - baseline_alloc));
   // Expectation: a small handful of new allocations (the new ver CB +
   // possibly a leaf cline shift), but NOT 1000 of them. Without Phase D
   // we'd see allocator growth proportional to the update count.
   CHECK(post_alloc - baseline_alloc <= 5);

   // Verify the latest value is what we expect.
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   std::string expected(80, 'A' + (999 % 26));
   CHECK(*v == expected);
}

TEST_CASE("Phase D: smaller-size updates also coalesce in place",
          "[per_txn][phaseD][in_place]")
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

   // Update with progressively smaller values within the slot's capacity.
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
   CHECK(post_alloc - baseline_alloc <= 5);

   // Final value: the last (1-byte) update.
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   REQUIRE(c.seek(to_kv("k")));
   auto v = c.value<std::string>();
   REQUIRE(v.has_value());
   CHECK(v->size() == 1);
   CHECK(*v == "B");
}

TEST_CASE("Phase A: cross-txn updates leave the latest value visible",
          "[per_txn][phaseA]")
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
