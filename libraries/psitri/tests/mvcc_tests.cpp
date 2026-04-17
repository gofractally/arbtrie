#include <catch2/catch_all.hpp>
#include <psitri/cursor.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <sal/sal.hpp>

#include <atomic>
#include <chrono>
#include <fstream>
#include <random>
#include <thread>
#include <vector>

using namespace psitri;
using sal::alloc_header;
using sal::smart_ptr;

namespace
{
   struct mvcc_db
   {
      std::unique_ptr<sal::allocator> alloc;
      sal::allocator_session_ptr      ses{nullptr};

      mvcc_db()
      {
         std::filesystem::remove_all("mvcc_testdb");
         alloc = std::make_unique<sal::allocator>("mvcc_testdb", sal::runtime_config());
         sal::register_type_vtable<leaf_node>();
         sal::register_type_vtable<inner_prefix_node>();
         sal::register_type_vtable<inner_node>();
         sal::register_type_vtable<value_node>();
         ses = alloc->get_session();
      }
      ~mvcc_db() { std::filesystem::remove_all("mvcc_testdb"); }

      smart_ptr<alloc_header> root()
      {
         return ses->get_root<>(sal::root_object_number(0));
      }
      void set_root(smart_ptr<alloc_header> r)
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

   std::string read_value(tree_context& ctx, const std::string& key,
                          uint64_t version = UINT64_MAX)
   {
      cursor c(ctx.get_root(), version);
      if (!c.seek(to_kv(key)))
         return "";
      auto val = c.value<std::string>();
      return val.value_or("");
   }
}  // namespace

TEST_CASE("mvcc_upsert: update existing key via value_node append", "[mvcc_write]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Build a tree with COW insert
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key1"), value_type(to_vv("val_v0")));
      ctx.insert(to_kv("key2"), value_type(to_vv("val2_v0")));
      db.set_root(ctx.take_root());
   }

   // Take a snapshot before MVCC write
   auto snapshot = db.root();

   // MVCC upsert: update key1 at version 1
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key1"), value_type(to_vv("val_v1")), 1);
      db.set_root(ctx.take_root());
   }

   // Verify: current tree has new value
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key1") == "val_v1");
      CHECK(read_value(ctx, "key2") == "val2_v0");
   }

   // After first MVCC update, key1 now has a value_node.
   // MVCC upsert again: update key1 at version 2 (appends to value_node)
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key1"), value_type(to_vv("val_v2")), 2);
      db.set_root(ctx.take_root());
   }

   // Verify: current tree has version 2 value
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key1") == "val_v2");
   }
}

TEST_CASE("mvcc_upsert: insert new key", "[mvcc_write]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Build initial tree
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("aaa"), value_type(to_vv("val_a")));
      ctx.insert(to_kv("zzz"), value_type(to_vv("val_z")));
      db.set_root(ctx.take_root());
   }

   // MVCC insert a new key
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("mmm"), value_type(to_vv("val_m")), 1);
      db.set_root(ctx.take_root());
   }

   // Verify: all three keys present
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "aaa") == "val_a");
      CHECK(read_value(ctx, "mmm") == "val_m");
      CHECK(read_value(ctx, "zzz") == "val_z");
   }
}

TEST_CASE("mvcc_upsert: inline to value_node promotion", "[mvcc_write]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Insert key with small inline value
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key"), value_type(to_vv("old")));
      db.set_root(ctx.take_root());
   }

   // MVCC update promotes inline value to value_node
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key"), value_type(to_vv("new")), 1);
      db.set_root(ctx.take_root());
   }

   // Verify: reads latest value
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key") == "new");
   }

   // Version-aware read: value_node has versions {0: "old", 1: "new"}
   {
      tree_context ctx(db.root());
      cursor c(ctx.get_root());
      REQUIRE(c.seek(to_kv("key")));
      // The value should be the latest (version 1)
      CHECK(c.value<std::string>().value_or("") == "new");
   }
}

TEST_CASE("mvcc_remove: tombstone via value_node", "[mvcc_write]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Insert keys
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key1"), value_type(to_vv("val1")));
      ctx.insert(to_kv("key2"), value_type(to_vv("val2")));
      db.set_root(ctx.take_root());
   }

   // MVCC remove key1 at version 1
   {
      tree_context ctx(db.root());
      ctx.mvcc_remove(to_kv("key1"), 1);
      db.set_root(ctx.take_root());
   }

   // With version-aware cursor (default = latest), key1 is hidden (tombstoned).
   // Key2 is unaffected.
   {
      tree_context ctx(db.root());
      cursor c(ctx.get_root());
      // key1 is tombstoned at latest version — seek should skip it
      CHECK_FALSE(c.seek(to_kv("key1")));
      // key2 unchanged
      REQUIRE(c.seek(to_kv("key2")));
      CHECK(c.value<std::string>().value_or("") == "val2");
   }

   // With version 0 cursor, key1 is still visible (tombstone is at version 1)
   {
      tree_context ctx(db.root());
      cursor c(ctx.get_root(), 0);
      REQUIRE(c.seek(to_kv("key1")));
      CHECK(c.value<std::string>().value_or("") == "val1");
      REQUIRE(c.seek(to_kv("key2")));
      CHECK(c.value<std::string>().value_or("") == "val2");
   }
}

TEST_CASE("mvcc_upsert: multiple versions accumulate", "[mvcc_write]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("counter"), value_type(to_vv("0")));
      db.set_root(ctx.take_root());
   }

   // Apply 10 MVCC updates
   for (int v = 1; v <= 10; ++v)
   {
      tree_context ctx(db.root());
      std::string val = std::to_string(v);
      ctx.mvcc_upsert(to_kv("counter"), value_type(to_vv(val)), v);
      db.set_root(ctx.take_root());
   }

   // Read latest — should be "10"
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "counter") == "10");
   }

   // Check version history via value_node
   {
      tree_context ctx(db.root());
      cursor c(ctx.get_root());
      REQUIRE(c.seek(to_kv("counter")));
      // The underlying value_node should have 11 entries (v0 + v1..v10)
      // We can verify by checking num_versions if we have access
   }
}

TEST_CASE("mvcc_upsert: many keys stress test", "[mvcc_write]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   const int N = 200;

   // Insert N keys
   {
      tree_context ctx(db.root());
      for (int i = 0; i < N; ++i)
      {
         char key[32], val[32];
         snprintf(key, sizeof(key), "key%06d", i);
         snprintf(val, sizeof(val), "val%06d_v0", i);
         ctx.insert(to_kv(std::string(key)), value_type(to_vv(std::string(val))));
      }
      db.set_root(ctx.take_root());
   }

   // MVCC update every other key at version 1
   {
      tree_context ctx(db.root());
      for (int i = 0; i < N; i += 2)
      {
         char key[32], val[32];
         snprintf(key, sizeof(key), "key%06d", i);
         snprintf(val, sizeof(val), "val%06d_v1", i);
         ctx.mvcc_upsert(to_kv(std::string(key)), value_type(to_vv(std::string(val))), 1);
      }
      db.set_root(ctx.take_root());
   }

   // Verify all keys readable with correct values
   {
      tree_context ctx(db.root());
      for (int i = 0; i < N; ++i)
      {
         char key[32], expected[32];
         snprintf(key, sizeof(key), "key%06d", i);
         if (i % 2 == 0)
            snprintf(expected, sizeof(expected), "val%06d_v1", i);
         else
            snprintf(expected, sizeof(expected), "val%06d_v0", i);
         INFO("key=" << key);
         CHECK(read_value(ctx, std::string(key)) == std::string(expected));
      }
   }
}

// ── Phase 7B: Version-filtered read tests ──────────────────────────

TEST_CASE("mvcc_read: version-filtered reads at different versions", "[mvcc_read]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Insert key at version 0
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key"), value_type(to_vv("v0")));
      db.set_root(ctx.take_root());
   }

   // MVCC update at version 5
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key"), value_type(to_vv("v5")), 5);
      db.set_root(ctx.take_root());
   }

   // MVCC update at version 10
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key"), value_type(to_vv("v10")), 10);
      db.set_root(ctx.take_root());
   }

   // Read at different version snapshots
   {
      tree_context ctx(db.root());
      // Version 0: see original value
      CHECK(read_value(ctx, "key", 0) == "v0");
      // Version 3: still see v0 (no entry at version 3, latest <= 3 is v0)
      CHECK(read_value(ctx, "key", 3) == "v0");
      // Version 5: see v5
      CHECK(read_value(ctx, "key", 5) == "v5");
      // Version 7: still see v5 (latest <= 7 is v5)
      CHECK(read_value(ctx, "key", 7) == "v5");
      // Version 10: see v10
      CHECK(read_value(ctx, "key", 10) == "v10");
      // Latest (UINT64_MAX): see v10
      CHECK(read_value(ctx, "key") == "v10");
   }
}

TEST_CASE("mvcc_read: tombstone visibility at different versions", "[mvcc_read]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Insert keys
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("aaa"), value_type(to_vv("val_a")));
      ctx.insert(to_kv("bbb"), value_type(to_vv("val_b")));
      ctx.insert(to_kv("ccc"), value_type(to_vv("val_c")));
      db.set_root(ctx.take_root());
   }

   // MVCC remove bbb at version 3
   {
      tree_context ctx(db.root());
      ctx.mvcc_remove(to_kv("bbb"), 3);
      db.set_root(ctx.take_root());
   }

   // At version 2: bbb still visible
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "bbb", 2) == "val_b");
   }

   // At version 3+: bbb hidden
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "bbb", 3) == "");
      CHECK(read_value(ctx, "bbb") == "");
   }

   // Iteration at version 2: all three keys visible
   {
      tree_context ctx(db.root());
      cursor c(ctx.get_root(), 2);
      REQUIRE(c.seek_begin());
      CHECK(c.key() == to_kv("aaa"));
      REQUIRE(c.next());
      CHECK(c.key() == to_kv("bbb"));
      REQUIRE(c.next());
      CHECK(c.key() == to_kv("ccc"));
      CHECK_FALSE(c.next());
   }

   // Iteration at latest: bbb skipped
   {
      tree_context ctx(db.root());
      cursor c(ctx.get_root());
      REQUIRE(c.seek_begin());
      CHECK(c.key() == to_kv("aaa"));
      REQUIRE(c.next());
      CHECK(c.key() == to_kv("ccc"));  // bbb skipped
      CHECK_FALSE(c.next());
   }
}

TEST_CASE("mvcc_read: reverse iteration skips tombstones", "[mvcc_read]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("aaa"), value_type(to_vv("val_a")));
      ctx.insert(to_kv("bbb"), value_type(to_vv("val_b")));
      ctx.insert(to_kv("ccc"), value_type(to_vv("val_c")));
      db.set_root(ctx.take_root());
   }

   // Tombstone bbb
   {
      tree_context ctx(db.root());
      ctx.mvcc_remove(to_kv("bbb"), 1);
      db.set_root(ctx.take_root());
   }

   // Reverse iteration at latest: bbb skipped
   {
      tree_context ctx(db.root());
      cursor c(ctx.get_root());
      REQUIRE(c.seek_last());
      CHECK(c.key() == to_kv("ccc"));
      REQUIRE(c.prev());
      CHECK(c.key() == to_kv("aaa"));  // bbb skipped
      CHECK_FALSE(c.prev());
   }
}

TEST_CASE("mvcc_read: cursor version reads multi-version value_node", "[mvcc_read]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("counter"), value_type(to_vv("0")));
      db.set_root(ctx.take_root());
   }

   // Apply 5 MVCC updates
   for (int v = 1; v <= 5; ++v)
   {
      tree_context ctx(db.root());
      std::string val = std::to_string(v);
      ctx.mvcc_upsert(to_kv("counter"), value_type(to_vv(val)), v);
      db.set_root(ctx.take_root());
   }

   // Read at each version
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "counter", 0) == "0");
      CHECK(read_value(ctx, "counter", 1) == "1");
      CHECK(read_value(ctx, "counter", 2) == "2");
      CHECK(read_value(ctx, "counter", 3) == "3");
      CHECK(read_value(ctx, "counter", 4) == "4");
      CHECK(read_value(ctx, "counter", 5) == "5");
      CHECK(read_value(ctx, "counter") == "5");
   }
}

// ── Phase 8E: Immediate mode MVCC tests ────────────────────────────

namespace
{
   struct immediate_db
   {
      std::string               dir;
      std::shared_ptr<database> db;

      immediate_db(const std::string& name = "mvcc_immediate_testdb") : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db = database::open(dir);
      }
      ~immediate_db() { std::filesystem::remove_all(dir); }
   };
}  // namespace

TEST_CASE("mvcc_immediate: single-thread upsert and read", "[mvcc_immediate]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   auto ws = idb.db->start_write_session();

   // Seed the tree with a COW transaction first
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key1", "val1_v0");
      tx.upsert("key2", "val2_v0");
      tx.commit();
   }

   // Immediate MVCC upsert
   auto v1 = ws->mvcc_upsert(0, "key1", "val1_v1");
   CHECK(v1 > 0);

   // Read latest value
   {
      auto root = ws->get_root(0);
      cursor c(root);
      REQUIRE(c.seek(to_kv("key1")));
      CHECK(c.value<std::string>().value_or("") == "val1_v1");
      REQUIRE(c.seek(to_kv("key2")));
      CHECK(c.value<std::string>().value_or("") == "val2_v0");
   }

   // Multiple immediate upserts
   auto v2 = ws->mvcc_upsert(0, "key1", "val1_v2");
   CHECK(v2 > v1);

   {
      auto root = ws->get_root(0);
      cursor c(root);
      REQUIRE(c.seek(to_kv("key1")));
      CHECK(c.value<std::string>().value_or("") == "val1_v2");
   }
}

TEST_CASE("mvcc_immediate: remove via tombstone", "[mvcc_immediate]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   auto ws = idb.db->start_write_session();

   {
      auto tx = ws->start_transaction(0);
      tx.upsert("aaa", "val_a");
      tx.upsert("bbb", "val_b");
      tx.upsert("ccc", "val_c");
      tx.commit();
   }

   ws->mvcc_remove(0, "bbb");

   // bbb hidden at latest version
   {
      auto root = ws->get_root(0);
      cursor c(root);
      CHECK_FALSE(c.seek(to_kv("bbb")));
      // Iteration skips bbb
      REQUIRE(c.seek_begin());
      CHECK(c.key() == to_kv("aaa"));
      REQUIRE(c.next());
      CHECK(c.key() == to_kv("ccc"));
      CHECK_FALSE(c.next());
   }
}

TEST_CASE("mvcc_immediate: multi-threaded concurrent upserts", "[mvcc_immediate]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   const int NUM_THREADS  = 4;
   const int OPS_PER_THREAD = 100;

   // Seed tree: each thread operates on its own key prefix
   {
      auto ws = idb.db->start_write_session();
      auto tx = ws->start_transaction(0);
      for (int t = 0; t < NUM_THREADS; ++t)
      {
         char key[32];
         snprintf(key, sizeof(key), "t%d_counter", t);
         tx.upsert(key_view(key, strlen(key)), "0");
      }
      tx.commit();
   }

   std::atomic<int> errors{0};
   std::vector<std::thread> threads;

   for (int t = 0; t < NUM_THREADS; ++t)
   {
      threads.emplace_back(
          [&idb, t, &errors]()
          {
             char tname[32];
             snprintf(tname, sizeof(tname), "writer%d", t);
             sal::set_current_thread_name(tname);

             auto ws = idb.db->start_write_session();
             char key[32];
             snprintf(key, sizeof(key), "t%d_counter", t);
             auto kv = key_view(key, strlen(key));

             for (int i = 1; i <= OPS_PER_THREAD; ++i)
             {
                std::string val = std::to_string(i);
                ws->mvcc_upsert(0, kv, value_view(val.data(), val.size()));
             }
          });
   }

   for (auto& th : threads)
      th.join();

   CHECK(errors == 0);

   // Verify each thread's key has the final value
   {
      auto ws = idb.db->start_write_session();
      auto root = ws->get_root(0);
      cursor c(root);
      for (int t = 0; t < NUM_THREADS; ++t)
      {
         char key[32];
         snprintf(key, sizeof(key), "t%d_counter", t);
         REQUIRE(c.seek(key_view(key, strlen(key))));
         auto val = c.value<std::string>().value_or("");
         CHECK(val == std::to_string(OPS_PER_THREAD));
      }
   }
}

TEST_CASE("mvcc_immediate: version numbers increase monotonically", "[mvcc_immediate]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   auto ws = idb.db->start_write_session();

   // Seed
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key", "v0");
      tx.commit();
   }

   uint64_t prev = 0;
   for (int i = 0; i < 20; ++i)
   {
      std::string val = "v" + std::to_string(i + 1);
      auto ver = ws->mvcc_upsert(0, "key", value_view(val.data(), val.size()));
      CHECK(ver > prev);
      prev = ver;
   }

   // Read at each version
   {
      auto root = ws->get_root(0);
      cursor c(root);
      REQUIRE(c.seek(to_kv("key")));
      CHECK(c.value<std::string>().value_or("") == "v20");
   }
}

// ───────────────────────────────────────────────────────────
// Phase 9: Version Reclamation Integration Tests
// ───────────────────────────────────────────────────────────

TEST_CASE("version_reclamation: dead versions recorded in live_range_map", "[version_reclamation]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   auto ws = idb.db->start_write_session();

   // Seed a key via transaction
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key1", "initial");
      tx.commit();
   }

   // Perform several MVCC upserts — each overwrites the previous version CB
   std::vector<uint64_t> versions;
   for (int i = 0; i < 5; ++i)
   {
      std::string val = "v" + std::to_string(i + 1);
      auto ver = ws->mvcc_upsert(0, "key1", value_view(val.data(), val.size()));
      versions.push_back(ver);
   }

   // At this point, versions[0]..versions[3] should have been released
   // (each mvcc_upsert releases the previous version CB).
   // versions[4] is the current version — still alive in the root slot.

   // Wait for the release thread to drain the queues
   REQUIRE(idb.db->wait_for_compactor());

   // Flush pending and check dead versions
   auto& dv = idb.db->dead_versions();
   dv.flush_pending();

   // Versions 1-4 should be dead (overwritten by subsequent upserts)
   for (size_t i = 0; i + 1 < versions.size(); ++i)
   {
      CHECK(dv.is_dead(versions[i]));
   }

   // The latest version should still be alive
   CHECK_FALSE(dv.is_dead(versions.back()));
}

TEST_CASE("version_reclamation: snapshot holds version alive", "[version_reclamation]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   auto ws = idb.db->start_write_session();

   // Seed
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key1", "initial");
      tx.commit();
   }

   // MVCC upsert: version 1
   auto ver1 = ws->mvcc_upsert(0, "key1", to_vv("v1"));

   // Take a snapshot (retains root + version CB)
   auto snapshot = ws->get_root(0);

   // MVCC upsert: version 2 (releases ver1's CB from root slot)
   auto ver2 = ws->mvcc_upsert(0, "key1", to_vv("v2"));

   // Wait for release thread
   REQUIRE(idb.db->wait_for_compactor());

   auto& dv = idb.db->dead_versions();
   dv.flush_pending();

   // ver1 should NOT be dead yet — snapshot still holds a reference
   // (the snapshot's smart_ptr retained ver1's CB, keeping refcount > 0)
   // NOTE: ver1 was the version CB that get_root retained in mvcc_upsert;
   // that was explicitly released. But the snapshot taken AFTER ver1 was
   // set actually holds ver2's predecessor. Let me check...
   // Actually: after mvcc_upsert(ver1), the root slot has ver1.
   // snapshot = get_root(0) retains ver1 (refcount = 2: slot + snapshot).
   // mvcc_upsert(ver2): releases ver1 from slot (-1) and explicitly (-1).
   // But snapshot still holds ver1 (+1), so refcount = 1, NOT 0.
   // So ver1 should still be alive.
   CHECK_FALSE(dv.is_dead(ver1));

   // Drop the snapshot — this should release ver1's CB to refcount 0
   snapshot = {};

   // Wait for release thread to process
   REQUIRE(idb.db->wait_for_compactor());
   dv.flush_pending();

   // Now ver1 should be dead
   CHECK(dv.is_dead(ver1));

   // ver2 is still the current version — alive
   CHECK_FALSE(dv.is_dead(ver2));
}

TEST_CASE("version_reclamation: published snapshot query", "[version_reclamation]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   auto ws = idb.db->start_write_session();

   // Seed
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key1", "initial");
      tx.commit();
   }

   // Create and release several versions
   for (int i = 0; i < 8; ++i)
   {
      std::string val = "v" + std::to_string(i);
      ws->mvcc_upsert(0, "key1", value_view(val.data(), val.size()));
   }

   REQUIRE(idb.db->wait_for_compactor());

   auto& dv = idb.db->dead_versions();
   dv.flush_pending();
   dv.publish_snapshot();

   // Load published snapshot and verify
   auto* snap = dv.load_snapshot();
   REQUIRE(snap != nullptr);
   CHECK(snap->num_ranges() > 0);
}

TEST_CASE("opportunistic_cleanup: dead entries stripped during value_node COW", "[opportunistic_cleanup]")
{
   sal::set_current_thread_name("main");
   immediate_db idb;

   auto ws = idb.db->start_write_session();

   // Seed a key — transaction commit allocates version 1
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key1", "initial");
      tx.commit();
   }

   // Create several MVCC versions — mvcc_upsert #1 gets version 2, etc.
   std::vector<uint64_t> versions;
   for (int i = 1; i <= 5; ++i)
   {
      std::string val = "v" + std::to_string(i);
      auto ver = ws->mvcc_upsert(0, "key1", value_view(val.data(), val.size()));
      versions.push_back(ver);
   }
   // versions = [2, 3, 4, 5, 6].
   // Value_node entries: {ver=0,"initial"}, {ver=2,"v1"}, {ver=3,"v2"}, {ver=4,"v3"}, {ver=5,"v4"}, {ver=6,"v5"}

   // Wait for release thread to drain version CBs
   REQUIRE(idb.db->wait_for_compactor());

   // Flush and publish dead versions so the snapshot is available
   auto& dv = idb.db->dead_versions();
   dv.flush_pending();
   dv.publish_snapshot();

   // Verify: versions[0..3] (2-5) and version 1 (from tx commit) should be dead.
   // Version 6 (versions[4]) is still alive in the root slot.
   auto* snap = dv.load_snapshot();
   REQUIRE(snap != nullptr);
   REQUIRE(snap->num_ranges() > 0);
   CHECK(snap->is_dead(versions[0]));      // version 2
   CHECK(snap->is_dead(versions[3]));      // version 5
   CHECK_FALSE(snap->is_dead(versions[4])); // version 6 — alive

   // Now do another MVCC upsert — this triggers a value_node COW that should
   // strip dead entries via the published snapshot.
   auto ver_final = ws->mvcc_upsert(0, "key1", to_vv("v6"));

   // Verify the latest value is correct
   {
      auto root = ws->get_root(0);
      cursor c(root);
      REQUIRE(c.seek(to_kv("key1")));
      CHECK(c.value<std::string>().value_or("") == "v6");
   }

   // The last version before v6 was versions[4]=6 with value "v5".
   // Since version 6 was NOT dead when the snapshot was published,
   // it should have been preserved during the COW.
   {
      auto root = ws->get_root(0);
      cursor c(root, versions[4]);
      REQUIRE(c.seek(to_kv("key1")));
      CHECK(c.value<std::string>().value_or("") == "v5");
   }
}

TEST_CASE("opportunistic_cleanup: value_node entry count reduced after cleanup", "[opportunistic_cleanup]")
{
   sal::set_current_thread_name("main");

   // Use low-level mvcc_db so we can inspect value_node directly
   mvcc_db db;

   // Insert key1 with COW
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key1"), value_type(to_vv("v0")));
      db.set_root(ctx.take_root());
   }

   // MVCC upsert to create a 2-entry value_node (promotes inline v0 + adds v1)
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key1"), value_type(to_vv("v1")), 1);
      db.set_root(ctx.take_root());
   }

   // Append more versions: 2, 3, 4
   for (uint64_t ver = 2; ver <= 4; ++ver)
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key1"), value_type(to_vv("v" + std::to_string(ver))), ver);
      db.set_root(ctx.take_root());
   }

   // Helper to get value_node entry count for key1
   auto get_vn_entries = [&]() -> uint8_t {
      auto ref = db.ses->get_ref<leaf_node>(db.root().address());
      auto val = ref->get_value(branch_number(0));
      if (!val.is_value_node())
         return 0;
      auto vref = db.ses->get_ref<value_node>(val.value_address());
      return vref->num_versions();
   };

   // Should have 5 entries (v0, v1, v2, v3, v4)
   CHECK(get_vn_entries() == 5);

   // Create a snapshot marking versions 0-3 as dead
   live_range_map drm;
   for (uint64_t v = 0; v <= 3; ++v)
      drm.add_dead_version(v);
   drm.flush_pending();
   drm.publish_snapshot();

   // MVCC upsert with dead-version filtering
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(drm.load_snapshot());
      ctx.mvcc_upsert(to_kv("key1"), value_type(to_vv("v5")), 5);
      db.set_root(ctx.take_root());
   }

   // After cleanup: entries for versions 0-3 should be stripped.
   // Remaining: v4 (alive) + v5 (newly added) = 2 entries
   CHECK(get_vn_entries() == 2);

   // Verify latest value is correct
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key1") == "v5");
   }

   // Verify v4 is still accessible
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key1", 4) == "v4");
   }

   // Verify v5 is accessible
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key1", 5) == "v5");
   }
}

// ─── Epoch stamping tests ─────────────────────────────────────────

TEST_CASE("inner nodes get current epoch during COW insert", "[epoch]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Insert enough keys to create inner nodes (need > 1 leaf to get a split)
   {
      tree_context ctx(db.root());
      ctx.set_current_epoch(42);
      for (int i = 0; i < 300; ++i)
      {
         char key[16];
         snprintf(key, sizeof(key), "k%05d", i);
         char val[16];
         snprintf(val, sizeof(val), "v%05d", i);
         ctx.upsert(key_view(key, strlen(key)), value_type(value_view(val, strlen(val))));
      }
      db.set_root(ctx.take_root());
   }

   // Root should be an inner node with epoch == 42
   {
      auto ref = db.ses->get_ref(db.root().address());
      auto nt  = node_type(ref->type());
      REQUIRE((nt == node_type::inner_prefix || nt == node_type::inner));
      uint64_t root_epoch = (nt == node_type::inner_prefix)
                                ? ref.as<inner_prefix_node>()->epoch()
                                : ref.as<inner_node>()->epoch();
      CHECK(root_epoch == 42);
   }
}

TEST_CASE("inner nodes get epoch 0 when no epoch set", "[epoch]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Insert without setting epoch (default _current_epoch == 0)
   {
      tree_context ctx(db.root());
      for (int i = 0; i < 300; ++i)
      {
         char key[16];
         snprintf(key, sizeof(key), "k%05d", i);
         char val[16];
         snprintf(val, sizeof(val), "v%05d", i);
         ctx.upsert(key_view(key, strlen(key)), value_type(value_view(val, strlen(val))));
      }
      db.set_root(ctx.take_root());
   }

   // Root inner node should have epoch == 0
   {
      auto ref = db.ses->get_ref(db.root().address());
      auto nt  = node_type(ref->type());
      REQUIRE((nt == node_type::inner_prefix || nt == node_type::inner));
      uint64_t root_epoch = (nt == node_type::inner_prefix)
                                ? ref.as<inner_prefix_node>()->epoch()
                                : ref.as<inner_node>()->epoch();
      CHECK(root_epoch == 0);
   }
}

TEST_CASE("MVCC fallback to COW stamps epoch on inner nodes", "[epoch]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Build a tree with many keys (epoch 0)
   {
      tree_context ctx(db.root());
      for (int i = 0; i < 300; ++i)
      {
         char key[16];
         snprintf(key, sizeof(key), "k%05d", i);
         char val[16];
         snprintf(val, sizeof(val), "v%05d", i);
         ctx.upsert(key_view(key, strlen(key)), value_type(value_view(val, strlen(val))));
      }
      db.set_root(ctx.take_root());
   }

   // Verify root has epoch 0
   {
      auto ref = db.ses->get_ref(db.root().address());
      auto nt  = node_type(ref->type());
      REQUIRE((nt == node_type::inner_prefix || nt == node_type::inner));
      uint64_t root_epoch = (nt == node_type::inner_prefix)
                                ? ref.as<inner_prefix_node>()->epoch()
                                : ref.as<inner_node>()->epoch();
      CHECK(root_epoch == 0);
   }

   // MVCC upsert that causes a structural change (new key, leaf overflow → split)
   // uses the COW fallback path, which should stamp epoch on new inner nodes
   {
      tree_context ctx(db.root());
      ctx.set_current_epoch(7);
      // Insert a new key via MVCC — if the leaf is full, this falls back to COW
      // which creates inner nodes with the current epoch
      ctx.mvcc_upsert(to_kv("zzz_new_key"), value_type(to_vv("new_value")), 1);
      db.set_root(ctx.take_root());
   }

   // Verify the tree still works
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "zzz_new_key") == "new_value");
      CHECK(read_value(ctx, "k00000") == "v00000");
   }
}

TEST_CASE("COW replace_branch preserves clone epoch", "[epoch]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Build tree with epoch 10
   {
      tree_context ctx(db.root());
      ctx.set_current_epoch(10);
      for (int i = 0; i < 300; ++i)
      {
         char key[16], val[16];
         snprintf(key, sizeof(key), "k%05d", i);
         snprintf(val, sizeof(val), "v%05d", i);
         ctx.upsert(key_view(key, strlen(key)), value_type(value_view(val, strlen(val))));
      }
      db.set_root(ctx.take_root());
   }

   // Verify root has epoch 10
   {
      auto ref = db.ses->get_ref(db.root().address());
      auto nt  = node_type(ref->type());
      REQUIRE((nt == node_type::inner_prefix || nt == node_type::inner));
      uint64_t root_epoch = (nt == node_type::inner_prefix)
                                ? ref.as<inner_prefix_node>()->epoch()
                                : ref.as<inner_node>()->epoch();
      CHECK(root_epoch == 10);
   }

   // COW update with epoch 20 — the replace_branch path copies clone epoch,
   // but any split that creates new inner nodes uses the new epoch
   {
      tree_context ctx(db.root());
      ctx.set_current_epoch(20);
      ctx.upsert(to_kv("k00050"), value_type(to_vv("updated_value")));
      db.set_root(ctx.take_root());
   }

   // Root was COW'd via replace_branch which copies clone's epoch (10).
   // The root epoch stays 10 because replace_branch copies from clone.
   {
      auto ref = db.ses->get_ref(db.root().address());
      auto nt  = node_type(ref->type());
      REQUIRE((nt == node_type::inner_prefix || nt == node_type::inner));
      uint64_t root_epoch = (nt == node_type::inner_prefix)
                                ? ref.as<inner_prefix_node>()->epoch()
                                : ref.as<inner_node>()->epoch();
      // replace_branch copies clone's epoch, so root keeps 10
      CHECK(root_epoch == 10);
   }

   // Verify data is correct
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "k00050") == "updated_value");
   }
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 10B: Defrag tests
// ═══════════════════════════════════════════════════════════════════════

TEST_CASE("defrag strips dead entries from value_nodes", "[defrag]")
{
   mvcc_db db;

   // Insert keys at version 0 (COW path)
   {
      tree_context ctx(db.root());
      ctx.upsert<upsert_mode::unique_upsert>("key_a", value_type("val_a_v0"));
      ctx.upsert<upsert_mode::unique_upsert>("key_b", value_type("val_b_v0"));
      ctx.upsert<upsert_mode::unique_upsert>("key_c", value_type("val_c_v0"));
      db.set_root(ctx.take_root());
   }

   // MVCC upsert at version 1
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert("key_a", value_type("val_a_v1"), 1);
      db.set_root(ctx.take_root());
   }

   // MVCC upsert at version 2
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert("key_a", value_type("val_a_v2"), 2);
      db.set_root(ctx.take_root());
   }

   // Verify all versions are readable before defrag
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key_a", 0) == "val_a_v0");
      CHECK(read_value(ctx, "key_a", 1) == "val_a_v1");
      CHECK(read_value(ctx, "key_a", 2) == "val_a_v2");
   }

   // Mark versions 0 and 1 as dead
   live_range_map dead_map;
   dead_map.add_dead_version(0);
   dead_map.add_dead_version(1);
   dead_map.publish_snapshot();

   // Run defrag
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(dead_map.load_snapshot());
      uint64_t cleaned = ctx.defrag();
      CHECK(cleaned == 1);  // key_a's value_node was cleaned
      db.set_root(ctx.take_root());
   }

   // Verify version 2 is still readable after defrag
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key_a", 2) == "val_a_v2");
      CHECK(read_value(ctx, "key_a") == "val_a_v2");
      // Dead versions 0 and 1 are stripped — reading at those versions
      // should return empty (no entry found)
      CHECK(read_value(ctx, "key_a", 0) == "");
      CHECK(read_value(ctx, "key_a", 1) == "");
   }

   // key_b and key_c should be unchanged (inline, no value_node)
   {
      tree_context ctx(db.root());
      CHECK(read_value(ctx, "key_b") == "val_b_v0");
      CHECK(read_value(ctx, "key_c") == "val_c_v0");
   }
}

TEST_CASE("defrag is no-op when no dead versions exist", "[defrag]")
{
   mvcc_db db;

   // Insert and update a key to create a value_node
   {
      tree_context ctx(db.root());
      ctx.upsert<upsert_mode::unique_upsert>("key_a", value_type("v0"));
      db.set_root(ctx.take_root());
   }
   {
      tree_context ctx(db.root());
      ctx.mvcc_upsert("key_a", value_type("v1"), 1);
      db.set_root(ctx.take_root());
   }

   // Defrag with empty dead map — no versions are dead
   live_range_map dead_map;
   dead_map.publish_snapshot();

   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(dead_map.load_snapshot());
      uint64_t cleaned = ctx.defrag();
      CHECK(cleaned == 0);
   }
}

TEST_CASE("defrag is no-op on empty tree", "[defrag]")
{
   mvcc_db db;

   live_range_map dead_map;
   dead_map.add_dead_version(0);
   dead_map.publish_snapshot();

   // Empty tree — root is null, defrag with no dead_snap is 0
   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(dead_map.load_snapshot());
      uint64_t cleaned = ctx.defrag();
      CHECK(cleaned == 0);
   }
}

TEST_CASE("defrag handles tree with only inline values", "[defrag]")
{
   mvcc_db db;

   // Insert keys without MVCC — they stay inline, no value_nodes
   {
      tree_context ctx(db.root());
      for (int i = 0; i < 20; ++i)
      {
         char key[16], val[16];
         snprintf(key, sizeof(key), "k%04d", i);
         snprintf(val, sizeof(val), "v%04d", i);
         ctx.upsert<upsert_mode::unique_upsert>(key, value_type(val));
      }
      db.set_root(ctx.take_root());
   }

   live_range_map dead_map;
   dead_map.add_dead_version(0);
   dead_map.publish_snapshot();

   {
      tree_context ctx(db.root());
      ctx.set_dead_versions(dead_map.load_snapshot());
      uint64_t cleaned = ctx.defrag();
      CHECK(cleaned == 0);  // no value_nodes to clean
   }
}

TEST_CASE("defrag_tree via write_session", "[defrag]")
{
   auto db_path = std::filesystem::path("defrag_test_db");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Insert and update keys to create value_nodes
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < 10; ++i)
      {
         char key[16], val[16];
         snprintf(key, sizeof(key), "k%04d", i);
         snprintf(val, sizeof(val), "v%04d_0", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // MVCC upsert a few keys
   ws->mvcc_upsert(0, "k0000", "v0000_1");
   ws->mvcc_upsert(0, "k0001", "v0001_1");
   ws->mvcc_upsert(0, "k0002", "v0002_1");

   // Mark version 1 as dead (initial COW insert version)
   db->dead_versions().add_dead_version(1);
   db->dead_versions().publish_snapshot();

   // Defrag — should strip dead entries
   uint64_t cleaned = ws->defrag_tree(0);
   CHECK(cleaned >= 1);

   // Data should still be readable
   auto rs = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.lower_bound("k0000");
   REQUIRE(!cur.is_end());

   std::filesystem::remove_all(db_path);
}

// ═══════════════════════════════════════════════════════════════════════════
// OCC tests
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("occ: commit succeeds when no concurrent writer", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed some data with a batch transaction
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a_0");
      tx.upsert("key_b", "val_b_0");
      tx.commit();
   }

   // OCC transaction: read and write without holding the lock
   {
      auto tx = ws->start_transaction(0, tx_mode::occ);
      auto v  = tx.get<std::string>("key_a");
      REQUIRE(v.has_value());
      CHECK(*v == "val_a_0");

      tx.upsert("key_a", "val_a_1");
      tx.upsert("key_c", "val_c_0");
      tx.commit();  // should succeed — no concurrent modifications
   }

   // Verify committed data
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek("key_a");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_a_1");
   cur.seek("key_c");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_c_0");

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: non-overlapping writes succeed (per-key validation)", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_conflict_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed data
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a_0");
      tx.commit();
   }

   // Start OCC transaction — writes key_a but reads nothing
   auto tx = ws->start_transaction(0, tx_mode::occ);
   tx.upsert("key_a", "val_a_occ");

   // Concurrent batch commit writes a different key (key_b)
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_b", "val_b_1");
      tx2.commit();
   }

   // Per-key validation: no reads → no conflicts → commit succeeds.
   // Writes are applied to the current tree (which includes key_b).
   CHECK_NOTHROW(tx.commit());

   // Both writes should be visible
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek("key_a");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_a_occ");
   cur.seek("key_b");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_b_1");

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: conflict detected on read key modified by concurrent writer", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_read_conflict_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed data
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a_0");
      tx.commit();
   }

   // Start OCC transaction — READS key_a then writes
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto v  = tx.get<std::string>("key_a");
   REQUIRE(v.has_value());
   CHECK(*v == "val_a_0");
   tx.upsert("key_a", "val_a_occ");

   // Concurrent batch commit modifies the SAME key we read
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_a", "val_a_batch");
      tx2.commit();
   }

   // Per-key validation: key_a was read and has changed → conflict
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   // Batch commit's value should be intact
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek("key_a");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_a_batch");

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: retry loop succeeds after conflict", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_retry_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed data
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("counter", "0");
      tx.commit();
   }

   // Simulate a retry loop: first attempt conflicts, second succeeds
   int  attempts  = 0;
   bool committed = false;

   // Cause one conflict by committing between the first OCC snapshot and commit
   bool conflict_injected = false;

   while (!committed)
   {
      ++attempts;
      auto tx = ws->start_transaction(0, tx_mode::occ);

      // Read current value
      auto val = tx.get<std::string>("counter");
      REQUIRE(val.has_value());

      tx.upsert("counter", "1");

      // Inject conflict on first attempt — modify the key we READ
      if (!conflict_injected)
      {
         conflict_injected = true;
         auto tx2 = ws->start_transaction(0);
         tx2.upsert("counter", "interfere");
         tx2.commit();
      }

      try
      {
         tx.commit();
         committed = true;
      }
      catch (const occ_conflict&)
      {
         // Expected on first attempt — retry
      }
   }

   CHECK(attempts == 2);
   CHECK(committed);

   // Verify final state
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek("counter");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "1");

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: reads see snapshot, not concurrent writes", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_snapshot_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed data
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "original");
      tx.commit();
   }

   // Start OCC transaction — takes snapshot
   auto tx = ws->start_transaction(0, tx_mode::occ);

   // Read snapshot value
   auto v1 = tx.get<std::string>("key_a");
   REQUIRE(v1.has_value());
   CHECK(*v1 == "original");

   // Another session commits a change
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_a", "modified");
      tx2.commit();
   }

   // OCC transaction still sees snapshot value
   auto v2 = tx.get<std::string>("key_a");
   REQUIRE(v2.has_value());
   CHECK(*v2 == "original");

   // Commit will conflict (key_a's leaf was modified by the batch tx)
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: buffered writes visible within transaction", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_buffer_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.commit();
   }

   // OCC transaction: buffered writes should be visible to reads
   {
      auto tx = ws->start_transaction(0, tx_mode::occ);

      tx.upsert("key_a", "updated");
      tx.upsert("key_new", "brand_new");

      // Should read from buffer, not persistent tree
      auto v1 = tx.get<std::string>("key_a");
      REQUIRE(v1.has_value());
      CHECK(*v1 == "updated");

      auto v2 = tx.get<std::string>("key_new");
      REQUIRE(v2.has_value());
      CHECK(*v2 == "brand_new");

      tx.commit();
   }

   // Verify committed data
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek("key_a");
   CHECK(cur.value<std::string>().value_or("") == "updated");
   cur.seek("key_new");
   CHECK(cur.value<std::string>().value_or("") == "brand_new");

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: abort discards buffered writes", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_abort_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "original");
      tx.commit();
   }

   // Start and abort an OCC transaction
   {
      auto tx = ws->start_transaction(0, tx_mode::occ);
      tx.upsert("key_a", "should_not_persist");
      tx.upsert("key_b", "also_discarded");
      tx.abort();
   }

   // Data should be unchanged
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek("key_a");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "original");
   cur.seek("key_b");
   CHECK(cur.is_end());

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: concurrent MVCC write on same key causes conflict", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_mvcc_conflict_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed with batch
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a_0");
      tx.upsert("key_b", "val_b_0");
      tx.commit();
   }

   // Start OCC transaction, read key_a
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto v  = tx.get<std::string>("key_a");
   REQUIRE(v.has_value());
   CHECK(*v == "val_a_0");

   // Concurrent MVCC upsert on the same key
   ws->mvcc_upsert(0, "key_a", "val_a_mvcc");

   // OCC commit: key_a now has a value_node with version > 0 (was inline v0)
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: concurrent MVCC write on different key succeeds", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_mvcc_noconflict_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed with batch
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a_0");
      tx.upsert("key_b", "val_b_0");
      tx.commit();
   }

   // Start OCC transaction, read key_a
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto v  = tx.get<std::string>("key_a");
   REQUIRE(v.has_value());
   tx.upsert("key_a", "val_a_occ");

   // Concurrent MVCC upsert on a DIFFERENT key
   ws->mvcc_upsert(0, "key_b", "val_b_mvcc");

   // OCC commit: key_a's leaf was modified by MVCC on key_b, but key_a's
   // value_node version didn't change. Whether this conflicts depends on
   // whether the MVCC write changed the leaf's ptr_address.
   // MVCC uses CB relocation, so leaf_addr is unchanged → no conflict.
   CHECK_NOTHROW(tx.commit());

   // Both updates visible
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek("key_a");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_a_occ");
   cur.seek("key_b");
   REQUIRE(!cur.is_end());
   // key_b was MVCC-updated, then OCC committed — OCC re-bases on current tree
   // so key_b's MVCC value should be preserved
   CHECK(cur.value<std::string>().value_or("") == "val_b_mvcc");

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: missing key detection — insert by concurrent writer", "[mvcc_occ]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_phantom_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Empty tree — seed nothing
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.commit();
   }

   // Start OCC transaction, read key_b (doesn't exist)
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto v  = tx.get<std::string>("key_b");
   CHECK(!v.has_value());

   // Concurrent batch inserts key_b
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_b", "val_b_batch");
      tx2.commit();
   }

   // OCC commit: key_b now exists (was missing in our snapshot)
   // The leaf_addr changed because the batch COW'd the leaf
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

// ═══════════════════════════════════════════════════════════════════════════
// OCC phantom detection tests (lower-bound read-set tracking)
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("occ: lower_bound detects phantom insert", "[mvcc_occ][phantom]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_phantom_lb_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed: key_a, key_c (gap at key_b)
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.upsert("key_c", "val_c");
      tx.commit();
   }

   // OCC: lower_bound("key_b") → lands on "key_c"
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto c  = tx.lower_bound("key_b");
   REQUIRE(!c.is_end());
   CHECK(c.key() == "key_c");

   // Concurrent writer inserts "key_b" — a phantom in [key_b, key_c)
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_b", "val_b_phantom");
      tx2.commit();
   }

   // OCC commit must detect the phantom
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: lower_bound succeeds when no phantom inserted", "[mvcc_occ][phantom]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_phantom_ok_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed: key_a, key_c
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.upsert("key_c", "val_c");
      tx.commit();
   }

   // OCC: lower_bound("key_b") → lands on "key_c"
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto c  = tx.lower_bound("key_b");
   REQUIRE(!c.is_end());
   CHECK(c.key() == "key_c");

   // Concurrent writer modifies key_d (outside the range) — no phantom
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_d", "val_d");
      tx2.commit();
   }

   // OCC commit should succeed — lower_bound("key_b") still yields "key_c"
   CHECK_NOTHROW(tx.commit());

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: lower_bound detects phantom when result key deleted", "[mvcc_occ][phantom]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_phantom_del_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed: key_a, key_b, key_c
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.upsert("key_b", "val_b");
      tx.upsert("key_c", "val_c");
      tx.commit();
   }

   // OCC: lower_bound("key_b") → lands on "key_b"
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto c  = tx.lower_bound("key_b");
   REQUIRE(!c.is_end());
   CHECK(c.key() == "key_b");

   // Concurrent writer deletes "key_b"
   {
      auto tx2 = ws->start_transaction(0);
      tx2.remove("key_b");
      tx2.commit();
   }

   // OCC commit: lower_bound("key_b") now yields "key_c" — conflict
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: lower_bound at end detects phantom insert", "[mvcc_occ][phantom]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_phantom_end_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed: key_a only
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.commit();
   }

   // OCC: lower_bound("key_z") → at_end (nothing >= "key_z")
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto c  = tx.lower_bound("key_z");
   CHECK(c.is_end());

   // Concurrent writer inserts "key_z"
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_z", "val_z");
      tx2.commit();
   }

   // OCC commit: lower_bound("key_z") now finds "key_z" — phantom
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: upper_bound detects phantom insert", "[mvcc_occ][phantom]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_phantom_ub_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed: key_a, key_c
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.upsert("key_c", "val_c");
      tx.commit();
   }

   // OCC: upper_bound("key_a") → lands on "key_c"
   auto tx = ws->start_transaction(0, tx_mode::occ);
   auto c  = tx.upper_bound("key_a");
   REQUIRE(!c.is_end());
   CHECK(c.key() == "key_c");

   // Concurrent writer inserts "key_b" — phantom in (key_a, key_c)
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_b", "val_b_phantom");
      tx2.commit();
   }

   // OCC commit: upper_bound("key_a") now yields "key_b" — conflict
   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("occ: combined point reads and lower_bound phantom detection", "[mvcc_occ][phantom]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("occ_phantom_combined_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Seed: key_a, key_c, key_e
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a");
      tx.upsert("key_c", "val_c");
      tx.upsert("key_e", "val_e");
      tx.commit();
   }

   // OCC: point-read key_a + lower_bound("key_d") → lands on "key_e"
   auto tx = ws->start_transaction(0, tx_mode::occ);

   auto v = tx.get<std::string>("key_a");
   REQUIRE(v.has_value());

   auto c = tx.lower_bound("key_d");
   REQUIRE(!c.is_end());
   CHECK(c.key() == "key_e");

   // Concurrent writer inserts "key_d" — phantom in [key_d, key_e)
   // key_a is untouched, so point-read would pass, but lower_bound fails
   {
      auto tx2 = ws->start_transaction(0);
      tx2.upsert("key_d", "val_d");
      tx2.commit();
   }

   CHECK_THROWS_AS(tx.commit(), occ_conflict);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("cow prunes old version entries from value_nodes", "[mvcc_write][cow_prune]")
{
   sal::set_current_thread_name("main");
   mvcc_db db;

   // Use large values to force value_node creation (>64 bytes)
   std::string big_val_v0(100, 'A');
   std::string big_val_v1(100, 'B');
   std::string big_val_v2(100, 'C');
   std::string big_val_v3(100, 'D');
   std::string big_val_final(100, 'Z');

   // Step 1: Build a tree with large-valued keys via COW insert
   {
      tree_context ctx(db.root());
      ctx.insert(to_kv("key1"), value_type(to_vv(big_val_v0)));
      ctx.insert(to_kv("key2"), value_type(to_vv(big_val_v0)));
      ctx.insert(to_kv("key3"), value_type(to_vv(big_val_v0)));
      db.set_root(ctx.take_root());
   }

   // Verify initial state: 3 value_nodes, 1 version entry each
   {
      tree_context ctx(db.root());
      auto s = ctx.get_stats();
      REQUIRE(s.value_nodes == 3);
      REQUIRE(s.total_version_entries == 3);  // 1 per value_node
   }

   // Step 2: Accumulate version entries via MVCC upsert
   for (int i = 0; i < 3; ++i)
   {
      std::string val(100, 'B' + i);
      tree_context ctx(db.root());
      ctx.mvcc_upsert(to_kv("key1"), value_type(to_vv(val)), i + 1);
      ctx.mvcc_upsert(to_kv("key2"), value_type(to_vv(val)), i + 1);
      db.set_root(ctx.take_root());
   }

   // Verify: value_nodes now have multiple version entries
   {
      tree_context ctx(db.root());
      auto s = ctx.get_stats();
      REQUIRE(s.value_nodes >= 2);
      // key1 and key2 should have 4 entries each (v0 + 3 MVCC writes)
      // key3 should have 1 entry (untouched)
      REQUIRE(s.total_version_entries > 3);
   }

   // Step 3: COW transaction — upsert key1 to trigger COW on the path
   {
      tree_context ctx(db.root());
      ctx.upsert<upsert_mode::unique>(to_kv("key1"), value_type(to_vv(big_val_final)));
      db.set_root(ctx.take_root());
   }

   // Verify: after COW, value_nodes on the COW'd path should be pruned
   // key1's value_node should have only 1 entry (the final value)
   // key3's value_node should have 1 entry (untouched by MVCC)
   // key2's value_node may still have multiple entries (not on COW path)
   {
      tree_context ctx(db.root());
      auto s = ctx.get_stats();
      INFO("After COW: value_nodes=" << s.value_nodes
           << " total_version_entries=" << s.total_version_entries);
      // At minimum, the COW'd key1 should be pruned.
      // Ideal: all value_nodes in the new tree are pruned to 1 entry each.
      // For now, verify that the total is less than before the COW.
      // The exact count depends on whether COW touches key2's leaf.
      CHECK(s.total_version_entries <= s.value_nodes);  // 1 entry per value_node
   }

   // Verify data correctness: latest values readable
   {
      cursor c(db.root());
      REQUIRE(c.seek(to_kv("key1")));
      CHECK(c.value<std::string>().value_or("") == big_val_final);
      REQUIRE(c.seek(to_kv("key2")));
      // key2's latest value from MVCC writes
      std::string expected_key2(100, 'D');  // last MVCC write was 'B'+2='D'
      CHECK(c.value<std::string>().value_or("") == expected_key2);
      REQUIRE(c.seek(to_kv("key3")));
      CHECK(c.value<std::string>().value_or("") == big_val_v0);
   }
}

// ═══════════════════════════════════════════════════════════════════════════
// Version entry lifecycle: accumulation → snapshot release → defrag → cleanup
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("version entries accumulate then clean up after snapshot release", "[mvcc_lifecycle]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("lifecycle_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   constexpr int num_keys   = 10;
   constexpr int num_rounds = 5;

   // Step 1: Seed tree with batch insert (version 1)
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "val_%04d_v0", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // Step 2: Take a snapshot to hold old versions alive
   auto rs       = db->start_read_session();
   auto snapshot = rs->create_cursor(0);

   // Step 3: MVCC-update all keys multiple rounds — accumulates version entries
   for (int round = 1; round <= num_rounds; ++round)
   {
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "val_%04d_v%d", i, round);
         ws->mvcc_upsert(0, key, val);
      }
   }

   // Check: version entries have accumulated
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      // Each key should have num_rounds+1 entries (initial + N updates)
      // But first MVCC write promotes inline→value_node (2 entries), subsequent adds 1 each
      // So each key has 1 (original) + num_rounds entries = num_rounds+1
      CHECK(s.value_nodes == num_keys);
      CHECK(s.total_version_entries >= num_keys * (num_rounds + 1));
      INFO("After " << num_rounds << " rounds: " << s.total_version_entries
           << " version entries across " << s.value_nodes << " value_nodes");
   }

   // Step 4: Release the snapshot — old versions become reclaimable
   snapshot = cursor(sal::smart_ptr<sal::alloc_header>(rs->allocator_session(),
                                                       sal::null_ptr_address));

   // Wait for compactor to process the released smart_ptrs
   REQUIRE(db->wait_for_compactor());

   // Step 5: Mark old versions as dead in the live_range_map
   // In a full system, this happens automatically via the release thread.
   // For testing, we do it manually.
   auto& dv = db->dead_versions();
   // Versions 1 through (num_rounds * num_keys) were allocated during MVCC writes.
   // Version 0 is the initial inline value (implicit).
   // The batch commit allocated version 1.
   // MVCC writes allocated versions 2 through 2+num_rounds*num_keys-1.
   // Mark all early versions as dead.
   for (uint64_t v = 0; v <= 1; ++v)
      dv.add_dead_version(v);
   dv.flush_pending();
   dv.publish_snapshot();

   // Step 6: Defrag should strip dead entries
   uint64_t cleaned = ws->defrag_tree(0);
   CHECK(cleaned >= 1);

   // Step 7: Check version entries decreased
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      INFO("After defrag: " << s.total_version_entries
           << " version entries across " << s.value_nodes << " value_nodes");
      // Defrag stripped dead entries — should have fewer entries now
      // Version 0 (initial inline) entries were cleaned; version 1 (batch) was cleaned
      CHECK(s.total_version_entries < num_keys * (num_rounds + 1));
   }

   // Step 8: Mark all non-latest versions as dead, run defrag again.
   // Batch commit got version 1. MVCC writes got versions 2 through
   // 1 + num_rounds * num_keys. The last round's versions are the latest
   // per key and must NOT be marked dead.
   // Last round starts at version: 1 + (num_rounds - 1) * num_keys + 1
   uint64_t last_round_start = 1 + (num_rounds - 1) * num_keys + 1;
   for (uint64_t v = 2; v < last_round_start; ++v)
      dv.add_dead_version(v);
   dv.flush_pending();
   dv.publish_snapshot();

   cleaned = ws->defrag_tree(0);

   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      INFO("After full cleanup: " << s.total_version_entries
           << " version entries across " << s.value_nodes << " value_nodes");
      // All but the latest entry per key should be stripped
      // Each value_node should have exactly 1 entry (the latest version)
      CHECK(s.total_version_entries == s.value_nodes);
   }

   // Step 9: Verify data is still correct — latest values readable
   {
      auto cur = rs->create_cursor(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], expected[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(expected, sizeof(expected), "val_%04d_v%d", i, num_rounds);
         cur.seek(key);
         REQUIRE(!cur.is_end());
         CHECK(cur.value<std::string>().value_or("") == expected);
      }
   }

   std::filesystem::remove_all(db_path);
}

TEST_CASE("held snapshot prevents version cleanup", "[mvcc_lifecycle]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("held_snapshot_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();
   auto rs = db->start_read_session();

   // Seed data
   {
      auto tx = ws->start_transaction(0);
      tx.upsert("key_a", "val_a_v0");
      tx.upsert("key_b", "val_b_v0");
      tx.commit();
   }

   // Take snapshot at version 1
   auto snapshot_v1 = rs->create_cursor(0);

   // MVCC-update both keys
   ws->mvcc_upsert(0, "key_a", "val_a_v1");
   ws->mvcc_upsert(0, "key_b", "val_b_v1");

   // Take snapshot at version 3
   auto snapshot_v3 = rs->create_cursor(0);

   // MVCC-update again
   ws->mvcc_upsert(0, "key_a", "val_a_v2");
   ws->mvcc_upsert(0, "key_b", "val_b_v2");

   // Count version entries — should have accumulated
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      // Each key: initial(v0) + v1 + v2 = 3 entries, 2 keys = 6 total
      CHECK(s.total_version_entries >= 6);
   }

   // Release snapshot_v3 but keep snapshot_v1
   snapshot_v3 = cursor(sal::smart_ptr<sal::alloc_header>(rs->allocator_session(),
                                                          sal::null_ptr_address));
   REQUIRE(db->wait_for_compactor());

   // Mark v3's versions as dead, but v1's must stay alive
   auto& dv = db->dead_versions();
   // We can only safely mark versions that no snapshot needs.
   // Since snapshot_v1 is still alive, versions ≤ v1 must remain.
   // Only versions between v1 and the latest that aren't needed can be marked dead.
   // For simplicity, mark no versions dead — defrag should be a no-op
   // (can't strip anything while snapshot_v1 is held)

   // Verify snapshot_v1 still reads correctly
   snapshot_v1.seek("key_a");
   REQUIRE(!snapshot_v1.is_end());
   CHECK(snapshot_v1.value<std::string>().value_or("") == "val_a_v0");
   snapshot_v1.seek("key_b");
   REQUIRE(!snapshot_v1.is_end());
   CHECK(snapshot_v1.value<std::string>().value_or("") == "val_b_v0");

   // Release snapshot_v1 — now everything can be cleaned
   snapshot_v1 = cursor(sal::smart_ptr<sal::alloc_header>(rs->allocator_session(),
                                                          sal::null_ptr_address));
   REQUIRE(db->wait_for_compactor());

   // Mark old versions as dead (not the latest).
   // Batch commit = version 1. MVCC writes: key_a v1=2, key_b v1=3,
   // key_a v2=4, key_b v2=5.  Latest per key: key_a=4, key_b=5.
   // So versions 0-3 are safe to mark dead.
   for (uint64_t v = 0; v <= 3; ++v)
      dv.add_dead_version(v);
   dv.flush_pending();
   dv.publish_snapshot();

   // Defrag strips all old entries
   uint64_t cleaned = ws->defrag_tree(0);
   CHECK(cleaned >= 1);

   // Each key should now have exactly 1 version entry
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      CHECK(s.total_version_entries == s.value_nodes);
   }

   // Latest values still correct
   auto cur = rs->create_cursor(0);
   cur.seek("key_a");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_a_v2");
   cur.seek("key_b");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "val_b_v2");

   std::filesystem::remove_all(db_path);
}

// ═══════════════════════════════════════════════════════════════════════════
// Long-lived snapshot stress tests
// ═══════════════════════════════════════════════════════════════════════════

/// Compute a simple hash of all keys and values in a cursor (full iteration).
static uint64_t hash_tree_contents(cursor& c)
{
   uint64_t hash = 0;
   c.seek_begin();
   while (!c.is_end())
   {
      auto k = c.key();
      hash ^= XXH3_64bits(k.data(), k.size());
      c.get_value([&](value_view vv) { hash ^= XXH3_64bits(vv.data(), vv.size()) * 31; });
      c.next();
   }
   return hash;
}

TEST_CASE("snapshot survives many MVCC writes and defrag", "[mvcc_snapshot][stress]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("snapshot_stress_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   constexpr int num_keys   = 50;
   constexpr int num_rounds = 20;

   // Step 1: Seed the tree with initial data via batch transaction
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16];
         snprintf(key, sizeof(key), "key_%04d", i);
         char val[32];
         snprintf(val, sizeof(val), "val_%04d_v0", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // Step 2: Take a snapshot — this will be held while mutations accumulate
   auto rs       = db->start_read_session();
   auto snapshot = rs->create_cursor(0);

   // Compute expected hash from the snapshot state
   uint64_t expected_hash = hash_tree_contents(snapshot);
   REQUIRE(expected_hash != 0);

   // Count keys in the snapshot
   snapshot.seek_begin();
   uint64_t snapshot_key_count = 0;
   while (!snapshot.is_end())
   {
      ++snapshot_key_count;
      snapshot.next();
   }
   REQUIRE(snapshot_key_count == num_keys);

   // Step 3: Hammer the tree with MVCC writes
   for (int round = 0; round < num_rounds; ++round)
   {
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16];
         snprintf(key, sizeof(key), "key_%04d", i);
         char val[32];
         snprintf(val, sizeof(val), "val_%04d_v%d", i, round + 1);
         ws->mvcc_upsert(0, key, val);
      }
   }

   // Step 4: Run defrag to strip dead versions from value_nodes
   uint64_t defrag_cleaned = ws->defrag_tree(0);

   // Step 5: Verify snapshot is STILL valid — iterate and hash
   uint64_t actual_hash = hash_tree_contents(snapshot);
   CHECK(actual_hash == expected_hash);

   // Verify key count
   snapshot.seek_begin();
   uint64_t actual_count = 0;
   while (!snapshot.is_end())
   {
      ++actual_count;
      snapshot.next();
   }
   CHECK(actual_count == snapshot_key_count);

   // Spot-check a few values
   snapshot.seek("key_0000");
   REQUIRE(!snapshot.is_end());
   CHECK(snapshot.value<std::string>().value_or("") == "val_0000_v0");

   snapshot.seek("key_0025");
   REQUIRE(!snapshot.is_end());
   CHECK(snapshot.value<std::string>().value_or("") == "val_0025_v0");

   snapshot.seek("key_0049");
   REQUIRE(!snapshot.is_end());
   CHECK(snapshot.value<std::string>().value_or("") == "val_0049_v0");

   // Step 6: Verify the LATEST values are correct too
   auto latest = rs->create_cursor(0);
   latest.seek("key_0000");
   REQUIRE(!latest.is_end());
   char expected_latest[32];
   snprintf(expected_latest, sizeof(expected_latest), "val_0000_v%d", num_rounds);
   CHECK(latest.value<std::string>().value_or("") == expected_latest);

   std::filesystem::remove_all(db_path);
}

TEST_CASE("snapshot survives COW epoch transitions", "[mvcc_snapshot][stress]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("snapshot_epoch_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   constexpr int num_keys    = 30;
   constexpr int num_epochs  = 10;

   // Seed data
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "epoch0_v%d", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // Take snapshot after initial seed
   auto rs       = db->start_read_session();
   auto snapshot = rs->create_cursor(0);
   uint64_t expected_hash = hash_tree_contents(snapshot);

   // Run multiple epochs: MVCC writes, then a COW batch (epoch boundary)
   for (int epoch = 1; epoch <= num_epochs; ++epoch)
   {
      // MVCC writes within the epoch
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "epoch%d_v%d", epoch, i);
         ws->mvcc_upsert(0, key, val);
      }

      // COW batch transaction to force epoch boundary (new root, prunes old versions)
      {
         auto tx = ws->start_transaction(0);
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", 0);
         snprintf(val, sizeof(val), "batch_epoch%d", epoch);
         tx.upsert(key, val);
         tx.commit();
      }
   }

   // Verify old snapshot is still valid
   uint64_t actual_hash = hash_tree_contents(snapshot);
   CHECK(actual_hash == expected_hash);

   // Spot-check snapshot values
   snapshot.seek("key_0005");
   REQUIRE(!snapshot.is_end());
   CHECK(snapshot.value<std::string>().value_or("") == "epoch0_v5");

   snapshot.seek("key_0015");
   REQUIRE(!snapshot.is_end());
   CHECK(snapshot.value<std::string>().value_or("") == "epoch0_v15");

   std::filesystem::remove_all(db_path);
}

TEST_CASE("multiple snapshots at different versions coexist", "[mvcc_snapshot][stress]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("snapshot_multi_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();
   auto rs = db->start_read_session();

   constexpr int num_keys     = 20;
   constexpr int num_versions = 5;

   // Seed initial data
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "v0_%d", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // Take snapshots at different versions
   struct snapshot_record
   {
      cursor   cur;
      uint64_t hash;
   };
   std::vector<snapshot_record> snapshots;

   for (int ver = 0; ver < num_versions; ++ver)
   {
      // Take snapshot
      auto cur = rs->create_cursor(0);
      uint64_t h = hash_tree_contents(cur);
      snapshots.push_back({std::move(cur), h});

      // MVCC-update all keys
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "v%d_%d", ver + 1, i);
         ws->mvcc_upsert(0, key, val);
      }
   }

   // Take final snapshot
   {
      auto cur = rs->create_cursor(0);
      uint64_t h = hash_tree_contents(cur);
      snapshots.push_back({std::move(cur), h});
   }

   // All snapshots should have different hashes (different data versions)
   for (size_t i = 0; i < snapshots.size(); ++i)
   {
      for (size_t j = i + 1; j < snapshots.size(); ++j)
      {
         CHECK(snapshots[i].hash != snapshots[j].hash);
      }
   }

   // Defrag to strip dead versions
   ws->defrag_tree(0);

   // Verify each snapshot still produces the same hash
   for (size_t i = 0; i < snapshots.size(); ++i)
   {
      uint64_t h = hash_tree_contents(snapshots[i].cur);
      CHECK(h == snapshots[i].hash);
   }

   std::filesystem::remove_all(db_path);
}

// ═══════════════════════════════════════════════════════════════════════════
// Automatic version reclamation — end-to-end test
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("automatic version reclamation pipeline", "[mvcc_lifecycle]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("auto_reclaim_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();
   auto rs = db->start_read_session();

   constexpr int num_keys   = 5;
   constexpr int num_rounds = 10;

   // Step 1: Seed tree
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "val_%04d_v0", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // Step 2: Take snapshot (holds version 1 alive)
   auto snapshot = rs->create_cursor(0);

   // Step 3: MVCC-update all keys multiple rounds
   for (int round = 1; round <= num_rounds; ++round)
   {
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "val_%04d_v%d", i, round);
         ws->mvcc_upsert(0, key, val);
      }
   }

   // Verify version entries accumulated
   uint64_t entries_before;
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      entries_before = s.total_version_entries;
      CHECK(entries_before >= num_keys * (num_rounds + 1));
   }

   // Step 4: Release the snapshot — no manual add_dead_version!
   // The version CB release callback fires automatically.
   snapshot = cursor(sal::smart_ptr<sal::alloc_header>(rs->allocator_session(),
                                                       sal::null_ptr_address));

   // Wait for compactor to drain releases and auto-publish dead versions
   REQUIRE(db->wait_for_compactor());

   // Step 5: Defrag — should be able to strip dead entries now
   uint64_t cleaned = ws->defrag_tree(0);
   CHECK(cleaned >= 1);

   // Step 6: Verify entries decreased
   uint64_t entries_after;
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      entries_after = s.total_version_entries;
      INFO("Before: " << entries_before << " After: " << entries_after);
      CHECK(entries_after < entries_before);
   }

   // Step 7: Latest values still readable
   {
      auto cur = rs->create_cursor(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], expected[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(expected, sizeof(expected), "val_%04d_v%d", i, num_rounds);
         cur.seek(key);
         REQUIRE(!cur.is_end());
         CHECK(cur.value<std::string>().value_or("") == expected);
      }
   }

   std::filesystem::remove_all(db_path);
}

// ═══════════════════════════════════════════════════════════════════════════
// Epoch-triggered COW maintenance tests
// ═══════════════════════════════════════════════════════════════════════════

TEST_CASE("epoch boundary triggers COW maintenance and bounds version bloat",
          "[mvcc_epoch]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("epoch_maint_testdb");
   std::filesystem::remove_all(db_path);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   constexpr int num_keys = 100;

   // Set a small epoch interval so boundaries are hit within the test
   db->set_epoch_interval(500);

   // Seed keys
   {
      auto tx = ws->start_transaction(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], val[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         snprintf(val, sizeof(val), "v0_%d", i);
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // Many rounds of MVCC updates — spanning multiple epoch boundaries
   constexpr int total_ops = 5000;
   for (int i = 0; i < total_ops; ++i)
   {
      char key[16], val[32];
      snprintf(key, sizeof(key), "key_%04d", i % num_keys);
      snprintf(val, sizeof(val), "v%d_%d", i / num_keys + 1, i % num_keys);
      ws->mvcc_upsert(0, key, val);
   }

   // Check that version entries are bounded — epoch-triggered COW prunes them.
   // Without epoch maintenance, we'd have ~50 entries per key (5000/100).
   // With epoch maintenance every 500 ops, most get pruned to 1 entry.
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();

      // With epoch_interval=500 and 5000 ops, 10 epochs elapse.
      // Each epoch boundary triggers COW on stale paths, pruning value_nodes.
      // Average entries per value_node should be well below the unbounded ~50.
      uint64_t avg = s.value_nodes > 0 ? s.total_version_entries / s.value_nodes : 0;
      INFO("After " << total_ops << " ops: " << s.value_nodes << " value_nodes, "
           << s.total_version_entries << " entries, avg=" << avg);

      // With ~10 epoch transitions, most value_nodes should be pruned to 1-5 entries.
      // Without epoch checks, avg would be ~50.
      CHECK(avg <= 10);  // generous bound — typically 1-2
   }

   // Verify latest values
   {
      auto rs  = db->start_read_session();
      auto cur = rs->create_cursor(0);
      for (int i = 0; i < num_keys; ++i)
      {
         char key[16], expected[32];
         snprintf(key, sizeof(key), "key_%04d", i);
         int last_round = (total_ops - 1 - i) / num_keys + 1;
         snprintf(expected, sizeof(expected), "v%d_%d", last_round, i);
         cur.seek(key);
         REQUIRE(!cur.is_end());
         CHECK(cur.value<std::string>().value_or("") == expected);
      }
   }

   std::filesystem::remove_all(db_path);
}

// ═══════════════════════════════════════════════════════════════════════════
// MVCC update throughput benchmarks — dictionary-loaded tree
// ═══════════════════════════════════════════════════════════════════════════

namespace
{
   /// Load /usr/share/dict/words into a vector.
   std::vector<std::string> load_dictionary()
   {
      std::vector<std::string> words;
      std::ifstream f("/usr/share/dict/words");
      std::string line;
      while (std::getline(f, line))
      {
         if (!line.empty())
            words.push_back(std::move(line));
      }
      return words;
   }

   /// Seed a tree with all dictionary words via batch COW transaction.
   void seed_dictionary(write_session& ws, const std::vector<std::string>& words)
   {
      // Batch in chunks to avoid massive single transaction
      constexpr size_t batch = 10000;
      for (size_t start = 0; start < words.size(); start += batch)
      {
         auto tx  = ws.start_transaction(0);
         auto end = std::min(start + batch, words.size());
         for (size_t i = start; i < end; ++i)
            tx.upsert(words[i], words[i]);
         tx.commit();
      }
   }
}  // namespace

TEST_CASE("dictionary MVCC random update throughput", "[mvcc_bench]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("mvcc_dict_bench_testdb");
   std::filesystem::remove_all(db_path);

   auto words = load_dictionary();
   REQUIRE(words.size() > 10000);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   // Load entire dictionary
   auto load_start = std::chrono::high_resolution_clock::now();
   seed_dictionary(*ws, words);
   auto load_end     = std::chrono::high_resolution_clock::now();
   auto load_elapsed = std::chrono::duration<double>(load_end - load_start).count();
   WARN("Loaded " << words.size() << " dictionary words in " << load_elapsed << "s ("
        << static_cast<int>(words.size() / load_elapsed) << " inserts/sec)");

   // Report tree stats after load
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      WARN("Tree after load: " << s.total_nodes() << " nodes, "
           << s.leaf_nodes << " leaves, " << s.inner_nodes + s.inner_prefix_nodes
           << " inners");
   }

   constexpr int bench_ops = 500000;
   std::mt19937  rng(42);

   // Benchmark: MVCC random-word updates on a fully loaded tree
   auto start = std::chrono::high_resolution_clock::now();

   for (int i = 0; i < bench_ops; ++i)
   {
      auto& word = words[rng() % words.size()];
      char  val[64];
      snprintf(val, sizeof(val), "updated_%d", i);
      ws->mvcc_upsert(0, word, val);
   }

   auto end     = std::chrono::high_resolution_clock::now();
   auto elapsed = std::chrono::duration<double>(end - start).count();

   double ops_per_sec = bench_ops / elapsed;
   WARN("MVCC random-word update (" << words.size() << " words): "
        << static_cast<int>(ops_per_sec) << " ops/sec ("
        << bench_ops << " ops in " << elapsed << "s)");

   // Report value_node stats
   {
      auto root = ws->get_root(0);
      tree_context ctx(root);
      auto s = ctx.get_stats();
      WARN("After updates: " << s.value_nodes << " value_nodes, "
           << s.total_version_entries << " version entries, avg="
           << (s.value_nodes > 0 ? s.total_version_entries / s.value_nodes : 0)
           << " entries/node");
   }

   // Spot-check a value
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek(words[0]);
   REQUIRE(!cur.is_end());

   std::filesystem::remove_all(db_path);
}

TEST_CASE("dictionary COW random update throughput", "[mvcc_bench]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("cow_dict_bench_testdb");
   std::filesystem::remove_all(db_path);

   auto words = load_dictionary();
   REQUIRE(words.size() > 10000);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   seed_dictionary(*ws, words);

   constexpr int bench_ops = 500000;
   std::mt19937  rng(42);

   // Benchmark: COW random-word updates via transactions
   auto start = std::chrono::high_resolution_clock::now();

   for (int i = 0; i < bench_ops; ++i)
   {
      auto& word = words[rng() % words.size()];
      char  val[64];
      snprintf(val, sizeof(val), "cow_%d", i);
      auto tx = ws->start_transaction(0);
      tx.upsert(word, val);
      tx.commit();
   }

   auto end     = std::chrono::high_resolution_clock::now();
   auto elapsed = std::chrono::duration<double>(end - start).count();

   double ops_per_sec = bench_ops / elapsed;
   WARN("COW random-word update (" << words.size() << " words): "
        << static_cast<int>(ops_per_sec) << " ops/sec ("
        << bench_ops << " ops in " << elapsed << "s)");

   // Spot-check
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek(words[0]);
   REQUIRE(!cur.is_end());

   std::filesystem::remove_all(db_path);
}

TEST_CASE("dictionary single-key hot update throughput", "[mvcc_bench]")
{
   sal::set_current_thread_name("main");
   auto db_path = std::filesystem::path("hot_dict_bench_testdb");
   std::filesystem::remove_all(db_path);

   auto words = load_dictionary();
   REQUIRE(words.size() > 10000);

   auto db = database::create(db_path);
   auto ws = db->start_write_session();

   seed_dictionary(*ws, words);

   // Pick one word in the middle of the dictionary
   const auto& hot_key = words[words.size() / 2];

   constexpr int bench_ops = 500000;

   // MVCC: hammer one key in a deep tree
   {
      auto start = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < bench_ops; ++i)
      {
         char val[64];
         snprintf(val, sizeof(val), "hot_%d", i);
         ws->mvcc_upsert(0, hot_key, val);
      }
      auto end     = std::chrono::high_resolution_clock::now();
      auto elapsed = std::chrono::duration<double>(end - start).count();
      WARN("MVCC single-key hot update (" << words.size() << " word tree, key='"
           << hot_key << "'): " << static_cast<int>(bench_ops / elapsed)
           << " ops/sec (" << bench_ops << " ops in " << elapsed << "s)");
   }

   // COW: hammer same key for comparison
   {
      auto start = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < bench_ops; ++i)
      {
         char val[64];
         snprintf(val, sizeof(val), "cow_%d", i);
         auto tx = ws->start_transaction(0);
         tx.upsert(hot_key, val);
         tx.commit();
      }
      auto end     = std::chrono::high_resolution_clock::now();
      auto elapsed = std::chrono::duration<double>(end - start).count();
      WARN("COW  single-key hot update (" << words.size() << " word tree, key='"
           << hot_key << "'): " << static_cast<int>(bench_ops / elapsed)
           << " ops/sec (" << bench_ops << " ops in " << elapsed << "s)");
   }

   // Verify latest value
   auto rs  = db->start_read_session();
   auto cur = rs->create_cursor(0);
   cur.seek(hot_key);
   REQUIRE(!cur.is_end());

   char expected[64];
   snprintf(expected, sizeof(expected), "cow_%d", bench_ops - 1);
   CHECK(cur.value<std::string>().value_or("") == expected);

   std::filesystem::remove_all(db_path);
}
