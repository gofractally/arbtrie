#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>
#include <fstream>
#include <map>

using namespace psitri;

constexpr int GAP_SCALE = 1;

namespace
{
   struct test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "coverage_gap_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }
      ~test_db() { std::filesystem::remove_all(dir); }
   };

   struct temp_tree_edit
   {
      explicit temp_tree_edit(write_session& ses)
          : tx(ses.start_write_transaction(ses.create_temporary_tree()))
      {
      }

      void upsert(key_view key, value_view value) { tx.upsert(key, value); }
      int  remove(key_view key) { return tx.remove(key); }
      bool remove_range_any(key_view lower, key_view upper)
      {
         return tx.remove_range_any(lower, upper);
      }

      uint64_t remove_range_counted(key_view lower, key_view upper)
      {
         return tx.remove_range_counted(lower, upper);
      }

      template <ConstructibleBuffer T>
      std::optional<T> get(key_view key) const
      {
         return tx.get<T>(key);
      }

      int32_t get(key_view key, Buffer auto* buffer) const { return tx.get(key, buffer); }

      cursor snapshot_cursor() const { return tx.snapshot_cursor(); }
      uint64_t count_keys() const
      {
         auto c = snapshot_cursor();
         return c.count_keys();
      }

      write_transaction tx;
   };

   temp_tree_edit start_temp_edit(test_db& t) { return temp_tree_edit(*t.ses); }

   std::string gkey(int i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "gkey-%08d", i);
      return buf;
   }

   std::string gval(int i, size_t size = 20)
   {
      std::string val(size, '\0');
      for (size_t j = 0; j < size; ++j)
         val[j] = static_cast<char>('A' + ((i + j) % 26));
      return val;
   }
}  // namespace

// ============================================================
// database::create() — static factory function (0% coverage)
// ============================================================

TEST_CASE("database::create creates new database", "[database][create]")
{
   const std::string dir = "create_testdb";
   std::filesystem::remove_all(dir);

   {
      auto db = database::create(dir);
      REQUIRE(db);
      REQUIRE(std::filesystem::exists(dir / std::filesystem::path("data")));
      REQUIRE(std::filesystem::exists(dir / std::filesystem::path("dbfile.bin")));

      // Should be able to write and read
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      tx.upsert("hello", "world");
      tx.commit();

      auto rses = db->start_read_session();
      auto root = rses->get_root(0);
      REQUIRE(root);
      auto c = root.snapshot_cursor();
      std::string buf;
      REQUIRE(c.get(to_key_view(std::string("hello")), &buf) >= 0);
      REQUIRE(buf == "world");
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("database::create throws if directory already exists", "[database][create]")
{
   const std::string dir = "create_exists_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   REQUIRE_THROWS_AS(database::create(dir), std::runtime_error);

   std::filesystem::remove_all(dir);
}

// ============================================================
// database::set_runtime_config() — 0% coverage
// ============================================================

TEST_CASE("database::set_runtime_config updates config", "[database][config]")
{
   const std::string dir = "config_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      runtime_config cfg;
      auto db = database::open(dir, open_mode::create_or_open, cfg);

      // Write some data
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 50; ++i)
         tx.upsert(gkey(i), gval(i));
      tx.commit();

      // Change config
      runtime_config new_cfg;
      new_cfg.sync_mode = sal::sync_type::msync_sync;
      db->set_runtime_config(new_cfg);

      // Write more data with new config — should still work
      auto tx2 = ses->start_transaction(0);
      for (int i = 50; i < 100; ++i)
         tx2.upsert(gkey(i), gval(i));
      tx2.commit();

      // Verify all data readable
      auto rses = db->start_read_session();
      auto root = rses->get_root(0);
      REQUIRE(root);
      auto c = root.snapshot_cursor();
      REQUIRE(c.count_keys() == 100);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// Database constructor error paths
// ============================================================

TEST_CASE("database constructor rejects corrupted magic number", "[database][error]")
{
   const std::string dir = "magic_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   // Create a valid database first
   {
      auto db = database::open(dir);
   }

   // Corrupt the magic number in dbfile.bin
   {
      sal::mapping dbfile(dir + "/dbfile.bin", sal::access_mode::read_write);
      auto* state = reinterpret_cast<detail::database_state*>(dbfile.data());
      state->magic = 0xDEADBEEF;
      dbfile.sync(sal::sync_type::full);
   }

   REQUIRE_THROWS_AS(database::open(dir, open_mode::open_existing), std::runtime_error);

   std::filesystem::remove_all(dir);
}

TEST_CASE("database constructor rejects wrong file size", "[database][error]")
{
   const std::string dir = "badsize_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   // Create a valid database first
   {
      auto db = database::open(dir);
   }

   // Truncate dbfile.bin to a wrong size (but non-zero so it doesn't get re-initialized)
   {
      auto dbfile_path = std::filesystem::path(dir) / "dbfile.bin";
      auto original_size = std::filesystem::file_size(dbfile_path);

      // Read current contents, write back with extra bytes appended
      std::vector<char> data(original_size + 64);
      {
         std::ifstream in(dbfile_path, std::ios::binary);
         in.read(data.data(), original_size);
      }
      {
         std::ofstream out(dbfile_path, std::ios::binary | std::ios::trunc);
         out.write(data.data(), data.size());
      }
   }

   REQUIRE_THROWS_AS(database::open(dir, open_mode::open_existing), std::runtime_error);

   std::filesystem::remove_all(dir);
}

// ============================================================
// range_remove with value_node entries (lines 58-72 in range_remove.hpp)
//
// value_nodes are created when a value exceeds ~64 bytes inline.
// range_remove over keys with large values should exercise the
// value_node case in the range_remove dispatch.
// ============================================================

TEST_CASE("range_remove with large values (value_node path)", "[range_remove][value_node]")
{
   test_db t;
   auto    cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;

   // Insert keys with large values (>64 bytes → value_node allocation)
   for (int i = 0; i < 200 / GAP_SCALE; ++i)
   {
      auto key = gkey(i);
      auto val = gval(i, 100 + (i % 200));  // 100-300 byte values
      cur.upsert(to_key_view(key), to_value_view(val));
      oracle[key] = val;
   }

   REQUIRE(cur.count_keys() == oracle.size());

   SECTION("remove range in the middle")
   {
      int lo = 50 / GAP_SCALE;
      int hi = 150 / GAP_SCALE;
      auto lo_key = gkey(lo);
      auto hi_key = gkey(hi);

      uint64_t removed = cur.remove_range_counted(lo_key, hi_key);
      REQUIRE(removed > 0);

      // Update oracle
      auto it = oracle.lower_bound(lo_key);
      while (it != oracle.end() && it->first < hi_key)
         it = oracle.erase(it);

      REQUIRE(cur.count_keys() == oracle.size());

      // Verify remaining values are intact
      auto rc = cur.snapshot_cursor();
      for (auto& [key, val] : oracle)
      {
         std::string buf;
         INFO("key: " << key);
         REQUIRE(rc.get(to_key_view(key), &buf) >= 0);
         REQUIRE(buf == val);
      }
   }

   SECTION("remove all via range")
   {
      uint64_t removed = cur.remove_range_counted("", max_key);
      REQUIRE(removed == oracle.size());
      REQUIRE(cur.count_keys() == 0);
   }
}

// ============================================================
// range_remove spanning multiple inner nodes with large values
// Forces the general case in range_remove_inner (start and
// boundary in different branches, middle branches fully removed)
// ============================================================

TEST_CASE("range_remove across inner boundaries with value_nodes", "[range_remove][value_node]")
{
   test_db t;
   auto    cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;

   // Create a tree with prefix groups and large values to force
   // both inner_prefix_node creation and value_node allocation
   const char* prefixes[] = {"aaa/", "bbb/", "ccc/", "ddd/", "eee/", "fff/"};
   for (auto* pfx : prefixes)
   {
      for (int i = 0; i < 30 / GAP_SCALE; ++i)
      {
         char buf[64];
         snprintf(buf, sizeof(buf), "%s%05d", pfx, i);
         std::string key(buf);
         auto        val = gval(i, 150);  // large value → value_node
         cur.upsert(to_key_view(key), to_value_view(val));
         oracle[key] = val;
      }
   }

   // Range that spans across multiple prefix groups:
   // removes all of bbb/, ccc/, ddd/ and partial aaa/ and eee/
   std::string lo = "aaa/00010";
   std::string hi = "eee/00010";

   uint64_t removed = cur.remove_range_counted(lo, hi);
   REQUIRE(removed > 0);

   auto it = oracle.lower_bound(lo);
   while (it != oracle.end() && it->first < hi)
      it = oracle.erase(it);

   REQUIRE(cur.count_keys() == oracle.size());

   // Verify remaining
   auto rc = cur.snapshot_cursor();
   for (auto& [key, val] : oracle)
   {
      std::string buf;
      INFO("key: " << key);
      REQUIRE(rc.get(to_key_view(key), &buf) >= 0);
      REQUIRE(buf == val);
   }
}

// ============================================================
// range_remove that produces multiple result branches
// (line 32 in range_remove.hpp: result.count() > 1 → make_inner)
// This requires a shared-mode range_remove where the root has
// ref > 1, causing shared-mode dispatch.
// ============================================================

TEST_CASE("range_remove on shared root (snapshot + modify)", "[range_remove][shared]")
{
   const std::string dir = "shared_range_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   auto db  = database::open(dir);
   auto ses = db->start_write_session();

   // Commit a tree with multiple prefix groups
   {
      auto tx = ses->start_transaction(0);
      for (int i = 0; i < 200 / GAP_SCALE; ++i)
      {
         auto key = gkey(i);
         auto val = gval(i, 80);  // above value_node threshold
         tx.upsert(key, val);
      }
      tx.commit();
   }

   // Take a snapshot (bumps root ref count)
   auto snap_root = ses->get_root(0);

   // Now modify via a new transaction — this forces shared-mode operations
   // because the root has ref > 1
   {
      auto tx = ses->start_transaction(0);
      // Range remove in the middle
      auto lo = gkey(50 / GAP_SCALE);
      auto hi = gkey(150 / GAP_SCALE);
      tx.remove_range_counted(lo, hi);
      tx.commit();
   }

   // Verify snapshot is still intact
   {
      auto c = snap_root.snapshot_cursor();
      REQUIRE(c.count_keys() == 200 / GAP_SCALE);
   }

   // Verify modified tree
   {
      auto root = ses->get_root(0);
      REQUIRE(root);
      auto c = root.snapshot_cursor();
      uint64_t count = c.count_keys();
      REQUIRE(count < 200 / GAP_SCALE);
      REQUIRE(count > 0);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// Mixed insert/range_remove with value_nodes and small values
// Exercises range_remove_leaf with value_node release (lines 117-121)
// ============================================================

TEST_CASE("range_remove releases value_nodes correctly", "[range_remove][value_node]")
{
   test_db t;
   auto    cur = start_temp_edit(t);

   std::map<std::string, std::string> oracle;

   // Mix of small inline values and large value_node values
   for (int i = 0; i < 100 / GAP_SCALE; ++i)
   {
      auto key = gkey(i);
      // Alternate: small inline value vs large value_node value
      size_t val_size = (i % 3 == 0) ? 200 : 10;
      auto   val      = gval(i, val_size);
      cur.upsert(to_key_view(key), to_value_view(val));
      oracle[key] = val;
   }

   // Remove a range that includes both small and large values
   auto lo = gkey(20 / GAP_SCALE);
   auto hi = gkey(80 / GAP_SCALE);

   uint64_t removed = cur.remove_range_counted(lo, hi);
   REQUIRE(removed > 0);

   auto it = oracle.lower_bound(lo);
   while (it != oracle.end() && it->first < hi)
      it = oracle.erase(it);

   REQUIRE(cur.count_keys() == oracle.size());

   // Insert new keys into the gap — verifies freed space is reclaimed
   for (int i = 1000; i < 1000 + 50 / GAP_SCALE; ++i)
   {
      auto key = gkey(i);
      auto val = gval(i, 150);
      cur.upsert(to_key_view(key), to_value_view(val));
      oracle[key] = val;
   }

   REQUIRE(cur.count_keys() == oracle.size());

   // Verify everything
   auto rc = cur.snapshot_cursor();
   for (auto& [key, val] : oracle)
   {
      std::string buf;
      INFO("key: " << key);
      REQUIRE(rc.get(to_key_view(key), &buf) >= 0);
      REQUIRE(buf == val);
   }
}

// ============================================================
// database::wait_for_compactor — partial coverage
// ============================================================

TEST_CASE("wait_for_compactor returns true on idle database", "[database][compactor]")
{
   const std::string dir = "compactor_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 50; ++i)
         tx.upsert(gkey(i), gval(i));
      tx.commit();

      // Compactor should drain quickly on a small database
      REQUIRE(db->wait_for_compactor(std::chrono::milliseconds(5000)));
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// database::get_stats / dump — exercise diagnostic paths
// ============================================================

TEST_CASE("database dump and get_stats", "[database][diagnostics]")
{
   test_db t;
   auto    tx = t.ses->start_transaction(0);
   for (int i = 0; i < 100; ++i)
      tx.upsert(gkey(i), gval(i, 50));
   tx.commit();

   // Exercise dump() and get_stats()
   auto dump = t.db->dump();
   auto stats = t.db->get_stats();
   std::string stats_str = stats.to_string();
   REQUIRE(!stats_str.empty());
   REQUIRE(stats.total_segments > 0);
   REQUIRE(stats.database_file_bytes > 0);
   // Regression: total_live_objects must reflect the 100 inserted keys
   // (plus tree-internal nodes). Pre-fix it was wired to dump's
   // total_read_nodes which under-counts after compaction. Should be > 0.
   REQUIRE(stats.total_live_objects > 0);
}

// ============================================================
// database::recover() and reset_reference_counts() — public API
// ============================================================

TEST_CASE("database::recover rebuilds from segments", "[database][recover]")
{
   const std::string dir = "recover_api_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 200; ++i)
         tx.upsert(gkey(i), gval(i));
      tx.commit();

      // Call recover() directly on a live database
      db->recover();

      // Data should still be accessible
      auto rses = db->start_read_session();
      auto root = rses->get_root(0);
      REQUIRE(root);
      auto c = root.snapshot_cursor();
      REQUIRE(c.count_keys() == 200);
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("database::reset_reference_counts on live database", "[database][recover]")
{
   const std::string dir = "reset_ref_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = database::open(dir);
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 200; ++i)
         tx.upsert(gkey(i), gval(i));
      tx.commit();

      db->reset_reference_counts();

      auto rses = db->start_read_session();
      auto root = rses->get_root(0);
      REQUIRE(root);
      auto c = root.snapshot_cursor();
      REQUIRE(c.count_keys() == 200);
   }

   std::filesystem::remove_all(dir);
}

// Exercise collapse paths with inner_prefix_node subtrees.
// Inserts keys with long shared prefixes to create inner_prefix nodes,
// then removes keys to trigger collapse/promotion.
TEST_CASE("collapse with inner_prefix subtree", "[coverage][collapse]")
{
   test_db tdb("collapse_ipn_testdb");
   auto    cur = start_temp_edit(tdb);

   // Group A: 18 keys with long shared prefix → inner_prefix subtree
   std::vector<std::string> group_a;
   for (int i = 0; i < 18; ++i)
   {
      std::string k;
      k += '\x01';
      k += std::string(180, 'X');
      char buf[8];
      snprintf(buf, sizeof(buf), "%03d", i);
      k += buf;
      group_a.push_back(k);
      cur.upsert(to_key_view(k), to_value_view(gval(i, 40)));
   }

   // Group B: keys under a different byte to create second branch
   std::vector<std::string> group_b;
   for (int i = 0; i < 4; ++i)
   {
      std::string k;
      k += '\x40';
      char buf[8];
      snprintf(buf, sizeof(buf), "%03d", i);
      k += buf;
      group_b.push_back(k);
      cur.upsert(to_key_view(k), to_value_view(gval(i + 100, 40)));
   }

   REQUIRE(cur.count_keys() == 22);

   // Remove all B keys one at a time → triggers merge/promotion paths
   // at the top inner_node, exercising collapse with the inner_prefix subtree
   for (auto& k : group_b)
      cur.remove(to_key_view(k));

   REQUIRE(cur.count_keys() == 18);

   // Verify all A keys survived
   for (auto& k : group_a)
   {
      std::string val;
      REQUIRE(cur.get(to_key_view(k), &val) >= 0);
   }
}
