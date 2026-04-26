#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>

using namespace psitri;

namespace
{
   struct test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "tree_handle_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = database::open(dir);
         ses = db->start_write_session();
      }

      ~test_db() { std::filesystem::remove_all(dir); }

      void wait_for_compactor(int max_ms = 5000)
      {
         if (!db->wait_for_compactor(std::chrono::milliseconds(max_ms)))
         {
            WARN("compactor did not drain within " << max_ms
                 << "ms, pending=" << ses->get_pending_release_count());
         }
      }

      void assert_no_leaks(const std::string&            context = "",
                           std::initializer_list<uint32_t> roots   = {0})
      {
         for (auto ri : roots)
            ses->set_root(ri, {}, sal::sync_type::none);
         wait_for_compactor();
         uint64_t allocated = ses->get_total_allocated_objects();
         INFO(context << " allocated=" << allocated << " (expected 0)");
         REQUIRE(allocated == 0);
      }

      void verify_key(uint32_t           root_index,
                      const std::string& key,
                      const std::string& expected)
      {
         auto root = ses->get_root(root_index);
         REQUIRE(root);
         auto   c = root.snapshot_cursor();
         auto   v = c.get<std::string>(key);
         REQUIRE(v.has_value());
         CHECK(*v == expected);
      }
   };
}  // namespace

// ════════════════════════════════════════════════════════════════════
// tree_handle::primary() — basic mutation and read via handle
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: primary handle basic CRUD", "[tree_handle]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   auto h  = tx.primary();

   h.upsert("key_a", "val_a");
   h.upsert("key_b", "val_b");
   h.upsert("key_c", "val_c");

   SECTION("get returns inserted values")
   {
      auto v = h.get<std::string>("key_a");
      REQUIRE(v.has_value());
      CHECK(*v == "val_a");
   }

   SECTION("update modifies existing key")
   {
      h.update("key_b", "val_b_updated");
      auto v = h.get<std::string>("key_b");
      REQUIRE(v.has_value());
      CHECK(*v == "val_b_updated");
   }

   SECTION("remove deletes key")
   {
      int removed = h.remove("key_c");
      CHECK(removed >= 0);
      auto v = h.get<std::string>("key_c");
      CHECK_FALSE(v.has_value());
   }

   SECTION("snapshot_cursor iterates all keys")
   {
      auto c = h.snapshot_cursor();
      c.seek_begin();
      std::vector<std::string> keys;
      while (!c.is_end())
      {
         keys.push_back(std::string(c.key()));
         c.next();
      }
      REQUIRE(keys.size() == 3);
      CHECK(keys[0] == "key_a");
      CHECK(keys[1] == "key_b");
      CHECK(keys[2] == "key_c");
   }

   SECTION("lower_bound positions correctly")
   {
      auto c = h.lower_bound("key_b");
      REQUIRE(!c.is_end());
      CHECK(c.key() == "key_b");
   }

   SECTION("upper_bound positions past exact match")
   {
      auto c = h.upper_bound("key_b");
      REQUIRE(!c.is_end());
      CHECK(c.key() == "key_c");
   }

   tx.commit();
}

// ════════════════════════════════════════════════════════════════════
// tree_handle equivalence with backward-compat API
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: primary() matches backward-compat API", "[tree_handle]")
{
   test_db t;

   // Write via backward-compat API
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("k1", "v1");
      tx.upsert("k2", "v2");
      tx.commit();
   }

   // Read via tree_handle
   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();

      auto v1 = h.get<std::string>("k1");
      REQUIRE(v1.has_value());
      CHECK(*v1 == "v1");

      auto v2 = h.get<std::string>("k2");
      REQUIRE(v2.has_value());
      CHECK(*v2 == "v2");

      // Modify via handle, verify via backward-compat
      h.upsert("k3", "v3");
      auto v3 = tx.get<std::string>("k3");
      REQUIRE(v3.has_value());
      CHECK(*v3 == "v3");

      tx.commit();
   }

   // Verify persisted
   auto rs  = t.db->start_read_session();
   auto cur = rs->snapshot_cursor(0);
   cur.seek("k3");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "v3");
}

// ════════════════════════════════════════════════════════════════════
// tree_handle with micro mode (buffered writes)
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: works with micro mode", "[tree_handle]")
{
   test_db t;

   // Seed data
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.commit();
   }

   auto tx = t.ses->start_transaction(0, tx_mode::expect_failure);
   auto h  = tx.primary();

   h.upsert("b", "updated");
   h.upsert("c", "3");

   auto v = h.get<std::string>("b");
   REQUIRE(v.has_value());
   CHECK(*v == "updated");

   v = h.get<std::string>("c");
   REQUIRE(v.has_value());
   CHECK(*v == "3");

   int removed = h.remove("a");
   CHECK(removed >= 0);

   v = h.get<std::string>("a");
   CHECK_FALSE(v.has_value());

   tx.commit();

   // Verify committed
   auto rs  = t.db->start_read_session();
   auto cur = rs->snapshot_cursor(0);
   cur.seek("a");
   CHECK((cur.is_end() || cur.key() != "a"));
   cur.seek("b");
   REQUIRE(!cur.is_end());
   CHECK(cur.value<std::string>().value_or("") == "updated");
}

// ════════════════════════════════════════════════════════════════════
// tree_handle with OCC mode
// ════════════════════════════════════════════════════════════════════
// tree_handle::open_subtree — open existing subtree via handle
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: open_subtree reads and modifies subtree", "[tree_handle][subtree]")
{
   test_db t;

   const int N = 50;

   // Create tree with a subtree
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("data", "root_data");

      auto sub_tx = t.ses->start_write_transaction(t.ses->create_temporary_tree());
      for (int i = 0; i < N; ++i)
      {
         char key[32], val[32];
         snprintf(key, sizeof(key), "sub_%03d", i);
         snprintf(val, sizeof(val), "val_%03d", i);
         sub_tx.upsert(key_view(key, strlen(key)), value_view(val, strlen(val)));
      }
      tx.upsert_subtree("child", sub_tx.get_tree());
      tx.commit();
   }

   // Open subtree via tree_handle, modify it, commit
   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();

      REQUIRE(h.is_subtree("child"));

      auto sub = h.open_subtree("child");

      // Read existing data through handle
      auto v = sub.get<std::string>("sub_000");
      REQUIRE(v.has_value());
      CHECK(*v == "val_000");

      // Count keys
      auto rc = sub.snapshot_cursor();
      rc.seek_begin();
      int count = 0;
      while (!rc.is_end())
      {
         ++count;
         rc.next();
      }
      CHECK(count == N);

      // Modify subtree via handle
      sub.upsert("sub_new", "added_value");
      sub.remove("sub_000");

      tx.commit();
   }

   // Verify changes persisted
   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();
      auto sub = h.open_subtree("child");

      auto v = sub.get<std::string>("sub_new");
      REQUIRE(v.has_value());
      CHECK(*v == "added_value");

      CHECK_FALSE(sub.get<std::string>("sub_000").has_value());

      auto rc = sub.snapshot_cursor();
      rc.seek_begin();
      int count = 0;
      while (!rc.is_end())
      {
         ++count;
         rc.next();
      }
      CHECK(count == N);  // removed 1, added 1

      tx.abort();
   }

   t.assert_no_leaks("after open_subtree modify");
}

// ════════════════════════════════════════════════════════════════════
// tree_handle::create_subtree — create new empty subtree
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: create_subtree creates and populates new subtree", "[tree_handle][subtree]")
{
   test_db t;

   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();

      h.upsert("top_key", "top_val");

      auto sub = h.create_subtree("new_tree");
      sub.upsert("inner_a", "val_a");
      sub.upsert("inner_b", "val_b");
      sub.upsert("inner_c", "val_c");

      // Verify reads within subtree handle
      auto v = sub.get<std::string>("inner_b");
      REQUIRE(v.has_value());
      CHECK(*v == "val_b");

      tx.commit();
   }

   // Verify subtree persisted
   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();

      REQUIRE(h.is_subtree("new_tree"));

      auto sub = h.open_subtree("new_tree");
      auto v   = sub.get<std::string>("inner_a");
      REQUIRE(v.has_value());
      CHECK(*v == "val_a");

      auto rc = sub.snapshot_cursor();
      rc.seek_begin();
      int count = 0;
      while (!rc.is_end())
      {
         ++count;
         rc.next();
      }
      CHECK(count == 3);

      tx.abort();
   }

   t.assert_no_leaks("after create_subtree");
}

// ════════════════════════════════════════════════════════════════════
// Nested subtrees via tree_handle
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: nested subtrees (create within open)", "[tree_handle][subtree][nested]")
{
   test_db t;

   // Create outer subtree as a detached temporary tree.
   {
      auto tx = t.ses->start_transaction(0);
      auto sub_tx = t.ses->start_write_transaction(t.ses->create_temporary_tree());
      sub_tx.upsert("outer_data", "hello");
      tx.upsert_subtree("outer", sub_tx.get_tree());
      tx.commit();
   }

   // Open outer, create inner via handles
   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();

      auto outer = h.open_subtree("outer");

      // Verify outer data
      auto v = outer.get<std::string>("outer_data");
      REQUIRE(v.has_value());
      CHECK(*v == "hello");

      // Create inner subtree within outer
      auto inner = outer.create_subtree("inner_tree");
      inner.upsert("deep_key", "deep_val");

      tx.commit();
   }

   // Verify nested structure persisted
   {
      auto root = t.ses->get_root(0);
      auto outer = root.get_subtree("outer");
      REQUIRE(outer);

      auto inner = outer.get_subtree("inner_tree");
      REQUIRE(inner);

      auto v = inner.get<std::string>("deep_key");
      REQUIRE(v.has_value());
      CHECK(*v == "deep_val");
   }

   t.assert_no_leaks("after nested subtree create");
}

// ════════════════════════════════════════════════════════════════════
// Opening the same subtree twice returns the same change_set
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: open_subtree deduplicates change_sets", "[tree_handle][subtree]")
{
   test_db t;

   // Create tree with subtree
   {
      auto tx = t.ses->start_transaction(0);
      auto sub_tx = t.ses->start_write_transaction(t.ses->create_temporary_tree());
      sub_tx.upsert("a", "1");
      sub_tx.upsert("b", "2");
      tx.upsert_subtree("child", sub_tx.get_tree());
      tx.commit();
   }

   {
      auto tx  = t.ses->start_transaction(0);
      auto h   = tx.primary();

      auto sub1 = h.open_subtree("child");
      sub1.upsert("a", "modified");

      auto sub2 = h.open_subtree("child");

      // sub2 should see sub1's write (same change_set)
      auto v = sub2.get<std::string>("a");
      REQUIRE(v.has_value());
      CHECK(*v == "modified");

      tx.commit();
   }
}

// ════════════════════════════════════════════════════════════════════
// tree_handle::get_stats
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: get_stats returns valid stats", "[tree_handle]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   auto h  = tx.primary();

   for (int i = 0; i < 100; ++i)
   {
      char key[32], val[32];
      snprintf(key, sizeof(key), "key_%04d", i);
      snprintf(val, sizeof(val), "val_%04d", i);
      h.upsert(key_view(key, strlen(key)), value_view(val, strlen(val)));
   }

   auto stats = h.get_stats();
   CHECK(stats.total_keys == 100);

   tx.abort();
}

// ════════════════════════════════════════════════════════════════════
// transaction_frame_ref subtree forwarding
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: transaction_frame_ref open/create_subtree", "[tree_handle][subtree]")
{
   test_db t;

   // Create tree with a subtree
   {
      auto tx = t.ses->start_transaction(0);
      auto sub_tx = t.ses->start_write_transaction(t.ses->create_temporary_tree());
      sub_tx.upsert("data", "value");
      tx.upsert_subtree("existing_sub", sub_tx.get_tree());
      tx.commit();
   }

   // Use open_subtree via frame_ref
   {
      auto tx   = t.ses->start_transaction(0);
      auto save = tx.sub_transaction();

      auto sub = save.open_subtree("existing_sub");
      sub.upsert("added", "via_frame");

      auto new_sub = save.create_subtree("brand_new");
      new_sub.upsert("fresh", "value");

      save.commit();
      tx.commit();
   }

   // Verify
   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();

      auto sub = h.open_subtree("existing_sub");
      auto v   = sub.get<std::string>("added");
      REQUIRE(v.has_value());
      CHECK(*v == "via_frame");

      REQUIRE(h.is_subtree("brand_new"));
      auto ns = h.open_subtree("brand_new");
      v       = ns.get<std::string>("fresh");
      REQUIRE(v.has_value());
      CHECK(*v == "value");

      tx.abort();
   }

   t.assert_no_leaks("after frame_ref subtree ops");
}

// ════════════════════════════════════════════════════════════════════
// tree_handle: remove_range works in batch mode
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: remove_range works in batch mode", "[tree_handle]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   auto h  = tx.primary();

   h.upsert("a", "1");
   h.upsert("b", "2");
   h.upsert("c", "3");
   h.upsert("d", "4");

   uint64_t removed = h.remove_range("b", "d");
   CHECK(removed == 2);  // removes b, c

   auto rc = h.snapshot_cursor();
   rc.seek_begin();
   std::vector<std::string> keys;
   while (!rc.is_end())
   {
      keys.push_back(std::string(rc.key()));
      rc.next();
   }
   REQUIRE(keys.size() == 2);
   CHECK(keys[0] == "a");
   CHECK(keys[1] == "d");

   tx.commit();
}

// ════════════════════════════════════════════════════════════════════
// tree_handle: insert and upsert_sorted
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: insert and upsert_sorted", "[tree_handle]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   auto h  = tx.primary();

   h.insert("b", "2");
   h.insert("a", "1");
   h.upsert_sorted("c", "3");  // must come after 'b' in sort order

   auto v = h.get<std::string>("a");
   REQUIRE(v.has_value());
   CHECK(*v == "1");

   v = h.get<std::string>("c");
   REQUIRE(v.has_value());
   CHECK(*v == "3");

   tx.commit();
}

// ════════════════════════════════════════════════════════════════════
// tree_handle: get with buffer output
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: get with Buffer output", "[tree_handle]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   auto h  = tx.primary();

   h.upsert("key", "hello_world");

   std::string buf;
   int32_t     sz = h.get("key", &buf);
   REQUIRE(sz == 11);
   CHECK(buf == "hello_world");

   sz = h.get("missing", &buf);
   CHECK(sz == cursor::value_not_found);

   tx.abort();
}

// ════════════════════════════════════════════════════════════════════
// tree_handle: lower_bound and upper_bound at boundaries
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: lower/upper bound edge cases", "[tree_handle]")
{
   test_db t;

   auto tx = t.ses->start_transaction(0);
   auto h  = tx.primary();

   h.upsert("apple", "1");
   h.upsert("banana", "2");
   h.upsert("cherry", "3");

   SECTION("lower_bound before first key")
   {
      auto c = h.lower_bound("aaa");
      REQUIRE(!c.is_end());
      CHECK(c.key() == "apple");
   }

   SECTION("lower_bound after last key")
   {
      auto c = h.lower_bound("zzz");
      CHECK(c.is_end());
   }

   SECTION("upper_bound on last key")
   {
      auto c = h.upper_bound("cherry");
      CHECK(c.is_end());
   }

   SECTION("upper_bound between keys")
   {
      auto c = h.upper_bound("b");
      REQUIRE(!c.is_end());
      CHECK(c.key() == "banana");
   }

   tx.abort();
}

// ════════════════════════════════════════════════════════════════════
// tree_handle: subtree write-back on commit with multiple subtrees
// ════════════════════════════════════════════════════════════════════

TEST_CASE("tree_handle: multiple subtree write-back on commit", "[tree_handle][subtree]")
{
   test_db t;

   // Create tree with two subtrees
   {
      auto tx = t.ses->start_transaction(0);
      auto sub_tx1 = t.ses->start_write_transaction(t.ses->create_temporary_tree());
      sub_tx1.upsert("x", "10");
      tx.upsert_subtree("sub_1", sub_tx1.get_tree());

      auto sub_tx2 = t.ses->start_write_transaction(t.ses->create_temporary_tree());
      sub_tx2.upsert("y", "20");
      tx.upsert_subtree("sub_2", sub_tx2.get_tree());

      tx.commit();
   }

   // Modify both subtrees via handles
   {
      auto tx = t.ses->start_transaction(0);
      auto h  = tx.primary();

      auto s1 = h.open_subtree("sub_1");
      s1.upsert("x", "11");
      s1.upsert("x2", "12");

      auto s2 = h.open_subtree("sub_2");
      s2.upsert("y", "21");

      tx.commit();
   }

   // Verify both subtrees were written back
   {
      auto root = t.ses->get_root(0);
      auto sub1 = root.get_subtree("sub_1");
      REQUIRE(sub1);
      auto v = sub1.get<std::string>("x");
      REQUIRE(v.has_value());
      CHECK(*v == "11");
      v = sub1.get<std::string>("x2");
      REQUIRE(v.has_value());
      CHECK(*v == "12");

      auto sub2 = root.get_subtree("sub_2");
      REQUIRE(sub2);
      v = sub2.get<std::string>("y");
      REQUIRE(v.has_value());
      CHECK(*v == "21");
   }

   t.assert_no_leaks("after multiple subtree write-back");
}

// ════════════════════════════════════════════════════════════════════
// Multi-root: open_root basic CRUD
// ════════════════════════════════════════════════════════════════════

TEST_CASE("multi-root: open_root basic CRUD", "[tree_handle][multi_root]")
{
   test_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("primary_key", "primary_val");

      auto root5 = tx.open_root(5);
      root5.upsert("r5_a", "val_a");
      root5.upsert("r5_b", "val_b");

      tx.commit();
   }

   t.verify_key(0, "primary_key", "primary_val");
   t.verify_key(5, "r5_a", "val_a");
   t.verify_key(5, "r5_b", "val_b");
}

// ════════════════════════════════════════════════════════════════════
// Multi-root: multiple roots in ascending order
// ════════════════════════════════════════════════════════════════════

TEST_CASE("multi-root: multiple roots in ascending order", "[tree_handle][multi_root]")
{
   test_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("r0", "zero");

      auto r3 = tx.open_root(3);
      r3.upsert("r3", "three");

      auto r7 = tx.open_root(7);
      r7.upsert("r7", "seven");

      auto r10 = tx.open_root(10);
      r10.upsert("r10", "ten");

      tx.commit();
   }

   t.verify_key(0, "r0", "zero");
   t.verify_key(3, "r3", "three");
   t.verify_key(7, "r7", "seven");
   t.verify_key(10, "r10", "ten");
}

// ════════════════════════════════════════════════════════════════════
// Multi-root: abort releases all locks
// ════════════════════════════════════════════════════════════════════

TEST_CASE("multi-root: abort releases all held locks", "[tree_handle][multi_root]")
{
   test_db t;

   // First transaction: populate roots
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("original", "data");
      auto r5 = tx.open_root(5);
      r5.upsert("r5_orig", "data");
      tx.commit();
   }

   // Second transaction: modify then abort
   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("should_not_persist", "nope");
      auto r5 = tx.open_root(5);
      r5.upsert("r5_should_not_persist", "nope");
      tx.abort();
   }

   // Third transaction: should be able to lock same roots (no deadlock)
   {
      auto tx = t.ses->start_transaction(0);
      auto v  = tx.get<std::string>("original");
      REQUIRE(v.has_value());
      CHECK(*v == "data");

      // Verify aborted writes did not persist
      auto v2 = tx.get<std::string>("should_not_persist");
      CHECK(!v2.has_value());

      auto r5 = tx.open_root(5);
      auto v3 = r5.get<std::string>("r5_orig");
      REQUIRE(v3.has_value());
      CHECK(*v3 == "data");

      auto v4 = r5.get<std::string>("r5_should_not_persist");
      CHECK(!v4.has_value());

      tx.abort();
   }
}

// ════════════════════════════════════════════════════════════════════
// Multi-root: destructor abort releases locks
// ════════════════════════════════════════════════════════════════════

TEST_CASE("multi-root: destructor abort releases locks", "[tree_handle][multi_root]")
{
   test_db t;

   // Let transaction go out of scope without commit/abort
   {
      auto tx = t.ses->start_transaction(0);
      auto r5 = tx.open_root(5);
      r5.upsert("temp", "temp");
      // ~transaction calls abort()
   }

   // Should be able to reacquire locks
   {
      auto tx = t.ses->start_transaction(0);
      auto r5 = tx.open_root(5);
      auto v  = r5.get<std::string>("temp");
      CHECK(!v.has_value());
      tx.abort();
   }
}

// ════════════════════════════════════════════════════════════════════
// Multi-root: subtrees within additional roots
// ════════════════════════════════════════════════════════════════════

TEST_CASE("multi-root: subtrees within additional roots", "[tree_handle][multi_root][subtree]")
{
   test_db t;

   {
      auto tx = t.ses->start_transaction(0);
      tx.upsert("primary", "data");

      auto r5 = tx.open_root(5);
      r5.upsert("top_level", "r5_data");

      auto sub = r5.create_subtree("nested");
      sub.upsert("inner_key", "inner_val");

      tx.commit();
   }

   t.verify_key(5, "top_level", "r5_data");

   // Verify subtree within root 5
   auto root = t.ses->get_root(5);
   auto nested = root.get_subtree("nested");
   REQUIRE(nested);
   auto v = nested.get<std::string>("inner_key");
   REQUIRE(v.has_value());
   CHECK(*v == "inner_val");
}

// ════════════════════════════════════════════════════════════════════
// Multi-root: open_root on empty root creates usable tree
// ════════════════════════════════════════════════════════════════════

TEST_CASE("multi-root: open_root on empty root", "[tree_handle][multi_root]")
{
   test_db t;

   // Root 42 has never been written to
   {
      auto tx  = t.ses->start_transaction(0);
      auto r42 = tx.open_root(42);
      r42.upsert("first_key", "first_val");
      tx.commit();
   }

   t.verify_key(42, "first_key", "first_val");
}

// ════════════════════════════════════════════════════════════════════
// Multi-root: read via tree_handle on additional root
// ════════════════════════════════════════════════════════════════════

TEST_CASE("multi-root: read operations via tree_handle", "[tree_handle][multi_root]")
{
   test_db t;

   // Populate root 3
   {
      auto tx = t.ses->start_transaction(3);
      tx.upsert("apple", "1");
      tx.upsert("banana", "2");
      tx.upsert("cherry", "3");
      tx.commit();
   }

   // Read through multi-root handle
   {
      auto tx = t.ses->start_transaction(0);
      auto r3 = tx.open_root(3);

      auto v = r3.get<std::string>("banana");
      REQUIRE(v.has_value());
      CHECK(*v == "2");

      auto lb = r3.lower_bound("b");
      REQUIRE(!lb.is_end());
      CHECK(lb.key() == "banana");

      auto ub = r3.upper_bound("banana");
      REQUIRE(!ub.is_end());
      CHECK(ub.key() == "cherry");

      auto rc = r3.snapshot_cursor();
      rc.seek_begin();
      int count = 0;
      while (!rc.is_end())
      {
         ++count;
         rc.next();
      }
      CHECK(count == 3);

      tx.abort();
   }
}
