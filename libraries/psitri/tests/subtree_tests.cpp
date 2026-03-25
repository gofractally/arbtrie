#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>
#include <random>

using namespace psitri;

constexpr int SCALE = 1;

namespace
{
   struct test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "subtree_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = std::make_shared<database>(dir, runtime_config());
         ses = db->start_write_session();
      }

      ~test_db() { std::filesystem::remove_all(dir); }

      void validate_unique_refs(uint32_t root_index = 0)
      {
         auto root = ses->get_root(root_index);
         if (!root)
            return;
         tree_context ctx(root);
         auto         ref = root.session()->get_ref(root.address());
         ctx.validate_unique_refs(ref);
      }

      void wait_for_compactor(int max_ms = 5000)
      {
         if (!db->wait_for_compactor(std::chrono::milliseconds(max_ms)))
         {
            WARN("compactor did not drain within " << max_ms
                 << "ms, pending=" << ses->get_pending_release_count());
         }
      }

      /// Release all roots, wait for compactor, assert zero allocated objects
      void assert_no_leaks(const std::string& context = "")
      {
         ses->set_root(0, {}, sal::sync_type::none);
         wait_for_compactor();
         uint64_t allocated = ses->get_total_allocated_objects();
         INFO(context << " allocated=" << allocated << " (expected 0)");
         REQUIRE(allocated == 0);
      }
   };

   /// Create a multi-level tree with N keys that will span inner nodes.
   /// Uses randomized key patterns to ensure inner_prefix_node and multi-level structure.
   sal::smart_ptr<sal::alloc_header> make_subtree(write_session& ses, int n,
                                                   const std::string& prefix = "sub")
   {
      auto cur = ses.create_write_cursor();
      for (int i = 0; i < n; ++i)
      {
         // Use zero-padded keys to get good distribution across branches
         char key_buf[64];
         snprintf(key_buf, sizeof(key_buf), "%s%06d", prefix.c_str(), i);
         char val_buf[80];
         snprintf(val_buf, sizeof(val_buf), "value_for_%s%06d", prefix.c_str(), i);
         cur->upsert(key_view(key_buf, strlen(key_buf)), value_view(val_buf, strlen(val_buf)));
      }
      return cur->take_root();
   }

   /// Create a subtree with large values (>64 bytes, forces value_node allocation)
   sal::smart_ptr<sal::alloc_header> make_subtree_large_values(
       write_session& ses, int n, const std::string& prefix = "big")
   {
      auto cur = ses.create_write_cursor();
      // Large value > 64 bytes to force value_node creation
      std::string large_val(200, 'X');
      for (int i = 0; i < n; ++i)
      {
         char key_buf[64];
         snprintf(key_buf, sizeof(key_buf), "%s%06d", prefix.c_str(), i);
         large_val[0] = 'A' + (i % 26);
         cur->upsert(key_view(key_buf, strlen(key_buf)), to_value_view(large_val));
      }
      return cur->take_root();
   }

   /// Count keys via cursor iteration (ground truth)
   uint64_t count_via_cursor(const sal::smart_ptr<sal::alloc_header>& root)
   {
      if (!root) return 0;
      cursor c(root);
      uint64_t count = 0;
      c.seek_begin();
      while (!c.is_end())
      {
         ++count;
         c.next();
      }
      return count;
   }
}  // namespace

// ============================================================
// Basic subtree write/read via write_cursor
// ============================================================

TEST_CASE("subtree: store and retrieve via write_cursor", "[subtree][write-cursor]")
{
   test_db t;

   // 500/SCALE keys — enough for multi-level tree (inner nodes + prefix nodes)
   const int N = 500 / SCALE;
   auto subtree_root = make_subtree(*t.ses, N);
   REQUIRE(subtree_root);

   auto cur = t.ses->create_write_cursor();
   cur->upsert(to_key("parent_key"), to_value("regular_value"));

   // Store subtree — ownership transfers from subtree_root to the tree
   cur->upsert(to_key("child_tree"), std::move(subtree_root));
   REQUIRE_FALSE(subtree_root);  // smart_ptr should be null after move

   SECTION("get returns value_subtree for subtree keys")
   {
      std::string buf;
      auto        rc = cur->read_cursor();
      int32_t     sz = rc.get(to_key("child_tree"), &buf);
      REQUIRE(sz == cursor::value_subtree);
   }

   SECTION("get returns normal value for non-subtree keys")
   {
      auto val = cur->get<std::string>(to_key("parent_key"));
      REQUIRE(val.has_value());
      REQUIRE(*val == "regular_value");
   }

   SECTION("is_subtree correctly identifies subtree keys")
   {
      REQUIRE(cur->is_subtree(to_key("child_tree")));
      REQUIRE_FALSE(cur->is_subtree(to_key("parent_key")));
      REQUIRE_FALSE(cur->is_subtree(to_key("nonexistent")));
   }

   SECTION("get_subtree returns usable smart_ptr with all keys accessible")
   {
      auto sub = cur->get_subtree(to_key("child_tree"));
      REQUIRE(sub);

      // Verify count matches
      cursor c(sub);
      REQUIRE(c.count_keys() == (uint64_t)N);

      // Spot-check a few keys
      char key_buf[64];
      snprintf(key_buf, sizeof(key_buf), "sub%06d", 0);
      REQUIRE(c.get<std::string>(key_view(key_buf, strlen(key_buf))).has_value());
      snprintf(key_buf, sizeof(key_buf), "sub%06d", N - 1);
      REQUIRE(c.get<std::string>(key_view(key_buf, strlen(key_buf))).has_value());
   }

   SECTION("get_subtree_cursor returns working write_cursor")
   {
      auto sub_cur = cur->get_subtree_cursor(to_key("child_tree"));
      REQUIRE(static_cast<bool>(sub_cur));

      // Read existing data
      char key_buf[64];
      snprintf(key_buf, sizeof(key_buf), "sub%06d", N / 2);
      auto val = sub_cur.get<std::string>(key_view(key_buf, strlen(key_buf)));
      REQUIRE(val.has_value());

      // Modify the subtree (COW — doesn't affect parent until stored back)
      sub_cur.upsert(to_key("new_key"), to_value("new_val"));
      auto nv = sub_cur.get<std::string>(to_key("new_key"));
      REQUIRE(nv.has_value());
      REQUIRE(*nv == "new_val");
   }

   SECTION("get_subtree for non-subtree key returns null")
   {
      auto sub = cur->get_subtree(to_key("parent_key"));
      REQUIRE_FALSE(sub);

      auto sub2 = cur->get_subtree(to_key("nonexistent"));
      REQUIRE_FALSE(sub2);
   }
}

// ============================================================
// Replace subtree with leak detection
// ============================================================

TEST_CASE("subtree: replace subtree value - no leaks", "[subtree][leak]")
{
   test_db t;

   const int N = 300 / SCALE;

   {
      auto txn = t.ses->start_transaction(0);

      // Store first subtree (multi-level)
      auto sub1 = make_subtree(*t.ses, N, "first");
      txn.upsert(to_key("tree"), std::move(sub1));

      // Verify first subtree
      {
         auto sub = txn.get_subtree(to_key("tree"));
         cursor c(sub);
         REQUIRE(c.count_keys() == (uint64_t)N);
      }

      // Replace with second subtree — old one should be released
      auto sub2 = make_subtree(*t.ses, N, "second");
      txn.upsert(to_key("tree"), std::move(sub2));

      // Verify second subtree replaced first
      {
         auto sub = txn.get_subtree(to_key("tree"));
         cursor c(sub);
         REQUIRE(c.count_keys() == (uint64_t)N);
         char key_buf[64];
         snprintf(key_buf, sizeof(key_buf), "second%06d", 0);
         REQUIRE(c.get<std::string>(key_view(key_buf, strlen(key_buf))).has_value());
         snprintf(key_buf, sizeof(key_buf), "first%06d", 0);
         REQUIRE_FALSE(c.get<std::string>(key_view(key_buf, strlen(key_buf))).has_value());
      }

      txn.commit();
   }

   // Release root and verify no leaks
   t.assert_no_leaks("after replacing subtree");
}

// ============================================================
// Delete key with subtree value - leak detection
// ============================================================

TEST_CASE("subtree: delete key with subtree value - no leaks", "[subtree][leak]")
{
   test_db t;

   const int N = 300 / SCALE;

   {
      auto txn = t.ses->start_transaction(0);
      txn.upsert(to_key("keep"), to_value("keeper"));

      auto sub = make_subtree(*t.ses, N);
      txn.upsert(to_key("tree"), std::move(sub));

      REQUIRE(txn.is_subtree(to_key("tree")));

      // Remove the subtree key — all subtree nodes should cascade-release
      int removed = txn.remove(to_key("tree"));
      REQUIRE(removed >= 0);

      // Key should be gone
      REQUIRE_FALSE(txn.is_subtree(to_key("tree")));

      // Other key still present
      auto val = txn.get<std::string>(to_key("keep"));
      REQUIRE(val.has_value());
      REQUIRE(*val == "keeper");

      txn.commit();
   }

   t.assert_no_leaks("after removing subtree key");
}

// ============================================================
// Replace data <-> subtree with leak detection
// ============================================================

TEST_CASE("subtree: replace data value with subtree and back - no leaks", "[subtree][leak]")
{
   test_db t;

   const int N = 200 / SCALE;

   {
      auto txn = t.ses->start_transaction(0);
      txn.upsert(to_key("key"), to_value("plain_data"));

      // Replace plain data with a multi-level subtree
      auto sub = make_subtree(*t.ses, N);
      txn.upsert(to_key("key"), std::move(sub));
      REQUIRE(txn.is_subtree(to_key("key")));

      // Replace subtree back with plain data — subtree nodes should release
      txn.upsert(to_key("key"), to_value("back_to_data"));
      REQUIRE_FALSE(txn.is_subtree(to_key("key")));
      auto val = txn.get<std::string>(to_key("key"));
      REQUIRE(*val == "back_to_data");

      txn.commit();
   }

   t.assert_no_leaks("after data->subtree->data replacement");
}

// ============================================================
// Subtree with large values (value_nodes) - leak detection
// ============================================================

TEST_CASE("subtree: large-value subtrees - no leaks", "[subtree][leak]")
{
   test_db t;

   const int N = 100 / SCALE;

   {
      auto txn = t.ses->start_transaction(0);

      // Create subtree with values > 64 bytes (forces value_node allocation)
      auto sub = make_subtree_large_values(*t.ses, N);
      txn.upsert(to_key("big_tree"), std::move(sub));

      // Verify subtree contents
      {
         auto root = txn.get_subtree(to_key("big_tree"));
         REQUIRE(root);
         cursor c(root);
         REQUIRE(c.count_keys() == (uint64_t)N);
      }

      // Replace with a different large-value subtree
      auto sub2 = make_subtree_large_values(*t.ses, N, "new");
      txn.upsert(to_key("big_tree"), std::move(sub2));

      // Remove the key entirely
      txn.remove(to_key("big_tree"));
      txn.commit();
   }

   t.assert_no_leaks("after large-value subtree remove");
}

// ============================================================
// Transaction abort discards subtree - leak detection
// ============================================================

TEST_CASE("subtree: transaction abort discards subtree - no leaks", "[subtree][leak]")
{
   test_db t;

   const int N = 200 / SCALE;

   // Abort a transaction that stores a subtree
   {
      auto sub = make_subtree(*t.ses, N);
      auto txn = t.ses->start_transaction(0);
      txn.upsert(to_key("tree"), std::move(sub));
      txn.abort();
   }

   t.wait_for_compactor();
   // Root should be empty (abort rolls back)
   auto root = t.ses->get_root(0);
   REQUIRE_FALSE(root);

   // Everything should be freed
   uint64_t allocated = t.ses->get_total_allocated_objects();
   INFO("allocated after abort=" << allocated << " (expected 0)");
   REQUIRE(allocated == 0);
}

// ============================================================
// Cursor subtree navigation
// ============================================================

TEST_CASE("subtree: cursor iteration with subtree values", "[subtree][cursor]")
{
   test_db t;

   const int N = 100 / SCALE;
   auto cur = t.ses->create_write_cursor();
   cur->upsert(to_key("a_data"), to_value("hello"));

   auto sub = make_subtree(*t.ses, N);
   cur->upsert(to_key("b_tree"), std::move(sub));

   cur->upsert(to_key("c_data"), to_value("world"));

   // Iterate and check types
   auto rc = cur->read_cursor();
   REQUIRE(rc.seek_begin());

   // First key: a_data (regular)
   REQUIRE(rc.key() == "a_data");
   REQUIRE_FALSE(rc.is_subtree());
   REQUIRE(rc.value_size() == 5);  // "hello"

   // Second key: b_tree (subtree)
   REQUIRE(rc.next());
   REQUIRE(rc.key() == "b_tree");
   REQUIRE(rc.is_subtree());
   REQUIRE(rc.value_size() == cursor::value_subtree);
   auto sub_ptr = rc.subtree();
   REQUIRE(sub_ptr);

   // Verify subtree contents via subtree_cursor
   auto sc = rc.subtree_cursor();
   REQUIRE(sc.seek_begin());

   // Third key: c_data (regular)
   REQUIRE(rc.next());
   REQUIRE(rc.key() == "c_data");
   REQUIRE_FALSE(rc.is_subtree());

   REQUIRE_FALSE(rc.next());  // end
}

// ============================================================
// Nested subtrees with leak detection
// ============================================================

TEST_CASE("subtree: nested subtrees (tree of trees) - no leaks", "[subtree][nested][leak]")
{
   test_db t;

   const int N = 100 / SCALE;

   {
      auto txn = t.ses->start_transaction(0);

      // Create inner subtree (multi-level)
      auto inner = make_subtree(*t.ses, N, "inner");

      // Create outer subtree containing the inner subtree
      auto outer_cur = t.ses->create_write_cursor();
      outer_cur->upsert(to_key("outer_data"), to_value("hello_outer"));
      outer_cur->upsert(to_key("nested"), std::move(inner));
      auto outer = outer_cur->take_root();

      // Store outer subtree in main tree
      txn.upsert(to_key("top_level"), to_value("root_data"));
      txn.upsert(to_key("outer_tree"), std::move(outer));

      // Navigate: main -> outer -> inner
      REQUIRE(txn.is_subtree(to_key("outer_tree")));

      auto outer_root = txn.get_subtree(to_key("outer_tree"));
      REQUIRE(outer_root);
      cursor oc(outer_root);
      REQUIRE(oc.seek(to_key("nested")));
      REQUIRE(oc.is_subtree());

      auto inner_root = oc.subtree();
      REQUIRE(inner_root);
      cursor ic(inner_root);
      REQUIRE(ic.count_keys() == (uint64_t)N);

      txn.commit();
   }

   // Release root — nested subtrees should cascade-destroy
   t.assert_no_leaks("after nested subtree release");
}

// ============================================================
// Multiple subtrees with leak detection
// ============================================================

TEST_CASE("subtree: multiple subtrees coexist - no leaks", "[subtree][leak]")
{
   test_db t;

   const int NUM_SUBTREES = 10;
   const int N            = 100 / SCALE;

   {
      auto txn = t.ses->start_transaction(0);

      // Store multiple multi-level subtrees
      for (int i = 0; i < NUM_SUBTREES; ++i)
      {
         char key_buf[32];
         snprintf(key_buf, sizeof(key_buf), "tree_%02d", i);
         char prefix[16];
         snprintf(prefix, sizeof(prefix), "t%02d_", i);
         auto sub = make_subtree(*t.ses, N, prefix);
         txn.upsert(key_view(key_buf, strlen(key_buf)), std::move(sub));
      }

      // Also store some regular keys
      txn.upsert(to_key("regular_a"), to_value("va"));
      txn.upsert(to_key("regular_b"), to_value("vb"));

      // Verify each subtree independently
      for (int i = 0; i < NUM_SUBTREES; ++i)
      {
         char key_buf[32];
         snprintf(key_buf, sizeof(key_buf), "tree_%02d", i);
         REQUIRE(txn.is_subtree(key_view(key_buf, strlen(key_buf))));

         auto sub = txn.get_subtree(key_view(key_buf, strlen(key_buf)));
         REQUIRE(sub);
         cursor c(sub);
         REQUIRE(c.count_keys() == (uint64_t)N);
      }

      txn.commit();
   }

   t.assert_no_leaks("after multiple subtrees release");
}

// ============================================================
// COW isolation: modifying subtree cursor doesn't affect parent
// ============================================================

TEST_CASE("subtree: COW isolation of get_subtree_cursor", "[subtree][cow]")
{
   test_db t;

   const int N = 200 / SCALE;
   auto sub = make_subtree(*t.ses, N);
   auto cur = t.ses->create_write_cursor();
   cur->upsert(to_key("tree"), std::move(sub));

   // Get a write cursor into the subtree
   auto sub_cur = cur->get_subtree_cursor(to_key("tree"));
   sub_cur.upsert(to_key("extra"), to_value("added"));

   // The parent tree's subtree should NOT see the new key (COW isolation)
   {
      auto sub2 = cur->get_subtree(to_key("tree"));
      cursor c(sub2);
      REQUIRE_FALSE(c.get<std::string>(to_key("extra")).has_value());
      REQUIRE(c.count_keys() == (uint64_t)N);  // original count
   }

   // But the sub_cur should see it
   REQUIRE(sub_cur.get<std::string>(to_key("extra")).has_value());
   REQUIRE(sub_cur.count_keys() == (uint64_t)(N + 1));

   // Store modified subtree back to parent
   cur->upsert(to_key("tree"), sub_cur.take_root());

   // Now parent should see the modification
   {
      auto sub3 = cur->get_subtree(to_key("tree"));
      cursor c(sub3);
      REQUIRE(c.get<std::string>(to_key("extra")).has_value());
      REQUIRE(c.count_keys() == (uint64_t)(N + 1));
   }
}

// ============================================================
// Shared-mode subtree operations (via transaction on persisted root)
// ============================================================

TEST_CASE("subtree: shared-mode replace via transaction - no leaks", "[subtree][shared][leak]")
{
   test_db t;

   const int N = 300 / SCALE;

   // First transaction: create tree with subtree
   {
      auto txn = t.ses->start_transaction(0);
      txn.upsert(to_key("data"), to_value("hello"));
      auto sub = make_subtree(*t.ses, N);
      txn.upsert(to_key("child"), std::move(sub));
      txn.commit();
   }

   // Second transaction: replace the subtree (shared mode — root has ref > 1)
   {
      auto txn = t.ses->start_transaction(0);

      // Verify original subtree
      REQUIRE(txn.is_subtree(to_key("child")));
      {
         auto sub = txn.get_subtree(to_key("child"));
         cursor c(sub);
         REQUIRE(c.count_keys() == (uint64_t)N);
      }

      // Replace with a new subtree
      auto new_sub = make_subtree(*t.ses, N / 2, "new");
      txn.upsert(to_key("child"), std::move(new_sub));

      // Verify replacement
      {
         auto sub = txn.get_subtree(to_key("child"));
         cursor c(sub);
         REQUIRE(c.count_keys() == (uint64_t)(N / 2));
      }

      txn.commit();
   }

   t.assert_no_leaks("after shared-mode subtree replace");
}

// ============================================================
// Shared-mode remove subtree key via transaction
// ============================================================

TEST_CASE("subtree: shared-mode remove subtree key - no leaks", "[subtree][shared][leak]")
{
   test_db t;

   const int N = 300 / SCALE;

   // First transaction: populate tree with subtree
   {
      auto txn = t.ses->start_transaction(0);
      for (int i = 0; i < 20; ++i)
      {
         char key_buf[32], val_buf[32];
         snprintf(key_buf, sizeof(key_buf), "data_%02d", i);
         snprintf(val_buf, sizeof(val_buf), "val_%02d", i);
         txn.upsert(key_view(key_buf, strlen(key_buf)), value_view(val_buf, strlen(val_buf)));
      }
      auto sub = make_subtree(*t.ses, N);
      txn.upsert(to_key("subtree_key"), std::move(sub));
      txn.commit();
   }

   // Second transaction: remove the subtree key (shared-mode COW)
   {
      auto txn = t.ses->start_transaction(0);
      int removed = txn.remove(to_key("subtree_key"));
      REQUIRE(removed >= 0);
      REQUIRE_FALSE(txn.is_subtree(to_key("subtree_key")));

      // Other keys still intact
      auto val = txn.get<std::string>(to_key("data_00"));
      REQUIRE(val.has_value());

      txn.commit();
   }

   t.assert_no_leaks("after shared-mode subtree remove");
}

// ============================================================
// Persist, reload, modify subtree
// ============================================================

TEST_CASE("subtree: persist and reload with modification", "[subtree][persistence]")
{
   test_db t;

   const int N = 200 / SCALE;

   // Store subtree via transaction
   {
      auto sub = make_subtree(*t.ses, N);
      auto txn = t.ses->start_transaction(0);
      txn.upsert(to_key("persistent_tree"), std::move(sub));
      txn.commit();
   }

   // Read it back from persisted root and verify all keys
   {
      auto root = t.ses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      REQUIRE(c.seek(to_key("persistent_tree")));
      REQUIRE(c.is_subtree());

      auto sub_root = c.subtree();
      REQUIRE(sub_root);
      REQUIRE(count_via_cursor(sub_root) == (uint64_t)N);
   }

   // Modify: get subtree, add keys, store back
   {
      auto txn = t.ses->start_transaction(0);
      auto sub_cur = txn.get_subtree_cursor(to_key("persistent_tree"));
      REQUIRE(static_cast<bool>(sub_cur));
      REQUIRE(sub_cur.count_keys() == (uint64_t)N);

      // Add more keys
      for (int i = N; i < N + 50 / SCALE; ++i)
      {
         char key_buf[64], val_buf[64];
         snprintf(key_buf, sizeof(key_buf), "sub%06d", i);
         snprintf(val_buf, sizeof(val_buf), "added_%d", i);
         sub_cur.upsert(key_view(key_buf, strlen(key_buf)), value_view(val_buf, strlen(val_buf)));
      }
      REQUIRE(sub_cur.count_keys() == (uint64_t)(N + 50 / SCALE));

      // Store modified subtree back
      txn.upsert(to_key("persistent_tree"), sub_cur.take_root());
      txn.commit();
   }

   // Verify modification persisted
   {
      auto root = t.ses->get_root(0);
      cursor c(root);
      REQUIRE(c.seek(to_key("persistent_tree")));
      auto sub_root = c.subtree();
      REQUIRE(count_via_cursor(sub_root) == (uint64_t)(N + 50 / SCALE));
   }

   t.assert_no_leaks("after subtree modification persist cycle");
}

// ============================================================
// Interleaved insert/replace/remove cycles - leak stress
// ============================================================

TEST_CASE("subtree: interleaved operations - no leaks", "[subtree][leak][stress]")
{
   test_db t;

   const int N         = 200 / SCALE;
   const int NUM_CYCLES = 5;

   for (int cycle = 0; cycle < NUM_CYCLES; ++cycle)
   {
      auto txn = t.ses->start_transaction(0);

      // Insert subtrees
      for (int i = 0; i < 3; ++i)
      {
         char key_buf[32], prefix[16];
         snprintf(key_buf, sizeof(key_buf), "tree_%d_%d", cycle, i);
         snprintf(prefix, sizeof(prefix), "c%d_t%d_", cycle, i);
         auto sub = make_subtree(*t.ses, N, prefix);
         txn.upsert(key_view(key_buf, strlen(key_buf)), std::move(sub));
      }

      // Replace one subtree
      {
         char key_buf[32], prefix[16];
         snprintf(key_buf, sizeof(key_buf), "tree_%d_0", cycle);
         snprintf(prefix, sizeof(prefix), "rep%d_", cycle);
         auto sub = make_subtree(*t.ses, N / 2, prefix);
         txn.upsert(key_view(key_buf, strlen(key_buf)), std::move(sub));
      }

      // Remove one subtree
      {
         char key_buf[32];
         snprintf(key_buf, sizeof(key_buf), "tree_%d_1", cycle);
         txn.remove(key_view(key_buf, strlen(key_buf)));
      }

      txn.commit();
   }

   t.assert_no_leaks("after interleaved subtree operations");
}

// ============================================================
// Subtree count via cursor
// ============================================================

TEST_CASE("subtree: count_keys on subtree", "[subtree][count]")
{
   test_db t;

   const int N = 500 / SCALE;
   auto      sub = make_subtree(*t.ses, N);

   auto cur = t.ses->create_write_cursor();
   cur->upsert(to_key("tree"), std::move(sub));

   auto sub_root = cur->get_subtree(to_key("tree"));
   cursor c(sub_root);
   REQUIRE(c.count_keys() == (uint64_t)N);
   REQUIRE(count_via_cursor(sub_root) == (uint64_t)N);
}

// ============================================================
// range_remove on tree containing subtrees
// ============================================================

TEST_CASE("subtree: range_remove over subtree keys - no leaks", "[subtree][range_remove][leak]")
{
   test_db t;

   const int N = 100 / SCALE;

   {
      auto txn = t.ses->start_transaction(0);

      // Insert mix of regular keys and subtree keys
      for (int i = 0; i < 10; ++i)
      {
         char key_buf[32];
         snprintf(key_buf, sizeof(key_buf), "key_%02d", i);
         if (i % 3 == 0)
         {
            // Subtree value
            char prefix[16];
            snprintf(prefix, sizeof(prefix), "s%d_", i);
            auto sub = make_subtree(*t.ses, N, prefix);
            txn.upsert(key_view(key_buf, strlen(key_buf)), std::move(sub));
         }
         else
         {
            // Regular value
            txn.upsert(key_view(key_buf, strlen(key_buf)), to_value("regular"));
         }
      }

      // Remove a range that includes subtree keys
      txn.remove_range(to_key("key_02"), to_key("key_08"));

      // Verify surviving keys
      auto rc = txn.read_cursor();
      rc.seek_begin();
      std::vector<std::string> remaining;
      while (!rc.is_end())
      {
         remaining.push_back(std::string(rc.key()));
         rc.next();
      }
      // Should have key_00, key_01, key_08, key_09
      REQUIRE(remaining.size() == 4);
      REQUIRE(remaining[0] == "key_00");
      REQUIRE(remaining[1] == "key_01");
      REQUIRE(remaining[2] == "key_08");
      REQUIRE(remaining[3] == "key_09");

      txn.commit();
   }

   t.assert_no_leaks("after range_remove over subtree keys");
}
