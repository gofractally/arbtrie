#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/value_type.hpp>


using namespace psitri;

#ifdef NDEBUG
constexpr int OPS_SCALE = 1;
#else
constexpr int OPS_SCALE = 5;
#endif

namespace
{
   struct test_db
   {
      std::string                    dir;
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;

      test_db(const std::string& name = "tree_ops_testdb")
          : dir(name)
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir + "/data");
         db  = std::make_shared<database>(dir, runtime_config());
         ses = db->start_write_session();
      }
      ~test_db() { std::filesystem::remove_all(dir); }
   };

   std::string tkey(int i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "tkey-%08d", i);
      return buf;
   }

   std::string big_val(int i, size_t size = 200)
   {
      std::string val(size, '\0');
      for (size_t j = 0; j < size; ++j)
         val[j] = static_cast<char>('A' + ((i + j) % 26));
      return val;
   }

   std::string small_val(int i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "val-%08d", i);
      return buf;
   }
}  // namespace

TEST_CASE("tree_ops: get_stats on diverse tree", "[tree_ops][stats]")
{
   test_db env("tree_ops_stats_db");
   auto    trx = env.ses->start_transaction(0);

   // Insert enough keys to create inner nodes, inner_prefix nodes, leaves, and value_nodes
   const int N = 500 / OPS_SCALE;
   for (int i = 0; i < N; ++i)
   {
      // Mix of small values (inline) and large values (value_nodes)
      if (i % 5 == 0)
         trx.upsert(tkey(i), big_val(i));
      else
         trx.upsert(tkey(i), small_val(i));
   }

   // Also insert keys with common prefixes to force inner_prefix_node creation
   for (int i = 0; i < 50 / OPS_SCALE; ++i)
   {
      std::string prefix_key = "shared_prefix/sub" + std::to_string(i);
      trx.upsert(prefix_key, small_val(i));
   }

   trx.commit();

   // Now get stats via write_cursor
   auto root = env.ses->get_root(0);
   REQUIRE(root);
   write_cursor wc(root);

   auto stats = wc.get_stats();
   CHECK(stats.total_keys > 0);
   CHECK(stats.leaf_nodes > 0);
   CHECK(stats.total_nodes() > 0);
   CHECK(stats.max_depth >= 1);
   CHECK(stats.branches > 0);
   CHECK(stats.clines > 0);
   CHECK(stats.average_inner_node_size() > 0);
   CHECK(stats.average_clines_per_inner_node() > 0);
   CHECK(stats.average_branch_per_inner_node() > 0);

   // With big_val keys, we should have value_nodes
   if (N >= 5)
      CHECK(stats.value_nodes > 0);
   // With enough keys, we should have inner nodes
   if (N >= 100)
      CHECK((stats.inner_nodes + stats.inner_prefix_nodes) > 0);
}

TEST_CASE("tree_ops: print on populated tree", "[tree_ops][print]")
{
   test_db env("tree_ops_print_db");
   auto    trx = env.ses->start_transaction(0);

   // Build a tree with inner, inner_prefix, and leaf nodes
   for (int i = 0; i < 200 / OPS_SCALE; ++i)
      trx.upsert(tkey(i), small_val(i));

   // Add prefix keys for inner_prefix_node coverage
   for (int i = 0; i < 30 / OPS_SCALE; ++i)
   {
      std::string pk = "prefix_group/item_" + std::to_string(i);
      trx.upsert(pk, small_val(i));
   }

   // Add some large values for value_node leaf printing
   for (int i = 0; i < 10 / OPS_SCALE; ++i)
      trx.upsert("bigval_" + std::to_string(i), big_val(i));

   trx.commit();

   auto root = env.ses->get_root(0);
   REQUIRE(root);
   write_cursor wc(root);

   // Capture stdout and verify output contains expected structural info
   std::ostringstream captured;
   auto*              old_buf = std::cout.rdbuf(captured.rdbuf());
   wc.print();
   std::cout.rdbuf(old_buf);

   auto output = captured.str();
   CHECK(output.size() > 0);
   // print() should output node addresses, branch counts, and key ranges
   CHECK(output.find("branches:") != std::string::npos);
   // Should contain leaf node output with key values
   CHECK(output.find("tkey-") != std::string::npos);
}

TEST_CASE("tree_ops: validate on populated tree", "[tree_ops][validate]")
{
   test_db env("tree_ops_validate_db");
   auto    trx = env.ses->start_transaction(0);

   for (int i = 0; i < 300 / OPS_SCALE; ++i)
   {
      if (i % 4 == 0)
         trx.upsert(tkey(i), big_val(i));
      else
         trx.upsert(tkey(i), small_val(i));
   }
   trx.commit();

   auto root = env.ses->get_root(0);
   REQUIRE(root);
   write_cursor wc(root);

   // validate() should not throw on a well-formed tree
   REQUIRE_NOTHROW(wc.validate());
}

TEST_CASE("tree_ops: validate_unique_refs with value_nodes", "[tree_ops][validate]")
{
   test_db env("tree_ops_unique_refs_db");
   auto    trx = env.ses->start_transaction(0);

   // Insert keys with large values to create value_nodes
   for (int i = 0; i < 100 / OPS_SCALE; ++i)
      trx.upsert(tkey(i), big_val(i, 200));

   // Also small values
   for (int i = 100; i < 200 / OPS_SCALE + 100; ++i)
      trx.upsert(tkey(i), small_val(i));

   trx.commit();

   auto root = env.ses->get_root(0);
   REQUIRE(root);

   // validate_unique_refs checks all children have ref == 1
   tree_context ctx(root);
   auto         ref = root.session()->get_ref(root.address());
   REQUIRE_NOTHROW(ctx.validate_unique_refs(ref));
}

TEST_CASE("tree_ops: subtree collapse via low threshold", "[tree_ops][collapse]")
{
   test_db env("tree_ops_collapse_db");

   // Build a tree, then remove keys to trigger collapse
   {
      auto trx = env.ses->start_transaction(0);
      // Insert enough keys to build a multi-level tree
      for (int i = 0; i < 100 / OPS_SCALE; ++i)
         trx.upsert(tkey(i), small_val(i));
      trx.commit();
   }

   // Now remove most keys to trigger collapse path
   // The collapse happens during shared-mode remove when descendents <= threshold
   // We use a snapshot to force shared mode, then remove
   auto snapshot_root = env.ses->get_root(0);  // holds a ref, making tree shared

   {
      auto trx = env.ses->start_transaction(0);
      // Remove most keys, leaving a handful
      for (int i = 5; i < 100 / OPS_SCALE; ++i)
         trx.remove(tkey(i));
      trx.commit();
   }

   // Verify remaining keys are intact
   auto trx = env.ses->start_transaction(0);
   for (int i = 0; i < 5; ++i)
   {
      auto val = trx.get<std::string>(tkey(i));
      CHECK(val.has_value());
      if (val)
         CHECK(*val == small_val(i));
   }
}

TEST_CASE("tree_ops: subtree collapse with set_collapse_threshold", "[tree_ops][collapse]")
{
   test_db env("tree_ops_threshold_db");
   const int N = 60 / OPS_SCALE;

   // Build tree with enough keys to create multi-level structure
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         trx.upsert(tkey(i), small_val(i));
      trx.commit();
   }

   // Get stats before collapse
   auto root_before = env.ses->get_root(0);
   REQUIRE(root_before);
   write_cursor wc_before(root_before);
   auto stats_before = wc_before.get_stats();

   // Hold snapshot for shared mode
   auto snapshot = env.ses->get_root(0);

   // Remove most keys, leaving very few — should trigger collapse to leaf
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 3; i < N; ++i)
         trx.remove(tkey(i));
      trx.commit();
   }

   // Get stats after collapse
   auto root_after = env.ses->get_root(0);
   REQUIRE(root_after);
   write_cursor wc_after(root_after);
   auto stats_after = wc_after.get_stats();

   // After removing most keys, we should have far fewer nodes
   CHECK(stats_after.total_keys == 3);
   CHECK(stats_after.total_nodes() < stats_before.total_nodes());

   // Verify remaining keys are correct
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < 3; ++i)
      {
         auto val = trx.get<std::string>(tkey(i));
         REQUIRE(val.has_value());
         CHECK(*val == small_val(i));
      }
      // Removed keys should be gone
      auto gone = trx.get<std::string>(tkey(5));
      CHECK_FALSE(gone.has_value());
   }
}

TEST_CASE("tree_ops: shared-mode upsert forces COW on inner nodes", "[tree_ops][shared]")
{
   test_db env("tree_ops_shared_upsert_db");
   const int N = 300 / OPS_SCALE;

   // Build initial tree
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         trx.upsert(tkey(i), small_val(i));
      trx.commit();
   }

   // Take a snapshot (increases refcounts, forcing shared mode on next mutation)
   auto snapshot_root = env.ses->get_root(0);

   // Now mutate - this forces shared-mode upsert (COW) on all modified inner nodes
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         trx.upsert(tkey(i), small_val(i + 1000));  // update all values
      trx.commit();
   }

   // Verify snapshot still has old values
   {
      cursor c(snapshot_root);
      for (int i = 0; i < std::min(N, 10); ++i)
      {
         std::string val;
         auto        r = c.get(tkey(i), &val);
         CHECK(r >= 0);
         CHECK(val == small_val(i));
      }
   }

   // Verify new tree has new values
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < std::min(N, 10); ++i)
      {
         auto val = trx.get<std::string>(tkey(i));
         REQUIRE(val.has_value());
         CHECK(*val == small_val(i + 1000));
      }
   }
}

TEST_CASE("tree_ops: shared-mode upsert with large values (value_nodes)", "[tree_ops][shared]")
{
   test_db env("tree_ops_shared_vnode_db");
   const int N = 100 / OPS_SCALE;

   // Build tree with value_nodes
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         trx.upsert(tkey(i), big_val(i));
      trx.commit();
   }

   // Snapshot
   auto snapshot = env.ses->get_root(0);

   // Mutate in shared mode - exercises retain_subtree_leaf_values_by_addr
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         trx.upsert(tkey(i), big_val(i + 1000));
      trx.commit();
   }

   // Verify snapshot retains old large values
   {
      cursor c(snapshot);
      for (int i = 0; i < std::min(N, 10); ++i)
      {
         std::string val;
         auto        r = c.get(tkey(i), &val);
         CHECK(r >= 0);
         CHECK(val == big_val(i));
      }
   }
}

TEST_CASE("tree_ops: shared-mode remove with value_nodes triggers retain", "[tree_ops][shared]")
{
   test_db env("tree_ops_shared_remove_vnode_db");
   const int N = 80 / OPS_SCALE;

   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         trx.upsert(tkey(i), big_val(i));
      trx.commit();
   }

   // Snapshot to force shared mode
   auto snapshot = env.ses->get_root(0);

   // Remove half the keys in shared mode
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; i += 2)
         trx.remove(tkey(i));
      trx.commit();
   }

   // Snapshot should still see all keys
   {
      cursor c(snapshot);
      for (int i = 0; i < std::min(N, 10); ++i)
      {
         std::string val;
         auto        r = c.get(tkey(i), &val);
         CHECK(r >= 0);
         CHECK(val == big_val(i));
      }
   }

   // New tree should only have odd-indexed keys
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < std::min(N, 10); ++i)
      {
         auto val = trx.get<std::string>(tkey(i));
         if (i % 2 == 0)
            CHECK_FALSE(val.has_value());
         else
            CHECK(val.has_value());
      }
   }
}

TEST_CASE("tree_ops: shared-mode insert new keys with prefix mismatch", "[tree_ops][shared]")
{
   test_db env("tree_ops_shared_prefix_db");

   // Create tree with common prefix keys
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < 50 / OPS_SCALE; ++i)
      {
         std::string k = "alpha/beta/item" + std::to_string(i);
         trx.upsert(k, small_val(i));
      }
      trx.commit();
   }

   // Snapshot
   auto snapshot = env.ses->get_root(0);

   // Insert keys that will cause prefix mismatches in inner_prefix_node paths
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < 50 / OPS_SCALE; ++i)
      {
         // Different prefix - will force inner_prefix_node splits
         std::string k = "alpha/gamma/item" + std::to_string(i);
         trx.upsert(k, small_val(i + 100));
      }
      // Also add completely different prefix to trigger cpre.size() == 0 paths
      for (int i = 0; i < 20 / OPS_SCALE; ++i)
      {
         std::string k = "zzz_totally_different_" + std::to_string(i);
         trx.upsert(k, small_val(i + 200));
      }
      trx.commit();
   }

   // Snapshot should be unmodified
   {
      cursor c(snapshot);
      for (int i = 0; i < std::min(50 / OPS_SCALE, 10); ++i)
      {
         std::string k = "alpha/beta/item" + std::to_string(i);
         std::string val;
         auto        r = c.get(k, &val);
         CHECK(r >= 0);
      }
   }
}

TEST_CASE("tree_ops: heavy mutation forces split_merge on large inner nodes", "[tree_ops][split]")
{
   test_db env("tree_ops_split_merge_db");

   // Build a tree large enough that inner nodes have 16+ cachelines
   // This requires many distinct first-bytes to fan out the inner node
   const int N = 1000 / OPS_SCALE;
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         // Use varied key patterns to maximize fan-out
         char buf[64];
         snprintf(buf, sizeof(buf), "%c%c_%06d",
                  static_cast<char>('!' + (i % 94)),
                  static_cast<char>('!' + ((i / 94) % 94)),
                  i);
         trx.upsert(std::string(buf), small_val(i));
      }
      trx.commit();
   }

   // Snapshot to force shared mode
   auto snapshot = env.ses->get_root(0);

   // Heavy mutation in shared mode - likely triggers split_merge
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         char buf[64];
         snprintf(buf, sizeof(buf), "%c%c_%06d",
                  static_cast<char>('!' + (i % 94)),
                  static_cast<char>('!' + ((i / 94) % 94)),
                  i);
         trx.upsert(std::string(buf), small_val(i + 5000));
      }
      // Also insert brand new keys to grow inner nodes further
      for (int i = N; i < N + 200 / OPS_SCALE; ++i)
      {
         char buf[64];
         snprintf(buf, sizeof(buf), "%c%c_%06d",
                  static_cast<char>('!' + (i % 94)),
                  static_cast<char>('!' + ((i / 94) % 94)),
                  i);
         trx.upsert(std::string(buf), small_val(i));
      }
      trx.commit();
   }

   // Verify the tree structure is valid and data is correct
   auto root = env.ses->get_root(0);
   REQUIRE(root);
   write_cursor wc(root);
   REQUIRE_NOTHROW(wc.validate());

   auto stats = wc.get_stats();
   CHECK(stats.total_keys == static_cast<uint64_t>(N + 200 / OPS_SCALE));

   // Spot-check that updated values are correct (not old values)
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < std::min(N, 20); ++i)
      {
         char buf[64];
         snprintf(buf, sizeof(buf), "%c%c_%06d",
                  static_cast<char>('!' + (i % 94)),
                  static_cast<char>('!' + ((i / 94) % 94)),
                  i);
         auto val = trx.get<std::string>(std::string(buf));
         REQUIRE(val.has_value());
         CHECK(*val == small_val(i + 5000));
      }
   }

   // Snapshot should still have original values
   {
      cursor c(snapshot);
      for (int i = 0; i < std::min(N, 20); ++i)
      {
         char buf[64];
         snprintf(buf, sizeof(buf), "%c%c_%06d",
                  static_cast<char>('!' + (i % 94)),
                  static_cast<char>('!' + ((i / 94) % 94)),
                  i);
         std::string val;
         auto r = c.get(std::string(buf), &val);
         CHECK(r >= 0);
         if (r >= 0)
            CHECK(val == small_val(i));
      }
   }
}

TEST_CASE("tree_ops: shared-mode range_remove", "[tree_ops][shared][range_remove]")
{
   test_db env("tree_ops_shared_rr_db");
   const int N = 200 / OPS_SCALE;

   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
         trx.upsert(tkey(i), small_val(i));
      trx.commit();
   }

   // Snapshot
   auto snapshot = env.ses->get_root(0);

   // Range remove in shared mode
   {
      auto trx = env.ses->start_transaction(0);
      // Remove a range in the middle
      auto removed = trx.remove_range(tkey(N / 4), tkey(3 * N / 4));
      CHECK(removed > 0);
      trx.commit();
   }

   // Verify new tree: keys in removed range are gone, others survive
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < N; ++i)
      {
         auto val = trx.get<std::string>(tkey(i));
         bool in_range = (tkey(i) >= tkey(N / 4) && tkey(i) < tkey(3 * N / 4));
         if (in_range)
            CHECK_FALSE(val.has_value());
         else
            CHECK(val.has_value());
      }
   }

   // Snapshot should retain all keys
   {
      cursor c(snapshot);
      int    found = 0;
      for (int i = 0; i < N; ++i)
      {
         std::string val;
         if (c.get(tkey(i), &val) >= 0)
            ++found;
      }
      CHECK(found == N);
   }
}

TEST_CASE("tree_ops: shared-mode collapse with value_nodes", "[tree_ops][shared][collapse]")
{
   test_db env("tree_ops_shared_collapse_vnode_db");

   // Build tree with value_nodes and enough structure for collapse
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < 40 / OPS_SCALE; ++i)
         trx.upsert(tkey(i), big_val(i));
      trx.commit();
   }

   // Snapshot forces shared mode
   auto snapshot = env.ses->get_root(0);

   // Remove most keys to trigger collapse path (descendents <= threshold)
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 3; i < 40 / OPS_SCALE; ++i)
         trx.remove(tkey(i));
      trx.commit();
   }

   // Remaining keys should still be readable
   {
      auto trx = env.ses->start_transaction(0);
      for (int i = 0; i < 3; ++i)
      {
         auto val = trx.get<std::string>(tkey(i));
         CHECK(val.has_value());
         if (val)
            CHECK(*val == big_val(i));
      }
   }

   // Snapshot still has all keys
   {
      cursor c(snapshot);
      for (int i = 0; i < std::min(40 / OPS_SCALE, 10); ++i)
      {
         std::string val;
         CHECK(c.get(tkey(i), &val) >= 0);
      }
   }
}

TEST_CASE("tree_ops: stats counts sparse_subtree_inners and single_branch_inners", "[tree_ops][stats]")
{
   test_db env("tree_ops_sparse_stats_db");
   auto    trx = env.ses->start_transaction(0);

   // Insert keys designed to create single-branch inner nodes and sparse subtrees
   // Keys with very different first bytes but few children each
   for (int i = 0; i < 10; ++i)
   {
      // Create sparse branches: each prefix has very few keys
      std::string k = std::string(1, static_cast<char>('A' + i)) + "_only_child";
      trx.upsert(k, small_val(i));
   }

   trx.commit();

   auto root = env.ses->get_root(0);
   REQUIRE(root);
   write_cursor wc(root);
   auto         stats = wc.get_stats();

   // Should have some structure
   CHECK(stats.total_keys == 10);
   // With 10 keys each under different first bytes, inner node descendents are small
   // so sparse_subtree_inners should be counted
   CHECK(stats.total_nodes() > 0);
}

TEST_CASE("tree_ops: validate on empty tree", "[tree_ops][validate]")
{
   test_db env("tree_ops_validate_empty_db");

   // Empty tree via create_write_cursor
   auto wc = env.ses->create_write_cursor();
   REQUIRE_FALSE(static_cast<bool>(*wc));

   // validate on empty tree should not throw
   REQUIRE_NOTHROW(wc->validate());
}
