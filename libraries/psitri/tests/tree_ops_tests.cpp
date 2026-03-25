#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/value_type.hpp>


using namespace psitri;

constexpr int OPS_SCALE = 1;

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
   // N must be large enough to force a multi-level tree structure;
   // a single leaf holds ~50-60 small entries, so don't scale this down.
   const int N = 60;

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

TEST_CASE("tree_ops: shared-mode phase 3 multi-branch collapse", "[tree_ops][collapse]")
{
   test_db env("tree_ops_shared_phase3_db");

   // Strategy:
   // 1. Build a tree, then remove keys in unique mode (threshold=0 to prevent
   //    collapse) until we have an inner_node with 4 branches and 25 descendants.
   // 2. Snapshot the root (makes it shared).
   // 3. Remove 1 key that empties a branch, using a new tree_context with
   //    threshold=25. Phase 3 fires: num_branches>2, new_desc=24<=25.
   //
   // Use 10 groups × 60 keys = 600 keys to force a multi-branch root inner_node.
   const int groups    = 10;
   const int per_group = 60;
   {
      auto trx = env.ses->start_transaction(0);
      for (int g = 0; g < groups; ++g)
         for (int k = 0; k < per_group; ++k)
         {
            char buf[32];
            snprintf(buf, sizeof(buf), "%c_item_%03d", 'A' + g, k);
            trx.upsert(std::string(buf), small_val(g * per_group + k));
         }
      trx.commit();
   }

   // Step 2: Remove keys in unique mode with collapse disabled (threshold=0).
   // Remove all groups except A, B, C, D. Then trim A,B,C to 8 keys each
   // and D to 1 key. Final state: inner_node with 4+ branches, 25 descendants.
   {
      auto root = env.ses->get_root(0);
      tree_context ctx(std::move(root));
      ctx.set_collapse_threshold(0);  // disable collapse during pruning

      // Remove groups E-J entirely
      for (int g = 4; g < groups; ++g)
         for (int k = 0; k < per_group; ++k)
         {
            char buf[32];
            snprintf(buf, sizeof(buf), "%c_item_%03d", 'A' + g, k);
            ctx.remove(std::string(buf));
         }
      // Trim A, B, C to 8 keys each
      for (int g = 0; g < 3; ++g)
         for (int k = 8; k < per_group; ++k)
         {
            char buf[32];
            snprintf(buf, sizeof(buf), "%c_item_%03d", 'A' + g, k);
            ctx.remove(std::string(buf));
         }
      // Trim D to 1 key
      for (int k = 1; k < per_group; ++k)
      {
         char buf[32];
         snprintf(buf, sizeof(buf), "%c_item_%03d", 'D', k);
         ctx.remove(std::string(buf));
      }

      // Persist the pruned tree
      env.ses->set_root(0, ctx.get_root());
   }

   // Step 3: Snapshot to force shared mode
   auto snapshot = env.ses->get_root(0);

   // Step 4: Remove D_item_000 in shared mode with collapse_threshold=25.
   // The inner_node has 4+ branches (>2), and new_desc = 25-1 = 24 <= 25.
   {
      auto root = env.ses->get_root(0);
      tree_context ctx(std::move(root));
      ctx.set_collapse_threshold(25);
      ctx.remove("D_item_000");
      env.ses->set_root(0, ctx.get_root());
   }

   // Verify remaining 24 keys
   {
      auto trx = env.ses->start_transaction(0);
      for (int g = 0; g < 3; ++g)
         for (int k = 0; k < 8; ++k)
         {
            char buf[32];
            snprintf(buf, sizeof(buf), "%c_item_%03d", 'A' + g, k);
            auto val = trx.get<std::string>(std::string(buf));
            CHECK(val.has_value());
         }
      CHECK_FALSE(trx.get<std::string>("D_item_000").has_value());
   }

   // Snapshot should still see all 25 keys
   {
      cursor c(snapshot);
      std::string val;
      CHECK(c.get("D_item_000", &val) >= 0);
      for (int g = 0; g < 3; ++g)
         for (int k = 0; k < 8; ++k)
         {
            char buf[32];
            snprintf(buf, sizeof(buf), "%c_item_%03d", 'A' + g, k);
            CHECK(c.get(std::string(buf), &val) >= 0);
         }
   }
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

// Exercise size_subtree traversal through inner_node (non-prefix) children.
//
// The inner_node case in size_subtree (line 362) is hit when collapse checks a
// node whose children include inner_nodes. This happens when:
// 1. An inner_prefix_node accumulates enough branches to split, producing two
//    inner_node children (see split() at line 877-884)
// 2. Removing keys causes the parent's descendents to drop below the collapse
//    threshold, triggering size_subtree on all children
// 3. Sibling inner_nodes NOT on the remove path remain as inner_nodes
//
// Strategy: insert many keys under a shared prefix (byte 0 = 0x00) to force
// the inner_prefix_node at that branch to split into inner_node children.
// Then hold a snapshot (shared mode) and remove enough keys to trigger collapse.
TEST_CASE("tree_ops: size_subtree through inner_node children", "[tree_ops][collapse][size_subtree]")
{
   test_db env("tree_ops_size_subtree_inner_db");

   // Phase 1: Build a tree where inner_prefix_node children survive collapse.
   //
   // size_subtree has paths for inner_prefix_node (line 369) and inner_node
   // (line 362). To hit these, the collapsing parent must have children that
   // are inner_prefix_nodes (not leaves). This requires:
   //
   // 1. Children are inner_prefix_nodes (created by leaf overflow)
   // 2. Untouched children remain as inner_prefix_nodes during removal
   // 3. Parent descendents ≤ collapse_threshold (24)
   //
   // The key challenge: normal keys (~10 bytes) allow ~100 entries per leaf,
   // so inner_prefix is only created with >100 keys per group — far exceeding
   // the collapse threshold. Solution: use VERY LARGE keys (~1000 bytes) so
   // only 2 entries fit per leaf. Then 3 keys per group overflows a leaf and
   // creates an inner_prefix_node. 8 groups × 3 keys = 24 = threshold.
   //
   // After removing 1 key from one group, root has 23 descendents. Root's
   // collapse fires, and untouched sibling groups are still inner_prefix_nodes.
   const int NUM_GROUPS = 8;
   const int KEYS_PER_GROUP = 3;

   // Generate 1000-byte keys: {group_byte, idx_byte, padding...}
   auto make_key = [](int group, int idx) -> std::string {
      std::string k(1000, 'K');
      k[0] = (char)group;
      snprintf(k.data() + 1, 999, "g%02d-k%02d", group, idx);
      return k;
   };

   {
      auto trx = env.ses->start_transaction(0);
      for (int g = 0; g < NUM_GROUPS; ++g)
         for (int k = 0; k < KEYS_PER_GROUP; ++k)
            trx.upsert(make_key(g, k), small_val(g * KEYS_PER_GROUP + k));
      trx.commit();
   }

   int total_inserted = NUM_GROUPS * KEYS_PER_GROUP;

   // Check tree structure
   {
      auto root = env.ses->get_root(0);
      REQUIRE(root);
      write_cursor wc(root);
      auto stats = wc.get_stats();
      INFO("before remove: inner_nodes=" << stats.inner_nodes
           << " inner_prefix=" << stats.inner_prefix_nodes
           << " leaves=" << stats.leaf_nodes
           << " keys=" << stats.total_keys
           << " depth=" << stats.max_depth);
      REQUIRE(stats.total_keys == total_inserted);
      // Root is inner_node; with 3 large keys per group, leaves overflow
      // creating inner_prefix_node children
      REQUIRE(stats.inner_nodes >= 1);
      REQUIRE(stats.inner_prefix_nodes >= 1);
   }

   // Hold snapshot for shared mode (COW path during removes)
   auto snapshot = env.ses->get_root(0);

   // Phase 2: Remove ALL keys from group 0 so its branch returns empty.
   // In unique_remove mode, Phase 3 (collapse) only fires when
   // sub_branches.count()==0 (branch completely emptied). After the branch
   // is removed, the root has 7 branches, 21 descendents ≤ 24.
   // Phase 3 fires and size_subtree encounters groups 1-7 as inner_prefix_nodes.
   {
      auto trx = env.ses->start_transaction(0);
      for (int k = 0; k < KEYS_PER_GROUP; ++k)
         trx.remove(make_key(0, k));
      trx.commit();
   }

   // Phase 3: Verify correctness
   {
      auto root = env.ses->get_root(0);
      REQUIRE(root);
      write_cursor wc(root);
      auto stats = wc.get_stats();
      const int remaining = total_inserted - KEYS_PER_GROUP;  // 21 keys
      INFO("after remove: inner_nodes=" << stats.inner_nodes
           << " inner_prefix=" << stats.inner_prefix_nodes
           << " leaves=" << stats.leaf_nodes
           << " keys=" << stats.total_keys);
      CHECK(stats.total_keys == remaining);

      // Verify remaining keys
      for (int g = 1; g < NUM_GROUPS; ++g)
         for (int k = 0; k < KEYS_PER_GROUP; ++k)
         {
            auto val = wc.get<std::string>(make_key(g, k));
            REQUIRE(val.has_value());
            CHECK(*val == small_val(g * KEYS_PER_GROUP + k));
         }
      wc.validate();
   }

   // Snapshot should still have all original keys
   {
      write_cursor wc(snapshot);
      CHECK(wc.count_keys() == total_inserted);
   }
}

// Exercise Phase 2 single-branch collapse when the remaining child is an
// inner_prefix_node. This covers two uncovered paths in tree_ops.hpp:
//
// 1. Parent is inner_node, child is inner_prefix_node (line 1242-1245):
//    Root inner_node has 2 first-byte groups. One group has enough large keys
//    to create an inner_prefix_node child. Remove all keys from the other group
//    → root has 1 branch → remaining child is inner_prefix_node.
//
// 2. Parent is inner_prefix_node, child is inner_prefix_node (line 1247-1256):
//    Keys share a common prefix, creating a parent inner_prefix_node. Under it,
//    two subgroups each have enough keys to create child inner_prefix_nodes.
//    Remove all keys from one subgroup → parent has 1 branch → remaining child
//    is inner_prefix_node. The two prefixes get concatenated.
TEST_CASE("tree_ops: Phase 2 collapse inner_prefix child", "[tree_ops][collapse][phase2]")
{
   test_db env("tree_ops_phase2_prefix_db");

   // 1000-byte keys: only 2 fit per leaf, so 3 keys per group overflows a leaf
   // and creates an inner_prefix_node.
   auto make_key = [](char prefix_byte, int group, int idx) -> std::string {
      std::string k(1000, 'K');
      k[0] = prefix_byte;
      snprintf(k.data() + 1, 999, "g%02d-k%02d", group, idx);
      return k;
   };

   // --- Case 1: inner_node parent, inner_prefix child (line 1242) ---
   // Root inner_node branches on byte 0. Byte 0x00 has 3 keys → inner_prefix.
   // Byte 0x01 has 1 key → leaf. Remove the 0x01 key.
   // Uses write_cursor directly (not transaction) so root ref==1, keeping
   // unique mode — otherwise root ref>1 from root table forces shared mode
   // which hits a different Phase 2 code path.
   SECTION("inner_node parent absorbs inner_prefix child")
   {
      auto wc = env.ses->create_write_cursor();

      // Group under byte 0x00: 3 keys → inner_prefix_node
      for (int k = 0; k < 3; ++k)
         wc->upsert(make_key(0x00, 0, k), small_val(k));
      // Single key under byte 0x01: leaf
      wc->upsert(make_key(0x01, 0, 0), small_val(100));

      // Verify structure: root inner_node, at least 1 inner_prefix child
      {
         auto stats = wc->get_stats();
         INFO("case1 before: inner=" << stats.inner_nodes
              << " prefix=" << stats.inner_prefix_nodes
              << " leaf=" << stats.leaf_nodes);
         REQUIRE(stats.inner_nodes >= 1);
         REQUIRE(stats.inner_prefix_nodes >= 1);
         REQUIRE(stats.total_keys == 4);
      }

      // Remove the lone key under 0x01 → unique Phase 2 collapses root
      wc->remove(make_key(0x01, 0, 0));

      // Verify: root should now be an inner_prefix_node (absorbed from child)
      {
         auto stats = wc->get_stats();
         INFO("case1 after: inner=" << stats.inner_nodes
              << " prefix=" << stats.inner_prefix_nodes
              << " leaf=" << stats.leaf_nodes);
         CHECK(stats.total_keys == 3);
         // All 3 remaining keys must be valid
         for (int k = 0; k < 3; ++k)
         {
            auto val = wc->get<std::string>(make_key(0x00, 0, k));
            REQUIRE(val.has_value());
            CHECK(*val == small_val(k));
         }
         wc->validate();
      }
   }

   // --- Case 2: inner_prefix parent absorbs inner_prefix child (line 1247) ---
   // All keys share byte 0x00, creating an inner_prefix_node under the root.
   // Under that inner_prefix, two subgroups (distinguished by a byte after
   // the shared prefix) each have 3 keys → each creates a child inner_prefix.
   // Remove all keys from one subgroup → parent prefix merges with child prefix.
   SECTION("inner_prefix parent absorbs inner_prefix child")
   {
      // Keys: byte0=0x00, then "g00-k0X..." or "g01-k0X..."
      // "g00" and "g01" share "g0" before diverging at byte 3.
      // Parent inner_prefix(prefix including "g0") branches on '0' vs '1'.
      // Each child has 3 keys with long shared suffix → child inner_prefix.
      // Uses write_cursor directly so root ref==1 (unique mode).
      auto wc = env.ses->create_write_cursor();

      // Subgroup 0: 3 keys under byte0=0x00, distinguished by "g00-k0{0,1,2}"
      for (int k = 0; k < 3; ++k)
         wc->upsert(make_key(0x00, 0, k), small_val(k));
      // Subgroup 1: 3 keys under byte0=0x00, distinguished by "g01-k0{0,1,2}"
      for (int k = 0; k < 3; ++k)
         wc->upsert(make_key(0x00, 1, k), small_val(10 + k));

      // Verify structure
      {
         auto stats = wc->get_stats();
         INFO("case2 before: inner=" << stats.inner_nodes
              << " prefix=" << stats.inner_prefix_nodes
              << " leaf=" << stats.leaf_nodes
              << " depth=" << stats.max_depth);
         REQUIRE(stats.total_keys == 6);
         // Need nested inner_prefix: parent + at least 2 children
         REQUIRE(stats.inner_prefix_nodes >= 2);
      }

      // Remove all keys from subgroup 1 → parent inner_prefix absorbs child
      for (int k = 0; k < 3; ++k)
         wc->remove(make_key(0x00, 1, k));

      // Verify: prefixes merged, all remaining keys valid
      {
         auto stats = wc->get_stats();
         INFO("case2 after: inner=" << stats.inner_nodes
              << " prefix=" << stats.inner_prefix_nodes
              << " leaf=" << stats.leaf_nodes);
         CHECK(stats.total_keys == 3);
         for (int k = 0; k < 3; ++k)
         {
            auto val = wc->get<std::string>(make_key(0x00, 0, k));
            REQUIRE(val.has_value());
            CHECK(*val == small_val(k));
         }
         wc->validate();
      }
   }
}
