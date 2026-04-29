#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/value_type.hpp>
#include <psitri/write_session_impl.hpp>
#include <cstdio>
#include <string>

using namespace psitri;

namespace
{
   struct database_test_dir
   {
      explicit database_test_dir(std::string name) : dir(std::move(name))
      {
         std::filesystem::remove_all(dir);
         std::filesystem::create_directories(dir / "data");
      }

      ~database_test_dir() { std::filesystem::remove_all(dir); }

      std::filesystem::path dir;
   };
}  // namespace

TEST_CASE("database: temporary tree stays detached until published", "[psitri][database]")
{
   database_test_dir tmp("database_temp_tree_testdb");
   auto              db = database::open(tmp.dir);
   auto              ws = db->start_write_session();

   auto tx = ws->start_write_transaction(ws->create_temporary_tree());
   tx.upsert("hello", "world");

   auto tree = tx.get_tree();
   REQUIRE(tree.get<std::string>("hello") == std::optional<std::string>("world"));

   auto rs = db->start_read_session();
   REQUIRE_FALSE(rs->get_root(0));
}

TEST_CASE("database: independently built subtrees publish through one transaction",
          "[psitri][database]")
{
   database_test_dir tmp("database_subtree_publish_testdb");
   auto              db = database::open(tmp.dir);
   auto              ws = db->start_write_session();

   tree base_subtree;
   {
      auto tx = ws->start_write_transaction(ws->create_temporary_tree());
      tx.upsert("base", "value");
      base_subtree = tx.get_tree();
   }

   auto b3a_tx = ws->start_write_transaction(base_subtree);
   b3a_tx.upsert("block3a", "trx1");
   auto b3a = b3a_tx.get_tree();

   auto b3b_tx = ws->start_write_transaction(base_subtree);
   b3b_tx.upsert("block3b", "trx3");
   b3b_tx.upsert("block3b", "trx4");
   auto b3b = b3b_tx.get_tree();

   auto tx = ws->start_transaction(0);
   tx.upsert_subtree("b3a", std::move(b3a));
   tx.upsert_subtree("b3b", std::move(b3b));
   tx.commit();

   auto rs   = db->start_read_session();
   auto root = rs->get_root(0);
   REQUIRE(root);
   auto b3a_root = root.get_subtree("b3a");
   auto b3b_root = root.get_subtree("b3b");
   REQUIRE(b3a_root);
   REQUIRE(b3b_root);
   REQUIRE(b3a_root.get<std::string>("block3a") == std::optional<std::string>("trx1"));
   REQUIRE(b3b_root.get<std::string>("block3b") == std::optional<std::string>("trx4"));
}

TEST_CASE("database: tree stats collect per-depth fanout and leaf density",
          "[psitri][database][tree-stats]")
{
   database_test_dir tmp("database_tree_stats_testdb");
   auto              db = database::open(tmp.dir);
   auto              ws = db->start_write_session();

   {
      auto tx = ws->start_transaction(0);
      for (uint32_t i = 0; i < 768; ++i)
      {
         auto key = std::string("account-") + std::to_string(i / 16) + "-" +
                    std::to_string(i % 16);
         auto value = std::string("value-payload-") + std::to_string(i);
         tx.upsert(key, value);
      }
      tx.commit();
   }

   auto stats = db->tree_stats();
   REQUIRE(stats.roots_checked >= 1);
   REQUIRE(stats.leaf_nodes > 0);
   REQUIRE(stats.leaf_keys == 768);
   REQUIRE(stats.max_depth > 1);
   REQUIRE(stats.depth_stats.size() > stats.max_depth);

   uint64_t inner_nodes = 0;
   uint64_t inner_prefix_nodes = 0;
   uint64_t value_nodes = 0;
   uint64_t flat_value_nodes = 0;
   uint64_t inner_branches = 0;
   uint64_t single_branch_inners = 0;
   uint64_t low_fanout_inners = 0;
   uint64_t leaf_nodes = 0;
   uint64_t leaf_keys = 0;
   uint64_t selected_leaf_keys = 0;
   uint64_t leaf_alloc_bytes = 0;
   uint64_t leaf_used_bytes = 0;
   uint64_t leaf_dead_bytes = 0;
   uint64_t leaf_empty_bytes = 0;
   uint64_t full_leaf_nodes = 0;
   uint64_t full_leaf_dead_bytes = 0;
   uint64_t full_leaf_empty_bytes = 0;

   for (const auto& row : stats.depth_stats)
   {
      REQUIRE(row.fanout.total() == row.total_inner_nodes());
      inner_nodes += row.inner_nodes;
      inner_prefix_nodes += row.inner_prefix_nodes;
      value_nodes += row.value_nodes;
      flat_value_nodes += row.flat_value_nodes;
      inner_branches += row.inner_branches;
      single_branch_inners += row.single_branch_inners;
      low_fanout_inners += row.low_fanout_inners;
      leaf_nodes += row.leaf_nodes;
      leaf_keys += row.leaf_keys;
      selected_leaf_keys += row.selected_leaf_keys;
      leaf_alloc_bytes += row.leaf_alloc_bytes;
      leaf_used_bytes += row.leaf_used_bytes;
      leaf_dead_bytes += row.leaf_dead_bytes;
      leaf_empty_bytes += row.leaf_empty_bytes;
      full_leaf_nodes += row.full_leaf_nodes;
      full_leaf_dead_bytes += row.full_leaf_dead_bytes;
      full_leaf_empty_bytes += row.full_leaf_empty_bytes;
   }

   REQUIRE(inner_nodes == stats.inner_nodes);
   REQUIRE(inner_prefix_nodes == stats.inner_prefix_nodes);
   REQUIRE(value_nodes == stats.value_nodes);
   REQUIRE(flat_value_nodes == stats.flat_value_nodes);
   REQUIRE(inner_branches == stats.inner_branches);
   REQUIRE(single_branch_inners == stats.single_branch_inners);
   REQUIRE(low_fanout_inners == stats.low_fanout_inners);
   REQUIRE(leaf_nodes == stats.leaf_nodes);
   REQUIRE(leaf_keys == stats.leaf_keys);
   REQUIRE(selected_leaf_keys == stats.selected_leaf_keys);
   REQUIRE(stats.selected_leaf_keys == stats.leaf_keys);
   REQUIRE(leaf_alloc_bytes == stats.total_leaf_alloc_bytes);
   REQUIRE(leaf_used_bytes == stats.total_leaf_used_bytes);
   REQUIRE(leaf_dead_bytes == stats.total_leaf_dead_bytes);
   REQUIRE(leaf_empty_bytes == stats.total_leaf_empty_bytes);
   REQUIRE(full_leaf_nodes == stats.full_leaf_nodes);
   REQUIRE(full_leaf_dead_bytes == stats.full_leaf_dead_bytes);
   REQUIRE(full_leaf_empty_bytes == stats.full_leaf_empty_bytes);
   REQUIRE(stats.total_leaf_used_bytes + stats.total_leaf_empty_bytes <=
           stats.total_leaf_alloc_bytes);
}

TEST_CASE("database: tree stats can sample a key range",
          "[psitri][database][tree-stats]")
{
   database_test_dir tmp("database_tree_stats_range_testdb");
   auto              db = database::open(tmp.dir);
   auto              ws = db->start_write_session();

   {
      auto tx = ws->start_transaction(0);
      for (uint32_t i = 0; i < 512; ++i)
      {
         char key[16];
         snprintf(key, sizeof(key), "k%04u", i);
         tx.upsert(std::string(key), "value");
      }
      tx.commit();
   }

   auto full = db->tree_stats();

   tree_stats_options options;
   options.root_index = 0;
   options.key_lower = std::string("k0040");
   options.key_upper = std::string("k0080");
   auto sampled = db->tree_stats(options);

   REQUIRE(sampled.key_range_enabled);
   REQUIRE(sampled.root_filter_enabled);
   REQUIRE(sampled.root_filter_index == 0);
   REQUIRE(sampled.roots_checked == 1);
   REQUIRE(sampled.selected_leaf_keys == 40);
   REQUIRE(sampled.leaf_keys >= sampled.selected_leaf_keys);
   REQUIRE(sampled.leaf_keys <= full.leaf_keys);
   REQUIRE(sampled.nodes_visited <= full.nodes_visited);

   uint64_t selected_by_depth = 0;
   for (const auto& row : sampled.depth_stats)
      selected_by_depth += row.selected_leaf_keys;
   REQUIRE(selected_by_depth == sampled.selected_leaf_keys);
}

TEST_CASE("database: tree stats deduplicates shared subtree nodes",
          "[psitri][database][tree-stats]")
{
   database_test_dir tmp("database_tree_stats_shared_subtree_testdb");
   auto              db = database::open(tmp.dir);
   auto              ws = db->start_write_session();

   auto sub_tx = ws->start_write_transaction(ws->create_temporary_tree());
   for (uint32_t i = 0; i < 64; ++i)
   {
      char key[16];
      snprintf(key, sizeof(key), "s%04u", i);
      sub_tx.upsert(std::string(key), "value");
   }
   auto shared_subtree = sub_tx.get_tree();

   {
      auto tx = ws->start_transaction(0);
      tx.upsert_subtree("left", shared_subtree);
      tx.upsert_subtree("right", shared_subtree);
      tx.commit();
   }

   auto stats = db->tree_stats();
   REQUIRE(stats.shared_nodes_skipped > 0);
   REQUIRE(stats.selected_leaf_keys == stats.leaf_keys);
}

TEST_CASE("database: tree stats can stop at a node budget",
          "[psitri][database][tree-stats]")
{
   database_test_dir tmp("database_tree_stats_budget_testdb");
   auto              db = database::open(tmp.dir);
   auto              ws = db->start_write_session();

   {
      auto tx = ws->start_transaction(0);
      for (uint32_t i = 0; i < 256; ++i)
      {
         char key[16];
         snprintf(key, sizeof(key), "b%04u", i);
         tx.upsert(std::string(key), "value");
      }
      tx.commit();
   }

   tree_stats_options options;
   options.max_nodes = 1;
   auto stats = db->tree_stats(options);
   REQUIRE(stats.scan_truncated);
   REQUIRE(stats.max_nodes == 1);
   REQUIRE(stats.nodes_visited == 1);
}
