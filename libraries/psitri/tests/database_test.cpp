#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/value_type.hpp>
#include <psitri/write_session_impl.hpp>

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
