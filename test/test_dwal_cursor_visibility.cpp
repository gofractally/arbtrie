// Test: DWAL cursor visibility after committed transactions
// Verifies that create_cursor(latest) and transaction::create_cursor()
// can see data committed by prior transactions on the same root.

#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>

#include <catch2/catch_test_macros.hpp>
#include <filesystem>

namespace fs = std::filesystem;

static auto make_test_db(const std::string& name)
{
   auto path = fs::temp_directory_path() / name;
   fs::remove_all(path);

   auto psi_db = psitri::database::create(path / "data");
   psitri::dwal::dwal_config cfg;
   cfg.max_rw_entries = 100000;
   cfg.merge_threads  = 1;
   auto dwal_db = std::make_shared<psitri::dwal::dwal_database>(
       psi_db, path / "wal", cfg);

   struct result {
      fs::path path;
      std::shared_ptr<psitri::database> psi;
      std::shared_ptr<psitri::dwal::dwal_database> dwal;
   };
   return result{path, psi_db, dwal_db};
}

static void cleanup(auto& db)
{
   db.dwal->request_shutdown();
   db.dwal.reset();
   db.psi.reset();
   fs::remove_all(db.path);
}

TEST_CASE("DWAL cursor sees committed data from prior transactions", "[dwal][cursor]")
{
   auto db = make_test_db("test_dwal_cursor_vis");

   SECTION("point lookup via get_latest sees committed data")
   {
      // Commit 3 keys in separate transactions
      for (int i = 0; i < 3; i++)
      {
         auto tx = db.dwal->start_write_transaction(0);
         tx.upsert("key" + std::to_string(i), "val" + std::to_string(i));
         tx.commit();
      }

      // Point lookups should find all 3
      for (int i = 0; i < 3; i++)
      {
         auto r = db.dwal->get_latest(0, "key" + std::to_string(i));
         REQUIRE(r.found);
         CHECK(std::string(r.value.data) == "val" + std::to_string(i));
      }
   }

   SECTION("create_cursor(latest) sees committed data")
   {
      for (int i = 0; i < 3; i++)
      {
         auto tx = db.dwal->start_write_transaction(0);
         tx.upsert("key" + std::to_string(i), "val" + std::to_string(i));
         tx.commit();
      }

      auto cursor = db.dwal->create_cursor(0, psitri::dwal::read_mode::latest);
      auto& mc = cursor.cursor();
      mc.seek_begin();

      int count = 0;
      while (!mc.is_end())
      {
         count++;
         mc.next();
      }
      CHECK(count == 3);
   }

   SECTION("create_cursor(latest) lower_bound finds committed key")
   {
      for (int i = 0; i < 3; i++)
      {
         auto tx = db.dwal->start_write_transaction(0);
         tx.upsert("key" + std::to_string(i), "val" + std::to_string(i));
         tx.commit();
      }

      auto cursor = db.dwal->create_cursor(0, psitri::dwal::read_mode::latest);
      auto& mc = cursor.cursor();
      REQUIRE(mc.lower_bound("key1"));
      CHECK(mc.key() == "key1");
   }

   SECTION("transaction::create_cursor sees uncommitted writes")
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("a", "1");
      tx.upsert("b", "2");
      tx.upsert("c", "3");

      auto tc = tx.create_cursor();
      auto& mc = tc.cursor();
      mc.seek_begin();

      int count = 0;
      while (!mc.is_end())
      {
         count++;
         mc.next();
      }
      CHECK(count == 3);

      // Also test lower_bound within transaction
      REQUIRE(mc.lower_bound("b"));
      CHECK(mc.key() == "b");

      tx.abort();
   }

   SECTION("transaction::create_cursor sees prior committed + uncommitted")
   {
      // Commit 2 keys
      {
         auto tx = db.dwal->start_write_transaction(0);
         tx.upsert("committed1", "v1");
         tx.upsert("committed2", "v2");
         tx.commit();
      }

      // Open new tx, add 1 uncommitted key
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("uncommitted", "v3");

      auto tc = tx.create_cursor();
      auto& mc = tc.cursor();
      mc.seek_begin();

      int count = 0;
      while (!mc.is_end())
      {
         count++;
         mc.next();
      }
      CHECK(count == 3);  // 2 committed + 1 uncommitted

      tx.abort();
   }

   cleanup(db);
}
