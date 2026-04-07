// Test: DWAL cursor visibility after committed transactions
// Verifies all four read modes: trie, buffered, fresh, latest.

#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/dwal_transaction.hpp>
#include <psitri/dwal/merge_cursor.hpp>

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <thread>

namespace fs = std::filesystem;

struct test_db {
   fs::path path;
   std::shared_ptr<psitri::database> psi;
   std::shared_ptr<psitri::dwal::dwal_database> dwal;
};

static test_db make_test_db(const std::string& name,
                            psitri::dwal::dwal_config cfg = {})
{
   auto path = fs::temp_directory_path() / name;
   fs::remove_all(path);

   auto psi_db = psitri::database::create(path / "data");
   if (cfg.max_rw_entries == 0)
      cfg.max_rw_entries = 100000;
   if (cfg.merge_threads == 0)
      cfg.merge_threads = 1;
   auto dwal_db = std::make_shared<psitri::dwal::dwal_database>(
       psi_db, path / "wal", cfg);

   return {path, psi_db, dwal_db};
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

TEST_CASE("DWAL buffered mode sees data after swap", "[dwal][cursor][buffered]")
{
   psitri::dwal::dwal_config cfg;
   cfg.max_rw_entries = 10;  // swap after 10 entries
   cfg.merge_threads = 1;
   auto db = make_test_db("test_dwal_buffered_swap", cfg);

   // Insert 15 entries — exceeds max_rw_entries, triggers swap on commit
   for (int i = 0; i < 15; i++)
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("key" + std::to_string(i), "val" + std::to_string(i));
      tx.commit();
   }

   {
      // Buffered cursor should see the swapped data in RO
      auto cursor = db.dwal->create_cursor(0, psitri::dwal::read_mode::buffered);
      auto& mc = cursor.cursor();
      mc.seek_begin();

      int count = 0;
      while (!mc.is_end())
      {
         count++;
         mc.next();
      }
      // At least 10 should have been swapped to RO
      CHECK(count >= 10);
   }  // cursor destroyed before cleanup

   cleanup(db);
}

TEST_CASE("DWAL time-based swap makes data visible to buffered", "[dwal][cursor][time-swap]")
{
   psitri::dwal::dwal_config cfg;
   cfg.max_rw_entries = 100000;  // won't trigger by count
   cfg.max_freshness_delay = std::chrono::milliseconds(1);
   cfg.merge_threads = 1;
   auto db = make_test_db("test_dwal_time_swap", cfg);

   // Insert 3 entries (under max_rw_entries)
   for (int i = 0; i < 3; i++)
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("key" + std::to_string(i), "val" + std::to_string(i));
      tx.commit();
   }

   // Sleep past the flush delay
   std::this_thread::sleep_for(std::chrono::milliseconds(5));

   // One more commit triggers the time check in should_swap
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("key_trigger", "val_trigger");
      tx.commit();
   }

   {
      auto cursor = db.dwal->create_cursor(0, psitri::dwal::read_mode::buffered);
      auto& mc = cursor.cursor();
      mc.seek_begin();

      int count = 0;
      while (!mc.is_end())
      {
         count++;
         mc.next();
      }
      CHECK(count >= 3);
   }

   cleanup(db);
}

TEST_CASE("DWAL fresh mode finds committed data via writer swap", "[dwal][cursor][fresh]")
{
   psitri::dwal::dwal_config cfg;
   cfg.max_rw_entries = 100000;
   cfg.max_freshness_delay = std::chrono::milliseconds(1);
   cfg.merge_threads = 1;
   auto db = make_test_db("test_dwal_fresh", cfg);

   // Insert 5 entries
   for (int i = 0; i < 5; i++)
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("key" + std::to_string(i), "val" + std::to_string(i));
      tx.commit();
   }

   // Launch reader thread that requests fresh mode
   std::atomic<int> reader_count{0};
   std::thread reader([&] {
      auto cursor = db.dwal->create_cursor(0, psitri::dwal::read_mode::fresh);
      auto& mc = cursor.cursor();
      mc.seek_begin();
      int c = 0;
      while (!mc.is_end()) { c++; mc.next(); }
      reader_count.store(c);
   });

   // Writer keeps committing — should trigger swap via readers_want_swap
   for (int i = 0; i < 100; i++)
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("extra" + std::to_string(i), "v");
      tx.commit();
      if (reader_count.load() > 0)
         break;
      std::this_thread::sleep_for(std::chrono::microseconds(100));
   }

   reader.join();
   CHECK(reader_count.load() >= 5);

   cleanup(db);
}

TEST_CASE("DWAL fresh mode with idle writer — merge thread swaps", "[dwal][cursor][fresh][merge]")
{
   psitri::dwal::dwal_config cfg;
   cfg.max_rw_entries = 5;  // small buffer → quick swap
   cfg.merge_threads = 1;
   auto db = make_test_db("test_dwal_fresh_idle", cfg);

   // Insert enough to trigger swap + merge
   for (int i = 0; i < 10; i++)
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("batch1_" + std::to_string(i), "val");
      tx.commit();
   }

   // Wait for merge to complete
   std::this_thread::sleep_for(std::chrono::milliseconds(50));

   // Insert more data (now in RW, merge_complete should be true)
   for (int i = 0; i < 3; i++)
   {
      auto tx = db.dwal->start_write_transaction(0);
      tx.upsert("batch2_" + std::to_string(i), "val");
      tx.commit();
   }

   {
      // Request fresh cursor — merge thread should detect readers_want_swap
      // and do the swap since writer is idle
      auto cursor = db.dwal->create_cursor(0, psitri::dwal::read_mode::fresh);
      auto& mc = cursor.cursor();
      mc.seek_begin();

      int count = 0;
      while (!mc.is_end()) { count++; mc.next(); }

      // Should see at least batch2 data (3 keys) in RO, plus batch1 in Tri
      CHECK(count >= 10);
   }

   cleanup(db);
}
