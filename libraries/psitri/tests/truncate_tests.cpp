#include <catch2/catch_all.hpp>
#include <filesystem>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/tree_ops.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri/value_type.hpp>

using namespace psitri;

namespace
{
   const std::string test_dir = "truncate_testdb";

   struct truncate_test_db
   {
      std::shared_ptr<database>      db;
      std::shared_ptr<write_session> ses;
      bool                           created = false;

      truncate_test_db()
      {
         std::filesystem::remove_all(test_dir);
         open();
      }

      ~truncate_test_db() { std::filesystem::remove_all(test_dir); }

      void open()
      {
         if (!created)
         {
            db      = database::open(test_dir);
            created = true;
         }
         else
         {
            db = database::open(test_dir);
         }
         ses = db->start_write_session();
      }

      void close()
      {
         ses.reset();
         db.reset();
      }

      /// Reopen the database (close then open)
      void reopen()
      {
         close();
         open();
      }

      uint64_t segment_count() { return db->dump().total_segments; }

      uint64_t segs_file_size()
      {
         return std::filesystem::file_size(test_dir + "/segs");
      }

      /// Insert N keys of the form "key_XXXXXXXX" with value_size-byte values
      void insert_keys(uint32_t n, uint32_t value_size = 100, uint32_t root_index = 0)
      {
         std::string val(value_size, 'x');
         auto        tx = ses->start_transaction(root_index);
         for (uint32_t i = 0; i < n; ++i)
         {
            char key[32];
            snprintf(key, sizeof(key), "key_%08u", i);
            tx.upsert(to_key(key), to_value_view(val));
         }
         tx.commit();
      }

      /// Remove N keys of the form "key_XXXXXXXX"
      void remove_keys(uint32_t start, uint32_t count, uint32_t root_index = 0)
      {
         auto tx = ses->start_transaction(root_index);
         for (uint32_t i = start; i < start + count; ++i)
         {
            char key[32];
            snprintf(key, sizeof(key), "key_%08u", i);
            tx.remove(to_key(key));
         }
         tx.commit();
      }

      /// Verify N keys are readable and have the expected value
      void verify_keys(uint32_t n, uint32_t value_size = 100, uint32_t root_index = 0)
      {
         auto root = ses->get_root(root_index);
         REQUIRE(root);
         cursor c(root);
         for (uint32_t i = 0; i < n; ++i)
         {
            char key[32];
            snprintf(key, sizeof(key), "key_%08u", i);
            std::string buf;
            INFO("verifying key: " << key);
            REQUIRE(c.get(to_key(key), &buf) >= 0);
            REQUIRE(buf.size() == value_size);
         }
      }
   };
}  // namespace

// ============================================================
// Basic truncation: grow, truncate, verify data intact
// ============================================================

TEST_CASE("compact_and_truncate reclaims trailing segments", "[truncate]")
{
   truncate_test_db t;

   // Insert enough data to allocate multiple segments (32 MB each)
   // 100K keys × 200 bytes ≈ 20 MB raw, but COW overhead during writes
   // will cause many more segments to be allocated
   t.insert_keys(100000, 200);

   auto before = t.segment_count();
   REQUIRE(before > 1);

   // Remove most keys to free segments
   t.remove_keys(0, 90000);

   // Wait for compactor and truncate
   t.db->compact_and_truncate();

   auto after = t.segment_count();
   INFO("segments before: " << before << " after: " << after);
   REQUIRE(after < before);

   // Verify remaining keys 90000-99999 are intact
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   for (uint32_t i = 90000; i < 100000; ++i)
   {
      char key[32];
      snprintf(key, sizeof(key), "key_%08u", i);
      std::string buf;
      INFO("key: " << key);
      REQUIRE(c.get(to_key(key), &buf) >= 0);
      REQUIRE(buf.size() == 200);
   }
}

// ============================================================
// Truncation on close: destructor reclaims space
// ============================================================

TEST_CASE("destructor truncates trailing free segments", "[truncate]")
{
   truncate_test_db t;

   t.insert_keys(100000, 200);

   auto size_after_insert = t.segs_file_size();

   // Remove most data so compactor can free segments
   t.remove_keys(0, 90000);
   t.db->wait_for_compactor();

   // Close without explicit truncation — destructor should truncate
   t.close();

   auto size_after_close = std::filesystem::file_size(test_dir + "/segs");
   INFO("segs file: " << size_after_insert << " -> " << size_after_close);
   REQUIRE(size_after_close < size_after_insert);

   // Reopen and verify remaining data
   t.open();
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   for (uint32_t i = 90000; i < 100000; ++i)
   {
      char key[32];
      snprintf(key, sizeof(key), "key_%08u", i);
      std::string buf;
      INFO("key: " << key);
      REQUIRE(c.get(to_key(key), &buf) >= 0);
      REQUIRE(buf.size() == 200);
   }
}

// ============================================================
// Multiple grow/shrink/reopen cycles
// ============================================================

TEST_CASE("grow shrink reopen cycles", "[truncate]")
{
   truncate_test_db t;

   for (int cycle = 0; cycle < 3; ++cycle)
   {
      INFO("cycle " << cycle);

      // Grow: insert 50K keys
      uint32_t base = cycle * 50000;
      t.insert_keys(50000, 200);

      auto seg_after_grow = t.segment_count();
      REQUIRE(seg_after_grow > 0);

      // Shrink: remove half the keys
      t.remove_keys(0, 25000);
      t.db->compact_and_truncate();

      auto seg_after_shrink = t.segment_count();
      INFO("segments: " << seg_after_grow << " -> " << seg_after_shrink);

      // Close and reopen
      t.reopen();

      // Verify the 25K surviving keys
      auto root = t.ses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      for (uint32_t i = 25000; i < 50000; ++i)
      {
         char key[32];
         snprintf(key, sizeof(key), "key_%08u", i);
         std::string buf;
         REQUIRE(c.get(to_key(key), &buf) >= 0);
      }

      // Remove the rest to start fresh next cycle
      t.remove_keys(25000, 25000);
      t.db->compact_and_truncate();
   }
}

// ============================================================
// Truncate is a no-op when nothing is free
// ============================================================

TEST_CASE("compact_and_truncate is safe when nothing to truncate", "[truncate]")
{
   truncate_test_db t;

   // Insert a small amount of data (fits in 1-2 segments)
   t.insert_keys(1000, 100);

   // Truncate — even if provider queue segments get recycled,
   // all data must remain intact
   t.db->compact_and_truncate();

   // Verify data intact
   t.verify_keys(1000, 100);
}

// ============================================================
// Multiple truncations without close
// ============================================================

TEST_CASE("repeated truncation without close", "[truncate]")
{
   truncate_test_db t;

   // Phase 1: grow big
   t.insert_keys(100000, 200);
   auto peak = t.segment_count();

   // Phase 2: remove half, truncate
   t.remove_keys(50000, 50000);
   t.db->compact_and_truncate();
   auto mid = t.segment_count();
   REQUIRE(mid <= peak);

   // Phase 3: remove more, truncate again
   t.remove_keys(25000, 25000);
   t.db->compact_and_truncate();
   auto low = t.segment_count();
   REQUIRE(low <= mid);

   // Verify surviving keys
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   for (uint32_t i = 0; i < 25000; ++i)
   {
      char key[32];
      snprintf(key, sizeof(key), "key_%08u", i);
      std::string buf;
      REQUIRE(c.get(to_key(key), &buf) >= 0);
   }
}

// ============================================================
// Grow after truncation: ensure new segments can be allocated
// ============================================================

TEST_CASE("grow after truncation allocates new segments", "[truncate]")
{
   truncate_test_db t;

   // Phase 1: grow, then delete everything and truncate
   t.insert_keys(100000, 200);
   t.remove_keys(0, 100000);
   t.db->compact_and_truncate();
   auto after_truncate = t.segment_count();

   // Phase 2: grow again — new segments should be allocated from the file
   t.insert_keys(100000, 200);
   auto after_regrow = t.segment_count();
   REQUIRE(after_regrow > after_truncate);

   // Verify all data
   t.verify_keys(100000, 200);
}

// ============================================================
// Truncate, reopen, grow, truncate, reopen — full lifecycle
// ============================================================

TEST_CASE("full lifecycle: truncate reopen grow truncate reopen", "[truncate]")
{
   truncate_test_db t;

   // Step 1: populate
   t.insert_keys(80000, 150);
   auto seg1 = t.segment_count();

   // Step 2: delete most, truncate
   t.remove_keys(0, 70000);
   t.db->compact_and_truncate();
   auto seg2 = t.segment_count();
   REQUIRE(seg2 < seg1);

   // Step 3: reopen
   t.reopen();
   auto seg3 = t.segment_count();
   REQUIRE(seg3 == seg2);  // no change from reopen alone

   // Verify surviving data
   auto root = t.ses->get_root(0);
   REQUIRE(root);
   cursor c(root);
   for (uint32_t i = 70000; i < 80000; ++i)
   {
      char key[32];
      snprintf(key, sizeof(key), "key_%08u", i);
      std::string buf;
      REQUIRE(c.get(to_key(key), &buf) >= 0);
   }

   // Step 4: grow again past original size
   t.insert_keys(120000, 150);
   auto seg4 = t.segment_count();
   REQUIRE(seg4 > seg3);

   // Step 5: delete all, truncate
   t.remove_keys(0, 120000);
   t.db->compact_and_truncate();
   auto seg5 = t.segment_count();
   REQUIRE(seg5 < seg4);

   // Step 6: reopen on minimal DB
   auto size_before = t.segs_file_size();
   t.reopen();
   auto size_after = t.segs_file_size();
   // Destructor truncation may have shrunk it further
   REQUIRE(size_after <= size_before);
}

// ============================================================
// File size matches segment count after truncation
// ============================================================

TEST_CASE("file size consistent with segment count", "[truncate]")
{
   truncate_test_db t;

   t.insert_keys(50000, 200);

   // Check consistency: file_size == segment_count * 32MB
   auto check_consistency = [&]()
   {
      auto segs      = t.segment_count();
      auto file_size = t.segs_file_size();
      INFO("segments: " << segs << " file_size: " << file_size
                         << " expected: " << segs * 32ULL * 1024 * 1024);
      REQUIRE(file_size == segs * 32ULL * 1024 * 1024);
   };

   check_consistency();

   // Remove, truncate, check
   t.remove_keys(0, 40000);
   t.db->compact_and_truncate();
   check_consistency();

   // Reopen, check
   t.reopen();
   check_consistency();

   // Grow again, check
   t.insert_keys(50000, 200);
   check_consistency();
}
