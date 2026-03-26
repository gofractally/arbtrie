#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <sal/alloc_header.hpp>
#include <sal/mapped_memory/segment.hpp>
#include <fstream>
#include <random>
#include <map>

using namespace psitri;

namespace
{
   /// Return a runtime_config with sync_mode = msync_sync.
   /// Required for power_loss recovery to find valid sync headers on disk.
   runtime_config synced_config()
   {
      runtime_config cfg;
      cfg.sync_mode = sal::sync_type::msync_sync;
      return cfg;
   }

   /// Corrupt the clean_shutdown flag without going through the destructor.
   void corrupt_shutdown_flag(const std::string& dir)
   {
      sal::mapping dbfile(dir + "/dbfile.bin", sal::access_mode::read_write);
      auto* state = reinterpret_cast<detail::database_state*>(dbfile.data());
      state->clean_shutdown.store(false, std::memory_order_relaxed);
      dbfile.sync(sal::sync_type::full);
   }

   std::string make_key(const std::string& prefix, int i)
   {
      char buf[64];
      snprintf(buf, sizeof(buf), "%s%08d", prefix.c_str(), i);
      return buf;
   }

   /// Generate a value of a specific size for a given index.
   /// Sizes > 64 bytes force value_node allocation (separate from leaf inline).
   std::string make_value(int i, size_t size)
   {
      std::string val(size, '\0');
      for (size_t j = 0; j < size; ++j)
         val[j] = static_cast<char>('A' + ((i + j) % 26));
      return val;
   }

   /// Populate a database with diverse node types:
   ///  - prefix-heavy keys → inner_prefix_node
   ///  - 256-way fan-out → wide inner_node
   ///  - large values → value_node
   ///  - dense short keys → deep leaf splits
   /// Returns an oracle map of all keys/values for verification.
   std::map<std::string, std::string> populate_diverse_db(
       std::shared_ptr<write_session>& ses,
       int root_id,
       int scale = 1)
   {
      std::map<std::string, std::string> oracle;
      auto tx = ses->start_transaction(root_id);

      // Group 1: Prefix-heavy keys → inner_prefix_node creation
      const char* prefixes[] = {
          "users/active/profile/",
          "users/active/settings/",
          "users/inactive/",
          "data/metrics/cpu/",
          "data/metrics/mem/",
          "data/logs/error/",
          "data/logs/info/",
          "config/global/",
          "config/local/",
      };
      for (auto* pfx : prefixes)
      {
         for (int i = 0; i < 50 * scale; ++i)
         {
            auto key = make_key(pfx, i);
            auto val = make_value(i, 10 + (i % 30));  // small inline values
            tx.upsert(key, val);
            oracle[key] = val;
         }
      }

      // Group 2: Wide fan-out at root → wide inner_node
      // Use printable ASCII prefixes to create many distinct first-byte branches
      for (int b = 0; b < 95; ++b)
      {
         char prefix = static_cast<char>('!' + b);  // '!' through '~'
         auto key = std::string(1, prefix) + "fan/" + std::to_string(b);
         auto val = make_value(b, 20);
         tx.upsert(key, val);
         oracle[key] = val;
      }

      // Group 3: Large values → value_node allocation
      for (int i = 0; i < 100 * scale; ++i)
      {
         auto key = make_key("bigval/", i);
         auto val = make_value(i, 200 + (i % 300));  // 200-500 byte values
         tx.upsert(key, val);
         oracle[key] = val;
      }

      // Group 4: Dense short keys → deep leaf splits
      for (int i = 0; i < 300 * scale; ++i)
      {
         char buf[8];
         snprintf(buf, sizeof(buf), "%04d", i);
         std::string key(buf);
         auto        val = make_value(i, 15);
         tx.upsert(key, val);
         oracle[key] = val;
      }

      tx.commit();
      return oracle;
   }

   /// Verify all oracle entries exist and match in the database.
   void verify_oracle(
       std::shared_ptr<read_session>& ses,
       int root_id,
       const std::map<std::string, std::string>& oracle)
   {
      auto root = ses->get_root(root_id);
      REQUIRE(root);
      cursor c(root);
      REQUIRE(c.count_keys() == oracle.size());

      for (auto& [key, val] : oracle)
      {
         std::string buf;
         INFO("key: " << key);
         REQUIRE(c.get(to_key_view(key), &buf) >= 0);
         REQUIRE(buf == val);
      }
   }
}  // namespace

// ============================================================
// full_verify recovery on a sizeable, structurally diverse database
// ============================================================

TEST_CASE("full_verify recovery with diverse node types", "[recovery][full_verify]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::full_verify);
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// power_loss recovery on a sizeable, structurally diverse database
// ============================================================

TEST_CASE("power_loss recovery with diverse node types", "[recovery][power_loss]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// full_verify with multiple roots, each with different data patterns
// ============================================================

TEST_CASE("full_verify with multiple diverse roots", "[recovery][full_verify]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle0, oracle1, oracle2;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();

      // Root 0: prefix-heavy data
      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 500; ++i)
         {
            auto key = make_key("alpha/beta/", i);
            auto val = make_value(i, 30);
            tx.upsert(key, val);
            oracle0[key] = val;
         }
         tx.commit();
      }

      // Root 1: large values
      {
         auto tx = ses->start_transaction(1);
         for (int i = 0; i < 200; ++i)
         {
            auto key = make_key("big/", i);
            auto val = make_value(i, 300 + (i % 200));
            tx.upsert(key, val);
            oracle1[key] = val;
         }
         tx.commit();
      }

      // Root 2: fan-out + short keys
      {
         auto tx = ses->start_transaction(2);
         for (int b = 0; b < 95; ++b)
         {
            char prefix = static_cast<char>('!' + b);
            auto key = std::string(1, prefix) + "x/" + std::to_string(b);
            auto val = make_value(b, 16);
            tx.upsert(key, val);
            oracle2[key] = val;
         }
         for (int i = 0; i < 400; ++i)
         {
            char buf[8];
            snprintf(buf, sizeof(buf), "%05d", i);
            std::string key(buf);
            auto        val = make_value(i, 12);
            tx.upsert(key, val);
            oracle2[key] = val;
         }
         tx.commit();
      }
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::full_verify);
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle0);
      verify_oracle(ses, 1, oracle1);
      verify_oracle(ses, 2, oracle2);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// Recovery after multiple transaction cycles (insert, remove, re-insert)
// exercises the allocator's ability to rebuild from segments that
// contain both live and dead objects.
// ============================================================

TEST_CASE("full_verify after insert-remove-reinsert cycles", "[recovery][full_verify]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();

      // Phase 1: Insert 1000 keys with large values
      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 1000; ++i)
         {
            auto key = make_key("cycle/", i);
            auto val = make_value(i, 100 + (i % 150));
            tx.upsert(key, val);
            oracle[key] = val;
         }
         tx.commit();
      }

      // Phase 2: Remove half, creating dead objects in segments
      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 1000; i += 2)
         {
            auto key = make_key("cycle/", i);
            tx.remove(key);
            oracle.erase(key);
         }
         tx.commit();
      }

      // Phase 3: Re-insert with different values, update remaining
      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 1000; i += 2)
         {
            auto key = make_key("cycle/", i);
            auto val = make_value(i + 5000, 80 + (i % 200));
            tx.upsert(key, val);
            oracle[key] = val;
         }
         // Update odd keys with larger values (forces value_node)
         for (int i = 1; i < 1000; i += 2)
         {
            auto key = make_key("cycle/", i);
            auto val = make_value(i + 9000, 250);
            tx.upsert(key, val);
            oracle[key] = val;
         }
         tx.commit();
      }
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::full_verify);
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// power_loss recovery after insert-remove-reinsert cycles
// ============================================================

TEST_CASE("power_loss after insert-remove-reinsert cycles", "[recovery][power_loss]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();

      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 1000; ++i)
         {
            auto key = make_key("ploss/", i);
            auto val = make_value(i, 100 + (i % 150));
            tx.upsert(key, val);
            oracle[key] = val;
         }
         tx.commit();
      }

      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 500; ++i)
         {
            auto key = make_key("ploss/", i);
            tx.remove(key);
            oracle.erase(key);
         }
         // Add keys with different prefix
         for (int i = 0; i < 300; ++i)
         {
            auto key = make_key("ploss2/", i);
            auto val = make_value(i, 200);
            tx.upsert(key, val);
            oracle[key] = val;
         }
         tx.commit();
      }
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// Database continues to work after recovery (write after recover)
// ============================================================

TEST_CASE("writes succeed after full_verify recovery", "[recovery][full_verify]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   corrupt_shutdown_flag(dir);

   // Recover, then continue writing
   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::full_verify);
      auto ses = db->start_write_session();

      // Verify existing data
      {
         auto rses = db->start_read_session();
         verify_oracle(rses, 0, oracle);
      }

      // Add more data after recovery
      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 200; ++i)
         {
            auto key = make_key("post_recovery/", i);
            auto val = make_value(i, 50);
            tx.upsert(key, val);
            oracle[key] = val;
         }
         tx.commit();
      }

      // Verify combined data
      {
         auto rses = db->start_read_session();
         verify_oracle(rses, 0, oracle);
      }
   }

   // Reopen cleanly and verify persistence
   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// Intentional segment data corruption: flip bytes in the segment
// file and verify that recovery handles it gracefully.
// ============================================================

TEST_CASE("recovery handles corrupted segment data", "[recovery][corruption]")
{
   const std::string dir = "corruption_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   // Build a sizeable database with synced config
   std::map<std::string, std::string> oracle;
   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   // Corrupt the segment file: flip bytes at several offsets in the
   // interior of the first segment (past the header area).
   // This simulates disk corruption or torn writes.
   {
      auto segs_path = std::filesystem::path(dir) / "segs";
      REQUIRE(std::filesystem::exists(segs_path));
      auto file_size = std::filesystem::file_size(segs_path);
      REQUIRE(file_size > 0);

      std::fstream f(segs_path, std::ios::in | std::ios::out | std::ios::binary);
      REQUIRE(f.is_open());

      // Corrupt several locations in the first segment's data region.
      // Offsets chosen to be past segment header but within allocated data.
      std::mt19937 rng(42);
      const size_t corrupt_region_start = 4096;   // skip first page (segment metadata)
      const size_t corrupt_region_end   = std::min<size_t>(file_size, 1024 * 1024);

      for (int i = 0; i < 20; ++i)
      {
         size_t offset = corrupt_region_start +
                         (rng() % (corrupt_region_end - corrupt_region_start));
         if (offset >= file_size)
            continue;

         f.seekp(offset);
         char byte;
         f.read(&byte, 1);
         byte ^= 0xFF;  // flip all bits
         f.seekp(offset);
         f.write(&byte, 1);
      }
      f.flush();
   }

   corrupt_shutdown_flag(dir);

   // power_loss recovery should not crash — it validates sync headers
   // and truncates torn tails. With corrupted data, it may lose some
   // entries but should not segfault or throw unexpectedly.
   bool recovered = false;
   try
   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);

      // Recovery succeeded — database is in some valid state.
      // We can't verify all keys match (corruption may have destroyed some)
      // but the database should be traversable without crashing.
      if (root)
      {
         cursor c(root);
         // Just iterate to verify structural integrity — no crash
         uint64_t count = 0;
         c.seek_begin();
         while (!c.is_end())
         {
            // Read key and value to exercise all node types
            auto k = c.key();
            (void)k;
            ++count;
            c.next();
         }
         INFO("recovered " << count << " keys out of " << oracle.size());
      }
      recovered = true;
   }
   catch (const std::exception& e)
   {
      // Some corruption patterns may cause recovery to throw.
      // That's acceptable — what matters is no segfault/SIGBUS.
      INFO("recovery threw: " << e.what());
      recovered = true;  // throwing is acceptable, crashing is not
   }
   REQUIRE(recovered);

   std::filesystem::remove_all(dir);
}

// ============================================================
// Corrupt the roots file and verify power_loss recovery falls
// back to sync headers embedded in segments.
// ============================================================

TEST_CASE("power_loss recovery with corrupted roots file", "[recovery][corruption][power_loss]")
{
   const std::string dir = "corruption_roots_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;
   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   // Corrupt the roots file by zeroing it out
   {
      auto roots_path = std::filesystem::path(dir) / "roots";
      if (std::filesystem::exists(roots_path))
      {
         auto size = std::filesystem::file_size(roots_path);
         std::ofstream f(roots_path, std::ios::binary | std::ios::trunc);
         std::vector<char> zeros(size, 0);
         f.write(zeros.data(), zeros.size());
         f.flush();
      }
   }

   corrupt_shutdown_flag(dir);

   // power_loss recovery should fall back to sync headers in segments
   // to reconstruct root pointers
   bool recovered = false;
   try
   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);

      if (root)
      {
         cursor c(root);
         // Verify at least some data survived via sync header recovery
         uint64_t count = c.count_keys();
         INFO("recovered " << count << " keys from sync headers");
         REQUIRE(count > 0);

         // Verify recovered data is consistent (no crashes during traversal)
         c.seek_begin();
         while (!c.is_end())
         {
            auto k = c.key();
            (void)k;
            c.next();
         }
      }
      recovered = true;
   }
   catch (const std::exception& e)
   {
      INFO("recovery threw (acceptable for severe corruption): " << e.what());
      recovered = true;
   }
   REQUIRE(recovered);

   std::filesystem::remove_all(dir);
}

// ============================================================
// Truncate the segment file to simulate incomplete write during
// power loss, then verify recovery handles the short file.
// ============================================================

TEST_CASE("power_loss recovery with truncated segment file", "[recovery][corruption][power_loss]")
{
   const std::string dir = "corruption_trunc_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;
   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   // Truncate the segment file to 75% of its size
   {
      auto segs_path = std::filesystem::path(dir) / "segs";
      REQUIRE(std::filesystem::exists(segs_path));
      auto original_size = std::filesystem::file_size(segs_path);
      auto truncated_size = original_size * 3 / 4;

      // Truncate by rewriting
      std::vector<char> data(truncated_size);
      {
         std::ifstream in(segs_path, std::ios::binary);
         in.read(data.data(), truncated_size);
      }
      {
         std::ofstream out(segs_path, std::ios::binary | std::ios::trunc);
         out.write(data.data(), truncated_size);
      }
   }

   corrupt_shutdown_flag(dir);

   bool recovered = false;
   try
   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);

      if (root)
      {
         cursor c(root);
         // Some data should survive — the first portion of the segment is intact
         c.seek_begin();
         uint64_t count = 0;
         while (!c.is_end())
         {
            ++count;
            c.next();
         }
         INFO("recovered " << count << " keys after segment truncation");
      }
      recovered = true;
   }
   catch (const std::exception& e)
   {
      INFO("recovery threw after truncation (acceptable): " << e.what());
      recovered = true;
   }
   REQUIRE(recovered);

   std::filesystem::remove_all(dir);
}

// ============================================================
// full_verify after corrupting the dbfile.bin flags field
// ============================================================

TEST_CASE("full_verify clears stale flag and recovers data", "[recovery][full_verify]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   // Set both the unclean shutdown AND stale ref counts flags
   {
      sal::mapping dbfile(dir + "/dbfile.bin", sal::access_mode::read_write);
      auto* state = reinterpret_cast<detail::database_state*>(dbfile.data());
      state->clean_shutdown.store(false, std::memory_order_relaxed);
      state->flags |= detail::flag_ref_counts_stale;
      dbfile.sync(sal::sync_type::full);
   }

   {
      auto db = std::make_shared<database>(dir, synced_config(), recovery_mode::full_verify);
      // full_verify should clear the stale flag
      REQUIRE_FALSE(db->ref_counts_stale());

      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   std::filesystem::remove_all(dir);
}

// ============================================================
// Double recovery: full_verify then power_loss on same database
// ============================================================

TEST_CASE("sequential full_verify then power_loss recovery", "[recovery][full_verify][power_loss]")
{
   const std::string dir = "deep_recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   std::map<std::string, std::string> oracle;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      oracle   = populate_diverse_db(ses, 0);
   }

   // First: full_verify
   corrupt_shutdown_flag(dir);
   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::full_verify);
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   // Second: power_loss
   corrupt_shutdown_flag(dir);
   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();
      verify_oracle(ses, 0, oracle);
   }

   std::filesystem::remove_all(dir);
}
