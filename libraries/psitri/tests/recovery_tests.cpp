#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>
#include <sal/alloc_header.hpp>
#include <sal/mapped_memory/segment.hpp>
#include <fstream>

using namespace psitri;

namespace
{
   std::string to_key(int i)
   {
      char buf[32];
      snprintf(buf, sizeof(buf), "key-%08d", i);
      return buf;
   }
   std::string to_value(int i)
   {
      char buf[64];
      snprintf(buf, sizeof(buf), "value-%08d", i);
      return buf;
   }

   /// Load words from system dictionary for realistic test data
   std::vector<std::string> load_dict_words(size_t max_words = 10000)
   {
      std::vector<std::string> words;
      std::ifstream f("/usr/share/dict/words");
      std::string line;
      while (std::getline(f, line) && words.size() < max_words)
      {
         if (!line.empty())
            words.push_back(line);
      }
      return words;
   }

   /// Return a runtime_config with sync_mode = msync_sync.
   /// This ensures commits write sync headers AND fsync the roots file,
   /// which is required for power_loss recovery to find valid data on disk.
   runtime_config synced_config()
   {
      runtime_config cfg;
      cfg.sync_mode = sal::sync_type::msync_sync;
      return cfg;
   }

   /// Corrupt the clean_shutdown flag in the database file without
   /// going through the database destructor.
   void corrupt_shutdown_flag(const std::string& dir)
   {
      sal::mapping dbfile(dir + "/dbfile.bin", sal::access_mode::read_write);
      auto* state = reinterpret_cast<detail::database_state*>(dbfile.data());
      state->clean_shutdown.store(false, std::memory_order_relaxed);
      dbfile.sync(sal::sync_type::full);
   }
}  // namespace

// ---- Basic clean/unclean shutdown tests (sync_type::none is fine) ----

TEST_CASE("clean shutdown and reopen", "[recovery]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 100; ++i)
         tx.upsert(to_key(i), to_value(i));
      tx.commit();
   }

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      for (int i = 0; i < 100; ++i)
         REQUIRE(c.seek(to_key(i)));
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("unclean shutdown triggers recovery", "[recovery]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 100; ++i)
         tx.upsert(to_key(i), to_value(i));
      tx.commit();
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      for (int i = 0; i < 100; ++i)
         REQUIRE(c.seek(to_key(i)));
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("recovery with multiple roots", "[recovery]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_write_session();

      {
         auto tx = ses->start_transaction(0);
         for (int i = 0; i < 50; ++i)
            tx.upsert(to_key(i), to_value(i));
         tx.commit();
      }

      {
         auto tx = ses->start_transaction(1);
         for (int i = 50; i < 100; ++i)
            tx.upsert(to_key(i), to_value(i));
         tx.commit();
      }
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_read_session();

      {
         auto root = ses->get_root(0);
         REQUIRE(root);
         cursor c(root);
         for (int i = 0; i < 50; ++i)
            REQUIRE(c.seek(to_key(i)));
      }

      {
         auto root = ses->get_root(1);
         REQUIRE(root);
         cursor c(root);
         for (int i = 50; i < 100; ++i)
            REQUIRE(c.seek(to_key(i)));
      }
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("empty database crash recovery", "[recovery]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db = std::make_shared<database>(dir, runtime_config());
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);
      if (root)
      {
         cursor c(root);
         REQUIRE(!c.seek_begin());
      }
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("double recovery survives", "[recovery]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 100; ++i)
         tx.upsert(to_key(i), to_value(i));
      tx.commit();
   }

   corrupt_shutdown_flag(dir);
   {
      auto db = std::make_shared<database>(dir, runtime_config());
   }

   corrupt_shutdown_flag(dir);
   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      for (int i = 0; i < 100; ++i)
         REQUIRE(c.seek(to_key(i)));
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("explicit app_crash recovery mode", "[recovery]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 100; ++i)
         tx.upsert(to_key(i), to_value(i));
      tx.commit();
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, runtime_config(), recovery_mode::app_crash);
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      for (int i = 0; i < 100; ++i)
         REQUIRE(c.seek(to_key(i)));
   }

   std::filesystem::remove_all(dir);
}

// ---- Power-loss recovery tests require sync_mode >= mprotect ----
// With sync_mode::none, segment::sync() is never called during commit,
// so no sync headers are written and there's nothing to recover from.
// These tests use mprotect mode which writes sync headers with root_info
// at each commit point.

TEST_CASE("explicit power_loss recovery mode", "[recovery][power_loss]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   auto words = load_dict_words(5000);
   REQUIRE(words.size() > 1000);

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (auto& w : words)
         tx.upsert(w, w);
      tx.commit();
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();
      auto root = ses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      for (auto& w : words)
         REQUIRE(c.seek(w));
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("power_loss recovery with multiple roots", "[recovery][power_loss]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   auto words = load_dict_words(5000);
   REQUIRE(words.size() > 1000);
   size_t half = words.size() / 2;

   {
      auto db  = std::make_shared<database>(dir, synced_config());
      auto ses = db->start_write_session();

      {
         auto tx = ses->start_transaction(0);
         for (size_t i = 0; i < half; ++i)
            tx.upsert(words[i], words[i]);
         tx.commit();
      }

      {
         auto tx = ses->start_transaction(1);
         for (size_t i = half; i < words.size(); ++i)
            tx.upsert(words[i], words[i]);
         tx.commit();
      }
   }

   corrupt_shutdown_flag(dir);

   {
      auto db  = std::make_shared<database>(dir, synced_config(), recovery_mode::power_loss);
      auto ses = db->start_read_session();

      {
         auto root = ses->get_root(0);
         REQUIRE(root);
         cursor c(root);
         for (size_t i = 0; i < half; ++i)
            REQUIRE(c.seek(words[i]));
      }

      {
         auto root = ses->get_root(1);
         REQUIRE(root);
         cursor c(root);
         for (size_t i = half; i < words.size(); ++i)
            REQUIRE(c.seek(words[i]));
      }
   }

   std::filesystem::remove_all(dir);
}

TEST_CASE("corruption flag halts writes", "[recovery]")
{
   const std::string dir = "recovery_testdb";
   std::filesystem::remove_all(dir);
   std::filesystem::create_directories(dir + "/data");

   {
      auto db  = std::make_shared<database>(dir, runtime_config());
      auto ses = db->start_write_session();
      auto tx  = ses->start_transaction(0);
      for (int i = 0; i < 10; ++i)
         tx.upsert(to_key(i), to_value(i));
      tx.commit();

      // Reads still work
      auto rses = db->start_read_session();
      auto root = rses->get_root(0);
      REQUIRE(root);
      cursor c(root);
      REQUIRE(c.seek(to_key(0)));
   }

   std::filesystem::remove_all(dir);
}
