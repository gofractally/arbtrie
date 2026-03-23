#include <catch2/catch_all.hpp>
#include <psitri/database.hpp>
#include <psitri/database_impl.hpp>
#include <psitri/transaction.hpp>
#include <psitri/write_session_impl.hpp>
#include <psitri/read_session_impl.hpp>

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
