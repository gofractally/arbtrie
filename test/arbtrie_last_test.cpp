#include <arbtrie/database.hpp>
#include <arbtrie/iterator.hpp>
#include <arbtrie/transaction.hpp>
#include <catch2/catch_all.hpp>

#include <filesystem>
#include <iostream>
#include <string>

using namespace arbtrie;

// Test environment setup
struct TestEnv
{
   std::filesystem::path          db_path;
   database*                      db = nullptr;
   std::shared_ptr<write_session> ws = nullptr;

   TestEnv()
   {
      // Create a unique temporary directory for the test
      auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();
      db_path =
          std::filesystem::temp_directory_path() / ("arbtrie_test_" + std::to_string(timestamp));

      std::cout << "Creating database at " << db_path << std::endl;
      std::filesystem::create_directories(db_path);

      // Configure and open the database
      config cfg;
      cfg.run_compact_thread = false;  // Disable compaction for tests
      cfg.cache_on_read      = true;   // Enable cache

      // Create and open the database
      database::create(db_path, cfg);
      db = new database(db_path, cfg);
      ws = db->start_write_session();
   }

   ~TestEnv()
   {
      ws.reset();  // Use reset instead of delete for shared_ptr
      delete db;
      std::filesystem::remove_all(db_path);
   }
};

TEST_CASE("Last operation test", "[last]")
{
   TestEnv env;

   // Start a transaction
   auto tx = env.ws->start_transaction();

   // Insert some test keys
   std::cout << "Inserting test keys" << std::endl;
   tx.insert("key1", "value1");
   tx.insert("key2", "value2");
   tx.insert("key3", "value3");

   // Commit the inserts
   tx.commit_and_continue();

   SECTION("Test last() after insert")
   {
      std::cout << "Starting transaction and calling last()" << std::endl;
      tx.start();

      REQUIRE(tx.last());
      std::string key(tx.key().data(), tx.key().size());
      std::cout << "Last key: " << key << std::endl;
      REQUIRE(key == "key3");
   }

   SECTION("Test last() after commit_and_continue")
   {
      // Insert another key
      tx.insert("key4", "value4");

      // Commit and continue
      tx.commit_and_continue();

      // Start the transaction
      tx.start();

      // Call last()
      REQUIRE(tx.last());
      std::string key(tx.key().data(), tx.key().size());
      std::cout << "Last key after commit_and_continue: " << key << std::endl;
      REQUIRE(key == "key4");
   }

   SECTION("Test last() with prefix")
   {
      // Start the transaction
      tx.start();

      // Call last() with prefix
      REQUIRE(tx.last("key"));
      std::string key(tx.key().data(), tx.key().size());
      std::cout << "Last key with prefix 'key': " << key << std::endl;
      REQUIRE(key == "key3");

      // Call last() with prefix that doesn't exist
      REQUIRE_FALSE(tx.last("nonexistent"));
      REQUIRE_FALSE(tx.valid());
   }
}