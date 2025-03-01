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

TEST_CASE("Debug last() operation", "[last][debug]")
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

   SECTION("Test start() followed by last()")
   {
      std::cout << "Starting transaction and calling last()" << std::endl;

      // Print state before start
      std::cout << "Before start(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start() << std::endl;

      // Start the transaction
      tx.start();

      // Print state after start
      std::cout << "After start(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start() << std::endl;

      // Check if we can call first() after start
      std::cout << "Calling first() after start()" << std::endl;
      bool first_result = tx.first();
      std::cout << "first() returned: " << (first_result ? "true" : "false") << std::endl;

      if (first_result)
      {
         std::string key(tx.key().data(), tx.key().size());
         std::cout << "First key: " << key << std::endl;
      }

      // Start a new transaction to test last()
      tx.commit_and_continue();
      tx.start();

      // Print state before last
      std::cout << "Before last(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start() << std::endl;

      // Call last() and check result
      std::cout << "Calling last()" << std::endl;
      bool last_result = tx.last();

      // Print state after last
      std::cout << "After last(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start()
                << ", last_result=" << (last_result ? "true" : "false") << std::endl;

      if (last_result)
      {
         std::string key(tx.key().data(), tx.key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }

   SECTION("Test begin() followed by last()")
   {
      std::cout << "Testing begin() followed by last()" << std::endl;

      // Print state before begin
      std::cout << "Before begin(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start() << std::endl;

      // Call begin() and check result
      bool begin_result = tx.begin();

      // Print state after begin
      std::cout << "After begin(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start()
                << ", begin_result=" << (begin_result ? "true" : "false") << std::endl;

      if (begin_result)
      {
         std::string key(tx.key().data(), tx.key().size());
         std::cout << "First key from begin(): " << key << std::endl;
      }

      // Call last() and check result
      std::cout << "Calling last() after begin()" << std::endl;
      bool last_result = tx.last();

      // Print state after last
      std::cout << "After last(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start()
                << ", last_result=" << (last_result ? "true" : "false") << std::endl;

      if (last_result)
      {
         std::string key(tx.key().data(), tx.key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }

   SECTION("Test direct last() without start()")
   {
      std::cout << "Testing direct last() without start()" << std::endl;

      // Print state before last
      std::cout << "Before last(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start() << std::endl;

      // Call last() directly without start() and check result
      bool last_result = tx.last();

      // Print state after last
      std::cout << "After last(): is_valid=" << tx.valid() << ", is_end=" << tx.is_end()
                << ", is_start=" << tx.is_start()
                << ", last_result=" << (last_result ? "true" : "false") << std::endl;

      if (last_result)
      {
         std::string key(tx.key().data(), tx.key().size());
         std::cout << "Last key: " << key << std::endl;
      }
   }
}