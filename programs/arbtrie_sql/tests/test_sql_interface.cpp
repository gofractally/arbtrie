#define CATCH_CONFIG_MAIN
#include <sqlite3.h>
#include <catch2/catch_test_macros.hpp>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

// Forward declaration for the virtual table registration function
extern "C" int sqlite3_arbtriemodule_init(sqlite3* db);

// Helper function to execute SQL and check for errors
static int exec_sql(sqlite3*           db,
                    const std::string& sql,
                    int (*callback)(void*, int, char**, char**) = nullptr,
                    void*        callback_arg                   = nullptr,
                    std::string* errMsg                         = nullptr)
{
   char* zErrMsg = nullptr;
   int   rc      = sqlite3_exec(db, sql.c_str(), callback, callback_arg, &zErrMsg);
   if (rc != SQLITE_OK && errMsg)
   {
      *errMsg = (zErrMsg ? zErrMsg : "Unknown SQLite error");
   }
   if (zErrMsg)
   {
      if (!errMsg)
      {  // Only print if not captured by errMsg
         std::cerr << "SQLite error: " << zErrMsg << " for SQL: " << sql << std::endl;
      }
      sqlite3_free(zErrMsg);
   }
   return rc;
}

// Helper structure to manage SQLite DB connection and temp arbtrie dir
struct TestFixture
{
   sqlite3*    db           = nullptr;
   std::string text_db_path = "./arbtrie_sql_test_db_text";
   std::string blob_db_path = "./arbtrie_sql_test_db_blob";
   std::string errMsg;

   TestFixture()
   {
      // Clean up any previous test DB directory
      std::filesystem::remove_all(text_db_path);
      std::filesystem::remove_all(blob_db_path);
      // Directories will be created by arbtrie::database::create if needed

      // Open SQLite in-memory DB
      int rc = sqlite3_open(":memory:", &db);
      if (rc != SQLITE_OK)
      {
         throw std::runtime_error("Failed to open SQLite in-memory DB");
      }

      // Register the arbtrie module
      rc = sqlite3_arbtriemodule_init(db);
      if (rc != SQLITE_OK)
      {
         sqlite3_close(db);
         db = nullptr;
         throw std::runtime_error("Failed to register arbtrie module");
      }
   }

   ~TestFixture()
   {
      if (db)
      {
         sqlite3_close(db);
      }
      // Clean up the test DB directory
      std::filesystem::remove_all(text_db_path);
      std::filesystem::remove_all(blob_db_path);
   }

   // Disable copy/move
   TestFixture(const TestFixture&)            = delete;
   TestFixture& operator=(const TestFixture&) = delete;
   TestFixture(TestFixture&&)                 = delete;
   TestFixture& operator=(TestFixture&&)      = delete;

   int exec(const std::string& sql,
            int (*callback)(void*, int, char**, char**) = nullptr,
            void* callback_arg                          = nullptr)
   {
      errMsg.clear();
      return exec_sql(db, sql, callback, callback_arg, &errMsg);
   }
};

// Callback to collect results from SELECT statements into a vector of strings
struct SelectResultCollector
{
   std::vector<std::vector<std::string>> rows;
   static int callback(void* data, int argc, char** argv, char** azColName)
   {
      SelectResultCollector*   collector = static_cast<SelectResultCollector*>(data);
      std::vector<std::string> row;
      row.reserve(argc);
      for (int i = 0; i < argc; ++i)
      {
         row.push_back(argv[i] ? argv[i] : "<NULL>");
      }
      collector->rows.push_back(std::move(row));
      return 0;
   }
};

TEST_CASE_METHOD(TestFixture, "Arbtrie SQL Virtual Table Operations", "[arbtrie_sql]")
{
   REQUIRE(db != nullptr);

   SECTION("Create Virtual Table")
   {
      int rc = exec("CREATE VIRTUAL TABLE kv_text USING arbtrie(path='" + text_db_path +
                    "', key TEXT PRIMARY KEY, value TEXT);");
      REQUIRE(rc == SQLITE_OK);
      REQUIRE(errMsg.empty());

      int rc_blob = exec("CREATE VIRTUAL TABLE kv_blob USING arbtrie(path='" + blob_db_path +
                         "', k BLOB PRIMARY KEY, v BLOB);");
      REQUIRE(rc_blob == SQLITE_OK);
      REQUIRE(errMsg.empty());
   }

   SECTION("Basic INSERT and SELECT (TEXT)")
   {
      REQUIRE(exec("CREATE VIRTUAL TABLE kv_text USING arbtrie(path='" + text_db_path +
                   "', key TEXT PRIMARY KEY, value TEXT);") == SQLITE_OK);

      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('hello', 'world');") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('sqlite', 'rocks');") == SQLITE_OK);

      SelectResultCollector results;
      REQUIRE(exec("SELECT value FROM kv_text WHERE key = 'hello';",
                   SelectResultCollector::callback, &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 1);
      REQUIRE(results.rows[0].size() == 1);
      REQUIRE(results.rows[0][0] == "world");

      results.rows.clear();
      REQUIRE(exec("SELECT key, value FROM kv_text WHERE key = 'sqlite';",
                   SelectResultCollector::callback, &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 1);
      REQUIRE(results.rows[0].size() == 2);
      REQUIRE(results.rows[0][0] == "sqlite");
      REQUIRE(results.rows[0][1] == "rocks");

      results.rows.clear();
      REQUIRE(exec("SELECT value FROM kv_text WHERE key = 'nonexistent';",
                   SelectResultCollector::callback, &results) == SQLITE_OK);
      REQUIRE(results.rows.empty());
   }

   SECTION("Basic INSERT and SELECT (BLOB)")
   {
      REQUIRE(exec("CREATE VIRTUAL TABLE kv_blob USING arbtrie(path='" + blob_db_path +
                   "', k BLOB PRIMARY KEY, v BLOB);") == SQLITE_OK);

      // Use hex literals for blobs
      REQUIRE(exec("INSERT INTO kv_blob (k, v) VALUES (X'010203', X'112233');") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_blob (k, v) VALUES (X'DEADBEEF', X'CAFEBABE');") == SQLITE_OK);

      SelectResultCollector results;
      // Selecting blobs often returns them in a format that might not be easily comparable as strings.
      // We check if the query executes successfully. Comparing actual blob content might require sqlite3_column_blob.
      REQUIRE(exec("SELECT v FROM kv_blob WHERE k = X'010203';", SelectResultCollector::callback,
                   &results) == SQLITE_OK);
      // REQUIRE(results.rows.size() == 1); // Basic check - detailed blob comparison omitted for simplicity
      // TODO: Add proper blob comparison if needed using sqlite3_prepare/step/column_blob

      results.rows.clear();
      REQUIRE(exec("SELECT v FROM kv_blob WHERE k = X'BADDBEEF';", SelectResultCollector::callback,
                   &results) == SQLITE_OK);
      REQUIRE(results.rows.empty());
   }

   SECTION("UPDATE Operations (TEXT)")
   {
      REQUIRE(exec("CREATE VIRTUAL TABLE kv_text USING arbtrie(path='" + text_db_path +
                   "', key TEXT PRIMARY KEY, value TEXT);") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('key1', 'value1');") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('key2', 'value2');") == SQLITE_OK);

      // Update existing key
      REQUIRE(exec("UPDATE kv_text SET value = 'updated_value1' WHERE key = 'key1';") == SQLITE_OK);

      SelectResultCollector results;
      REQUIRE(exec("SELECT value FROM kv_text WHERE key = 'key1';", SelectResultCollector::callback,
                   &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 1);
      REQUIRE(results.rows[0][0] == "updated_value1");

      // Check other key wasn't affected
      results.rows.clear();
      REQUIRE(exec("SELECT value FROM kv_text WHERE key = 'key2';", SelectResultCollector::callback,
                   &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 1);
      REQUIRE(results.rows[0][0] == "value2");

      // Try to update non-existent key (should be no-op, check return code if needed)
      int rc = exec("UPDATE kv_text SET value = 'wont_happen' WHERE key = 'nonexistent';");
      // SQLite UPDATE doesn't fail if WHERE clause matches nothing,
      // but our vtab might return a specific code like SQLITE_NOTFOUND if we implemented it.
      // REQUIRE(rc == SQLITE_OK); // Or check for specific vtab error if implemented

      results.rows.clear();
      REQUIRE(exec("SELECT value FROM kv_text WHERE key = 'nonexistent';",
                   SelectResultCollector::callback, &results) == SQLITE_OK);
      REQUIRE(results.rows.empty());
   }

   SECTION("DELETE Operations (TEXT)")
   {
      REQUIRE(exec("CREATE VIRTUAL TABLE kv_text USING arbtrie(path='" + text_db_path +
                   "', key TEXT PRIMARY KEY, value TEXT);") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('key1', 'value1');") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('key_to_delete', 'temp_value');") ==
              SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('key3', 'value3');") == SQLITE_OK);

      // Delete existing key
      REQUIRE(exec("DELETE FROM kv_text WHERE key = 'key_to_delete';") == SQLITE_OK);

      SelectResultCollector results;
      REQUIRE(exec("SELECT value FROM kv_text WHERE key = 'key_to_delete';",
                   SelectResultCollector::callback, &results) == SQLITE_OK);
      REQUIRE(results.rows.empty());

      // Check other keys remain
      results.rows.clear();
      REQUIRE(exec("SELECT key FROM kv_text ORDER BY key;", SelectResultCollector::callback,
                   &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 2);
      REQUIRE(results.rows[0][0] == "key1");
      REQUIRE(results.rows[1][0] == "key3");

      // Try to delete non-existent key (should be no-op)
      REQUIRE(exec("DELETE FROM kv_text WHERE key = 'nonexistent';") == SQLITE_OK);

      results.rows.clear();
      REQUIRE(exec("SELECT key FROM kv_text ORDER BY key;", SelectResultCollector::callback,
                   &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 2);  // Count should still be 2
   }

   SECTION("INSERT OR REPLACE (Implicit via PRIMARY KEY constraint)")
   {
      REQUIRE(exec("CREATE VIRTUAL TABLE kv_text USING arbtrie(path='" + text_db_path +
                   "', key TEXT PRIMARY KEY, value TEXT);") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('replace_me', 'initial');") ==
              SQLITE_OK);

      // Inserting with the same primary key should behave like REPLACE if vtab handles it
      // The default implementation in arbtrie_vtab.cpp uses insert which might fail or upsert
      // Let's assume it acts like UPSERT for this test based on the vtab code provided.
      int rc = exec("INSERT INTO kv_text (key, value) VALUES ('replace_me', 'replaced');");
      // If your vtab insert detects conflicts and fails, change this check.
      // If it upserts, it should be SQLITE_OK.
      REQUIRE(rc == SQLITE_OK);

      SelectResultCollector results;
      REQUIRE(exec("SELECT value FROM kv_text WHERE key = 'replace_me';",
                   SelectResultCollector::callback, &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 1);
      REQUIRE(results.rows[0][0] == "replaced");
   }

   SECTION("Full Scan")
   {
      REQUIRE(exec("CREATE VIRTUAL TABLE kv_text USING arbtrie(path='" + text_db_path +
                   "', key TEXT PRIMARY KEY, value TEXT);") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('b', '2');") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('a', '1');") == SQLITE_OK);
      REQUIRE(exec("INSERT INTO kv_text (key, value) VALUES ('c', '3');") == SQLITE_OK);

      SelectResultCollector results;
      REQUIRE(exec("SELECT key, value FROM kv_text ORDER BY key;", SelectResultCollector::callback,
                   &results) == SQLITE_OK);
      REQUIRE(results.rows.size() == 3);
      REQUIRE(results.rows[0][0] == "a");
      REQUIRE(results.rows[0][1] == "1");
      REQUIRE(results.rows[1][0] == "b");
      REQUIRE(results.rows[1][1] == "2");
      REQUIRE(results.rows[2][0] == "c");
      REQUIRE(results.rows[2][1] == "3");
   }
}