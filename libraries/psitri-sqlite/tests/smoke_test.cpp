// Minimal smoke test for psitri-backed SQLite
#include <sqlite3.h>
#include <cstdio>
#include <cstdlib>
#include <filesystem>

int main() {
   namespace fs = std::filesystem;
   auto tmp = fs::temp_directory_path() / "psitri_smoke_test";
   fs::remove_all(tmp);
   fs::create_directories(tmp);
   auto db_path = (tmp / "test.db").string();

   sqlite3* db = nullptr;
   std::printf("Opening database at %s...\n", db_path.c_str());
   int rc = sqlite3_open(db_path.c_str(), &db);
   std::printf("sqlite3_open returned %d\n", rc);
   if (rc != SQLITE_OK) {
      std::fprintf(stderr, "Cannot open: %s\n", sqlite3_errmsg(db));
      return 1;
   }

   std::printf("Creating table...\n");
   char* err = nullptr;
   rc = sqlite3_exec(db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT)", nullptr, nullptr, &err);
   std::printf("CREATE TABLE returned %d\n", rc);
   if (rc != SQLITE_OK) {
      std::fprintf(stderr, "Error: %s\n", err ? err : "unknown");
      sqlite3_free(err);
   }

   std::printf("Inserting row...\n");
   rc = sqlite3_exec(db, "INSERT INTO t1 VALUES(1, 'hello')", nullptr, nullptr, &err);
   std::printf("INSERT returned %d\n", rc);
   if (rc != SQLITE_OK) {
      std::fprintf(stderr, "Error: %s\n", err ? err : "unknown");
      sqlite3_free(err);
   }

   std::printf("Querying...\n");
   sqlite3_stmt* stmt = nullptr;
   rc = sqlite3_prepare_v2(db, "SELECT * FROM t1", -1, &stmt, nullptr);
   std::printf("PREPARE returned %d\n", rc);
   if (rc == SQLITE_OK) {
      while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
         std::printf("  row: id=%lld, name=%s\n",
            sqlite3_column_int64(stmt, 0),
            sqlite3_column_text(stmt, 1));
      }
      std::printf("STEP returned %d (SQLITE_DONE=%d)\n", rc, SQLITE_DONE);
      sqlite3_finalize(stmt);
   }

   sqlite3_close(db);
   db = nullptr;

   // Reopen and verify data persists
   std::printf("\n--- Reopening database ---\n");
   rc = sqlite3_open(db_path.c_str(), &db);
   if (rc != SQLITE_OK) {
      std::fprintf(stderr, "Reopen failed: %s\n", sqlite3_errmsg(db));
      return 1;
   }

   stmt = nullptr;
   rc = sqlite3_prepare_v2(db, "SELECT * FROM t1", -1, &stmt, nullptr);
   std::printf("PREPARE returned %d\n", rc);
   if (rc != SQLITE_OK) {
      std::fprintf(stderr, "Query after reopen failed: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return 1;
   }

   int rows = 0;
   while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      std::printf("  row: id=%lld, name=%s\n",
         sqlite3_column_int64(stmt, 0),
         sqlite3_column_text(stmt, 1));
      rows++;
   }
   sqlite3_finalize(stmt);

   if (rows != 1) {
      std::fprintf(stderr, "FAIL: expected 1 row after reopen, got %d\n", rows);
      sqlite3_close(db);
      return 1;
   }

   sqlite3_close(db);
   fs::remove_all(tmp);
   std::printf("SUCCESS\n");
   return 0;
}
