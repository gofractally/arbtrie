// Minimal smoke test for psitri-backed SQLite
#include <sqlite3.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
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

   // ── Test 2: TEXT PRIMARY KEY with blob values (bank bench pattern) ──
   std::printf("\n--- Test 2: TEXT key + BLOB value readback ---\n");
   auto db2_path = (tmp / "test2.db").string();
   sqlite3* db2 = nullptr;
   rc = sqlite3_open(db2_path.c_str(), &db2);
   if (rc != SQLITE_OK) {
      std::fprintf(stderr, "open2 failed: %s\n", sqlite3_errmsg(db2));
      return 1;
   }
   sqlite3_exec(db2, "PRAGMA synchronous=OFF", 0, 0, &err);
   rc = sqlite3_exec(db2, "CREATE TABLE kv(k TEXT PRIMARY KEY, v BLOB)", 0, 0, &err);
   if (rc != SQLITE_OK) {
      std::fprintf(stderr, "create kv: %s\n", err);
      sqlite3_free(err);
      return 1;
   }

   // Insert within BEGIN/COMMIT, then read back within same transaction
   sqlite3_exec(db2, "BEGIN", 0, 0, 0);

   sqlite3_stmt* put2 = nullptr;
   sqlite3_prepare_v2(db2, "INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", -1, &put2, 0);
   uint64_t bal = 1000;
   sqlite3_bind_text(put2, 1, "alice", 5, SQLITE_STATIC);
   sqlite3_bind_blob(put2, 2, &bal, sizeof(bal), SQLITE_STATIC);
   rc = sqlite3_step(put2);
   std::printf("INSERT alice: step rc=%d\n", rc);
   sqlite3_finalize(put2);

   // Read back within same BEGIN
   sqlite3_stmt* get2 = nullptr;
   sqlite3_prepare_v2(db2, "SELECT v FROM kv WHERE k=?", -1, &get2, 0);
   sqlite3_bind_text(get2, 1, "alice", 5, SQLITE_STATIC);
   rc = sqlite3_step(get2);
   if (rc == SQLITE_ROW) {
      uint64_t readback = 0;
      const void* blob = sqlite3_column_blob(get2, 0);
      int blen = sqlite3_column_bytes(get2, 0);
      std::printf("  blob ptr=%p len=%d\n", blob, blen);
      if (blob && blen >= (int)sizeof(readback)) {
         std::memcpy(&readback, blob, sizeof(readback));
         std::printf("  READ OK: alice = %lu (expected 1000)\n", (unsigned long)readback);
         if (readback != 1000) {
            std::fprintf(stderr, "FAIL: wrong value\n");
            return 1;
         }
      } else {
         std::fprintf(stderr, "FAIL: blob null or too short\n");
         return 1;
      }
   } else {
      std::fprintf(stderr, "FAIL: SELECT returned rc=%d (expected SQLITE_ROW=%d)\n", rc, SQLITE_ROW);
      std::fprintf(stderr, "  errmsg: %s\n", sqlite3_errmsg(db2));
      sqlite3_finalize(get2);
      sqlite3_close(db2);
      return 1;
   }
   sqlite3_finalize(get2);

   sqlite3_exec(db2, "COMMIT", 0, 0, 0);

   // Test 2b: read after COMMIT (separate transaction)
   std::printf("  Test 2b: read in new transaction after commit...\n");
   sqlite3_exec(db2, "BEGIN", 0, 0, 0);
   sqlite3_stmt* get3 = nullptr;
   sqlite3_prepare_v2(db2, "SELECT v FROM kv WHERE k=?", -1, &get3, 0);
   sqlite3_bind_text(get3, 1, "alice", 5, SQLITE_STATIC);
   rc = sqlite3_step(get3);
   if (rc == SQLITE_ROW) {
      uint64_t rb2 = 0;
      std::memcpy(&rb2, sqlite3_column_blob(get3, 0), sizeof(rb2));
      std::printf("  READ OK: alice = %lu\n", (unsigned long)rb2);
   } else {
      std::fprintf(stderr, "  FAIL: read after commit rc=%d\n", rc);
      sqlite3_finalize(get3);
      sqlite3_close(db2);
      return 1;
   }
   sqlite3_finalize(get3);
   sqlite3_exec(db2, "COMMIT", 0, 0, 0);

   sqlite3_close(db2);

   fs::remove_all(tmp);
   std::printf("SUCCESS\n");
   return 0;
}
