#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include "duckdb.hpp"
#include <psitri-sql/psitri_sql.hpp>
#include <psitri-sql/row_encoding.hpp>

#include <filesystem>
#include <string>

namespace fs = std::filesystem;

// Helper: run a query and return the materialized result
static duckdb::unique_ptr<duckdb::MaterializedQueryResult>
run(duckdb::Connection& conn, const std::string& sql) {
   auto result = conn.Query(sql);
   if (result->HasError()) {
      FAIL("SQL error: " << result->GetError() << "\n  query: " << sql);
   }
   return result;
}

// Helper: create a temp directory that cleans up on destruction
struct TempDir {
   fs::path path;
   TempDir() : path(fs::temp_directory_path() / ("psitri_sql_test_" + std::to_string(std::rand()))) {
      fs::create_directories(path);
   }
   ~TempDir() { fs::remove_all(path); }
   std::string str() const { return path.string(); }
};

// Helper: set up a DuckDB + psitri instance with an attached database
struct TestDB {
   TempDir                          tmp;
   duckdb::DuckDB                   db;
   duckdb::Connection               conn;

   TestDB() : db(make_db()), conn(db) {
      run(conn, "ATTACH '" + tmp.str() + "/test.db' AS tdb (TYPE psitri)");
   }

private:
   static duckdb::DuckDB make_db() {
      duckdb::DBConfig config;
      config.options.autoload_known_extensions = false;
      config.options.autoinstall_known_extensions = false;
      duckdb::DuckDB db(nullptr, &config);
      psitri_sql::RegisterPsitriStorage(db);
      return db;
   }
};

TEST_CASE("CREATE TABLE and basic INSERT/SELECT", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.users (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)");

   run(conn, "INSERT INTO tdb.main.users VALUES (1, 'Alice', 30)");
   run(conn, "INSERT INTO tdb.main.users VALUES (2, 'Bob', 25)");
   run(conn, "INSERT INTO tdb.main.users VALUES (3, 'Charlie', 35)");

   auto result = run(conn, "SELECT id, name, age FROM tdb.main.users ORDER BY id");
   REQUIRE(result->RowCount() == 3);

   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(1, 0).ToString() == "Alice");
   CHECK(result->GetValue(2, 0).GetValue<int32_t>() == 30);

   CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 2);
   CHECK(result->GetValue(1, 1).ToString() == "Bob");
   CHECK(result->GetValue(2, 1).GetValue<int32_t>() == 25);

   CHECK(result->GetValue(0, 2).GetValue<int32_t>() == 3);
   CHECK(result->GetValue(1, 2).ToString() == "Charlie");
   CHECK(result->GetValue(2, 2).GetValue<int32_t>() == 35);
}

TEST_CASE("SELECT with WHERE filter", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.items (id INTEGER PRIMARY KEY, label VARCHAR)");
   run(conn, "INSERT INTO tdb.main.items VALUES (10, 'apple'), (20, 'banana'), (30, 'cherry')");

   auto result = run(conn, "SELECT label FROM tdb.main.items WHERE id = 20");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).ToString() == "banana");
}

TEST_CASE("SELECT COUNT and aggregates", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.nums (id INTEGER PRIMARY KEY, val DOUBLE)");
   run(conn, "INSERT INTO tdb.main.nums VALUES (1, 10.5), (2, 20.0), (3, 30.5)");

   auto result = run(conn, "SELECT COUNT(*), SUM(val), AVG(val) FROM tdb.main.nums");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).GetValue<int64_t>() == 3);
   CHECK(result->GetValue(1, 0).GetValue<double>() == Catch::Approx(61.0));
   CHECK(result->GetValue(2, 0).GetValue<double>() == Catch::Approx(61.0 / 3.0));
}

TEST_CASE("Multiple data types", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.types ("
             "  id INTEGER PRIMARY KEY,"
             "  flag BOOLEAN,"
             "  big BIGINT,"
             "  val DOUBLE,"
             "  txt VARCHAR"
             ")");
   run(conn, "INSERT INTO tdb.main.types VALUES (1, true, 9223372036854775807, 3.14, 'hello')");
   run(conn, "INSERT INTO tdb.main.types VALUES (2, false, -100, -2.718, 'world')");

   auto result = run(conn, "SELECT * FROM tdb.main.types ORDER BY id");
   REQUIRE(result->RowCount() == 2);

   // Row 0
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(1, 0).GetValue<bool>() == true);
   CHECK(result->GetValue(2, 0).GetValue<int64_t>() == 9223372036854775807LL);
   CHECK(result->GetValue(3, 0).GetValue<double>() == Catch::Approx(3.14));
   CHECK(result->GetValue(4, 0).ToString() == "hello");

   // Row 1
   CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 2);
   CHECK(result->GetValue(1, 1).GetValue<bool>() == false);
   CHECK(result->GetValue(2, 1).GetValue<int64_t>() == -100);
   CHECK(result->GetValue(3, 1).GetValue<double>() == Catch::Approx(-2.718));
   CHECK(result->GetValue(4, 1).ToString() == "world");
}

TEST_CASE("Empty table returns no rows", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.empty (id INTEGER PRIMARY KEY)");
   auto result = run(conn, "SELECT * FROM tdb.main.empty");
   CHECK(result->RowCount() == 0);
}

TEST_CASE("Multiple tables in same database", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.t1 (id INTEGER PRIMARY KEY, x INTEGER)");
   run(conn, "CREATE TABLE tdb.main.t2 (id INTEGER PRIMARY KEY, y VARCHAR)");

   run(conn, "INSERT INTO tdb.main.t1 VALUES (1, 100), (2, 200)");
   run(conn, "INSERT INTO tdb.main.t2 VALUES (1, 'foo'), (2, 'bar')");

   auto r1 = run(conn, "SELECT COUNT(*) FROM tdb.main.t1");
   auto r2 = run(conn, "SELECT COUNT(*) FROM tdb.main.t2");
   CHECK(r1->GetValue(0, 0).GetValue<int64_t>() == 2);
   CHECK(r2->GetValue(0, 0).GetValue<int64_t>() == 2);
}

TEST_CASE("Large batch insert", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.batch (id INTEGER PRIMARY KEY, data VARCHAR)");

   // Insert 1000 rows
   for (int i = 0; i < 1000; i += 100) {
      std::string sql = "INSERT INTO tdb.main.batch VALUES ";
      for (int j = i; j < i + 100; ++j) {
         if (j > i) sql += ", ";
         sql += "(" + std::to_string(j) + ", 'row_" + std::to_string(j) + "')";
      }
      run(conn, sql);
   }

   auto result = run(conn, "SELECT COUNT(*) FROM tdb.main.batch");
   CHECK(result->GetValue(0, 0).GetValue<int64_t>() == 1000);

   // Verify ordering
   auto ordered = run(conn, "SELECT id FROM tdb.main.batch ORDER BY id LIMIT 5");
   CHECK(ordered->GetValue(0, 0).GetValue<int32_t>() == 0);
   CHECK(ordered->GetValue(0, 1).GetValue<int32_t>() == 1);
   CHECK(ordered->GetValue(0, 2).GetValue<int32_t>() == 2);
   CHECK(ordered->GetValue(0, 3).GetValue<int32_t>() == 3);
   CHECK(ordered->GetValue(0, 4).GetValue<int32_t>() == 4);
}

TEST_CASE("Negative and zero integer keys", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.signed_keys (id INTEGER PRIMARY KEY, val VARCHAR)");
   run(conn, "INSERT INTO tdb.main.signed_keys VALUES (-100, 'neg'), (0, 'zero'), (100, 'pos')");

   auto result = run(conn, "SELECT id, val FROM tdb.main.signed_keys ORDER BY id");
   REQUIRE(result->RowCount() == 3);

   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == -100);
   CHECK(result->GetValue(1, 0).ToString() == "neg");

   CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 0);
   CHECK(result->GetValue(1, 1).ToString() == "zero");

   CHECK(result->GetValue(0, 2).GetValue<int32_t>() == 100);
   CHECK(result->GetValue(1, 2).ToString() == "pos");
}

TEST_CASE("VARCHAR primary key", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.str_pk (name VARCHAR PRIMARY KEY, score INTEGER)");
   run(conn, "INSERT INTO tdb.main.str_pk VALUES ('alice', 95), ('bob', 87), ('charlie', 91)");

   auto result = run(conn, "SELECT name, score FROM tdb.main.str_pk ORDER BY name");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(0, 0).ToString() == "alice");
   CHECK(result->GetValue(0, 1).ToString() == "bob");
   CHECK(result->GetValue(0, 2).ToString() == "charlie");
}

// ===========================================================================
// DELETE tests
// ===========================================================================

TEST_CASE("DELETE single row", "[psitri-sql][delete]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.d1 (id INTEGER PRIMARY KEY, val VARCHAR)");
   run(conn, "INSERT INTO tdb.main.d1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");

   run(conn, "DELETE FROM tdb.main.d1 WHERE id = 2");
   auto result = run(conn, "SELECT id, val FROM tdb.main.d1 ORDER BY id");
   REQUIRE(result->RowCount() == 2);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 3);
}

TEST_CASE("DELETE all rows", "[psitri-sql][delete]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.d2 (id INTEGER PRIMARY KEY)");
   run(conn, "INSERT INTO tdb.main.d2 VALUES (1), (2), (3)");
   run(conn, "DELETE FROM tdb.main.d2");

   auto result = run(conn, "SELECT COUNT(*) FROM tdb.main.d2");
   CHECK(result->GetValue(0, 0).GetValue<int64_t>() == 0);
}

TEST_CASE("DELETE with complex WHERE", "[psitri-sql][delete]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.d3 (id INTEGER PRIMARY KEY, score INTEGER)");
   run(conn, "INSERT INTO tdb.main.d3 VALUES (1, 10), (2, 50), (3, 30), (4, 80), (5, 20)");
   run(conn, "DELETE FROM tdb.main.d3 WHERE score > 25");

   auto result = run(conn, "SELECT id FROM tdb.main.d3 ORDER BY id");
   REQUIRE(result->RowCount() == 2);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 5);
}

// ===========================================================================
// UPDATE tests
// ===========================================================================

TEST_CASE("UPDATE single column", "[psitri-sql][update]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.u1 (id INTEGER PRIMARY KEY, name VARCHAR, score INTEGER)");
   run(conn, "INSERT INTO tdb.main.u1 VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Charlie', 70)");

   run(conn, "UPDATE tdb.main.u1 SET score = 100 WHERE id = 2");
   auto result = run(conn, "SELECT score FROM tdb.main.u1 WHERE id = 2");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 100);
}

TEST_CASE("UPDATE all rows", "[psitri-sql][update]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.u2 (id INTEGER PRIMARY KEY, val INTEGER)");
   run(conn, "INSERT INTO tdb.main.u2 VALUES (1, 10), (2, 20), (3, 30)");

   run(conn, "UPDATE tdb.main.u2 SET val = val * 2");
   auto result = run(conn, "SELECT id, val FROM tdb.main.u2 ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(1, 0).GetValue<int32_t>() == 20);
   CHECK(result->GetValue(1, 1).GetValue<int32_t>() == 40);
   CHECK(result->GetValue(1, 2).GetValue<int32_t>() == 60);
}

// ===========================================================================
// DROP TABLE tests
// ===========================================================================

TEST_CASE("DROP TABLE", "[psitri-sql][drop]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.dropme (id INTEGER PRIMARY KEY)");
   run(conn, "INSERT INTO tdb.main.dropme VALUES (1)");
   run(conn, "DROP TABLE tdb.main.dropme");

   // Table should no longer exist
   auto result = conn.Query("SELECT * FROM tdb.main.dropme");
   CHECK(result->HasError());
}

// ===========================================================================
// Persistence tests
// ===========================================================================

TEST_CASE("Tables persist across re-attach", "[psitri-sql][persistence]") {
   TempDir tmp;
   std::string db_path = tmp.str() + "/persist.db";

   // Session 1: create and populate
   {
      duckdb::DBConfig config;
      config.options.autoload_known_extensions = false;
      config.options.autoinstall_known_extensions = false;
      duckdb::DuckDB db(nullptr, &config);
      psitri_sql::RegisterPsitriStorage(db);
      duckdb::Connection conn(db);

      run(conn, "ATTACH '" + db_path + "' AS pdb (TYPE psitri)");
      run(conn, "CREATE TABLE pdb.main.persist_test (id INTEGER PRIMARY KEY, name VARCHAR)");
      run(conn, "INSERT INTO pdb.main.persist_test VALUES (1, 'hello'), (2, 'world')");
   }

   // Session 2: re-attach and verify data survives
   {
      duckdb::DBConfig config;
      config.options.autoload_known_extensions = false;
      config.options.autoinstall_known_extensions = false;
      duckdb::DuckDB db(nullptr, &config);
      psitri_sql::RegisterPsitriStorage(db);
      duckdb::Connection conn(db);

      run(conn, "ATTACH '" + db_path + "' AS pdb (TYPE psitri)");
      auto result = run(conn, "SELECT id, name FROM pdb.main.persist_test ORDER BY id");
      REQUIRE(result->RowCount() == 2);
      CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
      CHECK(result->GetValue(1, 0).ToString() == "hello");
      CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 2);
      CHECK(result->GetValue(1, 1).ToString() == "world");
   }
}

// ===========================================================================
// NULL handling tests
// ===========================================================================

TEST_CASE("INSERT and SELECT with NULLs", "[psitri-sql][null]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.nulls (id INTEGER PRIMARY KEY, a VARCHAR, b INTEGER)");
   run(conn, "INSERT INTO tdb.main.nulls VALUES (1, NULL, 10)");
   run(conn, "INSERT INTO tdb.main.nulls VALUES (2, 'hello', NULL)");
   run(conn, "INSERT INTO tdb.main.nulls VALUES (3, NULL, NULL)");

   auto result = run(conn, "SELECT id, a, b FROM tdb.main.nulls ORDER BY id");
   REQUIRE(result->RowCount() == 3);

   CHECK(result->GetValue(1, 0).IsNull());
   CHECK(result->GetValue(2, 0).GetValue<int32_t>() == 10);

   CHECK(result->GetValue(1, 1).ToString() == "hello");
   CHECK(result->GetValue(2, 1).IsNull());

   CHECK(result->GetValue(1, 2).IsNull());
   CHECK(result->GetValue(2, 2).IsNull());
}

// ===========================================================================
// ALTER TABLE tests
// ===========================================================================

TEST_CASE("ALTER TABLE RENAME", "[psitri-sql][alter]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.old_name (id INTEGER PRIMARY KEY, val INTEGER)");
   run(conn, "INSERT INTO tdb.main.old_name VALUES (1, 42)");
   run(conn, "ALTER TABLE tdb.main.old_name RENAME TO new_name");

   auto result = run(conn, "SELECT val FROM tdb.main.new_name WHERE id = 1");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 42);
}

TEST_CASE("ALTER TABLE ADD COLUMN", "[psitri-sql][alter]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.addcol (id INTEGER PRIMARY KEY, x INTEGER)");
   run(conn, "INSERT INTO tdb.main.addcol VALUES (1, 10)");
   run(conn, "ALTER TABLE tdb.main.addcol ADD COLUMN y VARCHAR");

   // New column should be NULL for existing rows
   auto result = run(conn, "SELECT id, x, y FROM tdb.main.addcol");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(1, 0).GetValue<int32_t>() == 10);
   // y was added after the row was inserted, so it will be null or default
}

TEST_CASE("ALTER TABLE RENAME COLUMN", "[psitri-sql][alter]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.rencol (id INTEGER PRIMARY KEY, old_col INTEGER)");
   run(conn, "INSERT INTO tdb.main.rencol VALUES (1, 99)");
   run(conn, "ALTER TABLE tdb.main.rencol RENAME COLUMN old_col TO new_col");

   auto result = run(conn, "SELECT new_col FROM tdb.main.rencol WHERE id = 1");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 99);
}

// ===========================================================================
// Upsert / duplicate key behavior
// ===========================================================================

TEST_CASE("INSERT overwrites duplicate keys (upsert behavior)", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.upsert (id INTEGER PRIMARY KEY, val VARCHAR)");
   run(conn, "INSERT INTO tdb.main.upsert VALUES (1, 'first')");
   run(conn, "INSERT INTO tdb.main.upsert VALUES (1, 'second')");

   auto result = run(conn, "SELECT val FROM tdb.main.upsert WHERE id = 1");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).ToString() == "second");
}

// ===========================================================================
// Filter pushdown verification
// ===========================================================================

TEST_CASE("Filter pushdown on PK equality", "[psitri-sql][filter]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.fp (id INTEGER PRIMARY KEY, data VARCHAR)");
   for (int i = 0; i < 100; i++) {
      run(conn, "INSERT INTO tdb.main.fp VALUES (" + std::to_string(i) +
                ", 'val_" + std::to_string(i) + "')");
   }

   // This should use cursor seek, not full scan
   auto result = run(conn, "SELECT data FROM tdb.main.fp WHERE id = 50");
   REQUIRE(result->RowCount() == 1);
   CHECK(result->GetValue(0, 0).ToString() == "val_50");

   // Non-existent key
   result = run(conn, "SELECT data FROM tdb.main.fp WHERE id = 999");
   CHECK(result->RowCount() == 0);
}

// ===========================================================================
// CREATE TABLE AS SELECT
// ===========================================================================

TEST_CASE("CREATE TABLE AS SELECT", "[psitri-sql][ctas]") {
   TestDB t;
   auto& conn = t.conn;

   // Create source table in DuckDB's in-memory catalog
   run(conn, "CREATE TABLE src (id INTEGER, name VARCHAR)");
   run(conn, "INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

   // CREATE TABLE AS into psitri
   run(conn, "CREATE TABLE tdb.main.dest AS SELECT * FROM src");

   auto result = run(conn, "SELECT id, name FROM tdb.main.dest ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(1, 0).ToString() == "Alice");
   CHECK(result->GetValue(1, 2).ToString() == "Charlie");
}

// ===========================================================================
// Row encoding round-trip tests
// ===========================================================================

TEST_CASE("Row encoding round-trip", "[psitri-sql][row-encoding]") {
   using namespace psitri_sql;

   SECTION("integer key encoding preserves order") {
      auto neg = encode_key({ColumnValue::make_int(SqlType::INTEGER, -1)});
      auto zero = encode_key({ColumnValue::make_int(SqlType::INTEGER, 0)});
      auto pos = encode_key({ColumnValue::make_int(SqlType::INTEGER, 1)});
      CHECK(neg < zero);
      CHECK(zero < pos);
   }

   SECTION("string key encoding preserves order") {
      auto a = encode_key({ColumnValue::make_varchar("apple")});
      auto b = encode_key({ColumnValue::make_varchar("banana")});
      auto c = encode_key({ColumnValue::make_varchar("cherry")});
      CHECK(a < b);
      CHECK(b < c);
   }

   SECTION("composite key round-trip") {
      std::vector<ColumnValue> cols = {
         ColumnValue::make_int(SqlType::INTEGER, 42),
         ColumnValue::make_varchar("hello"),
      };
      auto encoded = encode_key(cols);
      auto decoded = decode_key(encoded, {SqlType::INTEGER, SqlType::VARCHAR});
      REQUIRE(decoded.size() == 2);
      CHECK(decoded[0].i64 == 42);
      CHECK(decoded[1].str == "hello");
   }

   SECTION("value encoding round-trip") {
      std::vector<ColumnValue> cols = {
         ColumnValue::make_varchar("test"),
         ColumnValue::make_int(SqlType::BIGINT, 12345),
         ColumnValue::make_double(3.14),
         ColumnValue::make_bool(true),
      };
      auto encoded = encode_value(cols);
      auto decoded = decode_value(encoded, {SqlType::VARCHAR, SqlType::BIGINT, SqlType::DOUBLE, SqlType::BOOLEAN});
      REQUIRE(decoded.size() == 4);
      CHECK(decoded[0].str == "test");
      CHECK(decoded[1].i64 == 12345);
      CHECK(decoded[2].f64 == Catch::Approx(3.14));
      CHECK(decoded[3].i64 == 1);
   }

   SECTION("TableMeta serialization round-trip") {
      TableMeta meta;
      meta.table_name = "test_table";
      meta.schema_name = "main";
      meta.root_index = 7;
      meta.columns = {
         {"id", SqlType::INTEGER, true},
         {"name", SqlType::VARCHAR, false},
         {"score", SqlType::DOUBLE, false},
      };

      auto serialized = serialize_table_meta(meta);
      auto decoded = deserialize_table_meta(serialized);

      CHECK(decoded.table_name == "test_table");
      CHECK(decoded.schema_name == "main");
      CHECK(decoded.root_index == 7);
      REQUIRE(decoded.columns.size() == 3);
      CHECK(decoded.columns[0].name == "id");
      CHECK(decoded.columns[0].type == SqlType::INTEGER);
      CHECK(decoded.columns[0].is_primary_key == true);
      CHECK(decoded.columns[1].name == "name");
      CHECK(decoded.columns[1].type == SqlType::VARCHAR);
      CHECK(decoded.columns[1].is_primary_key == false);
   }

   SECTION("TableMeta with indexes and NOT NULL round-trip") {
      TableMeta meta;
      meta.table_name = "idx_table";
      meta.schema_name = "main";
      meta.root_index = 3;
      meta.columns = {
         {"id", SqlType::INTEGER, true, true},
         {"email", SqlType::VARCHAR, false, true},
         {"score", SqlType::INTEGER, false, false},
      };
      meta.indexes = {
         {"idx_email", 5, {1}, true},
         {"idx_score", 6, {2}, false},
      };

      auto serialized = serialize_table_meta(meta);
      auto decoded = deserialize_table_meta(serialized);

      REQUIRE(decoded.columns.size() == 3);
      CHECK(decoded.columns[0].not_null == true);
      CHECK(decoded.columns[1].not_null == true);
      CHECK(decoded.columns[2].not_null == false);

      REQUIRE(decoded.indexes.size() == 2);
      CHECK(decoded.indexes[0].name == "idx_email");
      CHECK(decoded.indexes[0].root_index == 5);
      CHECK(decoded.indexes[0].is_unique == true);
      REQUIRE(decoded.indexes[0].column_indices.size() == 1);
      CHECK(decoded.indexes[0].column_indices[0] == 1);
      CHECK(decoded.indexes[1].name == "idx_score");
      CHECK(decoded.indexes[1].is_unique == false);
   }

   SECTION("new type encoding round-trips") {
      // Unsigned integers
      auto u8_enc = encode_key({ColumnValue::make_uint(SqlType::UTINYINT, 200)});
      auto u8_dec = decode_key(u8_enc, {SqlType::UTINYINT});
      CHECK(static_cast<uint8_t>(u8_dec[0].i64) == 200);

      auto u16_enc = encode_key({ColumnValue::make_uint(SqlType::USMALLINT, 60000)});
      auto u16_dec = decode_key(u16_enc, {SqlType::USMALLINT});
      CHECK(static_cast<uint16_t>(u16_dec[0].i64) == 60000);

      auto u32_enc = encode_key({ColumnValue::make_uint(SqlType::UINTEGER, 3000000000UL)});
      auto u32_dec = decode_key(u32_enc, {SqlType::UINTEGER});
      CHECK(static_cast<uint32_t>(u32_dec[0].i64) == 3000000000UL);

      auto u64_enc = encode_key({ColumnValue::make_uint(SqlType::UBIGINT, 10000000000ULL)});
      auto u64_dec = decode_key(u64_enc, {SqlType::UBIGINT});
      CHECK(static_cast<uint64_t>(u64_dec[0].i64) == 10000000000ULL);

      // DATE
      auto date_enc = encode_key({ColumnValue::make_date(19000)});
      auto date_dec = decode_key(date_enc, {SqlType::DATE});
      CHECK(date_dec[0].i64 == 19000);

      // TIMESTAMP
      auto ts_enc = encode_key({ColumnValue::make_timestamp(1700000000000000LL)});
      auto ts_dec = decode_key(ts_enc, {SqlType::TIMESTAMP});
      CHECK(ts_dec[0].i64 == 1700000000000000LL);

      // HUGEINT
      auto hi_enc = encode_key({ColumnValue::make_hugeint(42, 999)});
      auto hi_dec = decode_key(hi_enc, {SqlType::HUGEINT});
      CHECK(hi_dec[0].i64 == 42);
      CHECK(hi_dec[0].u64_low == 999);
   }

   SECTION("unsigned key encoding preserves order") {
      auto a = encode_key({ColumnValue::make_uint(SqlType::UINTEGER, 100)});
      auto b = encode_key({ColumnValue::make_uint(SqlType::UINTEGER, 200)});
      auto c = encode_key({ColumnValue::make_uint(SqlType::UINTEGER, 3000000000UL)});
      CHECK(a < b);
      CHECK(b < c);
   }

   SECTION("NULL sorting in keys") {
      auto null_key = encode_key({ColumnValue::null_value(SqlType::INTEGER)});
      auto neg_key  = encode_key({ColumnValue::make_int(SqlType::INTEGER, -1000)});
      auto zero_key = encode_key({ColumnValue::make_int(SqlType::INTEGER, 0)});
      CHECK(null_key < neg_key);
      CHECK(neg_key < zero_key);
   }

   SECTION("value encoding with NULLs") {
      std::vector<ColumnValue> cols = {
         ColumnValue::null_value(SqlType::VARCHAR),
         ColumnValue::make_int(SqlType::INTEGER, 42),
         ColumnValue::null_value(SqlType::DOUBLE),
      };
      auto encoded = encode_value(cols);
      auto decoded = decode_value(encoded, {SqlType::VARCHAR, SqlType::INTEGER, SqlType::DOUBLE});
      REQUIRE(decoded.size() == 3);
      CHECK(decoded[0].is_null);
      CHECK(decoded[1].i64 == 42);
      CHECK(decoded[2].is_null);
   }
}

// ===========================================================================
// DATE / TIME / TIMESTAMP type tests
// ===========================================================================

TEST_CASE("DATE type support", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.dates (id INTEGER PRIMARY KEY, d DATE)");
   run(conn, "INSERT INTO tdb.main.dates VALUES (1, '2024-01-15'), (2, '2023-06-30'), (3, '2025-12-31')");

   auto result = run(conn, "SELECT id, d FROM tdb.main.dates ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(1, 0).ToString() == "2024-01-15");
   CHECK(result->GetValue(1, 1).ToString() == "2023-06-30");
   CHECK(result->GetValue(1, 2).ToString() == "2025-12-31");
}

TEST_CASE("TIMESTAMP type support", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.ts (id INTEGER PRIMARY KEY, t TIMESTAMP)");
   run(conn, "INSERT INTO tdb.main.ts VALUES (1, '2024-01-15 10:30:00'), (2, '2023-06-30 23:59:59')");

   auto result = run(conn, "SELECT id, t FROM tdb.main.ts ORDER BY id");
   REQUIRE(result->RowCount() == 2);
   CHECK(result->GetValue(1, 0).ToString() == "2024-01-15 10:30:00");
   CHECK(result->GetValue(1, 1).ToString() == "2023-06-30 23:59:59");
}

TEST_CASE("TIME type support", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.times (id INTEGER PRIMARY KEY, t TIME)");
   run(conn, "INSERT INTO tdb.main.times VALUES (1, '10:30:00'), (2, '23:59:59'), (3, '00:00:00')");

   auto result = run(conn, "SELECT id, t FROM tdb.main.times ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(1, 0).ToString() == "10:30:00");
   CHECK(result->GetValue(1, 1).ToString() == "23:59:59");
   CHECK(result->GetValue(1, 2).ToString() == "00:00:00");
}

TEST_CASE("Unsigned integer types", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.unsig (id INTEGER PRIMARY KEY, "
             "u8 UTINYINT, u16 USMALLINT, u32 UINTEGER, u64 UBIGINT)");
   run(conn, "INSERT INTO tdb.main.unsig VALUES (1, 255, 65535, 4294967295, 18446744073709551615)");
   run(conn, "INSERT INTO tdb.main.unsig VALUES (2, 0, 0, 0, 0)");

   auto result = run(conn, "SELECT u8, u16, u32, u64 FROM tdb.main.unsig ORDER BY id");
   REQUIRE(result->RowCount() == 2);
   CHECK(result->GetValue(0, 0).GetValue<uint8_t>() == 255);
   CHECK(result->GetValue(1, 0).GetValue<uint16_t>() == 65535);
   CHECK(result->GetValue(2, 0).GetValue<uint32_t>() == 4294967295UL);
   CHECK(result->GetValue(3, 0).GetValue<uint64_t>() == 18446744073709551615ULL);

   CHECK(result->GetValue(0, 1).GetValue<uint8_t>() == 0);
   CHECK(result->GetValue(1, 1).GetValue<uint16_t>() == 0);
   CHECK(result->GetValue(2, 1).GetValue<uint32_t>() == 0);
   CHECK(result->GetValue(3, 1).GetValue<uint64_t>() == 0);
}

TEST_CASE("TINYINT and SMALLINT types", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.smint (id INTEGER PRIMARY KEY, t TINYINT, s SMALLINT)");
   run(conn, "INSERT INTO tdb.main.smint VALUES (1, -128, -32768), (2, 127, 32767), (3, 0, 0)");

   auto result = run(conn, "SELECT t, s FROM tdb.main.smint ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(0, 0).GetValue<int8_t>() == -128);
   CHECK(result->GetValue(1, 0).GetValue<int16_t>() == -32768);
   CHECK(result->GetValue(0, 1).GetValue<int8_t>() == 127);
   CHECK(result->GetValue(1, 1).GetValue<int16_t>() == 32767);
}

TEST_CASE("FLOAT type support", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.fl (id INTEGER PRIMARY KEY, f FLOAT)");
   run(conn, "INSERT INTO tdb.main.fl VALUES (1, 3.14), (2, -2.5), (3, 0.0)");

   auto result = run(conn, "SELECT f FROM tdb.main.fl ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(0, 0).GetValue<float>() == Catch::Approx(3.14f).margin(0.01f));
   CHECK(result->GetValue(0, 1).GetValue<float>() == Catch::Approx(-2.5f));
   CHECK(result->GetValue(0, 2).GetValue<float>() == Catch::Approx(0.0f));
}

TEST_CASE("BLOB type support", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.bl (id INTEGER PRIMARY KEY, b BLOB)");
   run(conn, "INSERT INTO tdb.main.bl VALUES (1, '\\x0102030405'::BLOB)");

   auto result = run(conn, "SELECT b FROM tdb.main.bl WHERE id = 1");
   REQUIRE(result->RowCount() == 1);
   // BLOB stored and retrieved — just verify non-null
   CHECK(!result->GetValue(0, 0).IsNull());
}

// ===========================================================================
// NOT NULL constraint tests
// ===========================================================================

TEST_CASE("NOT NULL constraint on insert", "[psitri-sql][constraints]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.nn (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, score INTEGER)");

   // Valid insert
   run(conn, "INSERT INTO tdb.main.nn VALUES (1, 'Alice', 90)");

   // NULL in NOT NULL column should fail
   auto result = conn.Query("INSERT INTO tdb.main.nn VALUES (2, NULL, 80)");
   CHECK(result->HasError());

   // NULL in nullable column is OK
   run(conn, "INSERT INTO tdb.main.nn VALUES (3, 'Charlie', NULL)");

   auto check = run(conn, "SELECT COUNT(*) FROM tdb.main.nn");
   CHECK(check->GetValue(0, 0).GetValue<int64_t>() == 2);
}

TEST_CASE("NOT NULL constraint on update", "[psitri-sql][constraints]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.nn2 (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)");
   run(conn, "INSERT INTO tdb.main.nn2 VALUES (1, 'Alice')");

   auto result = conn.Query("UPDATE tdb.main.nn2 SET name = NULL WHERE id = 1");
   CHECK(result->HasError());

   // Value should be unchanged
   auto check = run(conn, "SELECT name FROM tdb.main.nn2 WHERE id = 1");
   CHECK(check->GetValue(0, 0).ToString() == "Alice");
}

TEST_CASE("PK columns are implicitly NOT NULL", "[psitri-sql][constraints]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.pknull (id INTEGER PRIMARY KEY, val VARCHAR)");

   auto result = conn.Query("INSERT INTO tdb.main.pknull VALUES (NULL, 'test')");
   CHECK(result->HasError());
}

// ===========================================================================
// VIEW tests
// ===========================================================================

TEST_CASE("CREATE VIEW and SELECT", "[psitri-sql][view]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.employees (id INTEGER PRIMARY KEY, name VARCHAR, dept VARCHAR, salary INTEGER)");
   run(conn, "INSERT INTO tdb.main.employees VALUES (1, 'Alice', 'eng', 100), (2, 'Bob', 'eng', 120), (3, 'Charlie', 'sales', 90)");

   run(conn, "CREATE VIEW tdb.main.eng_view AS SELECT id, name, salary FROM tdb.main.employees WHERE dept = 'eng'");

   auto result = run(conn, "SELECT name, salary FROM tdb.main.eng_view ORDER BY id");
   REQUIRE(result->RowCount() == 2);
   CHECK(result->GetValue(0, 0).ToString() == "Alice");
   CHECK(result->GetValue(0, 1).ToString() == "Bob");
}

TEST_CASE("DROP VIEW", "[psitri-sql][view]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.vt (id INTEGER PRIMARY KEY, val INTEGER)");
   run(conn, "CREATE VIEW tdb.main.vv AS SELECT * FROM tdb.main.vt");
   run(conn, "DROP VIEW tdb.main.vv");

   auto result = conn.Query("SELECT * FROM tdb.main.vv");
   CHECK(result->HasError());
}

// ===========================================================================
// SEQUENCE tests
// ===========================================================================

TEST_CASE("CREATE SEQUENCE basic", "[psitri-sql][sequence]") {
   TestDB t;
   auto& conn = t.conn;

   // Verify sequence can be created without error
   run(conn, "CREATE SEQUENCE tdb.main.my_seq START 1");

   // Sequences use DuckDB's internal transaction for nextval;
   // just verify the sequence catalog entry exists by creating IF NOT EXISTS
   run(conn, "CREATE SEQUENCE IF NOT EXISTS tdb.main.my_seq START 1");
}

// ===========================================================================
// Secondary index tests
// ===========================================================================

TEST_CASE("CREATE INDEX and index-assisted queries", "[psitri-sql][index]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.idx_test (id INTEGER PRIMARY KEY, email VARCHAR, score INTEGER)");
   run(conn, "INSERT INTO tdb.main.idx_test VALUES (1, 'alice@x.com', 90)");
   run(conn, "INSERT INTO tdb.main.idx_test VALUES (2, 'bob@x.com', 80)");
   run(conn, "INSERT INTO tdb.main.idx_test VALUES (3, 'charlie@x.com', 70)");

   run(conn, "CREATE INDEX idx_score ON tdb.main.idx_test(score)");

   // Data should still be queryable after index creation
   auto result = run(conn, "SELECT id, email FROM tdb.main.idx_test ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(1, 0).ToString() == "alice@x.com");
}

TEST_CASE("UNIQUE INDEX prevents duplicates", "[psitri-sql][index][unique]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.uq (id INTEGER PRIMARY KEY, email VARCHAR)");
   run(conn, "INSERT INTO tdb.main.uq VALUES (1, 'alice@x.com')");
   run(conn, "INSERT INTO tdb.main.uq VALUES (2, 'bob@x.com')");

   run(conn, "CREATE UNIQUE INDEX idx_email ON tdb.main.uq(email)");

   // Duplicate email should fail
   auto result = conn.Query("INSERT INTO tdb.main.uq VALUES (3, 'alice@x.com')");
   CHECK(result->HasError());

   // Different email should succeed
   run(conn, "INSERT INTO tdb.main.uq VALUES (3, 'charlie@x.com')");

   auto check = run(conn, "SELECT COUNT(*) FROM tdb.main.uq");
   CHECK(check->GetValue(0, 0).GetValue<int64_t>() == 3);
}

TEST_CASE("Index maintained through INSERT/DELETE", "[psitri-sql][index]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.idxm (id INTEGER PRIMARY KEY, cat VARCHAR)");
   run(conn, "INSERT INTO tdb.main.idxm VALUES (1, 'a'), (2, 'b'), (3, 'c')");
   run(conn, "CREATE UNIQUE INDEX idx_cat ON tdb.main.idxm(cat)");

   // Delete row with cat='b', then inserting cat='b' should succeed
   run(conn, "DELETE FROM tdb.main.idxm WHERE id = 2");
   run(conn, "INSERT INTO tdb.main.idxm VALUES (4, 'b')");

   auto result = run(conn, "SELECT id FROM tdb.main.idxm WHERE cat = 'b'");
   // Just verify query works; cat='b' should be id=4 now
   // (The query won't use index for non-PK, but the unique check works)
}

TEST_CASE("Index maintained through UPDATE", "[psitri-sql][index]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.idxu (id INTEGER PRIMARY KEY, code VARCHAR)");
   run(conn, "INSERT INTO tdb.main.idxu VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC')");
   run(conn, "CREATE UNIQUE INDEX idx_code ON tdb.main.idxu(code)");

   // Update code from BBB to DDD — should work
   run(conn, "UPDATE tdb.main.idxu SET code = 'DDD' WHERE id = 2");

   // Now 'BBB' is free — inserting it should work
   run(conn, "INSERT INTO tdb.main.idxu VALUES (4, 'BBB')");

   // But 'AAA' is still taken
   auto result = conn.Query("INSERT INTO tdb.main.idxu VALUES (5, 'AAA')");
   CHECK(result->HasError());

   auto check = run(conn, "SELECT COUNT(*) FROM tdb.main.idxu");
   CHECK(check->GetValue(0, 0).GetValue<int64_t>() == 4);
}

TEST_CASE("UNIQUE index violation on UPDATE", "[psitri-sql][index][unique]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.uqup (id INTEGER PRIMARY KEY, email VARCHAR)");
   run(conn, "INSERT INTO tdb.main.uqup VALUES (1, 'alice@x.com'), (2, 'bob@x.com')");
   run(conn, "CREATE UNIQUE INDEX idx_em ON tdb.main.uqup(email)");

   // Updating to duplicate email should fail
   auto result = conn.Query("UPDATE tdb.main.uqup SET email = 'alice@x.com' WHERE id = 2");
   CHECK(result->HasError());

   // Original values unchanged
   auto check = run(conn, "SELECT email FROM tdb.main.uqup WHERE id = 2");
   CHECK(check->GetValue(0, 0).ToString() == "bob@x.com");
}

// ===========================================================================
// Composite PK and multi-column tests
// ===========================================================================

TEST_CASE("Composite primary key", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.cpk (a INTEGER, b VARCHAR, val INTEGER, PRIMARY KEY (a, b))");
   run(conn, "INSERT INTO tdb.main.cpk VALUES (1, 'x', 100), (1, 'y', 200), (2, 'x', 300)");

   auto result = run(conn, "SELECT a, b, val FROM tdb.main.cpk ORDER BY a, b");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(1, 0).ToString() == "x");
   CHECK(result->GetValue(2, 0).GetValue<int32_t>() == 100);

   CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(1, 1).ToString() == "y");
   CHECK(result->GetValue(2, 1).GetValue<int32_t>() == 200);
}

// ===========================================================================
// JOIN across psitri tables
// ===========================================================================

TEST_CASE("JOIN between psitri tables", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount DOUBLE)");
   run(conn, "CREATE TABLE tdb.main.customers (id INTEGER PRIMARY KEY, name VARCHAR)");

   run(conn, "INSERT INTO tdb.main.customers VALUES (1, 'Alice'), (2, 'Bob')");
   run(conn, "INSERT INTO tdb.main.orders VALUES (10, 1, 99.99), (11, 2, 49.50), (12, 1, 25.00)");

   auto result = run(conn, "SELECT c.name, SUM(o.amount) as total "
                           "FROM tdb.main.orders o JOIN tdb.main.customers c ON o.customer_id = c.id "
                           "GROUP BY c.name ORDER BY c.name");
   REQUIRE(result->RowCount() == 2);
   CHECK(result->GetValue(0, 0).ToString() == "Alice");
   CHECK(result->GetValue(1, 0).GetValue<double>() == Catch::Approx(124.99));
   CHECK(result->GetValue(0, 1).ToString() == "Bob");
   CHECK(result->GetValue(1, 1).GetValue<double>() == Catch::Approx(49.50));
}

// ===========================================================================
// PK filter pushdown range queries
// ===========================================================================

TEST_CASE("PK range filter pushdown", "[psitri-sql][filter]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.rng (id INTEGER PRIMARY KEY, val VARCHAR)");
   for (int i = 0; i < 50; i++) {
      run(conn, "INSERT INTO tdb.main.rng VALUES (" + std::to_string(i) + ", 'v" + std::to_string(i) + "')");
   }

   // Range: id >= 10 AND id < 15
   auto result = run(conn, "SELECT id FROM tdb.main.rng WHERE id >= 10 AND id < 15 ORDER BY id");
   REQUIRE(result->RowCount() == 5);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 10);
   CHECK(result->GetValue(0, 4).GetValue<int32_t>() == 14);

   // Mixed PK and non-PK filter
   auto result2 = run(conn, "SELECT id, val FROM tdb.main.rng WHERE id > 20 AND val = 'v25'");
   REQUIRE(result2->RowCount() == 1);
   CHECK(result2->GetValue(0, 0).GetValue<int32_t>() == 25);
}

// ===========================================================================
// DATE/TIME arithmetic in queries
// ===========================================================================

TEST_CASE("DATE arithmetic and WHERE", "[psitri-sql][types]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.events (id INTEGER PRIMARY KEY, d DATE, label VARCHAR)");
   run(conn, "INSERT INTO tdb.main.events VALUES (1, '2024-01-01', 'new year')");
   run(conn, "INSERT INTO tdb.main.events VALUES (2, '2024-06-15', 'mid year')");
   run(conn, "INSERT INTO tdb.main.events VALUES (3, '2024-12-25', 'christmas')");

   auto result = run(conn, "SELECT label FROM tdb.main.events WHERE d > '2024-06-01' ORDER BY d");
   REQUIRE(result->RowCount() == 2);
   CHECK(result->GetValue(0, 0).ToString() == "mid year");
   CHECK(result->GetValue(0, 1).ToString() == "christmas");
}

// ===========================================================================
// Subqueries
// ===========================================================================

TEST_CASE("Subquery with psitri table", "[psitri-sql]") {
   TestDB t;
   auto& conn = t.conn;

   run(conn, "CREATE TABLE tdb.main.scores (id INTEGER PRIMARY KEY, score INTEGER)");
   run(conn, "INSERT INTO tdb.main.scores VALUES (1, 90), (2, 80), (3, 70), (4, 95), (5, 85)");

   // AVG(90,80,70,95,85) = 84, so scores > 84 are: 90(id=1), 85(id=5), 95(id=4)
   auto result = run(conn, "SELECT id FROM tdb.main.scores WHERE score > (SELECT AVG(score) FROM tdb.main.scores) ORDER BY id");
   REQUIRE(result->RowCount() == 3);
   CHECK(result->GetValue(0, 0).GetValue<int32_t>() == 1);
   CHECK(result->GetValue(0, 1).GetValue<int32_t>() == 4);
   CHECK(result->GetValue(0, 2).GetValue<int32_t>() == 5);
}
