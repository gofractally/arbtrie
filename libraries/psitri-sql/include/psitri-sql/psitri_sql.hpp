#pragma once

namespace duckdb {
class DatabaseInstance;
class DuckDB;
class DBConfig;
} // namespace duckdb

namespace psitri_sql {

// Register the psitri storage extension with a DuckDB instance.
// After calling this, users can:
//   ATTACH '/path/to/db' AS mydb (TYPE psitri);
//   CREATE TABLE mydb.main.users (id INTEGER PRIMARY KEY, name VARCHAR);
//   INSERT INTO mydb.main.users VALUES (1, 'Alice');
//   SELECT * FROM mydb.main.users;
void RegisterPsitriStorage(duckdb::DatabaseInstance& db);
void RegisterPsitriStorage(duckdb::DuckDB& db);

// Register on a DBConfig before creating the DuckDB instance.
// This is needed so DuckDB's ATTACH TYPE dispatch finds the extension
// without trying to autoload it as a file-based extension.
void RegisterPsitriStorage(duckdb::DBConfig& config);

} // namespace psitri_sql
