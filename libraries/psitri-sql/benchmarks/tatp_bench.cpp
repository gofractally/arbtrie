/**
 * TATP (Telecom Application Transaction Processing) Benchmark
 *
 * Standard OLTP benchmark simulating a Home Location Register (HLR) database.
 * 4 tables, 7 transaction types, 80% reads / 20% writes.
 *
 * Runs identical SQL workloads against:
 *   - psitri (via DuckDB storage extension)
 *   - DuckDB native (in-memory)
 *
 * Reference: https://tatpbenchmark.sourceforge.net/TATP_Description.pdf
 */

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <numeric>
#include <algorithm>

#include "duckdb.hpp"
#include <psitri-sql/psitri_sql.hpp>
#include <psitri-sql/row_encoding.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/transaction.hpp>
#include <psitri/database.hpp>
// psitri-sqlite provides the sqlite3 API (btree replaced by psitri DWAL)
#include <sqlite3.h>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct Config {
   uint32_t num_subscribers = 100000;
   uint32_t duration_secs   = 10;
   std::string engine       = "psitri";  // "psitri" or "duckdb"
   std::string sync         = "off";     // "off", "normal", "full", "extra"
};

// ---------------------------------------------------------------------------
// Transaction mix per TATP spec
// ---------------------------------------------------------------------------

enum class TxnType : uint8_t {
   GET_SUBSCRIBER_DATA   = 0,  // 35%
   GET_ACCESS_DATA       = 1,  // 35%
   GET_NEW_DESTINATION   = 2,  // 10%
   UPDATE_LOCATION       = 3,  // 14%
   INSERT_CALL_FWD       = 4,  //  2%
   DELETE_CALL_FWD       = 5,  //  2%
   UPDATE_SUBSCRIBER     = 6,  //  2%
};

static const char* txn_names[] = {
   "GetSubscriberData",
   "GetAccessData",
   "GetNewDestination",
   "UpdateLocation",
   "InsertCallForwarding",
   "DeleteCallForwarding",
   "UpdateSubscriberData",
};

// Cumulative distribution (out of 100)
static TxnType pick_txn(std::mt19937& rng) {
   std::uniform_int_distribution<int> dist(1, 100);
   int r = dist(rng);
   if (r <= 35) return TxnType::GET_SUBSCRIBER_DATA;
   if (r <= 70) return TxnType::GET_ACCESS_DATA;
   if (r <= 80) return TxnType::GET_NEW_DESTINATION;
   if (r <= 94) return TxnType::UPDATE_LOCATION;
   if (r <= 96) return TxnType::INSERT_CALL_FWD;
   if (r <= 98) return TxnType::DELETE_CALL_FWD;
   return TxnType::UPDATE_SUBSCRIBER;
}

// ---------------------------------------------------------------------------
// Helper: run SQL, ignore errors for benchmark transactions
// ---------------------------------------------------------------------------

static duckdb::unique_ptr<duckdb::MaterializedQueryResult>
exec(duckdb::Connection& conn, const std::string& sql) {
   return conn.Query(sql);
}

static void exec_ok(duckdb::Connection& conn, const std::string& sql) {
   auto r = conn.Query(sql);
   if (r->HasError()) {
      std::cerr << "SQL error: " << r->GetError() << "\n  query: " << sql << "\n";
      std::exit(1);
   }
}

// ---------------------------------------------------------------------------
// Schema creation
// ---------------------------------------------------------------------------

static void create_schema(duckdb::Connection& conn, const std::string& cat) {
   std::string q = cat.empty() ? "" : cat + ".main.";

   exec_ok(conn, "CREATE TABLE " + q + "subscriber ("
      "s_id INTEGER PRIMARY KEY, "
      "sub_nbr VARCHAR(15) NOT NULL, "
      "bit_1 SMALLINT, bit_2 SMALLINT, bit_3 SMALLINT, bit_4 SMALLINT, "
      "bit_5 SMALLINT, bit_6 SMALLINT, bit_7 SMALLINT, bit_8 SMALLINT, "
      "bit_9 SMALLINT, bit_10 SMALLINT, "
      "hex_1 SMALLINT, hex_2 SMALLINT, hex_3 SMALLINT, hex_4 SMALLINT, "
      "hex_5 SMALLINT, hex_6 SMALLINT, hex_7 SMALLINT, hex_8 SMALLINT, "
      "hex_9 SMALLINT, hex_10 SMALLINT, "
      "byte2_1 SMALLINT, byte2_2 SMALLINT, byte2_3 SMALLINT, byte2_4 SMALLINT, "
      "byte2_5 SMALLINT, byte2_6 SMALLINT, byte2_7 SMALLINT, byte2_8 SMALLINT, "
      "byte2_9 SMALLINT, byte2_10 SMALLINT, "
      "msc_location INTEGER, vlr_location INTEGER)");

   exec_ok(conn, "CREATE TABLE " + q + "access_info ("
      "s_id INTEGER NOT NULL, "
      "ai_type SMALLINT NOT NULL, "
      "data1 SMALLINT, data2 SMALLINT, "
      "data3 VARCHAR(3), data4 VARCHAR(5), "
      "PRIMARY KEY(s_id, ai_type))");

   exec_ok(conn, "CREATE TABLE " + q + "special_facility ("
      "s_id INTEGER NOT NULL, "
      "sf_type SMALLINT NOT NULL, "
      "is_active SMALLINT NOT NULL, "
      "error_cntrl SMALLINT, data_a SMALLINT, data_b VARCHAR(5), "
      "PRIMARY KEY(s_id, sf_type))");

   exec_ok(conn, "CREATE TABLE " + q + "call_forwarding ("
      "s_id INTEGER NOT NULL, "
      "sf_type SMALLINT NOT NULL, "
      "start_time SMALLINT NOT NULL, "
      "end_time SMALLINT, numberx VARCHAR(15), "
      "PRIMARY KEY(s_id, sf_type, start_time))");
}

// ---------------------------------------------------------------------------
// Data population
// ---------------------------------------------------------------------------

static std::string zero_pad(uint32_t id, int width = 15) {
   std::string s = std::to_string(id);
   if ((int)s.size() < width) s.insert(0, width - s.size(), '0');
   return s;
}

static void populate(duckdb::Connection& conn, const std::string& cat,
                     uint32_t N, std::mt19937& rng) {
   std::string q = cat.empty() ? "" : cat + ".main.";

   auto t0 = std::chrono::steady_clock::now();

   // Batch insert subscribers
   const uint32_t batch = 500;
   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO " + q + "subscriber VALUES ";
      for (uint32_t i = base; i < end; i++) {
         if (i > base) sql += ',';
         sql += '(';
         sql += std::to_string(i) + ",'" + zero_pad(i) + "'";
         // bit_1..bit_10
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() & 1);
         // hex_1..hex_10
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() % 16);
         // byte2_1..byte2_10
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() % 256);
         // msc_location, vlr_location
         sql += ',' + std::to_string(rng() % 0x7FFFFFFF);
         sql += ',' + std::to_string(rng() % 0x7FFFFFFF);
         sql += ')';
      }
      exec_ok(conn, sql);
   }

   // Populate access_info: 1-4 records per subscriber
   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO " + q + "access_info VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_records = 1 + (rng() % 4);
         for (int t = 1; t <= n_records; t++) {
            if (!first) sql += ',';
            first = false;
            sql += '(' + std::to_string(i) + ',' + std::to_string(t);
            sql += ',' + std::to_string(rng() % 256);
            sql += ',' + std::to_string(rng() % 256);
            sql += ",'" + std::string(1 + rng() % 3, 'A' + rng() % 26) + "'";
            sql += ",'" + std::string(1 + rng() % 5, 'a' + rng() % 26) + "'";
            sql += ')';
         }
      }
      exec_ok(conn, sql);
   }

   // Populate special_facility: 1-4 records per subscriber
   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO " + q + "special_facility VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_records = 1 + (rng() % 4);
         for (int t = 1; t <= n_records; t++) {
            if (!first) sql += ',';
            first = false;
            int is_active = (rng() % 100 < 85) ? 1 : 0;
            sql += '(' + std::to_string(i) + ',' + std::to_string(t);
            sql += ',' + std::to_string(is_active);
            sql += ',' + std::to_string(rng() % 256);
            sql += ',' + std::to_string(rng() % 256);
            sql += ",'" + std::string(1 + rng() % 5, 'x' + rng() % 3) + "'";
            sql += ')';
         }
      }
      exec_ok(conn, sql);
   }

   // Populate call_forwarding: 0-3 records per special_facility
   // We scan special_facility and add forwarding entries
   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO " + q + "call_forwarding VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_sf = 1 + (rng() % 4);
         for (int sf = 1; sf <= n_sf; sf++) {
            int n_cf = rng() % 4;  // 0-3
            int start_times[] = {0, 8, 16};
            for (int c = 0; c < n_cf; c++) {
               if (!first) sql += ',';
               first = false;
               int st = start_times[c];
               int et = st + 1 + (rng() % 8);
               sql += '(' + std::to_string(i) + ',' + std::to_string(sf);
               sql += ',' + std::to_string(st);
               sql += ',' + std::to_string(et);
               sql += ",'" + zero_pad(1 + rng() % N) + "'";
               sql += ')';
            }
         }
      }
      if (!first) {
         exec_ok(conn, sql);
      }
   }

   auto t1 = std::chrono::steady_clock::now();
   double load_secs = std::chrono::duration<double>(t1 - t0).count();
   std::printf("  Populated %u subscribers in %.2f s (%.0f rows/s)\n",
               N, load_secs, N / load_secs);
}

// ---------------------------------------------------------------------------
// Transaction implementations
// ---------------------------------------------------------------------------

struct TxnStats {
   uint64_t count    = 0;
   uint64_t success  = 0;
   uint64_t total_us = 0;
   std::vector<uint64_t> latencies;

   void record(bool ok, uint64_t us) {
      count++;
      if (ok) success++;
      total_us += us;
      latencies.push_back(us);
   }
};

static bool txn_get_subscriber_data(duckdb::Connection& conn, const std::string& q,
                                     uint32_t s_id) {
   auto r = exec(conn, "SELECT * FROM " + q + "subscriber WHERE s_id = " +
                 std::to_string(s_id));
   return !r->HasError() && r->RowCount() > 0;
}

static bool txn_get_access_data(duckdb::Connection& conn, const std::string& q,
                                 uint32_t s_id, int ai_type) {
   auto r = exec(conn, "SELECT data1, data2, data3, data4 FROM " + q +
                 "access_info WHERE s_id = " + std::to_string(s_id) +
                 " AND ai_type = " + std::to_string(ai_type));
   return !r->HasError();
}

static bool txn_get_new_destination(duckdb::Connection& conn, const std::string& q,
                                     uint32_t s_id, int sf_type, int start_time) {
   auto r = exec(conn, "SELECT cf.numberx FROM " + q + "special_facility sf, " +
                 q + "call_forwarding cf "
                 "WHERE sf.s_id = " + std::to_string(s_id) +
                 " AND sf.sf_type = " + std::to_string(sf_type) +
                 " AND sf.is_active = 1"
                 " AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type"
                 " AND cf.start_time <= " + std::to_string(start_time) +
                 " AND cf.end_time > " + std::to_string(start_time));
   return !r->HasError();
}

static bool txn_update_location(duckdb::Connection& conn, const std::string& q,
                                 const std::string& sub_nbr, uint32_t vlr_loc) {
   auto r1 = exec(conn, "SELECT s_id FROM " + q + "subscriber WHERE sub_nbr = '" +
                  sub_nbr + "'");
   if (r1->HasError() || r1->RowCount() == 0) return false;
   auto s_id = r1->GetValue(0, 0).GetValue<int32_t>();
   auto r2 = exec(conn, "UPDATE " + q + "subscriber SET vlr_location = " +
                  std::to_string(vlr_loc) + " WHERE s_id = " + std::to_string(s_id));
   return !r2->HasError();
}

static bool txn_insert_call_fwd(duckdb::Connection& conn, const std::string& q,
                                 uint32_t s_id, int sf_type, int start_time,
                                 int end_time, const std::string& numberx) {
   auto r = exec(conn, "INSERT INTO " + q + "call_forwarding VALUES (" +
                 std::to_string(s_id) + "," + std::to_string(sf_type) + "," +
                 std::to_string(start_time) + "," + std::to_string(end_time) +
                 ",'" + numberx + "')");
   return !r->HasError();
}

static bool txn_delete_call_fwd(duckdb::Connection& conn, const std::string& q,
                                 uint32_t s_id, int sf_type, int start_time) {
   auto r = exec(conn, "DELETE FROM " + q + "call_forwarding WHERE s_id = " +
                 std::to_string(s_id) + " AND sf_type = " + std::to_string(sf_type) +
                 " AND start_time = " + std::to_string(start_time));
   return !r->HasError();
}

static bool txn_update_subscriber_data(duckdb::Connection& conn, const std::string& q,
                                        uint32_t s_id, int bit_1, int sf_type,
                                        int data_a) {
   auto r1 = exec(conn, "UPDATE " + q + "subscriber SET bit_1 = " +
                  std::to_string(bit_1) + " WHERE s_id = " + std::to_string(s_id));
   if (r1->HasError()) return false;
   auto r2 = exec(conn, "UPDATE " + q + "special_facility SET data_a = " +
                  std::to_string(data_a) + " WHERE s_id = " + std::to_string(s_id) +
                  " AND sf_type = " + std::to_string(sf_type));
   return !r2->HasError();
}

// ---------------------------------------------------------------------------
// Benchmark driver (with prepared statement support)
// ---------------------------------------------------------------------------

static void print_stats(const char* engine_name, uint32_t N, double elapsed,
                        uint64_t total_txns, TxnStats stats[7]) {
   std::printf("\n=== TATP Benchmark Results: %s ===\n", engine_name);
   std::printf("Subscribers: %u | Duration: %.1f s | Total TPS: %.0f\n\n",
               N, elapsed, total_txns / elapsed);

   std::printf("%-25s  %8s  %8s  %8s  %8s  %8s\n",
               "Transaction", "Count", "TPS", "Avg(us)", "P50(us)", "P99(us)");
   std::printf("%-25s  %8s  %8s  %8s  %8s  %8s\n",
               "-------------------------", "--------", "--------",
               "--------", "--------", "--------");

   for (int i = 0; i < 7; i++) {
      auto& s = stats[i];
      if (s.count == 0) continue;
      std::sort(s.latencies.begin(), s.latencies.end());
      uint64_t p50 = s.latencies[s.latencies.size() / 2];
      uint64_t p99 = s.latencies[(size_t)(s.latencies.size() * 0.99)];
      uint64_t avg = s.total_us / s.count;
      std::printf("%-25s  %8llu  %8.0f  %8llu  %8llu  %8llu\n",
                  txn_names[i], (unsigned long long)s.count, s.count / elapsed,
                  (unsigned long long)avg, (unsigned long long)p50, (unsigned long long)p99);
   }
   std::printf("\n%-25s  %8llu  %8.0f\n", "TOTAL",
               (unsigned long long)total_txns, total_txns / elapsed);
}

// Prepared statement cache for all 7 TATP transaction types
struct PreparedStmts {
   duckdb::unique_ptr<duckdb::PreparedStatement> get_subscriber;    // $1=s_id
   duckdb::unique_ptr<duckdb::PreparedStatement> get_access;        // $1=s_id, $2=ai_type
   duckdb::unique_ptr<duckdb::PreparedStatement> get_new_dest;      // $1=s_id, $2=sf_type, $3=start_time
   duckdb::unique_ptr<duckdb::PreparedStatement> update_loc_sel;    // $1=sub_nbr
   duckdb::unique_ptr<duckdb::PreparedStatement> update_loc_upd;    // $1=vlr_loc, $2=s_id
   duckdb::unique_ptr<duckdb::PreparedStatement> insert_cf;         // $1=s_id, $2=sf_type, $3=st, $4=et, $5=numberx
   duckdb::unique_ptr<duckdb::PreparedStatement> delete_cf;         // $1=s_id, $2=sf_type, $3=start_time
   duckdb::unique_ptr<duckdb::PreparedStatement> update_sub_bit;    // $1=bit_1, $2=s_id
   duckdb::unique_ptr<duckdb::PreparedStatement> update_sub_sf;     // $1=data_a, $2=s_id, $3=sf_type

   void prepare(duckdb::Connection& conn, const std::string& q) {
      get_subscriber = conn.Prepare(
         "SELECT * FROM " + q + "subscriber WHERE s_id = $1");
      get_access = conn.Prepare(
         "SELECT data1, data2, data3, data4 FROM " + q + "access_info WHERE s_id = $1 AND ai_type = $2");
      get_new_dest = conn.Prepare(
         "SELECT cf.numberx FROM " + q + "special_facility sf, " + q + "call_forwarding cf "
         "WHERE sf.s_id = $1 AND sf.sf_type = $2 AND sf.is_active = 1 "
         "AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type "
         "AND cf.start_time <= $3 AND cf.end_time > $3");
      update_loc_sel = conn.Prepare(
         "SELECT s_id FROM " + q + "subscriber WHERE sub_nbr = $1");
      update_loc_upd = conn.Prepare(
         "UPDATE " + q + "subscriber SET vlr_location = $1 WHERE s_id = $2");
      insert_cf = conn.Prepare(
         "INSERT INTO " + q + "call_forwarding VALUES ($1, $2, $3, $4, $5)");
      delete_cf = conn.Prepare(
         "DELETE FROM " + q + "call_forwarding WHERE s_id = $1 AND sf_type = $2 AND start_time = $3");
      update_sub_bit = conn.Prepare(
         "UPDATE " + q + "subscriber SET bit_1 = $1 WHERE s_id = $2");
      update_sub_sf = conn.Prepare(
         "UPDATE " + q + "special_facility SET data_a = $1 WHERE s_id = $2 AND sf_type = $3");
   }
};

static void run_benchmark(duckdb::Connection& conn, const std::string& cat,
                          const Config& cfg) {
   std::string q = cat.empty() ? "" : cat + ".main.";
   uint32_t N = cfg.num_subscribers;

   // Prepare all statements
   PreparedStmts ps;
   ps.prepare(conn, q);

   std::mt19937 rng(42);
   TxnStats stats[7] = {};

   auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::seconds(cfg.duration_secs);
   uint64_t total_txns = 0;
   auto t0 = std::chrono::steady_clock::now();

   while (std::chrono::steady_clock::now() < deadline) {
      TxnType txn = pick_txn(rng);
      uint32_t s_id = 1 + (rng() % N);
      int sf_type = 1 + (rng() % 4);

      auto start = std::chrono::steady_clock::now();
      bool ok = false;

      switch (txn) {
         case TxnType::GET_SUBSCRIBER_DATA: {
            auto r = ps.get_subscriber->Execute((int32_t)s_id);
            if (!r->HasError()) {
               auto chunk = r->Fetch();
               ok = chunk && chunk->size() > 0;
            }
            break;
         }
         case TxnType::GET_ACCESS_DATA: {
            int ai_type = 1 + rng() % 4;
            auto r = ps.get_access->Execute((int32_t)s_id, (int16_t)ai_type);
            if (!r->HasError()) { r->Fetch(); ok = true; }
            break;
         }
         case TxnType::GET_NEW_DESTINATION: {
            int start_time = rng() % 24;
            auto r = ps.get_new_dest->Execute((int32_t)s_id, (int16_t)sf_type, (int16_t)start_time);
            if (!r->HasError()) { r->Fetch(); ok = true; }
            break;
         }
         case TxnType::UPDATE_LOCATION: {
            auto sub_nbr = zero_pad(s_id);
            uint32_t vlr_loc = rng() % 0x7FFFFFFF;
            auto r1 = ps.update_loc_sel->Execute(duckdb::Value(sub_nbr));
            if (!r1->HasError()) {
               auto chunk = r1->Fetch();
               if (chunk && chunk->size() > 0) {
                  auto sid = chunk->GetValue(0, 0).GetValue<int32_t>();
                  auto r2 = ps.update_loc_upd->Execute((int32_t)vlr_loc, sid);
                  ok = r2 && !r2->HasError();
               }
            }
            break;
         }
         case TxnType::INSERT_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            int end_time = (rng() % 3) * 8 + 1 + rng() % 8;
            auto numberx = zero_pad(1 + rng() % N);
            auto r = ps.insert_cf->Execute((int32_t)s_id, (int16_t)sf_type,
                                            (int16_t)start_time, (int16_t)end_time,
                                            duckdb::Value(numberx));
            ok = r && !r->HasError();
            break;
         }
         case TxnType::DELETE_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            auto r = ps.delete_cf->Execute((int32_t)s_id, (int16_t)sf_type, (int16_t)start_time);
            ok = r && !r->HasError();
            break;
         }
         case TxnType::UPDATE_SUBSCRIBER: {
            int bit_1 = rng() & 1;
            int data_a = rng() % 256;
            auto r1 = ps.update_sub_bit->Execute((int16_t)bit_1, (int32_t)s_id);
            if (!r1 || r1->HasError()) break;
            auto r2 = ps.update_sub_sf->Execute((int16_t)data_a, (int32_t)s_id, (int16_t)sf_type);
            ok = r2 && !r2->HasError();
            break;
         }
      }

      auto end_t = std::chrono::steady_clock::now();
      uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                       end_t - start).count();
      stats[(int)txn].record(ok, us);
      total_txns++;
   }

   auto t1 = std::chrono::steady_clock::now();
   double elapsed = std::chrono::duration<double>(t1 - t0).count();
   print_stats(cfg.engine.c_str(), N, elapsed, total_txns, stats);
}

// ---------------------------------------------------------------------------
// SQLite engine
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// SQLite engine (uses psitri-sqlite or system SQLite depending on link)
// ---------------------------------------------------------------------------

struct SqliteResult {
   bool has_error = false;
   std::vector<std::vector<std::string>> rows;
   int64_t RowCount() const { return (int64_t)rows.size(); }
   bool HasError() const { return has_error; }
};

static SqliteResult sqlite_exec(sqlite3* db, const std::string& sql) {
   SqliteResult result;
   sqlite3_stmt* stmt = nullptr;
   int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
   if (rc != SQLITE_OK) {
      result.has_error = true;
      return result;
   }
   int ncols = sqlite3_column_count(stmt);
   while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
      std::vector<std::string> row;
      for (int i = 0; i < ncols; i++) {
         auto* text = sqlite3_column_text(stmt, i);
         row.push_back(text ? (const char*)text : "");
      }
      result.rows.push_back(std::move(row));
   }
   if (rc != SQLITE_DONE) result.has_error = true;
   sqlite3_finalize(stmt);
   return result;
}

static void sqlite_exec_ok(sqlite3* db, const std::string& sql) {
   char* err = nullptr;
   int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err);
   if (rc != SQLITE_OK) {
      std::string msg = err ? err : "unknown error";
      sqlite3_free(err);
      std::cerr << "SQLite error: " << msg << "\n  query: " << sql.substr(0, 200) << "\n";
      std::exit(1);
   }
}

static void sqlite_create_schema(sqlite3* db) {
   sqlite_exec_ok(db, "CREATE TABLE subscriber ("
      "s_id INTEGER PRIMARY KEY, "
      "sub_nbr TEXT NOT NULL, "
      "bit_1 INTEGER, bit_2 INTEGER, bit_3 INTEGER, bit_4 INTEGER, "
      "bit_5 INTEGER, bit_6 INTEGER, bit_7 INTEGER, bit_8 INTEGER, "
      "bit_9 INTEGER, bit_10 INTEGER, "
      "hex_1 INTEGER, hex_2 INTEGER, hex_3 INTEGER, hex_4 INTEGER, "
      "hex_5 INTEGER, hex_6 INTEGER, hex_7 INTEGER, hex_8 INTEGER, "
      "hex_9 INTEGER, hex_10 INTEGER, "
      "byte2_1 INTEGER, byte2_2 INTEGER, byte2_3 INTEGER, byte2_4 INTEGER, "
      "byte2_5 INTEGER, byte2_6 INTEGER, byte2_7 INTEGER, byte2_8 INTEGER, "
      "byte2_9 INTEGER, byte2_10 INTEGER, "
      "msc_location INTEGER, vlr_location INTEGER)");
   sqlite_exec_ok(db, "CREATE UNIQUE INDEX idx_sub_nbr ON subscriber(sub_nbr)");

   sqlite_exec_ok(db, "CREATE TABLE access_info ("
      "s_id INTEGER NOT NULL, "
      "ai_type INTEGER NOT NULL, "
      "data1 INTEGER, data2 INTEGER, "
      "data3 TEXT, data4 TEXT, "
      "PRIMARY KEY(s_id, ai_type))");

   sqlite_exec_ok(db, "CREATE TABLE special_facility ("
      "s_id INTEGER NOT NULL, "
      "sf_type INTEGER NOT NULL, "
      "is_active INTEGER NOT NULL, "
      "error_cntrl INTEGER, data_a INTEGER, data_b TEXT, "
      "PRIMARY KEY(s_id, sf_type))");

   sqlite_exec_ok(db, "CREATE TABLE call_forwarding ("
      "s_id INTEGER NOT NULL, "
      "sf_type INTEGER NOT NULL, "
      "start_time INTEGER NOT NULL, "
      "end_time INTEGER, numberx TEXT, "
      "PRIMARY KEY(s_id, sf_type, start_time))");
}

static void sqlite_populate(sqlite3* db, uint32_t N, std::mt19937& rng) {
   auto t0 = std::chrono::steady_clock::now();

   sqlite_exec_ok(db, "BEGIN");

   const uint32_t batch = 500;
   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO subscriber VALUES ";
      for (uint32_t i = base; i < end; i++) {
         if (i > base) sql += ',';
         sql += '(';
         sql += std::to_string(i) + ",'" + zero_pad(i) + "'";
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() & 1);
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() % 16);
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() % 256);
         sql += ',' + std::to_string(rng() % 0x7FFFFFFF);
         sql += ',' + std::to_string(rng() % 0x7FFFFFFF);
         sql += ')';
      }
      sqlite_exec_ok(db, sql);
   }

   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO access_info VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_records = 1 + (rng() % 4);
         for (int t = 1; t <= n_records; t++) {
            if (!first) sql += ',';
            first = false;
            sql += '(' + std::to_string(i) + ',' + std::to_string(t);
            sql += ',' + std::to_string(rng() % 256);
            sql += ',' + std::to_string(rng() % 256);
            sql += ",'" + std::string(1 + rng() % 3, 'A' + rng() % 26) + "'";
            sql += ",'" + std::string(1 + rng() % 5, 'a' + rng() % 26) + "'";
            sql += ')';
         }
      }
      sqlite_exec_ok(db, sql);
   }

   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO special_facility VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_records = 1 + (rng() % 4);
         for (int t = 1; t <= n_records; t++) {
            if (!first) sql += ',';
            first = false;
            int is_active = (rng() % 100 < 85) ? 1 : 0;
            sql += '(' + std::to_string(i) + ',' + std::to_string(t);
            sql += ',' + std::to_string(is_active);
            sql += ',' + std::to_string(rng() % 256);
            sql += ',' + std::to_string(rng() % 256);
            sql += ",'" + std::string(1 + rng() % 5, 'x' + rng() % 3) + "'";
            sql += ')';
         }
      }
      sqlite_exec_ok(db, sql);
   }

   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO call_forwarding VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_sf = 1 + (rng() % 4);
         for (int sf = 1; sf <= n_sf; sf++) {
            int n_cf = rng() % 4;
            int start_times[] = {0, 8, 16};
            for (int c = 0; c < n_cf; c++) {
               if (!first) sql += ',';
               first = false;
               int st = start_times[c];
               int et = st + 1 + (rng() % 8);
               sql += '(' + std::to_string(i) + ',' + std::to_string(sf);
               sql += ',' + std::to_string(st);
               sql += ',' + std::to_string(et);
               sql += ",'" + zero_pad(1 + rng() % N) + "'";
               sql += ')';
            }
         }
      }
      if (!first) sqlite_exec_ok(db, sql);
   }

   sqlite_exec_ok(db, "COMMIT");

   auto t1 = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(t1 - t0).count();
   std::printf("  Populated %u subscribers in %.2f s (%.0f rows/s)\n", N, secs, N / secs);
}

struct SqliteStmts {
   sqlite3_stmt* get_subscriber = nullptr;
   sqlite3_stmt* get_access = nullptr;
   sqlite3_stmt* get_new_dest = nullptr;
   sqlite3_stmt* update_loc_sel = nullptr;
   sqlite3_stmt* update_loc_upd = nullptr;
   sqlite3_stmt* insert_cf = nullptr;
   sqlite3_stmt* delete_cf = nullptr;
   sqlite3_stmt* update_sub_bit = nullptr;
   sqlite3_stmt* update_sub_sf = nullptr;

   void prepare(sqlite3* db) {
      auto prep = [&](const char* sql, sqlite3_stmt** out) {
         if (sqlite3_prepare_v2(db, sql, -1, out, nullptr) != SQLITE_OK) {
            std::fprintf(stderr, "SQLite prepare failed: %s\n  sql: %s\n",
                         sqlite3_errmsg(db), sql);
            std::exit(1);
         }
      };
      prep("SELECT * FROM subscriber WHERE s_id = ?", &get_subscriber);
      prep("SELECT data1, data2, data3, data4 FROM access_info WHERE s_id = ? AND ai_type = ?", &get_access);
      prep("SELECT cf.numberx FROM special_facility sf, call_forwarding cf "
           "WHERE sf.s_id = ? AND sf.sf_type = ? AND sf.is_active = 1 "
           "AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type "
           "AND cf.start_time <= ? AND cf.end_time > ?", &get_new_dest);
      prep("SELECT s_id FROM subscriber WHERE sub_nbr = ?", &update_loc_sel);
      prep("UPDATE subscriber SET vlr_location = ? WHERE s_id = ?", &update_loc_upd);
      prep("INSERT OR IGNORE INTO call_forwarding VALUES (?, ?, ?, ?, ?)", &insert_cf);
      prep("DELETE FROM call_forwarding WHERE s_id = ? AND sf_type = ? AND start_time = ?", &delete_cf);
      prep("UPDATE subscriber SET bit_1 = ? WHERE s_id = ?", &update_sub_bit);
      prep("UPDATE special_facility SET data_a = ? WHERE s_id = ? AND sf_type = ?", &update_sub_sf);
   }

   ~SqliteStmts() {
      sqlite3_finalize(get_subscriber);
      sqlite3_finalize(get_access);
      sqlite3_finalize(get_new_dest);
      sqlite3_finalize(update_loc_sel);
      sqlite3_finalize(update_loc_upd);
      sqlite3_finalize(insert_cf);
      sqlite3_finalize(delete_cf);
      sqlite3_finalize(update_sub_bit);
      sqlite3_finalize(update_sub_sf);
   }
};

static void run_sqlite_benchmark(sqlite3* db, const Config& cfg) {
   uint32_t N = cfg.num_subscribers;

   SqliteStmts ps;
   ps.prepare(db);

   std::mt19937 rng(42);
   TxnStats stats[7] = {};

   auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::seconds(cfg.duration_secs);
   uint64_t total_txns = 0;
   auto t0 = std::chrono::steady_clock::now();

   while (std::chrono::steady_clock::now() < deadline) {
      TxnType txn = pick_txn(rng);
      uint32_t s_id = 1 + (rng() % N);
      int sf_type = 1 + (rng() % 4);

      auto start = std::chrono::steady_clock::now();
      bool ok = false;

      switch (txn) {
         case TxnType::GET_SUBSCRIBER_DATA: {
            sqlite3_reset(ps.get_subscriber);
            sqlite3_bind_int(ps.get_subscriber, 1, s_id);
            ok = sqlite3_step(ps.get_subscriber) == SQLITE_ROW;
            break;
         }
         case TxnType::GET_ACCESS_DATA: {
            int ai_type = 1 + rng() % 4;
            sqlite3_reset(ps.get_access);
            sqlite3_bind_int(ps.get_access, 1, s_id);
            sqlite3_bind_int(ps.get_access, 2, ai_type);
            sqlite3_step(ps.get_access);
            ok = true;
            break;
         }
         case TxnType::GET_NEW_DESTINATION: {
            int start_time = rng() % 24;
            sqlite3_reset(ps.get_new_dest);
            sqlite3_bind_int(ps.get_new_dest, 1, s_id);
            sqlite3_bind_int(ps.get_new_dest, 2, sf_type);
            sqlite3_bind_int(ps.get_new_dest, 3, start_time);
            sqlite3_bind_int(ps.get_new_dest, 4, start_time);
            sqlite3_step(ps.get_new_dest);
            ok = true;
            break;
         }
         case TxnType::UPDATE_LOCATION: {
            auto sub_nbr = zero_pad(s_id);
            uint32_t vlr_loc = rng() % 0x7FFFFFFF;
            sqlite3_reset(ps.update_loc_sel);
            sqlite3_bind_text(ps.update_loc_sel, 1, sub_nbr.c_str(), -1, SQLITE_TRANSIENT);
            if (sqlite3_step(ps.update_loc_sel) == SQLITE_ROW) {
               int sid = sqlite3_column_int(ps.update_loc_sel, 0);
               sqlite3_reset(ps.update_loc_upd);
               sqlite3_bind_int(ps.update_loc_upd, 1, vlr_loc);
               sqlite3_bind_int(ps.update_loc_upd, 2, sid);
               sqlite3_step(ps.update_loc_upd);
               ok = true;
            }
            break;
         }
         case TxnType::INSERT_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            int end_time = (rng() % 3) * 8 + 1 + rng() % 8;
            auto numberx = zero_pad(1 + rng() % N);
            sqlite3_reset(ps.insert_cf);
            sqlite3_bind_int(ps.insert_cf, 1, s_id);
            sqlite3_bind_int(ps.insert_cf, 2, sf_type);
            sqlite3_bind_int(ps.insert_cf, 3, start_time);
            sqlite3_bind_int(ps.insert_cf, 4, end_time);
            sqlite3_bind_text(ps.insert_cf, 5, numberx.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_step(ps.insert_cf);
            ok = true;
            break;
         }
         case TxnType::DELETE_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            sqlite3_reset(ps.delete_cf);
            sqlite3_bind_int(ps.delete_cf, 1, s_id);
            sqlite3_bind_int(ps.delete_cf, 2, sf_type);
            sqlite3_bind_int(ps.delete_cf, 3, start_time);
            sqlite3_step(ps.delete_cf);
            ok = true;
            break;
         }
         case TxnType::UPDATE_SUBSCRIBER: {
            int bit_1 = rng() & 1;
            int data_a = rng() % 256;
            sqlite3_reset(ps.update_sub_bit);
            sqlite3_bind_int(ps.update_sub_bit, 1, bit_1);
            sqlite3_bind_int(ps.update_sub_bit, 2, s_id);
            sqlite3_step(ps.update_sub_bit);
            sqlite3_reset(ps.update_sub_sf);
            sqlite3_bind_int(ps.update_sub_sf, 1, data_a);
            sqlite3_bind_int(ps.update_sub_sf, 2, s_id);
            sqlite3_bind_int(ps.update_sub_sf, 3, sf_type);
            sqlite3_step(ps.update_sub_sf);
            ok = true;
            break;
         }
      }

      auto end_t = std::chrono::steady_clock::now();
      uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end_t - start).count();
      stats[(int)txn].record(ok, us);
      total_txns++;
   }

   auto t1 = std::chrono::steady_clock::now();
   double elapsed = std::chrono::duration<double>(t1 - t0).count();
   print_stats("sqlite", N, elapsed, total_txns, stats);
}

// (psqlite custom-parser engine removed — replaced by btree_psitri.cpp approach.
//  The "sqlite" engine now uses psitri-backed SQLite when linked with psitri-sqlite.)

#if 0 // Dead code — psqlite custom parser removed
static void psqlite_populate(void* db, uint32_t N, std::mt19937& rng) {
   auto t0 = std::chrono::steady_clock::now();

   const uint32_t batch = 500;
   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO subscriber VALUES ";
      for (uint32_t i = base; i < end; i++) {
         if (i > base) sql += ',';
         sql += '(';
         sql += std::to_string(i) + ",'" + zero_pad(i) + "'";
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() & 1);
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() % 16);
         for (int b = 0; b < 10; b++) sql += ',' + std::to_string(rng() % 256);
         sql += ',' + std::to_string(rng() % 0x7FFFFFFF);
         sql += ',' + std::to_string(rng() % 0x7FFFFFFF);
         sql += ')';
      }
      psqlite_exec_ok(db, sql.c_str());
   }

   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO access_info VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_records = 1 + (rng() % 4);
         for (int t = 1; t <= n_records; t++) {
            if (!first) sql += ',';
            first = false;
            sql += '(' + std::to_string(i) + ',' + std::to_string(t);
            sql += ',' + std::to_string(rng() % 256);
            sql += ',' + std::to_string(rng() % 256);
            sql += ",'" + std::string(1 + rng() % 3, 'A' + rng() % 26) + "'";
            sql += ",'" + std::string(1 + rng() % 5, 'a' + rng() % 26) + "'";
            sql += ')';
         }
      }
      psqlite_exec_ok(db, sql.c_str());
   }

   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO special_facility VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_records = 1 + (rng() % 4);
         for (int t = 1; t <= n_records; t++) {
            if (!first) sql += ',';
            first = false;
            int is_active = (rng() % 100 < 85) ? 1 : 0;
            sql += '(' + std::to_string(i) + ',' + std::to_string(t);
            sql += ',' + std::to_string(is_active);
            sql += ',' + std::to_string(rng() % 256);
            sql += ',' + std::to_string(rng() % 256);
            sql += ",'" + std::string(1 + rng() % 5, 'x' + rng() % 3) + "'";
            sql += ')';
         }
      }
      psqlite_exec_ok(db, sql.c_str());
   }

   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);
      std::string sql = "INSERT INTO call_forwarding VALUES ";
      bool first = true;
      for (uint32_t i = base; i < end; i++) {
         int n_sf = 1 + (rng() % 4);
         for (int sf = 1; sf <= n_sf; sf++) {
            int n_cf = rng() % 4;
            int start_times[] = {0, 8, 16};
            for (int c = 0; c < n_cf; c++) {
               if (!first) sql += ',';
               first = false;
               int st = start_times[c];
               int et = st + 1 + (rng() % 8);
               sql += '(' + std::to_string(i) + ',' + std::to_string(sf);
               sql += ',' + std::to_string(st);
               sql += ',' + std::to_string(et);
               sql += ",'" + zero_pad(1 + rng() % N) + "'";
               sql += ')';
            }
         }
      }
      if (!first) psqlite_exec_ok(db, sql.c_str());
   }

   auto t1 = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(t1 - t0).count();
   std::printf("  Populated %u subscribers in %.2f s (%.0f rows/s)\n", N, secs, N / secs);
}

struct PsqliteStmts {
   psqlite3_stmt* get_subscriber = nullptr;
   psqlite3_stmt* get_access = nullptr;
   psqlite3_stmt* get_new_dest = nullptr;
   psqlite3_stmt* update_loc_sel = nullptr;
   psqlite3_stmt* update_loc_upd = nullptr;
   psqlite3_stmt* insert_cf = nullptr;
   psqlite3_stmt* delete_cf = nullptr;
   psqlite3_stmt* update_sub_bit = nullptr;
   psqlite3_stmt* update_sub_sf = nullptr;

   void prepare(psqlite3* db) {
      auto prep = [&](const char* sql, psqlite3_stmt** out) {
         if (psqlite3_prepare_v2(db, sql, -1, out, nullptr) != PSQLITE_OK) {
            std::fprintf(stderr, "psqlite prepare failed: %s\n  sql: %s\n",
                         psqlite3_errmsg(db), sql);
            std::exit(1);
         }
      };
      prep("SELECT * FROM subscriber WHERE s_id = ?", &get_subscriber);
      prep("SELECT data1, data2, data3, data4 FROM access_info WHERE s_id = ? AND ai_type = ?", &get_access);
      prep("SELECT cf.numberx FROM special_facility sf, call_forwarding cf "
           "WHERE sf.s_id = ? AND sf.sf_type = ? AND sf.is_active = 1 "
           "AND cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type "
           "AND cf.start_time <= ? AND cf.end_time > ?", &get_new_dest);
      prep("SELECT s_id FROM subscriber WHERE sub_nbr = ?", &update_loc_sel);
      prep("UPDATE subscriber SET vlr_location = ? WHERE s_id = ?", &update_loc_upd);
      prep("INSERT OR IGNORE INTO call_forwarding VALUES (?, ?, ?, ?, ?)", &insert_cf);
      prep("DELETE FROM call_forwarding WHERE s_id = ? AND sf_type = ? AND start_time = ?", &delete_cf);
      prep("UPDATE subscriber SET bit_1 = ? WHERE s_id = ?", &update_sub_bit);
      prep("UPDATE special_facility SET data_a = ? WHERE s_id = ? AND sf_type = ?", &update_sub_sf);
   }

   ~PsqliteStmts() {
      psqlite3_finalize(get_subscriber);
      psqlite3_finalize(get_access);
      psqlite3_finalize(get_new_dest);
      psqlite3_finalize(update_loc_sel);
      psqlite3_finalize(update_loc_upd);
      psqlite3_finalize(insert_cf);
      psqlite3_finalize(delete_cf);
      psqlite3_finalize(update_sub_bit);
      psqlite3_finalize(update_sub_sf);
   }
};

static void run_psqlite_benchmark(psqlite3* db, const Config& cfg) {
   uint32_t N = cfg.num_subscribers;

   PsqliteStmts ps;
   ps.prepare(db);

   std::mt19937 rng(42);
   TxnStats stats[7] = {};

   auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::seconds(cfg.duration_secs);
   uint64_t total_txns = 0;
   auto t0 = std::chrono::steady_clock::now();

   while (std::chrono::steady_clock::now() < deadline) {
      TxnType txn = pick_txn(rng);
      uint32_t s_id = 1 + (rng() % N);
      int sf_type = 1 + (rng() % 4);

      auto start = std::chrono::steady_clock::now();
      bool ok = false;

      switch (txn) {
         case TxnType::GET_SUBSCRIBER_DATA: {
            psqlite3_reset(ps.get_subscriber);
            psqlite3_bind_int(ps.get_subscriber, 1, s_id);
            ok = psqlite3_step(ps.get_subscriber) == PSQLITE_ROW;
            break;
         }
         case TxnType::GET_ACCESS_DATA: {
            int ai_type = 1 + rng() % 4;
            psqlite3_reset(ps.get_access);
            psqlite3_bind_int(ps.get_access, 1, s_id);
            psqlite3_bind_int(ps.get_access, 2, ai_type);
            psqlite3_step(ps.get_access);
            ok = true;
            break;
         }
         case TxnType::GET_NEW_DESTINATION: {
            int start_time = rng() % 24;
            psqlite3_reset(ps.get_new_dest);
            psqlite3_bind_int(ps.get_new_dest, 1, s_id);
            psqlite3_bind_int(ps.get_new_dest, 2, sf_type);
            psqlite3_bind_int(ps.get_new_dest, 3, start_time);
            psqlite3_bind_int(ps.get_new_dest, 4, start_time);
            psqlite3_step(ps.get_new_dest);
            ok = true;
            break;
         }
         case TxnType::UPDATE_LOCATION: {
            auto sub_nbr = zero_pad(s_id);
            uint32_t vlr_loc = rng() % 0x7FFFFFFF;
            psqlite3_reset(ps.update_loc_sel);
            psqlite3_bind_text(ps.update_loc_sel, 1, sub_nbr.c_str(), -1, PSQLITE_TRANSIENT);
            if (psqlite3_step(ps.update_loc_sel) == PSQLITE_ROW) {
               int sid = psqlite3_column_int(ps.update_loc_sel, 0);
               psqlite3_reset(ps.update_loc_upd);
               psqlite3_bind_int(ps.update_loc_upd, 1, vlr_loc);
               psqlite3_bind_int(ps.update_loc_upd, 2, sid);
               psqlite3_step(ps.update_loc_upd);
               ok = true;
            }
            break;
         }
         case TxnType::INSERT_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            int end_time = (rng() % 3) * 8 + 1 + rng() % 8;
            auto numberx = zero_pad(1 + rng() % N);
            psqlite3_reset(ps.insert_cf);
            psqlite3_bind_int(ps.insert_cf, 1, s_id);
            psqlite3_bind_int(ps.insert_cf, 2, sf_type);
            psqlite3_bind_int(ps.insert_cf, 3, start_time);
            psqlite3_bind_int(ps.insert_cf, 4, end_time);
            psqlite3_bind_text(ps.insert_cf, 5, numberx.c_str(), -1, PSQLITE_TRANSIENT);
            psqlite3_step(ps.insert_cf);
            ok = true;
            break;
         }
         case TxnType::DELETE_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            psqlite3_reset(ps.delete_cf);
            psqlite3_bind_int(ps.delete_cf, 1, s_id);
            psqlite3_bind_int(ps.delete_cf, 2, sf_type);
            psqlite3_bind_int(ps.delete_cf, 3, start_time);
            psqlite3_step(ps.delete_cf);
            ok = true;
            break;
         }
         case TxnType::UPDATE_SUBSCRIBER: {
            int bit_1 = rng() & 1;
            int data_a = rng() % 256;
            psqlite3_reset(ps.update_sub_bit);
            psqlite3_bind_int(ps.update_sub_bit, 1, bit_1);
            psqlite3_bind_int(ps.update_sub_bit, 2, s_id);
            psqlite3_step(ps.update_sub_bit);
            psqlite3_reset(ps.update_sub_sf);
            psqlite3_bind_int(ps.update_sub_sf, 1, data_a);
            psqlite3_bind_int(ps.update_sub_sf, 2, s_id);
            psqlite3_bind_int(ps.update_sub_sf, 3, sf_type);
            psqlite3_step(ps.update_sub_sf);
            ok = true;
            break;
         }
      }

      auto end_t = std::chrono::steady_clock::now();
      uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end_t - start).count();
      stats[(int)txn].record(ok, us);
      total_txns++;
   }

   auto t1 = std::chrono::steady_clock::now();
   double elapsed = std::chrono::duration<double>(t1 - t0).count();
   print_stats("psqlite", N, elapsed, total_txns, stats);
}
#endif // Dead code — psqlite custom parser removed

// ---------------------------------------------------------------------------
// Native psitri engine (bypasses DuckDB entirely)
// ---------------------------------------------------------------------------

// Root assignments for native engine
static constexpr uint32_t ROOT_SUBSCRIBER     = 1;
static constexpr uint32_t ROOT_ACCESS_INFO    = 2;
static constexpr uint32_t ROOT_SPECIAL_FAC    = 3;
static constexpr uint32_t ROOT_CALL_FWD       = 4;
static constexpr uint32_t ROOT_IDX_SUB_NBR    = 5;

using CV = psitri_sql::ColumnValue;
using ST = psitri_sql::SqlType;

// Subscriber value layout: sub_nbr, bit_1..10, hex_1..10, byte2_1..10, msc_location, vlr_location
static const std::vector<ST> sub_val_types = {
   ST::VARCHAR,
   ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT,
   ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT,
   ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT,
   ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT,
   ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT,
   ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::SMALLINT,
   ST::INTEGER, ST::INTEGER,
};

// Access info value: data1, data2, data3, data4
static const std::vector<ST> ai_val_types = {
   ST::SMALLINT, ST::SMALLINT, ST::VARCHAR, ST::VARCHAR
};

// Special facility value: is_active, error_cntrl, data_a, data_b
static const std::vector<ST> sf_val_types = {
   ST::SMALLINT, ST::SMALLINT, ST::SMALLINT, ST::VARCHAR
};

// Call forwarding value: end_time, numberx
static const std::vector<ST> cf_val_types = {
   ST::SMALLINT, ST::VARCHAR
};

static std::string make_sub_key(int32_t s_id) {
   return psitri_sql::encode_key({CV::make_int(ST::INTEGER, s_id)});
}

static std::string make_ai_key(int32_t s_id, int16_t ai_type) {
   return psitri_sql::encode_key({
      CV::make_int(ST::INTEGER, s_id), CV::make_int(ST::SMALLINT, ai_type)});
}

static std::string make_sf_key(int32_t s_id, int16_t sf_type) {
   return psitri_sql::encode_key({
      CV::make_int(ST::INTEGER, s_id), CV::make_int(ST::SMALLINT, sf_type)});
}

static std::string make_cf_key(int32_t s_id, int16_t sf_type, int16_t start_time) {
   return psitri_sql::encode_key({
      CV::make_int(ST::INTEGER, s_id), CV::make_int(ST::SMALLINT, sf_type),
      CV::make_int(ST::SMALLINT, start_time)});
}

static void native_populate(psitri::dwal::dwal_database& dwal_db,
                             uint32_t N, std::mt19937& rng) {
   auto t0 = std::chrono::steady_clock::now();

   const uint32_t batch = 500;
   for (uint32_t base = 1; base <= N; base += batch) {
      uint32_t end = std::min(base + batch, N + 1);

      // Batch writes in a multi-root transaction
      auto tx = dwal_db.start_transaction(
         {ROOT_SUBSCRIBER, ROOT_ACCESS_INFO, ROOT_SPECIAL_FAC,
          ROOT_CALL_FWD, ROOT_IDX_SUB_NBR});

      for (uint32_t i = base; i < end; i++) {
         // Subscriber
         auto sub_nbr = zero_pad(i);
         std::vector<CV> vals;
         vals.reserve(33);
         vals.push_back(CV::make_varchar(sub_nbr));
         for (int b = 0; b < 10; b++) vals.push_back(CV::make_int(ST::SMALLINT, rng() & 1));
         for (int b = 0; b < 10; b++) vals.push_back(CV::make_int(ST::SMALLINT, rng() % 16));
         for (int b = 0; b < 10; b++) vals.push_back(CV::make_int(ST::SMALLINT, rng() % 256));
         vals.push_back(CV::make_int(ST::INTEGER, rng() % 0x7FFFFFFF));
         vals.push_back(CV::make_int(ST::INTEGER, rng() % 0x7FFFFFFF));

         auto key = make_sub_key(i);
         tx.upsert(ROOT_SUBSCRIBER, key, psitri_sql::encode_value(vals));

         // Sub_nbr index: sub_nbr → encoded s_id key
         auto idx_key = psitri_sql::encode_key({CV::make_varchar(sub_nbr)});
         tx.upsert(ROOT_IDX_SUB_NBR, idx_key, key);

         // Access info: 1-4 per subscriber
         int n_ai = 1 + (rng() % 4);
         for (int t = 1; t <= n_ai; t++) {
            std::vector<CV> ai_vals = {
               CV::make_int(ST::SMALLINT, rng() % 256),
               CV::make_int(ST::SMALLINT, rng() % 256),
               CV::make_varchar(std::string(1 + rng() % 3, 'A' + rng() % 26)),
               CV::make_varchar(std::string(1 + rng() % 5, 'a' + rng() % 26)),
            };
            tx.upsert(ROOT_ACCESS_INFO, make_ai_key(i, t),
                       psitri_sql::encode_value(ai_vals));
         }

         // Special facility: 1-4 per subscriber
         int n_sf = 1 + (rng() % 4);
         for (int t = 1; t <= n_sf; t++) {
            int is_active = (rng() % 100 < 85) ? 1 : 0;
            std::vector<CV> sf_vals = {
               CV::make_int(ST::SMALLINT, is_active),
               CV::make_int(ST::SMALLINT, rng() % 256),
               CV::make_int(ST::SMALLINT, rng() % 256),
               CV::make_varchar(std::string(1 + rng() % 5, 'x' + rng() % 3)),
            };
            tx.upsert(ROOT_SPECIAL_FAC, make_sf_key(i, t),
                       psitri_sql::encode_value(sf_vals));
         }

         // Call forwarding: 0-3 per special_facility
         n_sf = 1 + (rng() % 4);
         for (int sf = 1; sf <= n_sf; sf++) {
            int n_cf = rng() % 4;
            int start_times[] = {0, 8, 16};
            for (int c = 0; c < n_cf; c++) {
               int st = start_times[c];
               int et = st + 1 + (rng() % 8);
               std::vector<CV> cf_vals = {
                  CV::make_int(ST::SMALLINT, et),
                  CV::make_varchar(zero_pad(1 + rng() % N)),
               };
               tx.upsert(ROOT_CALL_FWD, make_cf_key(i, sf, st),
                          psitri_sql::encode_value(cf_vals));
            }
         }
      }
      tx.commit();
   }

   auto t1 = std::chrono::steady_clock::now();
   double secs = std::chrono::duration<double>(t1 - t0).count();
   std::printf("  Populated %u subscribers in %.2f s (%.0f rows/s)\n", N, secs, N / secs);
}

static void run_native_benchmark(psitri::dwal::dwal_database& dwal_db,
                                  const Config& cfg) {
   uint32_t N = cfg.num_subscribers;
   std::mt19937 rng(42);
   TxnStats stats[7] = {};

   auto deadline = std::chrono::steady_clock::now() +
                   std::chrono::seconds(cfg.duration_secs);
   uint64_t total_txns = 0;
   auto t0 = std::chrono::steady_clock::now();

   while (std::chrono::steady_clock::now() < deadline) {
      TxnType txn = pick_txn(rng);
      uint32_t s_id = 1 + (rng() % N);
      int sf_type = 1 + (rng() % 4);

      auto start = std::chrono::steady_clock::now();
      bool ok = false;

      switch (txn) {
         case TxnType::GET_SUBSCRIBER_DATA: {
            auto r = dwal_db.get_latest(ROOT_SUBSCRIBER, make_sub_key(s_id));
            ok = r.found;
            break;
         }
         case TxnType::GET_ACCESS_DATA: {
            int ai_type = 1 + rng() % 4;
            auto r = dwal_db.get_latest(ROOT_ACCESS_INFO, make_ai_key(s_id, ai_type));
            ok = true;  // match SQL behavior: ok even if not found
            break;
         }
         case TxnType::GET_NEW_DESTINATION: {
            int start_time = rng() % 24;
            // Check special_facility(s_id, sf_type).is_active
            auto sf_r = dwal_db.get_latest(ROOT_SPECIAL_FAC, make_sf_key(s_id, sf_type));
            if (sf_r.found && sf_r.value.is_data() && sf_r.value.data.size() > 0) {
               auto sf_vals = psitri_sql::decode_value(sf_r.value.data, sf_val_types);
               if (!sf_vals[0].is_null && sf_vals[0].i64 == 1) {
                  // Scan call_forwarding for matching entries
                  auto prefix = make_sf_key(s_id, sf_type);  // (s_id, sf_type) prefix
                  auto cursor = dwal_db.create_cursor(ROOT_CALL_FWD,
                                   psitri::dwal::read_mode::latest);
                  auto& mc = cursor.cursor();
                  mc.seek_begin();
                  mc.lower_bound(prefix);
                  while (!mc.is_end()) {
                     auto k = mc.key();
                     if (k.substr(0, prefix.size()) != prefix) break;
                     // Decode CF key to get start_time
                     auto cf_key = psitri_sql::decode_key(k,
                        {ST::INTEGER, ST::SMALLINT, ST::SMALLINT});
                     int16_t cf_st = (int16_t)cf_key[2].i64;
                     if (cf_st <= start_time) {
                        // Check end_time
                        auto src = mc.current_source();
                        std::string_view val_data;
                        std::string val_owned;
                        if (src == psitri::dwal::merge_cursor::source::rw ||
                            src == psitri::dwal::merge_cursor::source::ro) {
                           val_data = mc.current_value().data;
                        } else {
                           auto* tri = mc.tri_cursor();
                           if (tri) {
                              tri->get_value([&](psitri::value_view vv) {
                                 val_owned.assign(reinterpret_cast<const char*>(vv.data()), vv.size());
                              });
                              val_data = val_owned;
                           }
                        }
                        if (!val_data.empty()) {
                           auto cf_vals = psitri_sql::decode_value(val_data, cf_val_types);
                           if (!cf_vals[0].is_null && cf_vals[0].i64 > start_time) {
                              ok = true;
                           }
                        }
                     }
                     mc.next();
                  }
               }
            }
            if (!ok) ok = true;  // match SQL: no error = ok
            break;
         }
         case TxnType::UPDATE_LOCATION: {
            auto sub_nbr = zero_pad(s_id);
            uint32_t vlr_loc = rng() % 0x7FFFFFFF;
            // Look up s_id from sub_nbr index
            auto idx_key = psitri_sql::encode_key({CV::make_varchar(sub_nbr)});
            auto idx_r = dwal_db.get_latest(ROOT_IDX_SUB_NBR, idx_key);
            if (idx_r.found && idx_r.value.is_data() && idx_r.value.data.size() > 0) {
               std::string pk_key(idx_r.value.data);
               // Read subscriber row
               auto sub_r = dwal_db.get_latest(ROOT_SUBSCRIBER, pk_key);
               if (sub_r.found && sub_r.value.is_data() && sub_r.value.data.size() > 0) {
                  auto vals = psitri_sql::decode_value(sub_r.value.data, sub_val_types);
                  // Update vlr_location (last column, index 32)
                  vals[32] = CV::make_int(ST::INTEGER, vlr_loc);
                  auto tx = dwal_db.start_write_transaction(ROOT_SUBSCRIBER);
                  tx.upsert(pk_key, psitri_sql::encode_value(vals));
                  tx.commit();
                  ok = true;
               }
            }
            break;
         }
         case TxnType::INSERT_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            int end_time = (rng() % 3) * 8 + 1 + rng() % 8;
            auto numberx = zero_pad(1 + rng() % N);
            std::vector<CV> cf_vals = {
               CV::make_int(ST::SMALLINT, end_time),
               CV::make_varchar(numberx),
            };
            auto tx = dwal_db.start_write_transaction(ROOT_CALL_FWD);
            tx.upsert(make_cf_key(s_id, sf_type, start_time),
                       psitri_sql::encode_value(cf_vals));
            tx.commit();
            ok = true;
            break;
         }
         case TxnType::DELETE_CALL_FWD: {
            int start_time = (rng() % 3) * 8;
            auto tx = dwal_db.start_write_transaction(ROOT_CALL_FWD);
            tx.remove(make_cf_key(s_id, sf_type, start_time));
            tx.commit();
            ok = true;
            break;
         }
         case TxnType::UPDATE_SUBSCRIBER: {
            int bit_1 = rng() & 1;
            int data_a = rng() % 256;
            auto sub_key = make_sub_key(s_id);
            // Read & update subscriber.bit_1
            auto sub_r = dwal_db.get_latest(ROOT_SUBSCRIBER, sub_key);
            if (sub_r.found && sub_r.value.is_data() && sub_r.value.data.size() > 0) {
               auto vals = psitri_sql::decode_value(sub_r.value.data, sub_val_types);
               vals[1] = CV::make_int(ST::SMALLINT, bit_1);  // bit_1 is index 1
               auto tx = dwal_db.start_write_transaction(ROOT_SUBSCRIBER);
               tx.upsert(sub_key, psitri_sql::encode_value(vals));
               tx.commit();
            }
            // Read & update special_facility.data_a
            auto sf_key = make_sf_key(s_id, sf_type);
            auto sf_r = dwal_db.get_latest(ROOT_SPECIAL_FAC, sf_key);
            if (sf_r.found && sf_r.value.is_data() && sf_r.value.data.size() > 0) {
               auto vals = psitri_sql::decode_value(sf_r.value.data, sf_val_types);
               vals[2] = CV::make_int(ST::SMALLINT, data_a);  // data_a is index 2
               auto tx = dwal_db.start_write_transaction(ROOT_SPECIAL_FAC);
               tx.upsert(sf_key, psitri_sql::encode_value(vals));
               tx.commit();
            }
            ok = true;
            break;
         }
      }

      auto end_t = std::chrono::steady_clock::now();
      uint64_t us = std::chrono::duration_cast<std::chrono::microseconds>(end_t - start).count();
      stats[(int)txn].record(ok, us);
      total_txns++;
   }

   auto t1 = std::chrono::steady_clock::now();
   double elapsed = std::chrono::duration<double>(t1 - t0).count();
   print_stats("native", N, elapsed, total_txns, stats);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
   Config cfg;

   // Simple arg parsing
   for (int i = 1; i < argc; i++) {
      std::string arg = argv[i];
      if (arg == "--engine" && i + 1 < argc) cfg.engine = argv[++i];
      else if (arg == "--subscribers" && i + 1 < argc) cfg.num_subscribers = std::atoi(argv[++i]);
      else if (arg == "--duration" && i + 1 < argc) cfg.duration_secs = std::atoi(argv[++i]);
      else if (arg == "--sync" && i + 1 < argc) cfg.sync = argv[++i];
      else if (arg == "--help") {
         std::printf("Usage: tatp-bench [options]\n"
                     "  --engine <psitri|duckdb|sqlite|native>  Storage engine (default: psitri)\n"
                     "  --subscribers <N>                Number of subscribers (default: 100000)\n"
                     "  --duration <secs>                Benchmark duration (default: 10)\n"
                     "  --sync <off|normal|full|extra>   Sync mode for sqlite engine (default: off)\n");
         return 0;
      }
   }

   std::printf("TATP Benchmark: engine=%s, subscribers=%u, duration=%us, sync=%s\n",
               cfg.engine.c_str(), cfg.num_subscribers, cfg.duration_secs, cfg.sync.c_str());

   duckdb::DBConfig db_config;
   db_config.options.autoload_known_extensions = false;
   db_config.options.autoinstall_known_extensions = false;
   duckdb::DuckDB db(nullptr, &db_config);

   if (cfg.engine == "psitri") {
      psitri_sql::RegisterPsitriStorage(db);
      duckdb::Connection conn(db);

      // Create temp directory for psitri storage
      auto tmp = fs::temp_directory_path() / "tatp_bench_psitri";
      fs::remove_all(tmp);
      fs::create_directories(tmp);
      std::string db_path = (tmp / "tatp.db").string();

      exec_ok(conn, "ATTACH '" + db_path + "' AS tdb (TYPE psitri)");

      std::printf("Creating schema...\n");
      create_schema(conn, "tdb");

      std::printf("Populating data...\n");
      std::mt19937 pop_rng(12345);
      populate(conn, "tdb", cfg.num_subscribers, pop_rng);

      std::printf("Creating indexes...\n");
      exec_ok(conn, "CREATE UNIQUE INDEX idx_sub_nbr ON tdb.main.subscriber(sub_nbr)");

      run_benchmark(conn, "tdb", cfg);

      // Cleanup
      fs::remove_all(tmp);

   } else if (cfg.engine == "duckdb") {
      duckdb::Connection conn(db);

      std::printf("Creating schema...\n");
      create_schema(conn, "");

      std::printf("Populating data...\n");
      std::mt19937 pop_rng(12345);
      populate(conn, "", cfg.num_subscribers, pop_rng);

      run_benchmark(conn, "", cfg);

   } else if (cfg.engine == "sqlite") {
      // Uses psitri-backed SQLite (btree replaced by DWAL) when linked with psitri-sqlite.
      // For comparison with real SQLite, rebuild linking against system SQLite3 instead.
      auto tmp = fs::temp_directory_path() / "tatp_bench_sqlite";
      fs::remove_all(tmp);
      fs::create_directories(tmp);
      std::string db_path = (tmp / "tatp.db").string();

      sqlite3* sdb = nullptr;
      int rc = sqlite3_open(db_path.c_str(), &sdb);
      if (rc != SQLITE_OK) {
         std::fprintf(stderr, "Cannot open SQLite DB: %s\n", sqlite3_errmsg(sdb));
         return 1;
      }

      // Set sync mode via PRAGMA synchronous
      if (cfg.sync == "off")         sqlite_exec_ok(sdb, "PRAGMA synchronous=OFF");
      else if (cfg.sync == "normal") sqlite_exec_ok(sdb, "PRAGMA synchronous=NORMAL");
      else if (cfg.sync == "full")   sqlite_exec_ok(sdb, "PRAGMA synchronous=FULL");
      else if (cfg.sync == "extra")  sqlite_exec_ok(sdb, "PRAGMA synchronous=EXTRA");

      // Use WAL mode for better concurrency (system SQLite default is DELETE)
      sqlite_exec_ok(sdb, "PRAGMA journal_mode=WAL");

      std::printf("Creating schema...\n");
      sqlite_create_schema(sdb);

      std::printf("Populating data...\n");
      std::mt19937 pop_rng(12345);
      sqlite_populate(sdb, cfg.num_subscribers, pop_rng);

      run_sqlite_benchmark(sdb, cfg);

      sqlite3_close(sdb);
      fs::remove_all(tmp);

   } else if (cfg.engine == "native") {
      // Direct psitri DWAL access — no SQL overhead
      auto tmp = fs::temp_directory_path() / "tatp_bench_native";
      fs::remove_all(tmp);
      fs::create_directories(tmp);
      std::string db_path = (tmp / "tatp.db").string();
      std::string wal_path = (tmp / "wal").string();

      auto psi_db = psitri::database::open(db_path, psitri::open_mode::create_or_open);
      psitri::dwal::dwal_config dwal_cfg;
      dwal_cfg.max_rw_entries = 100000;
      auto dwal_db = std::make_shared<psitri::dwal::dwal_database>(
         psi_db, wal_path, dwal_cfg);

      std::printf("Populating data...\n");
      std::mt19937 pop_rng(12345);
      native_populate(*dwal_db, cfg.num_subscribers, pop_rng);

      run_native_benchmark(*dwal_db, cfg);

      dwal_db.reset();
      fs::remove_all(tmp);

   } else {
      std::fprintf(stderr, "Unknown engine: %s\n", cfg.engine.c_str());
      return 1;
   }

   return 0;
}
