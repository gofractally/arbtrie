#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <stdexcept>

namespace psitri_duckdb {

// --------------------------------------------------------------------------
// Memcomparable key encoding
//
// Encodes SQL values into byte strings such that lexicographic comparison
// of the encoded bytes matches the SQL ordering. This allows psitri's
// ordered key/value store to serve as a primary key index directly.
//
// Encoding rules:
//   INTEGER (4 bytes):  XOR sign bit, store big-endian
//   BIGINT  (8 bytes):  XOR sign bit, store big-endian
//   DOUBLE  (8 bytes):  IEEE 754 → sortable unsigned, big-endian
//   VARCHAR (variable): escaped null encoding, null-terminated
//   BOOLEAN (1 byte):   0x00 or 0x01
//   NULL:               single 0x00 byte (sorts before all non-null)
//   NOT NULL prefix:    single 0x01 byte before the encoded value
//
// Composite keys: concatenate individual column encodings.
// --------------------------------------------------------------------------

// SQL type tags matching DuckDB's LogicalTypeId (subset we support)
enum class SqlType : uint8_t {
   BOOLEAN    = 1,
   TINYINT    = 2,
   SMALLINT   = 3,
   INTEGER    = 4,
   BIGINT     = 5,
   FLOAT      = 6,
   DOUBLE     = 7,
   VARCHAR    = 8,
   BLOB       = 9,
   // Unsigned integers
   UTINYINT   = 10,
   USMALLINT  = 11,
   UINTEGER   = 12,
   UBIGINT    = 13,
   // Date/time types (stored as integers internally by DuckDB)
   DATE       = 14,   // int32_t days since epoch
   TIME       = 15,   // int64_t microseconds since midnight
   TIMESTAMP  = 16,   // int64_t microseconds since epoch
   TIMESTAMP_TZ = 17, // same storage as TIMESTAMP
   INTERVAL   = 18,   // months:int32 + days:int32 + micros:int64
   // Large integers
   HUGEINT    = 19,   // 128-bit signed integer
   UHUGEINT   = 20,   // 128-bit unsigned integer
   // UUID stored as hugeint in DuckDB
   UUID       = 21,
};

// A single column value for encoding/decoding
struct ColumnValue {
   SqlType     type;
   bool        is_null = false;
   // Storage: integer types in i64, float types in f64, strings in str
   // For HUGEINT/UUID: upper 64 bits in i64, lower 64 bits in u64_low
   int64_t     i64 = 0;
   uint64_t    u64_low = 0;  // lower 64 bits for 128-bit types
   double      f64 = 0.0;
   std::string str;
   // INTERVAL fields
   int32_t     interval_months = 0;
   int32_t     interval_days = 0;
   int64_t     interval_micros = 0;

   static ColumnValue null_value(SqlType t) {
      ColumnValue v;
      v.type    = t;
      v.is_null = true;
      return v;
   }
   static ColumnValue make_int(SqlType t, int64_t val) {
      ColumnValue v;
      v.type = t;
      v.i64  = val;
      return v;
   }
   static ColumnValue make_bool(bool val) {
      ColumnValue v;
      v.type = SqlType::BOOLEAN;
      v.i64  = val ? 1 : 0;
      return v;
   }
   static ColumnValue make_double(double val) {
      ColumnValue v;
      v.type = SqlType::DOUBLE;
      v.f64  = val;
      return v;
   }
   static ColumnValue make_float(float val) {
      ColumnValue v;
      v.type = SqlType::FLOAT;
      v.f64  = val;
      return v;
   }
   static ColumnValue make_varchar(std::string val) {
      ColumnValue v;
      v.type = SqlType::VARCHAR;
      v.str  = std::move(val);
      return v;
   }
   static ColumnValue make_blob(std::string val) {
      ColumnValue v;
      v.type = SqlType::BLOB;
      v.str  = std::move(val);
      return v;
   }
   static ColumnValue make_uint(SqlType t, uint64_t val) {
      ColumnValue v;
      v.type = t;
      v.i64  = static_cast<int64_t>(val);  // stored in i64 for simplicity
      return v;
   }
   static ColumnValue make_hugeint(int64_t upper, uint64_t lower) {
      ColumnValue v;
      v.type    = SqlType::HUGEINT;
      v.i64     = upper;
      v.u64_low = lower;
      return v;
   }
   static ColumnValue make_uhugeint(uint64_t upper, uint64_t lower) {
      ColumnValue v;
      v.type    = SqlType::UHUGEINT;
      v.i64     = static_cast<int64_t>(upper);
      v.u64_low = lower;
      return v;
   }
   static ColumnValue make_uuid(int64_t upper, uint64_t lower) {
      ColumnValue v;
      v.type    = SqlType::UUID;
      v.i64     = upper;
      v.u64_low = lower;
      return v;
   }
   static ColumnValue make_interval(int32_t months, int32_t days, int64_t micros) {
      ColumnValue v;
      v.type            = SqlType::INTERVAL;
      v.interval_months = months;
      v.interval_days   = days;
      v.interval_micros = micros;
      return v;
   }
   // DATE/TIME/TIMESTAMP all stored as integers
   static ColumnValue make_date(int32_t days) {
      return make_int(SqlType::DATE, days);
   }
   static ColumnValue make_time(int64_t micros) {
      return make_int(SqlType::TIME, micros);
   }
   static ColumnValue make_timestamp(int64_t micros) {
      return make_int(SqlType::TIMESTAMP, micros);
   }
   static ColumnValue make_timestamp_tz(int64_t micros) {
      return make_int(SqlType::TIMESTAMP_TZ, micros);
   }
};

// Encode a single column value in memcomparable format (appends to out)
void encode_key_column(const ColumnValue& val, std::string& out);

// Encode a composite key (multiple columns) into a single byte string
std::string encode_key(const std::vector<ColumnValue>& columns);

// Decode a composite key back into column values
// types must be provided since encoding is not self-describing for fixed-width
std::vector<ColumnValue> decode_key(std::string_view encoded,
                                    const std::vector<SqlType>& types);

// --------------------------------------------------------------------------
// Value encoding (non-key columns)
//
// Simpler format — no ordering requirement. Length-prefixed columns with
// a null bitmap.
//
// Layout:
//   [null_bitmap: ceil(n/8) bytes] [col0_len:4LE col0_data] [col1_len:4LE col1_data] ...
//   Null columns are skipped (no length/data written).
// --------------------------------------------------------------------------

std::string encode_value(const std::vector<ColumnValue>& columns);
std::vector<ColumnValue> decode_value(std::string_view encoded,
                                      const std::vector<SqlType>& types);

// --------------------------------------------------------------------------
// Table metadata serialization (stored in catalog root)
// --------------------------------------------------------------------------

struct ColumnDef {
   std::string name;
   SqlType     type;
   bool        is_primary_key = false;
   bool        not_null = false;
};

struct IndexMeta {
   std::string              name;
   uint32_t                 root_index = 0;
   std::vector<uint32_t>    column_indices;  // which columns are indexed
   bool                     is_unique = false;
};

struct TableMeta {
   std::string            table_name;
   std::string            schema_name = "main";
   uint32_t               root_index = 0;  // which psitri root stores data
   std::vector<ColumnDef> columns;
   std::vector<IndexMeta> indexes;

   // Derived helpers
   std::vector<uint32_t> pk_column_indices() const;
   std::vector<uint32_t> value_column_indices() const;
   std::vector<SqlType>  pk_types() const;
   std::vector<SqlType>  value_types() const;
};

std::string   serialize_table_meta(const TableMeta& meta);
TableMeta     deserialize_table_meta(std::string_view data);

} // namespace psitri_duckdb
