#include <psitri-duckdb/row_encoding.hpp>
#include <bit>
#include <cassert>
#include <cstring>

namespace psitri_duckdb {

// --------------------------------------------------------------------------
// Key encoding helpers
// --------------------------------------------------------------------------

static void put_be16(std::string& out, uint16_t v) {
   uint16_t be = __builtin_bswap16(v);
   out.append(reinterpret_cast<const char*>(&be), 2);
}

static void put_be32(std::string& out, uint32_t v) {
   uint32_t be = __builtin_bswap32(v);
   out.append(reinterpret_cast<const char*>(&be), 4);
}

static void put_be64(std::string& out, uint64_t v) {
   uint64_t be = __builtin_bswap64(v);
   out.append(reinterpret_cast<const char*>(&be), 8);
}

static uint16_t read_be16(const char*& p) {
   uint16_t be;
   std::memcpy(&be, p, 2);
   p += 2;
   return __builtin_bswap16(be);
}

static uint32_t read_be32(const char*& p) {
   uint32_t be;
   std::memcpy(&be, p, 4);
   p += 4;
   return __builtin_bswap32(be);
}

static uint64_t read_be64(const char*& p) {
   uint64_t be;
   std::memcpy(&be, p, 8);
   p += 8;
   return __builtin_bswap64(be);
}

// Encode double as sortable uint64:
//   - If positive (sign bit 0): flip sign bit → all positives sort after negatives
//   - If negative (sign bit 1): flip all bits → reverses negative ordering
static uint64_t double_to_sortable(double d) {
   uint64_t u;
   std::memcpy(&u, &d, 8);
   if (u & (uint64_t(1) << 63)) {
      return ~u;  // negative: flip all bits
   } else {
      return u ^ (uint64_t(1) << 63);  // positive: flip sign bit
   }
}

static double sortable_to_double(uint64_t u) {
   if (u & (uint64_t(1) << 63)) {
      u ^= (uint64_t(1) << 63);  // positive: flip sign bit back
   } else {
      u = ~u;  // negative: flip all bits back
   }
   double d;
   std::memcpy(&d, &u, 8);
   return d;
}

// Escaped null encoding for variable-length data:
// Every 0x00 byte is escaped to 0x00 0xFF.
// Terminated by 0x00 0x00.
static void encode_bytes_escaped(std::string_view data, std::string& out) {
   for (char c : data) {
      out.push_back(c);
      if (c == '\0') {
         out.push_back('\xFF');  // escape null
      }
   }
   out.push_back('\0');
   out.push_back('\0');  // terminator
}

static std::string decode_bytes_escaped(const char*& p, const char* end) {
   std::string result;
   while (p + 1 <= end) {
      if (*p == '\0') {
         p++;
         if (p >= end) break;
         if (*p == '\0') {
            p++;  // terminator
            return result;
         } else if (*p == '\xFF') {
            result.push_back('\0');  // escaped null
            p++;
         }
      } else {
         result.push_back(*p);
         p++;
      }
   }
   return result;
}

void encode_key_column(const ColumnValue& val, std::string& out) {
   if (val.is_null) {
      out.push_back('\x00');  // NULL sorts first
      return;
   }
   out.push_back('\x01');  // NOT NULL prefix

   switch (val.type) {
      case SqlType::BOOLEAN:
         out.push_back(val.i64 ? '\x01' : '\x00');
         break;

      case SqlType::TINYINT:
         out.push_back(static_cast<char>(static_cast<uint8_t>(val.i64) ^ 0x80));
         break;

      case SqlType::UTINYINT:
         out.push_back(static_cast<char>(static_cast<uint8_t>(val.i64)));
         break;

      case SqlType::SMALLINT: {
         uint16_t u = static_cast<uint16_t>(static_cast<int16_t>(val.i64));
         put_be16(out, u ^ 0x8000);
         break;
      }

      case SqlType::USMALLINT:
         put_be16(out, static_cast<uint16_t>(val.i64));
         break;

      case SqlType::INTEGER:
      case SqlType::DATE: {
         uint32_t u = static_cast<uint32_t>(static_cast<int32_t>(val.i64));
         put_be32(out, u ^ 0x80000000u);
         break;
      }

      case SqlType::UINTEGER:
         put_be32(out, static_cast<uint32_t>(val.i64));
         break;

      case SqlType::BIGINT:
      case SqlType::TIME:
      case SqlType::TIMESTAMP:
      case SqlType::TIMESTAMP_TZ: {
         uint64_t u = static_cast<uint64_t>(val.i64);
         put_be64(out, u ^ (uint64_t(1) << 63));
         break;
      }

      case SqlType::UBIGINT:
         put_be64(out, static_cast<uint64_t>(val.i64));
         break;

      case SqlType::FLOAT: {
         double d = static_cast<float>(val.f64);
         put_be64(out, double_to_sortable(d));
         break;
      }

      case SqlType::DOUBLE:
         put_be64(out, double_to_sortable(val.f64));
         break;

      case SqlType::VARCHAR:
      case SqlType::BLOB:
         encode_bytes_escaped(val.str, out);
         break;

      case SqlType::HUGEINT: {
         // 128-bit signed: flip sign bit of upper, store big-endian
         uint64_t upper = static_cast<uint64_t>(val.i64) ^ (uint64_t(1) << 63);
         put_be64(out, upper);
         put_be64(out, val.u64_low);
         break;
      }

      case SqlType::UHUGEINT:
         put_be64(out, static_cast<uint64_t>(val.i64));
         put_be64(out, val.u64_low);
         break;

      case SqlType::UUID: {
         // DuckDB UUID: upper has sign bit flipped for sorting
         uint64_t upper = static_cast<uint64_t>(val.i64) ^ (uint64_t(1) << 63);
         put_be64(out, upper);
         put_be64(out, val.u64_low);
         break;
      }

      case SqlType::INTERVAL:
         // INTERVAL is not naturally orderable, but we encode for storage
         put_be32(out, static_cast<uint32_t>(val.interval_months) ^ 0x80000000u);
         put_be32(out, static_cast<uint32_t>(val.interval_days) ^ 0x80000000u);
         put_be64(out, static_cast<uint64_t>(val.interval_micros) ^ (uint64_t(1) << 63));
         break;
   }
}

std::string encode_key(const std::vector<ColumnValue>& columns) {
   std::string out;
   out.reserve(64);
   for (auto& col : columns) {
      encode_key_column(col, out);
   }
   return out;
}

std::vector<ColumnValue> decode_key(std::string_view encoded,
                                    const std::vector<SqlType>& types) {
   std::vector<ColumnValue> result;
   result.reserve(types.size());
   const char* p   = encoded.data();
   const char* end = p + encoded.size();

   for (auto type : types) {
      if (p >= end)
         throw std::runtime_error("decode_key: unexpected end of data");

      uint8_t null_flag = static_cast<uint8_t>(*p++);
      if (null_flag == 0x00) {
         result.push_back(ColumnValue::null_value(type));
         continue;
      }

      ColumnValue val;
      val.type = type;

      switch (type) {
         case SqlType::BOOLEAN:
            val.i64 = (*p++ != '\x00') ? 1 : 0;
            break;

         case SqlType::TINYINT:
            val.i64 = static_cast<int8_t>(static_cast<uint8_t>(*p++) ^ 0x80);
            break;

         case SqlType::UTINYINT:
            val.i64 = static_cast<uint8_t>(*p++);
            break;

         case SqlType::SMALLINT: {
            uint16_t u = read_be16(p) ^ 0x8000;
            val.i64    = static_cast<int16_t>(u);
            break;
         }

         case SqlType::USMALLINT:
            val.i64 = read_be16(p);
            break;

         case SqlType::INTEGER:
         case SqlType::DATE: {
            uint32_t u = read_be32(p) ^ 0x80000000u;
            val.i64    = static_cast<int32_t>(u);
            break;
         }

         case SqlType::UINTEGER:
            val.i64 = read_be32(p);
            break;

         case SqlType::BIGINT:
         case SqlType::TIME:
         case SqlType::TIMESTAMP:
         case SqlType::TIMESTAMP_TZ: {
            uint64_t u = read_be64(p) ^ (uint64_t(1) << 63);
            val.i64    = static_cast<int64_t>(u);
            break;
         }

         case SqlType::UBIGINT:
            val.i64 = static_cast<int64_t>(read_be64(p));
            break;

         case SqlType::FLOAT: {
            uint64_t u = read_be64(p);
            val.f64    = static_cast<float>(sortable_to_double(u));
            break;
         }

         case SqlType::DOUBLE: {
            uint64_t u = read_be64(p);
            val.f64    = sortable_to_double(u);
            break;
         }

         case SqlType::VARCHAR:
         case SqlType::BLOB:
            val.str = decode_bytes_escaped(p, end);
            break;

         case SqlType::HUGEINT:
         case SqlType::UUID: {
            uint64_t upper = read_be64(p) ^ (uint64_t(1) << 63);
            val.i64     = static_cast<int64_t>(upper);
            val.u64_low = read_be64(p);
            break;
         }

         case SqlType::UHUGEINT: {
            val.i64     = static_cast<int64_t>(read_be64(p));
            val.u64_low = read_be64(p);
            break;
         }

         case SqlType::INTERVAL: {
            val.interval_months = static_cast<int32_t>(read_be32(p) ^ 0x80000000u);
            val.interval_days   = static_cast<int32_t>(read_be32(p) ^ 0x80000000u);
            val.interval_micros = static_cast<int64_t>(read_be64(p) ^ (uint64_t(1) << 63));
            break;
         }
      }
      result.push_back(std::move(val));
   }
   return result;
}

// --------------------------------------------------------------------------
// Value encoding
// --------------------------------------------------------------------------

static void put_le32(std::string& out, uint32_t v) {
   out.append(reinterpret_cast<const char*>(&v), 4);
}

static uint32_t read_le32(const char*& p) {
   uint32_t v;
   std::memcpy(&v, p, 4);
   p += 4;
   return v;
}

static void encode_value_column(const ColumnValue& val, std::string& out) {
   switch (val.type) {
      case SqlType::BOOLEAN: {
         put_le32(out, 1);
         out.push_back(val.i64 ? '\x01' : '\x00');
         break;
      }
      case SqlType::TINYINT:
      case SqlType::UTINYINT: {
         put_le32(out, 1);
         out.push_back(static_cast<char>(val.i64));
         break;
      }
      case SqlType::SMALLINT:
      case SqlType::USMALLINT: {
         put_le32(out, 2);
         int16_t v = static_cast<int16_t>(val.i64);
         out.append(reinterpret_cast<const char*>(&v), 2);
         break;
      }
      case SqlType::INTEGER:
      case SqlType::UINTEGER:
      case SqlType::DATE: {
         put_le32(out, 4);
         int32_t v = static_cast<int32_t>(val.i64);
         out.append(reinterpret_cast<const char*>(&v), 4);
         break;
      }
      case SqlType::BIGINT:
      case SqlType::UBIGINT:
      case SqlType::TIME:
      case SqlType::TIMESTAMP:
      case SqlType::TIMESTAMP_TZ: {
         put_le32(out, 8);
         out.append(reinterpret_cast<const char*>(&val.i64), 8);
         break;
      }
      case SqlType::FLOAT: {
         put_le32(out, 4);
         float f = static_cast<float>(val.f64);
         out.append(reinterpret_cast<const char*>(&f), 4);
         break;
      }
      case SqlType::DOUBLE: {
         put_le32(out, 8);
         out.append(reinterpret_cast<const char*>(&val.f64), 8);
         break;
      }
      case SqlType::VARCHAR:
      case SqlType::BLOB:
         put_le32(out, static_cast<uint32_t>(val.str.size()));
         out.append(val.str);
         break;
      case SqlType::HUGEINT:
      case SqlType::UHUGEINT:
      case SqlType::UUID: {
         put_le32(out, 16);
         out.append(reinterpret_cast<const char*>(&val.i64), 8);
         out.append(reinterpret_cast<const char*>(&val.u64_low), 8);
         break;
      }
      case SqlType::INTERVAL: {
         put_le32(out, 16);
         out.append(reinterpret_cast<const char*>(&val.interval_months), 4);
         out.append(reinterpret_cast<const char*>(&val.interval_days), 4);
         out.append(reinterpret_cast<const char*>(&val.interval_micros), 8);
         break;
      }
   }
}

static ColumnValue decode_value_column(SqlType type, const char*& p, const char* end) {
   uint32_t len = read_le32(p);
   ColumnValue val;
   val.type = type;

   switch (type) {
      case SqlType::BOOLEAN:
         val.i64 = (*p != '\x00') ? 1 : 0;
         break;
      case SqlType::TINYINT:
         val.i64 = static_cast<int8_t>(*p);
         break;
      case SqlType::UTINYINT:
         val.i64 = static_cast<uint8_t>(*p);
         break;
      case SqlType::SMALLINT: {
         int16_t v;
         std::memcpy(&v, p, 2);
         val.i64 = v;
         break;
      }
      case SqlType::USMALLINT: {
         uint16_t v;
         std::memcpy(&v, p, 2);
         val.i64 = v;
         break;
      }
      case SqlType::INTEGER:
      case SqlType::DATE: {
         int32_t v;
         std::memcpy(&v, p, 4);
         val.i64 = v;
         break;
      }
      case SqlType::UINTEGER: {
         uint32_t v;
         std::memcpy(&v, p, 4);
         val.i64 = v;
         break;
      }
      case SqlType::BIGINT:
      case SqlType::TIME:
      case SqlType::TIMESTAMP:
      case SqlType::TIMESTAMP_TZ:
         std::memcpy(&val.i64, p, 8);
         break;
      case SqlType::UBIGINT: {
         uint64_t v;
         std::memcpy(&v, p, 8);
         val.i64 = static_cast<int64_t>(v);
         break;
      }
      case SqlType::FLOAT: {
         float f;
         std::memcpy(&f, p, 4);
         val.f64 = f;
         break;
      }
      case SqlType::DOUBLE:
         std::memcpy(&val.f64, p, 8);
         break;
      case SqlType::VARCHAR:
      case SqlType::BLOB:
         val.str.assign(p, len);
         break;
      case SqlType::HUGEINT:
      case SqlType::UHUGEINT:
      case SqlType::UUID:
         std::memcpy(&val.i64, p, 8);
         std::memcpy(&val.u64_low, p + 8, 8);
         break;
      case SqlType::INTERVAL:
         std::memcpy(&val.interval_months, p, 4);
         std::memcpy(&val.interval_days, p + 4, 4);
         std::memcpy(&val.interval_micros, p + 8, 8);
         break;
   }
   p += len;
   return val;
}

std::string encode_value(const std::vector<ColumnValue>& columns) {
   // Null bitmap: ceil(n/8) bytes
   size_t bitmap_bytes = (columns.size() + 7) / 8;
   std::string out(bitmap_bytes, '\0');

   for (size_t i = 0; i < columns.size(); i++) {
      if (columns[i].is_null) {
         out[i / 8] |= (1 << (i % 8));
      }
   }

   for (auto& col : columns) {
      if (!col.is_null) {
         encode_value_column(col, out);
      }
   }
   return out;
}

std::vector<ColumnValue> decode_value(std::string_view encoded,
                                      const std::vector<SqlType>& types) {
   size_t n             = types.size();
   size_t bitmap_bytes  = (n + 7) / 8;
   const char* p        = encoded.data();
   const char* end      = p + encoded.size();

   // Handle short encodings: if the encoded data is shorter than the bitmap
   // (e.g., columns were added after this row was written), treat extras as null.
   if (encoded.size() < bitmap_bytes) {
      std::vector<ColumnValue> result;
      result.reserve(n);
      for (size_t i = 0; i < n; i++) {
         if (i / 8 < encoded.size()) {
            bool is_null = (p[i / 8] >> (i % 8)) & 1;
            if (is_null) {
               result.push_back(ColumnValue::null_value(types[i]));
               continue;
            }
         }
         result.push_back(ColumnValue::null_value(types[i]));
      }
      return result;
   }

   const char* bitmap   = p;
   p += bitmap_bytes;

   std::vector<ColumnValue> result;
   result.reserve(n);

   for (size_t i = 0; i < n; i++) {
      bool is_null = (bitmap[i / 8] >> (i % 8)) & 1;
      if (is_null || p >= end) {
         result.push_back(ColumnValue::null_value(types[i]));
      } else {
         result.push_back(decode_value_column(types[i], p, end));
      }
   }
   return result;
}

// --------------------------------------------------------------------------
// TableMeta helpers
// --------------------------------------------------------------------------

std::vector<uint32_t> TableMeta::pk_column_indices() const {
   std::vector<uint32_t> indices;
   for (uint32_t i = 0; i < columns.size(); i++) {
      if (columns[i].is_primary_key)
         indices.push_back(i);
   }
   return indices;
}

std::vector<uint32_t> TableMeta::value_column_indices() const {
   std::vector<uint32_t> indices;
   for (uint32_t i = 0; i < columns.size(); i++) {
      if (!columns[i].is_primary_key)
         indices.push_back(i);
   }
   return indices;
}

std::vector<SqlType> TableMeta::pk_types() const {
   std::vector<SqlType> types;
   for (auto& col : columns) {
      if (col.is_primary_key) types.push_back(col.type);
   }
   return types;
}

std::vector<SqlType> TableMeta::value_types() const {
   std::vector<SqlType> types;
   for (auto& col : columns) {
      if (!col.is_primary_key) types.push_back(col.type);
   }
   return types;
}

// --------------------------------------------------------------------------
// Table metadata serialization
//
// Binary format:
//   schema_name_len:2LE  schema_name:bytes
//   table_name_len:2LE   table_name:bytes
//   root_index:4LE
//   num_columns:2LE
//   for each column:
//     name_len:2LE  name:bytes
//     type:1  is_pk:1
// --------------------------------------------------------------------------

std::string serialize_table_meta(const TableMeta& meta) {
   std::string out;
   out.reserve(256);

   auto put16 = [&](uint16_t v) {
      out.append(reinterpret_cast<const char*>(&v), 2);
   };
   auto put_str = [&](const std::string& s) {
      put16(static_cast<uint16_t>(s.size()));
      out.append(s);
   };

   put_str(meta.schema_name);
   put_str(meta.table_name);
   put_le32(out, meta.root_index);
   put16(static_cast<uint16_t>(meta.columns.size()));

   for (auto& col : meta.columns) {
      put_str(col.name);
      out.push_back(static_cast<char>(col.type));
      uint8_t flags = 0;
      if (col.is_primary_key) flags |= 0x01;
      if (col.not_null)       flags |= 0x02;
      out.push_back(static_cast<char>(flags));
   }

   // Indexes
   put16(static_cast<uint16_t>(meta.indexes.size()));
   for (auto& idx : meta.indexes) {
      put_str(idx.name);
      put_le32(out, idx.root_index);
      put16(static_cast<uint16_t>(idx.column_indices.size()));
      for (auto ci : idx.column_indices) {
         put16(static_cast<uint16_t>(ci));
      }
      out.push_back(idx.is_unique ? '\x01' : '\x00');
   }

   return out;
}

TableMeta deserialize_table_meta(std::string_view data) {
   const char* p   = data.data();
   const char* end = p + data.size();
   TableMeta meta;

   auto get16 = [&]() -> uint16_t {
      uint16_t v;
      std::memcpy(&v, p, 2);
      p += 2;
      return v;
   };
   auto get_str = [&]() -> std::string {
      uint16_t len = get16();
      std::string s(p, len);
      p += len;
      return s;
   };

   meta.schema_name = get_str();
   meta.table_name  = get_str();
   meta.root_index  = read_le32(p);
   uint16_t ncols   = get16();

   meta.columns.resize(ncols);
   for (uint16_t i = 0; i < ncols; i++) {
      meta.columns[i].name           = get_str();
      meta.columns[i].type           = static_cast<SqlType>(*p++);
      uint8_t flags = static_cast<uint8_t>(*p++);
      meta.columns[i].is_primary_key = (flags & 0x01) != 0;
      meta.columns[i].not_null       = (flags & 0x02) != 0;
   }

   // Indexes (optional, for backward compat)
   if (p < end) {
      uint16_t nidx = get16();
      meta.indexes.resize(nidx);
      for (uint16_t i = 0; i < nidx; i++) {
         meta.indexes[i].name = get_str();
         meta.indexes[i].root_index = read_le32(p);
         uint16_t ncols_idx = get16();
         meta.indexes[i].column_indices.resize(ncols_idx);
         for (uint16_t j = 0; j < ncols_idx; j++) {
            meta.indexes[i].column_indices[j] = get16();
         }
         meta.indexes[i].is_unique = (*p++ != '\x00');
      }
   }

   return meta;
}

} // namespace psitri_duckdb
