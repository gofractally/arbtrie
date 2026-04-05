#include <psitri-sql/psitri_insert.hpp>
#include <psitri-sql/psitri_catalog.hpp>
#include <psitri-sql/psitri_transaction.hpp>

#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_context.hpp"

#include <psitri/transaction.hpp>

namespace psitri_sql {

// ===========================================================================
// Helper: extract a ColumnValue from a DuckDB Vector at a given row
// ===========================================================================

ColumnValue extract_column_value(duckdb::Vector& vec, duckdb::idx_t row,
                                 SqlType type) {
   if (duckdb::FlatVector::IsNull(vec, row)) {
      return ColumnValue::null_value(type);
   }
   switch (type) {
      case SqlType::BOOLEAN:
         return ColumnValue::make_bool(duckdb::FlatVector::GetData<bool>(vec)[row]);
      case SqlType::TINYINT:
         return ColumnValue::make_int(SqlType::TINYINT,
                                      duckdb::FlatVector::GetData<int8_t>(vec)[row]);
      case SqlType::SMALLINT:
         return ColumnValue::make_int(SqlType::SMALLINT,
                                      duckdb::FlatVector::GetData<int16_t>(vec)[row]);
      case SqlType::INTEGER:
         return ColumnValue::make_int(SqlType::INTEGER,
                                      duckdb::FlatVector::GetData<int32_t>(vec)[row]);
      case SqlType::BIGINT:
         return ColumnValue::make_int(SqlType::BIGINT,
                                      duckdb::FlatVector::GetData<int64_t>(vec)[row]);
      case SqlType::FLOAT:
         return ColumnValue::make_float(duckdb::FlatVector::GetData<float>(vec)[row]);
      case SqlType::DOUBLE:
         return ColumnValue::make_double(duckdb::FlatVector::GetData<double>(vec)[row]);
      case SqlType::VARCHAR:
      case SqlType::BLOB: {
         auto str = duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row];
         return (type == SqlType::VARCHAR)
                   ? ColumnValue::make_varchar(str.GetString())
                   : ColumnValue::make_blob(str.GetString());
      }
      case SqlType::UTINYINT:
         return ColumnValue::make_uint(SqlType::UTINYINT,
                                       duckdb::FlatVector::GetData<uint8_t>(vec)[row]);
      case SqlType::USMALLINT:
         return ColumnValue::make_uint(SqlType::USMALLINT,
                                       duckdb::FlatVector::GetData<uint16_t>(vec)[row]);
      case SqlType::UINTEGER:
         return ColumnValue::make_uint(SqlType::UINTEGER,
                                       duckdb::FlatVector::GetData<uint32_t>(vec)[row]);
      case SqlType::UBIGINT:
         return ColumnValue::make_uint(SqlType::UBIGINT,
                                       duckdb::FlatVector::GetData<uint64_t>(vec)[row]);
      case SqlType::DATE:
         return ColumnValue::make_date(duckdb::FlatVector::GetData<int32_t>(vec)[row]);
      case SqlType::TIME:
         return ColumnValue::make_time(duckdb::FlatVector::GetData<int64_t>(vec)[row]);
      case SqlType::TIMESTAMP:
         return ColumnValue::make_timestamp(duckdb::FlatVector::GetData<int64_t>(vec)[row]);
      case SqlType::TIMESTAMP_TZ:
         return ColumnValue::make_timestamp_tz(duckdb::FlatVector::GetData<int64_t>(vec)[row]);
      case SqlType::HUGEINT: {
         auto h = duckdb::FlatVector::GetData<duckdb::hugeint_t>(vec)[row];
         return ColumnValue::make_hugeint(h.upper, h.lower);
      }
      case SqlType::UHUGEINT: {
         auto h = duckdb::FlatVector::GetData<duckdb::uhugeint_t>(vec)[row];
         return ColumnValue::make_uhugeint(h.upper, h.lower);
      }
      case SqlType::UUID: {
         auto h = duckdb::FlatVector::GetData<duckdb::hugeint_t>(vec)[row];
         return ColumnValue::make_uuid(h.upper, h.lower);
      }
      case SqlType::INTERVAL: {
         auto iv = duckdb::FlatVector::GetData<duckdb::interval_t>(vec)[row];
         return ColumnValue::make_interval(iv.months, iv.days, iv.micros);
      }
   }
   return ColumnValue::null_value(type);
}

// ===========================================================================
// PsitriInsert
// ===========================================================================

PsitriInsert::PsitriInsert(duckdb::LogicalOperator& op,
                           duckdb::TableCatalogEntry& table,
                           TableMeta meta,
                           duckdb::physical_index_vector_t<duckdb::idx_t> column_index_map)
    : duckdb::PhysicalOperator(duckdb::PhysicalOperatorType::EXTENSION,
                               {duckdb::LogicalType::BIGINT},
                               0),
      table_(table), meta_(std::move(meta)),
      column_index_map_(std::move(column_index_map)) {
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
PsitriInsert::GetGlobalSinkState(duckdb::ClientContext& context) const {
   auto state     = duckdb::make_uniq<PsitriInsertGlobalState>();
   state->catalog = &table_.catalog.Cast<PsitriCatalog>();
   state->meta    = meta_;
   return state;
}

duckdb::SinkResultType
PsitriInsert::Sink(duckdb::ExecutionContext& context,
                   duckdb::DataChunk& chunk,
                   duckdb::OperatorSinkInput& input) const {
   auto& gstate = input.global_state.Cast<PsitriInsertGlobalState>();
   auto& meta   = gstate.meta;

   // Get the psitri transaction for this table's root
   auto& txn    = PsitriTransaction::Get(context.client, table_.catalog);
   auto& root_tx = txn.GetOrCreateRootTransaction(meta.root_index);

   auto pk_indices  = meta.pk_column_indices();
   auto val_indices = meta.value_column_indices();

   // Flatten the input chunk for easier access
   chunk.Flatten();

   for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
      // Extract primary key columns
      std::vector<ColumnValue> pk_vals;
      pk_vals.reserve(pk_indices.size());
      for (auto idx : pk_indices) {
         pk_vals.push_back(
            extract_column_value(chunk.data[idx], row, meta.columns[idx].type));
      }

      // Extract value columns
      std::vector<ColumnValue> val_cols;
      val_cols.reserve(val_indices.size());
      for (auto idx : val_indices) {
         val_cols.push_back(
            extract_column_value(chunk.data[idx], row, meta.columns[idx].type));
      }

      // Check NOT NULL constraints
      for (uint32_t i = 0; i < meta.columns.size(); i++) {
         if (meta.columns[i].not_null) {
            bool is_pk = meta.columns[i].is_primary_key;
            // Find the value in pk_vals or val_cols
            bool found_null = false;
            if (is_pk) {
               for (size_t pi = 0; pi < pk_indices.size(); pi++) {
                  if (pk_indices[pi] == i && pk_vals[pi].is_null) {
                     found_null = true;
                     break;
                  }
               }
            } else {
               for (size_t vi = 0; vi < val_indices.size(); vi++) {
                  if (val_indices[vi] == i && val_cols[vi].is_null) {
                     found_null = true;
                     break;
                  }
               }
            }
            if (found_null) {
               throw duckdb::ConstraintException(
                  "NOT NULL constraint failed: %s.%s",
                  meta.table_name, meta.columns[i].name);
            }
         }
      }

      // Encode and upsert
      std::string key   = encode_key(pk_vals);
      std::string value = encode_value(val_cols);

      // Check UNIQUE constraints and maintain indexes
      for (auto& idx : meta.indexes) {
         // Build index key from indexed columns
         std::vector<ColumnValue> all_cols(meta.columns.size());
         {
            uint32_t pk_pos = 0, val_pos = 0;
            for (uint32_t c = 0; c < meta.columns.size(); c++) {
               if (meta.columns[c].is_primary_key)
                  all_cols[c] = pk_vals[pk_pos++];
               else
                  all_cols[c] = val_cols[val_pos++];
            }
         }
         std::vector<ColumnValue> idx_key_vals;
         for (auto ci : idx.column_indices) {
            idx_key_vals.push_back(all_cols[ci]);
         }
         std::string idx_key = encode_key(idx_key_vals);

         auto& idx_tx = txn.GetOrCreateRootTransaction(idx.root_index);
         if (idx.is_unique) {
            // Zero-copy uniqueness check via read_cursor callback
            bool is_duplicate = false;
            auto rc = idx_tx.read_cursor();
            if (rc.seek(idx_key)) {
               rc.get_value([&](psitri::value_view existing_pk) {
                  if (existing_pk.size() > 0 && existing_pk != key) {
                     is_duplicate = true;
                  }
               });
            }
            if (is_duplicate) {
               throw duckdb::ConstraintException(
                  "Duplicate key value violates unique constraint \"%s\"",
                  idx.name);
            }
         }
         idx_tx.upsert(idx_key, key);
      }

      root_tx.upsert(key, value);
      gstate.insert_count++;
   }

   return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType
PsitriInsert::Finalize(duckdb::Pipeline& pipeline, duckdb::Event& event,
                       duckdb::ClientContext& context,
                       duckdb::OperatorSinkFinalizeInput& input) const {
   return duckdb::SinkFinalizeType::READY;
}

duckdb::SourceResultType
PsitriInsert::GetData(duckdb::ExecutionContext& context,
                      duckdb::DataChunk& chunk,
                      duckdb::OperatorSourceInput& input) const {
   auto& gstate = sink_state->Cast<PsitriInsertGlobalState>();
   chunk.SetCardinality(1);
   chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
   return duckdb::SourceResultType::FINISHED;
}

} // namespace psitri_sql
