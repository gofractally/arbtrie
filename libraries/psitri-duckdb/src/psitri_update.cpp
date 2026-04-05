#include <psitri-duckdb/psitri_update.hpp>
#include <psitri-duckdb/psitri_catalog.hpp>
#include <psitri-duckdb/psitri_transaction.hpp>

#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_context.hpp"

namespace psitri_duckdb {

// Reuse from psitri_insert.cpp
extern ColumnValue extract_column_value(duckdb::Vector& vec, duckdb::idx_t row, SqlType type);

PsitriUpdate::PsitriUpdate(duckdb::LogicalOperator& op,
                           duckdb::TableCatalogEntry& table,
                           TableMeta meta,
                           std::vector<duckdb::PhysicalIndex> columns)
    : duckdb::PhysicalOperator(duckdb::PhysicalOperatorType::EXTENSION,
                               {duckdb::LogicalType::BIGINT}, 0),
      table_(table), meta_(std::move(meta)), columns_(std::move(columns)) {
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
PsitriUpdate::GetGlobalSinkState(duckdb::ClientContext& context) const {
   return duckdb::make_uniq<PsitriUpdateGlobalState>();
}

duckdb::SinkResultType
PsitriUpdate::Sink(duckdb::ExecutionContext& context,
                   duckdb::DataChunk& chunk,
                   duckdb::OperatorSinkInput& input) const {
   auto& gstate = input.global_state.Cast<PsitriUpdateGlobalState>();

   auto& txn     = PsitriTransaction::Get(context.client, table_.catalog);
   auto& root_tx = txn.GetOrCreateRootHandle(meta_.root_index);

   auto pk_indices  = meta_.pk_column_indices();
   auto val_indices = meta_.value_column_indices();
   auto pk_types    = meta_.pk_types();
   auto val_types   = meta_.value_types();

   chunk.Flatten();

   // DuckDB UPDATE chunk layout: [new_val_for_col_0, new_val_for_col_1, ..., row_id]
   // columns_[i].index tells which table column corresponds to chunk.data[i]
   // The last column is always the row_id

   auto& row_id_vec = chunk.data[chunk.ColumnCount() - 1];
   auto row_ids = duckdb::FlatVector::GetData<int64_t>(row_id_vec);

   // Build update_map: table_column_idx -> chunk_column_idx
   std::unordered_map<uint32_t, uint32_t> update_map;
   for (uint32_t i = 0; i < columns_.size(); i++) {
      update_map[columns_[i].index] = i;
   }

   for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
      // Look up the old key by row_id
      auto old_key_ptr = txn.LookupRowKey(meta_.root_index, row_ids[row]);
      if (!old_key_ptr) continue;

      // Read old PK and value from the old key
      auto old_pk_vals = decode_key(*old_key_ptr, pk_types);

      // Read old value columns via DWAL layered lookup
      std::vector<ColumnValue> old_val_vals;
      {
         auto result = root_tx.get(*old_key_ptr);
         if (result.found && result.value.is_data() && result.value.data.size() > 0) {
            old_val_vals = decode_value(result.value.data, val_types);
         }
         if (old_val_vals.empty()) {
            for (auto t : val_types) {
               old_val_vals.push_back(ColumnValue::null_value(t));
            }
         }
      }

      // Build full column list: start with old values, then apply updates
      std::vector<ColumnValue> all_cols(meta_.columns.size());
      {
         uint32_t pk_pos = 0, val_pos = 0;
         for (uint32_t i = 0; i < meta_.columns.size(); i++) {
            if (meta_.columns[i].is_primary_key) {
               all_cols[i] = old_pk_vals[pk_pos++];
            } else {
               all_cols[i] = old_val_vals[val_pos++];
            }
         }
      }

      // Apply updates from the chunk
      for (uint32_t i = 0; i < columns_.size(); i++) {
         uint32_t table_col = columns_[i].index;
         if (table_col < meta_.columns.size()) {
            all_cols[table_col] = extract_column_value(
               chunk.data[i], row, meta_.columns[table_col].type);
         }
      }

      // Build new PK and value
      std::vector<ColumnValue> new_pk_vals;
      for (auto idx : pk_indices) {
         new_pk_vals.push_back(all_cols[idx]);
      }
      std::vector<ColumnValue> new_val_cols;
      for (auto idx : val_indices) {
         new_val_cols.push_back(all_cols[idx]);
      }

      std::string new_key   = encode_key(new_pk_vals);
      std::string new_value = encode_value(new_val_cols);

      // Maintain secondary indexes
      if (!meta_.indexes.empty()) {
         // Build old all_cols for index key computation
         std::vector<ColumnValue> old_all_cols(meta_.columns.size());
         {
            uint32_t pk_pos = 0, val_pos = 0;
            for (uint32_t c = 0; c < meta_.columns.size(); c++) {
               if (meta_.columns[c].is_primary_key)
                  old_all_cols[c] = old_pk_vals[pk_pos++];
               else
                  old_all_cols[c] = old_val_vals[val_pos++];
            }
         }

         for (auto& idx : meta_.indexes) {
            auto& idx_tx = txn.GetOrCreateRootHandle(idx.root_index);

            // Remove old index entry
            std::vector<ColumnValue> old_idx_key_vals;
            for (auto ci : idx.column_indices) old_idx_key_vals.push_back(old_all_cols[ci]);
            std::string old_idx_key = encode_key(old_idx_key_vals);

            // Build new index entry
            std::vector<ColumnValue> new_idx_key_vals;
            for (auto ci : idx.column_indices) new_idx_key_vals.push_back(all_cols[ci]);
            std::string new_idx_key = encode_key(new_idx_key_vals);

            // Only update if index key changed
            if (old_idx_key != new_idx_key) {
               idx_tx.remove(old_idx_key);
               if (idx.is_unique) {
                  auto result = idx_tx.get(new_idx_key);
                  if (result.found && result.value.is_data() &&
                      result.value.data.size() > 0 && result.value.data != new_key) {
                     throw duckdb::ConstraintException(
                        "Duplicate key value violates unique constraint \"%s\"",
                        idx.name);
                  }
               }
               idx_tx.upsert(new_idx_key, new_key);
            } else if (new_key != *old_key_ptr) {
               // PK changed but index key didn't — update the index value (PK pointer)
               idx_tx.upsert(new_idx_key, new_key);
            }
         }
      }

      // If PK changed, remove the old key
      if (new_key != *old_key_ptr) {
         root_tx.remove(*old_key_ptr);
      }

      // Check NOT NULL constraints on updated columns
      for (uint32_t i = 0; i < meta_.columns.size(); i++) {
         if (meta_.columns[i].not_null && all_cols[i].is_null) {
            throw duckdb::ConstraintException(
               "NOT NULL constraint failed: %s.%s",
               meta_.table_name, meta_.columns[i].name);
         }
      }

      root_tx.upsert(new_key, new_value);
      gstate.update_count++;
   }

   return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType
PsitriUpdate::Finalize(duckdb::Pipeline& pipeline, duckdb::Event& event,
                       duckdb::ClientContext& context,
                       duckdb::OperatorSinkFinalizeInput& input) const {
   return duckdb::SinkFinalizeType::READY;
}

duckdb::SourceResultType
PsitriUpdate::GetData(duckdb::ExecutionContext& context,
                      duckdb::DataChunk& chunk,
                      duckdb::OperatorSourceInput& input) const {
   auto& gstate = sink_state->Cast<PsitriUpdateGlobalState>();
   chunk.SetCardinality(1);
   chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.update_count));
   return duckdb::SourceResultType::FINISHED;
}

} // namespace psitri_duckdb
