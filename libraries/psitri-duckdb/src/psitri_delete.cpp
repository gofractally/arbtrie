#include <psitri-duckdb/psitri_delete.hpp>
#include <psitri-duckdb/psitri_catalog.hpp>
#include <psitri-duckdb/psitri_transaction.hpp>

#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_context.hpp"

namespace psitri_duckdb {

PsitriDelete::PsitriDelete(duckdb::LogicalOperator& op,
                           duckdb::TableCatalogEntry& table,
                           TableMeta meta)
    : duckdb::PhysicalOperator(duckdb::PhysicalOperatorType::EXTENSION,
                               {duckdb::LogicalType::BIGINT}, 0),
      table_(table), meta_(std::move(meta)) {
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
PsitriDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
   return duckdb::make_uniq<PsitriDeleteGlobalState>();
}

duckdb::SinkResultType
PsitriDelete::Sink(duckdb::ExecutionContext& context,
                   duckdb::DataChunk& chunk,
                   duckdb::OperatorSinkInput& input) const {
   auto& gstate = input.global_state.Cast<PsitriDeleteGlobalState>();

   auto& txn     = PsitriTransaction::Get(context.client, table_.catalog);
   auto& root_tx = txn.GetOrCreateRootHandle(meta_.root_index);

   chunk.Flatten();

   // The row_id is always the last column in the chunk from DuckDB's delete plan
   auto& row_id_vec = chunk.data[chunk.ColumnCount() - 1];
   auto row_ids = duckdb::FlatVector::GetData<int64_t>(row_id_vec);

   auto pk_types = meta_.pk_types();
   auto val_types = meta_.value_types();

   for (duckdb::idx_t row = 0; row < chunk.size(); row++) {
      auto key_ptr = txn.LookupRowKey(meta_.root_index, row_ids[row]);
      if (key_ptr) {
         // Remove index entries before removing the row
         if (!meta_.indexes.empty()) {
            auto pk_vals = decode_key(*key_ptr, pk_types);
            // Read old value via DWAL layered lookup
            std::vector<ColumnValue> val_vals;
            auto result = root_tx.get(*key_ptr);
            if (result.found && result.value.is_data() && result.value.data.size() > 0) {
               val_vals = decode_value(result.value.data, val_types);
            }
            if (val_vals.empty()) {
               for (auto t : val_types) val_vals.push_back(ColumnValue::null_value(t));
            }
            std::vector<ColumnValue> all_cols(meta_.columns.size());
            uint32_t pk_pos = 0, val_pos = 0;
            for (uint32_t c = 0; c < meta_.columns.size(); c++) {
               if (meta_.columns[c].is_primary_key)
                  all_cols[c] = pk_vals[pk_pos++];
               else
                  all_cols[c] = val_vals[val_pos++];
            }
            for (auto& idx : meta_.indexes) {
               std::vector<ColumnValue> idx_key_vals;
               for (auto ci : idx.column_indices) idx_key_vals.push_back(all_cols[ci]);
               auto& idx_tx = txn.GetOrCreateRootHandle(idx.root_index);
               idx_tx.remove(encode_key(idx_key_vals));
            }
         }
         root_tx.remove(*key_ptr);
         gstate.delete_count++;
      }
   }

   return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType
PsitriDelete::Finalize(duckdb::Pipeline& pipeline, duckdb::Event& event,
                       duckdb::ClientContext& context,
                       duckdb::OperatorSinkFinalizeInput& input) const {
   return duckdb::SinkFinalizeType::READY;
}

duckdb::SourceResultType
PsitriDelete::GetData(duckdb::ExecutionContext& context,
                      duckdb::DataChunk& chunk,
                      duckdb::OperatorSourceInput& input) const {
   auto& gstate = sink_state->Cast<PsitriDeleteGlobalState>();
   chunk.SetCardinality(1);
   chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.delete_count));
   return duckdb::SourceResultType::FINISHED;
}

} // namespace psitri_duckdb
