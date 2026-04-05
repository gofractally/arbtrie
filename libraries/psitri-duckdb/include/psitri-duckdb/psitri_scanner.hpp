#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <psitri-duckdb/row_encoding.hpp>
#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/dwal_database.hpp>

#include <memory>
#include <optional>

namespace psitri_duckdb {

class PsitriCatalog;

// Bind data passed from PsitriTableEntry::GetScanFunction
struct PsitriScanBindData : public duckdb::FunctionData {
   PsitriCatalog* catalog = nullptr;
   TableMeta      meta;

   // PK filter pushdown from pushdown_complex_filter
   bool has_pk_eq_filter = false;
   std::string pk_eq_key;
   bool has_pk_prefix_filter = false;   // equality on leading PK columns (composite)
   std::string pk_prefix_key;
   bool has_lower_bound = false;
   bool has_upper_bound = false;
   std::string lower_bound_key;
   std::string upper_bound_key;

   // Secondary index filter pushdown
   bool has_index_lookup = false;
   uint32_t index_root = 0;          // root index of the secondary index
   std::string index_lookup_key;     // encoded index key to look up

   duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
   bool Equals(const duckdb::FunctionData& other) const override;
};

// Global state shared across all scan threads (we use single-threaded scan)
struct PsitriScanGlobalState : public duckdb::GlobalTableFunctionState {
   bool     finished = false;
   uint32_t root_index = 0;
   int64_t  next_row_id = 0;  // monotonic row_id counter for key mapping
   duckdb::idx_t MaxThreads() const override { return 1; }
};

// Local state per scan thread
struct PsitriScanLocalState : public duckdb::LocalTableFunctionState {
   std::vector<duckdb::column_t> column_ids;
   bool initialized = false;
   std::optional<psitri::dwal::owned_merge_cursor> merge_cursor;

   // Filter pushdown
   bool has_pk_eq_filter = false;
   std::string pk_eq_key;
   bool has_pk_prefix_filter = false;
   std::string pk_prefix_key;
   bool has_lower_bound = false;
   bool has_upper_bound = false;
   std::string lower_bound_key;
   std::string upper_bound_key;

   // Secondary index filter pushdown
   bool has_index_lookup = false;
   uint32_t index_root = 0;
   std::string index_lookup_key;
};

// The scan function itself
void PsitriScanFunction(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& data,
                        duckdb::DataChunk& output);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PsitriScanInitGlobal(duckdb::ClientContext& context,
                     duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PsitriScanInitLocal(duckdb::ExecutionContext& context,
                    duckdb::TableFunctionInitInput& input,
                    duckdb::GlobalTableFunctionState* global_state);

// Complex filter pushdown: extracts PK equality/range filters for cursor seek
void PsitriPushdownComplexFilter(duckdb::ClientContext& context,
                                 duckdb::LogicalGet& get,
                                 duckdb::FunctionData* bind_data,
                                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters);

} // namespace psitri_duckdb
