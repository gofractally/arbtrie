#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/index_vector.hpp"

#include <psitri-duckdb/row_encoding.hpp>

namespace psitri_duckdb {

class PsitriCatalog;

// Global sink state for insert
struct PsitriInsertGlobalState : public duckdb::GlobalSinkState {
   PsitriCatalog* catalog = nullptr;
   TableMeta      meta;
   int64_t        insert_count = 0;
};

// Physical operator for INSERT INTO psitri tables
class PsitriInsert : public duckdb::PhysicalOperator {
public:
   PsitriInsert(duckdb::LogicalOperator& op,
                duckdb::TableCatalogEntry& table,
                TableMeta meta,
                duckdb::physical_index_vector_t<duckdb::idx_t> column_index_map);

   // Sink interface
   duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                               duckdb::DataChunk& chunk,
                               duckdb::OperatorSinkInput& input) const override;

   duckdb::SinkFinalizeType Finalize(duckdb::Pipeline& pipeline,
                                     duckdb::Event& event,
                                     duckdb::ClientContext& context,
                                     duckdb::OperatorSinkFinalizeInput& input) const override;

   duckdb::unique_ptr<duckdb::GlobalSinkState>
   GetGlobalSinkState(duckdb::ClientContext& context) const override;

   // Source interface
   duckdb::SourceResultType GetData(duckdb::ExecutionContext& context,
                                    duckdb::DataChunk& chunk,
                                    duckdb::OperatorSourceInput& input) const override;

   bool IsSink() const override { return true; }
   bool IsSource() const override { return true; }
   bool ParallelSink() const override { return false; }

private:
   duckdb::TableCatalogEntry& table_;
   TableMeta meta_;
   duckdb::physical_index_vector_t<duckdb::idx_t> column_index_map_;
};

} // namespace psitri_duckdb
