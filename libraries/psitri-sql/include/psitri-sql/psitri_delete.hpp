#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <psitri-sql/row_encoding.hpp>

namespace psitri_sql {

struct PsitriDeleteGlobalState : public duckdb::GlobalSinkState {
   int64_t delete_count = 0;
};

class PsitriDelete : public duckdb::PhysicalOperator {
public:
   PsitriDelete(duckdb::LogicalOperator& op,
                duckdb::TableCatalogEntry& table,
                TableMeta meta);

   duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                               duckdb::DataChunk& chunk,
                               duckdb::OperatorSinkInput& input) const override;

   duckdb::SinkFinalizeType Finalize(duckdb::Pipeline& pipeline,
                                     duckdb::Event& event,
                                     duckdb::ClientContext& context,
                                     duckdb::OperatorSinkFinalizeInput& input) const override;

   duckdb::unique_ptr<duckdb::GlobalSinkState>
   GetGlobalSinkState(duckdb::ClientContext& context) const override;

   duckdb::SourceResultType GetData(duckdb::ExecutionContext& context,
                                    duckdb::DataChunk& chunk,
                                    duckdb::OperatorSourceInput& input) const override;

   bool IsSink() const override { return true; }
   bool IsSource() const override { return true; }
   bool ParallelSink() const override { return false; }

private:
   duckdb::TableCatalogEntry& table_;
   TableMeta meta_;
};

} // namespace psitri_sql
