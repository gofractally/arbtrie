#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/index_vector.hpp"

#include <psitri-duckdb/row_encoding.hpp>

namespace psitri_duckdb {

struct PsitriUpdateGlobalState : public duckdb::GlobalSinkState {
   int64_t update_count = 0;
};

class PsitriUpdate : public duckdb::PhysicalOperator {
public:
   PsitriUpdate(duckdb::LogicalOperator& op,
                duckdb::TableCatalogEntry& table,
                TableMeta meta,
                std::vector<duckdb::PhysicalIndex> columns);

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
   std::vector<duckdb::PhysicalIndex> columns_;  // which columns are being updated
};

} // namespace psitri_duckdb
