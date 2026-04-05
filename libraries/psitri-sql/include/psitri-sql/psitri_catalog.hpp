#pragma once

// Include select_statement.hpp first — it includes query_node.hpp which defines
// QueryNode. If we let catalog.hpp pull it in transitively via common.hpp,
// we hit a circular include where QueryNode is still incomplete.
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <psitri/database.hpp>
#include <psitri-sql/row_encoding.hpp>

#include <mutex>
#include <unordered_map>

namespace psitri_sql {

class PsitriSchemaEntry;

// Root 0 is reserved for catalog metadata.
// Keys: "table/{schema}/{name}" -> serialized TableMeta
// Keys: "__next_root" -> uint32_t (next available root index)
static constexpr uint32_t CATALOG_ROOT = 0;
static constexpr uint32_t FIRST_TABLE_ROOT = 1;

// ---------------------------------------------------------------------------
// PsitriCatalog -- the top-level catalog registered via StorageExtension
// ---------------------------------------------------------------------------
class PsitriCatalog : public duckdb::Catalog {
public:
   PsitriCatalog(duckdb::AttachedDatabase& db,
                 std::shared_ptr<psitri::database> storage);
   ~PsitriCatalog() override;

   std::string GetCatalogType() override { return "psitri"; }

   void Initialize(bool load_builtin) override;

   // Schema operations
   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateSchema(duckdb::CatalogTransaction transaction,
                duckdb::CreateSchemaInfo& info) override;

   void ScanSchemas(duckdb::ClientContext& context,
                    std::function<void(duckdb::SchemaCatalogEntry&)> callback) override;

   duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
   GetSchema(duckdb::CatalogTransaction transaction,
             const duckdb::string& schema_name,
             duckdb::OnEntryNotFound if_not_found,
             duckdb::QueryErrorContext error_context = duckdb::QueryErrorContext()) override;

   void DropSchema(duckdb::ClientContext& context, duckdb::DropInfo& info) override;

   // DML planning -- return unique_ptr<PhysicalOperator>
   duckdb::unique_ptr<duckdb::PhysicalOperator>
   PlanInsert(duckdb::ClientContext& context,
              duckdb::LogicalInsert& op,
              duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;

   duckdb::unique_ptr<duckdb::PhysicalOperator>
   PlanDelete(duckdb::ClientContext& context,
              duckdb::LogicalDelete& op,
              duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;

   duckdb::unique_ptr<duckdb::PhysicalOperator>
   PlanUpdate(duckdb::ClientContext& context,
              duckdb::LogicalUpdate& op,
              duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;

   duckdb::unique_ptr<duckdb::PhysicalOperator>
   PlanCreateTableAs(duckdb::ClientContext& context,
                     duckdb::LogicalCreateTable& op,
                     duckdb::unique_ptr<duckdb::PhysicalOperator> plan) override;

   duckdb::unique_ptr<duckdb::LogicalOperator>
   BindCreateIndex(duckdb::Binder& binder, duckdb::CreateStatement& stmt,
                   duckdb::TableCatalogEntry& table,
                   duckdb::unique_ptr<duckdb::LogicalOperator> plan) override;

   duckdb::DatabaseSize GetDatabaseSize(duckdb::ClientContext& context) override;
   bool InMemory() override { return false; }
   std::string GetDBPath() override;

   // Storage access
   std::shared_ptr<psitri::database>& GetStorage() { return storage_; }

   // Catalog metadata operations
   uint32_t AllocateRootIndex();
   void StoreTableMeta(const TableMeta& meta);
   std::optional<TableMeta> LoadTableMeta(const std::string& schema, const std::string& table);
   void RemoveTableMeta(const std::string& schema, const std::string& table);
   std::vector<TableMeta> ListTables(const std::string& schema);

private:
   std::shared_ptr<psitri::database> storage_;
   std::shared_ptr<psitri::write_session> catalog_session_;
   std::mutex schema_lock_;
   std::unordered_map<std::string, duckdb::unique_ptr<PsitriSchemaEntry>> schemas_;
};

// ---------------------------------------------------------------------------
// PsitriSchemaEntry -- represents one schema (default: "main")
// ---------------------------------------------------------------------------
class PsitriSchemaEntry : public duckdb::SchemaCatalogEntry {
public:
   PsitriSchemaEntry(duckdb::Catalog& catalog, duckdb::CreateSchemaInfo& info);

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateTable(duckdb::CatalogTransaction transaction,
               duckdb::BoundCreateTableInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateView(duckdb::CatalogTransaction transaction,
              duckdb::CreateViewInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateFunction(duckdb::CatalogTransaction transaction,
                  duckdb::CreateFunctionInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateIndex(duckdb::CatalogTransaction transaction,
               duckdb::CreateIndexInfo& info,
               duckdb::TableCatalogEntry& table) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateSequence(duckdb::CatalogTransaction transaction,
                  duckdb::CreateSequenceInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateType(duckdb::CatalogTransaction transaction,
              duckdb::CreateTypeInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateCollation(duckdb::CatalogTransaction transaction,
                   duckdb::CreateCollationInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateCopyFunction(duckdb::CatalogTransaction transaction,
                      duckdb::CreateCopyFunctionInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreateTableFunction(duckdb::CatalogTransaction transaction,
                       duckdb::CreateTableFunctionInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   CreatePragmaFunction(duckdb::CatalogTransaction transaction,
                        duckdb::CreatePragmaFunctionInfo& info) override;

   void Scan(duckdb::ClientContext& context, duckdb::CatalogType type,
             const std::function<void(duckdb::CatalogEntry&)>& callback) override;

   void Scan(duckdb::CatalogType type,
             const std::function<void(duckdb::CatalogEntry&)>& callback) override;

   void DropEntry(duckdb::ClientContext& context, duckdb::DropInfo& info) override;

   void Alter(duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) override;

   duckdb::optional_ptr<duckdb::CatalogEntry>
   GetEntry(duckdb::CatalogTransaction transaction,
            duckdb::CatalogType type,
            const duckdb::string& name) override;

   PsitriCatalog& GetPsitriCatalog() {
      return catalog.Cast<PsitriCatalog>();
   }

   // Load a pre-existing table entry (used during Initialize for persistence)
   void LoadTable(const std::string& name,
                  duckdb::unique_ptr<duckdb::CatalogEntry> entry) {
      std::lock_guard lock(tables_lock_);
      tables_[name] = std::move(entry);
   }

private:
   std::mutex tables_lock_;
   std::unordered_map<std::string, duckdb::unique_ptr<duckdb::CatalogEntry>> tables_;
};

// ---------------------------------------------------------------------------
// PsitriTableEntry -- represents one table backed by a psitri root
// ---------------------------------------------------------------------------
class PsitriTableEntry : public duckdb::TableCatalogEntry {
public:
   PsitriTableEntry(duckdb::Catalog& catalog,
                    duckdb::SchemaCatalogEntry& schema,
                    duckdb::CreateTableInfo& info,
                    TableMeta meta);

   duckdb::unique_ptr<duckdb::BaseStatistics>
   GetStatistics(duckdb::ClientContext& context, duckdb::column_t column_id) override;

   duckdb::TableFunction GetScanFunction(duckdb::ClientContext& context,
                                         duckdb::unique_ptr<duckdb::FunctionData>& bind_data) override;

   duckdb::TableStorageInfo GetStorageInfo(duckdb::ClientContext& context) override;

   const TableMeta& GetMeta() const { return meta_; }
   void SetMeta(TableMeta meta) { meta_ = std::move(meta); }

private:
   TableMeta meta_;
};

} // namespace psitri_sql
