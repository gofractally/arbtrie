#include <psitri-sql/psitri_catalog.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri-sql/psitri_transaction.hpp>
#include <psitri-sql/psitri_scanner.hpp>
#include <psitri-sql/psitri_insert.hpp>
#include <psitri-sql/psitri_delete.hpp>
#include <psitri-sql/psitri_update.hpp>

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

#include <psitri/write_session.hpp>
#include <psitri/transaction.hpp>
#include <psitri/cursor.hpp>

namespace psitri_sql {

// Helper to convert DuckDB LogicalType to our SqlType
static SqlType duckdb_type_to_sql(const duckdb::LogicalType& type) {
   switch (type.id()) {
      case duckdb::LogicalTypeId::BOOLEAN:       return SqlType::BOOLEAN;
      case duckdb::LogicalTypeId::TINYINT:       return SqlType::TINYINT;
      case duckdb::LogicalTypeId::SMALLINT:      return SqlType::SMALLINT;
      case duckdb::LogicalTypeId::INTEGER:       return SqlType::INTEGER;
      case duckdb::LogicalTypeId::BIGINT:        return SqlType::BIGINT;
      case duckdb::LogicalTypeId::FLOAT:         return SqlType::FLOAT;
      case duckdb::LogicalTypeId::DOUBLE:        return SqlType::DOUBLE;
      case duckdb::LogicalTypeId::VARCHAR:       return SqlType::VARCHAR;
      case duckdb::LogicalTypeId::BLOB:          return SqlType::BLOB;
      case duckdb::LogicalTypeId::UTINYINT:      return SqlType::UTINYINT;
      case duckdb::LogicalTypeId::USMALLINT:     return SqlType::USMALLINT;
      case duckdb::LogicalTypeId::UINTEGER:      return SqlType::UINTEGER;
      case duckdb::LogicalTypeId::UBIGINT:       return SqlType::UBIGINT;
      case duckdb::LogicalTypeId::DATE:          return SqlType::DATE;
      case duckdb::LogicalTypeId::TIME:          return SqlType::TIME;
      case duckdb::LogicalTypeId::TIMESTAMP:     return SqlType::TIMESTAMP;
      case duckdb::LogicalTypeId::TIMESTAMP_TZ:  return SqlType::TIMESTAMP_TZ;
      case duckdb::LogicalTypeId::INTERVAL:      return SqlType::INTERVAL;
      case duckdb::LogicalTypeId::HUGEINT:       return SqlType::HUGEINT;
      case duckdb::LogicalTypeId::UHUGEINT:      return SqlType::UHUGEINT;
      case duckdb::LogicalTypeId::UUID:          return SqlType::UUID;
      default:
         throw duckdb::NotImplementedException(
            "PsiTri SQL: unsupported column type: %s", type.ToString());
   }
}

static duckdb::LogicalType sql_type_to_duckdb(SqlType type) {
   switch (type) {
      case SqlType::BOOLEAN:      return duckdb::LogicalType::BOOLEAN;
      case SqlType::TINYINT:      return duckdb::LogicalType::TINYINT;
      case SqlType::SMALLINT:     return duckdb::LogicalType::SMALLINT;
      case SqlType::INTEGER:      return duckdb::LogicalType::INTEGER;
      case SqlType::BIGINT:       return duckdb::LogicalType::BIGINT;
      case SqlType::FLOAT:        return duckdb::LogicalType::FLOAT;
      case SqlType::DOUBLE:       return duckdb::LogicalType::DOUBLE;
      case SqlType::VARCHAR:      return duckdb::LogicalType::VARCHAR;
      case SqlType::BLOB:         return duckdb::LogicalType::BLOB;
      case SqlType::UTINYINT:     return duckdb::LogicalType::UTINYINT;
      case SqlType::USMALLINT:    return duckdb::LogicalType::USMALLINT;
      case SqlType::UINTEGER:     return duckdb::LogicalType::UINTEGER;
      case SqlType::UBIGINT:      return duckdb::LogicalType::UBIGINT;
      case SqlType::DATE:         return duckdb::LogicalType::DATE;
      case SqlType::TIME:         return duckdb::LogicalType::TIME;
      case SqlType::TIMESTAMP:    return duckdb::LogicalType::TIMESTAMP;
      case SqlType::TIMESTAMP_TZ: return duckdb::LogicalType::TIMESTAMP_TZ;
      case SqlType::INTERVAL:     return duckdb::LogicalType::INTERVAL;
      case SqlType::HUGEINT:      return duckdb::LogicalType::HUGEINT;
      case SqlType::UHUGEINT:     return duckdb::LogicalType::UHUGEINT;
      case SqlType::UUID:         return duckdb::LogicalType::UUID;
   }
   throw duckdb::InternalException("Unknown SqlType");
}

// ===========================================================================
// PsitriCatalog
// ===========================================================================

PsitriCatalog::PsitriCatalog(duckdb::AttachedDatabase& db,
                             std::shared_ptr<psitri::database> storage)
    : duckdb::Catalog(db), storage_(std::move(storage)) {
   catalog_session_ = storage_->start_write_session();
}

PsitriCatalog::~PsitriCatalog() = default;

void PsitriCatalog::Initialize(bool load_builtin) {
   auto system_txn = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());

   // Create the default "main" schema
   duckdb::CreateSchemaInfo info;
   info.schema       = "main";
   info.on_conflict  = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
   CreateSchema(system_txn, info);

   // Reload all persisted tables into the catalog
   auto& schema = schemas_["main"];
   auto tables = ListTables("main");
   for (auto& meta : tables) {
      auto table_info  = duckdb::make_uniq<duckdb::CreateTableInfo>();
      table_info->table  = meta.table_name;
      table_info->schema = "main";
      for (auto& col : meta.columns) {
         table_info->columns.AddColumn(duckdb::ColumnDefinition(
            col.name, sql_type_to_duckdb(col.type)));
      }
      auto entry = duckdb::make_uniq<PsitriTableEntry>(
         *this, *schema, *table_info, std::move(meta));
      schema->LoadTable(table_info->table, std::move(entry));
   }
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriCatalog::CreateSchema(duckdb::CatalogTransaction transaction,
                            duckdb::CreateSchemaInfo& info) {
   std::lock_guard lock(schema_lock_);
   auto it = schemas_.find(info.schema);
   if (it != schemas_.end()) {
      if (info.on_conflict == duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
         throw duckdb::CatalogException("Schema \"%s\" already exists", info.schema);
      }
      return it->second.get();
   }
   auto entry = duckdb::make_uniq<PsitriSchemaEntry>(*this, info);
   auto* ptr  = entry.get();
   schemas_[info.schema] = std::move(entry);
   return ptr;
}

void PsitriCatalog::ScanSchemas(duckdb::ClientContext& context,
                                std::function<void(duckdb::SchemaCatalogEntry&)> callback) {
   std::lock_guard lock(schema_lock_);
   for (auto& [name, schema] : schemas_) {
      callback(*schema);
   }
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
PsitriCatalog::GetSchema(duckdb::CatalogTransaction transaction,
                         const duckdb::string& schema_name,
                         duckdb::OnEntryNotFound if_not_found,
                         duckdb::QueryErrorContext error_context) {
   std::lock_guard lock(schema_lock_);
   auto it = schemas_.find(schema_name);
   if (it != schemas_.end()) {
      return it->second.get();
   }
   if (if_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
      throw duckdb::CatalogException("Schema \"%s\" not found", schema_name);
   }
   return nullptr;
}

void PsitriCatalog::DropSchema(duckdb::ClientContext& context, duckdb::DropInfo& info) {
   throw duckdb::NotImplementedException("DROP SCHEMA not yet supported");
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PsitriCatalog::PlanInsert(duckdb::ClientContext& context,
                          duckdb::LogicalInsert& op,
                          duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
   auto& table_entry = op.table.Cast<PsitriTableEntry>();
   auto insert = duckdb::make_uniq<PsitriInsert>(
      op, op.table, table_entry.GetMeta(), op.column_index_map);
   if (plan) {
      insert->children.push_back(std::move(plan));
   }
   return std::move(insert);
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PsitriCatalog::PlanDelete(duckdb::ClientContext& context,
                          duckdb::LogicalDelete& op,
                          duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
   auto& table_entry = op.table.Cast<PsitriTableEntry>();
   auto del = duckdb::make_uniq<PsitriDelete>(op, op.table, table_entry.GetMeta());
   if (plan) {
      del->children.push_back(std::move(plan));
   }
   return std::move(del);
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PsitriCatalog::PlanUpdate(duckdb::ClientContext& context,
                          duckdb::LogicalUpdate& op,
                          duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
   auto& table_entry = op.table.Cast<PsitriTableEntry>();
   auto upd = duckdb::make_uniq<PsitriUpdate>(
      op, op.table, table_entry.GetMeta(), op.columns);
   if (plan) {
      upd->children.push_back(std::move(plan));
   }
   return std::move(upd);
}

duckdb::unique_ptr<duckdb::PhysicalOperator>
PsitriCatalog::PlanCreateTableAs(duckdb::ClientContext& context,
                                 duckdb::LogicalCreateTable& op,
                                 duckdb::unique_ptr<duckdb::PhysicalOperator> plan) {
   auto& schema = op.schema.Cast<PsitriSchemaEntry>();
   auto system_txn = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
   schema.CreateTable(system_txn, *op.info);

   // Get the created table entry
   auto table_name = op.info->Base().table;
   auto table_entry_ptr = schema.GetEntry(system_txn, duckdb::CatalogType::TABLE_ENTRY, table_name);
   if (!table_entry_ptr) {
      throw duckdb::InternalException("CREATE TABLE AS: table was not created");
   }
   auto& table_entry = table_entry_ptr->Cast<PsitriTableEntry>();

   // Build identity column index map
   duckdb::physical_index_vector_t<duckdb::idx_t> column_index_map;
   for (duckdb::idx_t i = 0; i < table_entry.GetMeta().columns.size(); i++) {
      column_index_map.push_back(i);
   }

   auto insert = duckdb::make_uniq<PsitriInsert>(
      op, table_entry, table_entry.GetMeta(), std::move(column_index_map));
   if (plan) {
      insert->children.push_back(std::move(plan));
   }
   return std::move(insert);
}

duckdb::unique_ptr<duckdb::LogicalOperator>
PsitriCatalog::BindCreateIndex(duckdb::Binder& binder, duckdb::CreateStatement& stmt,
                               duckdb::TableCatalogEntry& table,
                               duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
   // Handle index creation ourselves, bypassing DuckDB's IndexBinder/ART pipeline.
   auto create_index_info = duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateIndexInfo>(
      std::move(stmt.info));

   auto& psitri_table = table.Cast<PsitriTableEntry>();
   auto meta = psitri_table.GetMeta();

   // Check if index already exists
   for (auto& idx : meta.indexes) {
      if (idx.name == create_index_info->index_name) {
         if (create_index_info->on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
            return std::move(plan);
         }
         throw duckdb::CatalogException("Index \"%s\" already exists", create_index_info->index_name);
      }
   }

   // Build IndexMeta
   IndexMeta idx_meta;
   idx_meta.name = create_index_info->index_name;
   idx_meta.root_index = AllocateRootIndex();
   idx_meta.is_unique = (create_index_info->constraint_type == duckdb::IndexConstraintType::UNIQUE);

   // Resolve column names from expressions to column indices
   for (auto& expr : create_index_info->expressions) {
      if (expr->type == duckdb::ExpressionType::COLUMN_REF) {
         auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
         auto col_name = col_ref.GetColumnName();
         bool found = false;
         for (uint32_t i = 0; i < meta.columns.size(); i++) {
            if (meta.columns[i].name == col_name) {
               idx_meta.column_indices.push_back(i);
               found = true;
               break;
            }
         }
         if (!found) {
            throw duckdb::BinderException("Column \"%s\" not found in table \"%s\"",
                                          col_name, meta.table_name);
         }
      } else {
         throw duckdb::NotImplementedException("Expression indexes not supported in psitri");
      }
   }

   // Use the active PsitriTransaction's write session to build the index
   auto& txn = PsitriTransaction::Get(binder.context, *this);
   auto& idx_tx = txn.GetOrCreateRootTransaction(idx_meta.root_index);

   // Read existing data and populate index
   auto rs = GetStorage()->start_read_session();
   auto cursor = rs->create_cursor(meta.root_index);
   auto pk_types = meta.pk_types();
   auto val_types = meta.value_types();

   cursor.seek_begin();
   while (!cursor.is_end()) {
      auto key_view = cursor.key();
      auto pk_vals = decode_key(key_view, pk_types);
      // Zero-copy value read via callback
      std::vector<ColumnValue> val_vals;
      cursor.get_value([&](psitri::value_view vv) {
         if (vv.size() > 0) {
            val_vals = decode_value(vv, val_types);
         }
      });
      if (val_vals.empty()) {
         for (auto t : val_types) val_vals.push_back(ColumnValue::null_value(t));
      }

      std::vector<ColumnValue> all_cols(meta.columns.size());
      uint32_t pk_pos = 0, val_pos = 0;
      for (uint32_t i = 0; i < meta.columns.size(); i++) {
         if (meta.columns[i].is_primary_key)
            all_cols[i] = pk_vals[pk_pos++];
         else
            all_cols[i] = val_vals[val_pos++];
      }

      std::vector<ColumnValue> idx_key_vals;
      for (auto ci : idx_meta.column_indices) {
         idx_key_vals.push_back(all_cols[ci]);
      }
      idx_tx.upsert(encode_key(idx_key_vals), std::string(key_view));
      cursor.next();
   }

   // Update table metadata
   meta.indexes.push_back(std::move(idx_meta));
   StoreTableMeta(meta);
   psitri_table.SetMeta(meta);

   return std::move(plan);
}

duckdb::DatabaseSize PsitriCatalog::GetDatabaseSize(duckdb::ClientContext& context) {
   duckdb::DatabaseSize size;
   size.free_blocks = 0;
   size.total_blocks = 0;
   size.used_blocks = 0;
   size.wal_size = 0;
   size.block_size = 0;
   return size;
}

std::string PsitriCatalog::GetDBPath() {
   return "";
}

// Catalog metadata helpers
uint32_t PsitriCatalog::AllocateRootIndex() {
   auto tx = catalog_session_->start_transaction(CATALOG_ROOT);
   uint32_t next = FIRST_TABLE_ROOT;
   if (auto val = tx.get<std::string>("__next_root")) {
      std::memcpy(&next, val->data(), sizeof(uint32_t));
   }
   uint32_t allocated = next;
   next++;
   std::string_view next_view(reinterpret_cast<const char*>(&next), sizeof(uint32_t));
   tx.upsert("__next_root", next_view);
   tx.commit();
   return allocated;
}

void PsitriCatalog::StoreTableMeta(const TableMeta& meta) {
   std::string key = "table/" + meta.schema_name + "/" + meta.table_name;
   std::string val = serialize_table_meta(meta);
   auto tx = catalog_session_->start_transaction(CATALOG_ROOT);
   tx.upsert(key, val);
   tx.commit();
}

std::optional<TableMeta> PsitriCatalog::LoadTableMeta(const std::string& schema,
                                                      const std::string& table) {
   std::string key = "table/" + schema + "/" + table;
   auto tx = catalog_session_->start_transaction(CATALOG_ROOT);
   auto val = tx.get<std::string>(key);
   tx.abort();
   if (!val) return std::nullopt;
   return deserialize_table_meta(*val);
}

void PsitriCatalog::RemoveTableMeta(const std::string& schema, const std::string& table) {
   std::string key = "table/" + schema + "/" + table;
   auto tx = catalog_session_->start_transaction(CATALOG_ROOT);
   tx.remove(key);
   tx.commit();
}

std::vector<TableMeta> PsitriCatalog::ListTables(const std::string& schema) {
   std::string prefix = "table/" + schema + "/";
   std::vector<TableMeta> result;

   auto rs     = storage_->start_read_session();
   auto cursor = rs->create_cursor(CATALOG_ROOT);
   cursor.lower_bound(prefix);
   while (!cursor.is_end()) {
      auto k = cursor.key();
      if (k.substr(0, prefix.size()) != prefix) break;
      cursor.get_value([&](psitri::value_view vv) {
         if (vv.size() > 0) {
            result.push_back(deserialize_table_meta(vv));
         }
      });
      cursor.next();
   }
   return result;
}

// ===========================================================================
// PsitriSchemaEntry
// ===========================================================================

PsitriSchemaEntry::PsitriSchemaEntry(duckdb::Catalog& catalog,
                                     duckdb::CreateSchemaInfo& info)
    : duckdb::SchemaCatalogEntry(catalog, info) {
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateTable(duckdb::CatalogTransaction transaction,
                               duckdb::BoundCreateTableInfo& info) {
   auto& catalog = GetPsitriCatalog();
   auto& base    = info.Base();

   // Build TableMeta from the DuckDB create table info
   TableMeta meta;
   meta.table_name  = base.table;
   meta.schema_name = name;
   meta.root_index  = catalog.AllocateRootIndex();

   // Find primary key columns and NOT NULL columns
   std::unordered_set<std::string> pk_columns;
   std::unordered_set<duckdb::idx_t> not_null_indices;
   for (auto& constraint : base.constraints) {
      if (constraint->type == duckdb::ConstraintType::UNIQUE) {
         auto& uc = constraint->Cast<duckdb::UniqueConstraint>();
         if (uc.IsPrimaryKey()) {
            if (uc.HasIndex()) {
               pk_columns.insert(base.columns.GetColumn(uc.GetIndex()).Name());
            } else {
               for (auto& col_name : uc.GetColumnNames()) {
                  pk_columns.insert(col_name);
               }
            }
         }
      } else if (constraint->type == duckdb::ConstraintType::NOT_NULL) {
         auto& nn = constraint->Cast<duckdb::NotNullConstraint>();
         not_null_indices.insert(nn.index.index);
      }
   }

   duckdb::idx_t col_idx = 0;
   for (auto& col : base.columns.Logical()) {
      ColumnDef def;
      def.name           = col.Name();
      def.type           = duckdb_type_to_sql(col.Type());
      def.is_primary_key = pk_columns.count(col.Name()) > 0;
      def.not_null       = def.is_primary_key || not_null_indices.count(col_idx) > 0;
      meta.columns.push_back(std::move(def));
      col_idx++;
   }

   // If no explicit PK, make first column the PK
   if (pk_columns.empty() && !meta.columns.empty()) {
      meta.columns[0].is_primary_key = true;
   }

   // Persist metadata
   catalog.StoreTableMeta(meta);

   // Create the DuckDB table entry
   auto table_info_copy = base.Copy();
   auto& table_info = table_info_copy->Cast<duckdb::CreateTableInfo>();
   auto entry      = duckdb::make_uniq<PsitriTableEntry>(
      catalog, *this, table_info, std::move(meta));

   std::lock_guard lock(tables_lock_);
   auto* ptr           = entry.get();
   tables_[base.table] = std::move(entry);
   return ptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateView(duckdb::CatalogTransaction transaction,
                              duckdb::CreateViewInfo& info) {
   std::lock_guard lock(tables_lock_);
   auto existing = tables_.find(info.view_name);
   if (existing != tables_.end()) {
      if (info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
         tables_.erase(existing);
      } else if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
         return existing->second.get();
      } else {
         throw duckdb::CatalogException("View \"%s\" already exists", info.view_name);
      }
   }
   auto copy = info.Copy();
   auto& view_info = copy->Cast<duckdb::CreateViewInfo>();
   auto entry = duckdb::make_uniq<duckdb::ViewCatalogEntry>(catalog, *this, view_info);
   auto* ptr = entry.get();
   tables_[info.view_name] = std::move(entry);
   return ptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateFunction(duckdb::CatalogTransaction, duckdb::CreateFunctionInfo&) {
   throw duckdb::NotImplementedException("CREATE FUNCTION not supported in psitri");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateIndex(duckdb::CatalogTransaction transaction,
                               duckdb::CreateIndexInfo& info,
                               duckdb::TableCatalogEntry& table) {
   // Index creation is handled by PsitriCatalog::BindCreateIndex
   throw duckdb::NotImplementedException("CreateIndex should not be called directly");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateSequence(duckdb::CatalogTransaction transaction,
                                  duckdb::CreateSequenceInfo& info) {
   std::lock_guard lock(tables_lock_);
   auto existing = tables_.find(info.name);
   if (existing != tables_.end()) {
      if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
         return existing->second.get();
      }
      throw duckdb::CatalogException("Sequence \"%s\" already exists", info.name);
   }
   auto entry = duckdb::make_uniq<duckdb::SequenceCatalogEntry>(catalog, *this, info);
   auto* ptr = entry.get();
   tables_[info.name] = std::move(entry);
   return ptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateType(duckdb::CatalogTransaction, duckdb::CreateTypeInfo&) {
   throw duckdb::NotImplementedException("CREATE TYPE not supported in psitri");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateCollation(duckdb::CatalogTransaction, duckdb::CreateCollationInfo&) {
   throw duckdb::NotImplementedException("CREATE COLLATION not supported in psitri");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction, duckdb::CreateCopyFunctionInfo&) {
   throw duckdb::NotImplementedException("COPY not supported in psitri");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreateTableFunction(duckdb::CatalogTransaction, duckdb::CreateTableFunctionInfo&) {
   throw duckdb::NotImplementedException("CREATE TABLE FUNCTION not supported in psitri");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::CreatePragmaFunction(duckdb::CatalogTransaction, duckdb::CreatePragmaFunctionInfo&) {
   throw duckdb::NotImplementedException("CREATE PRAGMA FUNCTION not supported in psitri");
}

void PsitriSchemaEntry::Scan(duckdb::ClientContext& context, duckdb::CatalogType type,
                             const std::function<void(duckdb::CatalogEntry&)>& callback) {
   if (type != duckdb::CatalogType::TABLE_ENTRY &&
       type != duckdb::CatalogType::VIEW_ENTRY &&
       type != duckdb::CatalogType::SEQUENCE_ENTRY) return;

   std::lock_guard lock(tables_lock_);
   if (type == duckdb::CatalogType::TABLE_ENTRY && tables_.empty()) {
      auto& catalog = GetPsitriCatalog();
      auto metas    = catalog.ListTables(name);
      for (auto& meta : metas) {
         auto info  = duckdb::make_uniq<duckdb::CreateTableInfo>();
         info->table = meta.table_name;
         info->schema = name;
         for (auto& col : meta.columns) {
            info->columns.AddColumn(duckdb::ColumnDefinition(
               col.name, sql_type_to_duckdb(col.type)));
         }
         auto entry = duckdb::make_uniq<PsitriTableEntry>(
            catalog, *this, *info, std::move(meta));
         tables_[info->table] = std::move(entry);
      }
   }
   for (auto& [tname, entry] : tables_) {
      if (entry->type == type) {
         callback(*entry);
      }
   }
}

void PsitriSchemaEntry::Scan(duckdb::CatalogType type,
                             const std::function<void(duckdb::CatalogEntry&)>& callback) {
   if (type != duckdb::CatalogType::TABLE_ENTRY &&
       type != duckdb::CatalogType::VIEW_ENTRY &&
       type != duckdb::CatalogType::SEQUENCE_ENTRY) return;
   std::lock_guard lock(tables_lock_);
   for (auto& [tname, entry] : tables_) {
      if (entry->type == type) {
         callback(*entry);
      }
   }
}

void PsitriSchemaEntry::DropEntry(duckdb::ClientContext& context, duckdb::DropInfo& info) {
   if (info.type == duckdb::CatalogType::TABLE_ENTRY) {
      auto& catalog = GetPsitriCatalog();
      catalog.RemoveTableMeta(name, info.name);
   } else if (info.type != duckdb::CatalogType::VIEW_ENTRY &&
              info.type != duckdb::CatalogType::SEQUENCE_ENTRY) {
      throw duckdb::NotImplementedException("Only DROP TABLE/VIEW/SEQUENCE is supported");
   }
   std::lock_guard lock(tables_lock_);
   tables_.erase(info.name);
}

void PsitriSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                              duckdb::AlterInfo& info) {
   if (info.GetCatalogType() != duckdb::CatalogType::TABLE_ENTRY) {
      throw duckdb::NotImplementedException("ALTER only supported for tables in psitri");
   }
   auto& alter_table = info.Cast<duckdb::AlterTableInfo>();
   auto& catalog = GetPsitriCatalog();

   switch (alter_table.alter_table_type) {
      case duckdb::AlterTableType::RENAME_TABLE: {
         auto& rename = alter_table.Cast<duckdb::RenameTableInfo>();
         auto old_name = rename.name;
         auto new_name = rename.new_table_name;

         // Load existing meta, update name, re-store
         auto meta = catalog.LoadTableMeta(name, old_name);
         if (!meta) {
            throw duckdb::CatalogException("Table \"%s\" not found", old_name);
         }
         catalog.RemoveTableMeta(name, old_name);
         meta->table_name = new_name;
         catalog.StoreTableMeta(*meta);

         // Update in-memory cache
         std::lock_guard lock(tables_lock_);
         auto it = tables_.find(old_name);
         if (it != tables_.end()) {
            auto entry = std::move(it->second);
            tables_.erase(it);
            tables_[new_name] = std::move(entry);
         }
         break;
      }
      case duckdb::AlterTableType::ADD_COLUMN: {
         auto& add_col = alter_table.Cast<duckdb::AddColumnInfo>();
         auto table_name = add_col.name;

         auto meta = catalog.LoadTableMeta(name, table_name);
         if (!meta) {
            throw duckdb::CatalogException("Table \"%s\" not found", table_name);
         }
         ColumnDef def;
         def.name = add_col.new_column.Name();
         def.type = duckdb_type_to_sql(add_col.new_column.Type());
         def.is_primary_key = false;
         meta->columns.push_back(std::move(def));
         catalog.StoreTableMeta(*meta);

         // Invalidate cached table entry so it gets reloaded
         std::lock_guard lock(tables_lock_);
         tables_.erase(table_name);
         break;
      }
      case duckdb::AlterTableType::REMOVE_COLUMN: {
         auto& remove_col = alter_table.Cast<duckdb::RemoveColumnInfo>();
         auto table_name = remove_col.name;

         auto meta = catalog.LoadTableMeta(name, table_name);
         if (!meta) {
            throw duckdb::CatalogException("Table \"%s\" not found", table_name);
         }
         auto& cols = meta->columns;
         auto it = std::find_if(cols.begin(), cols.end(),
            [&](const ColumnDef& c) { return c.name == remove_col.removed_column; });
         if (it == cols.end()) {
            if (remove_col.if_column_exists) return;
            throw duckdb::CatalogException("Column \"%s\" not found", remove_col.removed_column);
         }
         if (it->is_primary_key) {
            throw duckdb::CatalogException("Cannot drop primary key column \"%s\"",
                                           remove_col.removed_column);
         }
         cols.erase(it);
         catalog.StoreTableMeta(*meta);

         // Invalidate cached table entry
         std::lock_guard lock(tables_lock_);
         tables_.erase(table_name);
         break;
      }
      case duckdb::AlterTableType::RENAME_COLUMN: {
         auto& rename_col = alter_table.Cast<duckdb::RenameColumnInfo>();
         auto table_name = rename_col.name;

         auto meta = catalog.LoadTableMeta(name, table_name);
         if (!meta) {
            throw duckdb::CatalogException("Table \"%s\" not found", table_name);
         }
         for (auto& col : meta->columns) {
            if (col.name == rename_col.old_name) {
               col.name = rename_col.new_name;
               break;
            }
         }
         catalog.StoreTableMeta(*meta);

         std::lock_guard lock(tables_lock_);
         tables_.erase(table_name);
         break;
      }
      default:
         throw duckdb::NotImplementedException(
            "ALTER TABLE type %d not yet supported in psitri",
            static_cast<int>(alter_table.alter_table_type));
   }
}

duckdb::optional_ptr<duckdb::CatalogEntry>
PsitriSchemaEntry::GetEntry(duckdb::CatalogTransaction transaction,
                            duckdb::CatalogType type,
                            const duckdb::string& entry_name) {
   if (type != duckdb::CatalogType::TABLE_ENTRY &&
       type != duckdb::CatalogType::VIEW_ENTRY &&
       type != duckdb::CatalogType::SEQUENCE_ENTRY) {
      return nullptr;
   }

   std::lock_guard lock(tables_lock_);
   auto it = tables_.find(entry_name);
   if (it != tables_.end()) {
      return it->second.get();
   }

   // Views and sequences are in-memory only, no storage fallback
   if (type != duckdb::CatalogType::TABLE_ENTRY) {
      return nullptr;
   }

   // Try loading table from storage
   auto& catalog = GetPsitriCatalog();
   auto meta = catalog.LoadTableMeta(name, entry_name);
   if (!meta) return nullptr;

   auto info   = duckdb::make_uniq<duckdb::CreateTableInfo>();
   info->table  = meta->table_name;
   info->schema = name;
   for (auto& col : meta->columns) {
      info->columns.AddColumn(duckdb::ColumnDefinition(
         col.name, sql_type_to_duckdb(col.type)));
   }
   auto entry       = duckdb::make_uniq<PsitriTableEntry>(
      catalog, *this, *info, std::move(*meta));
   auto* ptr        = entry.get();
   tables_[entry_name] = std::move(entry);
   return ptr;
}

// ===========================================================================
// PsitriTableEntry
// ===========================================================================

PsitriTableEntry::PsitriTableEntry(duckdb::Catalog& catalog,
                                   duckdb::SchemaCatalogEntry& schema,
                                   duckdb::CreateTableInfo& info,
                                   TableMeta meta)
    : duckdb::TableCatalogEntry(catalog, schema, info),
      meta_(std::move(meta)) {
}

duckdb::unique_ptr<duckdb::BaseStatistics>
PsitriTableEntry::GetStatistics(duckdb::ClientContext& context, duckdb::column_t column_id) {
   return nullptr;
}

duckdb::TableFunction PsitriTableEntry::GetScanFunction(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::FunctionData>& bind_data) {

   auto data       = duckdb::make_uniq<PsitriScanBindData>();
   data->catalog   = &catalog.Cast<PsitriCatalog>();
   data->meta      = meta_;
   bind_data       = std::move(data);

   duckdb::TableFunction scan("psitri_scan", {}, PsitriScanFunction,
                              nullptr, PsitriScanInitGlobal, PsitriScanInitLocal);
   scan.projection_pushdown       = true;
   scan.filter_pushdown           = false;  // We don't use table_filters
   scan.pushdown_complex_filter   = PsitriPushdownComplexFilter;
   return scan;
}

duckdb::TableStorageInfo
PsitriTableEntry::GetStorageInfo(duckdb::ClientContext& context) {
   duckdb::TableStorageInfo info;
   // Estimate row count by scanning (capped for performance)
   auto& cat = catalog.Cast<PsitriCatalog>();
   auto rs = cat.GetStorage()->start_read_session();
   auto cursor = rs->create_cursor(meta_.root_index);
   cursor.seek_begin();
   duckdb::idx_t count = 0;
   constexpr duckdb::idx_t MAX_SCAN = 10000;
   while (!cursor.is_end() && count < MAX_SCAN) {
      count++;
      cursor.next();
   }
   info.cardinality = count;
   if (!cursor.is_end()) {
      // We hit the cap — estimate higher
      info.cardinality = count * 10;
   }
   return info;
}

} // namespace psitri_sql
