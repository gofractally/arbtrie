#include <psitri-duckdb/psitri_catalog.hpp>
#include <psitri-duckdb/psitri_transaction.hpp>

#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/common/enums/access_mode.hpp"

#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>

namespace psitri_duckdb {

// ---------------------------------------------------------------------------
// ATTACH handler: creates a PsitriCatalog backed by the given path
// ---------------------------------------------------------------------------
static duckdb::unique_ptr<duckdb::Catalog>
PsitriAttach(duckdb::StorageExtensionInfo* storage_info,
             duckdb::ClientContext& context,
             duckdb::AttachedDatabase& db,
             const duckdb::string& name,
             duckdb::AttachInfo& info,
             duckdb::AccessMode access_mode) {
   auto path = info.path;
   if (path.empty()) {
      throw duckdb::BinderException("ATTACH with TYPE psitri requires a path");
   }
   std::shared_ptr<psitri::database> storage;
   bool exists = std::filesystem::exists(path) && std::filesystem::is_directory(path);
   if (exists) {
      storage = std::make_shared<psitri::database>(path, psitri::runtime_config{});
   } else {
      storage = psitri::database::create(path);
   }

   // Create DWAL layer for buffered writes
   auto wal_dir = std::filesystem::path(path) / "wal";
   auto dwal_db = std::make_shared<psitri::dwal::dwal_database>(
      storage, wal_dir, psitri::dwal::dwal_config{});

   return duckdb::make_uniq<PsitriCatalog>(db, std::move(storage), std::move(dwal_db));
}

// ---------------------------------------------------------------------------
// Transaction manager factory
// ---------------------------------------------------------------------------
static duckdb::unique_ptr<duckdb::TransactionManager>
PsitriCreateTransactionManager(duckdb::StorageExtensionInfo* storage_info,
                               duckdb::AttachedDatabase& db,
                               duckdb::Catalog& catalog) {
   auto& psitri_catalog = catalog.Cast<PsitriCatalog>();
   return duckdb::make_uniq<PsitriTransactionManager>(db, psitri_catalog);
}

// ---------------------------------------------------------------------------
// StorageExtension
// ---------------------------------------------------------------------------
class PsitriStorageExtension : public duckdb::StorageExtension {
public:
   PsitriStorageExtension() {
      attach                     = PsitriAttach;
      create_transaction_manager = PsitriCreateTransactionManager;
   }
};

// ---------------------------------------------------------------------------
// Public registration function (call from your main or DuckDB setup)
// ---------------------------------------------------------------------------
void RegisterPsitriStorage(duckdb::DatabaseInstance& db) {
   auto& config = duckdb::DBConfig::GetConfig(db);
   config.storage_extensions["psitri"] = duckdb::make_uniq<PsitriStorageExtension>();
}

void RegisterPsitriStorage(duckdb::DuckDB& db) {
   RegisterPsitriStorage(*db.instance);
}

void RegisterPsitriStorage(duckdb::DBConfig& config) {
   config.storage_extensions["psitri"] = duckdb::make_uniq<PsitriStorageExtension>();
}

} // namespace psitri_duckdb
