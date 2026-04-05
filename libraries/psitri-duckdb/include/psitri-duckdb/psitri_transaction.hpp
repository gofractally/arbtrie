#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/reference_map.hpp"

#include <psitri/database.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri/dwal/transaction.hpp>

#include <mutex>
#include <map>
#include <unordered_map>
#include <memory>
#include <string>

namespace psitri_duckdb {

class PsitriCatalog;

// ---------------------------------------------------------------------------
// PsitriTransaction -- wraps DWAL transactions for one DuckDB transaction.
// Each root gets its own dwal::transaction (single write root) created lazily.
// ---------------------------------------------------------------------------
class PsitriTransaction : public duckdb::Transaction {
public:
   PsitriTransaction(duckdb::TransactionManager& manager,
                     duckdb::ClientContext& context,
                     PsitriCatalog& catalog);
   ~PsitriTransaction() override;

   void Start();
   void Commit();
   void Rollback();

   // Get a root_handle for a specific root. Creates a DWAL transaction if needed.
   psitri::dwal::transaction::root_handle& GetOrCreateRootHandle(uint32_t root_index);

   PsitriCatalog& GetCatalog() { return catalog_; }
   psitri::dwal::dwal_database& GetDwalDb();

   static PsitriTransaction& Get(duckdb::ClientContext& context, duckdb::Catalog& catalog);

   // Row ID → encoded key mapping for DELETE/UPDATE support.
   void RegisterRowKey(uint32_t root_index, int64_t row_id, std::string key);
   const std::string* LookupRowKey(uint32_t root_index, int64_t row_id) const;
   void ClearRowKeys(uint32_t root_index);

private:
   PsitriCatalog& catalog_;
   // Per-root DWAL transactions, created lazily via start_transaction(root_index)
   std::map<uint32_t, psitri::dwal::transaction> root_transactions_;
   std::unordered_map<uint32_t, std::unordered_map<int64_t, std::string>> row_id_keys_;
};

// ---------------------------------------------------------------------------
// PsitriTransactionManager
// ---------------------------------------------------------------------------
class PsitriTransactionManager : public duckdb::TransactionManager {
public:
   PsitriTransactionManager(duckdb::AttachedDatabase& db, PsitriCatalog& catalog);

   duckdb::Transaction& StartTransaction(duckdb::ClientContext& context) override;
   duckdb::ErrorData CommitTransaction(duckdb::ClientContext& context,
                                       duckdb::Transaction& transaction) override;
   void RollbackTransaction(duckdb::Transaction& transaction) override;
   void Checkpoint(duckdb::ClientContext& context, bool force) override;

private:
   PsitriCatalog& catalog_;
   std::mutex transaction_lock_;
   duckdb::reference_map_t<duckdb::Transaction, duckdb::unique_ptr<PsitriTransaction>> transactions_;
};

} // namespace psitri_duckdb
