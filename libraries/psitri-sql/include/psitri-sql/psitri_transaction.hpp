#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/reference_map.hpp"

#include <psitri/database.hpp>
#include <psitri/write_session.hpp>
#include <psitri/transaction.hpp>

#include <mutex>
#include <map>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>

namespace psitri_sql {

class PsitriCatalog;

// ---------------------------------------------------------------------------
// PsitriTransaction -- wraps a psitri write session for one DuckDB transaction
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

   // Get or create a psitri transaction for a specific root index.
   psitri::transaction& GetOrCreateRootTransaction(uint32_t root_index);

   psitri::write_session& GetWriteSession() { return *write_session_; }
   PsitriCatalog& GetCatalog() { return catalog_; }

   static PsitriTransaction& Get(duckdb::ClientContext& context, duckdb::Catalog& catalog);

   // Row ID → encoded key mapping for DELETE/UPDATE support.
   // During scan, we record the encoded key for each sequential row_id.
   // DELETE/UPDATE operators look up the key by row_id.
   void RegisterRowKey(uint32_t root_index, int64_t row_id, std::string key);
   const std::string* LookupRowKey(uint32_t root_index, int64_t row_id) const;
   void ClearRowKeys(uint32_t root_index);

private:
   PsitriCatalog& catalog_;
   std::shared_ptr<psitri::write_session> write_session_;
   // psitri::transaction is move-constructible but not move-assignable;
   // std::map supports emplace with move construction.
   std::map<uint32_t, psitri::transaction> root_transactions_;
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

} // namespace psitri_sql
