#include <psitri-duckdb/psitri_transaction.hpp>
#include <psitri-duckdb/psitri_catalog.hpp>

#include "duckdb/main/client_context.hpp"

namespace psitri_duckdb {

// ===========================================================================
// PsitriTransaction
// ===========================================================================

PsitriTransaction::PsitriTransaction(duckdb::TransactionManager& manager,
                                     duckdb::ClientContext& context,
                                     PsitriCatalog& catalog)
    : duckdb::Transaction(manager, context), catalog_(catalog) {
}

PsitriTransaction::~PsitriTransaction() {
   for (auto& [root_idx, tx] : root_transactions_) {
      if (!tx.is_committed() && !tx.is_aborted()) {
         tx.abort();
      }
   }
}

void PsitriTransaction::Start() {
   // Nothing to do — DWAL transactions are created lazily per root
}

void PsitriTransaction::Commit() {
   for (auto& [root_idx, tx] : root_transactions_) {
      tx.commit();
   }
   root_transactions_.clear();
}

void PsitriTransaction::Rollback() {
   for (auto& [root_idx, tx] : root_transactions_) {
      if (!tx.is_committed() && !tx.is_aborted()) {
         tx.abort();
      }
   }
   root_transactions_.clear();
}

psitri::dwal::transaction::root_handle&
PsitriTransaction::GetOrCreateRootHandle(uint32_t root_index) {
   auto it = root_transactions_.find(root_index);
   if (it != root_transactions_.end()) {
      return it->second.root(root_index);
   }
   auto [inserted, _] = root_transactions_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(root_index),
      std::forward_as_tuple(GetDwalDb().start_transaction(root_index)));
   return inserted->second.root(root_index);
}

psitri::dwal::dwal_database& PsitriTransaction::GetDwalDb() {
   return *catalog_.GetDwalDb();
}

PsitriTransaction& PsitriTransaction::Get(duckdb::ClientContext& context,
                                          duckdb::Catalog& catalog) {
   return duckdb::Transaction::Get(context, catalog).Cast<PsitriTransaction>();
}

void PsitriTransaction::RegisterRowKey(uint32_t root_index, int64_t row_id, std::string key) {
   row_id_keys_[root_index][row_id] = std::move(key);
}

const std::string* PsitriTransaction::LookupRowKey(uint32_t root_index, int64_t row_id) const {
   auto rit = row_id_keys_.find(root_index);
   if (rit == row_id_keys_.end()) return nullptr;
   auto kit = rit->second.find(row_id);
   if (kit == rit->second.end()) return nullptr;
   return &kit->second;
}

void PsitriTransaction::ClearRowKeys(uint32_t root_index) {
   row_id_keys_.erase(root_index);
}

// ===========================================================================
// PsitriTransactionManager
// ===========================================================================

PsitriTransactionManager::PsitriTransactionManager(duckdb::AttachedDatabase& db,
                                                   PsitriCatalog& catalog)
    : duckdb::TransactionManager(db), catalog_(catalog) {
}

duckdb::Transaction&
PsitriTransactionManager::StartTransaction(duckdb::ClientContext& context) {
   auto txn = duckdb::make_uniq<PsitriTransaction>(*this, context, catalog_);
   txn->Start();
   auto& result = *txn;
   std::lock_guard guard(transaction_lock_);
   transactions_.emplace(result, std::move(txn));
   return result;
}

duckdb::ErrorData
PsitriTransactionManager::CommitTransaction(duckdb::ClientContext& context,
                                            duckdb::Transaction& transaction) {
   auto& psitri_txn = transaction.Cast<PsitriTransaction>();
   try {
      psitri_txn.Commit();
   } catch (std::exception& e) {
      return duckdb::ErrorData(e);
   }
   std::lock_guard guard(transaction_lock_);
   transactions_.erase(transaction);
   return duckdb::ErrorData();
}

void PsitriTransactionManager::RollbackTransaction(duckdb::Transaction& transaction) {
   auto& psitri_txn = transaction.Cast<PsitriTransaction>();
   psitri_txn.Rollback();
   std::lock_guard guard(transaction_lock_);
   transactions_.erase(transaction);
}

void PsitriTransactionManager::Checkpoint(duckdb::ClientContext& context, bool force) {
   catalog_.GetStorage()->sync();
}

} // namespace psitri_duckdb
