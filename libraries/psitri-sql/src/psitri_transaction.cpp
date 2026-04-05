#include <psitri-sql/psitri_transaction.hpp>
#include <psitri-sql/psitri_catalog.hpp>

#include "duckdb/main/client_context.hpp"

namespace psitri_sql {

// ===========================================================================
// PsitriTransaction
// ===========================================================================

PsitriTransaction::PsitriTransaction(duckdb::TransactionManager& manager,
                                     duckdb::ClientContext& context,
                                     PsitriCatalog& catalog)
    : duckdb::Transaction(manager, context), catalog_(catalog) {
}

PsitriTransaction::~PsitriTransaction() {
   // Abort any uncommitted root transactions
   for (auto& [root_idx, tx] : root_transactions_) {
      tx.abort();
   }
}

void PsitriTransaction::Start() {
   write_session_ = catalog_.GetStorage()->start_write_session();
}

void PsitriTransaction::Commit() {
   for (auto& [root_idx, tx] : root_transactions_) {
      tx.commit();
   }
   root_transactions_.clear();
}

void PsitriTransaction::Rollback() {
   for (auto& [root_idx, tx] : root_transactions_) {
      tx.abort();
   }
   root_transactions_.clear();
}

psitri::transaction&
PsitriTransaction::GetOrCreateRootTransaction(uint32_t root_index) {
   auto it = root_transactions_.find(root_index);
   if (it != root_transactions_.end()) {
      return it->second;
   }
   // std::map::emplace with piecewise_construct to move-construct in place
   auto [inserted, _] = root_transactions_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(root_index),
      std::forward_as_tuple(write_session_->start_transaction(root_index)));
   return inserted->second;
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

} // namespace psitri_sql
