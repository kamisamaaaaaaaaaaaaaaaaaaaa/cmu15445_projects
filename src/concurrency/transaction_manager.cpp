//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  while (!txn->GetWriteSet()->empty()) {
    auto twr = txn->GetWriteSet()->back();

    if (twr.wtype_ == WType::INSERT) {
      auto tuple_meta = twr.table_heap_->GetTupleMeta(twr.rid_);
      tuple_meta.is_deleted_ = true;
      twr.table_heap_->UpdateTupleMeta(tuple_meta, twr.rid_);
    } else if (twr.wtype_ == WType::DELETE) {
      auto tuple_meta = twr.table_heap_->GetTupleMeta(twr.rid_);
      tuple_meta.is_deleted_ = false;
      twr.table_heap_->UpdateTupleMeta(tuple_meta, twr.rid_);
    }

    txn->GetWriteSet()->pop_back();
  }

  ReleaseLocks(txn);

  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
