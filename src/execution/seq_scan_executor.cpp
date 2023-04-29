//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

SeqScanExecutor::~SeqScanExecutor() {
  if (!init_throw_error && iter_ != nullptr) {
    delete iter_;
    iter_ = nullptr;
  }
}

void SeqScanExecutor::Init() {
  table_oid_ = plan_->GetTableOid();

  if (exec_ctx_->IsDelete()) {
    TryLockTable(bustub::LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid_);
  } else {
    auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
    if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
      TryLockTable(bustub::LockManager::LockMode::INTENTION_SHARED, table_oid_);
    }
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid_);
  auto &table = table_info->table_;
  // iter_ = new TableIterator(table->MakeIterator());
  iter_ = new TableIterator(table->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (iter_->IsEnd()) {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
        TryUnLockTable(table_oid_);
      }

      delete iter_;
      iter_ = nullptr;
      return false;
    }

    *tuple = iter_->GetTuple().second;

    if (exec_ctx_->IsDelete()) {
      TryLockRow(bustub::LockManager::LockMode::EXCLUSIVE, table_oid_, tuple->GetRid());
    } else {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
        TryLockRow(bustub::LockManager::LockMode::SHARED, table_oid_, tuple->GetRid());
      }
    }

    if (iter_->GetTuple().first.is_deleted_ ||
        (plan_->filter_predicate_ != nullptr &&
         plan_->filter_predicate_->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(table_oid_)->schema_)
                 .CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue)) {
      TryUnLockRow(table_oid_, tuple->GetRid(), true);
      ++(*iter_);
      continue;
    } else {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
        TryUnLockRow(table_oid_, tuple->GetRid());
      }
    }

    *tuple = iter_->GetTuple().second;
    *rid = tuple->GetRid();
    break;
  }

  ++(*iter_);

  return true;
}

}  // namespace bustub
