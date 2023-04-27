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
    try {
      bool success = exec_ctx_->GetLockManager()->LockTable(
          exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::INTENTION_EXCLUSIVE, table_oid_);
      if (!success) {
        // const std::string info = "seqscan(deleted) table IX lock fail";
        // init_throw_error = true;
        // throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      // const std::string info = "seqscan(deleted) table IX lock fail";
      // init_throw_error = true;
      // throw ExecutionException(info);
    }
  } else {
    auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
    if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
      try {
        bool success = exec_ctx_->GetLockManager()->LockTable(
            exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::INTENTION_SHARED, table_oid_);
        if (!success) {
          // const std::string info = "seqscan table IS lock fail";
          // init_throw_error = true;
          // throw ExecutionException(info);
        }
      } catch (TransactionAbortException &e) {
        // const std::string info = "seqscan table IS lock fail";
        // init_throw_error = true;
        // throw ExecutionException(info);
      }
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
      delete iter_;
      iter_ = nullptr;
      return false;
    }

    *tuple = iter_->GetTuple().second;

    if (exec_ctx_->IsDelete()) {
      try {
        auto success = exec_ctx_->GetLockManager()->LockRow(
            exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::EXCLUSIVE, table_oid_, tuple->GetRid());
        if (!success) {
          // const std::string info = "seqscan(delete) table X lock fail";
          // throw ExecutionException(info);
        }
      } catch (TransactionAbortException &e) {
        // const std::string info = "seqscan(delete) table X lock fail";
        // throw ExecutionException(info);
      }
    } else {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
        try {
          bool success = exec_ctx_->GetLockManager()->LockRow(
              exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::SHARED, table_oid_, tuple->GetRid());
          if (!success) {
            // const std::string info = "seqscan table S lock fail";
            // throw ExecutionException(info);
          }
        } catch (TransactionAbortException &e) {
          // const std::string info = "seqscan table S lock fail";
          // throw ExecutionException(info);
        }
      }
    }

    if (iter_->GetTuple().first.is_deleted_) {
      try {
        bool success =
            exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_oid_, tuple->GetRid(), true);
        if (!success) {
          // const std::string info = "seqscan(is_deleted) row force unlock fail";
          // throw ExecutionException(info);
        }
      } catch (TransactionAbortException &e) {
        // const std::string info = "seqscan(is_deleted) row force unlock fail";
        // throw ExecutionException(info);
      }
      ++(*iter_);
      continue;
    }
    *rid = iter_->GetRID();
    break;
  }

  ++(*iter_);

  return true;
}

}  // namespace bustub
