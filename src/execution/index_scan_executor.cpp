//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "type/value_factory.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  // printf("IndexScan Init\n");
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  auto b_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());

  auto keyvalues = plan_->GetKeyValues();
  if (keyvalues.empty()) {
    iter_ = b_tree_index->GetBeginIterator();
  } else {
    Tuple key = Tuple(keyvalues, index_info->index_->GetKeySchema());
    IntegerKeyType index_key;
    index_key.SetFromKey(key);
    iter_ = b_tree_index->GetBeginIterator(index_key);
  }

  end_ = b_tree_index->GetEndIterator();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);

  if (plan_->single_search_) {
    if (exec_ctx_->IsDelete()) {
      try {
        bool success = exec_ctx_->GetLockManager()->LockTable(
            exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
        if (!success) {
          const std::string info = "indexscan(deleted) table IX lock fail";
          throw ExecutionException(info);
        }
      } catch (TransactionAbortException &e) {
        const std::string info = "indexscan(deleted) table IX lock fail";
        throw ExecutionException(info);
      }
    } else {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
        try {
          bool success = exec_ctx_->GetLockManager()->LockTable(
              exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
          if (!success) {
            const std::string info = "indexscan table IS lock fail";
            throw ExecutionException(info);
          }
        } catch (TransactionAbortException &e) {
          const std::string info = "indexscan table IS lock fail";
          throw ExecutionException(info);
        }
      }
    }
  }

  has_out = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == end_) {
    return false;
  }

  while (iter_ != end_) {
    if (plan_->single_search_) {
      if (exec_ctx_->IsDelete()) {
        try {
          auto success = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(),
                                                              bustub::LockManager::LockMode::EXCLUSIVE,
                                                              table_info_->oid_, (*iter_).second);
          if (!success) {
            const std::string info = "indexscan(delete) table row X lock fail";
            throw ExecutionException(info);
          }
        } catch (TransactionAbortException &e) {
          const std::string info = "indexscan(delete) table row X lock fail";
          throw ExecutionException(info);
        }
      } else {
        auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
        if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
          try {
            bool success = exec_ctx_->GetLockManager()->LockRow(
                exec_ctx_->GetTransaction(), bustub::LockManager::LockMode::SHARED, table_info_->oid_, (*iter_).second);
            // printf("single_search 成功加锁\n");
            if (!success) {
              const std::string info = "indexscan table row S lock fail";
              throw ExecutionException(info);
            }
          } catch (TransactionAbortException &e) {
            const std::string info = "indexscan table row S lock fail";
            throw ExecutionException(info);
          }
        }
      }
    }

    auto tuplepair = table_info_->table_->GetTuple((*iter_).second);
    *tuple = tuplepair.second;
    *rid = tuple->GetRid();
    ++iter_;

    if (!plan_->GetKeyValues().empty()) {
      // p4 leaderboard index单点查询，输出一次即可
      if (plan_->single_search_) {
        // printf("单点查询\n");
        if (has_out) {
          auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
          if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
            try {
              bool success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
              if (!success) {
                const std::string info = "indexscan table unlock fail";
                throw ExecutionException(info);
              }
            } catch (TransactionAbortException &e) {
              const std::string info = "indexscan table unlock fail";
              throw ExecutionException(info);
            }
          }
          // printf("single search 结束\n");
          return false;
        } else {
          if (plan_->Predicate()
                  ->Evaluate(tuple, GetOutputSchema())
                  .CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
            // printf("single search合法\n");
            has_out = true;
            return true;
          }
          // printf("single search不合法\n");
          try {
            bool success = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_,
                                                                  tuple->GetRid(), true);
            if (!success) {
              const std::string info = "indexscan(is_deleted) row force unlock fail";
              throw ExecutionException(info);
            }
          } catch (TransactionAbortException &e) {
            const std::string info = "indexscan(is_deleted) row force unlock fail";
            throw ExecutionException(info);
          }

          return false;
        }
      } else {
        // p3 leaderboard-1
        if (plan_->Predicate()->Evaluate(tuple, GetOutputSchema()).CompareEquals(ValueFactory::GetBooleanValue(true)) ==
            CmpBool::CmpTrue) {
          return true;
        }
      }

    } else {
      return true;
    }
  }

  return false;
}

}  // namespace bustub
