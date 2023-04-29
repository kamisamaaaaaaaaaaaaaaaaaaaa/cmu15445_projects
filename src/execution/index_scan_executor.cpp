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
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());

  if (!plan_->single_search_) {
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
  }

  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);

  // leaderboard 4 indexscan 单点优化
  if (plan_->single_search_) {
    if (exec_ctx_->IsDelete()) {
      TryLockTable(bustub::LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);

    } else {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
        TryLockTable(bustub::LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      }
    }
    has_out = false;
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // leaderboard 4 单点优化
  if (plan_->single_search_) {
    if (has_out) {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
        TryUnLockTable(table_info_->oid_);
      }
      return false;
    }

    std::vector<RID> res;
    Tuple key = Tuple(plan_->key_values_, exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_->GetKeySchema());
    exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)->index_->ScanKey(key, &res, exec_ctx_->GetTransaction());

    *tuple = key;
    *rid = res[0];

    if (exec_ctx_->IsDelete()) {
      TryLockRow(bustub::LockManager::LockMode::EXCLUSIVE, table_info_->oid_, *rid);
    } else {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
        TryLockRow(bustub::LockManager::LockMode::SHARED, table_info_->oid_, *rid);
      }
    }

    if (plan_->Predicate()->Evaluate(&key, GetOutputSchema()).CompareEquals(ValueFactory::GetBooleanValue(true)) ==
        CmpBool::CmpTrue) {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
        TryUnLockRow(table_info_->oid_, *rid);
      }
      has_out = true;
      return true;
    }

    // b+树找不到key，测试保证不会出现着这种状况
    TryUnLockRow(table_info_->oid_, tuple->GetRid(), true);

    return false;
  }

  if (iter_ == end_) {
    return false;
  }

  while (iter_ != end_) {
    auto tuplepair = table_info_->table_->GetTuple((*iter_).second);
    *tuple = tuplepair.second;
    *rid = tuple->GetRid();
    ++iter_;

    if (!plan_->GetKeyValues().empty()) {
      // p3 leaderboard-1
      if (plan_->Predicate()->Evaluate(tuple, GetOutputSchema()).CompareEquals(ValueFactory::GetBooleanValue(true)) ==
          CmpBool::CmpTrue) {
        return true;
      }

    } else {
      return true;
    }
  }

  return false;
}

}  // namespace bustub
