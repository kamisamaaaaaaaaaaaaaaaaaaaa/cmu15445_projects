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
  auto b_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());

  auto keyvalues = plan_->GetKeyValues();
  if (keyvalues.size() == 0) {
    iter = b_tree_index->GetBeginIterator();
  } else {
    Tuple key = Tuple(keyvalues, index_info->index_->GetKeySchema());
    IntegerKeyType index_key;
    index_key.SetFromKey(key);
    iter = b_tree_index->GetBeginIterator(index_key);
  }

  end = b_tree_index->GetEndIterator();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter == end) {
    return false;
  }

  while (iter != end) {
    auto tuplepair = table_info_->table_->GetTuple((*iter).second);
    *tuple = tuplepair.second;
    *rid = tuple->GetRid();
    ++iter;

    if (!plan_->GetKeyValues().empty()) {
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
