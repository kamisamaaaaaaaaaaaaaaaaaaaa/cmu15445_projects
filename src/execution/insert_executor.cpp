//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  table_info_ = catalog->GetTable(table_oid);
  auto table_name = table_info_->name_;
  index_infos_ = catalog->GetTableIndexes(table_name);
  has_out = false;
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_out) return false;

  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tuplemeta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
    auto rid_optional = table_info_->table_->InsertTuple(tuplemeta, *tuple, exec_ctx_->GetLockManager(),
                                                         exec_ctx_->GetTransaction(), table_info_->oid_);
    if (rid_optional.has_value()) {
      *rid = rid_optional.value();
      nums++;

      for (auto &x : index_infos_) {
        Tuple partial_tuple =
            tuple->KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
        x->index_->InsertEntry(partial_tuple, *rid, exec_ctx_->GetTransaction());
      }
    }
  }

  std::vector<Value> values{};
  values.push_back(Value(INTEGER, nums));
  *tuple = Tuple(values, &GetOutputSchema());

  has_out = true;

  return true;
}

}  // namespace bustub
