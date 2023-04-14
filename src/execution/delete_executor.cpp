//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  table_info_ = catalog->GetTable(table_oid);
  auto table_name = table_info_->name_;
  index_infos_ = catalog->GetTableIndexes(table_name);
  has_out = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_out) return false;

  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      nums++;
    }

    for (auto &x : index_infos_) {
      x->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values{};
  values.push_back(Value(INTEGER, nums));
  *tuple = Tuple(values, &GetOutputSchema());

  has_out = true;

  return true;
}

}  // namespace bustub
