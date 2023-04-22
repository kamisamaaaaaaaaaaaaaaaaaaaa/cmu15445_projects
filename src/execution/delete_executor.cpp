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
  has_out_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_out_) {
    return false;
  }

  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto tuplemeta = table_info_->table_->GetTupleMeta(*rid);
    tuplemeta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuplemeta, *rid);

    nums++;

    for (auto &x : index_infos_) {
      Tuple partial_tuple =
          tuple->KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
      x->index_->DeleteEntry(partial_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values{};
  values.emplace_back(Value(INTEGER, nums));
  *tuple = Tuple(values, &GetOutputSchema());

  has_out_ = true;

  return true;
}

}  // namespace bustub
