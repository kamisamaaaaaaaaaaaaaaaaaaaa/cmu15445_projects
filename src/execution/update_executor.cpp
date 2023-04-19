//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_oid = plan_->TableOid();
  table_info_ = catalog->GetTable(table_oid);
  auto table_name = table_info_->name_;
  index_infos_ = catalog->GetTableIndexes(table_name);
  has_out = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_out) return false;

  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> values{};
    for (auto &express : plan_->target_expressions_) {
      values.push_back(express->Evaluate(tuple, table_info_->schema_));
    }
    Tuple new_tuple = Tuple(values, &table_info_->schema_);

    auto tuplemeta = table_info_->table_->GetTupleMeta(*rid);
    tuplemeta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuplemeta, *rid);

    auto new_tuplemeta = TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    auto rid_optional = table_info_->table_->InsertTuple(new_tuplemeta, new_tuple, exec_ctx_->GetLockManager(),
                                                         exec_ctx_->GetTransaction(), table_info_->oid_);
    if (rid_optional.has_value()) {
      nums++;
      *rid = rid_optional.value();

      for (auto &x : index_infos_) {
        Tuple partial_tuple =
            tuple->KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
        x->index_->DeleteEntry(partial_tuple, *rid, exec_ctx_->GetTransaction());

        Tuple partial_new_tuple =
            new_tuple.KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
        x->index_->InsertEntry(partial_new_tuple, *rid, exec_ctx_->GetTransaction());
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
