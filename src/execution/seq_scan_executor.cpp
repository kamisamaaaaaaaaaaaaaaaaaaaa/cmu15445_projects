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
  if (iter_ != nullptr) {
    delete iter_;
    iter_ = nullptr;
  }
}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid);
  auto &table = table_info->table_;
  iter_ = new TableIterator(table->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (iter_->IsEnd()) {
      delete iter_;
      iter_ = nullptr;
      return false;
    }

    *tuple = iter_->GetTuple().second;

    if (iter_->GetTuple().first.is_deleted_) {
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
