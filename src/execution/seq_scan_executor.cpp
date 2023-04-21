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
  if (iter != nullptr) {
    delete iter;
    iter = nullptr;
  }
}

void SeqScanExecutor::Init() {
  auto table_oid = plan_->GetTableOid();
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(table_oid);
  auto &table = table_info->table_;
  iter = new TableIterator(table->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (iter->IsEnd()) {
      delete iter;
      iter = nullptr;
      return false;
    }

    *tuple = iter->GetTuple().second;

    if (iter->GetTuple().first.is_deleted_) {
      ++(*iter);
      continue;
    } else {
      *rid = iter->GetRID();
      break;
    }
  }

  ++(*iter);

  return true;
}

}  // namespace bustub
