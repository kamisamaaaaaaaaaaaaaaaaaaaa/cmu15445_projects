#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void SortExecutor::Init() {
  child_executor_->Init();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.push_back(tuple);
  }

  auto order_bys = plan_->GetOrderBy();

  std::sort(tuples.begin(), tuples.end(), [&](const Tuple &a, const Tuple &b) {
    for (auto &x : order_bys) {
      auto express = x.second;
      auto a_val = express->Evaluate(&a, plan_->GetChildPlan()->OutputSchema());
      auto b_val = express->Evaluate(&b, plan_->GetChildPlan()->OutputSchema());

      auto ordertype = x.first;
      if (a_val.CompareLessThan(b_val) == CmpBool::CmpTrue) {
        if (ordertype == OrderByType::ASC || ordertype == OrderByType::DEFAULT) {
          return true;
        } else if (ordertype == OrderByType::DESC) {
          return false;
        }
      } else if (a_val.CompareGreaterThan(b_val) == CmpBool::CmpTrue) {
        if (ordertype == OrderByType::ASC || ordertype == OrderByType::DEFAULT) {
          return false;
        } else if (ordertype == OrderByType::DESC) {
          return true;
        }
      }
    }

    return true;
  });

  ptr = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ptr == tuples.size()) return false;

  *tuple = tuples[ptr];
  *rid = tuple->GetRid();
  ptr++;

  return true;
}

}  // namespace bustub
