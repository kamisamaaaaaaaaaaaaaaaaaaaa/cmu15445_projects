#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void TopNExecutor::Init() {
  child_executor_->Init();

  auto order_bys = plan_->GetOrderBy();

  auto cmp = [&](const Tuple &a, const Tuple &b) {
    for (auto &x : order_bys) {
      auto express = x.second;
      auto a_val = express->Evaluate(&a, plan_->GetChildPlan()->OutputSchema());
      auto b_val = express->Evaluate(&b, plan_->GetChildPlan()->OutputSchema());

      auto ordertype = x.first;
      if (a_val.CompareLessThan(b_val) == CmpBool::CmpTrue) {
        // 前k小，用大根堆，a<b时返回true,a优先级低放堆底
        // 前k大，用小根堆，a<b时返回false，a优先级低放堆顶
        return ordertype == OrderByType::ASC || ordertype == OrderByType::DEFAULT;
      }
      if (a_val.CompareGreaterThan(b_val) == CmpBool::CmpTrue) {
        return ordertype == OrderByType::DESC;
      }
    }
    return true;
  };

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> heap(cmp);
  heap_size_ = 0;

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    heap.push(tuple);
    ++heap_size_;
    if (heap.size() > plan_->GetN()) {
      heap.pop();
      --heap_size_;
    }
  }

  while (!heap.empty()) {
    result_.push_back(heap.top());
    heap.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_.empty()) {
    return false;
  }

  // ps:读结果的时候应该从堆底往堆顶读，如前k小的数用大根堆维护，堆顶应该是最大的，最后输出
  *tuple = result_.back();
  *rid = tuple->GetRid();
  result_.pop_back();

  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return heap_size_; }

}  // namespace bustub
