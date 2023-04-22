//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), aht_(plan->GetAggregates(), plan->GetAggregateTypes()), aht_iterator_(aht_.Begin()) {
  plan_ = plan;
  child_ = std::move(child);
  cnt_ = 0;
  has_out_ = false;
}

void AggregationExecutor::Init() {
  auto group_by = plan_->GetGroupBys();
  auto aggregates = plan_->GetAggregates();

  child_->Init();
  aht_.Clear();

  Tuple tuple;
  RID rid;

  while (child_->Next(&tuple, &rid)) {
    cnt_++;
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }

  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cnt_ == 0 && !has_out_) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }

    std::vector<Value> values;
    auto aggregate_types = plan_->GetAggregateTypes();
    for (size_t i = 0; i < plan_->GetAggregates().size(); i++) {
      if (aggregate_types[i] == AggregationType::CountStarAggregate) {
        values.push_back(ValueFactory::GetIntegerValue(0));
      } else {
        values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }

    *tuple = Tuple(values, &GetOutputSchema());

    has_out_ = true;
    return true;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> values;
  for (auto const &x : aht_iterator_.Key().group_bys_) {
    values.push_back(x);
  }
  for (auto const &x : aht_iterator_.Val().aggregates_) {
    values.push_back(x);
  }

  *tuple = Tuple(values, &GetOutputSchema());

  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
