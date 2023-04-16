//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  Tuple tuple;
  RID rid;

  left_executor_->Init();
  right_executor_->Init();

  while (left_executor_->Next(&tuple, &rid)) {
    left_tuples_.push_back(tuple);
  }

  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.push_back(tuple);
  }

  left_ptr = 0;
  right_ptr = 0;
  match = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (left_ptr < left_tuples_.size()) {
    if (right_ptr == right_tuples_.size()) {
      ++left_ptr;
      right_ptr = 0;
      match = false;
    }

    if (left_ptr == left_tuples_.size()) {
      return false;
    }

    auto value = plan_->Predicate().EvaluateJoin(&left_tuples_[left_ptr], plan_->GetLeftPlan()->OutputSchema(),
                                                 &right_tuples_[right_ptr], plan_->GetRightPlan()->OutputSchema());
    if (value.CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
      std::vector<Value> values;

      auto left_col_cnts = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
      auto right_col_cnts = plan_->GetRightPlan()->OutputSchema().GetColumnCount();

      for (uint32_t i = 0; i < left_col_cnts; i++) {
        values.push_back(left_tuples_[left_ptr].GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
      }

      for (uint32_t i = 0; i < right_col_cnts; i++) {
        values.push_back(right_tuples_[right_ptr].GetValue(&plan_->GetRightPlan()->OutputSchema(), i));
      }

      *tuple = Tuple(values, &GetOutputSchema());

      match = true;

      ++right_ptr;
      if (right_ptr == right_tuples_.size()) {
        ++left_ptr;
        right_ptr = 0;

        match = false;
      }
      break;
    } else {
      ++right_ptr;
      if (right_ptr == right_tuples_.size()) {
        if (!match && plan_->GetJoinType() == JoinType::LEFT) {
          std::vector<Value> values;
          auto left_col_cnts = plan_->GetLeftPlan()->OutputSchema().GetColumnCount();
          auto right_col_cnts = plan_->GetRightPlan()->OutputSchema().GetColumnCount();
          for (uint32_t i = 0; i < left_col_cnts; i++) {
            values.push_back(left_tuples_[left_ptr].GetValue(&plan_->GetLeftPlan()->OutputSchema(), i));
          }
          for (uint32_t i = 0; i < right_col_cnts; i++) {
            values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          }

          *tuple = Tuple(values, &GetOutputSchema());

          break;
        }

        ++left_ptr;
        right_ptr = 0;

        match = false;
      }
    }
  }

  if (left_ptr == left_tuples_.size()) {
    return false;
  }

  return true;
}

}  // namespace bustub
