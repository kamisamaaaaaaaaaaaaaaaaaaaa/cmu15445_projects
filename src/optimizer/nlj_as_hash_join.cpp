#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void Optimizer::UpdateLeftAndRightExpreFromChildForHashJoin(const AbstractExpressionRef &left_express,
                                                            const AbstractExpressionRef &right_express,
                                                            std::vector<AbstractExpressionRef> &left_key_expressions,
                                                            std::vector<AbstractExpressionRef> &right_key_expressions) {
  std::vector<AbstractExpressionRef> left_left_key_expressions;
  std::vector<AbstractExpressionRef> left_right_key_expressions;
  std::vector<AbstractExpressionRef> right_left_key_expressions;
  std::vector<AbstractExpressionRef> right_right_key_expressions;

  GetLeftAndRightExpreForHashJoin(left_express, left_left_key_expressions, left_right_key_expressions);
  GetLeftAndRightExpreForHashJoin(right_express, right_left_key_expressions, right_right_key_expressions);

  for (auto &x : left_left_key_expressions) {
    left_key_expressions.emplace_back(std::move(x));
  }
  for (auto &x : right_left_key_expressions) {
    left_key_expressions.emplace_back(std::move(x));
  }
  for (auto &x : left_right_key_expressions) {
    right_key_expressions.emplace_back(std::move(x));
  }
  for (auto &x : right_right_key_expressions) {
    right_key_expressions.emplace_back(std::move(x));
  }
}

void Optimizer::GetLeftAndRightExpreForHashJoin(const AbstractExpressionRef &expression,
                                                std::vector<AbstractExpressionRef> &left_key_expressions,
                                                std::vector<AbstractExpressionRef> &right_key_expressions) {
  if (const auto *expr = dynamic_cast<const ColumnValueExpression *>(expression.get()); expr != nullptr) {
    auto expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, expr->GetColIdx(), expr->GetReturnType());
    if (expr->GetTupleIdx() == 0) {
      left_key_expressions.emplace_back(std::move(expr_tuple_0));
    } else {
      right_key_expressions.emplace_back(std::move(expr_tuple_0));
    }

  } else if (const auto *expr = dynamic_cast<const LogicExpression *>(expression.get()); expr != nullptr) {
    if (expr->logic_type_ == LogicType::And) {
      auto left_express = expr->GetChildAt(0);
      auto right_express = expr->GetChildAt(1);
      UpdateLeftAndRightExpreFromChildForHashJoin(left_express, right_express, left_key_expressions,
                                                  right_key_expressions);
    }
  } else if (const auto *expr = dynamic_cast<const ComparisonExpression *>(expression.get()); expr != nullptr) {
    if (expr->comp_type_ == ComparisonType::Equal) {
      auto left_express = expr->GetChildAt(0);
      auto right_express = expr->GetChildAt(1);
      UpdateLeftAndRightExpreFromChildForHashJoin(left_express, right_express, left_key_expressions,
                                                  right_key_expressions);
    }
  }
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;

    GetLeftAndRightExpreForHashJoin(nlj_plan.Predicate(), left_key_expressions, right_key_expressions);

    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                              left_key_expressions, right_key_expressions, nlj_plan.GetJoinType());
  }
  return optimized_plan;
}

}  // namespace bustub
