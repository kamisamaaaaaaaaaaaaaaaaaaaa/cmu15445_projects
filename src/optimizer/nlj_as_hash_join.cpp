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

auto Optimizer::CheckOtherType(const AbstractExpressionRef &expr) -> bool {
  return !CheckConstant(expr) && !CheckColumnValue(expr);
}

auto Optimizer::CheckConstant(const AbstractExpressionRef &expr) -> bool {
  auto express = dynamic_cast<const ConstantValueExpression *>(expr.get());
  return express != nullptr;
}

auto Optimizer::CheckColumnValue(const AbstractExpressionRef &expr) -> bool {
  auto express = dynamic_cast<const ColumnValueExpression *>(expr.get());
  return express != nullptr;
}

// ... (=,>,<) const
auto Optimizer::CheckFilterExpr(const AbstractExpressionRef &expr) -> bool {
  const auto cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());

  if (cmp_expr == nullptr) {
    return false;
  }

  const auto *l_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(0).get());
  const auto *r_expr = dynamic_cast<const ConstantValueExpression *>(expr->GetChildAt(1).get());
  return l_expr != nullptr && r_expr != nullptr;
}

// ColumValue
auto Optimizer::BelongToLeftFromPushDown(const AbstractExpressionRef &expr, uint32_t l_cols) -> bool {
  const auto *express = dynamic_cast<const ColumnValueExpression *>(expr.get());

  auto col_idx = express->GetColIdx();

  return col_idx < l_cols;
}

// ... = ... return ... = ...
auto Optimizer::GetExprForRightPushDown(const AbstractExpressionRef &expr, uint32_t l_cols) -> AbstractExpressionRef {
  auto l_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(0).get());
  auto new_l_expr = std::make_shared<ColumnValueExpression>(0, l_expr->GetColIdx() - l_cols, l_expr->GetReturnType());

  if (CheckColumnValue(expr->GetChildAt(1))) {
    auto r_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(1).get());
    auto new_r_expr = std::make_shared<ColumnValueExpression>(0, r_expr->GetColIdx() - l_cols, r_expr->GetReturnType());
    auto new_expr = std::make_shared<ComparisonExpression>(new_l_expr, new_r_expr, ComparisonType::Equal);
    return new_expr;
  }
  auto new_expr = std::make_shared<ComparisonExpression>(new_l_expr, expr->GetChildAt(1), ComparisonType::Equal);
  return new_expr;
}

void Optimizer::GetUsefulExpr(std::vector<AbstractExpressionRef> &useful_expr,
                              std::vector<std::vector<AbstractExpressionRef>> &new_pd_expr,
                              std::vector<AbstractExpressionRef> &pd_expr, uint32_t l_cols) {
  for (auto &x : pd_expr) {
    if (CheckFilterExpr(x)) {
      if (BelongToLeftFromPushDown(x->GetChildAt(0), l_cols)) {
        new_pd_expr[0].push_back(x);
      } else {
        new_pd_expr[1].push_back(GetExprForRightPushDown(x, l_cols));
      }
    } else {
      if (BelongToLeftFromPushDown(x->GetChildAt(0), l_cols) && BelongToLeftFromPushDown(x->GetChildAt(1), l_cols)) {
        new_pd_expr[0].push_back(x);
      } else if (!BelongToLeftFromPushDown(x->GetChildAt(0), l_cols) &&
                 !BelongToLeftFromPushDown(x->GetChildAt(1), l_cols)) {
        new_pd_expr[1].push_back(GetExprForRightPushDown(x, l_cols));
      } else {
        useful_expr.push_back(x);
      }
    }
  }
}
auto Optimizer::BelongToLeftFromOwnExpr(const AbstractExpressionRef &expr) -> bool {
  const auto *express = dynamic_cast<const ColumnValueExpression *>(expr.get());
  return express->GetTupleIdx() == 0;
}

auto Optimizer::ParseExpr(const AbstractExpressionRef &expr,
                          std::vector<std::vector<AbstractExpressionRef>> &new_pd_expr,
                          std::vector<std::vector<AbstractExpressionRef>> &key_expr, uint32_t l_cols) -> bool {
  if (CheckConstant(expr)) {
    return true;
  }
  if (const auto *express = dynamic_cast<const LogicExpression *>(expr.get()); express != nullptr) {
    if (express->logic_type_ == LogicType::And) {
      bool l_success = ParseExpr(express->GetChildAt(0), new_pd_expr, key_expr, l_cols);

      bool r_success = ParseExpr(express->GetChildAt(1), new_pd_expr, key_expr, l_cols);

      return l_success && r_success;
    }
    return false;
  }
  if (const auto *express = dynamic_cast<const ComparisonExpression *>(expr.get()); express != nullptr) {
    if (CheckOtherType(express->GetChildAt(0)) || CheckOtherType(express->GetChildAt(1))) {
      return false;
    }
    if (express->comp_type_ == ComparisonType::Equal) {
      if (CheckFilterExpr(expr)) {
        if (BelongToLeftFromOwnExpr(express->GetChildAt(0))) {
          new_pd_expr[0].push_back(expr);
        } else {
          new_pd_expr[1].push_back(expr);
        }
      } else {
        GetKeyExprFromOwnExpr(expr, key_expr, new_pd_expr);
      }
    } else {
      if (!CheckFilterExpr(expr)) {
        return false;
      }
      if (BelongToLeftFromOwnExpr(express->GetChildAt(0))) {
        new_pd_expr[0].push_back(expr);
      } else {
        new_pd_expr[1].push_back(expr);
      }
    }
    return true;
  }

  return false;
}

void Optimizer::GetKeyExprFromOwnExpr(const AbstractExpressionRef &expr,
                                      std::vector<std::vector<AbstractExpressionRef>> &key_expr,
                                      std::vector<std::vector<AbstractExpressionRef>> &new_pd_expr) {
  auto l_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(0).get());
  auto r_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(1).get());
  auto l_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, l_expr->GetColIdx(), l_expr->GetReturnType());
  auto r_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, r_expr->GetColIdx(), r_expr->GetReturnType());
  if (BelongToLeftFromOwnExpr(expr->GetChildAt(0)) && !BelongToLeftFromOwnExpr(expr->GetChildAt(1))) {
    key_expr[0].emplace_back(std::move(l_expr_tuple_0));
    key_expr[1].emplace_back(std::move(r_expr_tuple_0));
  } else if (BelongToLeftFromOwnExpr(expr->GetChildAt(1)) && !BelongToLeftFromOwnExpr(expr->GetChildAt(0))) {
    key_expr[0].emplace_back(std::move(r_expr_tuple_0));
    key_expr[1].emplace_back(std::move(l_expr_tuple_0));
  } else if (BelongToLeftFromOwnExpr(expr->GetChildAt(0)) && BelongToLeftFromOwnExpr(expr->GetChildAt(1))) {
    new_pd_expr[0].push_back(expr);
  } else if (!BelongToLeftFromOwnExpr(expr->GetChildAt(0)) && !BelongToLeftFromOwnExpr(expr->GetChildAt(1))) {
    new_pd_expr[1].push_back(expr);
  }
}

//...=...
void Optimizer::GetKeyExprFromPushDown(const AbstractExpressionRef &expr,
                                       std::vector<std::vector<AbstractExpressionRef>> &key_expr, uint32_t l_cols) {
  auto l_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(0).get());
  auto r_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(1).get());

  if (BelongToLeftFromPushDown(expr->GetChildAt(0), l_cols)) {
    auto l_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, l_expr->GetColIdx(), l_expr->GetReturnType());
    auto r_expr_tuple_0 =
        std::make_shared<ColumnValueExpression>(0, r_expr->GetColIdx() - l_cols, r_expr->GetReturnType());
    key_expr[0].emplace_back(std::move(l_expr_tuple_0));
    key_expr[1].emplace_back(std::move(r_expr_tuple_0));
  } else {
    auto l_expr_tuple_0 =
        std::make_shared<ColumnValueExpression>(0, l_expr->GetColIdx() - l_cols, l_expr->GetReturnType());
    auto r_expr_tuple_0 = std::make_shared<ColumnValueExpression>(0, r_expr->GetColIdx(), r_expr->GetReturnType());
    key_expr[0].emplace_back(std::move(r_expr_tuple_0));
    key_expr[1].emplace_back(std::move(l_expr_tuple_0));
  }
}

auto Optimizer::GetFilterExpress(std::vector<AbstractExpressionRef> &exprs) -> AbstractExpressionRef {
  if (exprs.size() == 1) {
    return exprs[0];
  }

  auto fa = std::make_shared<LogicExpression>(exprs[0], exprs[1], LogicType::And);
  for (size_t i = 2; i < exprs.size(); i++) {
    auto new_fa = std::make_shared<LogicExpression>(exprs[i], fa, LogicType::And);
    fa = new_fa;
  }

  return fa;
}

auto Optimizer::HashJoinOptimize(const AbstractPlanNodeRef &plan, std::vector<AbstractExpressionRef> &pd_expr,
                                 bool &success) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    uint32_t l_cols = nlj_plan.GetChildAt(0)->OutputSchema().GetColumnCount();

    std::vector<AbstractExpressionRef> useful_expr;
    std::vector<std::vector<AbstractExpressionRef>> new_pd_expr(2, std::vector<AbstractExpressionRef>());
    std::vector<std::vector<AbstractExpressionRef>> key_expr(2, std::vector<AbstractExpressionRef>());

    GetUsefulExpr(useful_expr, new_pd_expr, pd_expr, l_cols);
    for (auto &x : useful_expr) {
      GetKeyExprFromPushDown(x, key_expr, l_cols);
    }

    if (ParseExpr(nlj_plan.Predicate(), new_pd_expr, key_expr, l_cols)) {
      auto left_child = HashJoinOptimize(nlj_plan.GetLeftPlan(), new_pd_expr[0], success);
      auto right_child = HashJoinOptimize(nlj_plan.GetRightPlan(), new_pd_expr[1], success);

      if (!success) {
        return plan;
      }

      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, left_child, right_child, key_expr[0],
                                                key_expr[1], nlj_plan.GetJoinType());
    }
    success = false;
    return plan;
  }
  if (plan->GetType() == PlanType::SeqScan || plan->GetType() == PlanType::MockScan) {
    success = true;
    if (!pd_expr.empty()) {
      auto filter_expr = GetFilterExpress(pd_expr);
      return std::make_shared<FilterPlanNode>(plan->output_schema_, filter_expr, plan);
    }
    return plan;
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(HashJoinOptimize(child, pd_expr, success));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractExpressionRef> pd_expr;
  bool success = true;
  auto root = HashJoinOptimize(plan, pd_expr, success);
  if (!success) {
    return plan;
  }
  return root;
}
}  // namespace bustub
