#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/arithmetic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
auto Optimizer::CheckArithMetic(AbstractExpressionRef expr) -> bool {
  auto expr_ = dynamic_cast<const ArithmeticExpression *>(expr.get());
  return expr_ != nullptr;
}

void Optimizer::ParseExprForColumnPruning(AbstractExpressionRef expr, std::vector<uint32_t> &output_cols) {
  if (CheckArithMetic(expr)) {
    ParseExprForColumnPruning(expr->GetChildAt(0), output_cols);
    ParseExprForColumnPruning(expr->GetChildAt(1), output_cols);
  } else if (CheckColumnValue(expr)) {
    auto expr_ = dynamic_cast<const ColumnValueExpression *>(expr.get());
    output_cols.push_back(expr_->GetColIdx());
  }
}
void Optimizer::GetOutputCols(const std::vector<AbstractExpressionRef> &exprs, std::vector<uint32_t> &output_cols) {
  for (auto &x : exprs) {
    ParseExprForColumnPruning(x, output_cols);
  }
}
auto Optimizer::GetSchema(const SchemaRef &schema, std::vector<uint32_t> &output_cols) -> SchemaRef {
  auto origin_cols = schema->GetColumns();
  std::vector<Column> new_cols;
  for (auto &x : output_cols) {
    new_cols.push_back(origin_cols[x]);
  }
  return std::make_shared<Schema>(new_cols);
}

// ...=... -1代表恒为false,1代表恒为true,0代表不确定
auto Optimizer::GetFilterRes(const AbstractExpressionRef &expr) -> int {
  auto cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());

  auto l_expr = expr->GetChildAt(0);
  auto r_expr = expr->GetChildAt(1);

  if (CheckConstant(l_expr) && CheckConstant(r_expr)) {
    std::vector<Column> dummy_cols;
    Schema dummy_schema{dummy_cols};
    auto res = cmp_expr->Evaluate(nullptr, dummy_schema);

    if (res.CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
      return 1;
    }
    return -1;
  }

  return 0;
}

auto Optimizer::OptimizeColumnPruning(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  if (plan->GetType() == PlanType::Projection) {
    if (plan->GetChildAt(0)->GetType() == PlanType::Projection) {
      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto child_pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan->GetChildAt(0).get());
      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      auto child_exprs = child_pj_plan->GetExpressions();
      std::vector<AbstractExpressionRef> new_child_exprs;
      for (auto &col : output_cols) {
        new_child_exprs.push_back(child_exprs[col]);
      }

      auto new_pj_plan =
          std::make_shared<ProjectionPlanNode>(pj_plan->output_schema_, new_child_exprs, child_pj_plan->GetChildAt(0));
      auto optimize_pj_plan = OptimizeColumnPruning(new_pj_plan);

      return optimize_pj_plan;
    } else if (plan->GetChildAt(0)->GetType() == PlanType::Aggregation) {
      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto agg_plan = dynamic_cast<const AggregationPlanNode *>(plan->GetChildAt(0).get());

      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      auto aggs = agg_plan->GetAggregates();
      auto agg_types = agg_plan->GetAggregateTypes();
      std::vector<AbstractExpressionRef> new_aggs;
      std::vector<AggregationType> new_agg_types;

      for (auto &col : output_cols) {
        if (col == 0) continue;
        new_aggs.push_back(aggs[col - 1]);
        new_agg_types.push_back(agg_types[col - 1]);
      }

      auto new_output_schema = GetSchema(agg_plan->output_schema_, output_cols);

      auto new_agg_plan = std::make_shared<AggregationPlanNode>(new_output_schema, agg_plan->GetChildAt(0),
                                                                agg_plan->GetGroupBys(), new_aggs, new_agg_types);
      auto optimize_agg_plan = OptimizeColumnPruning(new_agg_plan);

      auto new_pj_plan =
          std::make_shared<ProjectionPlanNode>(pj_plan->output_schema_, pj_plan->GetExpressions(), optimize_agg_plan);
      return new_pj_plan;
    }
  } else if (plan->GetType() == PlanType::Filter) {
    // printf("filter\n");
    auto filter_plan = dynamic_cast<const FilterPlanNode *>(plan.get());
    auto expr = filter_plan->GetPredicate();
    int res = GetFilterRes(expr);
    if (res == -1) {
      std::vector<std::vector<AbstractExpressionRef>> values;

      return std::make_shared<ValuesPlanNode>(filter_plan->output_schema_, values);
    } else if (res == 1) {
      auto child_plan = OptimizeColumnPruning(filter_plan->GetChildAt(0));
      return child_plan;
    } else {
      std::vector<AbstractPlanNodeRef> children;
      for (const auto &child : plan->GetChildren()) {
        children.emplace_back(OptimizeColumnPruning(child));
      }
      auto optimized_filter_plan = filter_plan->CloneWithChildren(std::move(children));
      return optimized_filter_plan;
    }
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}
}  // namespace bustub
