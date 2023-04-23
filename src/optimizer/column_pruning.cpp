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
// leaderboard-3
auto Optimizer::CheckArithMetic(const AbstractExpressionRef &expr) -> bool {
  auto express = dynamic_cast<const ArithmeticExpression *>(expr.get());
  return express != nullptr;
}

// 解析当前表达式获取编号
// 这里支持的表达式有：colval (arithMetic:+,-) colval，和colval
void Optimizer::ParseExprForColumnPruning(const AbstractExpressionRef &expr, std::vector<uint32_t> &output_cols) {
  if (CheckArithMetic(expr)) {
    ParseExprForColumnPruning(expr->GetChildAt(0), output_cols);
    ParseExprForColumnPruning(expr->GetChildAt(1), output_cols);
  } else if (CheckColumnValue(expr)) {
    auto express = dynamic_cast<const ColumnValueExpression *>(expr.get());
    output_cols.push_back(express->GetColIdx());
  }
}

// 提取每个表达式要用到的cols编号
// 如#0.1 #0.2 #0.3+#0.4
// 提取出{1,2,3,4}
void Optimizer::GetOutputCols(const std::vector<AbstractExpressionRef> &exprs, std::vector<uint32_t> &output_cols) {
  for (auto &x : exprs) {
    // 解析当前表达式获取编号
    ParseExprForColumnPruning(x, output_cols);
  }
}

// 获取新的scheme，groupbys + 需要的aggs
auto Optimizer::GetSchema(const SchemaRef &schema, std::vector<uint32_t> &output_cols, size_t group_by_nums)
    -> SchemaRef {
  auto origin_cols = schema->GetColumns();
  std::vector<Column> new_cols;

  // 由于上层pj需要的列可能会重复，如#0.1,#0.2,#0.1+#0.2，此时下层agg只需输出#0.1和#0.2即可，但output_cols里
  // 存的是{1,2,1,2}，所以需要去重
  std::unordered_set<uint32_t> s;

  // 先把groups加进来,groups表达式的需要一定是0~groups_size-1(先输出的groups)
  for (size_t i = 0; i < group_by_nums; i++) {
    new_cols.push_back(origin_cols[i]);
    s.insert(i);
  }

  // 然后再将需要的aggs加进来
  for (auto &x : output_cols) {
    if (s.count(x) == 0U) {
      new_cols.push_back(origin_cols[x]);
      s.insert(x);
    }
  }
  return std::make_shared<Schema>(new_cols);
}

// 若比较表达式恒为true则返回1，恒为false则返回-1，否则返回0
// 返回1或-1的情况有：const cmp const 或者 expr本身是true或false(const类型)
// 其余带有变量的都会返回0
auto Optimizer::GetFilterRes(const AbstractExpressionRef &expr) -> int {
  auto cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());

  // expr为const类型表达式，本身是true或者false
  if (cmp_expr == nullptr) {
    if (CheckConstant(expr)) {
      auto value_expr = dynamic_cast<const ConstantValueExpression *>(expr.get());
      auto value = value_expr->val_;
      if (value.CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
        return 1;
      }
      if (value.CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue) {
        return -1;
      }
      return 0;
    }
  }

  auto l_expr = expr->GetChildAt(0);
  auto r_expr = expr->GetChildAt(1);

  // const cmp const
  if (CheckConstant(l_expr) && CheckConstant(r_expr)) {
    std::vector<Column> dummy_cols;
    Schema dummy_schema{dummy_cols};
    // 调用expr本身的比较函数来判断真假
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
      // pj->pj优化，合并成一个pj，只需要上层pj所需要的表达式即可即可

      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto child_pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan->GetChildAt(0).get());

      // 获取上层pj需要输出的下层pj里表达式的编号（列号）
      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      // 从下层的pj中根据编号提取出需要的表达式
      auto child_exprs = child_pj_plan->GetExpressions();
      std::vector<AbstractExpressionRef> new_child_exprs(output_cols.size());
      for (size_t i = 0; i < output_cols.size(); i++) {
        new_child_exprs[i] = child_exprs[output_cols[i]];
      }

      // 创建出新的pj_plan
      auto new_pj_plan =
          std::make_shared<ProjectionPlanNode>(pj_plan->output_schema_, new_child_exprs, child_pj_plan->GetChildAt(0));

      // 从新的pj_plan往下递归
      auto optimize_pj_plan = OptimizeColumnPruning(new_pj_plan);

      return optimize_pj_plan;
    }
    if (plan->GetChildAt(0)->GetType() == PlanType::Aggregation) {
      // 上层pj，下层agg，agg只需要输出group_by+上层pj需要的列即可（group_by一定要输出，agg_exec规定输出group_by）

      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto agg_plan = dynamic_cast<const AggregationPlanNode *>(plan->GetChildAt(0).get());

      // 获取上层pj需要的列
      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      // 获取所有agg的表达式和类型，即agg_plan原本需要输出的列
      auto aggs = agg_plan->GetAggregates();
      auto agg_types = agg_plan->GetAggregateTypes();

      // agg可能为空，此时无需优化，直接往下走即可，兼容前面测试的空表情况
      if (aggs.empty()) {
        std::vector<AbstractPlanNodeRef> children;
        for (const auto &child : plan->GetChildren()) {
          children.emplace_back(OptimizeColumnPruning(child));
        }
        auto optimized_plan = plan->CloneWithChildren(std::move(children));
        return optimized_plan;
      }

      // 提取上层pg需要的agg
      std::vector<AbstractExpressionRef> new_aggs;
      std::vector<AggregationType> new_agg_types;
      for (auto &col : output_cols) {
        // 上层需要的列中可能存在group_by，group_by不需要加入agg中
        if (col < agg_plan->GetGroupBys().size()) {
          continue;
        }
        // 由于agg的输出形式为group_bys + agg1 + agg2 + ...
        // output_cols的编号（pj看到的）是从group_size开始的，映射到agg表达式数组里时要减去一个偏移
        new_aggs.push_back(aggs[col - agg_plan->GetGroupBys().size()]);
        new_agg_types.push_back(agg_types[col - agg_plan->GetGroupBys().size()]);
      }

      // 获取新的scheme，groupbys+需要的aggs
      auto new_output_schema = GetSchema(agg_plan->output_schema_, output_cols, agg_plan->GetGroupBys().size());

      // 创造新的agg_plan
      auto new_agg_plan = std::make_shared<AggregationPlanNode>(new_output_schema, agg_plan->GetChildAt(0),
                                                                agg_plan->GetGroupBys(), new_aggs, new_agg_types);
      // 从新的agg_plan往下递归
      auto optimize_agg_plan = OptimizeColumnPruning(new_agg_plan);

      // 创造新的nj_plan，其孩子为优化后的new_agg_plan
      auto new_pj_plan =
          std::make_shared<ProjectionPlanNode>(pj_plan->output_schema_, pj_plan->GetExpressions(), optimize_agg_plan);
      // 返回新的nj_plan
      return new_pj_plan;
    }
  } else if (plan->GetType() == PlanType::Filter) {
    // 若为filter，要过滤掉恒为真或假的条件
    auto filter_plan = dynamic_cast<const FilterPlanNode *>(plan.get());
    auto expr = filter_plan->GetPredicate();

    // 得到filter的结果
    int res = GetFilterRes(expr);
    if (res == -1) {
      // 若恒为假，那filter下面的表就没用了，因为下面表提供的tuple都会因为条件为假而无法输出
      // 所以直接用一个空表代替即可
      std::vector<std::vector<AbstractExpressionRef>> values;
      return std::make_shared<ValuesPlanNode>(filter_plan->output_schema_, values);
    }
    if (res == 1) {
      // 如果恒为真，也不需要filter了，直接让下面的表输出即可
      auto child_plan = OptimizeColumnPruning(filter_plan->GetChildAt(0));
      return child_plan;
    }

    // 否则保留过滤条件
    std::vector<AbstractPlanNodeRef> children;
    for (const auto &child : plan->GetChildren()) {
      children.emplace_back(OptimizeColumnPruning(child));
    }
    auto optimized_filter_plan = filter_plan->CloneWithChildren(std::move(children));
    return optimized_filter_plan;
  }

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeColumnPruning(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  return optimized_plan;
}

}  // namespace bustub
