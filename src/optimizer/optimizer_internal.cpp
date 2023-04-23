#include <algorithm>
#include <memory>

#include <unordered_set>
#include "catalog/catalog.h"
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
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/values_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
namespace bustub {

// leaderboard-1
auto Optimizer::MatchTwoKeysIndex(const std::string &table_name, const std::vector<uint32_t> &index_key_idxs)
    -> std::optional<std::tuple<index_oid_t, std::string>> {
  const auto &key_attrs = index_key_idxs;
  for (const auto *index_info : catalog_.GetTableIndexes(table_name)) {
    if (key_attrs == index_info->index_->GetKeyAttrs()) {
      return std::make_optional(std::make_tuple(index_info->index_oid_, index_info->name_));
    }
  }
  return std::nullopt;
}

auto Optimizer::OptimizeSelectIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeOrderByAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    if (filter_plan.GetChildPlan()->GetType() == PlanType::SeqScan) {
      const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*filter_plan.GetChildPlan());
      const auto expression = filter_plan.GetPredicate();
      if (const auto *expr = dynamic_cast<const LogicExpression *>(expression.get()); expr != nullptr) {
        if (expr->logic_type_ == LogicType::And) {
          auto left_expression = expr->GetChildAt(0);
          auto right_expression = expr->GetChildAt(1);

          if (const auto *l_expr = dynamic_cast<const ComparisonExpression *>(left_expression.get());
              l_expr != nullptr) {
            if (l_expr->comp_type_ == ComparisonType::Equal || l_expr->comp_type_ == ComparisonType::GreaterThan ||
                l_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
              if (const auto *r_expr = dynamic_cast<const ComparisonExpression *>(right_expression.get());
                  r_expr != nullptr) {
                if (r_expr->comp_type_ == ComparisonType::Equal || r_expr->comp_type_ == ComparisonType::GreaterThan ||
                    r_expr->comp_type_ == ComparisonType::GreaterThanOrEqual) {
                  if (const auto *l_l_expr = dynamic_cast<const ColumnValueExpression *>(l_expr->GetChildAt(0).get());
                      l_l_expr != nullptr) {
                    if (const auto *l_r_expr =
                            dynamic_cast<const ConstantValueExpression *>(l_expr->GetChildAt(1).get());
                        l_r_expr != nullptr) {
                      if (const auto *r_l_expr =
                              dynamic_cast<const ColumnValueExpression *>(r_expr->GetChildAt(0).get());
                          r_l_expr != nullptr) {
                        if (const auto *r_r_expr =
                                dynamic_cast<const ConstantValueExpression *>(r_expr->GetChildAt(1).get());
                            r_r_expr != nullptr) {
                          std::vector<uint32_t> index_key_idxs_1{l_l_expr->GetColIdx(), r_l_expr->GetColIdx()};
                          std::vector<uint32_t> index_key_idxs_2{r_l_expr->GetColIdx(), l_l_expr->GetColIdx()};

                          if (auto index = MatchTwoKeysIndex(seq_scan.table_name_, index_key_idxs_1);
                              index != std::nullopt) {
                            auto [index_oid, index_name] = *index;
                            std::vector<Value> values{l_r_expr->val_, r_r_expr->val_};
                            return std::make_shared<IndexScanPlanNode>(filter_plan.output_schema_, index_oid, values,
                                                                       expression);
                          }
                          if (auto index = MatchTwoKeysIndex(seq_scan.table_name_, index_key_idxs_2);
                              index != std::nullopt) {
                            auto [index_oid, index_name] = *index;
                            std::vector<Value> values{r_r_expr->val_, l_r_expr->val_};
                            return std::make_shared<IndexScanPlanNode>(filter_plan.output_schema_, index_oid, values,
                                                                       expression);
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }

      } else if (const auto *expr = dynamic_cast<const ComparisonExpression *>(expression.get()); expr != nullptr) {
        if (const auto *l_expr = dynamic_cast<const ColumnValueExpression *>(expr->GetChildAt(0).get());
            l_expr != nullptr) {
          if (const auto *r_expr = dynamic_cast<const ConstantValueExpression *>(expr->GetChildAt(1).get());
              r_expr != nullptr) {
            if (auto index = MatchIndex(seq_scan.table_name_, l_expr->GetColIdx()); index != std::nullopt) {
              auto [index_oid, index_name] = *index;
              std::vector<Value> values{r_expr->val_};
              return std::make_shared<IndexScanPlanNode>(filter_plan.output_schema_, index_oid, values, expression);
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

// leaderboard-3
auto Optimizer::CheckArithMetic(const AbstractExpressionRef &expr) -> bool {
  auto express = dynamic_cast<const ArithmeticExpression *>(expr.get());
  return express != nullptr;
}

void Optimizer::ParseExprForColumnPruning(const AbstractExpressionRef &expr, std::vector<uint32_t> &output_cols) {
  if (CheckArithMetic(expr)) {
    ParseExprForColumnPruning(expr->GetChildAt(0), output_cols);
    ParseExprForColumnPruning(expr->GetChildAt(1), output_cols);
  } else if (CheckColumnValue(expr)) {
    auto express = dynamic_cast<const ColumnValueExpression *>(expr.get());
    output_cols.push_back(express->GetColIdx());
  }
}
void Optimizer::GetOutputCols(const std::vector<AbstractExpressionRef> &exprs, std::vector<uint32_t> &output_cols) {
  for (auto &x : exprs) {
    ParseExprForColumnPruning(x, output_cols);
  }
}
auto Optimizer::GetSchema(const SchemaRef &schema, std::vector<uint32_t> &output_cols, size_t group_by_nums)
    -> SchemaRef {
  auto origin_cols = schema->GetColumns();
  std::vector<Column> new_cols;

  std::unordered_set<uint32_t> s;
  for (size_t i = 0; i < group_by_nums; i++) {
    new_cols.push_back(origin_cols[i]);
    s.insert(i);
  }

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
  // std::cout << plan->ToString() << std::endl;
  if (plan->GetType() == PlanType::Projection) {
    if (plan->GetChildAt(0)->GetType() == PlanType::Projection) {
      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto child_pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan->GetChildAt(0).get());
      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      auto child_exprs = child_pj_plan->GetExpressions();
      std::vector<AbstractExpressionRef> new_child_exprs(output_cols.size());
      for (size_t i = 0; i < output_cols.size(); i++) {
        new_child_exprs[i] = child_exprs[output_cols[i]];
      }

      auto new_pj_plan =
          std::make_shared<ProjectionPlanNode>(pj_plan->output_schema_, new_child_exprs, child_pj_plan->GetChildAt(0));
      auto optimize_pj_plan = OptimizeColumnPruning(new_pj_plan);

      return optimize_pj_plan;
    }
    if (plan->GetChildAt(0)->GetType() == PlanType::Aggregation) {
      auto pj_plan = dynamic_cast<const ProjectionPlanNode *>(plan.get());
      auto agg_plan = dynamic_cast<const AggregationPlanNode *>(plan->GetChildAt(0).get());

      std::vector<uint32_t> output_cols;
      GetOutputCols(pj_plan->GetExpressions(), output_cols);

      auto aggs = agg_plan->GetAggregates();
      auto agg_types = agg_plan->GetAggregateTypes();

      if (aggs.empty()) {
        std::vector<AbstractPlanNodeRef> children;
        for (const auto &child : plan->GetChildren()) {
          children.emplace_back(OptimizeColumnPruning(child));
        }
        auto optimized_plan = plan->CloneWithChildren(std::move(children));
        return optimized_plan;
      }

      std::vector<AbstractExpressionRef> new_aggs;
      std::vector<AggregationType> new_agg_types;

      for (auto &col : output_cols) {
        if (col < agg_plan->GetGroupBys().size()) {
          continue;
        }
        new_aggs.push_back(aggs[col - agg_plan->GetGroupBys().size()]);
        new_agg_types.push_back(agg_types[col - agg_plan->GetGroupBys().size()]);
      }

      // printf("aggs_size:%zu\n", new_aggs.size());
      auto new_output_schema = GetSchema(agg_plan->output_schema_, output_cols, agg_plan->GetGroupBys().size());
      // printf("out_cols:%zu sche_cols:%d\n", output_cols.size(), new_output_schema->GetColumnCount());

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
    }
    if (res == 1) {
      auto child_plan = OptimizeColumnPruning(filter_plan->GetChildAt(0));
      return child_plan;
    }
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
