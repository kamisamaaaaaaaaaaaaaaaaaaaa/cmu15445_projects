#include <algorithm>
#include <memory>

#include "catalog/catalog.h"
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
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {
auto Optimizer::MatchTwoKeysIndex(const std::string &table_name, std::vector<uint32_t> index_key_idxs)
    -> std::optional<std::tuple<index_oid_t, std::string>> {
  const auto key_attrs = index_key_idxs;
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
                          } else if (auto index = MatchTwoKeysIndex(seq_scan.table_name_, index_key_idxs_2);
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
}  // namespace bustub