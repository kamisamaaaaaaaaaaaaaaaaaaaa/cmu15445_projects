#include <memory>
#include <vector>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"

#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeMergeFilterScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeMergeFilterScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Filter) {
    const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
    BUSTUB_ASSERT(optimized_plan->children_.size() == 1, "must have exactly one children");
    const auto &child_plan = *optimized_plan->children_[0];
    if (child_plan.GetType() == PlanType::SeqScan) {
      const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(child_plan);
      if (CheckFilterExpr(filter_plan.GetPredicate())) {
        auto cmp_expr = dynamic_cast<const ComparisonExpression *>(filter_plan.GetPredicate().get());
        if (cmp_expr->comp_type_ == ComparisonType::Equal) {
          auto l_expr = dynamic_cast<const ColumnValueExpression *>(cmp_expr->GetChildAt(0).get());
          auto r_expr = dynamic_cast<const ConstantValueExpression *>(cmp_expr->GetChildAt(1).get());
          if (auto index = MatchIndex(seq_scan_plan.table_name_, l_expr->GetColIdx()); index != std::nullopt) {
            auto [index_oid, index_name] = *index;
            std::vector<Value> key_value{r_expr->val_};
            return std::make_shared<IndexScanPlanNode>(filter_plan.output_schema_, index_oid, key_value,
                                                       filter_plan.GetPredicate(), true);
          }
        }
      }

      if (seq_scan_plan.filter_predicate_ == nullptr) {
        return std::make_shared<SeqScanPlanNode>(filter_plan.output_schema_, seq_scan_plan.table_oid_,
                                                 seq_scan_plan.table_name_, filter_plan.GetPredicate());
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
