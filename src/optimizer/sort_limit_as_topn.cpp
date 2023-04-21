#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);

    BUSTUB_ENSURE(limit_plan.children_.size() == 1, "LimitPlanNode should have exactly 1 child.");

    auto const limit_plan_child = limit_plan.GetChildPlan();

    if (limit_plan_child->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*limit_plan_child);

      BUSTUB_ENSURE(sort_plan.children_.size() == 1, "SortPlanNode should have exactly 1 child.");

      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildPlan(), sort_plan.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
