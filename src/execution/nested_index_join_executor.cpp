//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor = std::move(child_executor);
}

void NestIndexJoinExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  RightTableInfo = catalog->GetTable(plan_->GetInnerTableOid());
  indexInfo = catalog->GetIndex(plan_->GetIndexOid());
  left_executor->Init();
  ptr = 0;
}

void NestIndexJoinExecutor::GetOutputTuple(Tuple *tuple, bool is_matched) {
  std::vector<Value> out_values;
  auto left_cols_cnt = left_executor->GetOutputSchema().GetColumnCount();
  auto right_cols_cnt = RightTableInfo->schema_.GetColumnCount();

  for (uint32_t i = 0; i < left_cols_cnt; i++) {
    out_values.push_back(left_tuple.GetValue(&left_executor->GetOutputSchema(), i));
  }

  if (is_matched) {
    for (uint32_t i = 0; i < right_cols_cnt; i++) {
      out_values.push_back(right_tuples[ptr].GetValue(&RightTableInfo->schema_, i));
    }
    ++ptr;
  } else {
    for (uint32_t i = 0; i < right_cols_cnt; i++) {
      out_values.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
    }
  }

  *tuple = Tuple(out_values, &GetOutputSchema());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // 如果当前左tuple对应的右tuple没有用完，则继续用
    if (ptr < right_tuples.size()) {
      GetOutputTuple(tuple, true);
      return true;
    }

    if (!left_executor->Next(&left_tuple, rid)) {
      return false;
    }

    // 获取左tuple用于在右表对应的index树里用于匹配的key值
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, left_executor->GetOutputSchema());
    std::vector<Value> values{value};
    Tuple tuple_ = Tuple(values, indexInfo->index_->GetKeySchema());

    // 在index树里匹配
    std::vector<RID> result;
    indexInfo->index_->ScanKey(tuple_, &result, exec_ctx_->GetTransaction());

    if (result.size() == 0) {
      // 如果匹配失败，则填充null输出
      if (plan_->GetJoinType() == JoinType::LEFT) {
        GetOutputTuple(tuple, false);
        return true;
      }

      continue;

    } else {
      right_tuples.clear();

      // 根据匹配的到的RID，找到匹配的右表中的所有完整的tuple，然后存起来
      for (auto &x : result) {
        auto right_tuple = RightTableInfo->table_->GetTuple(x).second;
        right_tuples.push_back(right_tuple);
      }
      ptr = 0;

      GetOutputTuple(tuple, true);
      return true;
    }
  }

  return true;
}

}  // namespace bustub
