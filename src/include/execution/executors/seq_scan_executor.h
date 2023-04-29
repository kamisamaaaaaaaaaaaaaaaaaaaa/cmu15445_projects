//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  ~SeqScanExecutor() override;

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  auto TryLockTable(const bustub::LockManager::LockMode &lock_mode, const table_oid_t &oid) {
    std::string type;
    if (lock_mode == bustub::LockManager::LockMode::EXCLUSIVE) {
      type = "X";
    } else if (lock_mode == bustub::LockManager::LockMode::INTENTION_EXCLUSIVE) {
      type = "IX";
    } else if (lock_mode == bustub::LockManager::LockMode::INTENTION_SHARED) {
      type = "IS";
    } else if (lock_mode == bustub::LockManager::LockMode::SHARED) {
      type = "S";
    } else if (lock_mode == bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE) {
      type = "SIX";
    }

    try {
      bool success = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, oid);
      if (!success) {
        const std::string info = "seqscan table " + type + " lock fail";
        init_throw_error = true;
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "seqscan table " + type + " lock fail";
      init_throw_error = true;
      throw ExecutionException(info);
    }
  }

  auto TryUnLockTable(const table_oid_t &oid) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);
      if (!success) {
        const std::string info = "seqscan table unlock fail";
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "seqscan table unlock fail";
      throw ExecutionException(info);
    }
  }

  auto TryLockRow(const bustub::LockManager::LockMode &lock_mode, const table_oid_t &oid, const RID &rid) {
    std::string type;
    if (lock_mode == bustub::LockManager::LockMode::EXCLUSIVE) {
      type = "X";
    } else if (lock_mode == bustub::LockManager::LockMode::SHARED) {
      type = "S";
    }

    try {
      auto success = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, oid, rid);

      if (!success) {
        const std::string info = "seqscan row " + type + " lock fail";
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "seqscan row " + type + " lock fail";
      throw ExecutionException(info);
    }
  }

  auto TryUnLockRow(const table_oid_t &oid, const RID &rid, bool force = false) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid, force);
      if (!success) {
        const std::string info = "seqscan row unlock fail";
        throw ExecutionException(info);
      }
    } catch (TransactionAbortException &e) {
      const std::string info = "seqscan row unlock fail";
      throw ExecutionException(info);
    }
  }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  TableIterator *iter_;
  table_oid_t table_oid_;
  bool init_throw_error{false};
};
}  // namespace bustub
