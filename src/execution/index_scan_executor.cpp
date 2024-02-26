//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_.get();
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
  vis_ = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (vis_) {
    return false;
  }
  vis_ = true;
  Schema dummy_schema({});
  auto key = plan_->pred_key_->Evaluate(nullptr, dummy_schema);
  auto idx = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  Tuple key_tu({key}, &idx->key_schema_);
  std::vector<RID> res;
  //用ScanKey搜索
  htable_->ScanKey(key_tu, &res, exec_ctx_->GetTransaction());
  if (res.empty()) {
    return false;
  }
  *rid = res[0];
  auto pair = table_heap_->GetTuple(*rid);
  if (pair.first.is_deleted_) {
    return false;
  }
  *tuple = pair.second;
  return true;
}

}  // namespace bustub
