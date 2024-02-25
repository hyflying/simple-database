//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  iterator_ = std::make_shared<TableIterator>(table_info->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while(true){
    if(iterator_->IsEnd()){
      return false;
    }
    auto meta = iterator_->GetTuple().first;
    if(meta.is_deleted_){
      ++(*iterator_);
      continue;
    }
    *tuple = iterator_->GetTuple().second;
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(tuple, GetOutputSchema());
      if (value.IsNull() || !value.GetAs<bool>()) {
        ++(*iterator_);
        continue;
      }
    }
    *rid = iterator_->GetRID();
    ++(*iterator_);
    break;
  }
  return true;
}

}  // namespace bustub
