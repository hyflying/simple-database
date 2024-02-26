//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
}

void InsertExecutor::Init() {
  child_executor_->Init();
  Column col("num", INTEGER);
  std::vector<Column> vec{col};
  schema_ = std::make_shared<const Schema>(vec);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  RID in_rid;
  Tuple in_tuple;
  while (true) {
    bool status = child_executor_->Next(&in_tuple, &in_rid);
    if (!status) {
      if (flag_) {
        return false;
      }
      flag_ = true;
      std::vector<Value> values;
      values.push_back(ValueFactory::GetIntegerValue(nums_));
      *tuple = Tuple(values, schema_.get());
      return true;
    }

    // check the tuple already in index?
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    //    BUSTUB_ENSURE(indexes.size() <= 1, "not only one index");
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      std::vector<RID> result;
      //      index_info->index_->ScanKey(key, &result, exec_ctx_->GetTransaction());
      //      BUSTUB_ENSURE(result.size() == 1, "index more than one tuple");
    }

    auto rid_opt =
        table_info_->table_->InsertTuple({exec_ctx_->GetTransaction()->GetTransactionTempTs(), false}, in_tuple);

    if (rid_opt.has_value()) {
      *rid = rid_opt.value();
    }
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      bool res = index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      if (!res) {
        throw ExecutionException("insert conflict");
      }
    }
    nums_++;
  }
}

}  // namespace bustub