//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <iostream>
#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "type/type.h"
#include "type/value_factory.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple in_tuple;
  while (true) {
    bool next = child_executor_->Next(&in_tuple, rid);
    if (!next) {
      if (flag_) {
        return false;
      }
      flag_ = true;
      std::vector<Value> values;
      values.push_back(ValueFactory::GetIntegerValue(nums_));
      *tuple = Tuple{values, &GetOutputSchema()};
      return true;
    }
    nums_++;

    auto del_tuple_info = table_info_->table_->GetTuple(*rid);
    TupleMeta meta{0, true};
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      index_info->index_->DeleteEntry(key, *rid, nullptr);
    }
    std::vector<Value> values;
    for (const auto &target_expression : plan_->target_expressions_) {
      auto value = target_expression->Evaluate(&in_tuple, table_info_->schema_);
      values.push_back(value);
    }
    Tuple new_tuple(values, &table_info_->schema_);
    auto rid_opt = table_info_->table_->InsertTuple({0, false}, new_tuple);
    if (!rid_opt.has_value()) {
      return false;
    }
    *rid = rid_opt.value();
    for (auto &index_info : indexes) {
      auto attr = index_info->index_->GetKeyAttrs();
      BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
      Tuple key({new_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
      index_info->index_->InsertEntry(key, *rid, nullptr);
    }
  }
}

}  // namespace bustub