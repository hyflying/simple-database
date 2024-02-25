//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {
  //catalog_ = exec_ctx_->GetCatalog();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  //table_heap_ = table_info_->table_.get();
  child_executor_->Init();
}
//
//auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
//  int32_t count = 0;
//  auto indexes = catalog_->GetTableIndexes(table_info_->name_);
//  while (child_executor_->Next(tuple,rid)){
//    count++;
//    table_heap_->UpdateTupleMeta({0,true}, tuple->GetRid());
//    for (auto &index_info : indexes) {
//      index_info->index_->DeleteEntry(*tuple, tuple->GetRid(),exec_ctx_->GetTransaction());
//    }
//  }
//  if(count == 0){
//    return false;
//  }
//  *tuple = Tuple{
//      std::vector<Value>{{TypeId::INTEGER, count}},
//      &this->GetOutputSchema()
//  };
//  return true;
//}
 auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
   Tuple in_tuple;
   while (true) {
     bool next = child_executor_->Next(&in_tuple, rid);
     if (!next) {
       if (flag_) {
         return false;
       }
       flag_ = true;
       std::vector<Value> values;
       values.emplace_back(ValueFactory::GetIntegerValue(nums_));
       *tuple = Tuple(values, &GetOutputSchema());
       return true;
     }
     nums_++;
     TupleMeta meta{0, true};
     table_info_->table_->UpdateTupleMeta(meta, *rid);
     auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
     for (auto &index_info : indexes) {
       auto attr = index_info->index_->GetKeyAttrs();
       BUSTUB_ENSURE(attr.size() == 1, "hashindex for many attrs?");
       Tuple key({in_tuple.GetValue(&table_info_->schema_, attr[0])}, &index_info->key_schema_);
       index_info->index_->DeleteEntry(key, *rid, nullptr);
     }
   }
   return true;
 }


}  // namespace bustub
