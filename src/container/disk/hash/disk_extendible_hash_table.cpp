//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  page_id_t header_page_id = INVALID_PAGE_ID;
  BasicPageGuard basic_header_guard = bpm_->NewPageGuarded(&header_page_id);
  auto header_guard = basic_header_guard.UpgradeWrite();
  header_page_id_ = header_page_id;
  auto *header_page = header_guard.template AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *header_page = page_guard.template As<ExtendibleHTableHeaderPage>();
  // get directory page
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  page_guard.Drop();
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto *directory_page = directory_guard.template As<ExtendibleHTableDirectoryPage>();
  // get bucket page
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_guard.Drop();
  auto bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto *bucket_page = bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  auto success = bucket_page->Lookup(key, value, cmp_);
  if (success) {
    result->push_back(value);
  }
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = page_guard.template AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }
  page_guard.Drop();
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto *directory_page = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto *bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  //能直接insert
  if (!bucket_page->IsFull()) {
    auto success = bucket_page->Insert(key, value, cmp_);
    return success;
  }
  auto local_depth = directory_page->GetLocalDepth(bucket_idx);
  auto global_depth = directory_page->GetGlobalDepth();
  if (local_depth == directory_max_depth_) {
    return false;
  }
  V val;
  //必须要先判断是否存在，否则key是重复的不需要插入，但却分裂了桶
  auto key_exist = bucket_page->Lookup(key, val, cmp_);
  if (key_exist) {
    return false;
  }
  if (local_depth == global_depth) {
    directory_page->IncrGlobalDepth();
  }
  //  if(local_depth < global_depth){
  //原bucket的local_depth+1
  directory_page->IncrLocalDepth(bucket_idx);
  //分裂一个新bucket
  auto new_bucket_idx = directory_page->GetSplitImageIndex(bucket_idx);
  auto new_local_depth = directory_page->GetLocalDepth(bucket_idx);
  page_id_t new_bucket_page_id;
  auto basic_bucket_page_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket_page_guard = basic_bucket_page_guard.UpgradeWrite();
  auto *new_bucket_page = new_bucket_page_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);
  uint32_t diff = 1U << new_local_depth;
  for (auto i = bucket_idx + diff; i < directory_page->Size(); i += diff) {
    directory_page->SetLocalDepth(i, new_local_depth);
    directory_page->SetBucketPageId(i, bucket_page_id);
  }
  for (auto i = bucket_idx; i >= 0; i -= diff) {
    directory_page->SetLocalDepth(i, new_local_depth);
    directory_page->SetBucketPageId(i, bucket_page_id);
    if (i < diff) {
      break;
    }
  }
  UpdateDirectoryMapping(directory_page, new_bucket_idx, new_bucket_page_id, new_local_depth, diff);
  for (uint32_t i = 0; i < bucket_page->Size(); i++) {
    auto entry = bucket_page->EntryAt(i);
    //有新的桶了，要重新分配原来桶里的元素
    auto bucket_id_key = directory_page->HashToBucketIndex(Hash(entry.first));
    auto bucket_page_id_key = directory_page->GetBucketPageId(bucket_id_key);
    // key不再分配给原来的bucket了
    if (bucket_page_id_key != bucket_page_id) {
      bucket_page->RemoveAt(i);
      new_bucket_page->Insert(entry.first, entry.second, cmp_);
    }
  }
  if (directory_page->HashToBucketIndex(hash) == bucket_idx) {
    return bucket_page->Insert(key, value, cmp_);
  }
  return new_bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  auto basic_directory_guard = bpm_->NewPageGuarded(&directory_page_id);
  auto directory_guard = basic_directory_guard.UpgradeWrite();
  auto *directory_page = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  return InsertToNewBucket(directory_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  auto basic_bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_guard = basic_bucket_guard.UpgradeWrite();
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  auto *bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  auto success = bucket_page->Insert(key, value, cmp_);
  return success;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t diff) {
  for (auto i = new_bucket_idx + diff; i < directory->Size(); i += diff) {
    directory->SetLocalDepth(i, new_local_depth);
    directory->SetBucketPageId(i, new_bucket_page_id);
  }
  for (auto i = new_bucket_idx; i >= 0; i -= diff) {
    directory->SetLocalDepth(i, new_local_depth);
    directory->SetBucketPageId(i, new_bucket_page_id);
    if (i < diff) {
      break;
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = page_guard.template AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  page_guard.Drop();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto *directory_page = directory_guard.template AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto *bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto success = bucket_page->Remove(key, cmp_);
  bucket_guard.Drop();
  for (uint32_t i = 0; i < directory_page->Size(); i++) {
    auto id = directory_page->GetBucketPageId(i);
    auto guard = bpm_->FetchPageWrite(id);
    auto page = guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    MergeBucket(directory_page, page, i);
    guard.Drop();
  }
  while (success && directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }
  return success;
}
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBucket(ExtendibleHTableDirectoryPage *directory_page,
                                                    ExtendibleHTableBucketPage<K, V, KC> *bucket_page,
                                                    uint32_t bucket_id) {
  if (!bucket_page->IsEmpty()) {
    return;
  }
  auto local_depth = directory_page->GetLocalDepth(bucket_id);
  if (local_depth == 0) {
    return;
  }
  auto split_bucket_idx = bucket_id ^ (1U << (local_depth - 1));
  auto split_bucket_page_id = directory_page->GetBucketPageId(split_bucket_idx);
  auto split_local_depth = directory_page->GetLocalDepth(split_bucket_idx);
  if (split_bucket_page_id == INVALID_PAGE_ID || local_depth != split_local_depth) {
    return;
  }
  auto split_bucket_guard = bpm_->FetchPageWrite(split_bucket_page_id);
  auto split_bucket_page = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bpm_->DeletePage(directory_page->GetBucketPageId(bucket_id));
  directory_page->SetBucketPageId(bucket_id, split_bucket_page_id);
  directory_page->DecrLocalDepth(bucket_id);
  directory_page->DecrLocalDepth(split_bucket_idx);
  MergeBucket(directory_page, split_bucket_page, split_bucket_idx);
}
template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
