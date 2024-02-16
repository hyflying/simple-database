//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (uint32_t i = 0; i < MaxSize(); i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  uint32_t mask = (1U << global_depth_) - 1;
  return mask & hash;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= Size()) {
    return INVALID_PAGE_ID;
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= Size()) {
    return;
  }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // GetSplitImageIndex要在global_depth_更新之后。
  return bucket_idx ^ (1U << (global_depth_ - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (global_depth_ >= max_depth_) {
    return;
  }
  uint32_t origin = (1U << global_depth_);
  for (uint32_t i = 0; i < origin; i++) {
    local_depths_[origin + i] = local_depths_[i];
    bucket_page_ids_[origin + i] = bucket_page_ids_[i];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (global_depth_ == 0) {
    return;
  }
  global_depth_--;
  auto target = 1U << global_depth_;
  for (uint32_t i = 0; i < target; i++) {
    local_depths_[target + i] = 0;
    bucket_page_ids_[target + i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < (1U << global_depth_); i++) {
    if (global_depth_ == local_depths_[i]) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1U << global_depth_; }
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1U << max_depth_; }
auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1U << global_depth_) - 1; }
auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1U << local_depths_[bucket_idx]) - 1;
}

}  // namespace bustub
