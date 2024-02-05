//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
  // Getter for fid_
 public:
  LRUKNode() = default;
  LRUKNode(size_t id, size_t k) : fid_(id), k_(k) {}
  ~LRUKNode() = default;
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.
  auto GetEvictable() -> bool { return is_evictable_; }
  void SetEvictable(bool evictable) { is_evictable_ = evictable; }
  auto GetHistory() { return history_; }
  void AddHistory(size_t timestamp) { history_.push_front(timestamp); }
  void RemoveHistory() {
    while (!history_.empty()) {
      history_.pop_back();
    }
  }
  auto GetFid() { return fid_; }
  void SetK(size_t k) { k_ = k; }
  auto GetKDistance(size_t cur) -> uint64_t {
    // 如果一个帧的历史访问次数少于k次，则将其后向k距离设置为正无穷大（+inf）
    if (history_.size() < k_) {
      return UINT32_MAX;
    }
    // 获取k次前访问的时间戳
    size_t k_distance;
    auto it = history_.begin();
    std::advance(it, k_ - 1);
    k_distance = *it;
    return cur - k_distance;
  }
  auto GetLastAccess() -> size_t {
    if (history_.empty()) {
      return UINT32_MAX;
    }
    return history_.front();
  }
  auto GetBackAccess() -> size_t {
    if (history_.empty()) {
      return UINT32_MAX;
    }
    return history_.back();
  }

 private:
  frame_id_t fid_;
  std::list<size_t> history_;
  size_t k_;
  bool is_evictable_{false};
  [[maybe_unused]] AccessType access_type_;
  // u_int32_t access_times_{1};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_store_;
  [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_size_{0};
  // size_t evictable_size_{0};
  size_t replacer_size_;  // max num of frame
  size_t k_;              // 满足k次将元素放入k_list
  // std::vector<std::shared_ptr<LRUKNode>> inf_list_;  // 访问次数小于k次
  // std::vector<std::shared_ptr<LRUKNode>> k_list_;    // 访问次数到达k次
  std::mutex latch_;
};

}  // namespace bustub
