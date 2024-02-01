//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  if(num_frames <= 0 || k <= 0){
    throw bustub::Exception(ExceptionType::INVALID,"Invalid parameter");
  }
}
void eraseFrame(std::vector<std::shared_ptr<LRUKNode>>& list, frame_id_t frameId){
  for(auto it=list.begin(); it != list.end(); it++){
    if((*it)->fid_ == frameId){
      list.erase(it);
      return;
    }
  }
}
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if(curr_size_ == 0){
    return false;
  }
  //先到inf_list找 遍历遇到的第一个is_evict = true的元素
  //没有的话到到k_list找，遍历遇到到第一个is_evict = true的元素
  for(auto it = inf_list_.begin(); it != inf_list_.end(); it++){
    if((*it)->is_evictable_){
      curr_size_--;
      *frame_id = (*it)->fid_;
      evictable_size_--;
      inf_list_.erase(it);
      node_store_.erase(*frame_id);
      return true;
    }
  }
  for(auto  it = k_list_.begin(); it < k_list_.end(); it++){
    if((*it)->is_evictable_){
      curr_size_--;
      evictable_size_--;
      *frame_id = (*it)->fid_;
      k_list_.erase(it);
      node_store_.erase(*frame_id);
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::shared_ptr<LRUKNode> node_ptr;
  bool exist = node_store_.find(frame_id) != node_store_.end();
  if(!exist){
    node_ptr = std::make_shared<LRUKNode>(frame_id, access_type);
    curr_size_++;
    node_store_[frame_id] = node_ptr;
    inf_list_.push_back(node_ptr);
  } else{
    node_ptr = node_store_[frame_id];
    node_ptr->access_times_ += 1;
    node_ptr->access_type_ = access_type;
    if(node_ptr->access_times_ < k_){
      eraseFrame(inf_list_, frame_id);
      inf_list_.push_back(node_ptr);
    } else if(node_ptr->access_times_ == k_){
      eraseFrame(inf_list_, frame_id);
      k_list_.push_back(node_ptr);
    }else{
      eraseFrame(k_list_, frame_id);
      k_list_.push_back(node_ptr);
    }
  }
  if(curr_size_ > replacer_size_){
    int value;
    bool canEvict = Evict(&value);
    if(!canEvict){
      throw Exception(ExceptionType::OUT_OF_MEMORY,"replacer out of memory");
    }
    eraseFrame(k_list_, value);
    eraseFrame(inf_list_, value);
    node_store_.erase(value);
  }

}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if(node_store_.find(frame_id) == node_store_.end()){
    throw Exception(ExceptionType::INVALID,"Invalid frame_id");
  }
  auto node_ptr = node_store_[frame_id];
  if((node_ptr->is_evictable_)^set_evictable){
    node_ptr->is_evictable_ = set_evictable;
    if(set_evictable){
      evictable_size_++;
    }else{
      evictable_size_--;
    }

  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if(node_store_.find(frame_id) == node_store_.end()){
    return;
  }
  auto node_ptr = node_store_[frame_id];
  if(!node_ptr->is_evictable_){
    throw Exception(ExceptionType::INVALID, "can't be removed");
  }
  evictable_size_--;
  curr_size_--;
  node_store_.erase(frame_id);
  for(auto it = inf_list_.begin(); it != inf_list_.end(); it++){
    if((*it)->fid_ == frame_id){
      inf_list_.erase(it);
    }
  }
  for(auto it = k_list_.begin(); it != inf_list_.end(); it++){
    if((*it)->fid_ == frame_id){
      k_list_.erase(it);
    }
  }
}

auto LRUKReplacer::Size() -> size_t { return evictable_size_; }



}  // namespace bustub
