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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() {
  for (auto &[k, v] : node_store_1_) {
    delete v;
  }
  for (auto &[k, v] : node_store_2_) {
    delete v;
  }
  for (auto &[k, v] : inevictable_store_) {
    delete v;
  }

  node_store_1_.clear();
  node_store_2_.clear();
  inevictable_store_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  --curr_size_;

  // 有INF节点先删INF
  if (!node_store_1_.empty()) {
    // 删掉his[0]最小的
    *frame_id = (*s1_.begin()).second;
    s1_.erase({(*s1_.begin()).first, (*s1_.begin()).second});

    delete node_store_1_[*frame_id];
    node_store_1_.erase(*frame_id);
  } else {
    // 删倒数第k个his最小的

    *frame_id = (*s2_.begin()).second;
    s2_.erase({(*s2_.begin()).first, (*s2_.begin()).second});

    delete node_store_2_[*frame_id];
    node_store_2_.erase(*frame_id);
  }

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  ++current_timestamp_;
  if (node_store_1_.count(frame_id) == 0U && node_store_2_.count(frame_id) == 0U &&
      inevictable_store_.count(frame_id) == 0U) {
    // 没有节点，先创建，并且一开始是不可驱逐的
    inevictable_store_[frame_id] = new LRUKNode();
  }

  // 如果不可驱逐，更新history，直接返回即可
  if (inevictable_store_.count(frame_id) != 0U) {
    inevictable_store_[frame_id]->history_.push_back(current_timestamp_);
    return;
  }

  // 获取该点，更新his
  LRUKNode *node = nullptr;

  if (node_store_1_.count(frame_id) != 0U) {
    node = node_store_1_[frame_id];
  } else if (node_store_2_.count(frame_id) != 0U) {
    node = node_store_2_[frame_id];
  }

  node->history_.push_back(current_timestamp_);

  // 小于k，说明原本已经按照{his[0],id}存在S1了，不用管，直接返回
  if (node->history_.size() < k_) {
    return;
  }

  // 否则说明更新后在s2_

  // 有可能原本在s1_，此时要删掉
  if (node_store_1_.count(frame_id) != 0U) {
    node_store_1_.erase(frame_id);
    s1_.erase({node->history_[0], frame_id});
  }

  // 有可能原本不在s2_，此时要插入
  if (node_store_2_.count(frame_id) == 0U) {
    s2_.insert({node->history_[node->history_.size() - k_], frame_id});
    node_store_2_[frame_id] = node;
  } else {
    // 否则先删掉s2_原来的，再插入
    s2_.erase({node->history_[node->history_.size() - k_ - 1], frame_id});
    s2_.insert({node->history_[node->history_.size() - k_], frame_id});
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  if (node_store_1_.count(frame_id) == 0U && node_store_2_.count(frame_id) == 0U &&
      inevictable_store_.count(frame_id) == 0U) {
    return;
  }

  if (set_evictable) {
    // 如果在s1_和s2_，说明本身就是可驱逐的，不用管
    if (node_store_1_.count(frame_id) != 0U || node_store_2_.count(frame_id) != 0U) {
      return;
    }

    // 否则将node从不可驱逐列表中删除，并根据其his插入s1_或s2_

    curr_size_++;
    auto node = inevictable_store_[frame_id];
    inevictable_store_.erase(frame_id);
    if (node->history_.size() < k_) {
      node_store_1_[frame_id] = node;
      s1_.insert({node->history_[0], frame_id});
    } else {
      node_store_2_[frame_id] = node;
      s2_.insert({node->history_[node->history_.size() - k_], frame_id});
    }

  } else {
    // 如果在inevictable_store_，说明本身就是不可驱逐的，不用管
    if (inevictable_store_.count(frame_id) != 0U) {
      return;
    }

    // 否则在s1_或s2_中找到并删除，并且加入不可驱逐列表
    curr_size_--;
    if (node_store_1_.count(frame_id) != 0U) {
      auto node = node_store_1_[frame_id];

      node_store_1_.erase(frame_id);
      s1_.erase({node->history_[0], frame_id});

      inevictable_store_[frame_id] = node;
    } else {
      auto node = node_store_2_[frame_id];

      node_store_2_.erase(frame_id);
      s2_.erase({node->history_[node->history_.size() - k_], frame_id});

      inevictable_store_[frame_id] = node;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (node_store_1_.count(frame_id) == 0 && node_store_2_.count(frame_id) == 0 &&
      inevictable_store_.count(frame_id) == 0) {
    return;
  }

  if (inevictable_store_.count(frame_id) != 0U) {
    return;
  }

  --curr_size_;

  // 若在s1_中，则从s1_中删除
  if (node_store_1_.count(frame_id) != 0U) {
    auto node = node_store_1_[frame_id];
    node_store_1_.erase(frame_id);
    s1_.erase({node->history_[0], frame_id});
    delete node;
  } else {
    // 否则从s2_中删除
    auto node = node_store_2_[frame_id];
    node_store_2_.erase(frame_id);
    s2_.erase({node->history_[node->history_.size() - k_], frame_id});
    delete node;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
