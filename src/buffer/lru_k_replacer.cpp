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
  head1_ = new LRUKNode(), tail1_ = new LRUKNode();
  head2_ = new LRUKNode(), tail2_ = new LRUKNode();

  head1_->right_ = tail1_, tail1_->left_ = head1_;
  head2_->right_ = tail2_, tail2_->left_ = head2_;

  size1_ = 0, size2_ = 0;
}

LRUKReplacer::~LRUKReplacer() {
  for (auto &[k, v] : node_store_1_) {
    delete (v);
  }
  for (auto &[k, v] : node_store_2_) {
    delete (v);
  }
  delete (head1_), delete (tail1_);
  delete (head2_), delete (tail2_);
}

void LRUKReplacer::RemoveNode(LRUKNode *node) {
  node->right_->left_ = node->left_;
  node->left_->right_ = node->right_;
}

void LRUKReplacer::InsertNode(LRUKNode *node1, LRUKNode *node2) {
  node2->right_ = node1->right_;
  node2->left_ = node1;
  node2->right_->left_ = node2;
  node2->left_->right_ = node2;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (size1_ == 0 && size2_ == 0) {
    return false;
  }

  if (size1_ != 0) {
    for (auto p = head1_->right_; p != tail1_; p = p->right_) {
      if (p->is_evictable_) {
        *frame_id = p->fid_;

        node_store_1_.erase(*frame_id);
        RemoveNode(p);
        delete (p);

        --size1_;
        --curr_size_;

        return true;
      }
    }
  }

  for (auto p = head2_->right_; p != tail2_; p = p->right_) {
    if (p->is_evictable_) {
      *frame_id = p->fid_;

      node_store_2_.erase(*frame_id);
      RemoveNode(p);
      delete (p);

      --size2_;
      --curr_size_;

      break;
    }
  }

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  // printf("record access\n");
  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    return;
  }

  ++current_timestamp_;

  if (node_store_1_.count(frame_id) == 0 && node_store_2_.count(frame_id) == 0) {
    auto new_node = new LRUKNode();
    new_node->fid_ = frame_id;
    node_store_1_[frame_id] = new_node;
    InsertNode(tail1_->left_, new_node);
  }

  LRUKNode *node;
  if (node_store_1_.count(frame_id) != 0U) {
    node = node_store_1_[frame_id];
  } else {
    node = node_store_2_[frame_id];
  }
  node->history_.push_back(current_timestamp_);

  if (node->history_.size() == k_) {
    RemoveNode(node);

    node_store_1_.erase(node->fid_);
    node_store_2_[node->fid_] = node;

    if (node->is_evictable_) {
      --size1_;
      ++size2_;
    }

    bool flag = false;
    for (auto p = head2_->right_; p != tail2_; p = p->right_) {
      if (p->history_.front() > node->history_.front()) {
        InsertNode(p->left_, node);
        flag = true;
        break;
      }
    }

    if (!flag) {
      InsertNode(tail2_->left_, node);
    }

  } else if (node->history_.size() > k_) {
    node->history_.pop_front();

    bool flag = false;
    for (auto p = node->right_; p != tail2_; p = p->right_) {
      if (p->history_.front() > node->history_.front()) {
        RemoveNode(node);
        InsertNode(p->left_, node);
        flag = true;
        break;
      }
    }

    if (!flag) {
      RemoveNode(node);
      InsertNode(tail2_->left_, node);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    return;
  }

  if (node_store_1_.count(frame_id) == 0 && node_store_2_.count(frame_id) == 0) {
    return;
  }

  if (node_store_1_.count(frame_id) != 0U) {
    auto node = node_store_1_[frame_id];
    if (!node->is_evictable_ && set_evictable) {
      node->is_evictable_ = true;
      ++size1_;
      ++curr_size_;
    } else if (node->is_evictable_ && !set_evictable) {
      node->is_evictable_ = false;
      --size1_;
      --curr_size_;
    }
  } else {
    auto node = node_store_2_[frame_id];
    if (!node->is_evictable_ && set_evictable) {
      node->is_evictable_ = true;
      ++size2_;
      ++curr_size_;
    } else if (node->is_evictable_ && !set_evictable) {
      node->is_evictable_ = false;
      --size2_;
      --curr_size_;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<int32_t>(replacer_size_)) {
    return;
  }

  if (node_store_1_.count(frame_id) == 0U && node_store_2_.count(frame_id) == 0U) {
    return;
  }

  if (node_store_1_.count(frame_id) != 0U) {
    auto node = node_store_1_[frame_id];
    if (node->is_evictable_) {
      --size1_;
      --curr_size_;
    }
    RemoveNode(node);
    node_store_1_.erase(node->fid_);
    delete (node);
  } else {
    auto node = node_store_2_[frame_id];
    if (node->is_evictable_) {
      --size2_;
      --curr_size_;
    }
    RemoveNode(node);
    node_store_2_.erase(node->fid_);
    delete (node);
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}
}  // namespace bustub
