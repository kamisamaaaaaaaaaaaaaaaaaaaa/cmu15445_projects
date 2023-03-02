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

namespace bustub
{

    LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

    auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool
    {
        if (curr_size_ == 0)
            return false;

        --curr_size_;

        // 有INF节点先删INF
        if (node_store_1.size())
        {
            // 按LRU，也就是最后一个his最小的
            *frame_id = (*S1.begin()).second;
            S1.erase({(*S1.begin()).first, (*S1.begin()).second});

            delete node_store_1[*frame_id];
            node_store_1.erase(*frame_id);
        }
        else
        {
            // 删倒数第k个his最小的
            *frame_id = (*S2.begin()).second;
            S2.erase({(*S2.begin()).first, (*S2.begin()).second});

            delete node_store_2[*frame_id];
            node_store_2.erase(*frame_id);
        }

        return true;
    }

    void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type)
    {
        ++current_timestamp_;
        if (node_store_1.count(frame_id) == 0 && node_store_2.count(frame_id) == 0 && inevictable_store.count(frame_id) == 0)
        {
            auto t = new LRUKNode();
            // 没有节点，先创建，并且一开始是不可驱逐的
            inevictable_store[frame_id] = t;
        }

        // 如果不可驱逐，更新history，直接返回即可
        if (inevictable_store.count(frame_id))
        {
            inevictable_store[frame_id]->history_.push_back(current_timestamp_);
            return;
        }

        // 获取该点，更新his
        LRUKNode *node;

        if (node_store_1.count(frame_id))
            node = node_store_1[frame_id];
        else if (node_store_2.count(frame_id))
            node = node_store_2[frame_id];

        node->history_.push_back(current_timestamp_);

        // 小于k，说明原本在s1，更新后还在s1
        if (node->history_.size() < k_)
        {
            // 原本在s1中的节点的val应该是该节点的倒数第二个，根据其找到节点删掉
            S1.erase({node->history_[node->history_.size() - 2], frame_id});
            // 插入新的节点
            S1.insert({current_timestamp_, frame_id});
        }
        else
        {
            // 否则说明更新后在s2

            // 有可能原本在s1，此时要删掉
            if (node_store_1.count(frame_id))
            {
                node_store_1.erase(frame_id);
                S1.erase({node->history_[node->history_.size() - 2], frame_id});
            }

            // 有可能原本不在S2，此时要插入
            if (node_store_2.count(frame_id) == 0)
            {
                S2.insert({node->history_[node->history_.size() - k_], frame_id});
                node_store_2[frame_id] = node;
            }
            else
            {
                // 否则先删掉s2原来的，再插入
                S1.erase({node->history_[node->history_.size() - k_ - 1], frame_id});
                S2.insert({node->history_[node->history_.size() - k_], frame_id});
            }
        }
    }

    void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
    {
        if (node_store_1.count(frame_id) == 0 && node_store_2.count(frame_id) == 0 && inevictable_store.count(frame_id) == 0)
            return;

        if (set_evictable)
        {
            // 如果在s1和s2，说明本身就是可驱逐的，不用管
            if (node_store_1.count(frame_id) || node_store_2.count(frame_id))
                return;
            else
            {
                // 否则将node从不可驱逐列表中删除，并根据其his插入s1或s2
                curr_size_++;
                auto node = inevictable_store[frame_id];
                inevictable_store.erase(frame_id);
                if (node->history_.size() < k_)
                {
                    node_store_1[frame_id] = node;
                    S1.insert({node->history_.back(), frame_id});
                }
                else
                {
                    node_store_2[frame_id] = node;
                    S2.insert({node->history_[node->history_.size() - k_], frame_id});
                }
            }
        }
        else
        {
            // 如果在inevictable_store，说明本身就是不可驱逐的，不用管
            if (inevictable_store.count(frame_id))
                return;

            // 否则在s1或s2中找到并删除，并且加入不可驱逐列表
            curr_size_--;
            if (node_store_1.count(frame_id))
            {
                auto node = node_store_1[frame_id];

                node_store_1.erase(frame_id);
                S1.erase({node->history_.back(), frame_id});

                inevictable_store[frame_id] = node;
            }
            else
            {
                auto node = node_store_2[frame_id];

                node_store_2.erase(frame_id);
                S2.erase({node->history_[node->history_.size() - k_], frame_id});

                inevictable_store[frame_id] = node;
            }
        }
    }

    void LRUKReplacer::Remove(frame_id_t frame_id)
    {
        if (node_store_1.count(frame_id) == 0 && node_store_2.count(frame_id) == 0 && inevictable_store.count(frame_id) == 0)
            return;

        // 若在S1中，则从S1中删除
        if (node_store_1.count(frame_id))
        {
            auto node = node_store_1[frame_id];
            node_store_1.erase(frame_id);
            S1.erase({node->history_.back(), frame_id});
            delete node;
        }
        else
        {
            // 否则从S2中删除
            auto node = node_store_2[frame_id];
            node_store_2.erase(frame_id);
            S2.erase({node->history_[node->history_.size() - k_], frame_id});
            delete node;
        }
    }

    auto LRUKReplacer::Size() -> size_t { return curr_size_; }

} // namespace bustub
