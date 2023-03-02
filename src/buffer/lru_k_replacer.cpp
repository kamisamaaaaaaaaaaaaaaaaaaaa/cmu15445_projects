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

    LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k)
    {
        head1 = new LRUKNode(), tail1 = new LRUKNode();
        head2 = new LRUKNode(), tail2 = new LRUKNode();

        head1->right = tail1, tail1->left = head1;
        head2->right = tail2, tail2->left = head2;
    }

    LRUKReplacer::~LRUKReplacer()
    {
        // 释放所有指针，防止内存泄露
        for (auto p = head1->right; p != tail1;)
        {
            auto next = p->right;
            delete p;
            p = next;
        }

        for (auto p = head2->right; p != tail2;)
        {
            auto next = p->right;
            delete p;
            p = next;
        }

        delete head1;
        delete head2;
        delete tail1;
        delete tail2;
    }

    // 判断链表1是否有可驱逐的点
    bool LRUKReplacer::empty1()
    {
        for (auto p = head1->right; p != tail1; p = p->right)
        {
            if (p->is_evictable_)
                return false;
        }
        return true;
    }

    auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool
    {
        // current_timestamp_++;
        if (curr_size_ == 0)
            return false;

        // 没有INF才删链表2，从前往后找，找到第一个可驱逐的最旧的点
        if (empty1())
        {
            auto t = head2->right;
            while (!t->is_evictable_)
                t = t->right;

            size_t id = t->fid_;
            *frame_id = id;

            curr_size_--;

            remove(node_store_2[id]);
            node_store_2.erase(id);

            delete t;
        }
        // 有INF，优先驱逐INF，按照LRU，从后往前，找到第一个可驱逐的最旧的点
        else
        {
            auto t = tail1->left;
            while (!t->is_evictable_)
                t = t->left;

            size_t id = t->fid_;
            *frame_id = id;

            curr_size_--;

            remove(node_store_1[id]);
            node_store_1.erase(id);

            delete t;
        }

        return true;
    }

    void LRUKReplacer::insert(LRUKNode *pre, LRUKNode *u)
    {
        u->right = pre->right;
        u->left = pre;
        u->right->left = u;
        u->left->right = u;
    }

    void LRUKReplacer::remove(LRUKNode *u)
    {
        u->left->right = u->right;
        u->right->left = u->left;
    }

    void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type)
    {
        current_timestamp_++;

        // 不存在这个点的话先创建这个点，然后插入链表1
        if (node_store_1.count(frame_id) == 0 && node_store_2.count(frame_id) == 0)
        {
            auto t = new LRUKNode();
            t->fid_ = frame_id;
            insert(head1, t);
            node_store_1[frame_id] = t;
        }

        // 获取这个点
        LRUKNode *node;
        if (node_store_1.count(frame_id))
            node = node_store_1[frame_id];
        else if (node_store_2.count(frame_id))
            node = node_store_2[frame_id];

        // 更新history
        node->history_.push_back(current_timestamp_);

        // 若更新完后少于k次访问，则插入链1头部
        if (node->history_.size() < k_)
        {
            remove(node);
            insert(head1, node);
        }
        else
        {
            // 否则在链2，找到合适的位置插进去，链2永远是按倒数第k个history递增的，从前往后找即可
            remove(node);

            // 如果原本在链1，要删去
            if (node_store_1.count(frame_id))
                node_store_1.erase(frame_id);

            // 如果没在链2，要插入
            if (node_store_2.count(frame_id) == 0)
                node_store_2[frame_id] = node;

            auto t = head2;
            while (t->right != tail2 && t->right->history_[t->right->history_.size() - k_] < node->history_[node->history_.size() - k_])
            {
                t = t->right;
            }
            insert(t, node);
        }
    }

    void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
    {
        // current_timestamp_++;
        if (node_store_1.count(frame_id) == 0 && node_store_2.count(frame_id) == 0)
            return;

        LRUKNode *node;
        if (node_store_1.count(frame_id))
            node = node_store_1[frame_id];
        else if (node_store_2.count(frame_id))
            node = node_store_2[frame_id];

        if (!node->is_evictable_ && set_evictable)
        {
            curr_size_++;
        }
        else if (node->is_evictable_ && !set_evictable)
        {
            curr_size_--;
        }

        node->is_evictable_ = set_evictable;
    }

    void LRUKReplacer::Remove(frame_id_t frame_id)
    {
        // current_timestamp_++;
        if (node_store_1.count(frame_id) == 0 && node_store_2.count(frame_id) == 0)
            return;

        LRUKNode *node;
        if (node_store_1.count(frame_id))
            node = node_store_1[frame_id];
        else if (node_store_2.count(frame_id))
            node = node_store_2[frame_id];

        curr_size_--;

        remove(node);
        if (node_store_1.count(frame_id))
            node_store_1.erase(frame_id);
        else if (node_store_2.count(frame_id))
            node_store_2.erase(frame_id);

        delete node;
    }

    auto LRUKReplacer::Size() -> size_t { return curr_size_; }

} // namespace bustub
