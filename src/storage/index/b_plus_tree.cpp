#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

// 二分查找
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const LeafPage *leaf_page, const KeyType &key) -> int {
  // std::cout << "binary" << leaf_page << std::endl;
  int l = 0;
  int r = leaf_page->GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }

  if (r >= 0 && comparator_(leaf_page->KeyAt(r), key) == 1) {
    r = -1;
  }

  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const InternalPage *internal_page, const KeyType &key) -> int {
  // if (internal_page == nullptr) return -1;

  // if (internal_page->GetSize() == 0) return 0;

  int l = 1;
  int r = internal_page->GetSize() - 1;
  // std::cout << "Binary: "
  //           << " key: " << key << " " << r << std::endl;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    // std::cout << "mid: " << mid << std::endl;
    if (comparator_(internal_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }

  if (r == -1 || comparator_(internal_page->KeyAt(r), key) == 1) {
    r = 0;
  }

  return r;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;

  // printf("---------------------------------\n");
  // std::cout << "GetValue: " << key << std::endl;

  // printf("Tree as Below-----------------------------------\n");
  // Print(bpm_);
  // printf("Tree as Above-----------------------------------\n");

  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();
  // auto *header_page = reinterpret_cast<BPlusTreeHeaderPage *>(bpm_->FetchPage(header_page_id_)->GetData());

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ctx.root_page_id_ = root_page_guard.PageId();
  // auto *root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(header_page->root_page_id_)->GetData());

  ctx.read_set_.emplace_back(std::move(root_page_guard));

  while (true) {
    // std::cout << "enter" << std::endl;
    if (root_page->IsLeafPage()) {
      // std::cout << "leaf process" << std::endl;
      auto *leaf = reinterpret_cast<const LeafPage *>(root_page);

      int index = BinaryFind(leaf, key);

      if (index < 0 || comparator_(leaf->KeyAt(index), key) != 0) {
        while (!ctx.read_set_.empty()) {
          ctx.read_set_.back().SetDirty(false);
          ctx.read_set_.back().Drop();
          ctx.read_set_.pop_back();
        }
        // while (!ids.empty()) {
        //   bpm_->UnpinPage(ids.back(), false);
        //   ids.pop_back();
        // }
        // std::cout << "Can not Find" << std::endl;
        // std::cout << "size: " << leaf->GetSize() << "index: " << index << " not find: " << key << std::endl;
        return false;
      }

      // std::cout << "size: " << leaf->GetSize() << "index: " << index << " find: " << key << std::endl;

      result->emplace_back(leaf->ValueAt(index));
      // std::cout << "increase" << std::endl;

      break;
    }

    // std::cout << "internal process" << std::endl;
    auto *internal = reinterpret_cast<const InternalPage *>(root_page);

    // std::cout << "start internal binary find" << std::endl;
    int index = BinaryFind(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    // ids.emplace_back(child_id);
    root_page_guard = bpm_->FetchPageRead(child_id);
    root_page = root_page_guard.As<BPlusTreePage>();
    ctx.read_set_.emplace_back(std::move(root_page_guard));
    // std::cout << "child_id: " << internal->ValueAt(index) << std::endl;
    // root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(child_id)->GetData());
  }

  // while (!ids.empty()) {
  //   bpm_->UnpinPage(ids.back(), false);
  //   ids.pop_back();
  // }
  while (!ctx.read_set_.empty()) {
    ctx.read_set_.back().SetDirty(false);
    ctx.read_set_.back().Drop();
    ctx.read_set_.pop_back();
  }

  header_page_guard.SetDirty(false);
  header_page_guard.Drop();
  // bpm_->UnpinPage(header_page_id_, false);

  // std::cout << "Find it!" << std::endl;
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf, const KeyType &key, const ValueType &value, page_id_t *new_id)
    -> KeyType {
  bool put_left = false;
  int mid = leaf->GetMinSize();
  KeyType mid_key = leaf->KeyAt(mid);
  if (comparator_(mid_key, key) == -1) {
    if (leaf->GetMaxSize() % 2 == 1) {
      mid++;
    }
  } else {
    if (leaf->GetMaxSize() % 2 == 0) {
      if (comparator_(leaf->KeyAt(mid - 1), key) == 1) {
        put_left = true;
        mid--;
      }
    } else {
      put_left = true;
    }
  }

  // auto *page = bpm_->NewPage(new_id);
  // std::cout << "new_page: " << page << std::endl;
  // if (page == nullptr) std::cout << "null_ptr" << std::endl;

  auto new_leaf_basic_guard = bpm_->NewPageGuarded(new_id);
  auto new_leaf = new_leaf_basic_guard.AsMut<LeafPage>();
  // auto *new_leaf = reinterpret_cast<LeafPage *>(bpm_->NewPage(new_id)->GetData());
  new_leaf_basic_guard.SetDirty(true);
  new_leaf_basic_guard.Drop();

  auto new_leaf_guard = bpm_->FetchPageWrite(*new_id);
  new_leaf = new_leaf_guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);
  // std::cout << new_leaf << std::endl;
  // std::cout << *new_id << "是否为叶节点： " << new_leaf->IsLeafPage() << std::endl;

  // std::cout << "leaf size: " << leaf->GetSize() << std::endl;

  // std::cout << "origin_leaf: ";
  // for (int i = 0; i < leaf->GetSize(); i++) {
  //   std::cout << leaf->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;

  // std::cout << "mid: " << mid << " put_left: " << put_left << std::endl;
  int leaf_size = leaf->GetSize();
  for (int i = mid, j = 0; i < leaf_size; i++, j++) {
    // std::cout << "move" << std::endl;
    new_leaf->SetAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
    new_leaf->IncreaseSize(1);
    leaf->IncreaseSize(-1);
  }

  // std::cout << "leaf: ";
  // for (int i = 0; i < leaf->GetSize(); i++) {
  //   std::cout << leaf->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;

  // std::cout << "new_leaf: ";
  // for (int i = 0; i < new_leaf->GetSize(); i++) {
  //   std::cout << new_leaf->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;

  // std::cout << "leaf size: " << leaf->GetSize() << std::endl;
  // std::cout << "new leaf size: " << new_leaf->GetSize() << std::endl;

  auto put_in_leaf = leaf;
  if (!put_left) {
    put_in_leaf = new_leaf;
  }

  int idx = BinaryFind(put_in_leaf, key);

  for (int i = put_in_leaf->GetSize(); i > idx + 1; i--) {
    put_in_leaf->SetAt(i, put_in_leaf->KeyAt(i - 1), put_in_leaf->ValueAt(i - 1));
  }
  put_in_leaf->SetAt(idx + 1, key, value);
  put_in_leaf->IncreaseSize(1);

  // if (put_left) {
  //   int idx = BinaryFind(leaf, key);

  //   for (int i = leaf->GetSize(); i > idx + 1; i--) {
  //     leaf->SetAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
  //   }
  //   leaf->SetAt(idx + 1, key, value);
  //   leaf->IncreaseSize(1);
  // } else {
  //   int idx = BinaryFind(new_leaf, key);

  //   for (int i = new_leaf->GetSize(); i > idx + 1; i--) {
  //     new_leaf->SetAt(i, new_leaf->KeyAt(i - 1), new_leaf->ValueAt(i - 1));
  //   }
  //   new_leaf->SetAt(idx + 1, key, value);
  //   new_leaf->IncreaseSize(1);
  // }

  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(*new_id);

  KeyType up_key = new_leaf->KeyAt(0);

  new_leaf_guard.SetDirty(true);
  new_leaf_guard.Drop();

  // std::cout << "right first key is: " << new_leaf->KeyAt(0) << std::endl;
  return up_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal, const KeyType &key, page_id_t *new_id,
                                   page_id_t new_child_id) -> KeyType {
  bool put_left = false;
  int mid = internal->GetMinSize();
  KeyType mid_key = internal->KeyAt(mid);

  KeyType up_key{};
  page_id_t up_key_id = -1;

  // 调整mid和up_key
  if (comparator_(mid_key, key) == -1) {
    if (comparator_(key, internal->KeyAt(mid + 1)) == 1) {
      up_key = internal->KeyAt(mid + 1);
    } else {
      up_key = key;
    }
    mid++;
  } else {
    up_key = internal->KeyAt(mid);
    put_left = true;
  }

  up_key_id = internal->ValueAt(mid);

  // 将up_key删掉
  // page_id_t up_key_page_id = internal->ValueAt(mid);
  if (comparator_(up_key, key) != 0) {
    // std::cout << "up_key_mid:" << mid << std::endl;
    for (int i = mid; i < internal->GetSize() - 1; i++) {
      internal->SetAt(i, internal->KeyAt(i + 1), internal->ValueAt(i + 1));
    }
    internal->IncreaseSize(-1);
  }

  auto new_internal_basic_guard = bpm_->NewPageGuarded(new_id);
  auto new_internal = new_internal_basic_guard.AsMut<InternalPage>();
  // auto *new_internal = reinterpret_cast<InternalPage *>(bpm_->NewPage(new_id)->GetData());
  new_internal_basic_guard.SetDirty(true);
  new_internal_basic_guard.Drop();

  auto new_internal_guard = bpm_->FetchPageWrite(*new_id);
  new_internal = new_internal_guard.AsMut<InternalPage>();
  new_internal->Init(internal_max_size_);

  // std::cout << "origin_internal_size: " << internal->GetSize() << std::endl;

  // std::cout << "origin_internal: ";
  // for (int i = 1; i < internal->GetSize(); i++) {
  //   std::cout << internal->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;

  // std::cout << "up_key: " << up_key << " mid: " << mid << std::endl;
  int internal_size = internal->GetSize();
  // std::cout << "inter_size: " << internal_size << std::endl;
  for (int i = mid, j = 1; i < internal_size; i++, j++) {
    // std::cout << "enter" << std::endl;
    new_internal->SetAt(j, internal->KeyAt(i), internal->ValueAt(i));
    new_internal->IncreaseSize(1);
    internal->IncreaseSize(-1);
  }
  // 注意size是孩子数，比key多1
  new_internal->IncreaseSize(1);
  // std::cout << "internal: ";
  // for (int i = 1; i < internal->GetSize(); i++) {
  //   std::cout << internal->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;

  // std::cout << "new_internal: ";
  // for (int i = 1; i < new_internal->GetSize(); i++) {
  //   std::cout << new_internal->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;
  // std::cout << new_internal->GetSize() << std::endl;

  if (comparator_(up_key, key) != 0) {
    auto put_in_internal = internal;
    if (!put_left) {
      put_in_internal = new_internal;
    }
    int idx = BinaryFind(put_in_internal, key);

    for (int i = put_in_internal->GetSize(); i > idx + 1; i--) {
      put_in_internal->SetAt(i, put_in_internal->KeyAt(i - 1), put_in_internal->ValueAt(i - 1));
    }
    put_in_internal->SetAt(idx + 1, key, new_child_id);

    if (!put_left && put_in_internal->GetSize() == 0) {
      put_in_internal->SetSize(2);
    } else {
      put_in_internal->IncreaseSize(1);
    }
    new_internal->SetAt(0, KeyType{}, up_key_id);
    // if (put_left) {
    //   int idx = BinaryFind(internal, key);

    //   for (int i = internal->GetSize(); i > idx + 1; i--) {
    //     internal->SetAt(i, internal->KeyAt(i - 1), internal->ValueAt(i - 1));
    //   }
    //   internal->SetAt(idx + 1, key, new_child_id);

    //   internal->IncreaseSize(1);

    // } else {
    //   int idx = BinaryFind(new_internal, key);

    //   // std::cout << idx << std::endl;

    //   for (int i = new_internal->GetSize(); i > idx + 1; i--) {
    //     new_internal->SetAt(i, new_internal->KeyAt(i - 1), new_internal->ValueAt(i - 1));
    //   }
    //   new_internal->SetAt(idx + 1, key, new_child_id);
    //   if (new_internal->GetSize() == 0) {
    //     new_internal->IncreaseSize(2);
    //   } else {
    //     new_internal->IncreaseSize(1);
    //   }
    // }
    // new_internal->SetAt(0, KeyType{}, up_key_id);
  } else {
    new_internal->SetAt(0, KeyType{}, new_child_id);
  }
  // std::cout << "internal: ";
  // for (int i = 1; i < internal->GetSize(); i++) {
  //   std::cout << internal->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;

  // std::cout << "new_internal: ";
  // for (int i = 1; i < new_internal->GetSize(); i++) {
  //   std::cout << new_internal->KeyAt(i) << " ";
  // }
  // std::cout << std::endl;

  // if (comparator_(key, up_key) == 0) {
  // }
  // if (new_internal->GetSize() == 2) {
  //   new_internal->SetAt(0, KeyType{}, up_key_id);
  // }

  new_internal_guard.SetDirty(true);
  new_internal_guard.Drop();

  return up_key;
};

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  // printf("-----------------------------------------\n");

  // std::cout << "insert: " << key << std::endl;

  // BasicPageGuard guard = bpm_->FetchPageBasic(header_page_id_);
  // auto root_page = guard.AsMut<BPlusTreeHeaderPage>();

  // std::cout << "header_page_id: " << header_page_id_ << std::endl;
  ctx.header_page_guard_ = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = ctx.header_page_guard_->AsMut<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    BasicPageGuard root_page_basic_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
    auto root_page_basic = root_page_basic_guard.AsMut<LeafPage>();
    // auto root_page = reinterpret_cast<LeafPage *>(bpm_->NewPage(&header_page->root_page_id_)->GetData());
    root_page_basic->Init(leaf_max_size_);
    root_page_basic_guard.SetDirty(true);
    root_page_basic_guard.Drop();
  }

  WritePageGuard root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto *root_page = root_page_guard.AsMut<BPlusTreePage>();
  ctx.root_page_id_ = root_page_guard.PageId();
  // auto root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(header_page->root_page_id_)->GetData());

  // std::cout << header_page->root_page_id_ << std::endl;

  // ids.emplace_back(header_page->root_page_id_);
  ctx.write_set_.emplace_back(std::move(root_page_guard));

  while (true) {
    // std::cout << "enter" << std::endl;
    // std::cout << "是否为叶节点: " << root_page->IsLeafPage() << std::endl;
    if (root_page->IsLeafPage()) {
      // std::cout << "leaf process" << std::endl;
      auto *leaf = reinterpret_cast<LeafPage *>(root_page);

      int index = BinaryFind(leaf, key);

      // std::cout << "key: " << key << " index: " << index << std::endl;

      if (index >= 0 && comparator_(leaf->KeyAt(index), key) == 0) {
        return false;
      }

      if (leaf->GetSize() == leaf->GetMaxSize()) {
        // printf("开始分裂\n");
        // //-------------------处理叶子节点的分裂------------------
        // printf("叶子分裂\n");
        page_id_t new_id;
        KeyType up_key = SplitLeaf(leaf, key, value, &new_id);
        // auto *new_leaf = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(new_id)->GetData());
        // std::cout << new_leaf << std::endl;
        // std::cout << "new_leaf is leaf: " << new_leaf->IsLeafPage() << std::endl;
        page_id_t new_child_id = new_id;
        // printf("叶子分裂的up_key为： ");
        // std::cout << up_key << std::endl;
        // printf("new_leaf_id： ");
        // std::cout << new_id << std::endl;

        ctx.write_set_.back().SetDirty(true);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();
        // bpm_->UnpinPage(ids.back(), true);
        // ids.pop_back();

        // bpm_->UnpinPage(new_id, true);

        // -------------------处理完叶子节点的分裂------------------

        bool split_fin = false;
        while (!ctx.write_set_.empty()) {
          auto internal_parent_guard = std::move(ctx.write_set_.back());
          ctx.write_set_.pop_back();

          auto internal_parent = internal_parent_guard.AsMut<InternalPage>();
          // auto internal_parent = reinterpret_cast<InternalPage *>(parent);

          if (internal_parent->GetSize() < internal_parent->GetMaxSize()) {
            // printf("internal分裂结束!\n");
            // std::cout << "final_up_key: " << up_key << std::endl;
            int idx = BinaryFind(internal_parent, up_key);
            // std::cout << "最后分裂的idx: " << idx << std::endl;

            for (int i = internal_parent->GetSize(); i > idx + 1; i--) {
              internal_parent->SetAt(i, internal_parent->KeyAt(i - 1), internal_parent->ValueAt(i - 1));
            }
            internal_parent->SetAt(idx + 1, up_key, new_child_id);
            internal_parent->IncreaseSize(1);
            split_fin = true;

            internal_parent_guard.SetDirty(true);
            internal_parent_guard.Drop();
            // bpm_->UnpinPage(ids.back(), true);
            // ids.pop_back();

            while (!ctx.write_set_.empty()) {
              ctx.write_set_.back().SetDirty(false);
              ctx.write_set_.back().Drop();
              ctx.write_set_.pop_back();
            }

            break;
          }
          // printf("internal分裂!\n");
          up_key = SplitInternal(internal_parent, up_key, &new_id, new_child_id);
          // printf("internal分裂的up_key为： ");
          // std::cout << up_key << std::endl;

          internal_parent_guard.SetDirty(true);
          internal_parent_guard.Drop();
          // bpm_->UnpinPage(ids.back(), true);
          // ids.pop_back();
          // bpm_->UnpinPage(new_id, true);

          new_child_id = new_id;
        }

        if (!split_fin) {
          // printf("根节点分裂!\n");
          page_id_t old_id = header_page->root_page_id_;

          auto new_root_page_basic_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
          auto new_root_page = new_root_page_basic_guard.AsMut<InternalPage>();
          ctx.root_page_id_ = new_root_page_basic_guard.PageId();
          new_root_page_basic_guard.SetDirty(true);
          new_root_page_basic_guard.Drop();

          auto new_root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
          new_root_page = new_root_page_guard.AsMut<InternalPage>();
          new_root_page->Init(internal_max_size_);
          // auto new_root_page = reinterpret_cast<InternalPage
          // *>(bpm_->NewPage(&header_page->root_page_id_)->GetData()); std::cout << "根的old id为：" << old_id <<
          // std::endl; std::cout << "根的new id为：" << header_page->root_page_id_ << std::endl;
          new_root_page->Init(internal_max_size_);
          // std::cout << "new_id:" << new_id << " old_id:" << old_id << std::endl;
          new_root_page->SetAt(1, up_key, new_id);
          new_root_page->SetAt(0, KeyType{}, old_id);
          new_root_page->SetSize(2);
          new_root_page_guard.SetDirty(true);
          new_root_page_guard.Drop();
          // bpm_->UnpinPage(header_page->root_page_id_, true);
          // std::cout << new_root_page->ValueAt(0) << " | " << new_root_page->KeyAt(1) << " | "
          //           << new_root_page->ValueAt(1) << std::endl;

          // auto left = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(1)->GetData());
          // auto right = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(2)->GetData());

          // std::cout << left << "left是否为叶节点:" << left->IsLeafPage() << std::endl;
          // std::cout << right << "right是否为叶节点:" << right->IsLeafPage() << std::endl;
        }

        break;
      }

      // std::cout << "increase" << std::endl;

      for (int i = leaf->GetSize(); i > index + 1; i--) {
        leaf->SetAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
      }

      leaf->SetAt(index + 1, key, value);

      leaf->IncreaseSize(1);

      ctx.write_set_.back().SetDirty(true);
      ctx.write_set_.back().Drop();
      ctx.write_set_.pop_back();

      while (!ctx.write_set_.empty()) {
        ctx.write_set_.back().SetDirty(false);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();
      }
      // bpm_->UnpinPage(ids.back(), true);
      // ids.pop_back();

      // while (!ids.empty()) {
      //   bpm_->UnpinPage(ids.back(), false);
      //   ids.pop_back();
      // }

      // std::cout << "insert index: " << index << " key: " << key << " Size: " << leaf->GetSize() << std::endl;

      break;
    }

    // std::cout << "internal process" << std::endl;
    auto *internal = reinterpret_cast<InternalPage *>(root_page);

    // std::cout << "start internal binary find" << std::endl;
    int index = BinaryFind(internal, key);

    // std::cout << "find_index: " << index << std::endl;

    page_id_t child_id = internal->ValueAt(index);

    // std::cout << "child_id: " << child_id << std::endl;

    // ids.emplace_back(child_id);

    root_page_guard = bpm_->FetchPageWrite(child_id);
    root_page = root_page_guard.AsMut<BPlusTreePage>();
    ctx.write_set_.emplace_back(std::move(root_page_guard));
  }

  ctx.header_page_guard_->SetDirty(true);
  ctx.header_page_guard_->Drop();
  // bpm_->UnpinPage(header_page_id_, true);
  // std::cout << "pool_size: " << bpm_->GetPoolSize() << " Usable_size: " << bpm_->Get_Usable_Size() << std::endl;

  // printf("Tree as Below-----------------------------------\n");
  // Print(bpm_);
  // printf("Tree as Above-----------------------------------\n");

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove_Res_Guards_Pop(std::deque<WritePageGuard> &guards, std::deque<int> &keys_index,
                                           const KeyType &origin_key, const KeyType &new_key) {
  while (!guards.empty()) {
    auto parent_guard = std::move(guards.back());
    guards.pop_back();
    auto parent_internal = parent_guard.AsMut<InternalPage>();

    if (comparator_(parent_internal->KeyAt(keys_index.back()), origin_key) == 0) {
      parent_internal->SetKeyAt(keys_index.back(), new_key);
      parent_guard.SetDirty(true);
    } else {
      parent_guard.SetDirty(false);
    }
    parent_guard.Drop();
    keys_index.pop_back();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // printf("---------------------------------------------------\n");
  // std::cout << "Remove: " << key << std::endl;
  // printf("-------------Tree As Below--------------\n");
  // Print(bpm_);
  // printf("-------------Tree As Above---------------\n");

  Context ctx;

  auto header_page_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return;
  }

  std::deque<int> keys_index;

  auto root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto root_page = root_page_guard.AsMut<BPlusTreePage>();

  ctx.write_set_.emplace_back(std::move(root_page_guard));

  while (true) {
    if (root_page->IsLeafPage()) {
      auto leaf = reinterpret_cast<LeafPage *>(root_page);
      int index = BinaryFind(leaf, key);

      if (index < 0 || comparator_(leaf->KeyAt(index), key) != 0) {
        while (!ctx.write_set_.empty()) {
          ctx.write_set_.back().SetDirty(false);
          ctx.write_set_.back().Drop();
          ctx.write_set_.pop_back();
        }
        break;
      }

      // 先删掉
      for (int i = index; i < leaf->GetSize() - 1; i++) {
        leaf->SetAt(i, leaf->KeyAt(i + 1), leaf->ValueAt(i + 1));
      }
      leaf->IncreaseSize(-1);

      if (ctx.write_set_.size() == 1) {
        ctx.write_set_.back().SetDirty(true);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();
        break;
      }

      // std::cout << leaf->GetSize() << " " << leaf->GetMinSize() << std::endl;

      if (leaf->GetSize() >= leaf->GetMinSize()) {
        // 叶子弹掉
        KeyType leaf_first = leaf->KeyAt(0);
        ctx.write_set_.back().SetDirty(true);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();

        // 看下要不要换关键字
        Remove_Res_Guards_Pop(ctx.write_set_, keys_index, key, leaf_first);
        // while (!guards.empty()) {
        //   auto parent_guard = std::move(guards.back());
        //   guards.pop_back();
        //   auto parent_internal = parent_guard.AsMut<InternalPage>();

        //   if (comparator_(parent_internal->KeyAt(keys_index.back()), key) == 0) {
        //     parent_internal->SetKeyAt(keys_index.back(), leaf_first);
        //     parent_guard.SetDirty(true);
        //   } else {
        //     parent_guard.SetDirty(false);
        //   }
        //   parent_guard.Drop();
        //   keys_index.pop_back();
        // }

        break;

      } else {
        // 情况1:兄弟可借(左兄弟可借的话先借左兄弟,再借右兄弟)

        // 情况2:兄弟不可借,此时合并,有左兄弟的话统一合并到左兄弟,否则右兄弟

        // 弹出叶节点
        auto leaf_guard = std::move(ctx.write_set_.back());
        ctx.write_set_.pop_back();

        // 弹出一个parent
        auto parent_guard = std::move(ctx.write_set_.back());
        ctx.write_set_.pop_back();
        auto parent_internal = parent_guard.AsMut<InternalPage>();
        auto parent_index = keys_index.back();
        keys_index.pop_back();

        // 下面记得搞完要给叶子drop_guard,还要修改parent
        //  看下叶子左兄弟
        if (parent_index > 0) {
          auto left_id = parent_internal->ValueAt(parent_index - 1);
          auto left_guard = bpm_->FetchPageWrite(left_id);
          auto left_leaf = left_guard.template AsMut<LeafPage>();

          // 把左兄弟最后一个借走，放到leaf的第一个位置
          if (left_leaf->GetSize() > left_leaf->GetMinSize()) {
            // printf("借叶子左兄弟！\n");
            for (int i = leaf->GetSize(); i >= 1; i--) {
              leaf->SetAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
            }
            leaf->SetAt(0, left_leaf->KeyAt(left_leaf->GetSize() - 1), left_leaf->ValueAt(left_leaf->GetSize() - 1));
            leaf->IncreaseSize(1);
            left_leaf->IncreaseSize(-1);

            // 然后修改parent的关键字
            //  auto parent_origin_key = parent_internal->KeyAt(parent_index);
            parent_internal->SetKeyAt(parent_index, leaf->KeyAt(0));
            parent_guard.SetDirty(true);
            parent_guard.Drop();

            KeyType leaf_first = leaf->KeyAt(0);
            // 修改了leaf和左兄弟
            leaf_guard.SetDirty(true);
            leaf_guard.Drop();

            left_guard.SetDirty(true);
            left_guard.Drop();

            // 把剩下的guard全部drop掉
            Remove_Res_Guards_Pop(ctx.write_set_, keys_index, key, leaf_first);
            // while (!guards.empty()) {
            //   parent_guard = std::move(guards.back());
            //   guards.pop_back();
            //   parent_internal = parent_guard.AsMut<InternalPage>();

            //   if (comparator_(parent_internal->KeyAt(keys_index.back()), key) == 0) {
            //     parent_internal->SetKeyAt(keys_index.back(), leaf_first);
            //     parent_guard.SetDirty(true);
            //   } else {
            //     parent_guard.SetDirty(false);
            //   }
            //   parent_guard.Drop();
            //   keys_index.pop_back();
            // }

            break;
          }

          left_guard.SetDirty(false);
          left_guard.Drop();
        }

        // 看下叶子右兄弟
        if (parent_index < parent_internal->GetSize() - 1) {
          auto right_id = parent_internal->ValueAt(parent_index + 1);
          auto right_guard = bpm_->FetchPageWrite(right_id);
          auto right_leaf = right_guard.template AsMut<LeafPage>();

          if (right_leaf->GetSize() > right_leaf->GetMinSize()) {
            // printf("借叶子右兄弟!\n");
            leaf->SetAt(leaf->GetSize(), right_leaf->KeyAt(0), right_leaf->ValueAt(0));
            for (int i = 0; i < right_leaf->GetSize() - 1; i++) {
              right_leaf->SetAt(i, right_leaf->KeyAt(i + 1), right_leaf->ValueAt(i + 1));
            }
            leaf->IncreaseSize(1);
            right_leaf->IncreaseSize(-1);

            // 修改parent
            if (parent_index > 0) {
              parent_internal->SetKeyAt(parent_index, leaf->KeyAt(0));
            }
            parent_internal->SetKeyAt(parent_index + 1, right_leaf->KeyAt(0));
            parent_guard.SetDirty(true);
            parent_guard.Drop();

            KeyType leaf_first = leaf->KeyAt(0);

            // 修改了leaf和右兄弟
            leaf_guard.SetDirty(true);
            leaf_guard.Drop();

            right_guard.SetDirty(true);
            right_guard.Drop();

            Remove_Res_Guards_Pop(ctx.write_set_, keys_index, key, leaf_first);
            // while (!guards.empty()) {
            //   parent_guard = std::move(guards.back());
            //   guards.pop_back();
            //   parent_internal = parent_guard.AsMut<InternalPage>();

            //   if (comparator_(parent_internal->KeyAt(keys_index.back()), key) == 0) {
            //     parent_internal->SetKeyAt(keys_index.back(), leaf_first);
            //     parent_guard.SetDirty(true);
            //   } else {
            //     parent_guard.SetDirty(false);
            //   }
            //   parent_guard.Drop();
            //   keys_index.pop_back();
            // }

            break;
          }

          right_guard.SetDirty(false);
          right_guard.Drop();
        }

        // printf("借不了，融合叶子节点！\n");
        KeyType leaf_first{};
        // 否则要融合，可能是递归
        // 先融合叶节点
        if (parent_index > 0) {
          // printf("融合左叶子\n");
          auto left_id = parent_internal->ValueAt(parent_index - 1);
          auto left_guard = bpm_->FetchPageWrite(left_id);
          auto left_leaf = left_guard.template AsMut<LeafPage>();

          for (int i = 0, j = left_leaf->GetSize(); i < leaf->GetSize(); i++, j++) {
            left_leaf->SetAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
            left_leaf->IncreaseSize(1);
          }
          leaf_first = left_leaf->KeyAt(0);
          left_leaf->SetNextPageId(leaf->GetNextPageId());

          for (int i = parent_index; i < parent_internal->GetSize() - 1; i++) {
            parent_internal->SetAt(i, parent_internal->KeyAt(i + 1), parent_internal->ValueAt(i + 1));
          }
          parent_internal->IncreaseSize(-1);
          // if (parent_internal->GetSize() == 1) {
          //   parent_internal->IncreaseSize(-1);
          // }

          left_guard.SetDirty(true);
          left_guard.Drop();

          leaf_guard.SetDirty(false);
          leaf_guard.Drop();

        } else {
          // printf("融合右叶子\n");
          auto right_id = parent_internal->ValueAt(parent_index + 1);
          auto right_guard = bpm_->FetchPageWrite(right_id);
          auto right_leaf = right_guard.template AsMut<LeafPage>();

          for (int i = leaf->GetSize(), j = 0; j < right_leaf->GetSize(); i++, j++) {
            leaf->SetAt(i, right_leaf->KeyAt(j), right_leaf->ValueAt(j));
            leaf->IncreaseSize(1);
          }
          leaf_first = leaf->KeyAt(0);
          leaf->SetNextPageId(right_leaf->GetNextPageId());

          for (int i = parent_index + 1; i < parent_internal->GetSize() - 1; i++) {
            parent_internal->SetAt(i, parent_internal->KeyAt(i + 1), parent_internal->ValueAt(i + 1));
          }

          parent_internal->IncreaseSize(-1);
          // if (parent_internal->GetSize() == 1) {
          //   parent_internal->IncreaseSize(-1);
          // }
          // std::cout << "leaf_size: " << leaf->GetSize() << std::endl;
          // std::cout << "parent_size: " << parent_internal->GetSize() << std::endl;
          right_guard.SetDirty(false);
          right_guard.Drop();

          leaf_guard.SetDirty(true);
          leaf_guard.Drop();
        }

        // printf("递归融合internal节点\n");
        // // 递归处理internal节点（借和融合）
        auto cur_guard = std::move(parent_guard);
        auto cur_internal = parent_internal;
        // printf("当前internal的key为：");
        // std::cout << cur_internal->KeyAt(parent_index) << std::endl;
        // printf("当前internal的size为:");
        // std::cout << cur_internal->GetSize() << " minsize为: " << cur_internal->GetMinSize() << std ::endl;

        while (!ctx.write_set_.empty() && cur_internal->GetSize() - 1 < cur_internal->GetMinSize()) {
          // std::cout << "guards_size: " << guards.size() << std::endl;
          parent_guard = std::move(ctx.write_set_.back());
          parent_internal = parent_guard.AsMut<InternalPage>();
          parent_index = keys_index.back();
          keys_index.pop_back();

          if (comparator_(parent_internal->KeyAt(parent_index), key) == 0) {
            parent_internal->SetKeyAt(parent_index, leaf_first);
          }

          // printf("当前parent的key为：");
          // std::cout << parent_internal->KeyAt(parent_index) << std::endl;
          // std::cout << "parent_index: " << parent_index << std::endl;
          // std::cout << "parent_size: " << parent_internal->GetSize() << std::endl;

          // 先看可不可以借
          if (parent_index > 0) {
            auto left_id = parent_internal->ValueAt(parent_index - 1);
            auto left_guard = bpm_->FetchPageWrite(left_id);
            auto left_internal = left_guard.template AsMut<InternalPage>();

            if (left_internal->GetSize() - 1 > left_internal->GetMinSize()) {
              // printf("借internal左兄弟\n");
              for (int i = cur_internal->GetSize(); i >= 2; i--) {
                cur_internal->SetAt(i, cur_internal->KeyAt(i - 1), cur_internal->ValueAt(i - 1));
              }
              cur_internal->SetAt(1, parent_internal->KeyAt(parent_index), cur_internal->ValueAt(0));
              cur_internal->SetAt(0, KeyType{}, left_internal->ValueAt(left_internal->GetSize() - 1));
              parent_internal->SetKeyAt(parent_index, left_internal->KeyAt(left_internal->GetSize() - 1));
              cur_internal->IncreaseSize(1);
              left_internal->IncreaseSize(-1);

              cur_guard.SetDirty(true);
              cur_guard.Drop();

              left_guard.SetDirty(true);
              left_guard.Drop();

              parent_guard.SetDirty(true);
              parent_guard.Drop();

              ctx.write_set_.pop_back();

              Remove_Res_Guards_Pop(ctx.write_set_, keys_index, key, leaf_first);
              // while (!guards.empty()) {
              //   parent_guard = std::move(guards.back());
              //   guards.pop_back();
              //   parent_internal = parent_guard.AsMut<InternalPage>();

              //   if (comparator_(parent_internal->KeyAt(keys_index.back()), key) == 0) {
              //     parent_internal->SetKeyAt(keys_index.back(), leaf_first);
              //     parent_guard.SetDirty(true);
              //   } else {
              //     parent_guard.SetDirty(false);
              //   }
              //   parent_guard.Drop();
              //   keys_index.pop_back();
              // }

              break;
            }

            left_guard.SetDirty(false);
            left_guard.Drop();
          }

          if (parent_index < parent_internal->GetSize() - 1) {
            auto right_id = parent_internal->ValueAt(parent_index + 1);
            auto right_guard = bpm_->FetchPageWrite(right_id);
            auto right_internal = right_guard.template AsMut<InternalPage>();

            if (right_internal->GetSize() - 1 > right_internal->GetMinSize()) {
              // printf("借internal右兄弟\n");
              cur_internal->SetAt(cur_internal->GetSize(), parent_internal->KeyAt(parent_index + 1),
                                  right_internal->ValueAt(0));
              parent_internal->SetKeyAt(parent_index + 1, right_internal->KeyAt(1));
              right_internal->SetAt(0, KeyType{}, right_internal->ValueAt(1));

              for (int i = 1; i < right_internal->GetSize() - 1; i++) {
                right_internal->SetAt(i, right_internal->KeyAt(i + 1), right_internal->ValueAt(i + 1));
              }

              cur_internal->IncreaseSize(1);
              right_internal->IncreaseSize(-1);

              cur_guard.SetDirty(true);
              cur_guard.Drop();

              right_guard.SetDirty(true);
              right_guard.Drop();

              parent_guard.SetDirty(true);
              parent_guard.Drop();

              ctx.write_set_.pop_back();

              Remove_Res_Guards_Pop(ctx.write_set_, keys_index, key, leaf_first);
              // while (!guards.empty()) {
              //   parent_guard = std::move(guards.back());
              //   guards.pop_back();
              //   parent_internal = parent_guard.AsMut<InternalPage>();

              //   if (comparator_(parent_internal->KeyAt(keys_index.back()), key) == 0) {
              //     parent_internal->SetKeyAt(keys_index.back(), leaf_first);
              //     parent_guard.SetDirty(true);
              //   } else {
              //     parent_guard.SetDirty(false);
              //   }
              //   parent_guard.Drop();
              //   keys_index.pop_back();
              // }

              break;
            }

            right_guard.SetDirty(false);
            right_guard.Drop();
          }

          // printf("internal融合!\n");
          // 否则融合，继续往上递归
          if (parent_index > 0) {
            // printf("和左节点融合\n");
            auto left_id = parent_internal->ValueAt(parent_index - 1);
            auto left_guard = bpm_->FetchPageWrite(left_id);
            auto left_internal = left_guard.template AsMut<InternalPage>();

            left_internal->SetAt(left_internal->GetSize(), parent_internal->KeyAt(parent_index),
                                 cur_internal->ValueAt(0));
            left_internal->IncreaseSize(1);

            for (int i = left_internal->GetSize(), j = 1; j < cur_internal->GetSize(); i++, j++) {
              left_internal->SetAt(i, cur_internal->KeyAt(j), cur_internal->ValueAt(j));
              left_internal->IncreaseSize(1);
            }

            for (int i = parent_index; i < parent_internal->GetSize() - 1; i++) {
              parent_internal->SetAt(i, parent_internal->KeyAt(i + 1), parent_internal->ValueAt(i + 1));
            }
            parent_internal->IncreaseSize(-1);

            cur_guard.SetDirty(false);
            cur_guard.Drop();

            left_guard.SetDirty(true);
            left_guard.Drop();

            cur_guard = std::move(parent_guard);
            cur_internal = parent_internal;

          } else {
            // printf("和右节点融合\n");
            auto right_id = parent_internal->ValueAt(parent_index + 1);
            auto right_guard = bpm_->FetchPageWrite(right_id);
            auto right_internal = right_guard.template AsMut<InternalPage>();
            // std::cout << "右节点的id: " << right_id << std::endl;

            cur_internal->SetAt(cur_internal->GetSize(), parent_internal->KeyAt(parent_index + 1),
                                right_internal->ValueAt(0));
            cur_internal->IncreaseSize(1);

            for (int i = cur_internal->GetSize(), j = 1; j < right_internal->GetSize(); i++, j++) {
              cur_internal->SetAt(i, right_internal->KeyAt(j), right_internal->ValueAt(j));
              cur_internal->IncreaseSize(1);
            }

            for (int i = parent_index + 1; i < parent_internal->GetSize() - 1; i++) {
              parent_internal->SetAt(i, parent_internal->KeyAt(i + 1), parent_internal->ValueAt(i + 1));
            }
            parent_internal->IncreaseSize(-1);

            // std::cout << "融合后节点的size: " << cur_internal->GetSize() << std::endl;
            // std::cout << "融合后parent的size: " << parent_internal->GetSize() << std::endl;

            cur_guard.SetDirty(true);
            cur_guard.Drop();

            right_guard.SetDirty(false);
            right_guard.Drop();

            cur_guard = std::move(parent_guard);
            cur_internal = parent_internal;
          }

          ctx.write_set_.pop_back();
        }

        if (parent_internal->GetSize() == 1) {
          header_page->root_page_id_ = parent_internal->ValueAt(0);
        }

        break;
      }
    }

    auto internal = reinterpret_cast<InternalPage *>(root_page);

    int index = BinaryFind(internal, key);
    keys_index.emplace_back(index);

    root_page_guard = bpm_->FetchPageWrite(internal->ValueAt(index));
    root_page = root_page_guard.AsMut<BPlusTreePage>();

    ctx.write_set_.emplace_back(std::move(root_page_guard));
  }

  header_page_guard.SetDirty(true);
  header_page_guard.Drop();

  // printf("-------------Tree As Below--------------\n");
  // Print(bpm_);
  // printf("-------------Tree As Above---------------\n");
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  Context ctx;

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();

  ctx.read_set_.emplace_back(std::move(root_page_guard));

  page_id_t begin_leaf = -1;
  if (root_page->IsLeafPage()) {
    begin_leaf = header_page->root_page_id_;
    ctx.read_set_.back().SetDirty(false);
    ctx.read_set_.back().Drop();
    ctx.read_set_.pop_back();
  } else {
    while (true) {
      auto internal = reinterpret_cast<const InternalPage *>(root_page);
      root_page_guard = bpm_->FetchPageRead(internal->ValueAt(0));
      root_page = root_page_guard.As<BPlusTreePage>();
      // auto left = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(internal->ValueAt(0))->GetData());
      if (root_page->IsLeafPage()) {
        begin_leaf = root_page_guard.PageId();
        root_page_guard.SetDirty(false);
        root_page_guard.Drop();
        while (!ctx.read_set_.empty()) {
          ctx.read_set_.back().SetDirty(false);
          ctx.read_set_.back().Drop();
          ctx.read_set_.pop_back();
        }
        break;
      }
      ctx.read_set_.emplace_back(std::move(root_page_guard));
    }
  }

  header_page_guard.SetDirty(false);
  header_page_guard.Drop();

  return INDEXITERATOR_TYPE(bpm_, begin_leaf, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  // auto root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(root_page_id)->GetData());

  ctx.read_set_.emplace_back(std::move(root_page_guard));

  page_id_t begin_leaf = -1;
  int index = -1;
  if (root_page->IsLeafPage()) {
    begin_leaf = ctx.read_set_.back().PageId();
    auto leaf = reinterpret_cast<const LeafPage *>(root_page);
    index = BinaryFind(leaf, key);

    if (comparator_(leaf->KeyAt(index), key) != 0) {
      begin_leaf = -1;
      index = -1;
    }

  } else {
    while (true) {
      auto internal = reinterpret_cast<const InternalPage *>(root_page);
      int idx = BinaryFind(internal, key);
      root_page_guard = bpm_->FetchPageRead(internal->ValueAt(idx));
      root_page = root_page_guard.As<BPlusTreePage>();
      // auto next = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(internal->ValueAt(idx))->GetData());
      if (root_page->IsLeafPage()) {
        begin_leaf = root_page_guard.PageId();
        auto leaf = reinterpret_cast<const LeafPage *>(root_page);
        index = BinaryFind(leaf, key);
        if (comparator_(leaf->KeyAt(index), key) != 0) {
          begin_leaf = -1;
          index = -1;
        }

        root_page_guard.SetDirty(false);
        root_page_guard.Drop();

        while (!ctx.read_set_.empty()) {
          ctx.read_set_.back().SetDirty(false);
          ctx.read_set_.back().Drop();
          ctx.read_set_.pop_back();
        }

        break;
      }
    }
  }

  header_page_guard.SetDirty(false);
  header_page_guard.Drop();

  return INDEXITERATOR_TYPE(bpm_, begin_leaf, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, -1, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  page_id_t root_page_id = header_page->root_page_id_;

  header_page_guard.SetDirty(false);
  header_page_guard.Drop();

  return root_page_id;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.emplace_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
