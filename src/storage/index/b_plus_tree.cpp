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
auto BPLUSTREE_TYPE::Binary_Find(LeafPage *leaf_page, const KeyType &key) -> int {
  // std::cout << "binary" << leaf_page << std::endl;
  int l = 0, r = leaf_page->GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page->KeyAt(mid), key) != 1)
      l = mid;
    else
      r = mid - 1;
  }

  if (r >= 0 && comparator_(leaf_page->KeyAt(r), key) == 1) r = -1;

  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Binary_Find(InternalPage *internal_page, const KeyType &key) -> int {
  // if (internal_page == nullptr) return -1;

  // if (internal_page->GetSize() == 0) return 0;

  int l = 1, r = internal_page->GetSize() - 1;
  // std::cout << "Binary: "
  //           << " key: " << key << " " << r << std::endl;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    // std::cout << "mid: " << mid << std::endl;
    if (comparator_(internal_page->KeyAt(mid), key) != 1)
      l = mid;
    else
      r = mid - 1;
  }

  if (r == -1 || comparator_(internal_page->KeyAt(r), key) == 1) r = 0;

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
  (void)ctx;

  // printf("---------------------------------\n");
  // std::cout << "GetValue: " << key << std::endl;

  // printf("Tree as Below-----------------------------------\n");
  // Print(bpm_);
  // printf("Tree as Above-----------------------------------\n");

  if (header_page_id_ == INVALID_PAGE_ID) return false;

  auto *header_page = reinterpret_cast<BPlusTreeHeaderPage *>(bpm_->FetchPage(header_page_id_)->GetData());

  if (header_page->root_page_id_ == INVALID_PAGE_ID) return false;

  auto *root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(header_page->root_page_id_)->GetData());

  std::deque<page_id_t> ids;

  ids.push_back(header_page->root_page_id_);

  while (true) {
    // std::cout << "enter" << std::endl;
    if (root_page->IsLeafPage()) {
      // std::cout << "leaf process" << std::endl;
      auto *leaf = reinterpret_cast<LeafPage *>(root_page);

      int index = Binary_Find(leaf, key);

      if (index < 0 || comparator_(leaf->KeyAt(index), key) != 0) {
        while (!ids.empty()) {
          bpm_->UnpinPage(ids.back(), false);
          ids.pop_back();
        }
        // std::cout << "Can not Find" << std::endl;
        // std::cout << "size: " << leaf->GetSize() << "index: " << index << " not find: " << key << std::endl;
        return false;
      }

      // std::cout << "size: " << leaf->GetSize() << "index: " << index << " find: " << key << std::endl;

      result->push_back(leaf->ValueAt(index));
      // std::cout << "increase" << std::endl;

      break;
    }

    // std::cout << "internal process" << std::endl;
    auto *internal = reinterpret_cast<InternalPage *>(root_page);

    // std::cout << "start internal binary find" << std::endl;
    int index = Binary_Find(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    ids.push_back(child_id);

    // std::cout << "child_id: " << internal->ValueAt(index) << std::endl;
    root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(child_id)->GetData());
  }

  while (!ids.empty()) {
    bpm_->UnpinPage(ids.back(), false);
    ids.pop_back();
  }

  bpm_->UnpinPage(header_page_id_, false);

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
auto BPLUSTREE_TYPE::Split_Leaf(LeafPage *leaf, const KeyType &key, const ValueType &value, page_id_t *new_id)
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
  auto *new_leaf = reinterpret_cast<LeafPage *>(bpm_->NewPage(new_id)->GetData());
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
  if (!put_left) put_in_leaf = new_leaf;

  int idx = Binary_Find(put_in_leaf, key);

  for (int i = put_in_leaf->GetSize(); i > idx + 1; i--) {
    put_in_leaf->SetAt(i, put_in_leaf->KeyAt(i - 1), put_in_leaf->ValueAt(i - 1));
  }
  put_in_leaf->SetAt(idx + 1, key, value);
  put_in_leaf->IncreaseSize(1);

  // if (put_left) {
  //   int idx = Binary_Find(leaf, key);

  //   for (int i = leaf->GetSize(); i > idx + 1; i--) {
  //     leaf->SetAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
  //   }
  //   leaf->SetAt(idx + 1, key, value);
  //   leaf->IncreaseSize(1);
  // } else {
  //   int idx = Binary_Find(new_leaf, key);

  //   for (int i = new_leaf->GetSize(); i > idx + 1; i--) {
  //     new_leaf->SetAt(i, new_leaf->KeyAt(i - 1), new_leaf->ValueAt(i - 1));
  //   }
  //   new_leaf->SetAt(idx + 1, key, value);
  //   new_leaf->IncreaseSize(1);
  // }

  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(*new_id);

  // std::cout << "right first key is: " << new_leaf->KeyAt(0) << std::endl;
  return new_leaf->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split_Internal(InternalPage *internal, const KeyType &key, page_id_t *new_id,
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

  auto *new_internal = reinterpret_cast<InternalPage *>(bpm_->NewPage(new_id)->GetData());
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
    int idx = Binary_Find(put_in_internal, key);

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
    //   int idx = Binary_Find(internal, key);

    //   for (int i = internal->GetSize(); i > idx + 1; i--) {
    //     internal->SetAt(i, internal->KeyAt(i - 1), internal->ValueAt(i - 1));
    //   }
    //   internal->SetAt(idx + 1, key, new_child_id);

    //   internal->IncreaseSize(1);

    // } else {
    //   int idx = Binary_Find(new_internal, key);

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

  return up_key;
};

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  // printf("-----------------------------------------\n");

  // std::cout << "insert: " << key << std::endl;
  auto *header_page = reinterpret_cast<BPlusTreeHeaderPage *>(bpm_->FetchPage(header_page_id_)->GetData());

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    auto root_page = reinterpret_cast<LeafPage *>(bpm_->NewPage(&header_page->root_page_id_)->GetData());
    root_page->Init(leaf_max_size_);
  }

  auto *root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(header_page->root_page_id_)->GetData());

  std::deque<BPlusTreePage *> path;
  std::deque<page_id_t> ids;

  // std::cout << header_page->root_page_id_ << std::endl;

  ids.push_back(header_page->root_page_id_);

  while (true) {
    // std::cout << "enter" << std::endl;
    // std::cout << "是否为叶节点: " << root_page->IsLeafPage() << std::endl;
    if (root_page->IsLeafPage()) {
      // std::cout << "leaf process" << std::endl;
      auto *leaf = reinterpret_cast<LeafPage *>(root_page);

      int index = Binary_Find(leaf, key);

      // std::cout << "key: " << key << " index: " << index << std::endl;

      if (index >= 0 && comparator_(leaf->KeyAt(index), key) == 0) return false;

      if (leaf->GetSize() == leaf->GetMaxSize()) {
        // printf("开始分裂\n");
        // //-------------------处理叶子节点的分裂------------------
        // printf("叶子分裂\n");
        page_id_t new_id;
        KeyType up_key = Split_Leaf(leaf, key, value, &new_id);
        // auto *new_leaf = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(new_id)->GetData());
        // std::cout << new_leaf << std::endl;
        // std::cout << "new_leaf is leaf: " << new_leaf->IsLeafPage() << std::endl;
        page_id_t new_child_id = new_id;
        // printf("叶子分裂的up_key为： ");
        // std::cout << up_key << std::endl;
        // printf("new_leaf_id： ");
        // std::cout << new_id << std::endl;

        bpm_->UnpinPage(ids.back(), true);
        ids.pop_back();

        bpm_->UnpinPage(new_id, true);

        //-------------------处理完叶子节点的分裂------------------

        bool split_fin = false;
        while (!path.empty()) {
          auto parent = path.back();
          path.pop_back();

          auto internal_parent = reinterpret_cast<InternalPage *>(parent);

          if (parent->GetSize() < parent->GetMaxSize()) {
            // printf("internal分裂结束!\n");
            // std::cout << "final_up_key: " << up_key << std::endl;
            int idx = Binary_Find(internal_parent, up_key);
            // std::cout << "最后分裂的idx: " << idx << std::endl;

            for (int i = internal_parent->GetSize(); i > idx + 1; i--) {
              internal_parent->SetAt(i, internal_parent->KeyAt(i - 1), internal_parent->ValueAt(i - 1));
            }
            internal_parent->SetAt(idx + 1, up_key, new_child_id);
            internal_parent->IncreaseSize(1);
            split_fin = true;

            bpm_->UnpinPage(ids.back(), true);
            ids.pop_back();

            while (!ids.empty()) {
              bpm_->UnpinPage(ids.back(), false);
              ids.pop_back();
            }

            break;
          } else {
            // printf("internal分裂!\n");
            up_key = Split_Internal(internal_parent, up_key, &new_id, new_child_id);
            // printf("internal分裂的up_key为： ");
            // std::cout << up_key << std::endl;

            bpm_->UnpinPage(ids.back(), true);
            ids.pop_back();
            bpm_->UnpinPage(new_id, true);

            new_child_id = new_id;
          }
        }

        if (!split_fin) {
          // printf("根节点分裂!\n");
          page_id_t old_id = header_page->root_page_id_;
          auto new_root_page = reinterpret_cast<InternalPage *>(bpm_->NewPage(&header_page->root_page_id_)->GetData());
          // std::cout << "根的old id为：" << old_id << std::endl;
          // std::cout << "根的new id为：" << header_page->root_page_id_ << std::endl;
          new_root_page->Init(internal_max_size_);
          // std::cout << "new_id:" << new_id << " old_id:" << old_id << std::endl;
          new_root_page->SetAt(1, up_key, new_id);
          new_root_page->SetAt(0, KeyType{}, old_id);
          new_root_page->SetSize(2);
          bpm_->UnpinPage(header_page->root_page_id_, true);
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

      bpm_->UnpinPage(ids.back(), true);
      ids.pop_back();

      while (!ids.empty()) {
        bpm_->UnpinPage(ids.back(), false);
        ids.pop_back();
      }

      // std::cout << "insert index: " << index << " key: " << key << " Size: " << leaf->GetSize() << std::endl;

      break;
    }

    path.push_back(root_page);

    // std::cout << "internal process" << std::endl;
    auto *internal = reinterpret_cast<InternalPage *>(root_page);

    // std::cout << "start internal binary find" << std::endl;
    int index = Binary_Find(internal, key);

    // std::cout << "find_index: " << index << std::endl;

    page_id_t child_id = internal->ValueAt(index);

    // std::cout << "child_id: " << child_id << std::endl;

    ids.push_back(child_id);

    root_page = reinterpret_cast<BPlusTreePage *>(bpm_->FetchPage(child_id)->GetData());
  }

  bpm_->UnpinPage(header_page_id_, true);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto *header_page = reinterpret_cast<BPlusTreeHeaderPage *>(bpm_->FetchPage(header_page_id_)->GetData());

  return header_page->root_page_id_;
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
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
