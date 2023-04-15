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

  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return false;
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ctx.root_page_id_ = root_page_guard.PageId();

  ctx.read_set_.emplace_back(std::move(root_page_guard));

  // crabbing，拿到根节点的读锁，把header的读锁放掉
  header_page_guard.SetDirty(false);
  header_page_guard.Drop();

  while (true) {
    if (root_page->IsLeafPage()) {
      auto *leaf = reinterpret_cast<const LeafPage *>(root_page);

      int index = BinaryFind(leaf, key);

      if (index < 0 || comparator_(leaf->KeyAt(index), key) != 0) {
        while (!ctx.read_set_.empty()) {
          ctx.read_set_.back().SetDirty(false);
          ctx.read_set_.back().Drop();
          ctx.read_set_.pop_back();
        }

        return false;
      }

      result->emplace_back(leaf->ValueAt(index));

      break;
    }

    auto *internal = reinterpret_cast<const InternalPage *>(root_page);

    int index = BinaryFind(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    root_page_guard = bpm_->FetchPageRead(child_id);
    root_page = root_page_guard.As<BPlusTreePage>();

    // crabbing，拿到当前读锁，将祖先节点全部放掉（其实只有父节点）
    while (!ctx.read_set_.empty()) {
      ctx.read_set_.back().SetDirty(false);
      ctx.read_set_.back().Drop();
      ctx.read_set_.pop_back();
    }

    ctx.read_set_.emplace_back(std::move(root_page_guard));
  }

  while (!ctx.read_set_.empty()) {
    ctx.read_set_.back().SetDirty(false);
    ctx.read_set_.back().Drop();
    ctx.read_set_.pop_back();
  }

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
  // 处理叶子分裂，new_id以指针的方式传回新page的id（右边的page），同时该函数返回上传的key

  bool put_left = false;  // key放在原来的节点，还是分裂出来的节点

  int mid = leaf->GetMinSize();  // 要将mid~最后的内容移动到新分裂的节点
  KeyType mid_key = leaf->KeyAt(mid);

  // 根据数量的奇偶性调整mid和put_left，保证右边数量比左边多,up_key为右边的第一个key
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

  // 生成新页
  auto new_leaf_basic_guard = bpm_->NewPageGuarded(new_id);
  auto new_leaf = new_leaf_basic_guard.AsMut<LeafPage>();
  new_leaf_basic_guard.SetDirty(true);
  new_leaf_basic_guard.Drop();

  auto new_leaf_guard = bpm_->FetchPageWrite(*new_id);
  new_leaf = new_leaf_guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);

  // 移动mid~最后的内容到新叶
  int leaf_size = leaf->GetSize();
  for (int i = mid, j = 0; i < leaf_size; i++, j++) {
    // std::cout << "move" << std::endl;
    new_leaf->SetAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
    new_leaf->IncreaseSize(1);
    leaf->IncreaseSize(-1);
  }

  auto put_in_leaf = leaf;
  if (!put_left) {
    put_in_leaf = new_leaf;
  }

  // 找到key在对应的叶子的位置，插进去
  int idx = BinaryFind(put_in_leaf, key);

  for (int i = put_in_leaf->GetSize(); i > idx + 1; i--) {
    put_in_leaf->SetAt(i, put_in_leaf->KeyAt(i - 1), put_in_leaf->ValueAt(i - 1));
  }
  put_in_leaf->SetAt(idx + 1, key, value);
  put_in_leaf->IncreaseSize(1);

  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(*new_id);

  KeyType up_key = new_leaf->KeyAt(0);

  new_leaf_guard.SetDirty(true);
  new_leaf_guard.Drop();

  return up_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal, const KeyType &key, page_id_t *new_id,
                                   page_id_t new_child_id) -> KeyType {
  // 处理internal分裂，new_id以指针的方式传回新page的id（右边的page），同时该函数返回上传的key，这里的key参数为
  // 下面上传过来的key,new_child_id为下层新分裂出的节点的页面id，是key的右孩子（key是new_child_id对应页面的第一个上传过来的）

  bool put_left = false;
  int mid = internal->GetMinSize();
  KeyType mid_key = internal->KeyAt(mid);

  KeyType up_key{};
  page_id_t up_key_id = -1;  // up_key对应的孩子

  // 调整mid和up_key,保证右边数量比左边多,up_key为右边的第一个key
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

  // 将up_key删掉,如果up_key不是下面传过来的key的话（是的话就不用删，直接分成两半即可，然后把下面传上来的key再上传）
  if (comparator_(up_key, key) != 0) {
    for (int i = mid; i < internal->GetSize() - 1; i++) {
      internal->SetAt(i, internal->KeyAt(i + 1), internal->ValueAt(i + 1));
    }
    internal->IncreaseSize(-1);
  }

  // 生成新叶
  auto new_internal_basic_guard = bpm_->NewPageGuarded(new_id);
  auto new_internal = new_internal_basic_guard.AsMut<InternalPage>();
  // auto *new_internal = reinterpret_cast<InternalPage *>(bpm_->NewPage(new_id)->GetData());
  new_internal_basic_guard.SetDirty(true);
  new_internal_basic_guard.Drop();

  auto new_internal_guard = bpm_->FetchPageWrite(*new_id);
  new_internal = new_internal_guard.AsMut<InternalPage>();
  new_internal->Init(internal_max_size_);

  // 将mid~最后的内容移动到新叶
  int internal_size = internal->GetSize();
  for (int i = mid, j = 1; i < internal_size; i++, j++) {
    new_internal->SetAt(j, internal->KeyAt(i), internal->ValueAt(i));
    new_internal->IncreaseSize(1);
    internal->IncreaseSize(-1);
  }
  // 注意size是孩子数，比key多1
  new_internal->IncreaseSize(1);

  // 如果up_key不是上传过来的key
  if (comparator_(up_key, key) != 0) {
    // 先把key放到对应的internal的对应位置
    auto put_in_internal = internal;
    if (!put_left) {
      put_in_internal = new_internal;
    }
    int idx = BinaryFind(put_in_internal, key);
    for (int i = put_in_internal->GetSize(); i > idx + 1; i--) {
      put_in_internal->SetAt(i, put_in_internal->KeyAt(i - 1), put_in_internal->ValueAt(i - 1));
    }
    // 然后把key的右孩子接上，是下层分裂出的右节点
    put_in_internal->SetAt(idx + 1, key, new_child_id);

    // 如果放的是右边的话，并且放完之后右边只有这个key，那么此时孩子数为2
    if (!put_left && put_in_internal->GetSize() == 0) {
      put_in_internal->SetSize(2);
    } else {
      // 注意size是孩子数，比key多1
      put_in_internal->IncreaseSize(1);
    }

    // 此时右边新叶子的第一个孩子为，上传的key的右孩子（因为上传的key原本应该位于右边新叶子的第一个）
    new_internal->SetAt(0, KeyType{}, up_key_id);

  } else {
    // 如果上传的key是下面传过来的key的话，那么直接分成两半后,右边节点的第一个孩子应该为下层新分裂出的孩子
    // 因为key本身应该位于右叶子的第一个位置，而new_child_id是的它的右孩子
    new_internal->SetAt(0, KeyType{}, new_child_id);
  }

  new_internal_guard.SetDirty(true);
  new_internal_guard.Drop();

  return up_key;
};

// 乐观insert:一路crabbing读锁到叶节点，拿写锁，安全直接insert返回
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimalInsert(const KeyType &key, const ValueType &value, Transaction *txn) -> int {
  // 0表示要用悲观insert一次，1表示乐观insert成功，2表示重复key

  Context ctx;

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  // 若没有根节点，返回0，此时悲观insert会创建根节点
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return 0;
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ctx.root_page_id_ = root_page_guard.PageId();

  ctx.read_set_.emplace_back(std::move(header_page_guard));

  while (true) {
    if (root_page->IsLeafPage()) {
      page_id_t leaf_id = root_page_guard.PageId();

      // 叶子节点放读锁
      root_page_guard.SetDirty(false);
      root_page_guard.Drop();

      // 叶子节点拿写锁
      auto leaf_guard = bpm_->FetchPageWrite(leaf_id);

      // 叶子节点父节点放读锁
      while (!ctx.read_set_.empty()) {
        ctx.read_set_.back().SetDirty(false);
        ctx.read_set_.back().Drop();
        ctx.read_set_.pop_back();
      }

      auto *leaf = leaf_guard.AsMut<LeafPage>();

      int index = BinaryFind(leaf, key);

      // 找重复插入，返回2
      if (index >= 0 && comparator_(leaf->KeyAt(index), key) == 0) {
        leaf_guard.SetDirty(false);
        leaf_guard.Drop();
        return 2;
      }

      // 不安全返回0
      if (leaf->GetSize() == leaf->GetMaxSize()) {
        leaf_guard.SetDirty(false);
        leaf_guard.Drop();
        return 0;
      }

      // 乐观插入成功，返回1
      for (int i = leaf->GetSize(); i > index + 1; i--) {
        leaf->SetAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
      }
      leaf->SetAt(index + 1, key, value);
      leaf->IncreaseSize(1);

      leaf_guard.SetDirty(true);
      leaf_guard.Drop();

      break;
    }

    while (!ctx.read_set_.empty()) {
      ctx.read_set_.back().SetDirty(false);
      ctx.read_set_.back().Drop();
      ctx.read_set_.pop_back();
    }

    auto *internal = reinterpret_cast<const InternalPage *>(root_page);

    int index = BinaryFind(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    ctx.read_set_.emplace_back(std::move(root_page_guard));

    root_page_guard = bpm_->FetchPageRead(child_id);
    root_page = root_page_guard.As<BPlusTreePage>();
  }

  return 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;

  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  int res = OptimalInsert(key, value, txn);

  // 成功
  if (res == 1) {
    return true;
  }
  // 重复
  if (res == 2) {
    return false;
  }

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

  bool header_drop = false;

  // crabbing，如过root安全（不满不会分裂），把header放掉
  if (root_page->GetSize() < root_page->GetMaxSize()) {
    ctx.header_page_guard_->SetDirty(false);
    ctx.header_page_guard_->Drop();
    header_drop = true;
  }

  ctx.write_set_.emplace_back(std::move(root_page_guard));

  while (true) {
    if (root_page->IsLeafPage()) {
      auto *leaf = reinterpret_cast<LeafPage *>(root_page);

      int index = BinaryFind(leaf, key);

      if (index >= 0 && comparator_(leaf->KeyAt(index), key) == 0) {
        return false;
      }

      if (leaf->GetSize() == leaf->GetMaxSize()) {
        // 处理叶子节点的分裂
        page_id_t new_id;
        KeyType up_key = SplitLeaf(leaf, key, value, &new_id);
        page_id_t new_child_id = new_id;

        ctx.write_set_.back().SetDirty(true);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();

        // 处理internal的分裂
        bool split_fin = false;
        while (!ctx.write_set_.empty()) {
          auto internal_parent_guard = std::move(ctx.write_set_.back());
          ctx.write_set_.pop_back();

          auto internal_parent = internal_parent_guard.AsMut<InternalPage>();

          if (internal_parent->GetSize() < internal_parent->GetMaxSize()) {
            // 此时分裂结束，把up_key插好，它对应的孩子填好即可

            int idx = BinaryFind(internal_parent, up_key);

            for (int i = internal_parent->GetSize(); i > idx + 1; i--) {
              internal_parent->SetAt(i, internal_parent->KeyAt(i - 1), internal_parent->ValueAt(i - 1));
            }
            internal_parent->SetAt(idx + 1, up_key, new_child_id);
            internal_parent->IncreaseSize(1);
            split_fin = true;

            internal_parent_guard.SetDirty(true);
            internal_parent_guard.Drop();

            while (!ctx.write_set_.empty()) {
              ctx.write_set_.back().SetDirty(false);
              ctx.write_set_.back().Drop();
              ctx.write_set_.pop_back();
            }

            break;
          }

          // internal分裂
          up_key = SplitInternal(internal_parent, up_key, &new_id, new_child_id);

          internal_parent_guard.SetDirty(true);
          internal_parent_guard.Drop();

          new_child_id = new_id;
        }

        // 如果上面是由于write_set空掉而退出循环的话，说明根节点分类了，此时要新生成一个根节点
        if (!split_fin) {
          page_id_t old_id = header_page->root_page_id_;

          // 新生成根节点
          auto new_root_page_basic_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);
          auto new_root_page = new_root_page_basic_guard.AsMut<InternalPage>();
          ctx.root_page_id_ = new_root_page_basic_guard.PageId();
          new_root_page_basic_guard.SetDirty(true);
          new_root_page_basic_guard.Drop();

          auto new_root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
          new_root_page = new_root_page_guard.AsMut<InternalPage>();
          new_root_page->Init(internal_max_size_);

          // 新的根节点的key和孩子分配一下
          new_root_page->SetAt(1, up_key, new_id);
          new_root_page->SetAt(0, KeyType{}, old_id);
          new_root_page->SetSize(2);
          new_root_page_guard.SetDirty(true);
          new_root_page_guard.Drop();
        }

        break;
      }

      // 如果正常叶节点不需要分类，直接插完返回即可
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

      break;
    }

    auto *internal = reinterpret_cast<InternalPage *>(root_page);

    int index = BinaryFind(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    root_page_guard = bpm_->FetchPageWrite(child_id);
    root_page = root_page_guard.AsMut<BPlusTreePage>();

    // crabbing，节点安全
    if (root_page->GetSize() < root_page->GetMaxSize()) {
      while (!ctx.write_set_.empty()) {
        ctx.write_set_.back().SetDirty(false);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();
      }
    }

    ctx.write_set_.emplace_back(std::move(root_page_guard));
  }

  if (!header_drop) {
    ctx.header_page_guard_->SetDirty(true);
    ctx.header_page_guard_->Drop();
  }

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
void BPLUSTREE_TYPE::RemoveResGuardsPop(std::deque<WritePageGuard> &guards, std::deque<int> &keys_index,
                                        const KeyType &origin_key, const KeyType &new_key) {
  while (!guards.empty()) {
    auto parent_guard = std::move(guards.back());
    guards.pop_back();
    auto parent_internal = parent_guard.AsMut<InternalPage>();

    // 有可能internal里面有删掉的key,要改掉，此时只有一种情况,删掉的是叶节点的第一个key，此时要将起直接父节点的key
    // 改成次小值，该if只会执行一次
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

// 乐观remove：一路crabbing读锁，直到叶节点拿写锁，若安全，则直接操作返回true
// （key的数量大于一半，且删的不是第一个key，如果要删第一个key的话，父节点的key要变）
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimalRemove(const KeyType &key, Transaction *txn) -> bool {
  Context ctx;

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  // 没有根节点不用删，返回true
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return true;
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ctx.root_page_id_ = root_page_guard.PageId();

  ctx.read_set_.emplace_back(std::move(header_page_guard));

  while (true) {
    if (root_page->IsLeafPage()) {
      page_id_t leaf_id = root_page_guard.PageId();

      // 如果是叶节点，先把读锁放掉
      root_page_guard.SetDirty(false);
      root_page_guard.Drop();

      // 到了叶节点先拿写锁
      auto leaf_guard = bpm_->FetchPageWrite(leaf_id);

      // 再把父节点的读锁放掉
      while (!ctx.read_set_.empty()) {
        ctx.read_set_.back().SetDirty(false);
        ctx.read_set_.back().Drop();
        ctx.read_set_.pop_back();
      }

      auto *leaf = leaf_guard.AsMut<LeafPage>();

      int index = BinaryFind(leaf, key);

      // 找不到删除的key，直接返回true
      if (index < 0 || comparator_(leaf->KeyAt(index), key) != 0) {
        leaf_guard.SetDirty(false);
        leaf_guard.Drop();
        break;
      }

      // 不安全返回false(删的是第一个key也不安全)
      if (leaf->GetSize() <= leaf->GetMinSize() || index == 0) {
        leaf_guard.SetDirty(false);
        leaf_guard.Drop();
        return false;
      }

      // 安全删完直接返回true
      for (int i = index; i < leaf->GetSize() - 1; i++) {
        leaf->SetAt(i, leaf->KeyAt(i + 1), leaf->ValueAt(i + 1));
      }
      leaf->IncreaseSize(-1);

      leaf_guard.SetDirty(true);
      leaf_guard.Drop();

      break;
    }

    // 把父节点读锁放掉
    while (!ctx.read_set_.empty()) {
      ctx.read_set_.back().SetDirty(false);
      ctx.read_set_.back().Drop();
      ctx.read_set_.pop_back();
    }

    auto *internal = reinterpret_cast<const InternalPage *>(root_page);

    int index = BinaryFind(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    // 将当前锁加入队列
    ctx.read_set_.emplace_back(std::move(root_page_guard));

    // 拿子节点的读锁
    root_page_guard = bpm_->FetchPageRead(child_id);
    root_page = root_page_guard.As<BPlusTreePage>();
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return;
  }

  if (OptimalRemove(key, txn)) {
    return;
  }

  Context ctx;

  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return;
  }

  // 记录沿路的key
  std::deque<int> keys_index;

  auto root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto root_page = root_page_guard.AsMut<BPlusTreePage>();

  bool header_drop = false;
  // crabbing，如果根节点安全，把header的锁放掉
  if ((root_page->IsLeafPage() && root_page->GetSize() > root_page->GetMinSize()) ||
      (!root_page->IsLeafPage() && root_page->GetSize() > root_page->GetMinSize() + 1)) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    header_drop = true;
  }

  ctx.write_set_.emplace_back(std::move(root_page_guard));

  while (true) {
    if (root_page->IsLeafPage()) {
      auto leaf = reinterpret_cast<LeafPage *>(root_page);
      int index = BinaryFind(leaf, key);

      // 找不到删除的key，直接返回
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

      // 如果当前只有一个节点，直接返回
      if (ctx.write_set_.size() == 1) {
        ctx.write_set_.back().SetDirty(true);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();
        break;
      }

      // 如果删完之后满足要求直接返回
      if (leaf->GetSize() >= leaf->GetMinSize()) {
        // 叶子弹掉
        KeyType leaf_first = leaf->KeyAt(0);
        ctx.write_set_.back().SetDirty(true);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();

        // 把沿路的guard放掉
        RemoveResGuardsPop(ctx.write_set_, keys_index, key, leaf_first);

        break;
      }

      // 处理叶节点的借与合并

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

      //  看下是否有叶子左兄弟
      if (parent_index > 0) {
        auto left_id = parent_internal->ValueAt(parent_index - 1);
        auto left_guard = bpm_->FetchPageWrite(left_id);
        auto left_leaf = left_guard.template AsMut<LeafPage>();

        // 把左兄弟最后一个借走，放到leaf的第一个位置
        if (left_leaf->GetSize() > left_leaf->GetMinSize()) {
          for (int i = leaf->GetSize(); i >= 1; i--) {
            leaf->SetAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
          }
          leaf->SetAt(0, left_leaf->KeyAt(left_leaf->GetSize() - 1), left_leaf->ValueAt(left_leaf->GetSize() - 1));
          leaf->IncreaseSize(1);
          left_leaf->IncreaseSize(-1);

          // 然后修改parent的关键字
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
          RemoveResGuardsPop(ctx.write_set_, keys_index, key, leaf_first);

          break;
        }

        // 借不了的话记得把左兄弟的guard drop掉
        left_guard.SetDirty(false);
        left_guard.Drop();
      }

      // 看下是否有叶子右兄弟
      if (parent_index < parent_internal->GetSize() - 1) {
        auto right_id = parent_internal->ValueAt(parent_index + 1);
        auto right_guard = bpm_->FetchPageWrite(right_id);
        auto right_leaf = right_guard.template AsMut<LeafPage>();

        // 看是否可借
        if (right_leaf->GetSize() > right_leaf->GetMinSize()) {
          // 把右兄弟的第一个借走放到最后一个位置
          leaf->SetAt(leaf->GetSize(), right_leaf->KeyAt(0), right_leaf->ValueAt(0));
          for (int i = 0; i < right_leaf->GetSize() - 1; i++) {
            right_leaf->SetAt(i, right_leaf->KeyAt(i + 1), right_leaf->ValueAt(i + 1));
          }
          leaf->IncreaseSize(1);
          right_leaf->IncreaseSize(-1);

          // 修改parent关键字
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

          RemoveResGuardsPop(ctx.write_set_, keys_index, key, leaf_first);

          break;
        }

        right_guard.SetDirty(false);
        right_guard.Drop();
      }

      // 借不了，融合叶子节点，有左兄弟优先和左兄弟融合，统一为将右边节点的内容放到左边节点

      KeyType leaf_first{};

      // 如果有叶子左兄弟
      if (parent_index > 0) {
        // printf("融合左叶子\n");
        auto left_id = parent_internal->ValueAt(parent_index - 1);
        auto left_guard = bpm_->FetchPageWrite(left_id);
        auto left_leaf = left_guard.template AsMut<LeafPage>();

        // 将该节点的内容接到左兄弟后面
        for (int i = 0, j = left_leaf->GetSize(); i < leaf->GetSize(); i++, j++) {
          left_leaf->SetAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
          left_leaf->IncreaseSize(1);
        }
        leaf_first = left_leaf->KeyAt(0);
        left_leaf->SetNextPageId(leaf->GetNextPageId());

        // 把当前parent_index删掉
        for (int i = parent_index; i < parent_internal->GetSize() - 1; i++) {
          parent_internal->SetAt(i, parent_internal->KeyAt(i + 1), parent_internal->ValueAt(i + 1));
        }
        parent_internal->IncreaseSize(-1);

        left_guard.SetDirty(true);
        left_guard.Drop();

        leaf_guard.SetDirty(false);
        leaf_guard.Drop();

      } else {
        // 否则融合右叶子
        auto right_id = parent_internal->ValueAt(parent_index + 1);
        auto right_guard = bpm_->FetchPageWrite(right_id);
        auto right_leaf = right_guard.template AsMut<LeafPage>();

        // 将右叶子的内容加到当前叶子的后面
        for (int i = leaf->GetSize(), j = 0; j < right_leaf->GetSize(); i++, j++) {
          leaf->SetAt(i, right_leaf->KeyAt(j), right_leaf->ValueAt(j));
          leaf->IncreaseSize(1);
        }
        leaf_first = leaf->KeyAt(0);
        leaf->SetNextPageId(right_leaf->GetNextPageId());

        // 将右叶子对应的parent_index+1的key删掉
        for (int i = parent_index + 1; i < parent_internal->GetSize() - 1; i++) {
          parent_internal->SetAt(i, parent_internal->KeyAt(i + 1), parent_internal->ValueAt(i + 1));
        }

        parent_internal->IncreaseSize(-1);

        right_guard.SetDirty(false);
        right_guard.Drop();

        leaf_guard.SetDirty(true);
        leaf_guard.Drop();
      }

      // 递归处理internal节点（借和融合）
      auto cur_guard = std::move(parent_guard);
      auto cur_internal = parent_internal;

      // ps:这里写的时候，叶子的min取下取整，internal的min取上取整
      while (!ctx.write_set_.empty() && cur_internal->GetSize() - 1 < cur_internal->GetMinSize()) {
        // std::cout << "guards_size: " << guards.size() << std::endl;
        parent_guard = std::move(ctx.write_set_.back());
        parent_internal = parent_guard.AsMut<InternalPage>();
        parent_index = keys_index.back();
        keys_index.pop_back();

        // 这句不加也行，因为上层的internal不可能存在要删的key
        if (comparator_(parent_internal->KeyAt(parent_index), key) == 0) {
          parent_internal->SetKeyAt(parent_index, leaf_first);
        }

        // 是否有internal左兄弟
        if (parent_index > 0) {
          auto left_id = parent_internal->ValueAt(parent_index - 1);
          auto left_guard = bpm_->FetchPageWrite(left_id);
          auto left_internal = left_guard.template AsMut<InternalPage>();

          if (left_internal->GetSize() - 1 > left_internal->GetMinSize()) {
            // 右旋操作，借internal的左兄弟

            // 把parent的key拉下来放第一个位置
            for (int i = cur_internal->GetSize(); i >= 2; i--) {
              cur_internal->SetAt(i, cur_internal->KeyAt(i - 1), cur_internal->ValueAt(i - 1));
            }
            cur_internal->SetAt(1, parent_internal->KeyAt(parent_index), cur_internal->ValueAt(0));

            // 第一个位置的左孩子应该是左兄弟的最后一个孩子
            cur_internal->SetAt(0, KeyType{}, left_internal->ValueAt(left_internal->GetSize() - 1));

            // parent的key换成左兄弟的最后一个key
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

            RemoveResGuardsPop(ctx.write_set_, keys_index, key, leaf_first);

            break;
          }

          left_guard.SetDirty(false);
          left_guard.Drop();
        }

        // 是否有internal右兄弟
        if (parent_index < parent_internal->GetSize() - 1) {
          auto right_id = parent_internal->ValueAt(parent_index + 1);
          auto right_guard = bpm_->FetchPageWrite(right_id);
          auto right_internal = right_guard.template AsMut<InternalPage>();

          if (right_internal->GetSize() - 1 > right_internal->GetMinSize()) {
            // 左旋操作，借internal的右兄弟

            // 把parent_index+1的key拉下来放最后一个位置，其右孩子应该为右兄弟的第一个孩子
            cur_internal->SetAt(cur_internal->GetSize(), parent_internal->KeyAt(parent_index + 1),
                                right_internal->ValueAt(0));

            // 把右兄弟的第一个key拉到parent_index+1
            parent_internal->SetKeyAt(parent_index + 1, right_internal->KeyAt(1));

            // 右兄弟的第一个孩子应该变为，原来的第一个key的右孩子
            right_internal->SetAt(0, KeyType{}, right_internal->ValueAt(1));

            // 把右兄弟的第一个key删掉
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

            RemoveResGuardsPop(ctx.write_set_, keys_index, key, leaf_first);

            break;
          }

          right_guard.SetDirty(false);
          right_guard.Drop();
        }

        // internal融合操作

        // 和左节点融合
        if (parent_index > 0) {
          auto left_id = parent_internal->ValueAt(parent_index - 1);
          auto left_guard = bpm_->FetchPageWrite(left_id);
          auto left_internal = left_guard.template AsMut<InternalPage>();

          // 先把key接到左节点后面
          left_internal->SetAt(left_internal->GetSize(), parent_internal->KeyAt(parent_index),
                               cur_internal->ValueAt(0));
          left_internal->IncreaseSize(1);

          // 再把当前internal的内容放到左兄弟后面
          for (int i = left_internal->GetSize(), j = 1; j < cur_internal->GetSize(); i++, j++) {
            left_internal->SetAt(i, cur_internal->KeyAt(j), cur_internal->ValueAt(j));
            left_internal->IncreaseSize(1);
          }

          // 把parent_index删掉
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
          // 和右节点融合
          auto right_id = parent_internal->ValueAt(parent_index + 1);
          auto right_guard = bpm_->FetchPageWrite(right_id);
          auto right_internal = right_guard.template AsMut<InternalPage>();

          // 把index+1的parent_key拉到当前节点后面
          cur_internal->SetAt(cur_internal->GetSize(), parent_internal->KeyAt(parent_index + 1),
                              right_internal->ValueAt(0));
          cur_internal->IncreaseSize(1);

          // 再把右节点的内容拉到当前节点
          for (int i = cur_internal->GetSize(), j = 1; j < right_internal->GetSize(); i++, j++) {
            cur_internal->SetAt(i, right_internal->KeyAt(j), right_internal->ValueAt(j));
            cur_internal->IncreaseSize(1);
          }

          // 然后把index+1删掉
          for (int i = parent_index + 1; i < parent_internal->GetSize() - 1; i++) {
            parent_internal->SetAt(i, parent_internal->KeyAt(i + 1), parent_internal->ValueAt(i + 1));
          }
          parent_internal->IncreaseSize(-1);

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

    auto internal = reinterpret_cast<InternalPage *>(root_page);

    int index = BinaryFind(internal, key);
    keys_index.emplace_back(index);

    root_page_guard = bpm_->FetchPageWrite(internal->ValueAt(index));
    root_page = root_page_guard.AsMut<BPlusTreePage>();

    // crabbing，若当前节点安全（数量大于一半），则把祖先节点的锁全部放掉
    if ((root_page->IsLeafPage() && root_page->GetSize() > root_page->GetMinSize()) ||
        (!root_page->IsLeafPage() && root_page->GetSize() > root_page->GetMinSize() + 1)) {
      while (!ctx.write_set_.empty()) {
        ctx.write_set_.back().SetDirty(false);
        ctx.write_set_.back().Drop();
        ctx.write_set_.pop_back();
      }
    }

    ctx.write_set_.emplace_back(std::move(root_page_guard));
  }

  if (!header_drop) {
    header_page_guard.SetDirty(true);
    header_page_guard.Drop();
  }
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

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return End();
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();

  // crabbing
  header_page_guard.SetDirty(false);
  header_page_guard.Drop();

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

      // crabbing
      while (!ctx.read_set_.empty()) {
        ctx.read_set_.back().SetDirty(false);
        ctx.read_set_.back().Drop();
        ctx.read_set_.pop_back();
      }

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

  auto guard = bpm_->FetchPageRead(begin_leaf);
  auto leaf = guard.As<LeafPage>();
  if (leaf->GetSize() == 0) {
    guard.SetDirty(false);
    guard.Drop();
    return End();
  }

  guard.SetDirty(false);
  guard.Drop();
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

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return End();
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();

  // crabbing
  header_page_guard.SetDirty(false);
  header_page_guard.Drop();

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

    while (!ctx.read_set_.empty()) {
      ctx.read_set_.back().SetDirty(false);
      ctx.read_set_.back().Drop();
      ctx.read_set_.pop_back();
    }

  } else {
    while (true) {
      auto internal = reinterpret_cast<const InternalPage *>(root_page);
      int idx = BinaryFind(internal, key);
      root_page_guard = bpm_->FetchPageRead(internal->ValueAt(idx));
      root_page = root_page_guard.As<BPlusTreePage>();

      // crabbing
      while (!ctx.read_set_.empty()) {
        ctx.read_set_.back().SetDirty(false);
        ctx.read_set_.back().Drop();
        ctx.read_set_.pop_back();
      }

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

      ctx.read_set_.emplace_back(std::move(root_page_guard));
    }
  }

  auto guard = bpm_->FetchPageRead(begin_leaf);
  auto leaf = guard.As<LeafPage>();
  if (leaf->GetSize() == 0) {
    guard.SetDirty(false);
    guard.Drop();
    return End();
  }

  guard.SetDirty(false);
  guard.Drop();

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
