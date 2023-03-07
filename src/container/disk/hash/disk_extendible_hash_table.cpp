//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/disk/hash/disk_extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                         const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_)->GetData());

  page_id_t bucket0_page_id;
  buffer_pool_manager_->NewPage(&bucket0_page_id);
  directory_page->SetBucketPageId(0, bucket0_page_id);
  directory_page->SetLocalDepth(0, 0);

  // unpin pages：说明这些页暂时没有使用
  buffer_pool_manager->UnpinPage(directory_page_id_, true);  // 已被修改
  buffer_pool_manager->UnpinPage(bucket0_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> page_id_t {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

  // std::cout << key << " " << KeyToDirectoryIndex(key, dir) << " " << bucket_page_id << std::endl;

  return bucket->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

  // std::cout << "insert " << key << " " << value << std::endl;

  if (bucket->IsFull()) return SplitInsert(transaction, key, value);

  return bucket->Insert(key, value, comparator_);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  // std::cout << "split_insert " << key << " " << value << std::endl;

  HashTableDirectoryPage *dir = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

  uint32_t pre_mask = dir->GetGlobalDepthMask();
  uint32_t dir_idx = KeyToDirectoryIndex(key, dir);

  if (dir->GetGlobalDepth() == dir->GetLocalDepth(dir_idx)) {
    // std::cout << "case1" << std::endl;
    uint32_t split_idx1 = dir_idx, split_idx2 = dir_idx + dir->Size();
    // std::cout << "split_idx1: " << split_idx1 << " "
    //           << "split_idx2: " << split_idx2 << std::endl;
    dir->IncrGlobalDepth();

    for (uint32_t i = 0; i < dir->Size(); i++) {
      if (i != split_idx1 && i != split_idx2) {
        dir->SetBucketPageId(i, dir->GetBucketPageId(i & pre_mask));
      }
    }

    page_id_t page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->NewPage(&page_id);
    HASH_TABLE_BUCKET_TYPE *new_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());
    // std::cout << "new_page_id: " << page_id << std::endl;
    dir->SetBucketPageId(split_idx2, page_id);
    dir->IncrLocalDepth(split_idx2);
    dir->IncrLocalDepth(split_idx1);

    std::vector<MappingType> items;
    bucket->GetAllItems(&items);

    for (auto &x : items) {
      if (KeyToDirectoryIndex(x.first, dir) == split_idx2) {
        new_bucket->Insert(x.first, x.second, comparator_);
        bucket->Remove(x.first, x.second, comparator_);
      }
    }

    page_id_t tar_bucket_page_id = KeyToPageId(key, dir);
    HASH_TABLE_BUCKET_TYPE *tar_bucket = FetchBucketPage(tar_bucket_page_id);
    tar_bucket->Insert(key, value, comparator_);

  } else {
    // std::cout << "case2" << std::endl;
    uint32_t diff = dir->GetGlobalDepth() - dir->GetLocalDepth(dir_idx);

    std::vector<uint32_t> affected_idxs;
    for (uint32_t i = 0; i < (1u << diff); i++) {
      if (i & 1) {
        affected_idxs.push_back(dir_idx + (i << dir->GetLocalDepth(dir_idx)));
      }
    }

    page_id_t page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->NewPage(&page_id);
    HASH_TABLE_BUCKET_TYPE *new_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_page->GetData());

    // std::cout << page_id << std::endl;

    std::vector<MappingType> items;
    bucket->GetAllItems(&items);

    for (auto &x : items) {
      if (((KeyToDirectoryIndex(x.first, dir) >> dir->GetLocalDepth(dir_idx)) & 1) == 1) {
        new_bucket->Insert(x.first, x.second, comparator_);
        bucket->Remove(x.first, x.second, comparator_);
      }
    }

    for (auto idx : affected_idxs) {
      dir->SetBucketPageId(idx, page_id);
      dir->IncrLocalDepth(idx);
    }

    page_id_t tar_bucket_page_id = KeyToPageId(key, dir);
    HASH_TABLE_BUCKET_TYPE *tar_bucket = FetchBucketPage(tar_bucket_page_id);
    tar_bucket->Insert(key, value, comparator_);
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

  return bucket->Remove(key, value, comparator_);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class DiskExtendibleHashTable<int, int, IntComparator>;

template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
