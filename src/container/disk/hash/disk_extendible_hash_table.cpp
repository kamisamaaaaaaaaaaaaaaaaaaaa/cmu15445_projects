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

namespace bustub
{

  template <typename KeyType, typename ValueType, typename KeyComparator>
  HASH_TABLE_TYPE::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                           const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
      : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn))
  {
    //  implement me!
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
  auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t
  {
    return static_cast<uint32_t>(hash_fn_.GetHash(key));
  }

  template <typename KeyType, typename ValueType, typename KeyComparator>
  inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t
  {
    return Hash(key) && dir_page->GetGlobalDepthMask();
  }

  template <typename KeyType, typename ValueType, typename KeyComparator>
  inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> page_id_t
  {
    return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
  }

  template <typename KeyType, typename ValueType, typename KeyComparator>
  auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage *
  {
    return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
  }

  template <typename KeyType, typename ValueType, typename KeyComparator>
  auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE *
  {
    return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
  }

  /*****************************************************************************
   * SEARCH
   *****************************************************************************/
  template <typename KeyType, typename ValueType, typename KeyComparator>
  auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool
  {
    HashTableDirectoryPage *page = FetchDirectoryPage();
    page_id_t bucket_page_id = KeyToPageId(key, page);
    HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

    return bucket->GetValue(key, comparator_, result);
  }

  /*****************************************************************************
   * INSERTION
   *****************************************************************************/
  template <typename KeyType, typename ValueType, typename KeyComparator>
  auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool
  {

    HashTableDirectoryPage *page = FetchDirectoryPage();
    page_id_t bucket_page_id = KeyToPageId(key, page);
    HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);

    return bucket->Insert(key, value, comparator_);
  }

  template <typename KeyType, typename ValueType, typename KeyComparator>
  auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool
  {
    return false;
  }

  /*****************************************************************************
   * REMOVE
   *****************************************************************************/
  template <typename KeyType, typename ValueType, typename KeyComparator>
  auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool
  {
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
  auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t
  {
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
  void HASH_TABLE_TYPE::VerifyIntegrity()
  {
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

} // namespace bustub
