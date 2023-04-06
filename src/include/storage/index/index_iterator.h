//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  // you may define your own constructor based on your member variables
  IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t cur, int index);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return bpm_ == itr.bpm_ && cur_ == itr.cur_ && index_ == itr.index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  BufferPoolManager *bpm_;
  page_id_t cur_;
  int index_;
  MappingType item_;
  // add your own private member variables here
};

}  // namespace bustub
