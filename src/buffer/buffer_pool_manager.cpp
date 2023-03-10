#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];

  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  replacer_.reset();
}

// 从空闲frame中获取frame
void BufferPoolManager::FromFreeListGetFrameId(frame_id_t *frame_id) {
  *frame_id = free_list_.back();
  free_list_.pop_back();
}

// 从可驱逐frame中获取frame
void BufferPoolManager::FromEvitableGetFrameId(frame_id_t *frame_id) {
  // 将某个可驱逐的frame的frame_id
  replacer_->Evict(frame_id);

  // 获取该frame放置的page
  Page *occupied_page = pages_ + (*frame_id);

  // 如果这个page被改动过，将改动的内容写回disk
  if (occupied_page->IsDirty()) {
    FlushPage(occupied_page->page_id_);
  }

  // 把对应page_id从表中抹除掉
  page_table_.erase(occupied_page->page_id_);
}

// 初始化对应一个新的page（其各个参数）
auto BufferPoolManager::InitNewPage(frame_id_t frame_id, page_id_t page_id) -> Page * {
  // 将page_id加入表中
  page_table_[page_id] = frame_id;

  // 获取该page
  Page *page = pages_ + frame_id;

  // 初始化其参数
  page->ResetMemory();
  page->page_id_ = page_id;
  page->pin_count_ = 0;
  page->is_dirty_ = false;

  return page;
}

// 锁定该page
void BufferPoolManager::PinPage(page_id_t page_id) {
  // 访问它并使其不可驱逐
  frame_id_t frame_id = page_table_[page_id];
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);
  (pages_ + frame_id)->pin_count_++;
}

// 生成一个新的page，对应的内容存在pages_[frame_id]，page里没有数据
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  // 如果没有空闲的frame或者可驱逐的frame，则返回nullptr
  if (free_list_.empty() && replacer_->Size() == 0) {
    std::cout << "has_fulled" << std::endl;
    page_id = nullptr;
    latch_.unlock();
    return nullptr;
  }

  // 分配一个page_id
  *page_id = AllocatePage();

  // 为该page找到一个replacer里面空闲或可驱逐的frame，并获取其frame_id
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    FromFreeListGetFrameId(&frame_id);
  } else {
    FromEvitableGetFrameId(&frame_id);
  }

  // 初始化page的内容
  Page *page = InitNewPage(frame_id, *page_id);

  // 锁定该page
  PinPage(*page_id);

  latch_.unlock();

  return page;
}

// 根据page_id获取该page
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  // 若不存在的话，则创造一个该page_id对应的空白page，并且据page_id从disk中把数据读到page里
  if (page_table_.count(page_id) == 0) {
    // ps:当没有可用frames时，直接返回nullptr（此时需要从disk直接读取）
    if (free_list_.empty() && replacer_->Size() == 0) {
      latch_.unlock();
      return nullptr;
    }

    // 先为这个page_id生成一个空白page
    frame_id_t frame_id;
    if (!free_list_.empty()) {
      FromFreeListGetFrameId(&frame_id);
    } else {
      FromEvitableGetFrameId(&frame_id);
    }

    Page *page = InitNewPage(frame_id, page_id);

    PinPage(page_id);

    // 再把数据从disk读到空白page里
    disk_manager_->ReadPage(page_id, page->GetData());

    latch_.unlock();

    return page;
  }

  // 如果有，则锁定该page，然后返回即可
  PinPage(page_id);

  latch_.unlock();

  return pages_ + page_table_[page_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  // std::cout << "Unpined" << std::endl;
  if (page_table_.count(page_id) == 0) {
    latch_.unlock();
    return false;
  }

  Page *page = pages_ + page_table_[page_id];

  // std::cout << "Unpin_address:" << page << std::endl;

  if (page->GetPinCount() == 0) {
    latch_.unlock();
    return false;
  }

  // pin_count是该page的同时锁定数，每解除一个减1
  --page->pin_count_;

  // 如果没有任何人锁定这个page，则可驱逐
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }

  // 只要被任何一个人改过，则is_dirty就为真，表示和disk里的内容不一致，因此这里是或的关系
  page->is_dirty_ |= is_dirty;

  latch_.unlock();

  return true;
}

// 将page_id对应的page的内容写入disk
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID || page_table_.count(page_id) == 0) {
    return false;
  }

  Page *page = pages_ + page_table_[page_id];
  disk_manager_->WritePage(page_id, page->GetData());
  // 写入disk后，此时pool里的内容就和disk内容一致了，is_dirty设为false
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto [k, v] : page_table_) {
    Page *page = pages_ + v;
    disk_manager_->WritePage(k, page->GetData());
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0U) {
    latch_.unlock();
    return true;
  }

  Page *page = pages_ + page_table_[page_id];
  if (page->GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }

  free_list_.push_front(page_table_[page_id]);

  page->ResetMemory();
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;

  DeallocatePage(page_id);

  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  page_id_t page_id = next_page_id_;
  next_page_id_++;
  return page_id;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return BasicPageGuard{this, FetchPage(page_id)};
}

// 获取一个带有读锁的page
auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  // 特判，否则下面的nullptr->Rlatch会报错
  if (page == nullptr) {
    return ReadPageGuard{this, nullptr};
  }
  page->RLatch();
  return ReadPageGuard{this, page};
}

// 获取一个带有写锁的page
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return WritePageGuard{this, nullptr};
  }
  page->WLatch();
  return WritePageGuard{this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  return BasicPageGuard{this, NewPage(page_id)};
}
}  // namespace bustub
