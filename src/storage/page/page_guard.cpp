#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

// 将that的内容拿过来，然后讲that清空
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

// 将page_guard内容清空，并且让其所持有的page unpin
void BasicPageGuard::Drop() {
  // printf("basic_drop\n");
  if (page_ == nullptr) {
    return;
  }

  if (page_->GetPageId() != INVALID_PAGE_ID) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }

  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

// 将that内容拿过来，如果自己本身有page，要先把page unpin
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();

    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  // printf("basic_析构\n");
  Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); };

// 由于ReadPageGuard持有的page有锁，所以对方的page拿过来替换前，记得先把原来page的锁打开，否则该page会一直持有锁
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  // printf("read =\n");
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->RUnlatch();
    }
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  // printf("read drop\n");
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  // printf("read析构\n");
  if (guard_.page_ != nullptr) {
    Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); };

// 由于WritePageGuard持有的page有锁，所以对方的page拿过来替换前，记得先把原来page的锁打开，否则该page会一直持有锁
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->WUnlatch();
    }
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  if (guard_.page_ != nullptr) {
    Drop();
  }
}  // NOLINT

}  // namespace bustub
