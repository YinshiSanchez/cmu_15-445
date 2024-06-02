//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/core.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  avaliable_ = std::vector<std::pair<std::condition_variable, bool>>(pool_size);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    avaliable_[i].second = true;
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock lock(latch_);
  frame_id_t frame_id(-1);
  if (!free_list_.empty()) {  // find from free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {  // find from replacer
    replacer_->Evict(&frame_id);
  }

  // allocate page
  if (frame_id >= 0) {
    // remove old page
    page_table_.erase(pages_[frame_id].page_id_);

    // insert new frame
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    // allocate new page
    *page_id = AllocatePage();
    page_table_[*page_id] = frame_id;
    pages_[frame_id].pin_count_ = 1;

    if (pages_[frame_id].IsDirty()) {
      // dirty page, need to write back to disk
      FlushFrame(frame_id);
    }
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].ResetMemory();

    return &pages_[frame_id];
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock lock(latch_);
  frame_id_t frame_id(-1);
  auto page_itr = page_table_.find(page_id);

  // page exists in buffer pool
  if (page_itr != page_table_.end()) {
    frame_id = page_itr->second;
    ++pages_[frame_id].pin_count_;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    if (!avaliable_[frame_id].second) {
      avaliable_[frame_id].first.wait(lock, [this, frame_id]() { return avaliable_[frame_id].second; });
    }
    return &pages_[frame_id];
  }

  if (!free_list_.empty()) {  // find from free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {  // find from replacer
    replacer_->Evict(&frame_id);
  }

  if (frame_id >= 0) {
    // remove old page
    page_table_.erase(pages_[frame_id].page_id_);

    // allocate new page
    page_table_[page_id] = frame_id;
    pages_[frame_id].pin_count_ = 1;

    // insert new frame
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    if (pages_[frame_id].IsDirty()) {
      FlushFrame(frame_id);
    }
    pages_[frame_id].page_id_ = page_id;
    ReadFrame(frame_id);

    return &pages_[frame_id];
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock lock(latch_);
  auto page_itr = page_table_.find(page_id);
  if (page_itr != page_table_.end()) {
    if (page_itr->second < 0 || page_itr->second > static_cast<int>(pool_size_)) {
      std::cerr << "ERROR: frame id=" << page_itr->second << " pool size=" << pool_size_ << std::endl;
    }
    pages_[page_itr->second].is_dirty_ |= is_dirty;
    if (pages_[page_itr->second].pin_count_ > 0) {
      --pages_[page_itr->second].pin_count_;
      if (pages_[page_itr->second].pin_count_ == 0) {
        replacer_->SetEvictable(page_itr->second, true);
      }
      return true;
    }
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id!");
  std::lock_guard lg(latch_);
  auto page_itr = page_table_.find(page_id);
  if (page_itr != page_table_.end()) {
    FlushFrame(page_itr->second);
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard lg(latch_);
  for ([[maybe_unused]] auto [_, frame_id] : page_table_) {  // NOTLINT
    FlushFrame(frame_id);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard lg(latch_);
  auto page_itr = page_table_.find(page_id);
  if (page_itr == page_table_.end()) {
    return true;
  }
  if (pages_[page_itr->second].pin_count_ != 0) {
    return false;
  }
  frame_id_t frame_id = page_itr->second;
  page_table_.erase(page_itr);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  if (pages_[frame_id].IsDirty()) {
    FlushFrame(frame_id);
  }
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page_ptr = FetchPage(page_id);
  if (page_ptr != nullptr) {
    page_ptr->RLatch();
  }
  return {this, page_ptr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page_ptr = FetchPage(page_id);
  if (page_ptr != nullptr) {
    page_ptr->WLatch();
  }
  return WritePageGuard{this, page_ptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

void BufferPoolManager::FlushFrame(frame_id_t frame_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
  avaliable_[frame_id].second = false;
  latch_.unlock();
  BUSTUB_ENSURE(future.get(), "write page failed!");
  latch_.lock();
  avaliable_[frame_id].second = true;
  avaliable_[frame_id].first.notify_all();
  pages_[frame_id].is_dirty_ = false;
}

void BufferPoolManager::ReadFrame(frame_id_t frame_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(), std::move(promise)});
  avaliable_[frame_id].second = false;
  latch_.unlock();
  BUSTUB_ENSURE(future.get(), "read page failed!");
  latch_.lock();
  avaliable_[frame_id].second = true;
  avaliable_[frame_id].first.notify_all();
}

}  // namespace bustub
