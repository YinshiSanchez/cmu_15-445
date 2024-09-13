//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

// init bucket_page_ids_ and local_depths
void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  for (int i = 0; i < 1 << max_depth_; i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  // 这个可以用%
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  // TODO(zht)
  if (bucket_idx >= MaxSize()) {
    return;
  }
  // uint32_t upper = 1 << global_depth_;
  // bool new_bucket_idx = false;
  // for (u_int32_t i = 0; i < upper; i++) {
  //   if (bucket_page_ids_[i] == bucket_page_id) {
  //     local_depths_[bucket_idx] = local_depths_[i];
  //     new_bucket_idx = true;
  //     break;
  //   }
  // }
  // if (!new_bucket_idx) {
  //   local_depths_[bucket_idx] = global_depth_;
  // }
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  // example bucket_idx: 0101  local_depth: 1 splitimageindex:0110
  // 低位相同,local_depth + 1位取反
  uint32_t local_depth = GetLocalDepth(bucket_idx);
  return bucket_idx ^ (1 << local_depth);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  // example global_depth:0  mask:0
  // example global_depth:1  mask:1
  // example global_depth:1  mask:11
  return (1 << global_depth_) - 1;
}
auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  // similar to GetGlobalDepthMask()
  return (1 << GetLocalDepth(bucket_idx)) - 1;
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  // when increase global_depth, we also need link additonal array
  if (global_depth_ < max_depth_) {
    uint32_t upper = 1 << (global_depth_ + 1);
    for (uint32_t i = 1 << global_depth_; i < upper; i++) {
      bucket_page_ids_[i] = bucket_page_ids_[HashToBucketIndex(i)];
      local_depths_[i] = local_depths_[HashToBucketIndex(i)];
    }
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  // when decrease global_depth, we also need init substractive array
  global_depth_--;
  uint32_t upper = 1 << (global_depth_ + 1);
  for (uint32_t i = 1 << global_depth_; i < upper; i++) {
    bucket_page_ids_[i] = INVALID_PAGE_ID;
    local_depths_[i] = 0;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  // if all local_depth values are less than the global_depth, it indicates that shrink is possible
  for (int i = 0; i < (1 << global_depth_); i++) {
    if (GetLocalDepth(i) == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << global_depth_; }
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }
auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (bucket_idx < MaxSize()) {
    local_depths_[bucket_idx] = local_depth;
  }
}
void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  // 在增加某一个local_depth的时候，我们需要将所有指向同一个bucket的array[i]全部更新
  uint32_t local_depth = GetLocalDepth(bucket_idx);
  local_depths_[bucket_idx]++;
  auto count = 1 << (global_depth_ - local_depth);
  uint32_t mask = GetLocalDepthMask(bucket_idx);
  auto stride = 1 << local_depth;
  for (auto i = 0; i < count; i++) {
    u_int32_t next_bucket_idx = (bucket_idx & mask) + i * stride;
    bucket_page_ids_[next_bucket_idx] = bucket_page_ids_[bucket_idx];
    local_depths_[next_bucket_idx] = local_depth + 1;
  }
  // local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub