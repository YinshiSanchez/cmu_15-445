//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
  BasicPageGuard header_page = bpm_->NewPageGuarded(&header_page_id_);
  WritePageGuard header_page_w = header_page.UpgradeWrite();
  auto ht_header = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page_w.GetDataMut());
  ht_header->Init(header_max_depth);
  header_page_w.Drop();
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  ReadPageGuard header_page_r = bpm_->FetchPageRead(header_page_id_);
  // GetData() return const char *
  auto header = reinterpret_cast<const ExtendibleHTableHeaderPage *>(header_page_r.GetData());
  // 这里不用调用init函数是因为读到的page里面相应的值已经写好，比如directory_page_ids_[],max_depth_
  uint32_t hash = Hash(key);
  uint32_t dirctory_index = header->HashToDirectoryIndex(hash);
  page_id_t dirctory_page_id = header->GetDirectoryPageId(dirctory_index);
  // dirctory_page_id not found.
  if (dirctory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // release the lock immediately after reading.
  header_page_r.Drop();
  ReadPageGuard dirctory_page_r = bpm_->FetchPageRead(dirctory_page_id);
  auto directory = reinterpret_cast<const ExtendibleHTableDirectoryPage *>(dirctory_page_r.GetData());
  uint32_t bucket_index = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_index);
  // 如果page_id没有找到
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // release the lock immediately after reading.
  dirctory_page_r.Drop();
  ReadPageGuard bucket_page_r = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page_r.GetData());
  V ans;
  if (bucket->Lookup(key, ans, cmp_)) {
    result->push_back(ans);
    return true;
  }
  // don't need to explicitly call the Drop() here because it will be automatically destructed
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  WritePageGuard header_page_w = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_page_w.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t dirctory_index = header->HashToDirectoryIndex(hash);
  page_id_t dirctory_page_id = header->GetDirectoryPageId(dirctory_index);
  if (dirctory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, dirctory_index, hash, key, value);
  }
  header_page_w.Drop();
  WritePageGuard dirctory_page_w = bpm_->FetchPageWrite(dirctory_page_id);
  auto directory = dirctory_page_w.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory, bucket_index, key, value);
  }
  // release the lock immediately after reading.
  // dirctory_page_w.Drop();
  WritePageGuard bucket_page_w = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket->IsFull()) {
    return bucket->Insert(key, value, cmp_);
  }
  while (bucket->IsFull()) {
    uint32_t local_depth = directory->GetLocalDepth(bucket_index);
    uint32_t global_depth = directory->GetGlobalDepth();
    if (global_depth == local_depth && global_depth == directory_max_depth_) {
      // directory full
      return false;
    }
    // apply new bucket
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    BasicPageGuard new_bucket_page = bpm_->NewPageGuarded(&new_bucket_page_id);
    if (new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    WritePageGuard new_bucket_page_w = new_bucket_page.UpgradeWrite();
    auto new_bucket = new_bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket->Init(bucket_max_size_);
    if (global_depth == local_depth) {
      directory->IncrGlobalDepth();
      global_depth = directory->GetGlobalDepth();
    }
    auto split_bucket_idx = directory->GetSplitImageIndex(bucket_index);
    MigrateEntries(bucket, new_bucket, split_bucket_idx, 1 << local_depth);
    directory->IncrLocalDepth(bucket_index);
    local_depth = directory->GetLocalDepth(bucket_index);
    // directory->local_depths_[split_bucket_idx] = local_depth;
    directory->SetLocalDepth(split_bucket_idx, local_depth);
    // directory->bucket_page_ids_[split_bucket_idx] = new_bucket_page;
    directory->SetBucketPageId(split_bucket_idx, new_bucket_page_id);
    auto count = 1 << (global_depth - local_depth);
    auto mask = directory->GetLocalDepthMask(split_bucket_idx);
    auto stride = 1 << local_depth;
    for (auto i = 0; i < count; i++) {
      directory->SetBucketPageId((split_bucket_idx & mask) + i * stride, new_bucket_page_id);
      directory->SetLocalDepth((split_bucket_idx & mask) + i * stride, local_depth);
    }
    if (directory->HashToBucketIndex(hash) != bucket_index) {
      bucket = new_bucket;
      bucket_index = split_bucket_idx;
    }
  }

  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t dirctory_page_id = INVALID_PAGE_ID;
  BasicPageGuard directory_page = bpm_->NewPageGuarded(&dirctory_page_id);
  if (dirctory_page_id == INVALID_PAGE_ID) {
    // assign page fail
    return false;
  }
  header->SetDirectoryPageId(directory_idx, dirctory_page_id);
  WritePageGuard directory_page_w = directory_page.UpgradeWrite();
  auto directory = directory_page_w.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);
  uint32_t bucket_index = directory->HashToBucketIndex(hash);
  return InsertToNewBucket(directory, bucket_index, key, value);
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard bucket_page = bpm_->NewPageGuarded(&bucket_page_id);
  if (bucket_page_id == INVALID_PAGE_ID) {
    // assign page fail
    return false;
  }
  WritePageGuard bucket_page_w = bucket_page.UpgradeWrite();
  auto bucket = bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);
  auto count = 1 << (directory->GetGlobalDepth() - 0);
  uint32_t mask = directory->GetLocalDepthMask(bucket_idx);
  auto stride = 1;
  for (auto i = 0; i < count; i++) {
    directory->SetLocalDepth((bucket_idx & mask) + i * stride, 0);
    directory->SetBucketPageId((bucket_idx & mask) + i * stride, bucket_page_id);
  }
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  WritePageGuard header_page_w = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_page_w.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t dirctory_index = header->HashToDirectoryIndex(hash);
  page_id_t dirctory_page_id = header->GetDirectoryPageId(dirctory_index);
  // dirctory_page_id not found.
  if (dirctory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_page_w.Drop();
  WritePageGuard dirctory_page_w = bpm_->FetchPageWrite(dirctory_page_id);
  auto directory = dirctory_page_w.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_index);
  // 如果page_id没有找到
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_page_w = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_w = bucket_page_w.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bool is_successful = bucket_w->Remove(key, cmp_);
  if (!is_successful) {
    // remove fail
    return false;
  }
  bucket_page_w.Drop();

  // up is release
  ReadPageGuard bucket_page_r = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = bucket_page_r.As<ExtendibleHTableBucketPage<K, V, KC>>();
  uint32_t local_depth = directory->GetLocalDepth(bucket_index);
  uint32_t merge_mask;
  while (local_depth > 0) {
    merge_mask = 1 << (local_depth - 1);
    uint32_t global_depth = directory->GetGlobalDepth();
    uint32_t merge_bucket_index = merge_mask ^ bucket_index;
    uint32_t merge_local_depth = directory->GetLocalDepth(merge_bucket_index);
    ReadPageGuard merge_bucket_page = bpm_->FetchPageRead(directory->GetBucketPageId(merge_bucket_index));
    auto merge_bucket = merge_bucket_page.As<ExtendibleHTableBucketPage<K, V, KC>>();
    if (local_depth > 0 && local_depth == merge_local_depth && (merge_bucket->IsEmpty() || bucket->IsEmpty())) {
      if (merge_bucket->IsEmpty()) {
        bpm_->DeletePage(directory->GetBucketPageId(merge_bucket_index));
        directory->DecrLocalDepth(bucket_index);
        directory->DecrLocalDepth(merge_bucket_index);
        directory->SetBucketPageId(merge_bucket_index, bucket_page_id);
        auto count = 1 << (global_depth - local_depth + 1);
        u_int32_t mask = directory->GetLocalDepthMask(bucket_index);
        auto stride = 1 << (local_depth - 1);
        for (auto i = 0; i < count; i++) {
          directory->SetLocalDepth((bucket_index & mask) + i * stride, directory->GetLocalDepth(bucket_index));
          directory->SetBucketPageId((bucket_index & mask) + i * stride, bucket_page_id);
        }

      } else {
        bpm_->DeletePage(bucket_page_id);
        directory->DecrLocalDepth(merge_bucket_index);
        directory->DecrLocalDepth(bucket_index);
        directory->SetBucketPageId(bucket_index, directory->GetBucketPageId(merge_bucket_index));
        auto count = 1 << (global_depth - merge_local_depth + 1);
        u_int32_t mask = directory->GetLocalDepthMask(merge_bucket_index);
        auto stride = 1 << (merge_local_depth - 1);
        for (auto i = 0; i < count; i++) {
          directory->SetLocalDepth((merge_bucket_index & mask) + i * stride,
                                   directory->GetLocalDepth(merge_bucket_index));
          directory->SetBucketPageId((merge_bucket_index & mask) + i * stride,
                                     directory->GetBucketPageId(merge_bucket_index));
        }
        bucket_index = merge_bucket_index;
        bucket = merge_bucket;
        bucket_page_id = directory->GetBucketPageId(merge_bucket_index);
      }
      local_depth = directory->GetLocalDepth(bucket_index);
    } else {
      break;
    }
  }
  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (int32_t i = old_bucket->Size() - 1; i >= 0; i--) {
    K key_temp = old_bucket->KeyAt(i);
    auto hash_temp = Hash(key_temp);
    // std::cout << key_temp << " " << local_depth_mask << " " << (hash_temp & local_depth_mask) << std::endl;
    if ((hash_temp & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {  // Parentheses added for clarity
      // insert to new
      auto &entry = old_bucket->EntryAt(i);
      new_bucket->Insert(entry.first, entry.second, cmp_);
      old_bucket->RemoveAt(i);
    }
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub