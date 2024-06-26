//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

// #define WORD_OFFSET(i) ((i) >> 6)
// #define BIT_OFFSET(i) ((i) & 0x3f)
// struct BitMap {
//   size_t size_;
//   uint64_t *data_;

//   BitMap() : size_(0), data_(nullptr) {}
//   explicit BitMap(size_t size) : size_(size) {
//     data_ = new uint64_t[WORD_OFFSET(size) + 1];
//     Clear();
//   }
//   ~BitMap() { delete[] data_; }
//   void Clear() {
//     size_t bm_size = WORD_OFFSET(size_);
//     for (size_t i = 0; i <= bm_size; i++) {
//       data_[i] = 0;
//     }
//   }
//   void Fill() {
//     size_t bm_size = WORD_OFFSET(size_);
//     for (size_t i = 0; i < bm_size; i++) {
//       data_[i] = 0xffffffffffffffff;
//     }
//     data_[bm_size] = 0;
//     for (size_t i = (bm_size << 6); i < size_; i++) {
//       data_[bm_size] |= 1UL << BIT_OFFSET(i);
//     }
//   }

//   auto GetBit(size_t i) -> uint64_t { return data_[WORD_OFFSET(i)] & (1UL << BIT_OFFSET(i)); }

//   void SetBit(size_t i) { data_[WORD_OFFSET(i)] |= (1UL << BIT_OFFSET(i)); }
// };

class LRUKNode {
 public:
  explicit LRUKNode(size_t k = 0);

  auto operator<(const LRUKNode &rhs) const -> bool;

  void Init(size_t k);

  auto Evictable() const -> bool { return is_evictable_; }

  void SetEvictable(bool set_evictable) { is_evictable_ = set_evictable; }

  auto Valid() const -> bool;

  void SetInvalid();

  auto KDistance() const -> size_t { return k_distance_; }

  void Access(size_t timestamp);

 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::vector<size_t> history_;
  size_t start_{0};
  size_t end_{0};
  size_t k_distance_;
  size_t k_;
  bool is_evictable_{false};
};

class LRUKImpl {
 public:
  LRUKImpl(size_t node_num, std::vector<LRUKNode> &node_ref);

  void Push(frame_id_t frame_id);

  auto Pop() -> frame_id_t;

  void Remove(frame_id_t frame_id);

 private:
  struct ListNode {
    ListNode *prev_;
    ListNode *next_;
    frame_id_t frame_id_;
  };

  struct List {
    ListNode *head_{nullptr};
    ListNode *tail_{nullptr};
    size_t size_{0};

    ~List() {
      auto curr = head_;
      while (curr != nullptr) {
        auto temp = curr;
        curr = curr->next_;
        delete temp;
      }
    }

    // auto Push(frame_id_t frame_id) -> ListNode * {
    //   ++size_;
    //   ListNode *new_node = new ListNode{nullptr, nullptr, frame_id};
    //   // push to tail
    //   if (tail_ == nullptr) {  // empty list
    //     head_ = new_node;
    //   } else {
    //     tail_->next_ = new_node;
    //     new_node->prev_ = tail_;
    //   }
    //   tail_ = new_node;
    //   return new_node;
    // }

    // auto Evict(std::vector<LRUKNode> &frame_ref) -> ListNode * {
    //   auto curr = head_;
    //   while (curr != nullptr) {
    //     if (frame_ref[curr->frame_id_].Evictable()) {
    //       return curr;
    //     }
    //     curr = curr->next_;
    //   }
    //   return nullptr;
    // }

    // void Remove(ListNode *node_ptr) {
    //   --size_;
    //   if (node_ptr->prev_ == nullptr) {  // node is head
    //     head_ = node_ptr->next_;
    //   } else {
    //     node_ptr->prev_->next_ = node_ptr->next_;
    //   }

    //   if (node_ptr->next_ == nullptr) {
    //     tail_ = node_ptr->prev_;
    //   } else {
    //     node_ptr->next_->prev_ = node_ptr->prev_;
    //   }
    //   delete node_ptr;
    // }
  };

  union Pos {
    ListNode *list_ptr_;
    uint64_t heap_pos_;
  };

  std::vector<LRUKNode> &node_ref_;   // reference to node store
  std::vector<frame_id_t> r_heap_;    // lru heap. heap_[0] is a sentinel element, indicate the size of heap;
  List f_list_;                       // lfu list.
  std::vector<Pos> frame_pos_map_;    // map frame id to posistion in heap/list
  std::vector<char> frame_pos_type_;  // true is list, false is heap

  void Push2List(frame_id_t frame_id);

  // select a frame from list to evict, not actually to delete it
  auto ListEvict() -> ListNode *;

  void RemoveFromList(const ListNode *node_ptr);

  void Push2Heap(frame_id_t frame_id, bool new_entry);

  // select a frame from list to evict, not actually to delete it
  auto HeapEvict() -> size_t;

  void RemoveFromHeap(size_t pos);

  void AmendHeap(size_t pos);
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

  auto Evictable(frame_id_t frame_id) -> bool { return node_store_[frame_id].Evictable(); }

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::vector<LRUKNode> node_store_;
  LRUKImpl node_heap_;
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;  // max size
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
