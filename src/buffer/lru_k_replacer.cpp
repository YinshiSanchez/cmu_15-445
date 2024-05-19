//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <sys/types.h>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

#define QUEUE_SIZE(START, END, K) (((END) - (START) + ((K) + 1)) % ((K) + 1))
#define QUEUE_POS(POS, K) ((POS) % ((K) + 1))

namespace bustub {

constexpr size_t INF = SIZE_MAX >> 1;

// LRUKNode

LRUKNode::LRUKNode(const size_t k) : history_(k + 1), k_(k) { k_distance_ = INF; }

auto LRUKNode::operator<(const LRUKNode &rhs) const -> bool { return false; }

void LRUKNode::Init(const size_t k) {
  start_ = 0;
  end_ = 0;
  k_ = k;
  k_distance_ = SIZE_MAX;
  is_evictable_ = false;
}

void LRUKNode::Access(const size_t timestamp) {
  if (QUEUE_SIZE(start_, end_, k_) == k_) {
    start_ = QUEUE_POS(start_ + 1, k_);
  }
  history_[end_++] = timestamp;
  end_ = QUEUE_POS(end_, k_);
  if (QUEUE_SIZE(start_, end_, k_) < k_) {
    k_distance_ = SIZE_MAX - history_[start_];
  } else {
    k_distance_ = INF - history_[start_];
  }
}

auto LRUKNode::Valid() const -> bool { return k_ != INF; }

void LRUKNode::SetInvalid() { k_ = INF; }

// LRUKImpl

LRUKImpl::LRUKImpl(const size_t node_num, std::vector<LRUKNode> &node_ref)
    : node_ref_(node_ref), r_heap_(node_num + 1), frame_pos_map_(node_num, Pos{nullptr}), frame_pos_type_(node_num, 1) {
  r_heap_[0] = 0;
}

void LRUKImpl::Push(const frame_id_t frame_id) {
  if (frame_pos_type_[frame_id] != 0) {
    if (frame_pos_map_[frame_id].list_ptr_ == nullptr) {  // new entry;
      Push2List(frame_id);
    } else if (node_ref_[frame_id].KDistance() < INF) {  // amount of history record reach K
      RemoveFromList(frame_pos_map_[frame_id].list_ptr_);
      Push2Heap(frame_id, true);
    }
  } else {  // old entry in lru heap
    Push2Heap(frame_id, false);
  }
}

auto LRUKImpl::Pop() -> frame_id_t {
  frame_id_t evict_frame = -1;
  if (f_list_.size_ > 0) {
    auto frame_ptr = ListEvict();
    if (frame_ptr != nullptr) {
      evict_frame = frame_ptr->frame_id_;
      RemoveFromList(frame_ptr);
    }
  }
  if (evict_frame == -1 && r_heap_[0] > 0) {
    evict_frame = HeapEvict();
    if (evict_frame != -1) {
      RemoveFromHeap(frame_pos_map_[evict_frame].heap_pos_);
    }
  }

  return evict_frame;
}

void LRUKImpl::Remove(const frame_id_t frame_id) {
  if (frame_pos_type_[frame_id] != 0) {
    RemoveFromList(frame_pos_map_[frame_id].list_ptr_);
  } else {
    RemoveFromHeap(frame_pos_map_[frame_id].heap_pos_);
  }
}

void LRUKImpl::Push2List(frame_id_t frame_id) {
  ++f_list_.size_;
  auto new_node = new ListNode{nullptr, nullptr, frame_id};
  // push to tail
  if (f_list_.tail_ == nullptr) {  // empty list
    f_list_.head_ = new_node;
  } else {
    f_list_.tail_->next_ = new_node;
    new_node->prev_ = f_list_.tail_;
  }
  f_list_.tail_ = new_node;
  frame_pos_map_[frame_id] = Pos{new_node};
}

auto LRUKImpl::ListEvict() -> ListNode * {
  auto curr = f_list_.head_;
  while (curr != nullptr) {
    if (node_ref_[curr->frame_id_].Evictable()) {
      return curr;
    }
    curr = curr->next_;
  }
  return nullptr;
}
void LRUKImpl::RemoveFromList(const ListNode *node_ptr) {
  --f_list_.size_;
  if (node_ptr->prev_ == nullptr) {  // node is head
    f_list_.head_ = node_ptr->next_;
  } else {
    node_ptr->prev_->next_ = node_ptr->next_;
  }

  if (node_ptr->next_ == nullptr) {
    f_list_.tail_ = node_ptr->prev_;
  } else {
    node_ptr->next_->prev_ = node_ptr->prev_;
  }

  frame_pos_map_[node_ptr->frame_id_] = Pos{nullptr};
  delete node_ptr;
}

void LRUKImpl::Push2Heap(const frame_id_t frame_id, bool new_entry) {
  if (new_entry) {  // new entry
    size_t cur_pos = ++r_heap_[0];
    auto parent_pos = cur_pos >> 1;
    while (parent_pos > 0) {
      if (node_ref_[frame_id].KDistance() > node_ref_[r_heap_[parent_pos]].KDistance()) {
        r_heap_[cur_pos] = r_heap_[parent_pos];
        frame_pos_map_[r_heap_[parent_pos]].heap_pos_ = cur_pos;
        cur_pos = parent_pos;
        parent_pos >>= 1;
      } else {
        break;
      }
    }
    r_heap_[cur_pos] = frame_id;
    frame_pos_type_[frame_id] = 0;
    frame_pos_map_[frame_id].heap_pos_ = cur_pos;
  } else {  // old entry
    AmendHeap(frame_pos_map_[frame_id].heap_pos_);
  }
}

auto LRUKImpl::HeapEvict() -> size_t {
  std::vector<size_t> queue((r_heap_[0] + 1) >> 1);  // posistion of nodes in heap
  size_t queue_size(1);
  size_t max_distance(0);
  int32_t max_frame(-1);

  queue[0] = 1;

  while (queue_size > 0) {
    std::vector<size_t> temp_queue((r_heap_[0] + 1) >> 1);
    size_t temp_size(0);

    for (size_t i = 0; i < queue_size; ++i) {
      auto cur_frame = r_heap_[queue[i]];
      if (node_ref_[cur_frame].Evictable()) {
        if (node_ref_[cur_frame].KDistance() > max_distance) {
          max_frame = cur_frame;
          max_distance = node_ref_[cur_frame].KDistance();
        }
      } else {  // cur_frame is non-evictable
        auto next_pos = queue[i] << 1;
        if (next_pos <= static_cast<uint64_t>(r_heap_[0]) &&
            node_ref_[r_heap_[next_pos]].KDistance() > max_distance) {  // left child
          temp_queue[temp_size++] = next_pos;
        }
        ++next_pos;
        if (next_pos <= static_cast<uint64_t>(r_heap_[0]) &&
            node_ref_[r_heap_[next_pos]].KDistance() > max_distance) {  // right child
          temp_queue[temp_size++] = next_pos;
        }
      }
    }
    queue_size = temp_size;
    queue = std::move(temp_queue);
  }

  return max_frame;
}

void LRUKImpl::RemoveFromHeap(const size_t pos) {
  BUSTUB_ASSERT(pos != INF, "try to remove an invalid frame");

  // move the lastest frame to cur pos
  auto remove_frame = r_heap_[pos];
  frame_pos_type_[remove_frame] = 1;
  frame_pos_map_[remove_frame].list_ptr_ = nullptr;

  auto cur_frame = r_heap_[r_heap_[0]];
  r_heap_[pos] = cur_frame;
  frame_pos_map_[cur_frame].heap_pos_ = pos;
  --r_heap_[0];

  // correct heap tree;
  AmendHeap(pos);
}

void LRUKImpl::AmendHeap(size_t pos) {
  int32_t cur_pos = pos;
  int32_t next_pos = pos << 1;
  while (next_pos <= r_heap_[0]) {
    if (next_pos < r_heap_[0] &&
        node_ref_[r_heap_[next_pos]].KDistance() < node_ref_[r_heap_[next_pos + 1]].KDistance()) {
      ++next_pos;
    }
    auto cur_frame = r_heap_[cur_pos];
    auto next_frame = r_heap_[next_pos];
    if (node_ref_[cur_frame].KDistance() > node_ref_[next_frame].KDistance()) {
      break;
    }

    std::swap(frame_pos_map_[cur_frame], frame_pos_map_[next_frame]);
    std::swap(r_heap_[cur_pos], r_heap_[next_pos]);
    cur_pos = next_pos;
    next_pos <<= 1;
  }
}

// LRUKReplacer

LRUKReplacer::LRUKReplacer(const size_t num_frames, const size_t k)
    : node_store_(num_frames, LRUKNode{k}), node_heap_(num_frames, node_store_), replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard lg(latch_);
  if (curr_size_ > 0) {
    auto temp = node_heap_.Pop();
    if (temp != -1) {
      node_store_[temp].SetInvalid();
      *frame_id = temp;
      --curr_size_;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(const frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame id!");

  std::lock_guard lg(latch_);
  auto &node = node_store_[frame_id];
  if (!node.Valid()) {
    node.Init(k_);
  }
  node.Access(++current_timestamp_);
  node_heap_.Push(frame_id);
}

void LRUKReplacer::SetEvictable(const frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame id!");
  std::lock_guard lg(latch_);

  if (set_evictable != node_store_[frame_id].Evictable()) {
    node_store_[frame_id].SetEvictable(set_evictable);
    if (set_evictable) {
      ++curr_size_;
    } else {
      --curr_size_;
    }
  }
}

void LRUKReplacer::Remove(const frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < replacer_size_, "invalid frame id!");
  std::lock_guard lg(latch_);
  BUSTUB_ASSERT(node_store_[frame_id].Evictable(), "try to evict a non-evictable frame");
  if (node_store_[frame_id].Valid()) {
    node_store_[frame_id].SetInvalid();
    node_heap_.Remove(frame_id);
    --curr_size_;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
