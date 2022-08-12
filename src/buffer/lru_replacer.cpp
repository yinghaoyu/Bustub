//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

// Remove the object that was accessed the least recently compared to all the elements being tracked by the Replacer,
// store its contents in the output parameter and return True. If the Replacer is empty return False.
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock<std::mutex> guard(latch_);
  if (lru_map_.empty()) {
    return false;
  }
  frame_id_t last = lru_list_.back();
  lru_map_.erase(last);
  lru_list_.pop_back();
  *frame_id = last;
  return true;
}

// This method should be called after a page is pinned to a frame in the BufferPoolManager.
// It should remove the frame containing the pinned page from the LRUReplacer
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> guard(latch_);
  if (lru_map_.count(frame_id) != 0) {
    lru_list_.erase(lru_map_[frame_id]);
    lru_map_.erase(frame_id);
  }
}

// This method should be called when the pin_count of a page becomes 0. This method should add the frame containing the
// unpinned page to the LRUReplacer.
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> guard(latch_);
  if (lru_map_.count(frame_id) != 0) {
    return;
  }

  while (lru_list_.size() >= capacity_) {
    frame_id_t last = lru_list_.back();
    lru_list_.pop_back();
    lru_map_.erase(last);
  }

  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::Size() {
  std::scoped_lock<std::mutex> guard(latch_);
  return lru_list_.size();
}

}  // namespace bustub
