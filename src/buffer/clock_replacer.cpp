//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include "common/macros.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  for (size_t _ = 0; _ < num_pages; _++) {
    frames_.emplace_back(std::make_tuple(false, false));
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (Size() == 0) {
    return false;
  }

  std::lock_guard<std::shared_mutex> guard(mutex_);
  // Never busy loop
  while (true) {
    auto &[contains, ref] = frames_[next_];
    if (contains) {
      if (ref) {
        // Reference recently, victim it next time
        ref = false;
      } else {
        *frame_id = next_;
        contains = false;
        return true;
      }
    }
    ++next_;
    if (next_ >= static_cast<int32_t>(frames_.size())) {
      next_ = 0;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < frames_.size(), "frame_id overflow");
  std::lock_guard<std::shared_mutex> guard(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = false;
  ref = false;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  BUSTUB_ASSERT(static_cast<size_t>(frame_id) < frames_.size(), "frame_id overflow");
  std::lock_guard<std::shared_mutex> guard(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = true;
  ref = true;  // Reference recently
}

size_t ClockReplacer::Size() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  size_t size = 0;
  for (auto &[contains, ref] : frames_) {
    size += static_cast<size_t>(contains);
  }
  return size;
}

}  // namespace bustub
