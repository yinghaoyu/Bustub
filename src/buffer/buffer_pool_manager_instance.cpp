//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

#include "common/logger.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].is_dirty_ = false;
    pages_[i].pin_count_ = 0;
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock<std::mutex> scoped_latch(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  FlushPg(page_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock<std::mutex> guard(latch_);
  for (const auto &entry : page_table_) {
    FlushPgImp(entry.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock<std::mutex> scoped_latch(latch_);

  auto frame_id = FindAvailablePg();  // 这里包含对淘汰页的写磁盘操作
  if (frame_id == -1) {
    // buffer pool所有的页都在使用中
    return nullptr;
  }

  // 分配新的内存页
  pages_[frame_id].page_id_ = AllocatePage();
  pages_[frame_id].pin_count_++;
  page_table_[pages_[frame_id].page_id_] = frame_id;

  // 初始化页面数据
  *page_id = pages_[frame_id].GetPageId();
  memset(pages_[frame_id].GetData(), 0, PAGE_SIZE);
  return &pages_[frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock<std::mutex> scoped_latch(latch_);

  // 该页在buffer pool中
  frame_id_t frame_id = FindPage(page_id);
  if (frame_id != -1) {
    pages_[frame_id].pin_count_++;  // pin the page
    replacer_->Pin(frame_id);       // notify replacer
    pages_[frame_id].is_dirty_ = true;
    return &pages_[frame_id];
  }

  // 该页不在缓冲区中，执行LRU替换页
  frame_id_t replace_frame_id = FindAvailablePg();
  if (replace_frame_id == -1) {
    return nullptr;
  }
  // 判断换出的页是否需要写磁盘
  FlushPg(pages_[replace_frame_id].page_id_);
  // 刷新页面映射
  page_table_.erase(pages_[replace_frame_id].page_id_);
  frame_id = replace_frame_id;      // 为了便于理解
  page_table_[page_id] = frame_id;  // page_id ---> frame_id
  // 刷新页面信息
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_++;
  pages_[frame_id].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  replacer_->Pin(frame_id);

  return &pages_[frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock<std::mutex> scoped_latch(latch_);
  DeallocatePage(page_id);

  auto frame_id = FindPage(page_id);
  if (frame_id != -1) {
    if (pages_[frame_id].GetPinCount() != 0) {
      // 引用计数大于0，说明该页面被使用不能删除
      return false;
    }
    page_table_.erase(page_id);
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].is_dirty_ = false;
    memset(pages_[frame_id].GetData(), 0, PAGE_SIZE);
    free_list_.push_back(frame_id);
  }
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> scoped_latch(latch_);
  auto frame_id = FindPage(page_id);
  if (frame_id == -1 || pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  pages_[frame_id].is_dirty_ = is_dirty;
  if (--pages_[frame_id].pin_count_ == 0) {
    // 如果该页的引用计数为0，说明可以加入到LRU队列中，可以成为被替换的候选人
    replacer_->Unpin(frame_id);
    FlushPg(page_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

frame_id_t BufferPoolManagerInstance::FindPage(page_id_t page_id) {
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    return iter->second;
  }
  return -1;
}

void BufferPoolManagerInstance::FlushPg(page_id_t page_id) {
  auto frame_id = FindPage(page_id);
  if(frame_id == -1){
    LOG_INFO("page_id = %d, frame_id = %d", page_id, frame_id);
    return;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    pages_[frame_id].pin_count_ = 0;
  }
}

frame_id_t BufferPoolManagerInstance::FindAvailablePg() {
  frame_id_t ans;
  // 如果空闲链表非空，不需要进行替换算法，直接返回一个空闲的frame
  // 这个情况是buffer pool未满
  if (!free_list_.empty()) {
    ans = free_list_.front();
    free_list_.pop_front();
    return ans;
  }
  // 如果空闲链表为空，表示buffer pool已经满了，这个时候需要执行LRU算法
  if (replacer_->Victim(&ans)) {
    auto pageid = pages_[ans].page_id_;
    // 淘汰的页面需要根据脏位写磁盘
    FlushPg(pageid);
    page_table_.erase(pageid);
    return ans;
  }
  return -1;
}

}  // namespace bustub
