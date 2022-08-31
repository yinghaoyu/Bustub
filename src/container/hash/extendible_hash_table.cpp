//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_value = Hash(key) & dir_page->GetGlobalDepthMask();
  return hash_value;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage *dir_page;
  // Avoid concurrency to create repeated directory page.
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (directory_page_id_ == INVALID_PAGE_ID) {
      // New page at first time
      LOG_DEBUG("create new directory, before %d", directory_page_id_);
      Page *page = AssertPage(buffer_pool_manager_->NewPage(&directory_page_id_));
      dir_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
      dir_page->SetPageId(directory_page_id_);
      assert(directory_page_id_ != INVALID_PAGE_ID);
      // LOG_DEBUG("create new directory %d", directory_page_id_);
      // Renew an initial bucket 0
      page_id_t bucket_page_id = INVALID_PAGE_ID;
      AssertPage(buffer_pool_manager_->NewPage(&bucket_page_id));
      dir_page->SetBucketPageId(0, bucket_page_id);
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
      assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true));
    }
  }

  // Fetch from buffer pool
  assert(directory_page_id_ != INVALID_PAGE_ID);
  dir_page = reinterpret_cast<HashTableDirectoryPage *>(AssertPage(buffer_pool_manager_->FetchPage(directory_page_id_))->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return AssertPage(buffer_pool_manager_->FetchPage(bucket_page_id));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::RetrieveBucket(Page *page) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *bucket_page = FetchBucketPage(bucket_page_id);

  bucket_page->RLatch();
  // LOG_DEBUG("Read %d", bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(bucket_page);
  bool ret = bucket->GetValue(key, comparator_, result);
  bucket_page->RUnlatch();

  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));

  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  Page *page = FetchBucketPage(bucket_page_id);
  page->WLatch();

  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(page);
  if (!bucket->IsFull()) {
    // Buctet not full, insert it directly
    bool ret = bucket->Insert(key, value, comparator_);
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    return ret;
  }

  // Split insert
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // We mark as ZeroBucket and OneBucket
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  int64_t zero_bucket_index = KeyToDirectoryIndex(key, dir_page);
  uint32_t zero_loacl_depth = dir_page->GetLocalDepth(zero_bucket_index);

  if (zero_loacl_depth >= MAX_BUCKET_DEPTH) {
    // Can not split
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    // LOG_DEBUG("Bucket is full and can not split.");
    // AutoMergeEmptyBucket();
    return false;
  }

  // Grow directory to twice
  if (zero_loacl_depth == dir_page->GetGlobalDepth()) {
    dir_page->IncrGlobalDepth();
  }

  page_id_t zero_bucket_page_id = KeyToPageId(key, dir_page);
  Page *zero_bucket_page = FetchBucketPage(zero_bucket_page_id);
  // Use write latch to avoid other write
  zero_bucket_page->WLatch();
  HASH_TABLE_BUCKET_TYPE *zero_bucket = RetrieveBucket(zero_bucket_page);

  std::vector<MappingType> key_value_copy = zero_bucket->GetKeyValueCopy();
  // Clear for redistribution
  zero_bucket->Clear();

  // Allocate new page for OneBucket
  page_id_t one_bucket_page_id;
  HASH_TABLE_BUCKET_TYPE *one_bucket = RetrieveBucket(AssertPage(buffer_pool_manager_->NewPage(&one_bucket_page_id)));

  // Increase local depth
  dir_page->IncrLocalDepth(zero_bucket_index);
  uint32_t one_bucket_index = dir_page->GetSplitImageIndex(zero_bucket_index);
  // OneBucket have the same local depth with ZeroBucket at first time
  dir_page->SetLocalDepth(one_bucket_index, dir_page->GetLocalDepth(zero_bucket_index));

  // New page for bucket, update directoy page
  dir_page->SetBucketPageId(one_bucket_index, one_bucket_page_id);

  // Update directory page, may be different bucket address point to the same bucket page
  uint32_t diff = 1 << dir_page->GetLocalDepth(zero_bucket_index);

  for (uint32_t i = zero_bucket_index; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, zero_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(zero_bucket_index));
  }
  for (uint32_t i = zero_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, zero_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(zero_bucket_index));
  }

  for (uint32_t i = one_bucket_index; i >= diff; i -= diff) {
    dir_page->SetBucketPageId(i, one_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(one_bucket_index));
  }
  for (uint32_t i = one_bucket_index; i < dir_page->Size(); i += diff) {
    dir_page->SetBucketPageId(i, one_bucket_page_id);
    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(one_bucket_index));
  }

  // Redistribute key-value pair
  uint32_t mask = dir_page->GetLocalDepthMask(zero_bucket_index);
  for (uint32_t i = 0; i < key_value_copy.size(); i++) {
    MappingType tmp = key_value_copy[i];
    uint32_t target_bucket_index = Hash(tmp.first) & mask;
    page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
    assert(target_bucket_page_id == zero_bucket_page_id || target_bucket_page_id == one_bucket_page_id);
    if (target_bucket_page_id == zero_bucket_page_id) {
      assert(zero_bucket->Insert(tmp.first, tmp.second, comparator_));
    } else {
      assert(one_bucket->Insert(tmp.first, tmp.second, comparator_));
    }
  }

  zero_bucket_page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(zero_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(one_bucket_page_id, true));

  //  dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();

  //  LOG_DEBUG("split finish");
  // re-insert original k-v
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  Page *page = FetchBucketPage(bucket_page_id);
  page->WLatch();
  HASH_TABLE_BUCKET_TYPE *bucket = RetrieveBucket(page);
  bool ret = bucket->Remove(key, value, comparator_);
  if (bucket->IsEmpty()) {
    // go merge
    page->WUnlatch();
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.RUnlock();
    Merge(transaction, bucket_index);
    return ret;
  }
  page->WUnlatch();
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, uint32_t target_bucket_index) {
  // LOG_DEBUG("start to merge");
  table_latch_.WLock();

  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  if (target_bucket_index >= dir_page->Size()) {
    // Invalid check
    table_latch_.WUnlock();
    return;
  }

  page_id_t target_bucket_page_id = dir_page->GetBucketPageId(target_bucket_index);
  // get image bucket
  uint32_t image_bucket_index = dir_page->GetSplitImageIndex(target_bucket_index);

  uint32_t local_depth = dir_page->GetLocalDepth(target_bucket_index);
  if (local_depth == 0) {
    // Can not merge because of depth is 0
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // Check the same local depth with split image
  if (local_depth != dir_page->GetLocalDepth(image_bucket_index)) {
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // Check empty target bucket index
  Page *target_page = FetchBucketPage(target_bucket_page_id);
  target_page->RLatch();
  HASH_TABLE_BUCKET_TYPE *target_bucket = RetrieveBucket(target_page);
  if (!target_bucket->IsEmpty()) {
    // Bucket is not empty
    target_page->RUnlatch();
    assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    table_latch_.WUnlock();
    return;
  }

  // Delete target bucket page
  target_page->RUnlatch();
  assert(buffer_pool_manager_->UnpinPage(target_bucket_page_id, false));
  assert(buffer_pool_manager_->DeletePage(target_bucket_page_id));

  page_id_t image_bucket_page_id = dir_page->GetBucketPageId(image_bucket_index);
  dir_page->SetBucketPageId(target_bucket_index, image_bucket_page_id);
  dir_page->DecrLocalDepth(target_bucket_index);
  dir_page->DecrLocalDepth(image_bucket_index);
  assert(dir_page->GetLocalDepth(target_bucket_index) == dir_page->GetLocalDepth(image_bucket_index));

  for (uint32_t i = 0; i < dir_page->Size(); i++) {
    if (dir_page->GetBucketPageId(i) == target_bucket_page_id || dir_page->GetBucketPageId(i) == image_bucket_page_id) {
      dir_page->SetBucketPageId(i, image_bucket_page_id);
      dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(target_bucket_index));
    }
  }

  // Shrink as much as possible
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  // dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
Page *ExtendibleHashTable<KeyType, ValueType, KeyComparator>::AssertPage(Page *page) {
  assert(page != nullptr);
  return page;
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
