//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  // LOG_INFO("Create an leaf page, id: %d", GetPageId());
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, array_[i].first) <= 0) {
      return i;
    }
  }
  return GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  // If leaf page is empty or key is larger than all elements
  if (GetSize() == 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    array_[GetSize()] = {key, value};
    IncreaseSize(1);
    return GetSize();
  }

  // Move array element
  for (int i = GetSize(); i > 0; --i) {
    // Find first element less than the key
    if (comparator(key, array_[i - 1].first) > 0) {
      array_[i] = {key, value};
      break;
    }
    array_[i] = array_[i - 1];
  }

  // If key is smaller than all elements
  if (comparator(key, array_[0].first) < 0) {
    array_[0] = {key, value};
  }

  IncreaseSize(1);

  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, int mark,
                                            BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > 0);

  int half = 0;
  if (mark == 0) {
    half = (GetSize() + 1) >> 1;
  } else {
    half = GetSize() >> 1;
  }
  recipient->CopyNFrom(array_ + GetSize() - half, half);
  IncreaseSize(-1 * half);
  (void)buffer_pool_manager;
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  assert(IsLeafPage() && GetSize() + size <= GetMaxSize());
  auto start = GetSize();
  for (int i = 0; i < size; ++i) {
    array_[start + i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    return false;
  }
  // binary search
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + (high - low) / 2;
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
      value = array_[mid].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  if (GetSize() == 0 || comparator(key, KeyAt(0)) < 0 || comparator(key, KeyAt(GetSize() - 1)) > 0) {
    // The key is not in this leaf page
    return GetSize();
  }

  // binary search
  int low = 0, high = GetSize() - 1, mid;
  while (low <= high) {
    mid = low + ((high - low) >> 1);
    if (comparator(key, KeyAt(mid)) > 0) {
      low = mid + 1;
    } else if (comparator(key, KeyAt(mid)) < 0) {
      high = mid - 1;
    } else {
      // Find the key, and overwrite data forward
      std::memmove((void *)(array_ + mid), (void *)(array_ + mid + 1),
                   static_cast<size_t>((GetSize() - mid - 1) * sizeof(MappingType)));
      IncreaseSize(-1);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                           BufferPoolManager *buffer_pool_manager) {
  // Move all Key-Value to recipient
  recipient->CopyNFrom(array_, GetSize());
  IncreaseSize(-1 * GetSize());
  // Update next page id
  recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {
  MappingType pair = GetItem(0);
  memmove((void *)(array_), (void *)(array_ + 1), static_cast<size_t>(GetSize() * sizeof(MappingType)));
  IncreaseSize(-1);

  recipient->CopyLastFrom(pair);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  assert(GetSize() + 1 <= GetMaxSize());
  array_[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  IncreaseSize(-1);
  MappingType pair = array_[GetSize()];

  recipient->CopyFirstFrom(pair);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
  assert(GetSize() + 1 < GetMaxSize());

  std::memmove((void *)(array_ + 1), (void *)(array_), GetSize() * sizeof(MappingType));
  IncreaseSize(1);
  array_[0] = item;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
