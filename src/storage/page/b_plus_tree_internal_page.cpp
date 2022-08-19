//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  // LOG_INFO("Create an internal page, id: %d", GetPageId());
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  assert(GetSize() > 1);

  if (comparator(key, array_[1].first) < 0) {
    return array_[0].second;
  } else if (comparator(key, array_[GetSize() - 1].first) >= 0) {
    return array_[GetSize() - 1].second;
  }

  int left = 1, right = GetSize() - 1;
  while (left < right && left + 1 != right) {  // must contain (left + 1 != right)
    int mid = left + ((right - left) >> 1);
    if (comparator(key, array_[mid].first) < 0) {
      right = mid;  // we get mid rather than mid-1
    } else if (comparator(key, array_[mid].first) > 0) {
      left = mid;
    } else {
      return array_[mid].second;
    }
  }
  
  return array_[left].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // Must be new page
  assert(GetSize() == 0);
  array_[0].second = old_value;  // First node don't have a key
  array_[1].first = new_key;
  array_[1].second = new_value;
  IncreaseSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // assert(GetSize() < INTERNAL_PAGE_SIZE);
  // Move array element
  for (int i = GetSize(); i > 0; --i) {
    if (array_[i - 1].second == old_value) {
      array_[i] = {new_key, new_value};
      IncreaseSize(1);
      break;
    }
    array_[i] = array_[i - 1];
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                           const KeyComparator &comparator) {
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient, int mark,
                                                BufferPoolManager *buffer_pool_manager) {
  // Copy half according to mark
  int half = 0;
  if (mark == 0) {
    half = (GetSize() + 1) >> 1;
  } else {
    half = GetSize() >> 1;
  }
  recipient->CopyNFrom(array_ + GetSize() - half, half, buffer_pool_manager);

  IncreaseSize(-1 * half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // must be a new page
  assert(GetSize() + size <= GetMaxSize());
  int start = GetSize();
  for (int i = 0; i < size; ++i) {
    array_[start + i] = *items++;
    // Adopt them by changing their parent page id
    auto *page = buffer_pool_manager->FetchPage(ValueAt(start + i));
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while CopyLastFrom");
    }
    auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
    // Child page's parent id should be the received page
    child->SetParentPageId(GetPageId());

    assert(child->GetParentPageId() == GetPageId());
    // Don't forget to unpin child page, otherwise it would reside buffer pool
    buffer_pool_manager->UnpinPage(child->GetPageId(), true);
  }

  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(0 <= index && index < GetSize());
  // Overwrite from back
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  assert(GetSize() == 0);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // Update middle key
  SetKeyAt(0, middle_key);

  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);

  IncreaseSize(-1 * GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > 0);
  // Save middle_key
  MappingType pair{middle_key, ValueAt(0)};

  // Remove first Key-Value pair
  Remove(0);

  recipient->CopyLastFrom(pair, buffer_pool_manager);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int start = GetSize();
  assert(start + 1 <= GetMaxSize());
  // Copy pair
  array_[start] = pair;

  // Update the parent id of the child node
  auto *page = buffer_pool_manager->FetchPage(ValueAt(start));
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());

  assert(child->GetParentPageId() == GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() > 0);
  // Remove last Key-Value pair
  IncreaseSize(-1);
  MappingType pair = array_[GetSize()];

  // delegate
  // Set middle_key to recipient
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(pair, buffer_pool_manager);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());

  std::memmove((void *)(array_ + 1), (void *)(array_), GetSize() * sizeof(MappingType));
  array_[0] = pair;

  // update child parent page id
  auto *page = buffer_pool_manager->FetchPage(ValueAt(0));
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while CopyLastFrom");
  }
  auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());

  assert(child->GetParentPageId() == GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
