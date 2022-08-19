//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // Find leaf page who cover the key
  auto *node = FindLeafPage(key, false, Operation::READONLY, transaction);
  auto *leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node->GetData());
  bool ret = false;
  if (leaf != nullptr) {
    ValueType value;
    // Find Key-Value in leaf page
    if (leaf->Lookup(key, value, comparator_)) {
      result->push_back(value);
      ret = true;
    }
    UnlockUnpinPages(Operation::READONLY, transaction);
    // if not in transaction, unlock R at end
    if (transaction == nullptr) {
      auto page_id = leaf->GetPageId();

      buffer_pool_manager_->FetchPage(page_id)->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, false);  // is_dirty = false
    }
  }
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  {
    // Create root page when first insert
    std::lock_guard<std::mutex> lock(mutex_);
    if (IsEmpty()) {
      StartNewTree(key, value);
      return true;
    }
  }

  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while StartNewTree");
  }
  auto *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  // We should update head page after root page changed
  UpdateRootPageId(true);
  // Init new page
  root->Init(root_page_id_, INVALID_PAGE_ID);
  root->Insert(key, value, comparator_);

  // Unpin root
  buffer_pool_manager_->UnpinPage(root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // Find leaf page who cover the key
  auto *node = FindLeafPage(key, false, Operation::INSERT, transaction);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node->GetData());
  if (leaf == nullptr) {
    return false;
  }
  // Already exists the key
  ValueType v;
  if (leaf->Lookup(key, v, comparator_)) {
    UnlockUnpinPages(Operation::INSERT, transaction);
    return false;
  }

  if (leaf->GetSize() < leaf_max_size_) {
    // Enough space in leaf page
    leaf->Insert(key, value, comparator_);
  } else {
    // Not enough
    // We split node, and create new leaf page
    int mark = 0;
    if (comparator_(key, leaf->KeyAt(leaf_max_size_ / 2)) > 0) {
      // Key-Value should be inserted into new leaf page
      mark = 1;
    }
    // Balance new leaf page size by mark
    auto *new_leaf = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(leaf, mark);
    if (mark == 0) {
      leaf->Insert(key, value, comparator_);
    } else {
      new_leaf->Insert(key, value, comparator_);
    }

    // Fix next page id
    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_leaf->GetPageId());

    // We should add new leaf page to parent page
    InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
  }

  UnlockUnpinPages(Operation::INSERT, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, int mark) {
  page_id_t page_id;
  // Create new page for node
  auto *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while Split");
  }

  auto *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(page_id);

  // Split node and move last half Key-Value to new page
  node->MoveHalfTo(new_node, mark, buffer_pool_manager_);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // If we split root page
  if (old_node->IsRootPage()) {
    // Create a new page as root page
    auto *page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while InsertIntoParent");
    }
    assert(page->GetPinCount() == 1);
    // New root page should be internal page
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());
    root->Init(root_page_id_);
    // Root page shuold be populated by old page and new page
    // In other words, root page have two child page
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // update to new 'root_page_id'
    UpdateRootPageId(false);

    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // is_dirty = true

    // parent is done
    buffer_pool_manager_->UnpinPage(root->GetPageId(), true);

  } else {
    // Split not in root page
    // Get parent page
    auto *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while InsertIntoParent");
    }
    auto *internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

    if (internal->GetSize() < internal_max_size_) {
      // Enough space
      // Parent page have a new child
      internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

      new_node->SetParentPageId(internal->GetPageId());

      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);  // is_dirty = true
    } else {
      // Not enough space
      // We should split parent page
      int mark = 0;
      if (comparator_(key, internal->KeyAt(internal_max_size_ / 2)) > 0) {
        mark = 1;
      }

      auto new_internal = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(
          internal, mark);  // Balance new internal page size by mark

      if (mark) {
        new_internal->Insert(key, new_node->GetPageId(), comparator_);
      } else {
        internal->Insert(key, new_node->GetPageId(), comparator_);
      }

      // Reference: https://zhuanlan.zhihu.com/p/149287061
      // Set new node's parent page id
      if (comparator_(key, new_internal->KeyAt(0)) < 0) {
        new_node->SetParentPageId(internal->GetPageId());
      } else if (comparator_(key, new_internal->KeyAt(0)) == 0) {
        new_node->SetParentPageId(new_internal->GetPageId());
      } else {
        new_node->SetParentPageId(new_internal->GetPageId());
        old_node->SetParentPageId(new_internal->GetPageId());
      }

      // new_node is done, unpin it
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

      // Recuesive
      InsertIntoParent(internal, new_internal->KeyAt(0), new_internal);
    }
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  // Find leaf page who cover the key
  auto *node = FindLeafPage(key, false, Operation::DELETE, transaction);
  auto leaf = reinterpret_cast<LeafPage *>(node->GetData());

  if (leaf != nullptr) {
    int size_before_deletion = leaf->GetSize();
    // Remove Key-Value in leaf page
    if (leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion) {
      if (CoalesceOrRedistribute(leaf, transaction)) {
        if (transaction != nullptr) {
          transaction->AddIntoDeletedPageSet(leaf->GetPageId());
        }
      }
    }
    UnlockUnpinPages(Operation::DELETE, transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  int max_size = 0;
  // Still follow the rule
  if (node->IsLeafPage()) {
    max_size = leaf_max_size_;
    // Question: Why leaf page use >= but internal page use >?
    if (node->GetSize() >= (leaf_max_size_ + 1) / 2) {
      return false;
    }
  } else {
    max_size = internal_max_size_;
    if (node->GetSize() > internal_max_size_ / 2) {
      return false;
    }
  }

  // Get parent page
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while CoalesceOrRedistribute");
  }
  auto parent = reinterpret_cast<InternalPage *>(page->GetData());

  // Find sibling nodes and try to find the previous (predecessor node) if possible
  // Get the index of node in parent
  int value_index = parent->ValueIndex(node->GetPageId());
  int sibling_page_id = INVALID_PAGE_ID;
  if (value_index == 0) {
    // Don't have predecessor, so we get successor
    sibling_page_id = parent->ValueAt(value_index + 1);
  } else {
    // Get predecessor if possible
    sibling_page_id = parent->ValueAt(value_index - 1);
  }

  page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while CoalesceOrRedistribute");
  }

  // Lock Write in sibling page
  page->WLatch();
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  auto sibling = reinterpret_cast<N *>(page->GetData());
  bool redistribute = false;

  // Whether to merge or redistribute according to the sum of the sizes of the two nodes
  if (sibling->GetSize() + node->GetSize() > max_size) {
    // No enough space
    redistribute = true;
    // Unpin parent
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);  // is_dirty = true
  }

  // No enough space, so we redistribute Key-Value pair from sibling to node
  if (redistribute) {
    if (value_index == 0) {
      Redistribute<N>(sibling, node, 0);
    } else {
      Redistribute<N>(sibling, node, 1);
    }
    return false;  // Node page should not be deleted
  }

  // Enough space, so we merge two node
  bool ret = false;
  if (value_index == 0) {
    // Sibling is successor of node (node -> sibling)
    // We delete sibling
    Coalesce<N>(node, sibling, parent, 1, transaction);
    // Save sibling into transaction
    if (transaction != nullptr) {
      transaction->AddIntoDeletedPageSet(sibling_page_id);
    }
    ret = false;
  } else {
    // Sibling is predecessor of node (sibling -> node)
    // We delete node
    Coalesce<N>(sibling, node, parent, value_index, transaction);
    ret = true;
  }

  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return ret;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  assert(neighbor_node->GetParentPageId() == node->GetParentPageId());
  // Neighbbor_node is predecessor of node (neighbbor_node -> node)
  // We should delete node
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);

  // Remove node's Key-Value information in parent node
  parent->Remove(index);

  // Key-Value pair is deleted from parent node, so we should recursive for parent node
  if (CoalesceOrRedistribute(parent, transaction)) {
    if (transaction != nullptr) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
    }
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // Four cases for redistribute

  assert(neighbor_node->GetParentPageId() == node->GetParentPageId());
  // Get parent page
  auto *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while MoveFirstToEndOf");
  }
  auto parent = reinterpret_cast<InternalPage *>(page->GetData());

  if (index == 0) {
    // Neighbor is successor of node (node -> neighbor)
    if (node->IsLeafPage()) {
      // Move Neighbor page's first Key-Value pair into end of node
      neighbor_node->MoveFirstToEndOf(node);
      // Update parent node Key-Value pair
      KeyType key = neighbor_node->KeyAt(0);
      parent->SetKeyAt(parent->ValueIndex(neighbor_node->GetPageId()), key);
    } else {
      // Internal node
      // Get middle key from parent node
      KeyType key;
      int index = parent->ValueIndex(neighbor_node->GetPageId());
      key = parent->KeyAt(index);
      neighbor_node->MoveFirstToEndOf(node, key, buffer_pool_manager_);
      // Update parent node Key-Value pair
      parent->SetKeyAt(index, neighbor_node->KeyAt(0));
    }
  } else {
    // Neighbor is predecessor of node (neighbor -> node)
    if (node->IsLeafPage()) {
      // Move neighbor page's last Key-Value pair into head of node
      neighbor_node->MoveLastToFrontOf(node);
      // Update parent node Key-Value pair
      KeyType key = node->KeyAt(0);
      parent->SetKeyAt(parent->ValueIndex(node->GetPageId()), key);
    } else {
      // Get middle key from parent node
      KeyType key;
      int index = parent->ValueIndex(node->GetPageId());
      key = parent->KeyAt(index);
      neighbor_node->MoveLastToFrontOf(node, key, buffer_pool_manager_);
      // Update parent node Key-Value pair
      parent->SetKeyAt(index, node->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // If root node is leaf page, it means we only have one leaf page
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      // We should remove root page as it's empty
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }
    return false;
  }
  // Root node is internal page and it only has one node
  if (old_root_node->GetSize() == 1) {
    // We should adjust, because root node is not necessary
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);

    // Tips: Old root page will be unpined by caller function
    // Question: When does old root page be removed from disk ?

    // Child page should become new root page
    root_page_id_ = root->ValueAt(0);
    // Update hander_page
    UpdateRootPageId(false);

    // Update new root page parent id
    auto *page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while AdjustRoot");
    }
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page->GetData());

    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);  // is_dirty = true
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType key{};
  Page *page = FindLeafPage(key, true);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto *page = FindLeafPage(key, false);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  int index = 0;
  if (leaf != nullptr) {
    index = leaf->KeyIndex(key, comparator_);
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  KeyType key{};
  Page *page = FindLeafPage(key, true);
  auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  while (leaf->GetNextPageId() != INVALID_PAGE_ID) {
    page_id_t next_page_id = leaf->GetNextPageId();
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(next_page_id);
    page->RLatch();
    leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  }
  return IndexIterator<KeyType, ValueType, KeyComparator>(leaf, leaf->GetSize(), buffer_pool_manager_);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Operation op, Transaction *transaction) {
  if (transaction == nullptr) {
    return;
  }

  // Unlock and unpin all parent page in the path
  for (auto *page : *transaction->GetPageSet()) {
    if (op == Operation::READONLY) {
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();

  // Delete page
  for (auto page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();

  // Unlock root_page_id_
  if (root_is_locked_) {
    root_is_locked_ = false;
    unlockRoot();
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::isSafe(N *node, Operation op) {
  int max_size = 0;
  if (node->IsLeafPage()) {
    max_size = leaf_max_size_;
  } else {
    max_size = internal_max_size_;
  }
  int min_size = max_size >> 1;

  if (op == Operation::INSERT) {
    return node->GetSize() < max_size;
  } else if (op == Operation::DELETE) {
    // >=: keep same with `coalesce logic`
    return node->GetSize() > min_size + 1;
  }
  return true;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Operation op, Transaction *transaction) {
  // Lock root_page_id_ when insert or delete
  if (op != Operation::READONLY) {
    lockRoot();
    root_is_locked_ = true;
  }

  if (IsEmpty()) {
    return nullptr;
  }
  // Begin from root page
  auto *parent = buffer_pool_manager_->FetchPage(root_page_id_);
  if (parent == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while FindLeafPage");
  }
  // Lock root page
  if (op == Operation::READONLY) {
    parent->RLatch();
  } else {
    parent->WLatch();
  }

  // Save root page in transaction
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(parent);
  }

  // Search B plus tree until the leaf node who cover the key
  auto *node = reinterpret_cast<BPlusTreePage *>(parent->GetData());
  Page *child = parent;
  while (!node->IsLeafPage()) {
    auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    page_id_t child_page_id, parent_page_id = node->GetPageId();
    // Find next page
    if (leftMost) {
      child_page_id = internal->ValueAt(0);
    } else {
      child_page_id = internal->Lookup(key, comparator_);
    }

    child = buffer_pool_manager_->FetchPage(child_page_id);
    if (child == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "all page are pinned while FindLeafPage");
    }

    if (op == Operation::READONLY) {
      // Lock Read in this page
      child->RLatch();
      // Unlock Read in parent page
      UnlockUnpinPages(op, transaction);
    } else {
      // Lock Write in this page
      child->WLatch();
    }
    // Deep search
    node = reinterpret_cast<BPlusTreePage *>(child->GetData());  // RTTI
    assert(node->GetParentPageId() == parent_page_id);
    // Check child page safety or not
    if (op != Operation::READONLY && isSafe(node, op)) {
      // Safety means we don't have to split the node
      // We should Unlock Write in all parent page
      UnlockUnpinPages(op, transaction);
    }
    // Save all path page in transaction
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(child);
    } else {
      parent->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      parent = child;
    }
  }
  return child;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
