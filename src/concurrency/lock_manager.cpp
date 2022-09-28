//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <stack>
#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

void LockManager::AbortImplicitly(Transaction *txn, AbortReason abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // To imple read_uncommitted, only lock exclusive when necessary
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    AbortImplicitly(txn, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // To imple repeatable_read, using Two-phase Locking (2PL) and wait-die
  // 2PL limits, during shrinking, can not lock anymore
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // Already in shared lock
  if (txn->IsSharedLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];

  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::SHARED);

  // wait
  lock_request_queue.cv_.wait(l, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  // Cycle detection thread abort this transaction
  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // grant
  lock_request.granted_ = true;

  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->emplace(rid);

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];

  auto &lock_request =
      lock_request_queue.request_queue_.emplace_back(txn->GetTransactionId(), LockManager::LockMode::EXCLUSIVE);

  // wait
  lock_request_queue.cv_.wait(l, [&lock_request_queue, &lock_request, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // grant
  lock_request.granted_ = true;

  txn->SetState(TransactionState::GROWING);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::SHRINKING) {
    AbortImplicitly(txn, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }

  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];

  if (lock_request_queue.upgrading_) {
    AbortImplicitly(txn, AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  lock_request_queue.upgrading_ = true;
  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  BUSTUB_ASSERT(it != lock_request_queue.request_queue_.end(), "Cannot find lock request when upgrade lock");
  BUSTUB_ASSERT(it->granted_, "Lock request has not be granted");
  BUSTUB_ASSERT(it->lock_mode_ == LockManager::LockMode::SHARED, "Lock request is not locked in SHARED mode");

  BUSTUB_ASSERT(txn->IsSharedLocked(rid), "Rid is not shared locked by transaction when upgrade");
  BUSTUB_ASSERT(!txn->IsExclusiveLocked(rid), "Rid is currently exclusive locked by transaction when upgrade");

  it->lock_mode_ = LockManager::LockMode::EXCLUSIVE;
  it->granted_ = false;
  // wait
  lock_request_queue.cv_.wait(l, [&lock_request_queue, &lock_request = *it, &txn] {
    return LockManager::IsLockCompatible(lock_request_queue, lock_request) ||
           txn->GetState() == TransactionState::ABORTED;
  });

  if (txn->GetState() == TransactionState::ABORTED) {
    AbortImplicitly(txn, AbortReason::DEADLOCK);
    return false;
  }

  // grant
  it->granted_ = true;
  lock_request_queue.upgrading_ = false;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }
  std::unique_lock<std::mutex> l(latch_);
  auto &lock_request_queue = lock_table_[rid];

  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  auto it = std::find_if(
      lock_request_queue.request_queue_.begin(), lock_request_queue.request_queue_.end(),
      [&txn](const LockManager::LockRequest &lock_request) { return txn->GetTransactionId() == lock_request.txn_id_; });
  BUSTUB_ASSERT(it != lock_request_queue.request_queue_.end(), "Cannot find lock request when unlock");

  // deletes the record for that data item in the linked list corresponding to that transaction
  auto following_it = lock_request_queue.request_queue_.erase(it);

  // tests the record that follows, to see if that request can now be granted
  if (following_it != lock_request_queue.request_queue_.end() && !following_it->granted_ &&
      LockManager::IsLockCompatible(lock_request_queue, *following_it)) {
    lock_request_queue.cv_.notify_all();
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  auto &v = waits_for_[t1];
  auto it = std::lower_bound(v.begin(), v.end(), t2);

  // t2 already in
  if (it != v.end() && *it == t2) {
    return;
  }

  v.insert(it, t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto &v = waits_for_[t1];
  auto it = std::find(v.begin(), v.end(), t2);

  if (it != v.end()) {
    v.erase(it);
  }
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> res;
  for (const auto &[txn_id, txn_id_v] : waits_for_) {
    std::transform(txn_id_v.begin(), txn_id_v.end(), std::back_inserter(res),
                   [&t1 = txn_id](const auto &t2) { return std::make_pair(t1, t2); });
  }
  return res;
}

bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::vector<txn_id_t> vertices;  // all nodes
  std::transform(waits_for_.begin(), waits_for_.end(), std::back_inserter(vertices),
                 [](const auto &pair) { return pair.first; });
  std::sort(vertices.begin(), vertices.end());

  // Recording node we visited
  std::unordered_map<txn_id_t, LockManager::VisitedType> visited;

  // Round-robin
  for (auto &&v : vertices) {
    // vertex is NOT_VISITED
    if (auto it = visited.find(v); it == visited.end()) {
      std::stack<txn_id_t> path;
      path.push(v);
      visited.emplace(v, LockManager::VisitedType::IN_STACK);

      auto has_cycle = ProcessDFSTree(txn_id, &path, &visited);
      if (has_cycle) {
        return true;
      }
    }
  }

  return false;
}

bool LockManager::ProcessDFSTree(txn_id_t *txn_id, std::stack<txn_id_t> *path,
                                 std::unordered_map<txn_id_t, LockManager::VisitedType> *visited) {
  bool has_cycle = false;

  // from: path->top()
  // to  : v
  for (auto &&v : waits_for_[path->top()]) {
    auto it = visited->find(v);

    // v already in visited, find a cycle
    if (it != visited->end() && it->second == LockManager::VisitedType::IN_STACK) {
      *txn_id = GetYoungestTransactionInCycle(path, v);
      has_cycle = true;
      break;
    }

    // v is NOT_VISITED
    if (it == visited->end()) {
      path->push(v);
      visited->emplace(v, LockManager::VisitedType::IN_STACK);

      has_cycle = ProcessDFSTree(txn_id, path, visited);
      if (has_cycle) {
        break;
      }
    }
  }

  // from: path->top() handled，we must handle next node
  visited->insert_or_assign(path->top(), LockManager::VisitedType::VISITED);
  path->pop();

  return has_cycle;
}

txn_id_t LockManager::GetYoungestTransactionInCycle(std::stack<txn_id_t> *path, txn_id_t vertex) {
  txn_id_t max_txn_id = 0;
  std::stack<txn_id_t> tmp;
  tmp.push(path->top());
  path->pop();

  // Get cycle path
  while (tmp.top() != vertex) {
    tmp.push(path->top());
    path->pop();
  }
  // Find Youngest Transaction and recovery path
  while (!tmp.empty()) {
    max_txn_id = std::max(max_txn_id, tmp.top());
    path->push(tmp.top());
    tmp.pop();
  }

  return max_txn_id;
}

void LockManager::BuildWaitsForGraph() {
  for (const auto &it : lock_table_) {
    const auto queue = it.second.request_queue_;
    std::vector<txn_id_t> holdings;
    std::vector<txn_id_t> waitings;

    for (const auto &lock_request : queue) {
      const auto txn = TransactionManager::GetTransaction(lock_request.txn_id_);
      // Ignore aborted transaction
      if (txn->GetState() == TransactionState::ABORTED) {
        continue;
      }

      if (lock_request.granted_) {
        holdings.push_back(lock_request.txn_id_);
      } else {
        waitings.push_back(lock_request.txn_id_);
      }
    }
    // Build directed graph
    for (auto &&t1 : waitings) {
      for (auto &&t2 : holdings) {
        AddEdge(t1, t2);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      if (!enable_cycle_detection_) {
        break;
      }

      waits_for_.clear();
      BuildWaitsForGraph();

      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        auto txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);

        for (const auto &wait_on_txn_id : waits_for_[txn_id]) {
          auto wait_on_txn = TransactionManager::GetTransaction(wait_on_txn_id);
          std::unordered_set<RID> lock_set;
          lock_set.insert(wait_on_txn->GetSharedLockSet()->begin(), wait_on_txn->GetSharedLockSet()->end());
          lock_set.insert(wait_on_txn->GetExclusiveLockSet()->begin(), wait_on_txn->GetExclusiveLockSet()->end());
          for (auto locked_rid : lock_set) {
            lock_table_[locked_rid].cv_.notify_all();
          }
        }

        waits_for_.clear();
        BuildWaitsForGraph();
      }
    }
  }
}

}  // namespace bustub
