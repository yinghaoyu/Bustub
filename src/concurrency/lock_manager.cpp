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

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockPrepare(Transaction *txn, const RID &rid) {
  // According to Two-phase Locking (2PL)
  // If a transaction is during shrink, it could not lock anymore, unlock only
  // So it must be error, we abort the transaction
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
    lock_table_[rid].request_queue_.clear();
  }
  return true;
}

std::list<LockManager::LockRequest>::iterator LockManager::GetIterator(std::list<LockRequest> &request_queue,
                                                                       txn_id_t txn_id) {
  for (auto iter = request_queue.begin(); iter != request_queue.end(); ++iter) {
    if (iter->txn_id_ == txn_id) {
      return iter;
    }
  }
  return request_queue.end();
}

void LockManager::CheckAborted(Transaction *txn, LockRequestQueue *lock_queue) {
  // If a transaction is during abort, it means deadlock
  // We should remove it from the queue
  if (txn->GetState() == TransactionState::ABORTED) {
    auto iter = GetIterator(lock_queue->request_queue_, txn->GetTransactionId());
    lock_queue->request_queue_.erase(iter);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
}

// Acquire a lock on RID in shared mode
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  // If a transaction is read_uncommitted, we needn't lock
  // So it must be error, we abort the transaction
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }

  // We should prepare transaction
  if (!LockPrepare(txn, rid)) {
    return false;
  }

  // Insert the request into rid queue
  LockRequestQueue *lock_queue = &lock_table_[rid];
  lock_queue->request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));

  // If a transaction already in exclusive lock
  //                      and in upgrading lock (upgrading lock means turning shared lock to exclusive lock)
  // We should wait
  while (txn->GetState() != TransactionState::ABORTED && (lock_queue->writing_ || lock_queue->upgrading_)) {
    lock_queue->cv_.wait(lock);
  }

  // If a transaction aborted, it means deadlock, we should remove it from queue
  CheckAborted(txn, lock_queue);

  txn->GetSharedLockSet()->emplace(rid);  // Transaction acquire shared lock on rid

  auto iter = GetIterator(lock_queue->request_queue_, txn->GetTransactionId());
  iter->granted_ = true;

  lock_queue->shared_count_++;  // Shared lock increase 1

  return true;
}

// Acquire a lock on RID in exclusive mode
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (!LockPrepare(txn, rid)) {
    return false;
  }

  // Insert the request into rid queue
  LockRequestQueue *lock_queue = &lock_table_[rid];
  lock_queue->request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE));

  // If a transaction already in exclusive lock
  //                      and in shared lock
  // We should wait
  while (txn->GetState() != TransactionState::ABORTED && (lock_queue->writing_ || lock_queue->shared_count_ > 0)) {
    lock_queue->cv_.wait(lock);
  }

  CheckAborted(txn, lock_queue);

  txn->GetExclusiveLockSet()->emplace(rid);  // Transaction acquire exclusive lock on rid

  auto iter = GetIterator(lock_queue->request_queue_, txn->GetTransactionId());
  iter->granted_ = true;

  lock_queue->writing_ = true;  // Queue is in exclusive

  return true;
}

// Upgrade a lock from a shared lock to an exclusive lock
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  // If a transaction did not acquire a shared lock on rid, upgrade error
  if (!txn->IsSharedLocked(rid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_ON_UNSHARED);
    return false;
  }

  LockRequestQueue *lock_queue = &lock_table_[rid];

  if (lock_queue->upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  lock_queue->upgrading_ = true;
  // If a transaction already in exclusive lock
  //                      and in shared lock
  // We should wait
  while (txn->GetState() != TransactionState::ABORTED && (lock_queue->shared_count_ > 1 || lock_queue->writing_)) {
    lock_queue->cv_.wait(lock);
  }

  CheckAborted(txn, lock_queue);

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);

  auto iter = GetIterator(lock_queue->request_queue_, txn->GetTransactionId());
  iter->granted_ = true;
  iter->lock_mode_ = LockMode::EXCLUSIVE;  // Updating shared lock to exclusive lock

  lock_queue->upgrading_ = false;
  lock_queue->writing_ = true;
  lock_queue->shared_count_--;

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockRequestQueue *lock_queue = &lock_table_[rid];

  // If a transaction did not acquire lock on rid
  if (!txn->IsSharedLocked(rid) && !txn->IsExclusiveLocked(rid)) {
    return false;
  }

  auto iter = GetIterator(lock_queue->request_queue_, txn->GetTransactionId());
  LockMode mode = iter->lock_mode_;
  lock_queue->request_queue_.erase(iter);  // remove from the queue

  // When a transaction at read commited, unlock shared lock should not turn to shrinking
  if (!(mode == LockMode::SHARED && txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  if (mode == LockMode::SHARED) {
    txn->GetSharedLockSet()->erase(rid);
    if (--lock_queue->shared_count_ == 0) {
      lock_queue->cv_.notify_all();
    }
  } else {
    txn->GetExclusiveLockSet()->erase(rid);
    lock_queue->writing_ = false;
    lock_queue->cv_.notify_all();
  }
  return true;
}

// Directed edge
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].push_back(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto iter = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (iter != waits_for_[t1].end()) {
    waits_for_[t1].erase(iter);
  }
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  // Remove out edge
  waits_for_.erase(txn_id);
  // Remove in edge
  Transaction *txn = TransactionManager::GetTransaction(txn_id);

  for (RID const &lock_rid : *(txn->GetSharedLockSet())) {
    for (LockRequest const &lock_request : lock_table_[lock_rid].request_queue_) {
      if (!lock_request.granted_) {
        RemoveEdge(lock_request.txn_id_, txn_id);
      }
    }
  }

  for (RID const &lock_rid : *(txn->GetExclusiveLockSet())) {
    for (LockRequest const &lock_request : lock_table_[lock_rid].request_queue_) {
      if (!lock_request.granted_) {
        RemoveEdge(lock_request.txn_id_, txn_id);
      }
    }
  }
}

bool LockManager::IsCycle(txn_id_t txn_id, std::vector<txn_id_t>& path) {
  if(waits_for_.find(txn_id) == waits_for_.end())
  {
    // Not have out edge
    return false;
  }
  if(std::find(path.begin(), path.end(), txn_id) != path.end())
  {
    // Already in path
    return true;
  }
  path.push_back(txn_id);
  std::vector<txn_id_t> &end = waits_for_[txn_id];
  for(auto const &txn_id: end)
  {
    if(IsCycle(txn_id, path))
    {
      return true;
    }
  }
  return false;
}

// Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
bool LockManager::HasCycle(txn_id_t *txn_id) {
  std::vector<txn_id_t> path;
  txn_id_t max = INVALID_TXN_ID;
  for(auto entry: waits_for_)
  {
    txn_id_t cur = entry.first;
    if(IsCycle(cur, path))
    {
      // wait-died
      max = std::max(max, cur);
    }
    path.clear();
  }
  *txn_id = max;
  return max != INVALID_TXN_ID;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto const &pair : waits_for_) {
    auto t1 = pair.first;
    for (auto const &t2 : pair.second) {
      result.emplace_back(t1, t2);
    }
  }
  return result;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      // TODO(student): remove the continue and add your cycle detection and abort code here
      std::unique_lock<std::mutex> lock(latch_);
      // First build the entire graph
      for (auto const &pair : lock_table_) {
        for (auto const &lock_request_waiting : pair.second.request_queue_) {
          // If it is waiting, add a edge to each lock-granted txn
          if (lock_request_waiting.granted_) {
            continue;
          }
          require_record_[lock_request_waiting.txn_id_] = pair.first;
          for (auto const &lock_request_granted : pair.second.request_queue_) {
            if (!lock_request_granted.granted_) {
              continue;
            }
            AddEdge(lock_request_waiting.txn_id_, lock_request_granted.txn_id_);
          }
        }
      }

      // Break every cycle
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        Transaction *txn = TransactionManager::GetTransaction(txn_id);
        txn->SetState(TransactionState::ABORTED);
        DeleteNode(txn_id);
        assert(lock_table_.find(require_record_[txn_id]) != lock_table_.end());
        lock_table_[require_record_[txn_id]].cv_.notify_all();
      }
      // clear internal data, next time we will construct them on-the-fly!
      waits_for_.clear();
      require_record_.clear();
    }
  }
}

}  // namespace bustub
