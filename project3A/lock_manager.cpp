/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
using namespace std;

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  return lockTemplate(txn,rid,LockMode::SHARED);
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  return lockTemplate(txn,rid,LockMode::EXCLUSIVE);
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  return lockTemplate(txn,rid,LockMode::UPGRADING);
}

bool LockManager::lockTemplate(Transaction *txn, const RID &rid, LockMode mode) {
  // step 1
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  unique_lock<mutex> tableLatch(mutex_);
  TxList &txList = lockTable_[rid];
  unique_lock<mutex> txListLatch(txList.mutex_);
  tableLatch.unlock();

  if (mode == LockMode::UPGRADING) {//step 2
    if (txList.hasUpgrading_) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    auto it = find_if(txList.locks_.begin(), txList.locks_.end(),
                      [txn](const TxItem &item) {return item.tid_ == txn->GetTransactionId();});
    if (it == txList.locks_.end() || it->mode_ != LockMode::SHARED || !it->granted_) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
    txList.locks_.erase(it);
    assert(txn->GetSharedLockSet()->erase(rid) == 1);
  }
  //step 3
  bool canGrant = txList.checkCanGrant(mode);
  txList.insert(txn,rid,mode,canGrant,&txListLatch);
  return true;
}


bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (strict_2PL_) {//step1
    if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  } else if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }
  unique_lock<mutex> tableLatch(mutex_);
  TxList &txList = lockTable_[rid];
  unique_lock<mutex> txListLatch(txList.mutex_);
  //step 2 remove txList and txn->lockset
  auto it = find_if(txList.locks_.begin(), txList.locks_.end(),
                    [txn](const TxItem &item) {return item.tid_ == txn->GetTransactionId();});
  assert(it != txList.locks_.end());
  auto lockSet = it->mode_ == LockMode::SHARED ? txn->GetSharedLockSet() : txn->GetExclusiveLockSet();
  assert(lockSet->erase(rid) == 1);
  txList.locks_.erase(it);
  if (txList.locks_.empty()) {
    lockTable_.erase(rid);
    return true;
  }
  tableLatch.unlock();
  //step 3 check can grant other
  for (auto &tx : txList.locks_) {
    if (tx.granted_)
      break;
    tx.Grant(); //grant blocking one
    if (tx.mode_ == LockMode::SHARED) {continue;}
    if (tx.mode_ == LockMode::UPGRADING) {
      txList.hasUpgrading_ = false;
      tx.mode_ = LockMode::EXCLUSIVE;
    }
    break;
  }
  return true;
}

} // namespace cmudb
