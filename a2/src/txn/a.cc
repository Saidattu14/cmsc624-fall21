#include "lock_manager.h"
#include <set>

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) { ready_txns_ = ready_txns; }

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
    
   // Implement this method!
    if(lock_table_.find(key) == lock_table_.end()) {
        LockRequest l(LockMode::EXCLUSIVE,txn);
        deque<LockRequest>* queue = new deque<LockRequest>();
        queue->push_back(l);
        lock_table_[key] = queue;
        return true;
    }
    else {
        if(txn_waits_.find(txn) == txn_waits_.end()) {
            txn_waits_[txn] = 1;
        } else {
            txn_waits_[txn] = txn_waits_[txn] + 1; 
        }
        LockRequest l(LockMode::EXCLUSIVE,txn);
        lock_table_[key]->push_back(l);
        return false;
    } 
    return true;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key)
{
    // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
    // simply use the same logic as 'WriteLock'.
    return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
    //
    // Implement this method!
    if(lock_table_.find(key) != lock_table_.end()) {
        Txn* tx1 = lock_table_[key]->front().txn_;
        if(tx1 == txn) {
            lock_table_[key]->pop_front();
            txn_waits_.erase(txn);
            if(lock_table_[key]->size() > 0) {
                Txn* txn1 = lock_table_[key]->front().txn_;
                txn_waits_[txn1] = txn_waits_[txn1] - 1;
                if(txn_waits_[txn1] == 0) {
                    ready_txns_->push_back(txn1);
                }
            }
        } else {
           deque<LockRequest>* queue = lock_table_[key];
           deque<LockRequest>::iterator it1;
           for(it1=queue->begin(); it1 != queue->end();++it1) {
              if(it1->txn_ == txn) {
                lock_table_[key]->erase(it1);
                break;
              }
            }
            txn_waits_.erase(txn);
        }
    }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners)
{
    //
    // Implement this method!
    if(lock_table_.find(key) == lock_table_.end()) {
        return LockMode::UNLOCKED;
    } else {
        owners->clear();
        deque<LockRequest>* queue = lock_table_[key];
        owners->push_back(queue->front().txn_);
        return LockMode::EXCLUSIVE;
    }
}

int LockManagerA::Get(Txn* txn) {
    return txn_waits_[txn];
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) { ready_txns_ = ready_txns; }

bool LockManagerB::WriteLock(Txn* txn, const Key& key)
{
    //
    // Implement this method!
     if(lock_table_.find(key) == lock_table_.end()) {
        LockRequest l(LockMode::EXCLUSIVE,txn);
        deque<LockRequest>* queue = new deque<LockRequest>();
        queue->push_back(l);
        lock_table_[key] = queue;
        return true;
    } else {
        if(txn_waits_.find(txn) == txn_waits_.end()) {
            txn_waits_[txn] = 1;
        } else {
            txn_waits_[txn]++; 
        }
        LockRequest l(LockMode::EXCLUSIVE,txn);
        lock_table_[key]->push_back(l);
        return false;
    }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key)
{
    //
    // Implement this method!
    if(lock_table_.find(key) == lock_table_.end()) {
        LockRequest l(LockMode::SHARED,txn);
        deque<LockRequest>* queue = new deque<LockRequest>();
        queue->push_back(l);
        lock_table_[key] = queue;
        return true;
    } else {
        deque<LockRequest>* queue = lock_table_[key];
        deque<LockRequest>::iterator it1;
        bool isContainsExclusiveLock = false;
        for(it1=queue->begin(); it1 != queue->end();++it1) {
            if(it1->mode_ == LockMode::EXCLUSIVE) {
               isContainsExclusiveLock = true;
               break;
            }
        }
        LockRequest l(LockMode::SHARED,txn);
        lock_table_[key]->push_back(l);
        if(isContainsExclusiveLock) {
            if(txn_waits_.find(txn) == txn_waits_.end()) {
                txn_waits_[txn] = 1;
            } else {
                txn_waits_[txn]++; 
            }
            return false;
        } else {
            return true;
        }
    }
}

void LockManagerB::Release(Txn* txn, const Key& key)
{
    //
    // Implement this method!
    if(lock_table_.find(key) != lock_table_.end()) {
        Txn* tx1 = lock_table_[key]->front().txn_;
        if(tx1 == txn) {
            lock_table_[key]->pop_front();
            if(lock_table_[key]->size() > 0) {
                if(lock_table_[key]->front().mode_ == LockMode::SHARED) {
                    deque<LockRequest>* queue = lock_table_[key];
                    deque<LockRequest>::iterator it1,it2;
                    for(it1=queue->begin(); it1 != queue->end();++it1) {
                        if(it1->mode_ == LockMode::SHARED) {
                            txn_waits_[it1->txn_] = txn_waits_[it1->txn_] - 1;
                            if(txn_waits_[it1->txn_] == 0) {
                                ready_txns_->push_back(it1->txn_);
                                txn_waits_.erase(it1->txn_);
                            }
                        } else {
                            break;
                        }
                    }
                } else {
                    Txn* tt = lock_table_[key]->front().txn_;
                    txn_waits_[tt] = txn_waits_[tt] - 1;
                    if(txn_waits_[tt] == 0) {
                        ready_txns_->push_back(tt);
                        txn_waits_.erase(tt);
                    }
                }
            }
        } else {
           deque<LockRequest>* queue = lock_table_[key];
           deque<LockRequest>::iterator it1,it2;
            for(it1=queue->begin(); it1 != queue->end();++it1) {
              if(it1->txn_ == txn) {
                lock_table_[key]->erase(it1);
                break;
              }
            }
            for(it2=queue->begin()+1;it2 != queue->end();++it2) {
                if(it2->mode_ == LockMode::SHARED) {
                    Txn* txn1 = it2->txn_;
                    txn_waits_[txn1] = txn_waits_[txn1] - 1;
                    if(txn_waits_[txn1] == 0) {
                        ready_txns_->push_back(txn1);
                        txn_waits_.erase(txn1);
                    }
                } else {
                    break;
                }
            }
        }
    }
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners)
{
    //
    // Implement this method!
    if(lock_table_.find(key) == lock_table_.end()) {
        return LockMode::UNLOCKED;
    } else {
        owners->clear();
        deque<LockRequest>* queue = lock_table_[key];
        if(queue->front().mode_ == LockMode::EXCLUSIVE) {
            owners->push_back(queue->front().txn_);
            return LockMode::EXCLUSIVE;
        } else {
            deque<LockRequest>::iterator it1;
            for(it1=queue->begin(); it1 != queue->end();++it1) {
                if(it1->mode_ == LockMode::SHARED) {
                    owners->push_back(it1->txn_);
                } else {
                    break;
                }
            }
            return LockMode::SHARED;
        }    
    }
}

int LockManagerB::Get(Txn* txn) {
    return txn_waits_[txn];
}
