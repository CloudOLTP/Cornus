#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "manager.h"
#include "lock_manager.h"
#include "f1_manager.h"

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT

#if CC_ALG == WAIT_DIE
bool
Row_lock::Compare::operator() (const LockEntry &en1, const LockEntry &en2) const
{
    // returns true if the first argument goes before the second argument
    // begin(): txn with the smallest ts (oldest)
    // end(): txn with the largest ts (youngest)
    return LOCK_MAN(en1.txn)->get_ts() < LOCK_MAN(en2.txn)->get_ts();
}
#endif

Row_lock::Row_lock()
{
    _row = NULL;
    pthread_mutex_init(&_latch, NULL);
    //_lock_type = LOCK_NONE;
    _max_num_waits = g_max_num_waits;
    _upgrading_txn = NULL;
}

Row_lock::Row_lock(row_t * row)
    : Row_lock()
{
    _row = row;
}

void
Row_lock::latch()
{
    pthread_mutex_lock( &_latch );
}

void
Row_lock::unlatch()
{
    pthread_mutex_unlock( &_latch );
}

void
Row_lock::init(row_t * row)
{
    exit(0);
    _row = row;
    pthread_mutex_init(&_latch, NULL);
    _lock_type = LOCK_NONE;
    _max_num_waits = g_max_num_waits;
}

RC
Row_lock::lock_get(LockType type, TxnManager * txn, bool need_latch)
{
    RC rc = RCOK;
    if (need_latch) latch();
#if CC_ALG == NO_WAIT
    LockEntry * entry = nullptr;
    for (auto it = _locking_set.begin(); it != _locking_set.end(); it++) {
        if (it->txn == txn)
            entry = &(*it);
    }

    if (entry) { // the txn is already a lock owner
        // upgrade lock from share to exclusive
        if (entry->type != type) {
            assert(type == LOCK_EX && entry->type == LOCK_SH);
            if (_locking_set.size() == 1)
                entry->type = type;
            else
                rc = ABORT;
        }
    } else {
        if (!_locking_set.empty() && conflict_lock(type, _locking_set.begin()->type)) {
            // conflict with existing lock owner
            rc = ABORT;
        }
        else {
#if !EARLY_LOCK_RELEASE
            // add to owner
            _locking_set.push_back(LockEntry{type, txn});
#else
            bool isHead = true;
            // check weak queue, increment commit semaphore if has any writes
            for (auto it : _weak_locking_queue) {
                assert(it.txn != txn);
                if (it.type == LOCK_EX) {
                    uint32_t sem = txn->dependency_semaphore->incr();
                    isHead = false;
#if DEBUG_ELR
                    printf("[row_lock-%p] txn-%lu increase semaphore to %u, "
                           "type = %d, "
                           "due to (txn-%lu, type-%d), size=%zu\n", this,
                           txn->get_txn_id(), sem, type,
                           it.txn->get_txn_id(), it.type, _weak_locking_queue.size());
#endif
                    break;
                }
            }
            _locking_set.push_back(LockEntry{type, txn, isHead});
#endif // #if !EARLY_LOCK_RELEASE
        }
    }
#else // #if CC_ALG == WAIT_DIE
    /*LockEntry * entry = NULL;
    for (std::set<LockEntry>::iterator it = _locking_set.begin();
         it != _locking_set.end(); it ++) {
        if (it->txn == txn)
            entry = (Row_lock::LockEntry *)&(*it);
    }
    if (entry != NULL) { // This txn is already holding a lock.
        if (entry->type == type) {} // the txn request the same lock type again, ignore
        else {
            // The transaction must be upgrading a SH-lock to EX-lock
            assert(entry->type == LOCK_SH && type == LOCK_EX);
            for (std::set<LockEntry>::iterator it = _locking_set.begin();
                 it != _locking_set.end(); it ++) {
                // If another transaction is already waiting for upgrade, must
                // abort due to conflict.
                if (it->type == LOCK_UPGRADING)
                    rc = ABORT;
            }
            if (rc == RCOK) {
                // If the transaction is the only lock owner, acquire LOCK_EX
                if (_locking_set.size() == 1)
                    entry->type = LOCK_EX;
                else { // otherwise wait for other (SH) owners to release.
                    entry->type = LOCK_UPGRADING;
                    rc = WAIT;
                }
            }
        }
    } else { // This is a new transaction
        if (_locking_set.empty() || (!conflict_lock(type, _locking_set.begin()->type)
                                     && _waiting_set.empty())) {
            // no conflict, directly acquire the lock
            _locking_set.insert( LockEntry {type, txn} );
            //if (_row && txn->get_txn_id() <= 5)
            //    printf("txn %ld locks tuple %ld. type=%d\n",
            //           txn->get_txn_id(), _row->get_primary_key(), _locking_set.begin()->type);
        } else {
            // if the txn conflicts with an older txn, abort
            for (auto entry : _locking_set)
                if (LOCK_MAN(entry.txn)->get_ts() < LOCK_MAN(txn)->get_ts())
                    rc = ABORT;
            for (auto entry : _waiting_set)
                if (LOCK_MAN(entry.txn)->get_ts() < LOCK_MAN(txn)->get_ts()
                    && conflict_lock(type, entry.type))
                    rc = ABORT;
            // Otherwise can wait
            if (rc != ABORT) {
                //if (_row && txn->get_txn_id() <= 5)
                //    printf("--txn %ld waits for tuple %ld\n", txn->get_txn_id(), _row->get_primary_key());
                _waiting_set.insert( LockEntry {type, txn} );
                txn->_start_wait_time = get_sys_clock();
                rc = WAIT;
            } //else {
                //if (_row && txn->get_txn_id() <= 5)
                //    printf("--txn %ld (%ld) fails to lock/wait for tuple %ld\n",
                //           txn->get_txn_id(), LOCK_MAN(txn)->get_ts(), _row->get_primary_key());
            //}
        }
    }*/
#endif
    if (need_latch) unlatch();
    return rc;
}

RC
Row_lock::lock_release(TxnManager * txn, RC rc) {
    assert(rc == COMMIT || rc == ABORT);
    latch();
#if CC_ALG == NO_WAIT
#if !EARLY_LOCK_RELEASE
    LockEntry entry = {LOCK_NONE, nullptr};
    for (auto it = _locking_set.begin();
         it != _locking_set.end(); it++) {
        if (it->txn == txn) {
            entry = *it;
            assert(entry.txn);
            _locking_set.erase(it);
            break;
        }
    }
#else
#if DEBUG_ELR
    if (rc == COMMIT) {
      printf("[row_lock-%p] txn-%lu try to release its lock\n",
             this, txn->get_txn_id());
    }
#endif
#if DEBUG_ELR
    assert(_weak_locking_queue.empty() || _weak_locking_queue.front().isHead);
#endif
    LockEntry entry = {LOCK_NONE, nullptr, true};
    bool found_in_owner = false;
    if (rc == ABORT) {
        for (auto it = _locking_set.begin();
             it != _locking_set.end(); it++) {
            if (it->txn == txn) {
                entry = *it;
#if DEBUG_ELR
                assert(entry.txn);
#endif
                _locking_set.erase(it);
                found_in_owner = true;
                break;
            }
        }
    }
    // 2. try to remove from weak lock queue if not found in locking set
    //  case 1 - EX (to remove), EX
    //  case 2 - EX (to remove), SH
    //  case 3 - SH, SH (to remove), EX
    //  case 4 - SH, SH, EX (to remove), SH, EX
    bool decremented = false;
    bool found_in_weak = false;
    if (!found_in_owner) {
        for (size_t i = 0; i < _weak_locking_queue.size(); i++) {
            auto it = _weak_locking_queue[i];
            if (it.txn == txn) {
                found_in_weak = true;
                // found the entry to remove: i-th object
                // if releasing write lock, decrement dependency from i+1 until
                // encountering the first write lock (inclusive)
                if (it.type == LOCK_EX) {
                    if (!it.isHead) {
                        // not head, must due to abort, no need to decrement
#if DEBUG_ELR
                        assert(rc == ABORT);
#endif
                        decremented = true;
                        break;
                    }
                    for (size_t j = i + 1; j < _weak_locking_queue.size();
                         j++) {
                        uint32_t sem = _weak_locking_queue[j]
                          .txn->dependency_semaphore->decr();
                        _weak_locking_queue[j].isHead = true;
#if DEBUG_ELR
                        printf(
                            "[row_lock-%p] txn-%lu decrease semaphore to %u, "
                            "type = %d, due to (txn-%lu, type-%d) releases locks\n",
                            this,
                            _weak_locking_queue[j].txn->get_txn_id(),
                            _weak_locking_queue[j].type,
                            sem,
                            txn->get_txn_id(),
                            it.type);
#endif
                        if (_weak_locking_queue[j].type == LOCK_EX) {
                            decremented = true;
                            break;
                        }
                    }
                } else {
                    // no need to decrement for removing reads
                    decremented = true;
                }
                // remove from weak queue
                _weak_locking_queue.erase(
                    _weak_locking_queue.begin() + (int) i);
#if DEBUG_ELR
                if (rc == COMMIT) {
                  printf(
                      "[row_lock-%p] remove txn-%lu from weak queue (current "
                      "length = %zu)\n",
                      this, txn->get_txn_id(), _weak_locking_queue.size());
                }
#endif
                break;
            }
        }
    }
    // decrement dependency in owner until encounter EX
    if (found_in_weak && !decremented) {
        for (auto itr = _locking_set.begin(); itr != _locking_set.end();
        itr++) {
            uint32_t sem = itr->txn->dependency_semaphore->decr();
            itr->isHead = true;
#if DEBUG_ELR
            printf("[row_lock-%p] txn-%lu decrease semaphore to %u in owners, "
                   "type = "
                   "%d, isHead = %d, due to txn-%lu releases locks\n",
                   this, itr->txn->get_txn_id(), sem, itr->type, itr->isHead,
                   txn->get_txn_id());
            assert(_locking_set.begin()->isHead);
#endif
        }
    }
    assert(found_in_weak || found_in_owner);
#if DEBUG_ELR
    if (rc == COMMIT) {
      printf("[row_lock-%p] txn-%lu finish releasing lock\n",
             this, txn->get_txn_id());
    }
#endif
#endif
#else // CC_ALG == WAIT_DIE
    /*LockEntry entry {LOCK_NONE, NULL};
    // remove from locking set
    for (std::set<LockEntry>::iterator it = _locking_set.begin();
         it != _locking_set.end(); it ++)
        if (it->txn == txn) {
            entry = *it;
            _locking_set.erase(it);
            break;
        }

    #if CONTROLLED_LOCK_VIOLATION
    //if (_row && txn->get_txn_id() <= 5)
    //    printf("entry.txn=%lx, type=%d\n", (uint64_t)entry.txn, entry.type);
    if (rc == COMMIT) {
        assert(entry.txn);
        if (!_weak_locking_queue.empty())
            assert(_weak_locking_queue.front().type == LOCK_EX);
        if (!_weak_locking_queue.empty() || entry.type == LOCK_EX) {
            _weak_locking_queue.push_back( LockEntry {entry.type, txn} );
            // mark that the txn has one more unsolved dependency
            LockManager * lock_manager = (LockManager *) txn->get_cc_manager();
            lock_manager->increment_dependency();
            //if (_row)
            //    printf("txn %ld retires to weak queue. tuple=%ld\n", txn->get_txn_id(), _row->get_primary_key());
        }
    }
    #endif
    // remove from waiting set
    for (std::set<LockEntry>::iterator it = _waiting_set.begin();
         it != _waiting_set.end(); it ++)
        if (it->txn == txn) {
            _waiting_set.erase(it);
            break;
        }

    // try to upgrade LOCK_UPGRADING to LOCK_EX
    if (_locking_set.size() == 1 && _locking_set.begin()->type == LOCK_UPGRADING) {
        LockEntry * entry = (LockEntry *) &(*_locking_set.begin());
        entry->type = LOCK_EX;
        entry->txn->set_txn_ready(RCOK);
    }
    // try to move txns from waiting set to locking set
    bool done = false;
    while (!done) {
        std::set<LockEntry>::reverse_iterator rit = _waiting_set.rbegin();
        if (rit != _waiting_set.rend() &&
            (_locking_set.empty()
             || !conflict_lock(rit->type, _locking_set.begin()->type)))
        {
            _locking_set.insert( LockEntry {rit->type, rit->txn} );
            //if (_row && txn->get_txn_id() <= 5)
            //    printf("--txn %ld wakes up txn %ld\n", txn->get_txn_id(), rit->txn->get_txn_id());
            rit->txn->set_txn_ready(RCOK);
            //_waiting_set.erase( rit );
            _waiting_set.erase( --rit.base() );
        } else
            done = true;
    }*/
#endif
    unlatch();
    return RCOK;
}

#if EARLY_LOCK_RELEASE
RC
Row_lock::lock_retire(TxnManager * txn)
{
    // NOTE
    // entry.txn can be NULL. This happens because Row_lock manager locates in
    // each bucket of the hash index. Records with different keys may map to the
    // same bucket and therefore share the manager. They will all call lock_release()
    // during commit. Namely, one txn may call lock_release() multiple times on
    // the same Row_lock manager. For now, we simply ignore calls except the
    // first one.
    assert(CC_ALG == NO_WAIT);
    latch();
    // find the entry in locking set
    for (auto it = _locking_set.begin(); it != _locking_set.end(); it ++) {
        if (it->txn == txn) {
            auto entry = *it;
            assert(entry.txn);
            // move into weak queue
            _weak_locking_queue.push_back( LockEntry {entry.type, txn, entry
            .isHead} );
#if DEBUG_ELR
//            printf("[row_lock-%p] txn-%lu retire lock, type = %d, "
//                   "isHead = %d, queue_size = "
//                   "%zu\n", this, txn->get_txn_id(), entry.type,
//                   entry.isHead, _weak_locking_queue.size());
            assert(entry.isHead == _weak_locking_queue[_weak_locking_queue
            .size() - 1].isHead);
#endif
            _locking_set.erase(it);
            break;
        } // if (it->txn == txn
    }
#if DEBUG_ELR
    assert(_weak_locking_queue.empty() || _weak_locking_queue.front().isHead);
#endif
    unlatch();
    return RCOK;
}
#endif

bool Row_lock::conflict_lock(LockType l1, LockType l2)
{
    if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
    else if (l1 == LOCK_UPGRADING && l2 == LOCK_UPGRADING)
        return true;
    else
        return false;
}

#endif
