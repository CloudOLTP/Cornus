#include "row.h"
#include "txn.h"
#include "row_occ.h"
#include "manager.h"
#include "occ_manager.h"

#if CC_ALG == OCC

Row_occ::Row_occ()
{
    _row = NULL;
    _version = 0;
    pthread_mutex_init(&_latch, NULL);
}

Row_occ::Row_occ(row_t * row)
    : Row_occ()
{
    _row = row;
    _version = 0;
}

void
Row_occ::init(row_t * row)
{
    exit(0);
    _row = row;
    _version = 0;
}

void
Row_occ::latch()
{
    pthread_mutex_lock( &_latch );
}

void
Row_occ::unlatch()
{
    pthread_mutex_unlock( &_latch );
}

RC
Row_occ::get_version_if_unlocked(uint64_t &version) {
    version = get_version();
    if (is_locked(version)) {
        return ABORT;
    } else {
        return RCOK;
    }
}

RC
Row_occ::lock_get(TxnManager * txn)
{
    uint64_t version;
    if (get_version_if_unlocked(version) == ABORT) {
#if DEBUG_PRINT
      printf("[node-%u, txn-%lu] failed acquiring lock on %p since locked.\n",
             g_node_id, txn->get_txn_id(), _row);
#endif
        return ABORT;
    }
    // TODO: get txn, skip if same txn
    // try to acquire lock
    if (_version.compare_exchange_strong(version, version + 1)) {
#if DEBUG_PRINT
      printf("[node-%u, txn-%lu] acquired lock on %p.\n",
             g_node_id, txn->get_txn_id(), _row);
#endif
        return RCOK;
    }
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] failed acquiring lock on %p.\n",
           g_node_id, txn->get_txn_id(), _row);
#endif
    return ABORT;
}

RC
Row_occ::lock_release(TxnManager * txn, RC rc) {
    assert(rc == COMMIT || rc == ABORT);
    uint64_t version;
    // release lock if locked (in case the lock is never acquired)
    if (get_version_if_unlocked(version) == ABORT) {
        // if locked, release the lock
        _version.compare_exchange_strong(version, version - 1);
    } // otherwise, lock is already released.
    return RCOK;
}


#endif
