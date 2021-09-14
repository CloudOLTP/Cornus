#include <sched.h>
#include <iomanip>
#include "global.h"
#include "worker_thread.h"
#include "txn.h"
#include "txn_table.h"

WorkerThread::WorkerThread(uint64_t thd_id)
    : BaseThread(thd_id, WORKER_THREAD)
{
    _mutex = new pthread_mutex_t;
    _cond = new pthread_cond_t;
    pthread_mutex_init(_mutex, NULL);
    pthread_cond_init (_cond, NULL);
    _native_txn = NULL;
    _is_ready = true;
}

RC WorkerThread::run() {
    // calculate which client thread this worker thread corresponds to.
    uint64_t max_txn_id = 0;
    // Main loop
    while ( (get_sys_clock() - _init_time) < 10) {
        // txn_id format:
        //     | unique number | worker_thread_id | node_id |
        uint64_t txn_id = max_txn_id ++;
        txn_id = txn_id * g_total_num_threads + _thd_id;
        txn_id = txn_id * g_num_nodes + g_node_id;
        _native_txn = new TxnManager(this);
        _native_txn->set_txn_id( txn_id );
        txn_table->add_txn( _native_txn );
        // run
        _native_txn->start();
        // clean up current txn
        txn_table->remove_txn(_native_txn);
        delete _native_txn;
        _native_txn = NULL;
    }
    return RCOK;
}
