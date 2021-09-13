#pragma once

#include "global.h"
#include <queue>
#include <stack>
#include "thread.h"

class TxnManager;

class WorkerThread : public BaseThread {
public:
    WorkerThread(uint64_t thd_id);
    RC                      run();
    TxnManager *            get_native_txn() { return _native_txn; }
	uint64_t                get_execution_time() { return get_sys_clock() - _init_time;};
	uint64_t                get_init_time() { return _init_time;};
private:
    TxnManager *            _native_txn;
    bool                    _is_ready;
    pthread_cond_t *        _cond;
    pthread_mutex_t *       _mutex;
	uint64_t                _init_time;
};
