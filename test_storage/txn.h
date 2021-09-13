#pragma once

#include "global.h"
#include "helper.h"

#include "rpc_client.h"
#include "semaphore_sync.h"
#include <pthread.h>

class TxnManager
{
public:
    enum State {
        RUNNING,
        PREPARED,
        COMMITTED,
        ABORTED,
        FAILED
    };
    TxnManager() : TxnManager(NULL) {};
    TxnManager(WorkerThread * thread);
    virtual ~TxnManager();

    // start to run transactions
    RC start();

    // rerun previously aborted transactions
    RC restart();

    void              set_txn_id(uint64_t txn_id) { _txn_id = txn_id; }
    uint64_t          get_txn_id()          { return _txn_id; }
    uint64_t          get_thd_id()          { return
    _worker_thread->get_thd_id(); }

    State             get_txn_state()       { return _txn_state; }
    void              set_txn_state(State state) { _txn_state = state; }
    void              set_decision(RC rc) { _decision = rc; };
    void              lock() {pthread_mutex_lock(&_latch);};
    void              unlock() {pthread_mutex_unlock(&_latch);}

    // Synchronization
    // ===============
    SemaphoreSync *   rpc_semaphore;
    SemaphoreSync *   rpc_log_semaphore;
    SemaphoreSync *   terminate_semaphore;
    pthread_mutex_t   _latch;


    // Distributed transactions
    // ========================
public:
    // client
    RC start();

    // server
    RC process_remote_request(const SundialRequest* request, SundialResponse* response);
    RC process_prepare_request(const SundialRequest* request, SundialResponse*
    response);

    inline State rc_to_state(RC rc) {
        switch(rc) {
            case ABORT: return ABORTED;
            case COMMIT: return COMMITTED;
            default: assert(false); return RUNNING;
        }
    };

private:
    WorkerThread *    _worker_thread;
    volatile State    _txn_state;
    volatile RC       _decision;
    // txn_id format.
    // | per thread monotonically increasing ID   |  thread ID   |   Node ID |
    uint64_t          _txn_id;
};
