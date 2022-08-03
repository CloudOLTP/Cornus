#pragma once

#include "global.h"
#include <queue>
#include <stack>
#include "thread.h"
#include "rpc_client.h"

class workload;
class QueryBase;
class Transport;
class TxnManager;
class SundialRPCClient;

class WorkerThread : public BaseThread {
public:
    WorkerThread(uint64_t thd_id);
    RC                      run();

    TxnManager *            get_native_txn() { return _native_txn; }
	uint64_t                get_execution_time() { return get_sys_clock() - _init_time;};
	uint64_t                get_init_time() { return _init_time;};
private:
    //void                  handle_req_finish(TxnManager * &txn_man);
    TxnManager *            _native_txn;

public:
    void                    wakeup();
    void                    add_to_pool();
    void                    wait();
private:
    bool                    _is_ready;
    pthread_cond_t *        _cond;
    pthread_mutex_t *       _mutex;
	uint64_t                _init_time;

  public:
    // for customized storage nodes
#if NODE_TYPE == STORAGE_NODE
    SundialRequest thd_requests_[NUM_NODES];
    SundialResponse thd_responses_[NUM_NODES];
#else
    // request in phase 1, as leader of paxos
    SundialRequest thd_requests_[NUM_STORAGE_NODES];
    SundialResponse thd_responses_[NUM_STORAGE_NODES];
#endif
    // request in phase 2, as leader of paxos
    SundialRequest thd_requests2_[NUM_STORAGE_NODES];
    SundialResponse thd_responses2_[NUM_STORAGE_NODES];
};
