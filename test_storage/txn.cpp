//
// Created by Zhihan Guo on 9/13/21.
//
#include "txn.h"
#include "redis_client.h"
#include "azure_blob_client.h"
#include "rpc_client.h"

TxnManager::TxnManager(WorkerThread * thread)
{
    _txn_state = RUNNING;
    _decision = COMMIT;
    _worker_thread = thread;
    rpc_semaphore = new SemaphoreSync();
    rpc_log_semaphore = new SemaphoreSync();
    terminate_semaphore = new SemaphoreSync();
    pthread_mutex_init(&_latch, NULL);
}

TxnManager::~TxnManager()
{
    delete rpc_semaphore;
    delete log_semaphore;
    delete terminate_semaphore;
}

RC TxnManager::start() {
    // send prepare request for 2/3 of the nodes
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == g_node_id || (i % 3 == 2))
            continue;
        SundialRequest request;
        SundialRequest response;
        request.set_txn_id( get_txn_id() );
        request.set_request_type( SundialRequest::PREPARE_REQ);
        request.set_node_id(i);
        rpc_client->sendRequestAsync(this, i, request, response);
        rpc_semaphore->incr();
    }
    // terminate txn
    uint64_t starttime = 0;
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == g_node_id)
            continue;
        rpc_log_semaphore->incr();
#if LOG_DEVICE == LOG_DVC_REDIS
        redis_client->log_if_ne(i, _txn_id);
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        azure_blob_client->log_if_ne(i, _txn_id);
#endif
    }
    rpc_log_semaphore->wait();
    INC_FLOAT_STATS(terminate, get_sys_clock() - starttime);
    INC_INT_STATS(num_terminate, 1);
    rpc_semaphore->wait();
    return _decision;
}