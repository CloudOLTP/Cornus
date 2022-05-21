//
// Created by Zhihan Guo on 9/13/21.
//
#include "txn.h"
#include "redis_client.h"
#include "azure_blob_client.h"
#include "rpc_client.h"
#include "worker_thread.h"
#include "stats.h"

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

uint64_t TxnManager::get_thd_id() {
	return _worker_thread->get_thd_id();
}

TxnManager::~TxnManager()
{
    delete rpc_semaphore;
    delete rpc_log_semaphore;
    delete terminate_semaphore;
}

RC TxnManager::start() { 
    // send prepare request for 2/3 of the nodes
/*
    for (uint64_t i = 0; i < g_num_nodes; i++) {
        if (i == g_node_id || (i % 3 == 2))
            continue;
        SundialRequest request;
        SundialResponse response;
        request.set_txn_id( get_txn_id() );
        request.set_request_type( SundialRequest::PREPARE_REQ);
        request.set_node_id(i);
        rpc_client->sendRequestAsync(i, request, response);
    }
*/
    // terminate txn
    uint64_t starttime = get_sys_clock();
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
	uint64_t term_time = get_sys_clock() - starttime;
    INC_FLOAT_STATS(terminate, term_time);
    INC_INT_STATS(num_terminate, 1);
    vector<double> &all =
            glob_stats->_stats[0]->term_latency;
    all.push_back(term_time);
	//printf("%.2f,", term_time / 1000000.0); // ms
    return _decision;
}

RC TxnManager::process_prepare_request(const SundialRequest* request,
    SundialResponse* response) {
	string data = "[LSN] placehold:" + string('d', 8 * 32 * 8);
	rpc_log_semaphore->incr();
	#if LOG_DEVICE == LOG_DVC_REDIS
	redis_client->log_if_ne_data(g_node_id, get_txn_id(), data);
	#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
	azure_blob_client->log_if_ne_data(g_node_id, get_txn_id(), data);
	#endif
	rpc_log_semaphore->wait();
	return _decision;
}
