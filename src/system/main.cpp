#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "global.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc.h"
#include "worker_thread.h"
#include "manager.h"
#include "query.h"
#include "txn_table.h"
#include "rpc_server.h"
#include "rpc_client.h"
#include "redis_client.h"
#include "azure_blob_client.h"

void * start_thread(void *);
void * start_rpc_server(void *);

// defined in parser.cpp
void parser(int argc, char ** argv);

int main(int argc, char* argv[])
{

    parser(argc, argv);
    cout << "[Sundial] start node " << g_node_id << endl;
    g_total_num_threads = g_num_worker_threads;

    glob_manager = new Manager;
    txn_table = new TxnTable();
    glob_manager->calibrate_cpu_frequency();

#if DISTRIBUTED || NUM_STORAGE_NODES > 0
    rpc_client = new SundialRPCClient();
    rpc_server = new SundialRPCServerImpl;
    pthread_t * pthread_rpc = new pthread_t;
    pthread_create(pthread_rpc, nullptr, start_rpc_server, nullptr);
#endif
    #if LOG_DEVICE == LOG_DVC_REDIS
        // assume a shared logging but store different node's info to different key
        cout << "[Sundial] creating Redis client" << endl;
        redis_client = new RedisClient();
    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        cout << "[Sundial] creating Azure Blob client" << endl;
        azure_blob_client = new AzureBlobClient();
    #endif

    glob_stats = new Stats;

    printf("[Sundial] mem_allocator initialized!\n");
    workload * m_wl;
    switch (WORKLOAD) {
        case YCSB :
            m_wl = new WorkloadYCSB;
            QueryYCSB::calculateDenom();
            break;
        case TPCC :
            m_wl = new WorkloadTPCC;
            break;
        default:
            assert(false);
    }

    glob_manager->set_workload(m_wl);
    m_wl->init();
    printf("[Sundial] workload initialized!\n");
    warmup_finish = true;

#if NODE_TYPE == COMPUTE_NODE
    uint64_t starttime;
    uint64_t endtime;
    pthread_barrier_init( &global_barrier, nullptr, g_total_num_threads);
    pthread_mutex_init( &global_lock, nullptr);

    // Thread numbering:
    //    worker_threads | input_thread | output_thread | logging_thread
    uint32_t next_thread_id = 0;
    worker_threads = new WorkerThread * [g_num_worker_threads];
    pthread_t ** pthreads_worker = new pthread_t * [g_num_worker_threads];
    for (uint32_t i = 0; i < g_num_worker_threads; i++) {
        worker_threads[i] = new WorkerThread(next_thread_id ++);
        pthreads_worker[i] = new pthread_t;
    }

    // make sure server is setup before moving on
    sleep(5);
#if DISTRIBUTED
    cout << "[Sundial] Synchronization starts" << endl;
    // Notify other nodes that the current node has finished initialization
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
        if (i == g_node_id) continue;
        SundialRequest request;
        SundialResponse response;
        request.set_request_type( SundialRequest::SYS_REQ );
        rpc_client->sendRequest(i, request, response);
    }
    // Can start only if all other nodes have also finished initialization
    while (glob_manager->num_sync_requests_received() < g_num_nodes - 1)
        usleep(1);
    cout << "[Sundial] Synchronization done" << endl;
#endif
#if NUM_STORAGE_NODES > 0
    cout << "[Sundial] Synchronize with Storage Nodes" << endl;
    SundialRequest new_request;
    SundialResponse new_response;
    new_request.set_request_type( SundialRequest::SYS_REQ );
    for (uint32_t i = 0; i < g_num_storage_nodes; i ++) {
      rpc_client->sendRequest(i, new_request, new_response, true);
    }
#endif

    for (uint64_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_create(pthreads_worker[i], nullptr, start_thread, (void *)
                                                              worker_threads[i]);
    assert(next_thread_id == g_total_num_threads);
    starttime = get_server_clock();
    start_thread((void *)(worker_threads[g_num_worker_threads - 1]));
    for (uint32_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_join(*pthreads_worker[i], nullptr);
    assert( glob_manager->are_all_worker_threads_done() );

#if DISTRIBUTED
    cout << "[Sundial] End synchronization starts" << endl;
    SundialRequest request;
    SundialResponse response;
    request.set_request_type( SundialRequest::SYS_REQ );
    // Notify other nodes the completion of the current node.
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
      if (i == g_node_id) continue;
        starttime = get_sys_clock();
        rpc_client->sendRequest(i, request, response);
        endtime = get_sys_clock() - starttime;
        INC_FLOAT_STATS(time_rpc, endtime);
        cout << "[Sundial] network roundtrip to node " << i << ": " <<
        endtime / 1000 << " us" << endl;
    }
    while (glob_manager->num_sync_requests_received() < (g_num_nodes - 1) * 2)
      usleep(1);
    cout << "[Sundial] End synchronization ends" << endl;
#endif
#if NUM_STORAGE_NODES > 0
    // only the first node has right to terminate
    if (g_node_id == 0) {
      cout << "[Sundial] Terminating Storage Nodes" << endl;
      new_request.set_request_type(SundialRequest::TERMINATE_REQ);
      for (uint32_t i = 0; i < g_num_storage_nodes; i++) {
        rpc_client->sendRequest(i, new_request, new_response, true);
      }
    }
#endif
    endtime = get_server_clock();
    cout << "Complete." << endl;
    if (STATS_ENABLE && (!FAILURE_ENABLE || (FAILURE_NODE != g_node_id)))
        glob_stats->print();
    for (uint32_t i = 0; i < g_num_worker_threads; i ++) {
        delete pthreads_worker[i];
        delete worker_threads[i];
    }
    delete [] pthreads_worker;
    delete [] worker_threads;
#else // #if NODE_TYPE == COMPUTE_NODE
    // terminate on receiving end synchronization
    while (glob_manager->active) {}
    cout << "Terminate." << endl;
#endif // #if NODE_TYPE == COMPUTE_NODE
    return 0;
}

void * start_thread(void * thread) {
    ((BaseThread *)thread)->run();
    return NULL;
}

void * start_rpc_server(void * input) {
    rpc_server->run();
    return NULL;
}

