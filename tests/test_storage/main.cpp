#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "global.h"
#include "rpc_server.h"
#include "rpc_client.h"
#if LOG_DEVICE == LOG_DVC_REDIS
#include "redis_client.h"
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
#include "azure_blob_client.h"
#endif
#include "semaphore_sync.h"
#include "stats.h"
#include "txn_table.h"
#include "worker_thread.h"

void * start_thread(void *);
void * start_rpc_server(void *);

// defined in parser.cpp
void parser(int argc, char ** argv);

int main(int argc, char* argv[])
{
    for (int i = 1; i < argc; i++) {
        assert(argv[i][0] == '-');
        if (argv[i][1] == 'G') {
            if (argv[i][2] == 'n')
                g_node_id = atoi( &argv[i][3] );
        }
    }

    std::cout << "[Sundial] start node " << g_node_id << std::endl;

    rpc_server = new SundialRPCServerImpl;
    pthread_t * pthread_rpc = new pthread_t;
    pthread_create(pthread_rpc, NULL, start_rpc_server, NULL);
    rpc_client = new SundialRPCClient();
#if LOG_DEVICE == LOG_DVC_REDIS
    // assume a shared logging but store different node's info to different key
    std::cout << "[Sundial] creat Redis client!" << std::endl;
    redis_client = new RedisClient();
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    std::cout << "[Sundial] creat Azure client!" << std::endl;
    azure_blob_client = new AzureBlobClient();
#endif

    glob_stats = new Stats;
	txn_table = new TxnTable();

    uint32_t next_thread_id = 0;
    WorkerThread ** worker_threads = new WorkerThread * [g_total_num_threads];
    pthread_t ** pthreads_worker = new pthread_t * [g_total_num_threads];
    for (uint32_t i = 0; i < g_total_num_threads; i++) {
        worker_threads[i] = new WorkerThread(next_thread_id ++);
        pthreads_worker[i] = new pthread_t;
    }
    // make sure server is setup before moving on
    sleep(30);

    // Notify other nodes that the current node has finished initialization
    int num_iter = 4;
    for (uint32_t i = 0; i < g_num_nodes; i++) {
        if (i == g_node_id) continue;
        SundialRequest request;
        SundialResponse response;
        request.set_request_type(SundialRequest::SYS_REQ);
        request.set_node_id(i);
        rpc_client->sendRequest(i, request, response);
    }

    std::cout << "[Sundial] test networking on node " << g_node_id << std::endl;
    uint64_t starttime;
    for (int iter = 0; iter < num_iter - 1; iter++) {
        for (uint32_t i = 0; i < g_num_nodes; i++) {
            if (i == g_node_id) continue;
            starttime = get_sys_clock();
            SundialRequest request;
            SundialResponse response;
            request.set_request_type(SundialRequest::SYS_REQ);
            request.set_node_id(i);
            rpc_client->sendRequest(i, request, response);

        }
    }

    std::cout << "[Sundial] test termination on node " << g_node_id <<std::endl;
    for (uint64_t i = 0; i < g_total_num_threads - 1; i++)
        pthread_create(pthreads_worker[i], NULL, start_thread, (void *)worker_threads[i]);
    start_thread((void *)(worker_threads[g_total_num_threads - 1]));
    for (uint32_t i = 0; i < g_total_num_threads - 1; i++)
        pthread_join(*pthreads_worker[i], NULL);

    // do not shut down until all nodes complete synchronization
    while (g_num_rpc_recv < (g_num_nodes - 1) * num_iter) {
    }
    glob_stats->print();
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
