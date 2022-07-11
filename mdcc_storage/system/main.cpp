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
void get_node_id();

// defined in parser.cpp
void parser(int argc, char ** argv);

int main(int argc, char* argv[])
{

    parser(argc, argv);
    cout << "[Sundial] start storage node " << g_node_id << endl;
    g_total_num_threads = NUM_RPC_SERVER_THREADS;

    glob_manager = new Manager;
    txn_table = new TxnTable();
    glob_manager->calibrate_cpu_frequency();

    rpc_client = new SundialRPCClient();
    rpc_server = new SundialRPCServerImpl;
    pthread_t * pthread_rpc = new pthread_t;
    pthread_create(pthread_rpc, NULL, start_rpc_server, NULL);

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

    // make sure server is setup before moving on
    sleep(5);

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

    // terminate on "q" or "quit"
    std::string line;
    while(std::getline(std::cin, line)) {
        if (line == "q" || line == "quit") {
            break;
        }
    }
    cout << "Terminate." << endl;

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

void get_node_id()
{
    // get server names
    vector<string> _urls;
    string line;
    std::ifstream file (ifconfig_file);
    assert(file.is_open());
    while (getline (file, line)) {
        if (line[0] == '#')
            continue;
        else {
            std::string delimiter = ":";
            std::string token = line.substr(0, line.find(delimiter));
            _urls.push_back(token);
        }
    }
    char hostname[1024];
    gethostname(hostname, 1023);
    printf("[!] My Hostname is %s\n", hostname);
    for (uint32_t i = 0; i < g_num_nodes_and_storage; i ++)  {
        if (_urls[i] == string(hostname)) {
            printf("[!] My node id id %u\n", i);
            g_node_id = i;
        }
    }
    file.close();
}
