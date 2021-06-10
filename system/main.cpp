
#include <string>
#include <fstream>

#include "global.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "tpcc.h"
#include "worker_thread.h"
#include "manager.h"
#include "query.h"
#include "txn_table.h"
#if LOG_LOCAL
#include "logging_thread.h"
#include "log.h"
#endif
#if LOG_NODE
#include "log.h"
#endif
#include "rpc_server.h"
#include "rpc_client.h"
#include "redis_client.h"
#include "azure_blob_client.h"

void * start_thread(void *);
void * start_rpc_server(void *);
void get_node_id();

// defined in parser.cpp
void parser(int argc, char ** argv);

#if LOG_NODE
int main(int argc, char* argv[])
{
    parser(argc, argv);
    // get_node_id(); // for better debug experience
    cout << "[Sundial] start node " << g_node_id << endl;
    glob_manager = new Manager;
    glob_manager->calibrate_cpu_frequency();
    char log_name[50];
    strcpy(log_name, "log_node_");
    strcat(log_name, std::to_string(g_node_id).c_str());
    log_manager = new LogManager(log_name);
    log_manager->run_flush_thread();
    g_total_num_threads = 1; // leave one thread slot to collect stats
    glob_stats = new Stats;
    rpc_client = new SundialRPCClient();
    rpc_server = new SundialRPCServerImpl;

    pthread_t * pthread_rpc = new pthread_t;
    pthread_create(pthread_rpc, NULL, start_rpc_server, NULL);
    
    // make sure server is setup before moving on
    sleep(SYNCHRONIZE_TIME);
    cout << "[Sundial] Synchronization starts" << endl;

    // Can start only if all other nodes have also finished initialization
    while (glob_manager->num_sync_requests_received() < g_num_nodes)
        usleep(1);
    cout << "[Sundial] Synchronization done" << endl;


    while (glob_manager->num_sync_requests_received() < (g_num_nodes) * 2)
        usleep(1);

    log_manager->stop_flush_thread();
    delete log_manager;
    cout << "[Sundial] Complete." << endl;
    glob_stats->profile_log();

    return 0;
}
#else
int main(int argc, char* argv[])
{
    parser(argc, argv);
    cout << "[Sundial] start node " << g_node_id << endl;
#if LOG_REMOTE && LOG_DEVICE == LOG_DVC_NATIVE
    g_storage_node_id = g_num_nodes_and_storage - 1 - g_node_id;
#endif

    g_total_num_threads = g_num_worker_threads;

    glob_manager = new Manager;
    txn_table = new TxnTable();
    glob_manager->calibrate_cpu_frequency();

#if DISTRIBUTED
    rpc_client = new SundialRPCClient();
    rpc_server = new SundialRPCServerImpl;
#if LOG_DEVICE == LOG_DVC_REDIS
    // assume a shared logging but store different node's info to different key
    redis_client = new RedisClient();
#endif
#if LOG_DEVICE == LOG_DVC_AZURE_BLOB
    azure_blob_client = new AzureBlobClient();
#endif
    pthread_t * pthread_rpc = new pthread_t;
    pthread_create(pthread_rpc, NULL, start_rpc_server, NULL);
#endif

#if LOG_LOCAL
    g_total_num_threads ++;
    log_manager = new LogManager();
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
    pthread_barrier_init( &global_barrier, NULL, g_total_num_threads);
    pthread_mutex_init( &global_lock, NULL);

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
    uint64_t starttime;
    uint64_t endtime;
#if DISTRIBUTED
    cout << "[Sundial] Synchronization starts" << endl;
    // Notify other nodes that the current node has finished initialization
#if LOG_REMOTE && LOG_DEVICE == LOG_DVC_NATIVE
    for (uint32_t i = 0; i < g_num_nodes_and_storage; i ++) {
    	cout << "[Sundial] contacting node-" << i << endl;
#else
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
#endif
        if (i == g_node_id) continue;
    	//cout << "[Sundial] contacting node-" << i << endl;
        SundialRequest request;
        SundialResponse response;
        request.set_request_type( SundialRequest::SYS_REQ );
        starttime = get_sys_clock();
        rpc_client->sendRequest(i, request, response);
        endtime = get_sys_clock() - starttime;
        INC_FLOAT_STATS(time_debug5, endtime);
        cout << "[Sundial] network roundtrip to node " << i << ": " <<
        endtime / 1000 << " us" << endl;
    }
    // Can start only if all other nodes have also finished initialization

    while (glob_manager->num_sync_requests_received() < g_num_nodes - 1)
        usleep(1);
    cout << "[Sundial] Synchronization done" << endl;
#endif
    for (uint64_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_create(pthreads_worker[i], NULL, start_thread, (void *)worker_threads[i]);

#if LOG_LOCAL
    LoggingThread * logging_thread = new LoggingThread(next_thread_id ++);
    pthread_t * pthreads_logging = new pthread_t;
    pthread_create(pthreads_logging, NULL, start_thread, (void *)logging_thread);
#endif
    assert(next_thread_id == g_total_num_threads);

    starttime = get_server_clock();
    start_thread((void *)(worker_threads[g_num_worker_threads - 1]));

    for (uint32_t i = 0; i < g_num_worker_threads - 1; i++)
        pthread_join(*pthreads_worker[i], NULL);

#if DISTRIBUTED
    cout << "[Sundial] End synchronization starts" << endl;
    assert( glob_manager->are_all_worker_threads_done() );
    SundialRequest request;
    SundialResponse response;
    request.set_request_type( SundialRequest::SYS_REQ );
    // Notify other nodes the completion of the current node.
#if LOG_REMOTE && LOG_DEVICE == LOG_DVC_NATIVE
    for (uint32_t i = 0; i < g_num_nodes_and_storage; i ++) {
#else
    for (uint32_t i = 0; i < g_num_nodes; i ++) {
#endif
        if (i == g_node_id) continue;
        rpc_client->sendRequest(i, request, response);
    }

    while (glob_manager->num_sync_requests_received() < (g_num_nodes - 1) * 2)
        usleep(1);
    cout << "[Sundial] End synchronization ends" << endl;
#endif
#if LOG_LOCAL
    pthread_join(*pthreads_logging, NULL);
#endif
    //assert( txn_table->get_size() == 0 );
    endtime = get_server_clock();
    cout << "Complete. Total RunTime = " << 1.0 * (endtime - starttime) / BILLION << endl;
    if (STATS_ENABLE && (!FAILURE_ENABLE || (FAILURE_NODE != g_node_id)))
        glob_stats->print();

    for (uint32_t i = 0; i < g_num_worker_threads; i ++) {
        delete pthreads_worker[i];
        delete worker_threads[i];
    }
    delete [] pthreads_worker;
    delete [] worker_threads;
#if LOG_LOCAL
    delete pthreads_logging;
    delete logging_thread;
    delete log_manager;
#endif
    return 0;
}
#endif

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
