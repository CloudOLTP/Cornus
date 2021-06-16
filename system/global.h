#pragma once

#include "stdint.h"
#include <iomanip>
#include <unistd.h>
#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <string.h>
#include <typeinfo>
#include <list>
#include <mm_malloc.h>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <sstream>
#include <time.h>
#include <sys/time.h>
#include <math.h>
//#include <boost/lockfree/queue.hpp>
#include "pthread.h"

#include "config.h"
#include "stats.h"

using std::cout;
using std::endl;
using std::set;
using std::map;
using std::string;

class Stats;
class DL_detect;
class Manager;
class Query_queue;
class Plock;
class VLLMan;
class TxnTable;
class Transport;
class FreeQueue;
class CacheManager;
class IndexHash;
class WorkerThread;
class LogManager;
class SundialRPCClient;
class SundialRPCServerImpl;
class RedisClient;
class AzureBlobClient;

typedef uint64_t ts_t; // time stamp type

// Global Data Structure
// =====================
extern Stats *          glob_stats;
extern Manager *        glob_manager;

#if LOG_LOCAL || LOG_NODE
extern LogManager *     log_manager;
#endif

extern bool volatile    warmup_finish;
extern bool volatile    enable_thread_mem_pool;
extern pthread_barrier_t global_barrier;
extern pthread_mutex_t  global_lock;

#if ENABLE_LOCAL_CACHING
extern CacheManager *   local_cache_man;
#endif

// Global Parameter
// ================
extern double           g_cpu_freq;
extern uint32_t         g_num_worker_threads;

extern uint32_t         g_total_num_threads;
extern ts_t             g_abort_penalty;
extern uint32_t         g_ts_alloc;
extern bool             g_sort_key_order;
extern bool             g_ts_batch_alloc;
extern uint32_t         g_ts_batch_num;
extern uint32_t         g_max_num_active_txns;
extern double           g_run_time;
extern uint64_t         g_max_clock_skew;

// TICTOC
// ------
extern uint32_t         g_max_num_waits;
extern uint64_t         g_local_cache_size;
extern double           g_read_intensity_thresh;

// YCSB
// ----
extern uint32_t         g_cc_alg;
extern double           g_perc_remote;
extern double           g_read_perc;
extern double           g_zipf_theta;
extern uint64_t         g_synth_table_size;
extern uint32_t         g_req_per_query;
extern uint32_t         g_init_parallelism;
extern double           g_readonly_perc;

// TPCC
// ----
extern uint32_t         g_num_wh;
extern double           g_perc_payment;
extern uint32_t         g_max_items;
extern uint32_t         g_cust_per_dist;
extern uint32_t         g_payment_remote_perc;
extern uint32_t         g_new_order_remote_perc;
extern double           g_perc_payment;
extern double           g_perc_new_order;
extern double           g_perc_order_status;
extern double           g_perc_delivery;


extern char *           output_file;
extern char             ifconfig_file[];

enum RC {RCOK, COMMIT, ABORT, WAIT, LOCAL_MISS, SPECULATE, ERROR, FINISH, FAIL};
enum access_t {RD, WR, XP, SCAN, INS, DEL};

struct RemoteRequestInfo {
        uint64_t key;
        uint64_t index_id;
        uint64_t table_id;
        access_t access_type;
    };

// INDEX
enum latch_t {LATCH_EX, LATCH_SH, LATCH_NONE};
// accessing type determines the latch type on nodes
enum idx_acc_t {INDEX_INSERT, INDEX_READ, INDEX_NONE};
// TIMESTAMP
enum TsType {R_REQ, W_REQ, P_REQ, XP_REQ};
enum Isolation {SR, SI, RR, NO_ACID};

// global_key_t is a pair of table_id (uint32) and key (uint64)
using global_key_t = std::pair<uint32_t, uint64_t>;


#define MSG(str, args...) { \
    printf("[%s : %d] " str, __FILE__, __LINE__, args); } \

// principal index structure. The workload may decide to use a different
// index structure for specific purposes. (e.g. non-primary key access should use hash)
#if INDEX_STRUCT == IDX_BTREE
#define INDEX        index_btree
#else  // IDX_HASH
#define INDEX        IndexHash
#endif

#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT
    class Row_lock;
    class LockManager;
    #define ROW_MAN Row_lock
    #define CC_MAN LockManager
#elif CC_ALG == TICTOC
    class Row_tictoc;
    class TicTocManager;
    #define ROW_MAN Row_tictoc
    #define CC_MAN TicTocManager
#elif CC_ALG == NAIVE_TICTOC
    class Row_naive_tictoc;
    class NaiveTicTocManager;
    #define ROW_MAN Row_naive_tictoc
    #define CC_MAN NaiveTicTocManager
#elif CC_ALG == F_ONE
    class Row_f1;
    class F1Manager;
    #define ROW_MAN Row_f1
    #define CC_MAN F1Manager
#elif CC_ALG == MAAT
    class Row_maat;
    class MaaTManager;
    #define ROW_MAN Row_maat
    #define CC_MAN MaaTManager
#elif CC_ALG == IDEAL_MVCC
    class Row_MVCC;
    class MVCCManager;
    #define ROW_MAN Row_MVCC
    #define CC_MAN MVCCManager
#elif CC_ALG == TCM
    class Row_TCM;
    class TCMManager;
    #define ROW_MAN Row_TCM
    #define CC_MAN TCMManager
#endif

// constants
// =========
#ifndef UINT64_MAX
#define UINT64_MAX         18446744073709551615UL
#endif // UINT64_MAX

// Distributed DBMS
// ================
extern uint32_t         g_num_nodes;
extern uint32_t         g_num_nodes_and_storage;
extern uint32_t         g_node_id;
extern uint32_t         g_storage_node_id;

extern uint32_t         g_num_input_threads;
extern uint32_t         g_num_output_threads;

extern  uint32_t        g_log_sz;

extern SundialRPCClient * rpc_client;
extern SundialRPCServerImpl * rpc_server;
#if LOG_DEVICE == LOG_DVC_REDIS
extern RedisClient *      redis_client;
#endif
#if LOG_DEVICE == LOG_DVC_AZURE_BLOB
extern AzureBlobClient *      azure_blob_client;
#endif

extern Transport *      transport;
//typedef boost::lockfree::queue<uint64_t, boost::lockfree::capacity<INOUT_QUEUE_SIZE>> InOutQueue;
//extern InOutQueue **    input_queues;
//extern InOutQueue **    output_queues;
extern WorkerThread **  worker_threads;
extern uint32_t         g_txn_table_size;
extern TxnTable *       txn_table;

extern FreeQueue *      free_queue_txn_man;

extern string           ifconfig_string;

extern uint64_t         g_failure_pt;

