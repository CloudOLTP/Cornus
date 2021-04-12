#include "stats.h"
#include "manager.h"
#include "query.h"
#include "txn_table.h"
#include "log.h"
#include "rpc_client.h"
#include "rpc_server.h"
#include "redis_client.h"

Stats *             glob_stats;
Manager *           glob_manager;

#if LOG_LOCAL || LOG_NODE
LogManager *        log_manager;
#endif

bool volatile       warmup_finish = false;
bool volatile       enable_thread_mem_pool = false;
pthread_barrier_t   global_barrier;
pthread_mutex_t     global_lock;

#if ENABLE_LOCAL_CACHING
CacheManager *      local_cache_man;
#endif

// Global Parameter
// ================
double g_cpu_freq             = 1; //CPU_FREQ;
uint64_t g_abort_penalty      = ABORT_PENALTY;
uint32_t g_ts_alloc           = TS_ALLOC;
bool g_sort_key_order         = SORT_KEY_ORDER;
bool g_ts_batch_alloc         = TS_BATCH_ALLOC;
uint32_t g_ts_batch_num       = TS_BATCH_NUM;
uint32_t g_max_num_active_txns = MAX_NUM_ACTIVE_TXNS;
double g_run_time             = RUN_TIME;
uint64_t g_max_clock_skew     = MAX_CLOCK_SKEW;

// YCSB
// ====
uint32_t g_cc_alg             = CC_ALG;
double g_perc_remote          = PERC_REMOTE;
double g_read_perc            = READ_PERC;
double g_zipf_theta           = ZIPF_THETA;
uint64_t g_synth_table_size   = SYNTH_TABLE_SIZE;
uint32_t g_req_per_query      = REQ_PER_QUERY;
uint32_t g_init_parallelism   = INIT_PARALLELISM;
double g_readonly_perc        = PERC_READONLY_DATA;

// TPCC
// ====
uint32_t g_num_wh             = NUM_WH;
uint32_t g_payment_remote_perc = PAYMENT_REMOTE_PERC;
uint32_t g_new_order_remote_perc = NEW_ORDER_REMOTE_PERC;
double g_perc_payment         = PERC_PAYMENT;
double g_perc_new_order       = PERC_NEWORDER;
double g_perc_order_status    = PERC_ORDERSTATUS;
double g_perc_delivery        = PERC_DELIVERY;

#if TPCC_SMALL
uint32_t        g_max_items             = 10000;
uint32_t        g_cust_per_dist         = 2000;
#else
uint32_t        g_max_items             = 100000;
uint32_t        g_cust_per_dist         = 3000;
#endif


char *          output_file             = NULL;
char            ifconfig_file[80]       = "ifconfig.txt";

// TICTOC
uint32_t        g_max_num_waits         = MAX_NUM_WAITS;
uint64_t        g_local_cache_size      = LOCAL_CACHE_SIZE;
double          g_read_intensity_thresh = READ_INTENSITY_THRESH;

// Distributed DBMS
// ================
uint32_t        g_num_worker_threads    = NUM_WORKER_THREADS; // TODO: better integration
uint32_t        g_total_num_threads     = 0;

uint32_t        g_num_nodes             = NUM_NODES;
uint32_t        g_num_nodes_and_storage = NUM_NODES * 2;

uint32_t        g_node_id;
uint32_t        g_storage_node_id;

uint32_t        g_num_input_threads     = NUM_INPUT_THREADS;
uint32_t        g_num_output_threads    = NUM_OUTPUT_THREADS;

uint32_t        g_log_sz                = LOG_SIZE_PER_WRITE;

SundialRPCClient *  rpc_client;
SundialRPCServerImpl * rpc_server;
RedisClient *       redis_client;

Transport *     transport;
InOutQueue **   input_queues;
InOutQueue **   output_queues;
WorkerThread ** worker_threads;

// TODO. tune this table size
uint32_t        g_txn_table_size        = NUM_WORKER_THREADS * 10; // TODO:
TxnTable *      txn_table;

FreeQueue *     free_queue_txn_man;
uint32_t        g_dummy_size            = 0;

string ifconfig_string =
  "localhost:10000\n"
  "localhost:10001";
