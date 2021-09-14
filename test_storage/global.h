//
// Created by Zhihan Guo on 8/22/21.
//

#ifndef SUNDIAL_TEST_GRPC_GLOBAL_H_
#define SUNDIAL_TEST_GRPC_GLOBAL_H_

#include "config-std.h"
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
#include <chrono>

using std::cout;
using std::endl;
using std::string;

class Stats;
class SundialRPCClient;
class SundialRPCServerImpl;
class TxnTable;

extern uint32_t         g_num_nodes;
extern uint32_t         g_node_id;
extern uint32_t         g_num_rpc_recv;
extern uint32_t         g_total_num_threads;
extern Stats *          glob_stats;
extern TxnTable *       txn_table;
extern char           ifconfig_file[];
enum RC {RCOK, COMMIT, ABORT, WAIT, LOCAL_MISS, SPECULATE, ERROR, FINISH, FAIL};

extern SundialRPCClient * rpc_client;
extern SundialRPCServerImpl * rpc_server;
#if LOG_DEVICE == LOG_DVC_REDIS
class RedisClient;
extern RedisClient *      redis_client;
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
class AzureBlobClient;
extern AzureBlobClient *      azure_blob_client;
#endif
#endif //SUNDIAL_TEST_GRPC_GLOBAL_H_
