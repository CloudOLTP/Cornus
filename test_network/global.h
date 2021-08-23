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

extern uint32_t         g_num_nodes;
extern uint32_t         g_node_id;

extern char           ifconfig_file[];
enum RC {RCOK, COMMIT, ABORT, WAIT, LOCAL_MISS, SPECULATE, ERROR, FINISH, FAIL};

extern SundialRPCClient * rpc_client;
extern SundialRPCServerImpl * rpc_server;
#if LOG_DEVICE == LOG_DVC_REDIS
extern RedisClient *      redis_client;
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
extern AzureBlobClient *      azure_blob_client;
#endif

inline uint64_t get_sys_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    double g_cpu_freq = 2.6;
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
    ret = (uint64_t) ((double)ret / g_cpu_freq); // nano second
#else
    timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

#endif //SUNDIAL_TEST_GRPC_GLOBAL_H_
