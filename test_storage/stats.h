#pragma once
#include "global.h"
#include "helper.h"

using std::vector;


enum StatsFloat {
    // termination time
    STAT_terminate,

    // node communication
    STAT_rpc,

    STAT_log_if_ne,
    STAT_log_if_ne_iso,
    STAT_log_if_ne_data,
    STAT_log_if_ne_data_iso,
    STAT_log_sync,
    STAT_log_sync_data,
    STAT_log_sync_data_iso,
    STAT_log_async,
    STAT_log_async_data,
    STAT_log_async_data_iso,

    NUM_FLOAT_STATS
};

enum StatsInt {
    // Txn affected by failure (running termination protocol)
    STAT_num_terminate,

    // node communication
    STAT_num_rpc,

    // remote logging
    STAT_num_log_if_ne,
    STAT_num_log_if_ne_iso,
    STAT_num_log_if_ne_data,
    STAT_num_log_if_ne_data_iso,
    STAT_num_log_sync,
    STAT_num_log_sync_data,
    STAT_num_log_sync_data_iso,
    STAT_num_log_async,
    STAT_num_log_async_data,
    STAT_num_log_async_data_iso,

    NUM_INT_STATS
};

class Stats_thd {
public:
    Stats_thd();
    void init(uint64_t thd_id);
    void clear();

    double * _float_stats;
    uint64_t * _int_stats;

    vector<double> term_latency;
};

class Stats {
public:
    Stats();
    // PER THREAD statistics
    Stats_thd ** _stats;

    void init();
    void init(uint64_t thread_id);
    void clear(uint64_t tid);
    void print();
    void output(std::ostream * os);


    std::string statsFloatName[NUM_FLOAT_STATS] = {
        // termination time
        "terminate",

        // node communication
        "rpc",

        // remote logging
        "log_if_ne",
        "log_if_ne_iso",
        "log_if_ne_data",
        "log_if_ne_data_iso",
        "log_sync",
        "log_sync_data",
        "log_sync_data_iso",
        "log_async",
        "log_async_data",
        "log_async_data_iso",
    };

    std::string statsIntName[NUM_INT_STATS] = {
        // termination time
        "num_terminate",

        // node communication
        "num_rpc",

        // remote logging
        "num_log_if_ne",
        "num_log_if_ne_iso",
        "num_log_if_ne_data",
        "num_log_if_ne_data_iso",
        "num_log_sync",
        "num_log_sync_data",
        "num_log_sync_data_iso",
        "num_log_async",
        "num_log_async_data",
        "num_log_async_data_iso",

    };
private:
    vector<double> _aggregate_term_latency;
};
