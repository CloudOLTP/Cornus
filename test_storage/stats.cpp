#include "global.h"
#include "helper.h"
#include "stats.h"

#include "rpc_client.h"

using std::setw;
using std::left;

Stats_thd::Stats_thd()
{
    _float_stats = (double *) _mm_malloc(sizeof(double) * NUM_FLOAT_STATS, 64);
    _int_stats = (uint64_t *) _mm_malloc(sizeof(uint64_t) * NUM_INT_STATS, 64);
    clear();
}

void Stats_thd::init(uint64_t thd_id) {
    clear();
}

void Stats_thd::clear() {
    for (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
        _float_stats[i] = 0;
    for (uint32_t i = 0; i < NUM_INT_STATS; i++)
        _int_stats[i] = 0;
}

////////////////////////////////////////////////
// class Stats
////////////////////////////////////////////////
Stats::Stats()
{
    _stats = new Stats_thd * [g_total_num_threads];
    for (uint32_t i = 0; i < g_total_num_threads; i++) {
        _stats[i] = (Stats_thd *) _mm_malloc(sizeof(Stats_thd), 64);
        new(_stats[i]) Stats_thd();
    }
}

void Stats::init() {
    _stats = (Stats_thd**) _mm_malloc(sizeof(Stats_thd*) * g_total_num_threads, 64);
}

void Stats::init(uint64_t thread_id) {
    _stats[thread_id] = (Stats_thd *)
        _mm_malloc(sizeof(Stats_thd), 64);

    _stats[thread_id]->init(thread_id);
}

void Stats::clear(uint64_t tid) {
        _stats[tid]->clear();
}


void Stats::output(std::ostream * os)
{
    std::ostream &out = *os;

    // debug remote log latency
    STAT_PRINT_AVG_US(double, log_if_ne, float, num_log_if_ne);
    STAT_PRINT_AVG_US(double, log_if_ne_data, float, num_log_if_ne_data);
    STAT_PRINT_AVG_US(double, log_if_ne_iso, float, num_log_if_ne_iso);
    STAT_PRINT_AVG_US(double, log_if_ne_data_iso, float, num_log_if_ne_data_iso);
    STAT_PRINT_AVG_US(double, log_sync, float, num_log_sync);
    STAT_PRINT_AVG_US(double, log_sync_data, float, num_log_sync_data);
    STAT_PRINT_AVG_US(double, log_sync_data_iso, float, num_log_sync_data_iso);
    STAT_PRINT_AVG_US(double, log_async, float, num_log_async);
    STAT_PRINT_AVG_US(double, log_async_data, float, num_log_async_data);
    STAT_PRINT_AVG_US(double, log_async_data_iso, float, num_log_async_data_iso);

    STAT_PRINT_AVG_US(double, terminate, float, num_terminate);
    STAT_PRINT_AVG_US(double, rpc, float, num_rpc);

	/*
    // print terminate latency distribution
    out << "    " << left << "median_term_latency:"
        << _aggregate_term_latency[(uint64_t)(total_num_terminate * 0.50)] /
        BILLION << endl;
    out << "    "  << left << "25%_term_latency:"
        << _aggregate_term_latency[(uint64_t)(total_num_terminate * 0.25)] /
        BILLION << endl;
    out << "    "  << left << "75%_term_latency:"
        << _aggregate_term_latency[(uint64_t)(total_num_terminate * 0.75)] /
        BILLION << endl;
    out << "    "  << left << "99%_term_latency:"
        << _aggregate_term_latency[(uint64_t)(total_num_terminate * 0.99)] /
        BILLION << endl;
    out << "    "  << left << "max_term_latency:"
        << _aggregate_term_latency[total_num_terminate - 1] / BILLION <<
        endl;
    out << "    "  << left << "min_term_latency:"
        << _aggregate_term_latency[0] / BILLION << endl;
    out << endl;
	*/
}

void Stats::print()
{
    std::ofstream file;
    output(&cout);
    return;
}
