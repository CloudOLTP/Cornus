//
// Created by Zhihan Guo on 9/13/21.
//

#ifndef SUNDIAL_TEST_STORAGE_HELPER_H_
#define SUNDIAL_TEST_STORAGE_HELPER_H_

#include <iostream>
#include "global.h"

#define BILLION 1000000000UL

#define ATOM_ADD(dest, value) \
    __sync_fetch_and_add(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
    __sync_fetch_and_add(&(dest), value)
#define ATOM_ADD_FETCH(dest, value) \
    __sync_add_and_fetch(&(dest), value)
#define ATOM_SUB(dest, value) \
    __sync_fetch_and_sub(&(dest), value)
// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
    __sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_SUB_FETCH(dest, value) \
    __sync_sub_and_fetch(&(dest), value)
#define ATOM_COUT(msg) \
    stringstream sstream; sstream << msg; cout << sstream.str();

#define COMPILER_BARRIER asm volatile("" ::: "memory");
#define PAUSE __asm__ ( "pause;" );

// STATS helper
// ============

#define INC_FLOAT_STATS(name, value) { \
        glob_stats->_stats[0]->_float_stats[STAT_##name] += value; }

#define INC_INT_STATS(name, value) {{ \
        ATOM_ADD(glob_stats->_stats[0]->_int_stats[STAT_##name],value); }}

#define STAT_SUM(type, sum, name) \
    type sum = 0; \
    for (uint32_t ii = 0; ii < g_total_num_threads; ii++) \
        sum += _stats[ii]->name;

#define STAT_MAX(type, max, name) \
    type max = 0; \
    for (uint32_t ii = 0; ii < g_total_num_threads; ii++) \
        max = _stats[ii]->name > max ? _stats[ii]->name : max;

#define STAT_MIN(type, min, name) \
    type min = 0; \
    for (uint32_t ii = 0; ii < g_total_num_threads; ii++) \
        min = _stats[ii]->name < min ? _stats[ii]->name : min;

#define STAT_PRINT_AVG_US(type_n, name_n, tn, name_d) \
    STAT_SUM(type_n, total_##name_n, _float_stats[STAT_##name_n]) \
    STAT_SUM(uint64_t, total_##name_d, _int_stats[STAT_##name_d]) \
    if (total_##name_d > 0) { \
        out << "    " << left << "average_" << #name_n << ": " << \
        total_##name_n * 1.0 / total_##name_d / 1000 << endl; \
    }

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

#endif //SUNDIAL_TEST_STORAGE_HELPER_H_
