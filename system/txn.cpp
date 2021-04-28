#include "txn.h"
#include "row.h"
#include "workload.h"
#include "ycsb.h"
#include "worker_thread.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "helper.h"
#include "manager.h"
#include "query.h"
#include "txn_table.h"
#include "cc_manager.h"
#include "store_procedure.h"
#include "ycsb_store_procedure.h"
#include "tpcc_store_procedure.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"

#include "tictoc_manager.h"
#include "lock_manager.h"
#include "f1_manager.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
#include "row_lock.h"
#endif
#include "log.h"
#include "redis_client.h"

// TODO. cleanup the accesses related malloc code.

TxnManager::TxnManager(QueryBase * query, WorkerThread * thread)
{
    _store_procedure = GET_WORKLOAD->create_store_procedure(this, query);
    _cc_manager = CCManager::create(this);
    _txn_state = RUNNING;
    _decision = COMMIT;
    _worker_thread = thread;

    _txn_start_time = get_sys_clock();
    _txn_restart_time = _txn_start_time;
    _lock_wait_time = 0;
    _net_wait_time = 0;
    _terminate_time = 0;
    num_local_write = 0;

    _is_sub_txn = false;
    _is_single_partition = true;
    _is_read_only = true;
    _is_remote_abort = false;

    log_semaphore = new SemaphoreSync();
    dependency_semaphore = new SemaphoreSync();
    rpc_semaphore = new SemaphoreSync();
    rpc_log_semaphore = new SemaphoreSync();
    pthread_mutex_init(&_latch, NULL);
}

TxnManager::~TxnManager()
{
    if (_store_procedure)
        delete _store_procedure;
    delete _cc_manager;
    for (auto kvp : _remote_nodes_involved)
        delete kvp.second;
    for (auto kvp : _log_nodes_involved)
        delete kvp.second;
    delete log_semaphore;
    delete dependency_semaphore;
    delete rpc_semaphore;
    delete rpc_log_semaphore;
}

void
TxnManager::update_stats()
{
    // TODO. collect stats for sub_queries.
    if (is_sub_txn())
        return;

#if WORKLOAD == TPCC && STATS_ENABLE
    uint32_t type = ((QueryTPCC *)_store_procedure->get_query())->type;
    if (_txn_state == COMMITTED) {
        glob_stats->_stats[GET_THD_ID]->_commits_per_txn_type[ type ]++;
        glob_stats->_stats[GET_THD_ID]->_time_per_txn_type[ type ] +=
            _finish_time - _txn_start_time - _lock_wait_time - _net_wait_time;
    } else
        glob_stats->_stats[GET_THD_ID]->_aborts_per_txn_type[ type ]++;
#endif

    if ( _txn_state == COMMITTED ) {
        INC_INT_STATS(num_commits, 1);
        uint64_t latency;
        if (is_single_partition()) {
            INC_FLOAT_STATS(single_part_execute_phase, _commit_start_time - _txn_restart_time);
        #if CONTROLLED_LOCK_VIOLATION
            INC_FLOAT_STATS(single_part_precommit_phase, _precommit_finish_time - _commit_start_time);
        #endif
        #if LOG_LOCAL
            INC_FLOAT_STATS(single_part_log_latency, _log_ready_time - _commit_start_time);
        #endif
            INC_FLOAT_STATS(single_part_commit_phase, _finish_time - _commit_start_time);
            INC_FLOAT_STATS(single_part_abort, _txn_restart_time - _txn_start_time);

            INC_INT_STATS(num_single_part_txn, 1);
            latency = _finish_time - _txn_start_time;
        } else {
            INC_FLOAT_STATS(multi_part_execute_phase, _prepare_start_time - _txn_restart_time);
        #if CONTROLLED_LOCK_VIOLATION
            INC_FLOAT_STATS(multi_part_precommit_phase, _precommit_finish_time - _prepare_start_time);
        #endif
            INC_FLOAT_STATS(multi_part_prepare_phase, _commit_start_time - _prepare_start_time);
            INC_FLOAT_STATS(multi_part_commit_phase, _finish_time - _commit_start_time);
            INC_FLOAT_STATS(multi_part_abort, _txn_restart_time - _txn_start_time);
            INC_FLOAT_STATS(multi_part_cleanup_phase, get_sys_clock() - _finish_time);

            INC_INT_STATS(num_multi_part_txn, 1);
            // latency = _commit_start_time - _txn_start_time; // why commit start time?
            latency = _finish_time - _txn_start_time;
            uint64_t total_time = get_sys_clock() - _txn_start_time;
            #if COLLECT_LATENCY
            INC_FLOAT_STATS(dist_txn_latency, latency);
            INC_FLOAT_STATS(time_debug7, total_time);
            vector<double> &all = glob_stats->_stats[GET_THD_ID]->dist_latency;
            all.push_back(latency);
            #endif
#if FAILURE_ENABLE
            if (_terminate_time != 0) {
                INC_FLOAT_STATS(terminate_time_co, _finish_time - _terminate_time);
                INC_INT_STATS(num_affected_txn_co, 1);
                vector<double> &all =
                    glob_stats->_stats[GET_THD_ID]->term_latency;
                all.push_back(_finish_time - _terminate_time);
            }
#endif
        }
#if COLLECT_LATENCY
        INC_FLOAT_STATS(txn_latency, latency);
        vector<double> &all = glob_stats->_stats[GET_THD_ID]->all_latency;
        all.push_back(latency);
#endif
    } else if ( _txn_state == ABORTED ) {
        INC_INT_STATS(num_aborts, 1);
        if (_store_procedure->is_self_abort()) {
            INC_INT_STATS(num_aborts_terminate, 1);
        } else {
            INC_INT_STATS(num_aborts_restart, 1);
        }
        if (_is_remote_abort) {
            INC_INT_STATS(num_aborts_remote, 1);
        } else {
            INC_INT_STATS(num_aborts_local, 1);
        }
    } else
        assert(false);
}

RC
TxnManager::restart() {
    assert(_txn_state == ABORTED);
    _is_single_partition = true;
    _is_read_only = true;
    _is_remote_abort = false;
    num_local_write = 0;

    _txn_restart_time = get_sys_clock();
    _store_procedure->init();
    for (auto kvp : _remote_nodes_involved)
        delete kvp.second;
    _remote_nodes_involved.clear();
    for (auto kvp : _log_nodes_involved)
        delete kvp.second;
    _log_nodes_involved.clear();
    return start();
}

RC
TxnManager::start()
{
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] start txn\n", g_node_id, _txn_id);
#endif
    RC rc = RCOK;
    _txn_state = RUNNING;
    _terminate_time = 0;
    // running transaction on the host node
    rc = _store_procedure->execute();
    // Handle single-partition transactions, skip if self failed
    if (is_single_partition()) {
#if DEBUG_PRINT
        printf("[node-%u, txn-%lu] process single part\n", g_node_id, _txn_id);
#endif
        _commit_start_time = get_sys_clock();
        rc = process_commit_phase_singlepart(rc);
    } else {
        if (rc == COMMIT) {
#if DEBUG_PRINT
            printf("[node-%u, txn-%lu] prepare phase\n", g_node_id, _txn_id);
#endif
            _prepare_start_time = get_sys_clock();
            rc = process_2pc_phase1();
        }
        if (rc != FAIL) {
#if DEBUG_PRINT
            printf("[node-%u, txn-%lu] commit phase\n", g_node_id, _txn_id);
#endif
            _commit_start_time = get_sys_clock();
            rc = process_2pc_phase2(rc);
        }
    }
    if (rc != FAIL) {
#if DEBUG_PRINT
        if (rc == COMMIT)
            printf("[node-%u, txn-%lu] txn commit\n", g_node_id, _txn_id);
        else {
            assert(rc == ABORT);
            printf("[node-%u, txn-%lu] txn aborted\n", g_node_id, _txn_id);
        }
#endif
        update_stats();
    } else {
#if DEBUG_PRINT
        printf("[node-%u, txn-%lu] txn abort, detected self failure\n",
        g_node_id, _txn_id);
#endif
        _txn_state = ABORTED;
    }
    return rc;
}

RC
TxnManager::termination_protocol() {
#if DEBUG_FAILURE || DEBUG_PRINT
	printf("[node-%u, txn-%lu] termination protocol\n", g_node_id, _txn_id);
#endif
    // received msg from failed node, need to learn the decision or force abort
    // possible return values: COMMIT, ABORT, FAIL(self is down)
    _terminate_time = get_sys_clock();
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        if (it->second->is_readonly)
            continue;
        rpc_log_semaphore->incr();
        if (redis_client->log_if_ne(it->first, get_txn_id()) == FAIL) {
            // self if fail, stop working and return
            _decision = FAIL;
            return FAIL;
        }
    }
    rpc_log_semaphore->wait();
    return _decision;
}

