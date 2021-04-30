//
// Created by Zhihan Guo on 4/25/21.
//

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


// RPC Server
// ==========

RC
TxnManager::process_prepare_request(const SundialRequest* request,
    SundialResponse* response) {
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] prepare request\n", g_node_id, _txn_id);
#endif

    if (!glob_manager->active) {
        _txn_state = ABORTED;
        return FAIL;
    }
    assert(_txn_state == RUNNING);
    RC rc = RCOK;
    uint32_t num_tuples = request->tuple_data_size();
#if LOG_LOCAL
    std::string record;
    char * log_record = NULL;
    uint32_t log_record_size = 0;
#endif
    // copy data to the write set.
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key = request->tuple_data(i).key();
        uint64_t table_id = request->tuple_data(i).table_id();
        char * data = get_cc_manager()->get_data(key, table_id);
        memcpy(data, request->tuple_data(i).data().c_str(), request->tuple_data(i).size());
    }
    // set up all nodes involved (including sender, excluding self)
    // so that termination protocol will not where to find
    for (int i = 0; i < request->nodes_size(); i++) {
        uint64_t node_id = request->nodes(i).nid();
        if (node_id == g_node_id)
            continue;
        _remote_nodes_involved[node_id] = new RemoteNodeInfo;
        // prepare request ensure all the nodes attached are rw
        _remote_nodes_involved[node_id]->is_readonly = false;
    }
#if LOG_LOCAL
    log_record_size = _cc_manager->get_log_record(log_record);
    if (log_record_size > 0) {
        assert(log_record);
        log_semaphore->incr();
        log_manager->log(this, log_record_size, log_record);
        delete [] log_record;
    }
#if CONTROLLED_LOCK_VIOLATION
    _cc_manager->process_precommit_phase_coord();
#endif
    log_semaphore->wait();
#endif
    // log votes
    if (num_tuples != 0) {
        // log prepare msg
#if LOG_REMOTE
        uint64_t starttime = get_sys_clock();
#if LOG_DEVICE == LOG_DVC_NATIVE
        send_log_request(g_storage_node_id, SundialRequest::LOG_YES_REQ);
#elif LOG_DEVICE == LOG_DVC_REDIS
        string data = "[LSN] placehold:" + string('d', num_tuples *
        g_log_sz * 8);
        rpc_log_semaphore->incr();
#if COMMIT_ALG == ONE_PC
        if (redis_client->log_if_ne_data(g_node_id, get_txn_id(), data) ==
        FAIL) {
            return FAIL;
        }
#else
        if (redis_client->log_async_data(g_node_id, get_txn_id(),
            PREPARED, data) == FAIL) {
            return FAIL;
        }
#endif
#endif
        rpc_log_semaphore->wait();
        // profile: avg time on logging a sync vote
        INC_FLOAT_STATS(time_debug2, get_sys_clock() - starttime);
        INC_INT_STATS(int_debug2, 1);
        _txn_state = PREPARED;
#endif
        _txn_state = PREPARED;
    } else {
        // readonly remote nodes
        _txn_state = COMMITTED;
        _cc_manager->cleanup(COMMIT); // release lock after log is received
        response->set_response_type( SundialResponse::PREPARED_OK_RO );
        return rc;
    }
    response->set_response_type( SundialResponse::PREPARED_OK );
    return rc;
}

RC
TxnManager::process_read_request(const SundialRequest* request,
                                    SundialResponse* response) {
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] read request\n", g_node_id, _txn_id);
#endif
    if (!glob_manager->active) {
        return FAIL;
    }

    // is already aborted by terminate request
    if (_txn_state != RUNNING) {
        response->set_response_type(SundialResponse::RESP_ABORT);
        return ABORT;
    }

    RC rc = RCOK;
    uint32_t num_tuples = request->tuple_data_size();
#if LOG_LOCAL
    std::string record;
    char * log_record = NULL;
    uint32_t log_record_size = 0;
#endif
    num_tuples = request->read_requests_size();
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key = request->read_requests(i).key();
        uint64_t index_id = request->read_requests(i).index_id();
        access_t access_type = (access_t)request->read_requests(i).access_type();

        INDEX * index = GET_WORKLOAD->get_index(index_id);
        set<row_t *> * rows = NULL;
        rc = get_cc_manager()->index_read(index, key, rows, 1);
        if (rc == ABORT) break;
        row_t * row = *rows->begin();
        get_cc_manager()->remote_key += 1;
        rc = get_cc_manager()->get_row(row, access_type, key);
        if (rc == ABORT) break;
        uint64_t table_id = row->get_table_id();
        SundialResponse::TupleData * tuple = response->add_tuple_data();
        uint64_t tuple_size = row->get_tuple_size();
        tuple->set_key(key);
        tuple->set_table_id( table_id );
        tuple->set_size( tuple_size );
        tuple->set_access_type( access_type );
        tuple->set_data( get_cc_manager()->get_data(key, table_id), tuple_size );
    }

    if (rc == ABORT) {
        if(_txn_state != ABORTED) {
            _cc_manager->cleanup(ABORT);
            _txn_state = ABORTED;
        }
        response->set_response_type( SundialResponse::RESP_ABORT );
    } else {
        response->set_response_type(SundialResponse::RESP_OK);
    }
    return rc;
}

RC
TxnManager::process_decision_request(const SundialRequest* request,
                                 SundialResponse* response, RC rc) {
    if (!glob_manager->active) {
        return FAIL;
    }
#if LOG_LOCAL
    record = std::to_string(_txn_id);
    log_record = (char *)record.c_str();
    log_record_size = record.length();
    log_semaphore->incr();
    log_manager->log(this, log_record_size, log_record);
#endif
#if LOG_REMOTE
    #if LOG_DEVICE == LOG_DVC_NATIVE
    SundialRequest::RequestType log_type = (request->request_type() ==
        SundialRequest::COMMIT_REQ)? SundialRequest::LOG_COMMIT_REQ :
            SundialRequest::LOG_ABORT_REQ;
    send_log_request(g_storage_node_id, log_type);
    #elif LOG_DEVICE == LOG_DVC_REDIS
    State status = (rc == COMMIT)? COMMITTED : ABORTED;
    rpc_log_semaphore->incr();
    if (redis_client->log_async(g_node_id, get_txn_id(), status) == FAIL) {
        pthread_mutex_unlock(&_latch);
        return FAIL;
    }
    #endif
    rpc_log_semaphore->wait();
#endif
    dependency_semaphore->wait();
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    _cc_manager->cleanup(rc); // release lock after log is received
    _finish_time = get_sys_clock();
#if FAILURE_ENABLE
    if (_terminate_time != 0) {
        INC_FLOAT_STATS(terminate_time_pa, _finish_time - _terminate_time);
        INC_INT_STATS(num_affected_txn_pa, 1);
        vector<double> &all =
                    glob_stats->_stats[GET_THD_ID]->term_latency;
        all.push_back(_finish_time - _terminate_time);
    }
#endif
    // OPTIMIZATION: release locks as early as possible.
    // No need to wait for this log since it is optional (shared log
    // optimization)
#if LOG_LOCAL
    log_semaphore->wait();
#endif
    response->set_response_type( SundialResponse::ACK );
    return rc;
}

RC
TxnManager::process_terminate_request(const SundialRequest* request,
                                     SundialResponse* response) {
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] terminate request\n", g_node_id,
                _txn_id);
#endif
    RC = RCOK;
    switch (_txn_state) {
        case RUNNING:
            // self has not voted yes, log abort and cleanup
#if LOG_REMOTE
            #if LOG_DEVICE == LOG_DVC_NATIVE
                send_log_request(g_storage_node_id, SundialRequest::LOG_ABORT_REQ);
                #elif LOG_DEVICE == LOG_DVC_REDIS
                if (redis_client->log_sync(g_node_id, get_txn_id(), ABORTED)
                == FAIL) {
                    return FAIL;
                }
                #endif
#endif
            _cc_manager->cleanup(ABORT);
            _txn_state = ABORTED;
            return ABORT;
        case PREPARED:
            // txn has voted, need to run termination protocol to find out
            rc = termination_protocol();
            rc = process_decision_request(request, response, rc);
            break;
        case COMMITTED:
            // txn is handled already
        case ABORTED:
            // txn is handled already
        default:
            break;
    }
    return rc;
}