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
#include "azure_blob_client.h"


// RPC Server
// ==========

RC
TxnManager::process_prepare_request(const SundialRequest* request,
    SundialResponse* response) {
    if (!glob_manager->active) {
        _txn_state = ABORTED;
        return FAIL;
    }
    assert(_txn_state == RUNNING);
    RC rc = RCOK;
    uint32_t num_tuples = request->tuple_data_size();
#if CC_ALG == OCC
    // if occ, validate if can commit or not
    rc = _cc_manager->validate();
    if (rc == ABORT) {
        _cc_manager->cleanup(rc);
        response->set_response_type( SundialResponse::PREPARED_ABORT );
        return rc;
    }
#else
    // copy data to the write set.
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key = request->tuple_data(i).key();
        uint64_t table_id = request->tuple_data(i).table_id();
        char * data = get_cc_manager()->get_data(key, table_id);
        memcpy(data, request->tuple_data(i).data().c_str(), request->tuple_data(i).size());
    }
#endif
    // set up all nodes involved (including sender, excluding self)
    // so that termination protocol will know where to find
    for (int i = 0; i < request->nodes_size(); i++) {
        uint64_t node_id = request->nodes(i).nid();
        if (node_id == g_node_id)
            continue;
        _remote_nodes_involved[node_id] = new RemoteNodeInfo;
        // prepare request ensure all the nodes attached are rw
        _remote_nodes_involved[node_id]->is_readonly = false;
    }

#if EARLY_LOCK_RELEASE
    _cc_manager->retire(); // release lock after log is received
    dependency_semaphore->wait();
#endif

    // log vote if the entire txn is read-write
    if (request->nodes_size() != 0 && COMMIT_ALG != COORDINATOR_LOG) {
        string data = "[LSN] placehold:" + string(num_tuples * g_log_sz * 8, 'd');
        rpc_log_semaphore->incr();
        #if LOG_DEVICE == LOG_DVC_REDIS
            #if COMMIT_ALG == ONE_PC
            redis_client->log_if_ne_data(g_node_id, get_txn_id(), data);
            #else
            redis_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
            #endif  // ONE_PC
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
            #if COMMIT_ALG == ONE_PC
            azure_blob_client->log_if_ne_data(g_node_id, get_txn_id(), data);
            #else
            azure_blob_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
            #endif  // ONE_PC
        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
            #if COMMIT_ALG == ONE_PC
            redis_client->log_if_ne_data(g_node_id, get_txn_id(), data);
            #else
            redis_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
            #endif
            rpc_log_semaphore->wait();
            sendRemoteLogRequest(PREPARED, num_tuples * g_log_sz * 8,
                                 request->coord_id(),
                                 num_tuples == 0?
                                 SundialRequest::PREPARED_OK_RO :
                                 SundialRequest::PREPARED_OK);
        #endif  // LOG_DEVICE
        rpc_log_semaphore->wait();
    }

    // log msg no matter it is readonly or not
    SundialResponse::ResponseType response_type = SundialResponse::PREPARED_OK;
    if (num_tuples != 0) {
        // read-write
        _txn_state = PREPARED;
    } else {
        // readonly remote nodes
        _txn_state = COMMITTED;
        // release lock (for pessimistic) and delete accesses
        _cc_manager->cleanup(COMMIT);
        response_type = SundialResponse::PREPARED_OK_RO;
    }
    response->set_response_type( response_type);
#if !(COMMIT_VAR == NO_VARIANT || COMMIT_VAR == COLOCATE)
    if (request->nodes_size() != 0 && COMMIT_ALG != COORDINATOR_LOG &&
    g_num_storage_nodes > 0) {
        response->set_request_type(SundialResponse::DummyReply);
    }
#endif
    return rc;
}

RC
TxnManager::process_read_request(const SundialRequest* request,
                                    SundialResponse* response) {
    if (!glob_manager->active) {
        return FAIL;
    }

#if FAILURE_ENABLE
    // is already aborted by terminate request
    if (_txn_state != RUNNING) {
        response->set_response_type(SundialResponse::RESP_ABORT);
        return ABORT;
    }
#endif

    RC rc = RCOK;
    uint32_t num_tuples = request->tuple_data_size();
    num_tuples = request->read_requests_size();
    for (uint32_t i = 0; i < num_tuples; i++) {
        uint64_t key = request->read_requests(i).key();
        uint64_t index_id = request->read_requests(i).index_id();
        access_t access_type = (access_t)request->read_requests(i).access_type();

        INDEX * index = GET_WORKLOAD->get_index(index_id);
        set<row_t *> * rows = nullptr;
        rc = get_cc_manager()->index_read(index, key, rows, 1);
        row_t * row = *rows->begin();
        get_cc_manager()->remote_key += 1;
        rc = get_cc_manager()->get_row(row, access_type, key);
        if (rc == ABORT) {
            break;
        }
        uint64_t table_id = row->get_table_id();
        SundialResponse::TupleData * tuple = response->add_tuple_data();
        uint64_t tuple_size = row->get_tuple_size();
        tuple->set_key(key);
        tuple->set_table_id( table_id );
        tuple->set_size( tuple_size );
        tuple->set_access_type( access_type );
        tuple->set_index_id( index_id );
        tuple->set_data( get_cc_manager()->get_data(key, table_id), tuple_size );
    }

    if (rc == ABORT) {
#if FAILURE_ENABLE
        if(_txn_state != ABORTED) {
            _cc_manager->cleanup(ABORT);
            _txn_state = ABORTED;
        }
#else
	_cc_manager->cleanup(ABORT);
#if DEBUG_PRINT
        printf("[remote txn-%lu] abort and cleaned up\n", _txn_id);
#endif
         _txn_state = ABORTED;
#endif
        response->set_response_type( SundialResponse::RESP_ABORT );
    } else {
#if DEBUG_PRINT
        printf("[remote txn-%lu] read completed, do not release lock yet\n",
               _txn_id);
#endif
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

    State status = (rc == COMMIT)? COMMITTED : ABORTED;
    rpc_log_semaphore->incr();
    #if LOG_DEVICE == LOG_DVC_REDIS
    redis_client->log_async(g_node_id, get_txn_id(), status);
    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    azure_blob_client->log_async(g_node_id, get_txn_id(), status);
    #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
    redis_client->log_async(g_node_id, get_txn_id(), status);
    rpc_log_semaphore->wait();
    sendRemoteLogRequest(rc_to_state(rc), 1, request->coord_id(),
                         SundialRequest::ACK);
    #endif

    rpc_log_semaphore->wait();
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    _cc_manager->cleanup(rc);
    _finish_time = get_sys_clock();
#if FAILURE_ENABLE
    // termination protocol is called when timeout (i.e. receiving terminate
    // request)
    if (_terminate_time != 0) {
        INC_FLOAT_STATS(terminate_time, _finish_time - _terminate_time);
        INC_INT_STATS(num_affected_txn, 1);
        vector<double> &all =
                    glob_stats->_stats[GET_THD_ID]->term_latency;
        all.push_back(_finish_time - _terminate_time);
    }
#endif
    // OPTIMIZATION: release locks as early as possible.
    // No need to wait for this log since it is optional (shared log
    // optimization)
    response->set_response_type( SundialResponse::ACK );
#if !(COMMIT_VAR == NO_VARIANT || COMMIT_VAR == COLOCATE)
    if (g_num_storage_nodes > 0)
        response->set_request_type(SundialResponse::DummyReply);
#endif
    return rc;
}

RC
TxnManager::process_terminate_request(const SundialRequest* request,
                                     SundialResponse* response) {
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] terminate request\n", g_node_id,
                _txn_id);
#endif
    RC rc = RCOK;
    switch (_txn_state) {
        case RUNNING:
            // self has not voted yes, log abort and cleanup
            #if LOG_DEVICE == LOG_DVC_REDIS
            if (redis_client->log_sync(g_node_id, get_txn_id(), ABORTED)
            == FAIL) {
                return FAIL;
            }
            #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
            if (azure_blob_client->log_sync(g_node_id, get_txn_id(), ABORTED)
            == FAIL) {
                return FAIL;
            }
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
