//
// Created by Zhihan Guo on 7/12/22.
//

#include <cmath>
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
#include "occ_manager.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
#include "row_lock.h"
#endif
#include "log.h"
#include "redis_client.h"
#include "azure_blob_client.h"

// MDCC server (leader)
RC
TxnManager::process_mdcc_2aclassic(const SundialRequest* request,
                                   SundialResponse* response) {
    // send 2a to acceptors
    SundialRequest new_request;
    SundialResponse new_response;
    new_request.set_request_type(SundialRequest::MDCC_Phase2a);
    // copy the rest of data
    new_request.set_txn_id( get_txn_id() );
    // node id representing leader identity (txn_id, leader_id) identifies a
    // paxos cluster
    new_request.set_node_id(request->node_id());
    auto num_tuples = request->tuple_data_size();
    for (int j = 0; j < num_tuples; j++) {
        auto tuple = new_request.add_tuple_data();
        tuple->set_key(request->tuple_data(j).key());
        tuple->set_table_id(request->tuple_data(j).key());
        tuple->set_size(request->tuple_data(j).size());
        tuple->set_data( get_cc_manager()->get_data(request->tuple_data(j).key(),
                                                    request->tuple_data(j).key()),
                         request->tuple_data(j).size());
        tuple->set_access_type(request->tuple_data(j).access_type());
        tuple->set_version(request->tuple_data(j).version());
        SundialRequest::MdccData * mdcc_data = new_request.add_mdcc_data();
        mdcc_data->set_ballot(request->mdcc_data(0).ballot());
    }
    for (size_t i = 0; i < g_num_storage_nodes; i++) {
        rpc_client->sendRequestAsync(this, i, new_request, new_response,
                                    true);
    }
    return process_mdcc_2bclassic(&new_request, response);
}

// MDCC server (leader / acceptor)
RC
TxnManager::process_mdcc_2bclassic(const SundialRequest* request,
                                   SundialResponse* response) {
    #if NODE_TYPE == STORAGE_NODE
    // if acceptor, need to copy the access info
    restore_from_remote_request(request);
    #endif
    // if participant (leader), no need.
    // validate occ
    RC rc = ((CC_MAN *) get_cc_manager())->validate();
    // log
    string data = "[LSN] placehold:" + std::string('d', request->tuple_data_size
    () * g_log_sz * 8);
    rpc_log_semaphore->incr();
    if (rc != ABORT) {
#if LOG_DEVICE == LOG_DVC_REDIS
        if (redis_client->log_async_data(g_node_id, get_txn_id(),
                                         PREPARED, data) == FAIL) {
            return FAIL;
        }
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        if (azure_blob_client->log_async_data(g_node_id, get_txn_id(),
        PREPARED, data) == FAIL) {
            return FAIL;
        }
#endif
    }
    rpc_log_semaphore->wait();
    // if self participant: send result (phase 2b) to coordinator
    response->set_txn_id(request->txn_id());
    response->set_node_id(request->node_id());
    #if NODE_TYPE == STORAGE_NODE
    response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
    response->set_response_type(SundialResponse::ACK);
    // send to coordinator
    SundialRequest new_request;
    new_request->set_request_type(SundialRequest::MDCC_Phase2bReply);
    SundialResponse new_response;
    rpc_client->sendRequestAsync(this, request->node_id(), new_request,
                                 new_response, true);
    #else
    response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
    if (rc == ABORT) {
        response->set_response_type
            (SundialResponse::PREPARED_ABORT);
    } else {
        response->set_response_type
            (SundialResponse::PREPARED_OK);
    }
    #endif
    return rc;
}

RC
TxnManager::process_mdcc_visibility(const SundialRequest *request,
                                    SundialResponse *response,
                                    RC rc) {
    // log to stable storage
    // 2pc: persistent decision
    rpc_log_semaphore->incr();
    #if LOG_DEVICE == LOG_DVC_REDIS
    redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    azure_blob_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
    #endif
    ((CC_MAN *) get_cc_manager())->cleanup(rc);
    rpc_log_semaphore->wait();
    // finish after log is stable.
    _finish_time = get_sys_clock();
    response->set_request_type(SundialResponse::MDCC_Visibility);
    response->set_response_type(SundialResponse::ACK);
    return RCOK;
}