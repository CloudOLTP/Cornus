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
    // first ensure serializability
    RC rc = ((CC_MAN *) get_cc_manager())->validate();
    // then send result to acceptors to persist and log
    // IMPORTATNT: need to pass the node id of sender (coordinator)
    num_local_write = request->tuple_data_size();
    process_mdcc_local_phase1(rc, request->coord_id(), false);
    // wait for ack from redis/azure
    rpc_log_semaphore->wait();
    // set response
    SundialResponse::ResponseType type = (rc == ABORT) ?
                                         SundialResponse::PREPARED_ABORT
                                                       : SundialResponse::PREPARED_OK;
    response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
    response->set_response_type(type);
    response->set_node_id(request->node_id());
    response->set_node_type(SundialResponse::PARTICIPANT);
    response->set_txn_id(get_txn_id());
    return rc;
}

RC TxnManager::process_mdcc_2bfast(const SundialRequest *request,
                                   SundialResponse *response) {
    // both storage node and participant share the same function
#if NODE_TYPE == STORAGE_NODE
    // if acceptor, need to copy the access info to restore
    ((CC_MAN *) get_cc_manager())->restore_from_remote_request(request);
    response->set_node_type(SundialResponse::STORAGE);
#else
    response->set_node_type(SundialResponse::PARTICIPANT);
#endif
    // if participant (leader), no need.
    // validate occ
    RC rc = ((CC_MAN *) get_cc_manager())->validate();
    // log
    string data = "[LSN] placehold:" + std::string(request->tuple_data_size
        () * g_log_sz * 8, 'd');
    rpc_log_semaphore->incr();
    State state = (rc == ABORT) ? PREPARED : ABORTED;
#if LOG_DEVICE == LOG_DVC_REDIS
    redis_client->log_async_data(g_node_id, get_txn_id(), state, data);
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    azure_blob_client->log_async_data(g_node_id, get_txn_id(), state, data);
#endif
    rpc_log_semaphore->wait();
    // set response
    SundialResponse::ResponseType type = (rc == ABORT) ?
        SundialResponse::PREPARED_ABORT : SundialResponse::PREPARED_OK;
    response->set_request_type(SundialResponse::MDCC_Phase2bFast);
    response->set_response_type(type);
    response->set_node_id(request->node_id());
    return rc;
}

void
TxnManager::process_mdcc_2bclassic(const SundialRequest* request,
                                   SundialResponse* response) {
    // if abort request, will not call this function, so we assume commit
    assert(NODE_TYPE == STORAGE_NODE);
    // log to redis
    string data = "[LSN] placehold:" + std::string(request->tuple_data_size
    () * g_log_sz * 8, 'd');
    rpc_log_semaphore->incr();
#if LOG_DEVICE == LOG_DVC_REDIS
    redis_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    azure_blob_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
#endif
    rpc_log_semaphore->wait();
    response->set_node_id(request->node_id());
    response->set_txn_id(request->txn_id());
    if (request->node_type() == SundialRequest::COORDINATOR) {
        response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
        response->set_response_type(SundialResponse::PREPARED_OK);
        response->set_node_type(SundialResponse::STORAGE);
    } else {
        // send result (phase 2b) to leader
        response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
        response->set_response_type(SundialResponse::ACK);
        response->set_node_type(SundialResponse::STORAGE);
        // send to coordinator
        auto nid = request->node_id();
        txn_requests_[nid].set_request_type(SundialRequest::MDCC_Phase2bReply);
        txn_requests_[nid].set_node_type(SundialRequest::STORAGE);
        txn_requests_[nid].set_node_id(request->node_id());
        txn_requests_[nid].set_txn_id(request->txn_id());
        rpc_client->sendRequestAsync(this, request->coord_id(), txn_requests_[nid],
                                     txn_responses_[nid], false);
    }
}


void
TxnManager::process_mdcc_2bclassic_abort(const SundialRequest* request,
                                   SundialResponse* response) {
    // if abort request, will not call this function, so we assume commit
    assert(NODE_TYPE == STORAGE_NODE);
    // log to redis
    rpc_log_semaphore->incr();
#if LOG_DEVICE == LOG_DVC_REDIS
    redis_client->log_async(g_node_id, get_txn_id(), ABORTED);
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    azure_blob_client->log_async(g_node_id, get_txn_id(), ABORTED);
#endif
    rpc_log_semaphore->wait();
    response->set_node_id(request->node_id());
    response->set_txn_id(request->txn_id());
    if (request->node_type() == SundialRequest::COORDINATOR) {
        response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
        response->set_response_type(SundialResponse::PREPARED_ABORT);
        response->set_node_type(SundialResponse::STORAGE);
    } else {
        // send result (phase 2b) to leader
        response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
        response->set_response_type(SundialResponse::ACK);
        response->set_node_type(SundialResponse::STORAGE);
        // send to coordinator
        auto nid = request->node_id();
        txn_requests_[nid].set_request_type
            (SundialRequest::MDCC_Phase2bReplyAbort);
        txn_requests_[nid].set_node_type(SundialRequest::STORAGE);
        txn_requests_[nid].set_node_id(request->node_id());
        txn_requests_[nid].set_txn_id(request->txn_id());
        rpc_client->sendRequestAsync(this, request->coord_id(),
                                     txn_requests_[nid],
                                     txn_responses_[nid], false);
    }
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
#if DEBUG_PRINT
    printf("[node-%u txn-%lu] finish mdcc visibility\n",
                 g_node_id, _txn_id);
#endif
    return RCOK;
}

void
TxnManager::process_mdcc_local_phase1(RC rc, uint64_t node_id, bool is_singlepart) {
  // used by participant (server): process_mdcc_2aclassic()
  // also used by coordinator (client, single partition): process_mdcc_singlepart
  replied_acceptors[g_node_id] = 0;
  // self also acts as an acceptor
  string data = "[LSN] placehold:" + std::string(num_local_write * g_log_sz * 8, 'd');
  // only incr as redis/azure will decrement
  SundialRequest::RequestType type;
  State status;
  if (!is_singlepart) {
    status = (rc == COMMIT) ? PREPARED : ABORTED;
#if BALLOT_TYPE == BALLOT_CLASSIC
    // participant (leader) for local partiton, send 2a to acceptors
    type = (rc == COMMIT) ? SundialRequest::MDCC_Phase2a :
        SundialRequest::MDCC_Phase2aAbort;
#else
    type = SundialRequest::MDCC_ProposeFast;
#endif
  } else {
    status = (rc == COMMIT)? COMMITTED : ABORTED;
    type = (rc == COMMIT)?
            SundialRequest::MDCC_SINGLEPART_COMMIT :
            SundialRequest::MDCC_SINGLEPART_ABORT;
  }
  // log to redis
  rpc_log_semaphore->incr();
#if LOG_DEVICE == LOG_DVC_REDIS
  redis_client->log_async_data(g_node_id, get_txn_id(), status, data);
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
  azure_blob_client->log_async_data(g_node_id, get_txn_id(), status, data);
#endif
  for (size_t i = 0; i < g_num_storage_nodes; i++) {
    txn_requests_[i].set_request_type(type);
    txn_requests_[i].set_txn_id( get_txn_id() );
    txn_requests_[i].set_coord_id(node_id);
    txn_requests_[i].set_node_id(g_node_id);
    if (g_node_id == node_id)
      txn_requests_[i].set_node_type(SundialRequest::COORDINATOR);
    else
      txn_requests_[i].set_node_type(SundialRequest::PARTICIPANT);
    assert(CC_ALG == OCC);
#if COMMIT_ALG == MDCC
    ((CC_MAN *) _cc_manager)->build_local_req(txn_requests_[i]);
#endif
    rpc_client->sendRequestAsync(this, i, txn_requests_[i], txn_responses_[i],
                                 true);
  }
}
