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
    process_mdcc_local_phase1(rc, request->node_id(), false);
    // wait for ack from redis/azure
    rpc_log_semaphore->wait();
    // set response
    SundialResponse::ResponseType type = (rc == ABORT) ?
        SundialReponse::PREPARED_ABORT : SundialResponse::PREPARED_OK;
    response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
    return rc;

RC TxnManager::process_mdcc_2bfast(const int *request, int *response) {
    // both storage node and participant share the same function
#if NODE_TYPE == STORAGE_NODE
    // if acceptor, need to copy the access info to restore
    ((CC_MAN *) get_cc_manager())->restore_from_remote_request(request);
#endif
    // if participant (leader), no need.
    // validate occ
    RC rc = ((CC_MAN *) get_cc_manager())->validate();
    // log
    string data = "[LSN] placehold:" + std::string('d', request->tuple_data_size
                                                            () * g_log_sz * 8);
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
        SundialReponse::PREPARED_ABORT : SundialResponse::PREPARED_OK;
    response->set_request_type(SundialResponse::MDCC_Phase2bFast);
}

RC
TxnManager::process_mdcc_2bclassic(const SundialRequest* request,
                                   SundialResponse* response) {
    assert(NODE_TYPE == STORAGE_NODE);
    // log to redis
    string data = "[LSN] placehold:" + std::string('d', request->tuple_data_size
    () * g_log_sz * 8);
    State status = PREPARE
    rpc_log_semaphore->incr();
#if LOG_DEVICE == LOG_DVC_REDIS
    redis_client->log_async_data(g_node_id, get_txn_id(), status, data);
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    azure_blob_client->log_async_data(g_node_id, get_txn_id(), status, data);
#endif
    rpc_log_semaphore->wait();

    // send result (phase 2b) to coordinator
    response->set_request_type(SundialResponse::MDCC_Phase2bClassic);
    response->set_response_type(SundialResponse::ACK);
    // send to coordinator
    SundialRequest new_request;
    new_request.set_request_type(SundialRequest::MDCC_Phase2bReply);
    SundialResponse new_response;
    rpc_client->sendRequestAsync(this, request->node_id(), new_request,
                                 new_response, true);
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

void
TxnManager::process_mdcc_local_phase1(RC rc, uint64_t node_id, bool is_singlepart) {
  // used by participant (server): process_mdcc_2aclassic()
  // also used by coordinator (client, single partition): process_mdcc_singlepart
  replied_acceptors[g_node_id] = 0;
  // self also acts as an acceptor
  string data = "[LSN] placehold:" + std::string('d', num_local_write * g_log_sz * 8);
  // only incr as redis/azure will decrement
  SundialRequest::RequestType type;
  State status;
  if (!is_singlepart) {
    status = (rc == COMMIT) ? PREPARED : ABORTED;
#if BALLOT_TYPE == CLASSIC_BALLOT
    // participant (leader) for local partiton, send 2a to acceptors
    type = (rc == COMMIT) ? SundialRequest::MDCC_Phase2a : SundialRequest::MDCC_ABORT_REQ;
#else
    type = SundialRequest::MDCC_ProposeFast;
#endif
  } else {
    status = (rc == COMMIT)? COMMITTED : ABORTED;
    type = (rc == COMMIT)?
            SundialRequest::MDCC_COMMIT_REQ : SundialRequest::MDCC_ABORT_REQ;
  }
  // log to redis
  rpc_log_semaphore->incr();
#if LOG_DEVICE == LOG_DVC_REDIS
  redis_client->log_async_data(g_node_id, get_txn_id(), status, data);
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
  azure_blob_client->log_async_data(g_node_id, get_txn_id(), status, data);
#endif
  SundialRequest new_request;
  SundialResponse new_response;
  new_request.set_request_type(type);
  new_request.set_txn_id( get_txn_id() );
  new_request.set_node_id(node_id);
  assert(CC_ALG == OCC);
  ((CC_MAN *) _cc_manager)->build_local_req(new_request);
  for (size_t i = 0; i < g_num_storage_nodes; i++) {
    rpc_client->sendRequestAsync(this, i, new_request, new_response,
                                 true);
  }
}