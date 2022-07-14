//
// Created by Zhihan Guo on 7/12/22.
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
#include "occ_manager.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
#include "row_lock.h"
#endif
#include "log.h"
#include "redis_client.h"
#include "azure_blob_client.h"


// MDCC client (coordinator)

RC TxnManager::process_mdcc() {
      // send proposal for all updates
      for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved
                                                               .end(); it ++) {
          SundialRequest::NodeData * participant;
            if (BALLOT_TYPE == BALLOT_CLASSIC) {
              // send proposal to leader (participant)
                SundialRequest &request = it->second->request;
                SundialResponse &response = it->second->response;
                request.Clear();
                response.Clear();
                request.set_txn_id( get_txn_id() );
                request.set_request_type( SundialRequest::MDCC_Propose );
                request.set_node_id( it->first );
                // attach coordinator
                participant = request.add_nodes();
                participant->set_nid(g_node_id);
                // attach participants
                for (auto itr = _remote_nodes_involved.begin(); itr !=
                    _remote_nodes_involved.end(); itr++) {
                    participant = request.add_nodes();
                    participant->set_nid(it->first);
                }
                // build request
                ((CC_MAN *)_cc_manager)->build_prepare_req( it->first, request );
                if (rpc_client->sendRequestAsync(this, it->first, request, response)
                    == FAIL) {
                    return FAIL; // self is down, no msg can be sent out
                }
                // TODO: increase msg count for corresponding nodes
                // create a char array of # participants involved
                // each count should not exceed g_num_storage_nodes + 1
                // in the txn class since the response handler need to update it
            } else {
              // TODO: send proposal to acceptors (participant + storage nodes)
            }
      }
      // process learned options
      //  iterate through the char array for each partition
      //  wait for quorum
      //  check one response, if prepared no, abort
      return RCOK;
}

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
    new_request.set_node_id(g_node_id);
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
        rpc_semaphore++;
    }
    return process_mdcc_2bclassic(&new_request, response);
}

// MDCC server (leader / acceptor)
RC
TxnManager::process_mdcc_2bclassic(const SundialRequest* request,
                                   SundialResponse* response) {
    // TODO(on storage node): if acceptor, need to copy the access to current
    //  access
    // if participant (leader), no need.
    // validate occ
    RC rc = ((CC_MAN *) get_cc_manager())->validate();
    // send result (phase 2b) to coordinator
    if (rc == ABORT) {
        response->set_response_type
            (SundialResponse::PREPARED_ABORT);
    } else {
        response->set_response_type
            (SundialResponse::PREPARED_OK);
    }
    return rc;
}
