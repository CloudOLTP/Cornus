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
#include "occ_manager.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
#include "row_lock.h"
#endif
#include "log.h"
#include "redis_client.h"
#include "azure_blob_client.h"


RC
TxnManager::process_commit_phase_singlepart(RC rc)
{
    if (rc == ABORT) {
        _store_procedure->txn_abort();
    }
    // create log record
    string data = "[LSN] placehold:" + string(num_local_write *
                                                       g_log_sz * 8, 'd');
#if EARLY_LOCK_RELEASE
    _cc_manager->retire(); // release lock after log is received
    // enforce dependency if early lock release, regardless of txn type
    dependency_semaphore->wait();
#endif
    // if logging didn't happen, process commit phase
    if (!is_read_only()) {
    #if LOG_DEVICE == LOG_DVC_REDIS
       redis_client->log_sync_data(g_node_id, get_txn_id(), rc_to_state(rc), data);
    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
       if (azure_blob_client->log_sync_data(g_node_id, get_txn_id(), rc_to_state(rc),
           data) == FAIL)
           return FAIL;
    #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
        // log locally
        redis_client->log_sync_data(g_node_id, get_txn_id(), rc_to_state(rc), data);
        sendRemoteLogRequest(rc_to_state(rc), num_local_write *  g_log_sz * 8);
        // wait for (1) remote log request sent to paxos leader (2) redis
        rpc_log_semaphore->wait(); // wait for redis
    #endif
    }
    _cc_manager->cleanup(rc);
    _finish_time = get_sys_clock();
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    return rc;
}

RC
TxnManager::process_2pc_phase1()
{
    // Start Two-Phase Commit
    _decision = COMMIT;

#if EARLY_LOCK_RELEASE
    _cc_manager->retire(); // release lock after log is received
    dependency_semaphore->wait();
#endif

    // if the entire txn is read-write, log to remote storage
    if (!is_txn_read_only() && COMMIT_ALG != COORDINATOR_LOG) {
        string data = "[LSN] placehold:" + string(num_local_write *
                g_log_sz * 8, 'd');
        rpc_log_semaphore->incr();
    #if COMMIT_ALG == ONE_PC
        #if LOG_DEVICE == LOG_DVC_REDIS
        redis_client->log_if_ne_data(g_node_id, get_txn_id(), data);
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        azure_blob_client->log_if_ne_data(g_node_id, get_txn_id(), data);
        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
        redis_client->log_if_ne_data(g_node_id, get_txn_id(), data);
        rpc_log_semaphore->wait();
        sendRemoteLogRequest(PREPARED, num_local_write * g_log_sz * 8);
        #endif
    #else
        #if LOG_DEVICE == LOG_DVC_REDIS
        redis_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        azure_blob_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
        redis_client->log_async_data(g_node_id, get_txn_id(), PREPARED, data);
        rpc_log_semaphore->wait();
        sendRemoteLogRequest(PREPARED, num_local_write * g_log_sz * 8);
        #endif
    #endif // COMMIT_ALG == ONE_PC
    }

    SundialRequest::NodeData * participant;
    int message_sent = 0;
    // send prepare request to participants
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        // if any failed or aborted, txn must abort, cannot enter this function
        assert(it->second->state == RUNNING);
        SundialRequest &request = it->second->request;
        SundialResponse &response = it->second->response;
        request.Clear();
        response.Clear();
        request.set_txn_id( get_txn_id() );
        request.set_request_type( SundialRequest::PREPARE_REQ);
        request.set_node_id( it->first );
        // attach coordinator
        participant = request.add_nodes();
        participant->set_nid(g_node_id);
        // attach participants
        if (!is_txn_read_only() || CC_ALG == OCC ) {
            for (auto itr = _remote_nodes_involved.begin(); itr !=
                _remote_nodes_involved.end(); itr++) {
                if (itr->second->is_readonly && CC_ALG != OCC)
                    continue;
                participant = request.add_nodes();
                participant->set_nid(it->first);
            }
        }
        ((CC_MAN *)_cc_manager)->build_prepare_req( it->first, request );
        if (rpc_client->sendRequestAsync(this, it->first, request, response)
        == FAIL) {
            return FAIL; // self is down, no msg can be sent out
        }
        message_sent++;
        rpc_semaphore->incr();
        // check if current node should crash
#if FAILURE_ENABLE
        if (message_sent != 1)
            continue;
        uint64_t ts = _worker_thread->get_execution_time();
        if (ts > g_failure_pt && (g_node_id == FAILURE_NODE)) {
            if (ATOM_CAS(glob_manager->active, true, false)) {
                #if DEBUG_PRINT || DEBUG_FAILURE
                printf("[node-%u, txn-%lu] node crashes (execution time = %.2f sec)\n",
                    g_node_id, _txn_id, ts / 1000000000.0);
                #endif
                glob_manager->failure_protocol();
            }
            return FAIL;
        }
#endif
    }

    // profile: # prepare phase
    INC_INT_STATS(num_prepare, 1);

    // wait for log if the txn is read/write
    // if coodinator log, will wait for data logging in next stage
    if (!is_txn_read_only() && COMMIT_ALG != COORDINATOR_LOG)
        rpc_log_semaphore->wait();

    // wait for vote
    rpc_semaphore->wait();
#if FAILURE_ENABLE
    // if all active vote yes but has failed node, run termination protocol
    if (_decision == FAIL) {
        // new decision is updated in termination protocol
		_decision = termination_protocol();
        if (_decision == FAIL)
            return FAIL; // self is down
    }
#endif
    _txn_state = PREPARED;
    return _decision;
}

void
TxnManager::handle_prepare_resp(SundialResponse* response) {
    switch (response->response_type()) {
        case SundialResponse::PREPARED_OK:
            _remote_nodes_involved[response->node_id()]->state =
                PREPARED;
            break;
        case SundialResponse::PREPARED_OK_RO:
            _remote_nodes_involved[response->node_id()]->state =
                COMMITTED;
            assert(_remote_nodes_involved[response->node_id()]->is_readonly);
            break;
        case SundialResponse::PREPARED_ABORT:
            _remote_nodes_involved[response->node_id()]->state =
                ABORTED;
            _decision = ABORT;
            break;
        case SundialResponse::RESP_FAIL:
            // remote node is down, run termination protocol
            _remote_nodes_involved[response->node_id()]->state =
                FAILED;
            // should not overwrite abort decision
            ATOM_CAS(_decision, COMMIT, FAIL);
            break;
        default:
            assert(false);
    }
}

RC
TxnManager::process_2pc_phase2(RC rc)
{
    bool remote_readonly = is_read_only() && (rc == COMMIT);
    if (remote_readonly && CC_ALG != OCC) {
        for (auto it = _remote_nodes_involved.begin();
             it != _remote_nodes_involved.end(); it++) {
            if (!(it->second->is_readonly)) {
                remote_readonly = false;
                break;
            }
        }
    }
    if (remote_readonly) { // no logging and remote message at all
        _finish_time = get_sys_clock();
        _cc_manager->cleanup(rc);
        _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
        return rc;
    }

    rpc_log_semaphore->incr();
    #if COMMIT_ALG == TWO_PC
        // 2pc: persistent decision
        #if LOG_DEVICE == LOG_DVC_REDIS
        redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        azure_blob_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
        redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
        rpc_log_semaphore->wait();
        sendRemoteLogRequest(rc_to_state(rc), 1);
        #endif
        // finish after log is stable.
        rpc_log_semaphore->wait();
        _finish_time = get_sys_clock();
    #elif COMMIT_ALG == ONE_PC
        // finish before sending out logs.
        _finish_time = get_sys_clock();
        #if LOG_DEVICE == LOG_DVC_REDIS
        rpc_log_semaphore->incr();
        redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc);
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        azure_blob_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
        redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc));
        rpc_log_semaphore->wait();
        sendRemoteLogRequest(rc_to_state(rc), 1);
        #endif
    #elif COMMIT_ALG == COORDINATOR_LOG
        // log all at once
        data = "[LSN] placehold:" + string(num_local_write * g_log_sz * 8, 'd');
        for (auto it = _remote_nodes_involved.begin();
             it != _remote_nodes_involved.end(); it++) {
            if (!(it->second->is_readonly)) {
                data += + string(num_local_write * g_log_sz * 8, 'd');
            }
        }
        // 2pc: persistent decision
        #if LOG_DEVICE == LOG_DVC_REDIS
        redis_client->log_async_data(g_node_id, get_txn_id(), rc_to_state(rc), data);
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        azure_blob_client->log_async_data(g_node_id, get_txn_id(), rc_to_state(rc), data);
        #elif LOG_DEVICE == LOG_DVC_CUSTOMIZED
        redis_client->log_async_data(g_node_id, get_txn_id(), rc_to_state(rc), data);
        rpc_log_semaphore->wait();
        sendRemoteLogRequest(rc_to_state(rc), data.length());
        #endif
        // finish after log is stable.
        rpc_log_semaphore->wait();
        _finish_time = get_sys_clock();
    #endif


    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        // No need to run this phase if the remote sub-txn has already committed
        // or aborted.
        if (it->second->state == ABORTED || it->second->state == COMMITTED ||
        it->second->state == FAILED)
            continue;
        SundialRequest &request = it->second->request;
        SundialResponse &response = it->second->response;
        request.Clear();
        response.Clear();
        request.set_txn_id( get_txn_id() );
        request.set_node_id( it->first );
        SundialRequest::RequestType type = (rc == COMMIT)?
            SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
        request.set_request_type( type );
        rpc_semaphore->incr();
        if (rpc_client->sendRequestAsync(this, it->first, request, response)
        == FAIL) {
            return FAIL;
        }
    }

    // OPTIMIZATION: release locks as early as possible.
    // No need to wait for this log since it is optional (shared log optimization)
#if COMMIT_ALG == ONE_PC
    rpc_log_semaphore->wait();
#endif
    _cc_manager->cleanup(rc);
    rpc_semaphore->wait();
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    return rc;
}


// For Read Request
// ====================

RC
TxnManager::send_remote_read_request(uint64_t node_id, uint64_t key, uint64_t index_id,
                                     uint64_t table_id, access_t access_type)
{
    _is_single_partition = false;
    if ( _remote_nodes_involved.find(node_id) == _remote_nodes_involved.end() ) {
        _remote_nodes_involved[node_id] = new RemoteNodeInfo;
        _remote_nodes_involved[node_id]->state = RUNNING;
        _remote_nodes_involved[node_id]->is_readonly = true;
    }

    SundialRequest &request = _remote_nodes_involved[node_id]->request;
    SundialResponse &response = _remote_nodes_involved[node_id]->response;
    request.Clear();
    response.Clear();
    request.set_txn_id( get_txn_id() );
    request.set_request_type( SundialRequest::READ_REQ );

    SundialRequest::ReadRequest * read_request = request.add_read_requests();
    read_request->set_key(key);
    read_request->set_index_id(index_id);
    read_request->set_access_type(access_type);
    request.set_node_id(node_id);
    if (rpc_client->sendRequest(node_id, request, response) == FAIL)
        return FAIL;

    if (access_type != RD) {
        _remote_nodes_involved[node_id]->is_readonly = false;
        set_txn_read_write();
    }

    // handle RPC response
    if (response.response_type() == SundialResponse::RESP_OK) {
        ((CC_MAN *)_cc_manager)->process_remote_read_response(node_id,
                                                         access_type, response);
        return RCOK;
    } else if (response.response_type() == SundialResponse::RESP_ABORT) {
        _remote_nodes_involved[node_id]->state = ABORTED;
        _is_remote_abort = true;
        return ABORT;
    } else {
        assert(response.response_type() == SundialResponse::RESP_FAIL);
        _remote_nodes_involved[node_id]->state = FAILED;
        _is_remote_abort = true;
        return ABORT;
    }
}

RC
TxnManager::send_remote_package(std::map<uint64_t, vector<RemoteRequestInfo *> > &remote_requests)
{
    _is_single_partition = false;
    for (auto it = remote_requests.begin(); it != remote_requests.end(); it ++) {
        uint64_t node_id = it->first;
        if ( _remote_nodes_involved.find(node_id) == _remote_nodes_involved.end() ) {
            _remote_nodes_involved[node_id] = new RemoteNodeInfo;
            _remote_nodes_involved[node_id]->state = RUNNING;
            _remote_nodes_involved[node_id]->is_readonly = true;
        }
        SundialRequest &request = _remote_nodes_involved[node_id]->request;
        request.set_txn_id( get_txn_id() );
        request.set_node_id( node_id );
        request.set_request_type( SundialRequest::READ_REQ );
        for (auto it2 = it->second.begin(); it2 != it->second.end(); it2 ++) {
            SundialRequest::ReadRequest * read_request = request.add_read_requests();
            read_request->set_key((*it2)->key);
            read_request->set_index_id((*it2)->index_id);
            read_request->set_access_type((*it2)->access_type);
        	if ((*it2)->access_type != RD) {
                _remote_nodes_involved[node_id]->is_readonly = false;
                set_txn_read_write();
            }
        }
    }

    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        if (rpc_client->sendRequestAsync(this, it->first, it->second->request,
            it->second->response) == FAIL) {
            // self if fail, stop working and return
            return FAIL;
        } else {
            rpc_semaphore->incr();
        }
    }
    rpc_semaphore->wait();
    RC rc = RCOK;
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        SundialResponse &response = it->second->response;
        if (response.response_type() == SundialResponse::RESP_OK) {
            ((CC_MAN *)_cc_manager)->process_remote_read_response(it->first, response);
        } else if (response.response_type() == SundialResponse::RESP_ABORT) {
            _remote_nodes_involved[it->first]->state = ABORTED;
            _is_remote_abort = true;
            rc = ABORT;
        } else {
            assert(response.response_type() == SundialResponse::RESP_FAIL);
            _remote_nodes_involved[it->first]->state = FAILED;
            _is_remote_abort = true;
            rc = ABORT;
        }
    }
    return rc;
}

void TxnManager::sendRemoteLogRequest(State state, uint64_t log_data_size) {
#if !COLOCATE
    if (g_num_storage_nodes == 0)  {
        return;
    }
    // send log request to leader of paxos, which is the storage node
    // with the same id as current compute node id
    // need to make sure # storage >= # compute
    size_t node_id = g_node_id;
    if (node_id >= g_num_storage_nodes) {
        node_id = node_id % g_num_storage_nodes;
    }
    txn_requests_[node_id].set_request_type
        (SundialRequest::PAXOS_LOG);
    txn_requests_[node_id].set_txn_id(get_txn_id());
    txn_requests_[node_id].set_log_data_size(log_data_size);
    txn_requests_[node_id].set_txn_state(state);
    txn_requests_[node_id].set_semaphore(reinterpret_cast<uint64_t>(rpc_log_semaphore));
    rpc_log_semaphore->incr();
    rpc_client->sendRequestAsync(this,
                                 node_id,
                                 txn_requests_[node_id],
                                 txn_responses_[node_id],
                                 true);
#else
    // send log request
    size_t sent = 0;
    for (size_t i = 0; i < g_num_storage_nodes; i++) {
        // XXX(zhihan): only send to # quorum of nodes to avoid null ref error
        if (sent == g_quorum)
            break;
        rpc_log_semaphore->incr();
        txn_requests_[i].set_request_type
          (SundialRequest::PAXOS_LOG_COLOCATE);
        txn_requests_[i].set_txn_id(get_txn_id());
        txn_requests_[i].set_log_data_size(log_data_size);
        txn_requests_[i].set_txn_state(state);
        txn_requests_[i].set_semaphore(reinterpret_cast<uint64_t>
        (rpc_log_semaphore));
        rpc_client->sendRequestAsync(this,
                                   i,
                                   txn_requests_[i],
                                   txn_responses_[i],
                                   true);
        sent++;
    }
#endif

}

void
TxnManager::sendReplicateRequest(State state, uint64_t log_data_size) {
    // log to local redis
    string data = "[LSN] placehold:" + string(log_data_size, 'd');
    // log to peer
    size_t sent = 0;
    for (size_t i = 0; i < g_num_storage_nodes; i++) {
        if (i == g_node_id)
            continue;
        // XXX(zhihan): only send to # quorum of nodes to avoid null ref error
        if (sent == g_quorum)
            break;
        rpc_log_semaphore->incr();
        txn_requests_[i].set_request_type
                (SundialRequest::PAXOS_REPLICATE);
        txn_requests_[i].set_txn_id(get_txn_id());
        txn_requests_[i].set_log_data_size(log_data_size);
        txn_requests_[i].set_txn_state(state);
        txn_requests_[i].set_semaphore(reinterpret_cast<uint64_t>(rpc_log_semaphore));
        rpc_client->sendRequestAsync(this,
                                     i,
                                     txn_requests_[i],
                                     txn_responses_[i],
                                     true);
        sent++;
    }
    // use sync request to avoid semaphore updating
    redis_client->log_sync_data(g_node_id, get_txn_id(), state, data);
    // wait for quorum
    rpc_log_semaphore->wait();
}

