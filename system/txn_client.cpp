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


RC
TxnManager::process_commit_phase_singlepart(RC rc)
{
    if (rc == ABORT) {
        _store_procedure->txn_abort();
	}
#if LOG_LOCAL
    // TODO. Changed from design A to design B
    // [Design A] the worker thread is detached from the transaction once the log
    // buffer is filled. The logging thread handles the rest of the commit.
    // [Design B] the worker thread sleeps until logging finishes and handles the
    // rest of the commit itself.
    // Design B is simpler than A for 2PC. Since it is hard to detach a
    // transaction from an RPC thread during an RPC call.
    // TODO Need to carefully test performance to make sure design B is not
    // slower than design A.
    if (rc == ABORT) {
        _cc_manager->cleanup(rc);
        _txn_state = ABORTED;
        rc = ABORT;
    } else { // rc == COMMIT
        char * log_record = NULL;
        uint32_t log_record_size = _cc_manager->get_log_record(log_record);
        if (log_record_size > 0) {
            assert(log_record);
            log_semaphore->incr();
            log_manager->log(this, log_record_size, log_record);
            delete [] log_record;
            // The worker thread will be waken up by the logging thread after
            // the logging operation finishes.
        }
  #if CONTROLLED_LOCK_VIOLATION
        //INC_INT_STATS(num_precommits, 1);
        _cc_manager->process_precommit_phase_coord();
  #endif
        _precommit_finish_time = get_sys_clock();
  #if ENABLE_ADMISSION_CONTROL
        // now the transaction has precommitted, the current thread is inactive,
        // need to increase the quota of another thread.
        //uint64_t wakeup_thread_id = glob_manager->next_wakeup_thread() % g_num_worker_threads;
        //glob_manager->get_worker_thread( wakeup_thread_id )->incr_quota();
        glob_manager->wakeup_next_thread();
  #endif
        // For read-write transactions, this waits for logging to complete.
        // For read-only transactions, this waits for dependent transactions to
        // commit (CLV only).
        uint64_t tt = get_sys_clock();

        log_semaphore->wait();

        _log_ready_time = get_sys_clock();
        INC_FLOAT_STATS(log_ready_time, get_sys_clock() - tt);

        dependency_semaphore->wait();
        INC_FLOAT_STATS(dependency_ready_time, get_sys_clock() - tt);

        rc = COMMIT;
        _cc_manager->cleanup(rc);
        _txn_state = COMMITTED;
    }
#else
    // if logging didn't happen, process commit phase
#if LOG_REMOTE
    if (!is_read_only()) {
    #if LOG_DEVICE == LOG_DVC_NATIVE
        SundialRequest::RequestType type = rc == COMMIT ? SundialRequest::LOG_COMMIT_REQ :
                SundialRequest::LOG_ABORT_REQ;
        send_log_request(g_storage_node_id, type);
        rpc_log_semaphore->wait();
    #elif LOG_DEVICE == LOG_DVC_REDIS
       string data = "[LSN] placehold:" + string('d', num_local_write *
                g_log_sz * 8);
       if (redis_client->log_sync_data(g_node_id, get_txn_id(), rc_to_state(rc),
           data) == FAIL)
           return FAIL;
    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
       string data = "[LSN] placehold:" + string('d', num_local_write *
                g_log_sz * 8);
       if (azure_blob_client->log_sync_data(g_node_id, get_txn_id(), rc_to_state(rc),
           data) == FAIL)
           return FAIL;
    #endif
    }
#endif
    _cc_manager->cleanup(rc);
    _finish_time = get_sys_clock();
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
#endif
    return rc;
}

RC
TxnManager::process_2pc_phase1()
{
    // Start Two-Phase Commit
    _decision = COMMIT;

#if LOG_LOCAL
    char * log_record = NULL;
    uint32_t log_record_size = _cc_manager->get_log_record(log_record);
    if (log_record_size > 0) {
        assert(log_record);
        log_semaphore->incr();
        log_manager->log(this, log_record_size, log_record);
        delete [] log_record;
    }
  #if CONTROLLED_LOCK_VIOLATION
    _cc_manager->process_precommit_phase_coord();
  #endif
    _precommit_finish_time = get_sys_clock();
  #if ENABLE_ADMISSION_CONTROL
    //uint64_t wakeup_thread_id = glob_manager->next_wakeup_thread() % g_num_worker_threads;
    //glob_manager->get_worker_thread( wakeup_thread_id )->incr_quota();
    glob_manager->wakeup_next_thread();
  #endif
#endif
#if LOG_REMOTE
        // asynchronously log prepare for this node
    if (!is_read_only()) {
    #if LOG_DEVICE == LOG_DVC_NATIVE
        SundialRequest::RequestType type = SundialRequest::LOG_YES_REQ; // always vote yes for now
        send_log_request(g_storage_node_id, type);
    #elif LOG_DEVICE == LOG_DVC_REDIS
        string data = "[LSN] placehold:" + string('d', num_local_write *
                g_log_sz * 8);
        rpc_log_semaphore->incr();
        #if COMMIT_ALG == ONE_PC
        if (redis_client->log_if_ne_data(g_node_id, get_txn_id(), data) ==
        FAIL) {
            return FAIL;
        }
        #else
        if (redis_client->log_async_data(g_node_id, get_txn_id(), PREPARED,
            data) == FAIL) {
            return FAIL;
        }
        #endif
    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        string data = "[LSN] placehold:" + string('d', num_local_write *
                g_log_sz * 8);
        rpc_log_semaphore->incr();
        #if COMMIT_ALG == ONE_PC
        if (azure_blob_client->log_if_ne_data(g_node_id, get_txn_id(), data) ==
        FAIL) {
            return FAIL;
        }
        #else
        if (azure_blob_client->log_async_data(g_node_id, get_txn_id(), PREPARED,
            data) == FAIL) {
            return FAIL;
        }
        #endif
    #endif
    }
#endif

    SundialRequest::NodeData * participant;
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
        // XXX(zhihan): attach participant list
        // attach coordinator
        participant = request.add_nodes();
        participant->set_nid(g_node_id);
        // attach participants
        for (auto itr = _remote_nodes_involved.begin(); itr !=
            _remote_nodes_involved.end(); itr ++) {
            if (itr->second->is_readonly)
                continue;
            participant = request.add_nodes();
            participant->set_nid(it->first);
        }
        ((LockManager *)_cc_manager)->build_prepare_req( it->first, request );
        if (rpc_client->sendRequestAsync(this, it->first, request, response)
        == FAIL) {
            return FAIL; // self is down, no msg can be sent out
        } else {
            rpc_semaphore->incr();
        }
    }

    // wait for log prepare to return
#if LOG_LOCAL
    log_semaphore->wait();
#endif

    // check if current node should crash
#if FAILURE_ENABLE
	uint64_t ts = _worker_thread->get_execution_time();
    if (ts > g_failure_pt && (g_node_id == FAILURE_NODE)) {
        if (ATOM_CAS(glob_manager->active, true, false)) {
#if DEBUG_PRINT || DEBUG_FAILURE
			printf("[node-%u, txn-%lu] node crashes (execution time = %.2f sec)\n", g_node_id, _txn_id, ts / 1000000000.0);
#endif
            glob_manager->failure_protocol();
        }
        return FAIL;
    }
#endif

    // profile: # prepare phase
    INC_INT_STATS(int_debug3, 1);

    // wait for log
    if (!is_read_only()) {
        rpc_log_semaphore->wait();
    }
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
#if LOG_LOCAL
    std::string record = std::to_string(_txn_id);
    char * log_record = (char *)record.c_str();
    uint32_t log_record_size = record.length();
    log_semaphore->incr();
    log_manager->log(this, log_record_size, log_record);
    // OPTIMIZATION: perform local logging and commit request in parallel
    // log_semaphore->wait();
#endif

    bool remote_readonly = is_read_only() && (rc == COMMIT);
    if (remote_readonly) {
        for (auto it = _remote_nodes_involved.begin();
             it != _remote_nodes_involved.end(); it++) {
            if (!(it->second->is_readonly)) {
                remote_readonly = false;
                break;
            }
        }
    }
    if (remote_readonly) { // no logging and remote message at all
        _cc_manager->cleanup(rc);
        _finish_time = get_sys_clock();
        _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
        return rc;
    }

#if LOG_REMOTE
    #if COMMIT_ALG == TWO_PC
        // 2pc: persistent decision
        uint64_t starttime = get_sys_clock();
        #if LOG_DEVICE == LOG_DVC_NATIVE
            SundialRequest::RequestType type = rc == COMMIT ? SundialRequest::LOG_COMMIT_REQ :
                    SundialRequest::LOG_ABORT_REQ;
            send_log_request(g_storage_node_id, type);
            rpc_log_semaphore->wait();
        #elif LOG_DEVICE == LOG_DVC_REDIS
            rpc_log_semaphore->incr();
            if (redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc)) ==
            FAIL) {
                return FAIL;
            }
            rpc_log_semaphore->wait();
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
            rpc_log_semaphore->incr();
            if (azure_blob_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc)) ==
            FAIL) {
                return FAIL;
            }
            rpc_log_semaphore->wait();
        #endif

        // profile: time spent on a sync log
        INC_FLOAT_STATS(time_debug4, get_sys_clock() - starttime);
        INC_INT_STATS(int_debug4, 1);
    #elif COMMIT_ALG == ONE_PC
        #if LOG_DEVICE == LOG_DVC_NATIVE
            SundialRequest::RequestType type = rc == COMMIT ? SundialRequest::LOG_COMMIT_REQ :
                    SundialRequest::LOG_ABORT_REQ;
            send_log_request(g_storage_node_id, type);
        #elif LOG_DEVICE == LOG_DVC_REDIS
            rpc_log_semaphore->incr();
            if (redis_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc)) ==
            FAIL) {
                return FAIL;
            }
        #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
            rpc_log_semaphore->incr();
            if (azure_blob_client->log_async(g_node_id, get_txn_id(), rc_to_state(rc)) ==
            FAIL) {
                return FAIL;
            }
        #endif
        INC_INT_STATS(int_debug4, 1);
    #endif
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

    _finish_time = get_sys_clock();
    // OPTIMIZATION: release locks as early as possible.
    // No need to wait for this log since it is optional (shared log optimization)
    dependency_semaphore->wait();
    log_semaphore->wait();
#if LOG_REMOTE && COMMIT_ALG == ONE_PC
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

    if (access_type != RD)
        _remote_nodes_involved[node_id]->is_readonly = false;

    // handle RPC response
    if (response.response_type() == SundialResponse::RESP_OK) {
        ((LockManager *)_cc_manager)->process_remote_read_response(node_id, access_type, response);
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
        	if ((*it2)->access_type != RD)
            	_remote_nodes_involved[node_id]->is_readonly = false;
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
            ((LockManager *)_cc_manager)->process_remote_read_response(it->first, response);
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


// For Logging
// ====================
RC
TxnManager::send_log_request(uint64_t node_id, SundialRequest::RequestType type)
{
    if ( _log_nodes_involved.find(node_id) == _log_nodes_involved.end() ) {
        _log_nodes_involved[node_id] = new RemoteNodeInfo;
        _log_nodes_involved[node_id]->state = RUNNING;
    }
    SundialRequest &request = _log_nodes_involved[node_id]->request;
    SundialResponse &response = _log_nodes_involved[node_id]->response;
    request.Clear();
    response.Clear();
    request.set_txn_id( get_txn_id() );
    request.set_request_type( type );

    char * log_record = NULL;
    uint32_t log_record_size = 0;
    if (type == SundialRequest::LOG_COMMIT_REQ) {   // only commit need to log modified data
        log_record_size = _cc_manager->get_log_record(log_record);
        request.set_log_data(log_record);
    }
    request.set_log_data_size(log_record_size);
    rpc_log_semaphore->incr();
    rpc_client->sendRequestAsync(this, node_id, request, response);
    return RCOK;
}


