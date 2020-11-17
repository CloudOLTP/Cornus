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

// TODO. cleanup the accesses related malloc code.

TxnManager::TxnManager(QueryBase * query, WorkerThread * thread)
{
    _store_procedure = GET_WORKLOAD->create_store_procedure(this, query);
    _cc_manager = CCManager::create(this);
    _txn_state = RUNNING;
    _worker_thread = thread;

    _txn_start_time = get_sys_clock();
    _txn_restart_time = _txn_start_time;
    _lock_wait_time = 0;
    _net_wait_time = 0;

    _is_sub_txn = false;
    _is_single_partition = true;
    _is_read_only = true;
    _is_remote_abort = false;

    log_semaphore = new SemaphoreSync();
    dependency_semaphore = new SemaphoreSync();
    rpc_semaphore = new SemaphoreSync();
    rpc_log_semaphore = new SemaphoreSync();
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
        #if LOG_ENABLE
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
    RC rc = RCOK;
    _txn_state = RUNNING;
    // running transaction on the host node
    rc = _store_procedure->execute();
    assert(rc == COMMIT || rc == ABORT);
    // Handle single-partition transactions
    if (is_single_partition()) {
        _commit_start_time = get_sys_clock();
        rc = process_commit_phase_singlepart(rc);
    } else {
        if (rc == ABORT)
            rc = process_2pc_phase2(ABORT);
        else {
            _prepare_start_time = get_sys_clock();
            process_2pc_phase1();
            _commit_start_time = get_sys_clock();
            rc = process_2pc_phase2(COMMIT);
        }
    }
    update_stats();
    return rc;
}

RC
TxnManager::process_commit_phase_singlepart(RC rc)
{
    if (rc == COMMIT) {
        _txn_state = COMMITTING;
    } else if (rc == ABORT) {
        _txn_state = ABORTING;
        _store_procedure->txn_abort();
    } else
        assert(false);
#if LOG_ENABLE
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
            //printf("[txn-%lu] inc log semaphore while logging\n", _txn_id);
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

        //printf("[txn-%lu] starts to wait for logging\n", _txn_id);
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
#if REMOTE_LOG
    SundialRequest::RequestType type = rc == COMMIT ? SundialRequest::LOG_COMMIT_REQ :
            SundialRequest::LOG_ABORT_REQ;
    send_log_request(g_storage_node_id, type);
    #if ASYNC_RPC
        rpc_log_semaphore->wait();
    #endif
#endif
    _cc_manager->cleanup(rc);
    _finish_time = get_sys_clock();
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
#endif
    return rc;
}

// For Distributed DBMS
// ====================
RC
TxnManager::send_log_request(uint64_t node_id, SundialRequest::RequestType type)
{
    // if ( _log_nodes_involved.find(node_id) == _log_nodes_involved.end() ) {
    //     _log_nodes_involved[node_id] = new RemoteNodeInfo;
    //     _log_nodes_involved[node_id]->state = RUNNING;
    // }
    // SundialRequest &request = _log_nodes_involved[node_id]->request;
    // SundialResponse &response = _log_nodes_involved[node_id]->response;
    SundialRequest request;
    SundialResponse response;
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
#if ASYNC_RPC
    rpc_log_semaphore->incr();
    rpc_client->sendRequestAsync(this, node_id, request, response);
    /*
    M_ASSERT(response.response_type() == SundialResponse::RESP_LOG_YES
            || response.response_type() == SundialResponse::RESP_LOG_ABORT
            || response.response_type() == SundialResponse::RESP_LOG_COMMIT,
            "type=%d\n", response.response_type());
            */
#endif
    return RCOK;
}

RC
TxnManager::send_remote_read_request(uint64_t node_id, uint64_t key, uint64_t index_id,
                                     uint64_t table_id, access_t access_type)
{
    // printf("[node-%u] txn-%lu send remote read on %lu to node-%lu\n", g_node_id, get_txn_id(), key, node_id);
    _is_single_partition = false;
    if ( _remote_nodes_involved.find(node_id) == _remote_nodes_involved.end() ) {
        _remote_nodes_involved[node_id] = new RemoteNodeInfo;
        _remote_nodes_involved[node_id]->state = RUNNING;
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

    rpc_client->sendRequest(node_id, request, response);

    // handle RPC response
    assert(response.response_type() == SundialResponse::RESP_OK
           || response.response_type() ==  SundialResponse::RESP_ABORT);
    if (response.response_type() == SundialResponse::RESP_OK) {
        ((LockManager *)_cc_manager)->process_remote_read_response(node_id, access_type, response);
        return RCOK;
    } else {
        _remote_nodes_involved[node_id]->state = ABORTED;
        _is_remote_abort = true;
        return ABORT;
    }
}

RC
TxnManager::process_2pc_phase1()
{
    // printf("[node-%u] txn-%lu enter phase 1\n", g_node_id, get_txn_id());
    // Start Two-Phase Commit
    _txn_state = PREPARING;
#if LOG_ENABLE
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
#if REMOTE_LOG
    SundialRequest::RequestType type = SundialRequest::LOG_YES_REQ; // always vote yes for now
    send_log_request(g_storage_node_id, type);
#endif
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        assert(it->second->state == RUNNING);
        SundialRequest &request = it->second->request;
        SundialResponse &response = it->second->response;
        request.Clear();
        response.Clear();
        request.set_txn_id( get_txn_id() );
        request.set_request_type( SundialRequest::PREPARE_REQ );

        ((LockManager *)_cc_manager)->build_prepare_req( it->first, request );

#if ASYNC_RPC
        rpc_semaphore->incr();
        rpc_client->sendRequestAsync(this, it->first, request, response);
#else
        rpc_client->sendRequest(it->first, request, response);
        // TODO. for now, assume prepare always succeeds
        assert (response.response_type() == SundialResponse::PREPARED_OK
                || response.response_type() == SundialResponse::PREPARED_OK_RO);
        if (response.response_type() == SundialResponse::PREPARED_OK)
            _remote_nodes_involved[it->first]->state = COMMITTING;
        else
            // the remote sub-txn is readonly and has released locks.
            // For CLV, this means the remote sub-txn does not depend on any
            // weak locks.
            _remote_nodes_involved[it->first]->state = COMMITTED;
#endif
    }
    log_semaphore->wait();
#if ASYNC_RPC
    rpc_semaphore->wait();
    rpc_log_semaphore->wait();
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        assert(it->second->state == RUNNING);
        SundialResponse &response = it->second->response;
        assert (response.response_type() == SundialResponse::PREPARED_OK
                || response.response_type() == SundialResponse::PREPARED_OK_RO);
        if (! (response.response_type() == SundialResponse::PREPARED_OK
                || response.response_type() == SundialResponse::PREPARED_OK_RO) )
            cout << response.response_type() << endl;
        if (response.response_type() == SundialResponse::PREPARED_OK)
            it->second->state = COMMITTING;
        else
            it->second->state = COMMITTED;
    }
#endif
    return COMMIT;
}

RC
TxnManager::process_2pc_phase2(RC rc)
{
    // printf("[node-%u] txn-%lu enter phase 2\n", g_node_id, get_txn_id());
    assert(rc == COMMIT || rc == ABORT);
    _txn_state = (rc == COMMIT)? COMMITTING : ABORTING;
    // TODO. for CLV this logging is optional. Here we use a conservative
    // implementation as logging is not on the critical path of locking anyway.
  #if LOG_ENABLE
    std::string record = std::to_string(_txn_id);
    char * log_record = (char *)record.c_str();
    uint32_t log_record_size = record.length();
    log_semaphore->incr();
    log_manager->log(this, log_record_size, log_record);
    // OPTIMIZATION: perform local logging and commit request in parallel
    // log_semaphore->wait();
  #endif
    bool remote_readonly = true;
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        if (!(it->second->state == ABORTED || it->second->state == COMMITTED)) {
            remote_readonly = false;
            break;
        }
    }
    if (remote_readonly && is_read_only()) { // no logging  and remote message at all
        _cc_manager->cleanup(rc);
        _finish_time = get_sys_clock(); 
        _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
        return rc;
    }
  #if REMOTE_LOG && COMMIT_ALG == TWO_PC
    SundialRequest::RequestType type = rc == COMMIT ? SundialRequest::LOG_COMMIT_REQ :
            SundialRequest::LOG_ABORT_REQ;
    send_log_request(g_storage_node_id, type);
    #if ASYNC_RPC
        rpc_log_semaphore->wait();
        _cc_manager->cleanup(rc); // release lock after receive log resp
    #endif
#endif
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        // No need to run this phase if the remote sub-txn has already committed
        // or aborted.
        if (it->second->state == ABORTED || it->second->state == COMMITTED) continue;
        SundialRequest &request = it->second->request;
        SundialResponse &response = it->second->response;
        request.Clear();
        response.Clear();
        request.set_txn_id( get_txn_id() );
        SundialRequest::RequestType type = (rc == COMMIT)?
            SundialRequest::COMMIT_REQ : SundialRequest::ABORT_REQ;
        request.set_request_type( type );
#if ASYNC_RPC
        rpc_semaphore->incr();
        rpc_client->sendRequestAsync(this, it->first, request, response);
#else
        rpc_client->sendRequest(it->first, request, response);
        assert (response.response_type() == SundialResponse::ACK);
        _remote_nodes_involved[it->first]->state = (rc == COMMIT)? COMMITTED : ABORTED;
#endif
    }
    #if REMOTE_LOG && COMMIT_ALG == ONE_PC
    SundialRequest::RequestType type = rc == COMMIT ? SundialRequest::LOG_COMMIT_REQ :
            SundialRequest::LOG_ABORT_REQ;
    send_log_request(g_storage_node_id, type);
#endif
    
    _finish_time = get_sys_clock();
    // OPTIMIZATION: release locks as early as possible.
    // No need to wait for this log since it is optional (shared log optimization)
    dependency_semaphore->wait();
    log_semaphore->wait();
#if ASYNC_RPC
    #if COMMIT_ALG == ONE_PC
        rpc_log_semaphore->wait(); 
        _cc_manager->cleanup(rc); // release lock after receive log resp
    #endif
    rpc_semaphore->wait();
    for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved.end(); it ++) {
        if (it->second->state == ABORTED || it->second->state == COMMITTED) continue;
        __attribute__((unused)) SundialResponse &response = it->second->response;
        assert (response.response_type() == SundialResponse::ACK);
        it->second->state = (rc == COMMIT)? COMMITTED : ABORTED;
    }
#endif
    _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
    return rc;
}

// RPC Server
// ==========
RC
TxnManager::process_remote_request(const SundialRequest* request, SundialResponse* response)
{
    RC rc = RCOK;
    uint32_t num_tuples;
  #if LOG_ENABLE
    std::string record;
    char * log_record = NULL;
    uint32_t log_record_size = 0;
  #endif
#if REMOTE_LOG
    int log_commit = 0;
    SundialRequest::RequestType log_type;
#endif
    switch(request->request_type()) {
        case SundialRequest::READ_REQ :
            num_tuples = request->read_requests_size();
            for (uint32_t i = 0; i < num_tuples; i++) {
                uint64_t key = request->read_requests(i).key();
   	// printf("[node-%u] txn-%lu rec remote read on %lu\n", g_node_id, get_txn_id(), key);
                uint64_t index_id = request->read_requests(i).index_id();
                access_t access_type = (access_t)request->read_requests(i).access_type();

                INDEX * index = GET_WORKLOAD->get_index(index_id);
                set<row_t *> * rows = NULL;
                // TODO. all the matching rows should be returned.
                rc = get_cc_manager()->index_read(index, key, rows, 1);
                assert(rc == RCOK || rc == ABORT);
                if (rc == ABORT) break;
                if (!rows) {
                    printf("[txn=%ld] key=%ld, index_id=%ld, access_type=%d\n",
                           get_txn_id(), key, index_id, access_type);
                }
                assert(rows);
                row_t * row = *rows->begin();
                get_cc_manager()->remote_key += 1;
                // printf("txn: %ld access key by remote req: %ld node: %u\n", get_txn_id(), key, g_node_id);
                rc = get_cc_manager()->get_row(row, access_type, key);
                if (rc == ABORT) break;
                uint64_t table_id = row->get_table_id();
                SundialResponse::TupleData * tuple = response->add_tuple_data();
                uint64_t tuple_size = row->get_tuple_size();
                tuple->set_key(key);
                tuple->set_table_id( table_id );
                tuple->set_size( tuple_size );
                tuple->set_data( get_cc_manager()->get_data(key, table_id), tuple_size );
            }
            if (rc == ABORT) {
                response->set_response_type( SundialResponse::RESP_ABORT );
                _cc_manager->cleanup(ABORT);
            } else
                response->set_response_type( SundialResponse::RESP_OK );
            return rc;
        case SundialRequest::PREPARE_REQ :
            // copy data to the write set.
            num_tuples = request->tuple_data_size();
            for (uint32_t i = 0; i < num_tuples; i++) {
                uint64_t key = request->tuple_data(i).key();
                uint64_t table_id = request->tuple_data(i).table_id();
                char * data = get_cc_manager()->get_data(key, table_id);
                memcpy(data, request->tuple_data(i).data().c_str(), request->tuple_data(i).size());
            }
  #if LOG_ENABLE
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
    #if REMOTE_LOG
        send_log_request(g_storage_node_id, SundialRequest::LOG_YES_REQ);
        #if ASYNC_RPC
            rpc_log_semaphore->wait();
        #endif
    #endif
            // readonly remote nodes
            if (num_tuples == 0) {
                _txn_state = COMMITTED;
                _cc_manager->cleanup(COMMIT); // release lock after log is received
                _finish_time = get_sys_clock();
                response->set_response_type( SundialResponse::PREPARED_OK_RO );
                return rc;
            }
            response->set_response_type( SundialResponse::PREPARED_OK );
            return rc;
        case SundialRequest::COMMIT_REQ :
#if REMOTE_LOG
            log_type = SundialRequest::LOG_COMMIT_REQ;
            log_commit = 1;
#endif
        case SundialRequest::ABORT_REQ :
#if REMOTE_LOG
            if (log_commit == 0)
                log_type = SundialRequest::LOG_ABORT_REQ;
#endif
  #if LOG_ENABLE
            record = std::to_string(_txn_id);
            log_record = (char *)record.c_str();
            log_record_size = record.length();
            log_semaphore->incr();
            log_manager->log(this, log_record_size, log_record);
  #endif
    #if REMOTE_LOG
        send_log_request(g_storage_node_id, log_type);
        #if ASYNC_RPC // for now, commit phase of part is same for 1pc and 2pc
            rpc_log_semaphore->wait();
        #endif
    #endif
            dependency_semaphore->wait();
            rc = (request->request_type() == SundialRequest::COMMIT_REQ)? COMMIT : ABORT;
            _txn_state = (rc == COMMIT)? COMMITTED : ABORTED;
            _cc_manager->cleanup(rc); // release lock after log is received
            _finish_time = get_sys_clock();
            // OPTIMIZATION: release locks as early as possible.
            // No need to wait for this log since it is optional (shared log
            // optimization)
            log_semaphore->wait();
            // #if ASYNC_RPC && COMMIT_ALG == ONE_PC
            //     rpc_log_semaphore->wait();
            // #endif
            response->set_response_type( SundialResponse::ACK );
            return rc;
        default:
            assert(false);
            exit(0);
    }
}
