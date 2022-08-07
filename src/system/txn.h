#pragma once

#include "global.h"
#include "helper.h"

//#include "rpc_server.h"
#include "rpc_client.h"
#include "semaphore_sync.h"
#include <pthread.h>

class workload;
//class WorkerThread;
class row_t;
class table_t;
class QueryBase;
class SubQuery;
class Message;
class StoreProcedure;
class CCManager;

class TxnManager
{
public:
    enum State {
        RUNNING,
        PREPARED,
        COMMITTED,
        ABORTED,
        FAILED
    };
    TxnManager() : TxnManager(nullptr, nullptr) {};
    TxnManager(QueryBase * query, WorkerThread * thread);
    virtual ~TxnManager();

    // start to run transactions
    RC start();

    // rerun previously aborted transactions
    RC restart();

    void              set_txn_id(uint64_t txn_id) { _txn_id = txn_id; }
    uint64_t          get_txn_id()          { return _txn_id; }
    // if coordinator is read-only
    bool              is_read_only()        { return _is_read_only; }
    bool              is_txn_read_only()    { return _is_txn_read_only; }
    bool              is_coordinator()        { return _is_coordinator; }
    void              set_read_only(bool readonly) { _is_read_only = readonly; }
    void              set_txn_read_write() { _is_txn_read_only = false; }
    bool              is_single_partition() { return _is_single_partition; }

    CCManager *       get_cc_manager()      { return _cc_manager; }
    StoreProcedure *  get_store_procedure() { return _store_procedure; };
    State             get_txn_state()       { return _txn_state; }
    void              set_txn_state(State state) { _txn_state = state; }
    void              set_decision(RC rc) { _decision = rc; };
    void              lock() {pthread_mutex_lock(&_latch);};
    void              unlock() {pthread_mutex_unlock(&_latch);}

    // Synchronization
    // ===============
    SemaphoreSync *   dependency_semaphore;
    SemaphoreSync *   rpc_semaphore;
    SemaphoreSync *   rpc_log_semaphore;
    SemaphoreSync *   phase1_log_semaphore;
    SemaphoreSync *   phase2_log_semaphore;
    pthread_mutex_t   _latch;


    // Distributed transactions
    // ========================
public:
    // client
    RC send_remote_read_request(uint64_t node_id, uint64_t key, uint64_t index_id,
                                uint64_t table_id, access_t access_type);
    RC send_remote_package(std::map<uint64_t, vector<RemoteRequestInfo *> > &remote_requests);
    RC process_2pc_phase1();
    RC process_2pc_phase2(RC rc);
    // server
    RC process_remote_request(const SundialRequest* request, SundialResponse* response);
    RC process_read_request(const SundialRequest* request, SundialResponse*
    response);
    RC process_prepare_request(const SundialRequest* request, SundialResponse*
    response);
    RC process_decision_request(const SundialRequest* request,
                             SundialResponse* response, RC rc);
    RC process_terminate_request(const SundialRequest* request, SundialResponse*
    response);
    RC termination_protocol();
    void handle_prepare_resp(SundialResponse* response);


    void set_sub_txn(bool is_sub_txn)     { _is_sub_txn = is_sub_txn; }
    bool is_sub_txn()                     { return _is_sub_txn; }
    inline State rc_to_state(RC rc) {
        switch(rc) {
            case ABORT: return ABORTED;
            case COMMIT: return COMMITTED;
            default: assert(false); return RUNNING;
        }
    };

public:
    // Stats
    // =====
    void              update_stats();
    uint64_t          num_local_write;
    uint64_t          num_aborted;

    // Debug
    void              print_state()       {};

private:
    // TODO. for now, a txn is mapped to a single thread.
    WorkerThread *    _worker_thread;
    StoreProcedure *  _store_procedure;
    CCManager *       _cc_manager;

    volatile State    _txn_state;
    volatile RC       _decision;
    bool              _is_single_partition;
    bool              _is_read_only;
    bool              _is_txn_read_only;
    bool              _is_remote_abort;
    bool              _is_coordinator;
    // txn_id format.
    // | per thread monotonically increasing ID   |  thread ID   |   Node ID |
    uint64_t          _txn_id;


    // Single-part transactions
    // ========================
    RC process_commit_phase_singlepart(RC rc);


    bool              _is_sub_txn;
    struct RemoteNodeInfo {
        volatile State state;
        bool is_readonly;
        // At any point in time, a remote node has at most 1 request and 1
        // response.
        SundialRequest request;
        SundialResponse response;
    };

    // used for native remote log
    std::map<uint32_t, RemoteNodeInfo *> _log_nodes_involved;

    // stats
    // =====
    uint64_t          _txn_start_time;
    uint64_t          _txn_restart_time;
    uint64_t          _prepare_start_time;
    uint64_t          _commit_start_time;
    uint64_t          _log_ready_time;
    uint64_t          _precommit_finish_time;
    uint64_t          _finish_time;
    uint64_t          _lock_wait_time;
    uint64_t          _net_wait_time;
    uint64_t          _terminate_time;

  public:
    std::map<uint32_t, RemoteNodeInfo *> _remote_nodes_involved;

    // Distributed transactions for MDCC
    // =================================
  public:
    // client
    RC process_mdcc_singlepart(RC rc);
    RC process_mdcc_phase1();
    RC process_mdcc_phase2(RC rc);
    // server
    RC process_mdcc_2aclassic(const SundialRequest* request, SundialResponse* response);
    void process_mdcc_2bclassic(const SundialRequest* request, SundialResponse*
    response);
    void process_mdcc_2bclassic_abort(const SundialRequest* request, SundialResponse* response);
    RC process_mdcc_2bfast(const SundialRequest* request, SundialResponse* response);
    RC process_mdcc_visibility(const SundialRequest* request, SundialResponse*
    response, RC rc);
    int get_replied_acceptors(size_t i) {return replied_acceptors[i].load
            (std::memory_order_relaxed);}
    int get_replied_acceptors2() {return replied_acceptors2.load
            (std::memory_order_relaxed);}
    void increment_replied_acceptors(size_t i) { replied_acceptors[i]++; }
    void increment_replied_acceptors2() { replied_acceptors2++; }

    // txn level requests
#if NODE_TYPE == STORAGE_NODE
    SundialRequest txn_requests_[NUM_NODES];
    SundialResponse txn_responses_[NUM_NODES];
#else
    // request in phase 1, as leader of paxos
    SundialRequest txn_requests_[NUM_STORAGE_NODES];
    SundialResponse txn_responses_[NUM_STORAGE_NODES];
#endif
    // request in phase 2, as leader of paxos
    SundialRequest txn_requests2_[NUM_STORAGE_NODES];
    SundialResponse txn_responses2_[NUM_STORAGE_NODES];

  private:
    void process_mdcc_local_phase1(RC rc, uint64_t g_node_id, bool is_singlepart=false);
    void sendRemoteLogRequest(SundialRequest::LogType log_type, State state,
                              uint64_t log_data_size);
    // used to track # of replies from each node and the stats will be used for
    // calculating quorum
    // each count should not exceed g_num_storage_nodes + 1
    // rpc_server will update the stats on receiving storage node's 2b msg
    // rpc_client will update the stats on receiving reply from participant's 2b
    // the txn_mdcc will read the stats
    std::atomic<int> replied_acceptors[NUM_NODES];
    std::atomic<int> replied_acceptors2;
};
