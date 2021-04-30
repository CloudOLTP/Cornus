#include "rpc_server.h"
#include "global.h"
#include "txn.h"
#include "txn_table.h"
#include "manager.h"
#include "log.h"


/*
SundialRPCServerImpl::~SundialRPCServerImpl(){
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    cq_->Shutdown();
}
*/

void
SundialRPCServerImpl::run() {
    //std::istringstream in(ifconfig_string);
    std::ifstream in(ifconfig_file);
    string line;
    uint32_t num_nodes = 0;
    while (getline (in, line)) {
        if (line[0] == '#')
            continue;
        else {
            if (num_nodes == g_node_id) {
                /*
                size_t pos = line.find(":");
                assert(pos != string::npos);
                string port_str = line.substr(pos + 1, line.length());
                port = atoi(port_str.c_str()); */
                //_server_name = line;
                break;
            }
            num_nodes ++;
        }
    }
    ServerBuilder builder;
    builder.AddListeningPort(line, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    // set up multiple server threads to start accepting requests
    // XXX(zhihan): only use one cq as it is thread safe. can switch to more cqs if does not scale 
    //_thread_pool = new pthread_t * [NUM_RPC_SERVER_THREADS];
#if LOG_NODE
    #if !WORKER_SERVER_SAME
    uint32_t num_thds = NUM_STORAGE_RPC_SERVER_THREADS;
    #else
    uint32_t num_thds = NUM_WORKER_THREADS;
    #endif
#else
    #if !WORKER_SERVER_SAME
    uint32_t num_thds = NUM_RPC_SERVER_THREADS;
    #else
    uint32_t num_thds = NUM_WORKER_THREADS;
    #endif
#endif
    _thread_pool = new std::thread * [num_thds];
    for (uint32_t i = 0; i < num_thds; i++) {
        //_thread_pool[i] = new pthread_t;
        //pthread_create(_thread_pool[i], NULL, HandleRpcs, NULL); 
        _thread_pool[i] = new std::thread(HandleRpcs, this);
    }
    cout <<"[Sundial] rpc server initialized, lisentening on " << line << endl;
}

void SundialRPCServerImpl::HandleRpcs(SundialRPCServerImpl * s) {
    // Spawn a new CallData instance to serve new clients.
    new CallData(&(s->service_), s->cq_.get());
    void* tag;  // uniquely identifies a request.
    bool ok;
    while (true) {
      // Block waiting to read the next event from the completion queue. The
      // event is uniquely identified by its tag, which in this case is the
      // memory address of a CallData instance.
      // The return value of Next should always be checked. This return value
      // tells us whether there is any kind of event or cq_ is shutting down.
      GPR_ASSERT(s->cq_->Next(&tag, &ok));
      GPR_ASSERT(ok);
      static_cast<CallData*>(tag)->Proceed();
    }
}

Status
SundialRPCServerImpl::contactRemote(ServerContext* context, const SundialRequest* request,
                     SundialResponse* response) {
    // Calls done_callback->Run() when it goes out of scope.
    // AutoClosureRunner done_runner(done_callback);
    processContactRemote(context, request, response);
    return Status::OK;
}

void
SundialRPCServerImpl::processContactRemote(ServerContext* context, const SundialRequest* request, 
        SundialResponse* response) {

    usleep(NETWORK_DELAY);
#if LOG_NODE
    usleep(LOG_DELAY);
    if (request->request_type() == SundialRequest::LOG_YES_REQ ||
        request->request_type() == SundialRequest::LOG_ABORT_REQ ||
        request->request_type() == SundialRequest::LOG_COMMIT_REQ) {
        uint64_t time_begin = get_sys_clock();
        log_manager->log(request, response);
        INC_FLOAT_STATS(time_debug3, get_sys_clock() - time_begin);
        INC_INT_STATS(int_debug3, 1);
        response->set_txn_id(request->txn_id());
        return;
    }
#endif

    uint64_t txn_id = request->txn_id();
    response->set_request_type((int)request->request_type());
    response->set_txn_id(txn_id);
    response->set_node_id(g_node_id);
    RC rc;
    TxnManager * txn;

#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] received request-%d\n", g_node_id, txn_id,
        request->request_type());
#endif
    switch (request->request_type()) {
        case SundialRequest::SYS_REQ:
            glob_manager->receive_sync_request();
            response->set_response_type( SundialResponse::SYS_RESP );
            return;
        case SundialRequest::READ_REQ:
            txn = txn_table->get_txn(txn_id);
            if (txn  == NULL) {
                txn = new TxnManager();
                txn->set_txn_id(txn_id);
                txn_table->add_txn(txn);
            }
            // only read and terminate need latch since
            // (1) read can only be concurrent with read and terminate
            // (2) read does not remove txn from txn table when getting txn
            txn->lock();
            rc = txn->process_read_request(request, response);
            txn->unlock();
        case SundialRequest::TERMINATE_REQ:
            txn = txn_table->get_txn(txn_id, true);
            if (txn == NULL) {
                return;
            }
            txn->lock();
            rc = txn->process_terminate_request(request, response);
            txn->unlock();
            delete txn;
        case SundialRequest::PREPARE_REQ:
            txn = txn_table->get_txn(txn_id, true);
            if (txn == NULL) {
                // txn already cleaned up
                response->set_response_type(SundialResponse::PREPARED_ABORT);
                return;
            }
            rc = txn->process_prepare_request(request, response);
            if (txn->get_txn_state() != TxnManager::PREPARED)
                delete txn;
        case SundialRequest::COMMIT_REQ:
            txn = txn_table->get_txn(txn_id, true);
            if (txn == NULL) {
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, COMMIT);
            delete  txn;
        case SundialRequest::ABORT_REQ:
            txn = txn_table->get_txn(txn_id, true);
            if (txn == NULL) {
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, ABORT);
            delete  txn;
        default:
            assert(false);
    }
    // the transaction handles the RPC call
    if (rc == FAIL || !glob_manager->active) {
#if DEBUG_PRINT
        printf("[node-%u, txn-%lu] reply failure response\n", g_node_id,
            txn_id);
#endif
        response->set_response_type(SundialResponse::RESP_FAIL);
    }
}


SundialRPCServerImpl::CallData::CallData(SundialRPC::AsyncService* service,
    ServerCompletionQueue* cq) : service_(service), cq_(cq),
    responder_(&ctx_), status_(CREATE) {
    Proceed();
}

void
SundialRPCServerImpl::CallData::Proceed() {
    if (status_ == CREATE) {
        service_->RequestcontactRemote(&ctx_, &request_, &responder_, cq_, cq_, this);  
        status_ = PROCESS;
    } else if (status_ == PROCESS) {
        // Spawn a new CallData instance to serve new clients while processing
        new CallData(service_, cq_);
        processContactRemote(&ctx_, &request_ , &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
    } else {
        GPR_ASSERT(status_ == FINISH);
        delete this;
    }
}

/*
void
SundialRPCServerImpl::Export(net_http::HTTPServerInterface* http_server) {
    ExportServiceTo(http_server);

    _thread_pool = new ThreadPool(NUM_RPC_SERVER_THREADS);
    _thread_pool->StartWorkers();
    SetThreadPool("contactRemote", _thread_pool);
}
*/
