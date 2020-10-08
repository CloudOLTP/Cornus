#include "rpc_server.h"
#include "global.h"
#include "txn.h"
#include "txn_table.h"
#include "manager.h"


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
    _thread_pool = new std::thread * [NUM_RPC_SERVER_THREADS];
    for (uint32_t i = 0; i < NUM_RPC_SERVER_THREADS; i++) {
        //_thread_pool[i] = new pthread_t;
        //pthread_create(_thread_pool[i], NULL, HandleRpcs, NULL); 
        _thread_pool[i] = new std::thread(HandleRpcs, this);
    }
    cout <<"sundial server initialized, lisentening on " << line << endl;
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
    if (request->request_type() == SundialRequest::SYS_REQ) {
        // At the beginning of run, (g_num_nodes - 1) sync requests are received
        // as global synchronization. At the end of the run, another
        // (g_num_nodes - 1) sync requests are received as the termination
        // synchronization.
        glob_manager->receive_sync_request();
        response->set_response_type( SundialResponse::SYS_RESP );
        return; 
    }

    uint64_t txn_id = request->txn_id();
    TxnManager * txn_man = txn_table->get_txn(txn_id);
    // If no TxnManager exists for the requesting transaction, create one.
    if (txn_man == NULL) {
        //printf("adding txnID=%ld into txn_table\n", txn_id);
        assert( request->request_type() == SundialRequest::READ_REQ );
        txn_man = new TxnManager();
        txn_man->set_txn_id( txn_id );
        txn_table->add_txn( txn_man );
    }
    // the transaction handles the RPC call
    txn_man->process_remote_request(request, response);

    // if the sub-transaction is no longer required, remove from txn_table
    if (response->response_type() == SundialResponse::RESP_ABORT
        || response->response_type() == SundialResponse::PREPARED_OK_RO
        || response->response_type() == SundialResponse::PREPARED_ABORT
        || response->response_type() == SundialResponse::ACK) {
        txn_table->remove_txn( txn_man );
        delete txn_man;
    }
}


SundialRPCServerImpl::CallData::CallData(SundialRPC::AsyncService* service, ServerCompletionQueue* cq) : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
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
