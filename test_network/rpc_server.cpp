#include "rpc_server.h"
#include "global.h"

void
SundialRPCServerImpl::run() {
    std::ifstream in(ifconfig_file);
    string line;
    uint32_t num_nodes = 0;
    while (getline (in, line)) {
        if (line[0] == '#')
            continue;
        else {
            if (num_nodes == g_node_id) {
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
    uint32_t num_thds = NUM_RPC_SERVER_THREADS;
    _thread_pool = new std::thread * [num_thds];
    for (uint32_t i = 0; i < num_thds; i++) {
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
    return;
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