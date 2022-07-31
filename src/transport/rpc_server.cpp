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
    // find self address from ifconfig file
    std::ifstream in(ifconfig_file);
    string line;
    uint32_t num_nodes = 0;
#if NODE_TYPE == COMPUTE_NODE
    while (getline (in, line)) {
        if (line[0] == '#')
            continue;
        else {
            if (num_nodes == g_node_id)
                break;
            num_nodes ++;
        }
    }
#else
    bool is_storage_node = false;
    while (getline (in, line)) {
      if (line[0] == '#')
        continue;
      else if (line[0] == '=' && line[1] == 's') {
        is_storage_node = true;
        continue;
      }
      if (is_storage_node) {
        if (num_nodes == g_node_id) {
          cout << "[Sundial] will start rpc server at " << line << endl;
          break;
        }
        num_nodes ++;
      }
    }
#endif

    ServerBuilder builder;
    builder.AddListeningPort(line, grpc::InsecureServerCredentials());
    builder.RegisterService(&service_);
    cq_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    // set up multiple server threads to start accepting requests
    // XXX(zhihan): only use one cq as it is thread safe. can switch to more cqs if does not scale 
    //_thread_pool = new pthread_t * [NUM_RPC_SERVER_THREADS];
    #if !WORKER_SERVER_SAME
    uint32_t num_thds = NUM_RPC_SERVER_THREADS;
    #else
    uint32_t num_thds = NUM_WORKER_THREADS;
    #endif
    _thread_pool = new std::thread * [num_thds];
    for (uint32_t i = 0; i < num_thds; i++) {
        _thread_pool[i] = new std::thread(HandleRpcs, this);
    }
    cout <<"[Sundial] rpc server initialized, listening on " << line << endl;
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
#if DEBUG_PRINT
    printf("finish processing contact remote\n");
#endif
    return Status::OK;
}

void
SundialRPCServerImpl::processContactRemote(ServerContext* context, const SundialRequest* request, 
        SundialResponse* response) {

    uint64_t txn_id = request->txn_id();
    SundialResponse::RequestType tpe = (SundialResponse::RequestType) ((int)
        request->request_type());
    response->set_request_type(tpe);
    response->set_txn_id(txn_id);
    response->set_node_id(g_node_id);
    RC rc = RCOK;
    TxnManager * txn;

    switch (request->request_type()) {
        case SundialRequest::SYS_REQ:
#if DEBUG_PRINT
            printf("[node-%u] receive system request\n",
                 g_node_id);
#endif
            glob_manager->receive_sync_request();
            return;
        case SundialRequest::READ_REQ:
#if DEBUG_PRINT
          printf("[node-%u, txn-%lu] receive remote read request\n",
                 g_node_id, txn_id);
#endif
            txn = txn_table->get_txn(txn_id);
            if (txn  == nullptr) {
                txn = new TxnManager();
                txn->set_txn_id(txn_id);
                txn_table->add_txn(txn);
            }
            // for failure case
            // only read and terminate need latch since
            // (1) read can only be concurrent with read and terminate
            // (2) read does not remove txn from txn table when getting txn
#if FAILURE_ENABLE
            txn->lock();
            rc = txn->process_read_request(request, response);
            txn->unlock();
#else
            rc = txn->process_read_request(request, response);
#endif
            if (rc == ABORT) {
                txn_table->remove_txn(txn, false);
                delete txn;
            }
            response->set_txn_id(txn_id);
            break;
        case SundialRequest::TERMINATE_REQ:
#if NODE_TYPE == COMPUTE_NODE
            txn = txn_table->get_txn(txn_id, false, true);
            if (txn == NULL) {
                return;
            }
            txn->lock();
            rc = txn->process_terminate_request(request, response);
            txn->unlock();
            txn_table->remove_txn(txn, false);
            delete txn;
            break;
#else
#if DEBUG_PRINT
            cout << "receive terminate request" << endl;
#endif
            response->set_request_type(SundialResponse::SYS_REQ);
            response->set_response_type(SundialResponse::ACK);
            glob_manager->recv_terminate_request = true;
#if DEBUG_PRINT
            cout << "set active status to false" << endl;
#endif
            return;
#endif
        case SundialRequest::PREPARE_REQ:
#if DEBUG_PRINT
          printf("[node-%u, txn-%lu] receive remote prepare request\n",
                 g_node_id, txn_id);
#endif
            txn = txn_table->get_txn(txn_id);
            if (txn == nullptr) {
                // txn already cleaned up
                response->set_response_type(SundialResponse::PREPARED_ABORT);
                return;
            }
            rc = txn->process_prepare_request(request, response);
            if (txn->get_txn_state() != TxnManager::PREPARED) {
                txn_table->remove_txn(txn, false);
                delete txn;
            }
            response->set_txn_id(txn_id);
            break;
        case SundialRequest::COMMIT_REQ:
#if DEBUG_PRINT
          printf("[node-%u, txn-%lu] receive remote commit request\n",
                 g_node_id, txn_id);
#endif
            txn = txn_table->get_txn(txn_id, false, true);
            if (txn == nullptr) {
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, COMMIT);
            txn_table->remove_txn(txn, false);
            delete txn;
            response->set_txn_id(txn_id);
            break;
        case SundialRequest::ABORT_REQ:
#if DEBUG_PRINT
          printf("[node-%u txn-%lu] receive remote abort request\n",
                 g_node_id, txn_id);
#endif
            txn = txn_table->get_txn(txn_id, false, true);
            if (txn == nullptr) {
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, ABORT);
            txn_table->remove_txn(txn, false);
            delete txn;
            response->set_txn_id(txn_id);
            break;
        case SundialRequest::MDCC_Propose:
            // from coordinator to participant in phase 1, classic
            txn = txn_table->get_txn(txn_id, true);
            // since using occ, it wont check conflict until next step,
            // so the txn cannot be removed and it must exist
            assert(txn);
            rc = txn->process_mdcc_2aclassic(request, response);
            if (rc == ABORT) {
                txn_table->remove_txn(txn, false);
                delete txn;
            } else {
                txn_table->return_txn(txn);
            }
            break;
        case SundialRequest::MDCC_Phase2a:
            // from leader to acceptors in phase 1, classic
            // leader may be coordinator or participant
            assert(NODE_TYPE == STORAGE_NODE);
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                txn = new TxnManager();
                txn->set_txn_id(txn_id);
                txn_table->add_txn(txn);
            }
            txn->lock();
            txn->process_mdcc_2bclassic(request, response);
            txn->unlock();
            // must return since phase 2a abort may delete it
            txn_table->return_txn(txn);
            break;
        case SundialRequest::MDCC_Phase2aAbort:
            // from leader to acceptors in phase 1, classic
            // leader may be coordinator or participant
            assert(NODE_TYPE == STORAGE_NODE);
            txn = txn_table->get_txn(txn_id, true, true);
            if (txn == nullptr) {
                txn = new TxnManager();
                txn->set_txn_id(txn_id);
                txn_table->add_txn(txn);
            }
            txn->lock();
            txn->process_mdcc_2bclassic_abort(request, response);
            txn->unlock();
            txn_table->return_txn(txn);
            // abort
            txn_table->remove_txn(txn, true);
            delete txn;
            break;
        case SundialRequest::MDCC_Phase2bReply:
            // from acceptor to leader in phase 1, classic
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                response->set_request_type(SundialResponse::MDCC_DummyReply);
                return;
            }
            txn->increment_replied_acceptors(request->node_id());
            response->set_request_type(SundialResponse::MDCC_DummyReply);
            // must return since this txn's home is on this node and delete
            // is handled in worker_thread.cpp
            txn_table->return_txn(txn);
            break;
        case SundialRequest::MDCC_Phase2bReplyAbort:
            // from acceptor to leader in phase 1, classic
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                response->set_request_type(SundialResponse::MDCC_DummyReply);
                return;
            }
            // set decision to abort
            // cannot use handle_resp since here we have type of request
            txn->_remote_nodes_involved[request->node_id()]->state =
                TxnManager::ABORTED;
            txn->increment_replied_acceptors(request->node_id());
            response->set_request_type(SundialResponse::MDCC_DummyReply);
            // must return since this txn's home is on this node and delete
            // is handled in worker_thread.cpp
            txn_table->return_txn(txn);
            break;
        case SundialRequest::MDCC_ProposeFast:
            // from coordinator to participant/acceptor, fast
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                txn = new TxnManager();
                txn->set_txn_id(txn_id);
                txn_table->add_txn(txn);
            }
            txn->lock();
            rc = txn->process_mdcc_2bfast(request, response);
            txn->unlock();
            txn_table->return_txn(txn);
            if (rc == ABORT) {
                txn_table->remove_txn(txn, true);
                delete txn;
            }
            break;
        case SundialRequest::MDCC_COMMIT_REQ:
            txn = txn_table->get_txn(txn_id, false, true);
            if (txn == nullptr) {
                response->set_request_type(SundialResponse::MDCC_Visibility);
                response->set_response_type(SundialResponse::ACK);
                return;
            }
#if DEBUG_PRINT
            printf("[node-%u txn-%lu] receive remote commit request\n",
                 g_node_id, txn_id);
#endif
            txn->lock();
            txn->process_mdcc_visibility(request, response, COMMIT);
            txn->unlock();
            txn_table->remove_txn(txn, false);
            delete txn;
            break;
        case SundialRequest::MDCC_ABORT_REQ:
            txn = txn_table->get_txn(txn_id, false, true);
            if (txn == nullptr) {
                response->set_request_type(SundialResponse::MDCC_Visibility);
                response->set_response_type(SundialResponse::ACK);
                return;
            }
#if DEBUG_PRINT
            printf("[node-%u txn-%lu] receive remote abort request\n",
                 g_node_id, txn_id);
#endif
            txn->lock();
            txn->process_mdcc_visibility(request, response, ABORT);
            txn->unlock();
            txn_table->remove_txn(txn, false);
            delete txn;
            break;
        case SundialRequest::MDCC_SINGLEPART_COMMIT:
            txn = new TxnManager();
            txn->set_txn_id(txn_id);
            txn_table->add_txn(txn);
#if DEBUG_PRINT
            printf("[node-%u txn-%lu] receive single part request\n",
                 g_node_id, txn_id);
#endif
            txn->process_mdcc_visibility(request, response, COMMIT);
            txn_table->remove_txn(txn, false);
            delete txn;
            break;
        case SundialRequest::MDCC_SINGLEPART_ABORT:
            txn = new TxnManager();
            txn->set_txn_id(txn_id);
            txn_table->add_txn(txn);
#if DEBUG_PRINT
            printf("[node-%u txn-%lu] receive single part request\n",
                 g_node_id, txn_id);
#endif
            txn->process_mdcc_visibility(request, response, ABORT);
            txn_table->remove_txn(txn, false);
            delete txn;
            break;
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
        //ctx_.AsyncNotifyWhenDone(this);
        service_->RequestcontactRemote(&ctx_, &request_, &responder_, cq_,
                                       cq_, this);
        status_ = PROCESS;
    } else if (status_ == PROCESS) {
        // Spawn a new CallData instance to serve new clients while processing
        new CallData(service_, cq_);
        processContactRemote(&ctx_, &request_ , &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
#if DEBUG_PRINT
        printf("request-%d is finish\n", request_.request_type());
#endif
    } else  {
        GPR_ASSERT(status_ == FINISH);
        delete this;
        if (glob_manager->recv_terminate_request) {
            glob_manager->active = false;
        }
#if DEBUG_PRINT
        printf("finished request is deleted\n");
#endif
    }
}
