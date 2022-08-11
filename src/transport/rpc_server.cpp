#include "rpc_server.h"
#include "global.h"
#include "txn.h"
#include "txn_table.h"
#include "manager.h"
#include "log.h"
#include "redis_client.h"
#include "azure_blob_client.h"


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
        _thread_pool[i] = new std::thread(HandleRpcs, this, i + 1);
    }
    cout <<"[Sundial] rpc server initialized, listening on " << line << endl;
}

void SundialRPCServerImpl::HandleRpcs(SundialRPCServerImpl * s, uint32_t
thd_id) {
    // Spawn a new CallData instance to serve new clients.
    new CallData(&(s->service_), s->cq_.get(), thd_id);
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
      static_cast<CallData*>(tag)->Proceed(thd_id);

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

    uint64_t txn_id = request->txn_id();
    if ((int) request->request_type() <= (int) SundialResponse::TERMINATE_REQ) {
        auto tpe = (SundialResponse::RequestType) ((int)
            request->request_type());
        response->set_request_type(tpe);
    }
    response->set_txn_id(txn_id);
    response->set_node_id(g_node_id);
    RC rc = RCOK;
    TxnManager * txn;
    string data;
    SundialRequest * forward_req;
    SundialResponse * forward_resp;
    size_t idx;

    switch (request->request_type()) {
        case SundialRequest::SYS_REQ:
            glob_manager->receive_sync_request();
            return;
        case SundialRequest::READ_REQ:
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
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
            rc = txn->process_read_request(request, response);
            if (rc == ABORT) {
                txn_table->remove_txn(txn);
                delete txn;
            }
            response->set_txn_id(txn_id);
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            break;
        case SundialRequest::TERMINATE_REQ:
#if NODE_TYPE == COMPUTE_NODE
            txn = txn_table->get_txn(txn_id,true);
            if (txn == nullptr) {
                return;
            }
            txn->lock();
            rc = txn->process_terminate_request(request, response);
            txn->unlock();
            txn_table->remove_txn(txn);
            delete txn;
            break;
#else
            cout << "receive terminate request" << endl;
            response->set_request_type(SundialResponse::SYS_REQ);
            response->set_response_type(SundialResponse::ACK);
            glob_manager->active = false;
#if DEBUG_PRINT
            cout << "set active status to false" << endl;
#endif
            return;
#endif
        case SundialRequest::PREPARE_REQ:
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
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
            txn->process_prepare_request(request, response);
            if (txn->get_txn_state() != TxnManager::PREPARED) {
                txn_table->remove_txn(txn);
                delete txn;
            }
            response->set_txn_id(txn_id);
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            break;
        case SundialRequest::COMMIT_REQ:
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
#if DEBUG_PRINT
          printf("[node-%u, txn-%lu] receive remote commit request\n",
                 g_node_id, txn_id);
#endif
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, COMMIT);
            txn_table->remove_txn(txn);
            delete txn;
            response->set_txn_id(txn_id);
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            break;
        case SundialRequest::ABORT_REQ:
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
#if DEBUG_PRINT
          printf("[node-%u txn-%lu] receive remote abort request\n",
                 g_node_id, txn_id);
#endif
            txn = txn_table->get_txn(txn_id, true);
            if (txn == nullptr) {
                response->set_response_type(SundialResponse::ACK);
                return;
            }
            rc = txn->process_decision_request(request, response, ABORT);
            txn_table->remove_txn(txn);
            delete txn;
            response->set_txn_id(txn_id);
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            break;
        case  SundialRequest::PAXOS_LOG:
#if DEBUG_PRINT
            printf("[node-%u txn-%lu] receive remote paxos log request\n",
                 g_node_id, txn_id);
#endif
            // create the txn to use its semaphore
            txn = new TxnManager();
            txn->set_txn_id(txn_id);
            txn->sendReplicateRequest(static_cast<TxnManager::State>(request->txn_state()),
                                      request->log_data_size());
            // forward request
#if COMMIT_VAR == CORNUS_OPT
            if (request->node_id() != request->coord_id() &&
            request->forward_msg() != SundialRequest::ACK) {
#if DEBUG_PRINT
                printf("[txn-%lu] send paxos log forward request-%d from "
                   "node-%lu\n", txn_id,
                   request->forward_msg(), request->node_id());
#endif
                idx = request->thd_id() * g_num_nodes + request->node_id();
                glob_manager->thd_requests_[idx].set_request_type
                    (SundialRequest::PAXOS_LOG_FORWARD);
                glob_manager->thd_requests_[idx].set_forward_msg(
                    request->forward_msg());
                glob_manager->thd_requests_[idx].set_txn_id(
                    txn_id);
                glob_manager->thd_requests_[idx].set_node_id
                    (request->node_id());
                rpc_client->sendRequestAsync(txn,
                                         request->coord_id(),
                                         glob_manager->thd_requests_[idx],
                                         glob_manager->thd_responses_[idx],
                                         false);
            }
#endif
            delete txn;
            // once logged, reply to participant or coordinator
            response->set_request_type(sundial_rpc::SundialResponse_RequestType_PAXOS_LOG_ACK);
            break;
        case SundialRequest::PAXOS_LOG_FORWARD:
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            assert(request->forward_msg() != SundialRequest::RESP_OK);
            // response->set_request_type(SundialResponse::DummyReply);
            response->set_request_type(SundialResponse::PAXOS_FORWARD_ACK);
            txn = txn_table->get_txn(txn_id, true);
            assert(txn);
            // handle reply, abort if not prepared ok
            txn->handle_prepare_resp((SundialResponse::ResponseType) ((int)
            request->forward_msg()), request->node_id());
            txn->rpc_semaphore->decr();
#if DEBUG_PRINT
            printf("[txn-%lu] receive paxos log forward request-%d from "
                   "node-%lu and "
                   "decr txn semaphore\n", txn_id,
                   request->forward_msg(), request->node_id());
#endif
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            break;
        case SundialRequest::PAXOS_REPLICATE:
#if LOG_DELAY > 0
            usleep(LOG_DELAY);
#endif
#if DEBUG_PRINT
            printf("[node-%u txn-%lu] receive remote paxos replicate\n",
                 g_node_id, txn_id);
#endif
            data = "[LSN] placehold:" + string(request->log_data_size(), 'd');
            redis_client->log_sync_data(request->node_id(), request->txn_id(),
                                        request->txn_state(), data);
            // once logged, reply to participant or coordinator
            response->set_request_type(sundial_rpc::SundialResponse_RequestType_PAXOS_LOG_ACK);
            break;
        case sundial_rpc::SundialRequest_RequestType_PAXOS_LOG_COLOCATE:
            data = "[LSN] placehold:" + string(request->log_data_size(), 'd');
            redis_client->log_sync_data(request->node_id(), request->txn_id(), request->txn_state(), data);
#if LOG_DELAY > 0
            if (request->receiver_id() != g_node_id)
                usleep(LOG_DELAY / 2);
#endif
            // once logged, reply to participant or coordinator
            response->set_request_type(sundial_rpc::SundialResponse_RequestType_PAXOS_LOG_ACK);
#if COMMIT_VAR == MDCC_CLASSIC
            if (request->node_id() != request->coord_id() &&
                request->forward_msg() != SundialRequest::ACK) {
                idx = request->thd_id() * g_num_nodes + request->node_id();
                txn = txn_table->get_txn(txn_id, true);
                glob_manager->thd_requests_[idx].set_request_type
                    (SundialRequest::PAXOS_LOG_COLOCATE_FORWARD);
                glob_manager->thd_requests_[idx].set_forward_msg(
                    request->forward_msg());
                glob_manager->thd_requests_[idx].set_txn_id(
                    txn_id);
                glob_manager->thd_requests_[idx].set_node_id
                    (request->node_id());
                rpc_client->sendRequestAsync(txn,
                                             request->coord_id(),
                                             glob_manager->thd_requests_[idx],
                                             glob_manager->thd_responses_[idx],
                                             false);
            }
#endif
#if LOG_DELAY > 0
            if (request->receiver_id() != g_node_id)
                usleep(LOG_DELAY / 2);
#endif
            break;
       case SundialRequest::PAXOS_LOG_COLOCATE_FORWARD:
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            assert(request->forward_msg() != SundialRequest::RESP_OK);
            // response->set_request_type(SundialResponse::DummyReply);
            response->set_request_type(SundialResponse::PAXOS_FORWARD_ACK);
            txn = txn_table->get_txn(txn_id, true);
            assert(txn);
            // handle reply, abort if not prepared ok
            txn->handle_prepare_resp((SundialResponse::ResponseType) ((int)
                request->forward_msg()), request->node_id());
            txn->rpc_semaphore->decr();
#if DEBUG_PRINT
            printf("[txn-%lu] receive paxos log forward request-%d from "
                   "node-%lu and "
                   "decr txn semaphore\n", txn_id,
                   request->forward_msg(), request->node_id());
#endif
#if LOG_DELAY > 0
            usleep(LOG_DELAY / 2);
#endif
            break;
        default:
            assert(false);
    }
    // the transaction handles the RPC call
#if FAILURE_ENABLE
    if (rc == FAIL || (!glob_manager->active)) {
#if DEBUG_PRINT
        printf("[node-%u, txn-%lu] reply failure response\n", g_node_id,
            txn_id);
#endif
        response->set_response_type(SundialResponse::RESP_FAIL);
    }
#endif
}


SundialRPCServerImpl::CallData::CallData(SundialRPC::AsyncService* service,
    ServerCompletionQueue* cq, uint32_t thd_id) : service_(service), cq_(cq),
    responder_(&ctx_), status_(CREATE) {
    Proceed(thd_id);
}

void
SundialRPCServerImpl::CallData::Proceed(uint32_t thd_id) {
    if (status_ == CREATE) {
        //ctx_.AsyncNotifyWhenDone(this);
        service_->RequestcontactRemote(&ctx_, &request_, &responder_, cq_,
                                       cq_, this);
        status_ = PROCESS;
    } else if (status_ == PROCESS) {
        // Spawn a new CallData instance to serve new clients while processing
        new CallData(service_, cq_, thd_id);
        reply_.set_thd_id(thd_id);
        processContactRemote(&ctx_, &request_ , &reply_);
        status_ = FINISH;
        responder_.Finish(reply_, Status::OK, this);
    } else  {
        GPR_ASSERT(status_ == FINISH);
        delete this;
    }
}
