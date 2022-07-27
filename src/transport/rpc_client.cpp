#include "global.h"
#include "rpc_client.h"
#include "stats.h"
#include "manager.h"
#include "txn.h"
#include "txn_table.h"

SundialRPCClient::SundialRPCClient() {
    _servers = new SundialRPCClientStub * [g_num_nodes];
    _storage_servers = new SundialRPCClientStub * [g_num_storage_nodes];
    _threads = new std::thread * [g_num_nodes];
    _storage_threads = new std::thread * [g_num_storage_nodes];
    // get server names
    std::ifstream in(ifconfig_file);
    string line;
    uint32_t node_id = 0;
    while ( node_id  < g_num_nodes && getline(in, line) )
    {
        if (line[0] == '#')
            continue;
        else if ((line[0] == '=' && line[1] == 'l') || node_id == g_num_nodes)
            break;
        else {
            string url = line;
#if NODE_TYPE == COMPUTE_NODE
            if (node_id == g_node_id) {
              node_id ++;
              continue;
            }
#endif
            _servers[node_id] = new SundialRPCClientStub(grpc::CreateChannel(url,
                grpc::InsecureChannelCredentials()));
            // spawn a reader thread for each server to indefinitely read completion
            // queue
            _threads[node_id] = new std::thread(AsyncCompleteRpc,
                                                        this, node_id);
            cout << "[Sundial] init rpc client to - " << node_id << " at " <<
                url << endl;
            node_id ++;
        }
    }
#if NUM_STORAGE_NODES > 0
    node_id = 0;
    bool is_storage_node = false;
    while ( node_id  < g_num_storage_nodes && getline(in, line) )
    {
      if (line[0] == '#')
        continue;
      else if ((line[0] == '=' && line[1] == 's')) {
          is_storage_node = true;
          continue;
      } else {
          if (!is_storage_node)
              continue;
#if NODE_TYPE == STORAGE_NODE
          if (node_id == g_node_id) {
            node_id ++;
            continue;
          }
#endif
        _storage_servers[node_id] = new SundialRPCClientStub
          (grpc::CreateChannel(line, grpc::InsecureChannelCredentials()));
        // spawn a reader thread for each server to indefinitely read completion
        // queue
        _storage_threads[node_id] = new std::thread(AsyncCompleteRpcStorage,
                                                    this, node_id);
        cout << "[Sundial] init rpc storage client - " << node_id << " at " <<
        line << endl;
        node_id ++;
      }
    }
#endif
    cout << "[Sundial] rpc client is initialized!" << endl;
}


void
SundialRPCClient::AsyncCompleteRpcStorage(SundialRPCClient * s, uint64_t
node_id) {
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    while (true) {
        s->_storage_servers[node_id]->cq_.Next(&got_tag, &ok);
        // The tag in this example is the memory location of the call object
        auto call = static_cast<AsyncClientCall*>(got_tag);
        if (!call->status.ok()) {
            printf("[REQ] client rec response fail: (%d) %s\n",
                   call->status.error_code(), call->status.error_message().c_str());
        }
        // handle return value for non-system response
        assert(call->reply->response_type() != SundialResponse::SYS_RESP);
        s->sendRequestDone(call->request, call->reply);
        // Once we're complete, deallocate the call object.
        delete call;
    }

}

void
SundialRPCClient::AsyncCompleteRpc(SundialRPCClient * s, uint64_t
node_id) {
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    while (true) {
        s->_servers[node_id]->cq_.Next(&got_tag, &ok);
        // The tag in this example is the memory location of the call object
        auto call = static_cast<AsyncClientCall*>(got_tag);
        if (!call->status.ok()) {
            printf("[REQ] client rec response fail: (%d) %s\n",
                   call->status.error_code(), call->status.error_message().c_str());
        }
        // handle return value for non-system response
        assert(call->reply->response_type() != SundialResponse::SYS_RESP);
        s->sendRequestDone(call->request, call->reply);
        // Once we're complete, deallocate the call object.
        delete call;
    }

}

RC
SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request,
    SundialResponse &response, bool is_storage) {
    if (!glob_manager->active && (request.request_type() !=
    SundialRequest::SYS_REQ))
        return FAIL;
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] send sync request-%d\n", g_node_id, request
        .txn_id(), request.request_type());
#endif
    ClientContext context;
    request.set_request_time(get_sys_clock());
    Status status;
    if (!is_storage)
        status = _servers[node_id]->contactRemote(&context, request, &response);
    else
        status = _storage_servers[node_id]->contactRemote(&context,
                                                              request, &response);
    if (!status.ok()) {
        printf("[REQ] client sendRequest fail: (%d) %s\n",
               status.error_code(), status.error_message().c_str());
        assert(false);
    }
    uint64_t latency = get_sys_clock() - request.request_time();
    glob_stats->_stats[GET_THD_ID]->_req_msg_avg_latency[response
    .response_type()] += latency;
    if (latency > glob_stats->_stats[GET_THD_ID]->_req_msg_max_latency
    [response.response_type()]) {
        glob_stats->_stats[GET_THD_ID]->_req_msg_max_latency
        [response.response_type()] = latency;
    }
    if (latency < glob_stats->_stats[GET_THD_ID]->_req_msg_min_latency
    [response.response_type()]) {
        glob_stats->_stats[GET_THD_ID]->_req_msg_min_latency
        [response.response_type()] = latency;
    }
    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response.response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response.response_type() ] += response.SpaceUsedLong();
	return RCOK;
}

RC
SundialRPCClient::sendRequestAsync(TxnManager * txn, uint64_t node_id,
                                   SundialRequest &request, SundialResponse
                                   &response, bool is_storage)
{
    if (!glob_manager->active && (request.request_type() !=
    SundialRequest::TERMINATE_REQ))
        return FAIL;
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] send async request-%d\n", g_node_id, request
        .txn_id(), request.request_type());
#endif
    if ((is_storage && NODE_TYPE == STORAGE_NODE) || (!is_storage &&
    NODE_TYPE == COMPUTE_NODE))
        assert( node_id != g_node_id);
    // call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;;
    request.set_request_time(get_sys_clock());
    request.set_thread_id(GET_THD_ID);
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    if (!is_storage)
        call->response_reader = _servers[node_id]->stub_->PrepareAsynccontactRemote(&call->context, request, &_servers[node_id]->cq_);
    else
        call->response_reader =
            _storage_servers[node_id]->stub_->PrepareAsynccontactRemote
            (&call->context, request, &_storage_servers[node_id]->cq_);

    // StartCall initiates the RPC call
    call->request = &request;
    call->response_reader->StartCall();
    call->reply = &response;
    call->response_reader->Finish(call->reply, &(call->status), (void*)call);
	return RCOK;
}

void
SundialRPCClient::sendRequestDone(SundialRequest * request, SundialResponse *
response)
{
    // RACE CONDITION (solved): should assign thd id to server thread
    uint64_t thread_id = request->thread_id();
    uint64_t latency = get_sys_clock() - request->request_time();
    glob_stats->_stats[thread_id]->_req_msg_avg_latency[response->response_type()] += latency;
    if (latency > glob_stats->_stats[thread_id]->_req_msg_max_latency
    [response->response_type()]) {
        glob_stats->_stats[thread_id]->_req_msg_max_latency
        [response->response_type()] = latency;
    }
    if (latency < glob_stats->_stats[thread_id]->_req_msg_min_latency
    [response->response_type()]) {
        glob_stats->_stats[thread_id]->_req_msg_min_latency
        [response->response_type()] = latency;
    }
    glob_stats->_stats[thread_id]->_resp_msg_count[ response->response_type() ]++;
    glob_stats->_stats[thread_id]->_resp_msg_size[ response->response_type() ] += response->SpaceUsedLong();

    uint64_t txn_id = response->txn_id();
#if DEBUG_PRINT
    printf("[node-%u, txn-%lu] receive remote reply-%d\n", g_node_id,
           txn_id, response->response_type());
#endif
        TxnManager * txn;
        switch (response->request_type()) {
            case SundialResponse::PREPARE_REQ :
                txn = txn_table->get_txn(txn_id);
                txn->handle_prepare_resp(response);
                break;
            case SundialResponse::MDCC_Phase2bClassic:
                if (response->node_type() == SundialResponse::PARTICIPANT) {
                    // case 1: sent from participant to coordinator as reply
                    txn = txn_table->get_txn(txn_id);
                    // update remote_node stats as well
                    txn->handle_prepare_resp(response);
                    txn->increment_replied_acceptors(response->node_id());
                } else if (response->node_type() == SundialResponse::STORAGE) {
                    // case 2: reply from acceptors which treats coordinator
                    // as leader. has to be prepared ok.
                    txn->increment_replied_acceptors(response->node_id());
                }
                return; // no need to update rpc semaphore
            case SundialResponse::MDCC_Phase2bFast :
                // sent from participant/acceptors to coordinator
                txn = txn_table->get_txn(txn_id);
                // update remote_node stats as well
                txn->handle_prepare_resp(response);
                txn->increment_replied_acceptors(response->node_id());
              return; // no need to update rpc semaphore
            case SundialResponse::MDCC_Visibility :
                txn = txn_table->get_txn(txn_id);
                txn->increment_replied_acceptors(request->node_id());
                return;
            case SundialResponse::MDCC_DummyReply:
                // no need decr semaphore as well
                return;
            case SundialResponse::TERMINATE_REQ:
                // dont decr semaphore, and terminate request dont need retrieve txn
                return;
            default:
                txn = txn_table->get_txn(txn_id);
                break;
        }
        txn->rpc_semaphore->decr();
}
