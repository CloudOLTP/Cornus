#include "global.h"
#include "rpc_client.h"
#include "stats.h"
#include "manager.h"
#include "txn.h"
#include "txn_table.h"

SundialRPCClient::SundialRPCClient() {
    _servers = new SundialRPCClientStub * [g_num_nodes];
    _storage_servers = new SundialRPCClientStub * [g_num_storage_nodes];
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
            if (node_id != g_node_id) {
                _servers[node_id] = new SundialRPCClientStub(grpc::CreateChannel(url,
                    grpc::InsecureChannelCredentials()));
                cout << "[Sundial] init rpc client - " << node_id << " at " << url << endl;
            }
            node_id ++;
        }
    }
    cout << "[Sundial] rpc client is initialized!" << endl;
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
        _storage_servers[node_id] = new SundialRPCClientStub
          (grpc::CreateChannel(line, grpc::InsecureChannelCredentials()));
        cout << "[Sundial] init rpc storage client - " << node_id << " at " <<
        line << endl;
        node_id ++;
      }
    }
    cout << "[Sundial] rpc client is initialized!" << endl;
#endif
    // spawn a reader thread to indefinitely read completion queue
    _thread = new std::thread(AsyncCompleteRpc, this);
    // use a single cq for different channels.
}

void
SundialRPCClient::AsyncCompleteRpc(SundialRPCClient * s) {
    void* got_tag;
    bool ok = false;
    // Block until the next result is available in the completion queue "cq".
    while (s->cq.Next(&got_tag, &ok)) {
        // The tag in this example is the memory location of the call object
        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
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
    if (is_storage)
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
    assert(node_id != g_node_id);
    // call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;;
    assert(node_id != g_node_id);
    request.set_request_time(get_sys_clock());
    request.set_thread_id(GET_THD_ID);
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    if (!is_storage)
        call->response_reader = _servers[node_id]->stub_->PrepareAsynccontactRemote(&call->context, request, &cq);
    else
        call->response_reader =
            _storage_servers[node_id]->stub_->PrepareAsynccontactRemote
            (&call->context, request, &cq);

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
            case SundialResponse::MDCC_Phase2bClassic :
                if (response->response_type() == SundialResponse::ACK)
                    return; // ignore if from acceptor to participant
                // else, sent from participant to coordinator
                txn = txn_table->get_txn(txn_id);
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
