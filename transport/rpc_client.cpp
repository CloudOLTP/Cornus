#include "global.h"
#include "rpc_client.h"
#include "stats.h"
#include "manager.h"
#include "txn.h"
#include "txn_table.h"

#define IS_LOG_RESPONSE(type)  \
     (type == SundialResponse::RESP_LOG_YES \
            || type == SundialResponse::RESP_LOG_ABORT \
            || type== SundialResponse::RESP_LOG_COMMIT)

SundialRPCClient::SundialRPCClient() {
#if LOG_REMOTE && LOG_DEVICE == LOG_DVC_NATIVE
    _servers = new SundialRPCClientStub * [g_num_nodes_and_storage];
#else
    _servers = new SundialRPCClientStub * [g_num_nodes];
#endif
    // get server names
    std::ifstream in(ifconfig_file);
    string line;
    uint32_t node_id = 0;
#if LOG_REMOTE && LOG_DEVICE == LOG_DVC_NATIVE
    while ( node_id  < g_num_nodes_and_storage && getline(in, line) ) {
#else
    while ( node_id  < g_num_nodes && getline(in, line) ) {
#endif
        if (line[0] == '#')
            continue;
        else if (line[0] == '=' && line[1] == 'l')
            break;
        else {
            string url = line;
            if (node_id != g_node_id) {
                _servers[node_id] = new SundialRPCClientStub(grpc::CreateChannel(url, grpc::InsecureChannelCredentials()));
                cout << "[Sundial] init rpc client - " << node_id << " at " << url << endl;
            }
            node_id ++;
        }
    }
    cout << "[Sundial] rpc client is initialized!" << endl;
    // spawn a reader thread to indefinitely read completion queue
    _thread = new std::thread(AsyncCompleteRpc, this);
    //pthread_create(_thread, NULL, AsyncCompleteRpc, this); 
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
            printf("[REQ] client rec response fail: (%d) %s\n", call->status.error_code(), call->status.error_message().c_str());
            assert(false);
        }
        // handle return value for non-system response
        assert(call->reply->response_type() != SundialResponse::SYS_RESP);
        s->sendRequestDone(call->reply);
        // Once we're complete, deallocate the call object.
        delete call;
    }
}

void
SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request, SundialResponse &response) {
    ClientContext context;
    Status status = _servers[node_id]->contactRemote(&context, request, &response);
    if (!status.ok()) {
        printf("[REQ] client sendRequest fail: (%d) %s\n", status.error_code(), status.error_message().c_str());
        assert(false);
    }
    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response.response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response.response_type() ] += response.SpaceUsedLong();
}

void
SundialRPCClient::sendRequestAsync(TxnManager * txn, uint64_t node_id,
                                   SundialRequest &request, SundialResponse &response)
{    
    assert(node_id != g_node_id);
    // call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;;
    //printf("[REQ] client send to node %ld. type=%s\n", node_id,
    //       SundialRequest::RequestType_Name(request.request_type()).c_str());
    assert(node_id != g_node_id);
    // RACE CONDITION: should assign thd id to server thread
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    call->response_reader = _servers[node_id]->stub_->PrepareAsynccontactRemote(&call->context, request, &cq);
    
    // StartCall initiates the RPC call
    // TODO(zhihan): set timeout
    // std::chrono::time_point<std::chrono::system_clock> _deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(3100);
    // call->context.set_deadline(_deadline);
    call->response_reader->StartCall();
    call->reply = &response;
    call->response_reader->Finish(call->reply, &(call->status), (void*)call);
}

// only called by commit protocol, read request will send synchronous request
void
SundialRPCClient::sendRequestDone(SundialResponse * response)
{
    uint64_t txn_id = response->txn_id();
    TxnManager * txn = txn_table->get_txn(txn_id);
    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response->response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response->response_type() ] += response->SpaceUsedLong();
    // mark as returned. 
    if (IS_LOG_RESPONSE(response->response_type())) {
        // not decrease semaphore for log resp
        txn->rpc_log_semaphore->decr();
    } else {
        txn->rpc_semaphore->decr();
    }
    // update txn status if return is abort
    // SundialResponse::RESP_LOG_ABORT is checked only for self-implemented
    // remote log manager
    if (response->response_type() == SundialResponse::PREPARED_ABORT ||
    response->response_type() == SundialResponse::RESP_LOG_ABORT ||
    response->response_type() == SundialResponse::RESP_ABORT) {
        txn->set_txn_state(TxnManager::ABORTED);
        txn->_remote_nodes_involved[response->node_id()]->state =
            TxnManager::ABORTED;
    } else if (response->response_type() == SundialResponse::PREPARED_OK) {
        txn->_remote_nodes_involved[response->node_id()]->state =
            TxnManager::PREPARED;
    } else if (response->response_type() == SundialResponse::PREPARED_OK_RO) {
        txn->_remote_nodes_involved[response->node_id()]->state =
            TxnManager::COMMITTED;
    }
    // set corresponding remote node status
}
}
