#include "global.h"
#include "rpc_client.h"
#include "stats.h"
#include "manager.h"
#include "txn.h"
#include "txn_table.h"

SundialRPCClient::SundialRPCClient() {
    _servers = new SundialRPCClientStub * [g_num_nodes];
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
        s->sendRequestDone(call->reply);
        // Once we're complete, deallocate the call object.
        delete call;
    }
}

RC
SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request,
    SundialResponse &response) {
    if (!glob_manager->active && (request.request_type() !=
    SundialRequest::SYS_REQ))
        return FAIL;
    ClientContext context;
    Status status = _servers[node_id]->contactRemote(&context, request, &response);
    if (!status.ok()) {
        printf("[REQ] client sendRequest to %lu fail: (%d) %s\n", node_id,
               status.error_code(), status.error_message().c_str());
    } else {
        printf("[REQ] client sendRequest to %lu succeed!\n", node_id);
    }
	return RCOK;
}

RC
SundialRPCClient::sendRequestAsync(uint64_t node_id,
                                   SundialRequest &request, SundialResponse &response)
{
    if (!glob_manager->active && (request.request_type() !=
    SundialRequest::TERMINATE_REQ))
        return FAIL;
    assert(node_id != g_node_id);
    // call object to store rpc data
    AsyncClientCall* call = new AsyncClientCall;;
    assert(node_id != g_node_id);
    call->response_reader = _servers[node_id]->stub_->PrepareAsynccontactRemote(&call->context, request, &cq);

    // StartCall initiates the RPC call
    call->response_reader->StartCall();
    call->reply = &response;
    call->response_reader->Finish(call->reply, &(call->status), (void*)call);
	return RCOK;
}


void
SundialRPCClient::sendRequestDone(SundialResponse * response)
{

}
