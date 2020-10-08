#include "global.h"
#include "rpc_client.h"
#include "stats.h"
#include "manager.h"
#include "txn.h"
#include "txn_table.h"

//void 
//SundialRPCClient::run() {
SundialRPCClient::SundialRPCClient() {
    _servers = new SundialRPCClientStub  * [g_num_nodes];
    // get server names
    //std::istringstream in(ifconfig_string);
    std::ifstream in(ifconfig_file);
    string line;
    uint32_t num_nodes = 0;
    while ( num_nodes < g_num_nodes && getline(in, line) ) {
        if (line[0] == '#')
            continue;
        else {
            string url = line;
            if (num_nodes != g_node_id) {
                SundialRPCClientStub c(grpc::CreateChannel(url, grpc::InsecureChannelCredentials()));
                _servers[num_nodes] = &c;
            }
            num_nodes ++;
        }
    }
    cout << "sundial client is initialized!" << endl;
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
        GPR_ASSERT(ok);
        assert(call->status.ok());
        // handle return value
        s->sendRequestDone(call->reply);
        // Once we're complete, deallocate the call object.
        delete call;
    }
}

void
SundialRPCClient::sendRequest(uint64_t node_id, SundialRequest &request, SundialResponse &response) {
    ClientContext context;
    Status status = _servers[node_id]->contactRemote(&context, request, &response);
    assert(status.ok());
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
    //printf("[REQ] send to node %ld. type=%s\n", node_id,
    //       SundialRequest::RequestType_Name(request.request_type()).c_str());
    assert(node_id != g_node_id);
    glob_stats->_stats[GET_THD_ID]->_req_msg_count[ request.request_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_req_msg_size[ request.request_type() ] += request.SpaceUsedLong();
    _servers[node_id]->stub_->PrepareAsynccontactRemote(&call->context, request, &cq);
    
    // StartCall initiates the RPC call
    // TODO(zhihan): set timeout
    // std::chrono::time_point<std::chrono::system_clock> _deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(3100);
    // call->context.set_deadline(_deadline);
    call->response_reader->StartCall();
    call->reply = response;
    call->response_reader->Finish(&(call->reply), &(call->status), (void*)call);
    // dead code for previous rpc
    /*
    Closure* call_done = NewCallback(this, &SundialRPCClient::sendRequestDone, rpc, txn, response);
    _servers[node_id]->contactRemote(rpc, &request, &response, call_done);
    assert( rpc::status::IsOk(rpc) );
    */
}


void
SundialRPCClient::sendRequestDone(SundialResponse& response)
{
    uint64_t txn_id = response.txn_id();
    TxnManager * txn = txn_table->get_txn(txn_id);
    //rpc->CheckSuccess();
    glob_stats->_stats[GET_THD_ID]->_resp_msg_count[ response.response_type() ] ++;
    glob_stats->_stats[GET_THD_ID]->_resp_msg_size[ response.response_type() ] += response.SpaceUsedLong();
    // mark as returned. 
    txn->rpc_semaphore->decr();
}
