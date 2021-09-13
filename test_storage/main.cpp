#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "global.h"
#include "rpc_server.h"
#include "rpc_client.h"
//#include "redis_client.h"
//#include "azure_blob_client.h"

// defined in parser.cpp
void parser(int argc, char ** argv);

int main(int argc, char* argv[])
{
    for (int i = 1; i < argc; i++) {
        assert(argv[i][0] == '-');
        if (argv[i][1] == 'G') {
            if (argv[i][2] == 'n')
                g_node_id = atoi( &argv[i][3] );
        }
    }

    std::cout << "[Sundial] start node " << g_node_id << std::endl;

    rpc_server = new SundialRPCServerImpl;
    rpc_server->run();
    rpc_client = new SundialRPCClient();

#if LOG_DEVICE == LOG_DVC_REDIS
    // assume a shared logging but store different node's info to different key
    std::cout << "creat Redis client!!!!!!!" << std::endl;
    redis_client = new RedisClient();
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
    std::cout << "creat Azure client!!!!!!!" << std::endl;
    azure_blob_client = new AzureBlobClient();
#endif

    // make sure server is setup before moving on
    sleep(30);

    std::cout << "[Sundial] Synchronization starts on node " << g_node_id << std::endl;
    // Notify other nodes that the current node has finished initialization
    int num_iter = 3;
    for (int iter = 0; iter < num_iter; iter++) {
        for (uint32_t i = 0; i < g_num_nodes; i++) {
            if (i == g_node_id) continue;
            SundialRequest request;
            SundialResponse response;
            request.set_request_type(SundialRequest::SYS_REQ);
            request.set_node_id(i);
            rpc_client->sendRequest(i, request, response);
        }
    }
    while (g_num_rpc_recv < (g_num_nodes - 1) * num_iter) {
	}
    return 0;
}

