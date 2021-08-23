#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>

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
            if (argv[i][2] == 'n') {
                g_node_id = atoi( &argv[i][3] );
        }
    }

    std::cout << "[Sundial] start node " << g_node_id << std::endl;

    rpc_client = new SundialRPCClient();
    rpc_server = new SundialRPCServerImpl;
    rpc_server->run();
//    pthread_t * pthread_rpc = new pthread_t;
//    pthread_create(pthread_rpc, NULL, start_rpc_server, NULL);

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
    uint64_t starttime;
    uint64_t endtime;

    std::cout << "[Sundial] Synchronization starts on node " << g_node_id << std::endl;
    // Notify other nodes that the current node has finished initialization
    for (int iter = 0; iter < 1; iter++) {
        for (uint32_t i = 0; i < g_num_nodes; i++) {
            if (i == g_node_id) continue;
            SundialRequest request;
            SundialResponse response;
            request.set_request_type(SundialRequest::SYS_REQ);
            starttime = get_sys_clock();
            rpc_client->sendRequest(i, request, response);
            endtime = get_sys_clock() - starttime;
            std::cout << "[Sundial] network round-trip from node " << g_node_id <<
            " to node " << i << ": " << endtime / 1000 << " us" << std::endl;
        }
    }
    return 0;
}

