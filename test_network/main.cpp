#include <string>
#include <fstream>
#include <iostream>
#include <unistd.h>

#include "rpc_server.h"
#include "rpc_client.h"
//#include "redis_client.h"
//#include "azure_blob_client.h"

void * start_thread(void *);
void * start_rpc_server(void *);
void get_node_id();

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

    cout << "[Sundial] start node " << g_node_id << endl;

    rpc_client = new SundialRPCClient();
    rpc_server = new SundialRPCServerImpl;
    rpc_server->run();
//    pthread_t * pthread_rpc = new pthread_t;
//    pthread_create(pthread_rpc, NULL, start_rpc_server, NULL);

    #if LOG_DEVICE == LOG_DVC_REDIS
        // assume a shared logging but store different node's info to different key
        cout << "creat Redis client!!!!!!!" << endl;
        redis_client = new RedisClient();
    #elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
        cout << "creat Azure client!!!!!!!" << endl;
        azure_blob_client = new AzureBlobClient();
    #endif

    // make sure server is setup before moving on
    sleep(20);
    uint64_t starttime;
    uint64_t endtime;

    cout << "[Sundial] Synchronization starts on node " << g_node_id << endl;
    // Notify other nodes that the current node has finished initialization
    for (int iter = 0; iter < 5; iter++) {
        for (uint32_t i = 0; i < g_num_nodes; i++) {
            if (i == g_node_id) continue;
            SundialRequest request;
            SundialResponse response;
            request.set_request_type(SundialRequest::SYS_REQ);
            starttime = get_sys_clock();
            rpc_client->sendRequest(i, request, response);
            endtime = get_sys_clock() - starttime;
            cout << "[Sundial] network round-trip from node" << g_node_id <<
            "to node " << i << ": " << endtime / 1000 << " us" << endl;
        }
    }
    return 0;
}


void * start_thread(void * thread) {
    ((BaseThread *)thread)->run();
    return NULL;
}

void * start_rpc_server(void * input) {
    rpc_server->run();
    return NULL;
}

void get_node_id()
{
    // get server names
    vector<string> _urls;
    string line;
    std::ifstream file (ifconfig_file);
    assert(file.is_open());
    while (getline (file, line)) {
        if (line[0] == '#')
            continue;
        else {
            std::string delimiter = ":";
            std::string token = line.substr(0, line.find(delimiter));
            _urls.push_back(token);
        }
    }
    char hostname[1024];
    gethostname(hostname, 1023);
    printf("[!] My Hostname is %s\n", hostname);
    for (uint32_t i = 0; i < g_num_nodes_and_storage; i ++)  {
        if (_urls[i] == string(hostname)) {
            printf("[!] My node id id %u\n", i);
            g_node_id = i;
        }
    }
    file.close();
}
