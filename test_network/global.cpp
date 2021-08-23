#include "rpc_client.h"
#include "rpc_server.h"
#include "redis_client.h"
#include "azure_blob_client.h"

char            ifconfig_file[80]       = "ifconfig.txt";


// Distributed DBMS
// ================
uint32_t        g_num_nodes             = NUM_NODES;
uint32_t        g_node_id               = NUM_NODES;

SundialRPCClient *  rpc_client;
SundialRPCServerImpl * rpc_server;
#if LOG_DEVICE == LOG_DVC_REDIS
RedisClient *       redis_client;
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
AzureBlobClient *       azure_blob_client;
#endif
