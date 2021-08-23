//
// Created by Zhihan Guo on 8/22/21.
//

#ifndef SUNDIAL_TEST_GRPC_GLOBAL_H_
#define SUNDIAL_TEST_GRPC_GLOBAL_H_

extern uint32_t         g_num_nodes;
extern uint32_t         g_node_id;

extern char *           ifconfig_file;

extern SundialRPCClient * rpc_client;
extern SundialRPCServerImpl * rpc_server;
#if LOG_DEVICE == LOG_DVC_REDIS
extern RedisClient *      redis_client;
#elif LOG_DEVICE == LOG_DVC_AZURE_BLOB
extern AzureBlobClient *      azure_blob_client;
#endif

#endif //SUNDIAL_TEST_GRPC_GLOBAL_H_
