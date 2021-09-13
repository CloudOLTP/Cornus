//
// Created by Zhihan Guo on 4/5/21.
//

#ifndef SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
#define SUNDIAL_TRANSPORT_REDIS_CLIENT_H_

#if LOG_DEVICE == LOG_DVC_REDIS

#include <cpp_redis/cpp_redis>
#include <string>

#include "helper.h"

class RedisClient {
  public:
    RedisClient();
    RC log_sync(uint64_t node_id, uint64_t txn_id, int status);
    RC log_async(uint64_t node_id, uint64_t txn_id, int status);
    RC log_if_ne(uint64_t node_id, uint64_t txn_id);
    RC log_if_ne_data(uint64_t node_id, uint64_t txn_id, std::string & data);
    RC log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
        std::string & data);
    RC log_async_data(uint64_t node_id, uint64_t txn_id, int status,
        std::string & data);
  private:
    cpp_redis::client client;
};

#endif

#endif //SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
