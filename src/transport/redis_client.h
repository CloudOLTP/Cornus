//
// Created by Zhihan Guo on 4/5/21.
//

#ifndef SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
#define SUNDIAL_TRANSPORT_REDIS_CLIENT_H_

#include <cpp_redis/cpp_redis>
#include <string>

#include "helper.h"
#include "config.h"

#if LOG_DEVICE == LOG_DVC_REDIS || LOG_DEVICE == LOG_DVC_CUSTOMIZED

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
    cpp_redis::client* clients[NUM_WORKER_THREADS];
    bool tls;
};

#endif

#endif //SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
