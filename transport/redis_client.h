//
// Created by Zhihan Guo on 4/5/21.
//

#ifndef SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
#define SUNDIAL_TRANSPORT_REDIS_CLIENT_H_

#include <cpp_redis/cpp_redis>
#include <string>

class RedisClient {
  public:
    RedisClient();
    void log_sync(uint64_t node_id, uint64_t txn_id, int status);
    void log_async(uint64_t node_id, uint64_t txn_id, int status);
    void log_if_ne(uint64_t node_id, uint64_t txn_id);
    void log_if_ne_data(uint64_t node_id, uint64_t txn_id, std::string & data);
    void log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
        std::string & data);
    void log_async_data(uint64_t node_id, uint64_t txn_id, int status,
        std::string & data);
  private:
    cpp_redis::client client;
};

#endif //SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
