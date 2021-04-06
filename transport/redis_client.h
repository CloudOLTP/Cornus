//
// Created by Zhihan Guo on 4/5/21.
//

#ifndef SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
#define SUNDIAL_TRANSPORT_REDIS_CLIENT_H_

class RedisClient {
  public:
    RedisClient();
    void log_sync(uint64_t node_id, uint64_t txn_id, int status);
    void log_async(uint64_t node_id, uint64_t txn_id, int status);
    void log_if_ne(uint64_t node_id, uint64_t txn_id);
    void log_if_ne_data(uint64_t node_id, uint64_t txn_id, string & data);
    void log_sync_data(uint64_t node_id, uint64_t txn_id, int status, string &
    data);
  private:
    cpp_redis::client client;
};

#endif //SUNDIAL_TRANSPORT_REDIS_CLIENT_H_
