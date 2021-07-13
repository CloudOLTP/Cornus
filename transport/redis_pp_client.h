//
// Created by Wuh-Chwen on 7/13/21.
//

#ifndef SUNDIAL_TRANSPORT_REDIS_PP_CLIENT_H_
#define SUNDIAL_TRANSPORT_REDIS_PP_CLIENT_H_
#include <sw/redis++/async_redis.h>
#include "redis_client.h"
#include "helper.h"

class RedisPlusPlusClient : RedisClient {
  public:
    RedisPlusPlusClient();
  private:
    sw::redis::AsyncRedis rppclient;
};

#endif # SUNDIAL_TRANSPORT_REDIS_PP_CLIENT_H_