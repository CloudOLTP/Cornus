#include <sw/redis++/async_redis.h>
#include <sw/redis++/redis++.h>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <unistd.h>

using namespace sw::redis;

int main() {
    ConnectionOptions co;
    // std::cout << getpid() << std::endl;
    // char c;
    // std::cin >> c;
    co.host = "cornusredis.redis.cache.windows.net";
    co.port = 6380;
    co.password = "Z78qaAZTnwJNXpekISSyLtmLgsaeRDVjiBcRcG4MkGc=";
    co.tls.enabled = true;
    co.tls.cacert = "/etc/ssl/certs/ca-certificates.crt";
    sw::redis::Redis client(co);
    // sw::redis::AsyncRedis client(co);
    auto a = client.get("wuh-chwen");
    sw::redis::OptionalString b = a;
    std::cout << (b ? *b : "NO VALUE") << std::endl;
    
    return 0;
}