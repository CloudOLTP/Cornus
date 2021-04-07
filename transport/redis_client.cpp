//
// Created by Zhihan Guo on 4/5/21.
//
#include <cpp_redis/cpp_redis>

#include "redis_client.h"
#include "txn.h"

using namespace sw::redis;

RedisClient::RedisClient() {
    std::ifstream in(ifconfig_file);
    string line;
    bool checked_marker = false;
    while(get_line(in, line)) {
        if (line.size >= 2 && line[0] == "#" && line[1] == "l") {
            checked_marker = true;
            continue;
        } else if (line[0] == "#") {
            continue;
        }
        if (checked_marker) {
            break;
        }
    }
    assert(line);
    client.connect(host=line.substr(0, line.find(":")),
        port=line.substr(line.find(":") + 1, line.size()),
        timeout_msecs = LOG_TIMEOUT / 1000);
}

void
RedisClient::log_sync(uint64_t node_id, uint64_t txn_id, int status) {
    string key = "data" + to_string(node_id) + "-" + to_string(txn_id);
    client.set(key, to_string(status), callback);
    client.sync_commit()
}

void
RedisClient::log_async(uint64_t node_id, uint64_t txn_id, int status) {
    // TODO: implement callback func
    string key = "data" + to_string(node_id) + "-" + to_string(txn_id);
    client.set(key, to_string(status), callback);
    client.commit()
}

// used for termination protocol, req is always LOG_ABORT
void
RedisClient::log_if_ne(uint64_t node_id, uint64_t txn_id) {
    // TODO: check what if key does not exist
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        local status = tonumber(redis.call(KEYS[1]))
        if (status != ARGV[1]) then return return tonumber(status)
        else redis.call('set', KEYS[1], ARGV[2])
        end
        return tonumber(ARGV[3]))";
    string key = "status" + to_string(node_id) + "-" + to_string(txn_id);
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {to_string(TxnManager::ABORTED),
                                     to_string(TxnManager::ABORTED)};
    client.eval(script, keys, args, callback);
    client.commit();
}

// used for prepare, req is always LOG_YES_REQ
void
RedisClient::log_if_ne_data(uint64_t node_id, uint64_t txn_id, string & data) {
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        local status = tonumber(redis.call(KEYS[2]))
        if (status == ARGV[2]) then return tonumber(status)
        else redis.call('set', KEYS[2], ARGV[3])
        end
        return tonumber(ARGV[3]))";
    string id = to_string(node_id) + "-" + to_string(txn_id);
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data,
                                     to_string(TxnManager::ABORTED),
                                     to_string(TxnManager::PREPARED)};
    client.eval(script, keys, args, callback);
    client.commit();
}

// synchronous
void
RedisClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
    string &
data) {
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        redis.call('set', KEYS[2], ARGV[2])
        )";
    string id = to_string(node_id) + "-" + to_string(txn_id);
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data,
                                     to_string(status)};
    client.eval(script, keys, args, callback);
    client.sync_commit();
}
