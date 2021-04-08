//
// Created by Zhihan Guo on 4/5/21.
//
#include <sstream>

#include "redis_client.h"
#include "txn.h"

void callback(cpp_redis::reply & response);

RedisClient::RedisClient() {
    std::ifstream in(ifconfig_file);
    string line;
    bool checked_marker = false;
    while(getline(in, line)) {
        if (line.size() >= 2 && line[0] == '=' && line[1] == 'l') {
            checked_marker = true;
            continue;
        } else if (line[0] == '#') {
            continue;
        }
        if (checked_marker) {
            break;
        }
    }
    std::cout << "[Sundial] connecting to redis server at " << line << std::endl;
	// host, port, timeout, callback ptr, timeout(ms), max_#retry, retry_interval
	size_t port;
	std::istringstream iss(line.substr(line.find(":") + 1, line.size()));
	iss >> port;
    client.connect(line.substr(0, line.find(":")),
        port,
		nullptr,
		LOG_TIMEOUT,
		0,
		0);
    std::cout << "[Sundial] connected to redis server!" << std::endl;
}


void 
callback(cpp_redis::reply & response) {
	// TODO(zhihan): since termination protocol is not implemented yet
	// current callback only needs to decrease txn's rpc_log_semaphore		
	// TODO(zhihan): need to find out if txn info is in response...
	std::cout << response.as_string() << std::endl;
}

void
RedisClient::log_sync(uint64_t node_id, uint64_t txn_id, int status) {
    string key = "data" + std::to_string(node_id) + "-" + std::to_string(txn_id);
    client.set(key, std::to_string(status), &callback);
    client.sync_commit();
}

void
RedisClient::log_async(uint64_t node_id, uint64_t txn_id, int status) {
    string key = "data" + std::to_string(node_id) + "-" + std::to_string(txn_id);
    client.set(key, std::to_string(status), callback);
    client.commit();
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
    string key = "status" + std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {std::to_string(TxnManager::ABORTED),
                                     std::to_string(TxnManager::ABORTED)};
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
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data,
                                     std::to_string(TxnManager::ABORTED),
                                     std::to_string(TxnManager::PREPARED)};
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
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data,
                                     std::to_string(status)};
    client.eval(script, keys, args, callback);
    client.sync_commit();
}
