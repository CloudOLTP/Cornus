//
// Created by Zhihan Guo on 4/5/21.
//
#include <sstream>

#include "redis_client.h"
#include "txn.h"
#include "txn_table.h"

void async_callback(cpp_redis::reply & response);
void ne_callback(cpp_redis::reply & response);
void sync_callback(cpp_redis::reply & response);

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
    client.connect(line.substr(0, line.find(":")), port, [](const std::string& host, std::size_t port, cpp_redis::connect_state status) {
        if (status == cpp_redis::connect_state::dropped) {
      		std::cout << "[Sundial] client disconnected from " << host << ":" << port << std::endl;
		}
	});
    client.flushall(sync_callback);
    client.sync_commit();
    std::cout << "[Sundial] connected to redis server!" << std::endl;
}


void 
sync_callback(cpp_redis::reply & response) {
}

void 
async_callback(cpp_redis::reply & response) {
    assert(response.is_integer());
    TxnManager * txn = txn_table->get_txn(response.as_integer());
    // mark as returned. 
    txn->rpc_log_semaphore->decr();
}

void 
ne_callback(cpp_redis::reply & response) {
    assert(response.is_array());
    TxnManager::State state = response.as_array()[0].as_integer();
    TxnManager * txn = txn_table->get_txn(response.as_array()[1].as_integer());
    // status can only be aborted/prepared/committed
    if (state == TxnManager::ABORTED)
        txn->set_txn_state(TxnManager::ABORTED);
    // mark as returned. 
    txn->rpc_log_semaphore->decr();
}

void
RedisClient::log_sync(uint64_t node_id, uint64_t txn_id, int status) {
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        return tonumber(ARGV[2])
        )";
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {"data-" + id};
    std::vector<std::string> args = {std::to_string(status), std::to_string(txn_id)};
    client.eval(script, keys, args, sync_callback);
    client.sync_commit();
}

void
RedisClient::log_async(uint64_t node_id, uint64_t txn_id, int status) {
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        return tonumber(ARGV[2])
        )";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"data-" + id};
    std::vector<std::string> args = {std::to_string(status), tid};
    client.eval(script, keys, args, async_callback);
    client.commit();
}

// used for termination protocol, req is always LOG_ABORT
void
RedisClient::log_if_ne(uint64_t node_id, uint64_t txn_id) {
    // TODO: check what if key does not exist
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    // TODO: change return type to match log_if_ne_data
    auto script = R"(
        local status = tonumber(redis.call('get', KEYS[1]))
        if (status != ARGV[1]) then 
		return {status, tonumber(ARGV[2])}
        else 
		redis.call('set', KEYS[1], ARGV[1])
        return {tonumber(ARGV[1]), tonumber(ARGV[2])}
        end
    )";
    string tid = std::to_string(txn_id);
    string key = "status" + std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {std::to_string(TxnManager::ABORTED), tid};
    client.eval(script, keys, args, ne_callback);
    client.commit();
}

// used for prepare, req is always LOG_YES_REQ
void
RedisClient::log_if_ne_data(uint64_t node_id, uint64_t txn_id, string & data) {
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        local status = tonumber(redis.call('get', KEYS[2]))
        if status == tonumber(ARGV[2]) then 
        return {status, tonumber(ARGV[4])}
        else 
		redis.call('set', KEYS[2], ARGV[3]) 
        end
        return {tonumber(ARGV[3]), tonumber(ARGV[4])};)";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data,
                                     std::to_string(TxnManager::ABORTED),
                                     std::to_string(TxnManager::PREPARED), tid};
    client.eval(script, keys, args, ne_callback);
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
        return tonumber(ARGV[3])
    )";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data,
                                     std::to_string(status),
									 tid};
    client.eval(script, keys, args, sync_callback);
    client.sync_commit();
}

void
RedisClient::log_async_data(uint64_t node_id, uint64_t txn_id, int status,
                           string & data) {
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        redis.call('set', KEYS[2], ARGV[2])
        return tonumber(ARGV[3])
    )";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data,
                                     std::to_string(status),
                                     tid};
    client.eval(script, keys, args, async_callback);
    client.commit();
}

