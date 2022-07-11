//
// Created by Zhihan Guo on 4/5/21.
//
#if LOG_DEVICE == LOG_DVC_REDIS
#include <sstream>
#include <cstdlib>

#include "redis_client.h"
#include "txn.h"
#include "txn_table.h"
#include "manager.h"

void async_callback(cpp_redis::reply & response);
void ne_callback(cpp_redis::reply & response);
void tp_callback(cpp_redis::reply & response);
void sync_callback(cpp_redis::reply & response);

RedisClient::RedisClient() {
    tls = LOG_TLS_REDIS;
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
    std::cout << "[Sundial] connecting to redis server at " << line.substr(0, line.find(" ")) << std::endl;
	// host, port, timeout, callback ptr, timeout(ms), max_#retry, retry_interval
	size_t port;
	std::istringstream iss(line.substr(line.find(":") + 1, line.size()));
	iss >> port;
    client.connect(line.substr(0, line.find(":")), port, [](const std::string& host, std::size_t port, cpp_redis::connect_state status) {
        if (status == cpp_redis::connect_state::dropped) {
      		std::cout << "[Sundial] client disconnected from " << host << ":" << port << std::endl;
		}
	});
    if (iss.eof() == false) { // an auth string is following
        string auth;
        iss >> auth;
        client.auth(auth, [](cpp_redis::reply & response){
            std::cout << "[Sundial] Auth response: " << response.as_string() << std::endl;
        });
    }
    client.flushall(sync_callback);
    client.sync_commit();
    std::cout << "[Sundial] connected to redis server!" << std::endl;
}


void 
sync_callback(cpp_redis::reply & response) {
}

void 
async_callback(cpp_redis::reply & response) {
    assert(response.is_array());
    TxnManager * txn = txn_table->get_txn(response.as_array()[0].as_integer(),
        false, false);
    uint64_t starttime = response.as_array()[1].as_integer();
    // mark as returned. 
    txn->rpc_log_semaphore->decr();
    INC_FLOAT_STATS(log_async, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_async, 1);
}

void 
ne_callback(cpp_redis::reply & response) {
    assert(response.is_array());
    TxnManager::State state = (TxnManager::State) response.as_array()[0].as_integer();
    uint64_t txnid = response.as_array()[1].as_integer();
    uint64_t starttime = response.as_array()[2].as_integer();
    TxnManager * txn = txn_table->get_txn(txnid, false, false);
    
    // status can only be aborted/prepared
    if (state == TxnManager::ABORTED)
        txn->set_txn_state(TxnManager::ABORTED);
    // mark as returned. 
    txn->rpc_log_semaphore->decr();
    INC_FLOAT_STATS(log_if_ne_data, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_if_ne_data, 1);
}

// termination protocol callback
void
tp_callback(cpp_redis::reply & response) {
    assert(response.is_array());
    TxnManager::State state = (TxnManager::State) response.as_array()[0].as_integer();
    TxnManager * txn = txn_table->get_txn(response.as_array()[1].as_integer()
        , false, false);
    uint64_t starttime = response.as_array()[2].as_integer();
    // default is commit, only need to set abort or committed
    if (state == TxnManager::ABORTED) {
        txn->set_decision(ABORT);
    } else if (state == TxnManager::COMMITTED) {
        txn->set_decision(COMMIT);
    } else if (state != TxnManager::PREPARED) {
		assert(false);
	}
    // mark as returned.
    txn->rpc_log_semaphore->decr();
    INC_FLOAT_STATS(log_if_ne, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_if_ne, 1);
}

RC
RedisClient::log_sync(uint64_t node_id, uint64_t txn_id, int status) {
    if (!glob_manager->active)
        return FAIL;
    uint64_t starttime = get_sys_clock();
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        return tonumber(ARGV[2])
        )";
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {"status-" + id};
    std::vector<std::string> args = {std::to_string(status),
                                     std::to_string(txn_id)};
    client.eval(script, keys, args, sync_callback);
    client.sync_commit();
    INC_FLOAT_STATS(log_sync, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_sync, 1);
    return RCOK;
}

RC
RedisClient::log_async(uint64_t node_id, uint64_t txn_id, int status) {
    if (!glob_manager->active)
        return FAIL;
    uint64_t starttime = get_sys_clock();
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        return {tonumber(ARGV[2]), tonumber(ARGV[3])}
        )";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"status-" + id};
    std::vector<std::string> args = {std::to_string(status), tid,
                                     std::to_string(starttime)};
    client.eval(script, keys, args, async_callback);
    client.commit();
    return RCOK;
}

// used for termination protocol, req is always LOG_ABORT
RC
RedisClient::log_if_ne(uint64_t node_id, uint64_t txn_id) {
    if (!glob_manager->active)
        return FAIL;
    uint64_t starttime = get_sys_clock();
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('setnx', KEYS[1], ARGV[1])
        local status = tonumber(redis.call('get', KEYS[1]))
        return {tonumber(status), tonumber(ARGV[2]), tonumber(ARGV[3])}
    )";
    string tid = std::to_string(txn_id);
    string key = "status" + std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {std::to_string(TxnManager::ABORTED),
                                     tid, std::to_string(starttime)};
    client.eval(script, keys, args, tp_callback);
    client.commit();
    return RCOK;
}

// used for prepare, req is always LOG_YES_REQ
RC
RedisClient::log_if_ne_data(uint64_t node_id, uint64_t txn_id, string & data) {
    if (!glob_manager->active)
        return FAIL;
    uint64_t starttime = get_sys_clock();
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        redis.call('setnx', KEYS[2], ARGV[2])
        local status = tonumber(redis.call('get', KEYS[2]))
        return {tonumber(status), tonumber(ARGV[3]), tonumber(ARGV[4])};)";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data, std::to_string(TxnManager::PREPARED),
                                     tid, std::to_string(starttime)};
    client.eval(script, keys, args, ne_callback);
    client.commit();
    return RCOK;
}

// synchronous
RC
RedisClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
    string &data) {
    if (!glob_manager->active)
        return FAIL;
    uint64_t starttime = get_sys_clock();
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
    INC_FLOAT_STATS(log_sync_data, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_sync_data, 1);
    return RCOK;
}

RC
RedisClient::log_async_data(uint64_t node_id, uint64_t txn_id, int status,
                           string & data) {
    if (!glob_manager->active)
        return FAIL;
    uint64_t starttime = get_sys_clock();
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        redis.call('set', KEYS[2], ARGV[2])
        return {tonumber(ARGV[3]), tonumber(ARGV[4])}
    )";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data, std::to_string(status),
                                     tid, std::to_string(starttime)};
    client.eval(script, keys, args, async_callback);
    client.commit();
    return RCOK;
}
#endif
