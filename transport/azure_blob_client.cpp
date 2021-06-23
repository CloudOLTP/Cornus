//
// Created by Kan Wu on 10/6/21.
//

#if LOG_DEVICE == LOG_DVC_AZURE_BLOB
#include <sstream>

#include "azure_blob_client.h"
#include "txn.h"
#include "txn_table.h"
#include "manager.h"


/*
void ab_async_callback(cpp_redis::reply & response);
void ab_ne_callback(cpp_redis::reply & response);
void ab_tp_callback(cpp_redis::reply & response);
void ab_sync_callback(cpp_redis::reply & response);
*/

AzureBlobClient::AzureBlobClient() {
    const utility::string_t storage_connection_string(U("DefaultEndpointsProtocol=https;AccountName=cornuslog;AccountKey=f/Bmf3ADcuEVX2DIQ+esfAGTuhFnYmusfjwIFWK/AvyA8Hi102GApBE5eIvGXill7qGJ6M2JU1bHVZrZkSQ4vw==;EndpointSuffix=core.windows.net"));

    try {
        // Retrieve storage account from connection string.
        storage_account = azure::storage::cloud_storage_account::parse(
                storage_connection_string);

        // Create the blob client.
        blob_client = storage_account.create_cloud_blob_client();

        // Retrieve a reference to a container.
        container = blob_client.get_container_reference(U("cornus-logs"));
    cout << "get here!" << endl;
        // Create the container if it doesn't already exist.
        container.create_if_not_exists();

        azure::storage::cloud_block_blob blob2 = container.get_block_blob_reference(U("test-blob"));
    cout << "get here!" << endl;
        blob2.upload_text(U("more text"));
        //blob2.delete_blob();
    cout << "get here!" << endl;
    }
    catch (const std::exception &e) {
        std::wcout << U("Error: ") << e.what() << std::endl;
    }

    std::cout << "[Sundial] connected to azure blob storage!" << std::endl;
}

/*
void 
ab_sync_callback(cpp_redis::reply & response) {
}

void 
ab_async_callback(cpp_redis::reply & response) {
    assert(response.is_integer());
    TxnManager * txn = txn_table->get_txn(response.as_integer(), false, false);
    // mark as returned. 
    txn->rpc_log_semaphore->decr();
}

void 
ab_ne_callback(cpp_redis::reply & response) {
    assert(response.is_array());
    TxnManager::State state = (TxnManager::State) response.as_array()[0].as_integer();
    TxnManager * txn = txn_table->get_txn(response.as_array()[1].as_integer(), false, false);
    // status can only be aborted/prepared
    if (state == TxnManager::ABORTED)
        txn->set_txn_state(TxnManager::ABORTED);
    // mark as returned. 
    txn->rpc_log_semaphore->decr();
}

// termination protocol callback
void
ab_tp_callback(cpp_redis::reply & response) {
    assert(response.is_array());
    TxnManager::State state = (TxnManager::State) response.as_array()[0].as_integer();
    TxnManager * txn = txn_table->get_txn(response.as_array()[1].as_integer()
        , false, false);
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
}
*/

RC
AzureBlobClient::log_sync(uint64_t node_id, uint64_t txn_id, int status) {
    cout << "get to log_sync!" <<endl;
    if (!glob_manager->active)
        return FAIL;

    // step 1: set pair: ('status-'+node_id+txn_id, status)
    // step 2: return txn_id for callback
    // step 3: ab_sync_callback = NULL
    // step 4: sync_commit

    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob = container.get_block_blob_reference(U("status-" + id));
    blob.upload_text(U(std::to_string(status)));


    /*
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        return tonumber(ARGV[2])
        )";
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {"status-" + id};
    std::vector<std::string> args = {std::to_string(status), std::to_string(txn_id)};
    client.eval(script, keys, args, ab_sync_callback);
    client.sync_commit();
    */
    return RCOK;
}

RC
AzureBlobClient::log_async(uint64_t node_id, uint64_t txn_id, int status) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set pair: ('status-'+node_id+txn_id, status)
    // step 2: return txn_id ??????
    // step 3: ab_async_callback ????? need to update txn_table
    // step 4: sync_commit ??????

    /*
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        return tonumber(ARGV[2])
        )";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"status-" + id};
    std::vector<std::string> args = {std::to_string(status), tid};
    client.eval(script, keys, args, ab_async_callback);
    client.commit();
    */
    return RCOK;
}

// used for termination protocol, req is always LOG_ABORT
RC
AzureBlobClient::log_if_ne(uint64_t node_id, uint64_t txn_id) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set if not exist, pair: ('status-'+node_id+txn_id, ABORTED)
    // step 2: get status = 'status-'+node_id+txn_id
    // step 3: return status, txn_id ??????? what is this status??????
    // step 4: ab_tp_callback ????? set txn state
    // step 5: sync_commit ??????

    /*
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('setnx', KEYS[1], ARGV[1])
        local status = tonumber(redis.call('get', KEYS[1]))
        return {tonumber(status), tonumber(ARGV[2])}
    )";
    string tid = std::to_string(txn_id);
    string key = "status" + std::to_string(node_id) + "-" + std::to_string(txn_id);
    std::vector<std::string> keys = {key};
    std::vector<std::string> args = {std::to_string(TxnManager::ABORTED), tid};
    client.eval(script, keys, args, ab_tp_callback);
    client.commit();
    */
    return RCOK;
}

// used for prepare, req is always LOG_YES_REQ
RC
AzureBlobClient::log_if_ne_data(uint64_t node_id, uint64_t txn_id, string & data) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set, pair: ('data-'+node_id+txn_id, data)
    // step 2: set if not exist, pair: ('status-'+node_id+txn_id, PREPARED)
    // step 2: get status = 'status-'+node_id+txn_id
    // step 3: return status, txn_id ??????? what is this status??????
    // step 4: ab_ne_callback ????? if aborted, set aborted
    // step 5: sync_commit

    /*
    // log format - key-value
    // key: "type(data/status)-node_id-txn_id"
    auto script = R"(
        redis.call('set', KEYS[1], ARGV[1])
        redis.call('setnx', KEYS[2], ARGV[2])
        local status = tonumber(redis.call('get', KEYS[2]))
        return {tonumber(status), tonumber(ARGV[3])};)";
    string tid = std::to_string(txn_id);
    string id = std::to_string(node_id) + "-" + tid;
    std::vector<std::string> keys = {"data-" + id, "status" + id};
    std::vector<std::string> args = {data, std::to_string(TxnManager::PREPARED), tid};
    client.eval(script, keys, args, ab_ne_callback);
    client.commit();
     */
    return RCOK;
}

// synchronous
RC
AzureBlobClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
    string &data) {
    if (!glob_manager->active)
        return FAIL;


    // step 1: set, pair: ('data-'+node_id+txn_id, data)
    // step 2: set  pair: ('status-'+node_id+txn_id, PREPARED)
    // step 3: return txn_id
    // step 4: ab_sync_callback = NULL
    // step 5: sync_commit


    /*
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
    client.eval(script, keys, args, ab_sync_callback);
    client.sync_commit();
     */
    return RCOK;
}

RC
AzureBlobClient::log_async_data(uint64_t node_id, uint64_t txn_id, int status,
                           string & data) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set, pair: ('data-'+node_id+txn_id, data)
    // step 2: set  pair: ('status-'+node_id+txn_id, PREPARED)
    // step 3: return txn_id
    // step 4: ab_sync_callback = NULL
    // step 5: sync_commit


    /*
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
    client.eval(script, keys, args, ab_async_callback);
    client.commit();
     */
    return RCOK;
}
#endif
