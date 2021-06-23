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
    const utility::string_t storage_connection_string(
            U("DefaultEndpointsProtocol=https;AccountName=cornuslog;AccountKey=f/Bmf3ADcuEVX2DIQ+esfAGTuhFnYmusfjwIFWK/AvyA8Hi102GApBE5eIvGXill7qGJ6M2JU1bHVZrZkSQ4vw==;EndpointSuffix=core.windows.net"));

    try {
        // Retrieve storage account from connection string.
        storage_account = azure::storage::cloud_storage_account::parse(
                storage_connection_string);

        // Create the blob client.
        blob_client = storage_account.create_cloud_blob_client();

        // Retrieve a reference to a container.
        container = blob_client.get_container_reference(U("cornus-logs"));
        // Create the container if it doesn't already exist.
        container.create_if_not_exists();

        azure::storage::cloud_block_blob blob2 = container.get_block_blob_reference(U("test-blob"));
        blob2.upload_text(U("more text"));
        //blob2.delete_blob();
    }
    catch (const std::exception &e) {
        std::wcout << U("Error: ") << e.what() << std::endl;
    }


    // test APIs
    log_sync(0, 1000, 10);
    log_sync(0, 2000, 10);


    log_async(0, 3000, 10);
    log_async(0, 4000, 10);

    string data_1 = "test_data_5000";
    string data_2 = "test_data_6000";
    log_sync_data(0, 5000, 10, data_1);
    log_sync_data(0, 6000, 10, data_2);

    log_sync_data(0, 6000, 10, data_1);
    log_sync_data(0, 7000, 10, data_2);

    std::cout << "[Sundial] connected to azure blob storage!" << std::endl;
}

/*
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
    cout << "get to log_sync!" << endl;
    if (!glob_manager->active)
        return FAIL;

    // step 1: set pair: ('status-'+node_id+txn_id, status)

    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob = container.get_block_blob_reference(U("status-" + id));
    blob.upload_text(U(std::to_string(status)));
    return RCOK;
}

RC
AzureBlobClient::log_async(uint64_t node_id, uint64_t txn_id, int status) {
    cout << "get to log_async!" << endl;
    if (!glob_manager->active)
        return FAIL;

    // step 1: set pair: ('status-'+node_id+txn_id, status)
    // step 2: ab_async_callback need to update txn_table for txn_id

    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob = container.get_block_blob_reference(U("status-" + id));
    pplx::task<void> upload_task = blob.upload_text_async(U(std::to_string(status)));
    upload_task.then(
            [txn_table, txn_id]() -> void {
                // when upload finish, update log_semaphore
                cout << "async upload finished!" << endl;
                TxnManager * txn = txn_table->get_txn(txn_id, false, false);
                cout << (void *)txn_table << " " << (void *) txn << " " << txn_id << endl;
                if (txn != NULL) {
                    txn->rpc_log_semaphore->decr();
                }
            });

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

    /*
    string tid = std::to_string(txn_id);
    string key = "status" + std::to_string(node_id) + "-" + tid;

    azure::storage::cloud_block_blob blob = container.get_block_blob_reference(U(key));
    azure::storage::access_condition condition = azure::storage::access_condition::generate_if_not_exists_condition();
    azure::storage::blob_request_options options;
    azure::storage::operation_context context;
    pplx::task<void> upload_task = blob.upload_text_async(U(std::to_string(TxnManager::ABORTED)), condition, options, context);
    upload_task.then(
            [txn_table, txn_id]() -> void {
                // step 1: check return value; whether uploading succeed
                // step 2: when upload finish, update log_semaphore
                cout << "async upload finished!" << endl;
                TxnManager * txn = txn_table->get_txn(txn_id, false, false);
                cout << (void *)txn_table << " " << (void *) txn << " " << txn_id << endl;
                if (txn != NULL) {
                    txn->rpc_log_semaphore->decr();
                }
                // TODO ab_tp_callback

            });
    */

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
AzureBlobClient::log_if_ne_data(uint64_t node_id, uint64_t txn_id, string &data) {
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
    // cout << "get to log_sync_data!" << endl;
    if (!glob_manager->active)
        return FAIL;

    // step 1: set, pair: ('data-'+node_id+txn_id, data)
    // step 2: set  pair: ('status-'+node_id+txn_id, PREPARED)

    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob_data = container.get_block_blob_reference(U("data-" + id));
    azure::storage::cloud_block_blob blob_status = container.get_block_blob_reference(U("status-" + id));

    blob_data.upload_text(U(data));
    blob_status.upload_text(U(std::to_string(status)));

    return RCOK;
}

RC
AzureBlobClient::log_async_data(uint64_t node_id, uint64_t txn_id, int status,
                                string &data) {
    cout << "get to log_sync_data!" << endl;
    if (!glob_manager->active)
        return FAIL;

    // step 1: set, pair: ('data-'+node_id+txn_id, data)
    // step 2: set  pair: ('status-'+node_id+txn_id, PREPARED)
    // step 3: async_callback, update log_semaphore

    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob_data = container.get_block_blob_reference(U("data-" + id));
    azure::storage::cloud_block_blob blob_status = container.get_block_blob_reference(U("status-" + id));

    pplx::task<void> upload_task_data = blob_data.upload_text_async(U(data));
    upload_task_data.then(
            [txn_table, txn_id]() -> void {
                pplx::task<void> upload_task_status = blob_status.upload_text_async(U(std::to_string(status)));
                upload_task_status.then(
                        [txn_table, txn_id]() -> void {
                            // when upload finish, update log_semaphore
                            cout << "async log data and status upload finished!" << endl;
                            TxnManager * txn = txn_table->get_txn(txn_id, false, false);
                            cout << (void *)txn_table << " " << (void *) txn << " " << txn_id << endl;
                            if (txn != NULL) {
                                txn->rpc_log_semaphore->decr();
                            }
                        });
            });

    return RCOK;
}

#endif
