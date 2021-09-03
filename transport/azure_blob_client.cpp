//
// Created by Kan Wu on 10/6/21.
//

#if LOG_DEVICE == LOG_DVC_AZURE_BLOB

#include <sstream>
#include <unistd.h>

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
            U("DefaultEndpointsProtocol=https;AccountName=cornuslog;AccountKey=eyXp2hguWSy9TvS8AGTp9n7O2GjqJIp/5bvT83BO7OWajfLhVmPNUL1qBWYfgj6dBs++aZ0Y0lja6K7vDIj83Q==;EndpointSuffix=core.windows.net"));

    try {
        // Retrieve storage account from connection string.
        storage_account = azure::storage::cloud_storage_account::parse(
                storage_connection_string);

        // Create the blob client.
        blob_client = storage_account.create_cloud_blob_client();

        // Retrieve a reference to a container.
        container = blob_client.get_container_reference(U("cornus-logs"));
        if (g_node_id == 0) {
            container.delete_container_if_exists();
            cout << "just delete the azure container" << endl;
        }
        usleep(100000000);
        cout << "is to create a new container" << endl;
        container.create_if_not_exists();
    }
    catch (const std::exception &e) {
        std::wcout << U("Expected Race Condition [node-") << g_node_id << U("] :")
        << e.what() << std::endl;
    }

    std::cout << "[Sundial] connected to azure blob storage!" << std::endl;
}

RC
AzureBlobClient::log_sync(uint64_t node_id, uint64_t txn_id, int status) {
    if (!glob_manager->active)
        return FAIL;

    uint64_t starttime = get_sys_clock();
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob = container.get_block_blob_reference(U("status-" + id));
    blob.upload_text(U(std::to_string(status)));
    INC_FLOAT_STATS(log_sync, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_sync, 1);
    return RCOK;
}

RC
AzureBlobClient::log_async(uint64_t node_id, uint64_t txn_id, int status) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set pair: ('status-'+node_id+txn_id, status)
    // step 2: ab_async_callback need to update txn_table for txn_id
    uint64_t starttime = get_sys_clock();
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob = container.get_block_blob_reference(U("status-" + id));
    pplx::task<void> upload_task = blob.upload_text_async(U(std::to_string(status)));
    upload_task.then(
            [txn_id, starttime]() -> void {
                // when upload finish, update log_semaphore
                TxnManager *txn = txn_table->get_txn(txn_id, false, false);
                if (txn != NULL) {
                    txn->rpc_log_semaphore->decr();
                }
                INC_FLOAT_STATS(log_async, get_sys_clock() - starttime);
                INC_INT_STATS(num_log_async, 1);
            });

    return RCOK;
}

// used for termination protocol, req is always LOG_ABORT, this function needs to be async
RC
AzureBlobClient::log_if_ne(uint64_t node_id, uint64_t txn_id) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set if not exist, pair: ('status-'+node_id+txn_id, ABORTED)
    // step 2: get status = 'status-'+node_id+txn_id
    // step 3: ab_tp_callback
    uint64_t starttime = get_sys_clock();
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob_status = container.get_block_blob_reference(U("status-" + id));
    azure::storage::access_condition condition = azure::storage::access_condition::generate_if_not_exists_condition();
    azure::storage::blob_request_options options;
    azure::storage::operation_context context;
    TxnManager::State state = TxnManager::ABORTED;

#if COMMIT_ALG == ONE_PC && AZURE_ISOLATION_ENABLE
    // version 1: upload_text_async
    auto t = blob_status.upload_text_async(U(std::to_string(state)), condition, options,
        context).then([blob_status, txn_id, starttime](pplx::task<void> previous_task) {
    	TxnManager::State state = TxnManager::ABORTED;
        try {
            previous_task.get(); // to throw exception if exists
            INC_FLOAT_STATS(log_if_ne_iso, get_sys_clock() - starttime);
            INC_INT_STATS(num_log_if_ne_iso, 1);
        }
        catch (azure::storage::storage_exception& e)
        {
#if !FAILURE_ENABLE
            std::cout << U("[ERROR] [log-if-ne]: ") << e.what() << std::endl;
#endif
            utility::string_t text = blob_status.download_text();
            state = (TxnManager::State) std::stoi(text);
        }

        TxnManager *txn = txn_table->get_txn(txn_id, false, false);
        if (txn != NULL) {
			// default is commit, only need to set abort or committed
			if (state == TxnManager::ABORTED) {
				txn->set_decision(ABORT);
			} else if (state == TxnManager::COMMITTED) {
				txn->set_decision(COMMIT);
			} else if (state != TxnManager::PREPARED) {
				std::cout << "[WARNING] [log-if-ne] unknown state: " << state << std::endl;
				assert(false);
			}
        	// mark as returned.
            txn->rpc_log_semaphore->decr();
		}
    });
#else
    try {
        auto t = blob_status.upload_text_async(U(std::to_string(state))).then
            ([starttime, blob_status, txn_id](pplx::task<void> previous_task) {
            previous_task.get(); // to throw exception if exists
            INC_FLOAT_STATS(log_if_ne, get_sys_clock() - starttime);
            INC_INT_STATS(num_log_if_ne, 1);
			// log abort for others succeed 
            TxnManager *txn = txn_table->get_txn(txn_id, false, false);
            if (txn != NULL) {
                txn->set_decision(ABORT);
                txn->rpc_log_semaphore->decr();
			}
        });
    } catch (const std::exception &e) {
#if !FAILURE_ENABLE
        std::cout << U("[ERROR] [log-if-ne]: ") << e.what() << std::endl;
#endif
        // log already exist
        utility::string_t text = blob_status.download_text();
        // check if text contain data, if so, take the substring
        std::size_t pos = text.find(",");
        if (pos != std::string::npos)
            state = (TxnManager::State) std::stoi(text.substr(0, pos));
        else
            state = (TxnManager::State) std::stoi(text);
        // status can only be aborted/prepared
        TxnManager *txn = txn_table->get_txn(txn_id, false, false);
        if (txn != NULL) {
			// default is commit, only need to set abort or committed
			if (state == TxnManager::ABORTED) {
				txn->set_decision(ABORT);
			} else if (state == TxnManager::COMMITTED) {
				txn->set_decision(COMMIT);
			} else if (state != TxnManager::PREPARED) {
				std::cout << "[WARNING] [log-if-ne] unknown state: " << state << std::endl;
				assert(false);
			}
        	// mark as returned.
            txn->rpc_log_semaphore->decr();
		}
    }
#endif
    return RCOK;
}

// used for prepare, req is always LOG_YES_REQ, this function needs to be async
RC
AzureBlobClient::log_if_ne_data(uint64_t node_id, uint64_t txn_id, string &data) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set, pair: ('data-'+node_id+txn_id, data)
    // step 2: set if not exist, pair: ('status-'+node_id+txn_id, PREPARED)
    // step 3: get status = 'status-'+node_id+txn_id
    // step 4: ab_ne_callback ????? if aborted, set aborted
    uint64_t starttime = get_sys_clock();
#if COMMIT_ALG == ONE_PC && AZURE_ISOLATION_ENABLE
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob_data = container.get_block_blob_reference(U("data-" + id));
    azure::storage::cloud_block_blob blob_status = container.get_block_blob_reference(U("status-" + id));
    // version 1: upload_tex_async
    auto t = blob_data.upload_text_async(U(data)).then([starttime, blob_status,
                                                        txn_id]() {
        TxnManager::State state = TxnManager::PREPARED;
        try {
            azure::storage::access_condition condition = azure::storage::access_condition::generate_if_not_exists_condition();
            azure::storage::blob_request_options options;
            azure::storage::operation_context context;
            blob_status.upload_text(U(std::to_string(TxnManager::PREPARED)), condition, options, context);
            INC_FLOAT_STATS(log_if_ne_data_iso, get_sys_clock() - starttime);
            INC_INT_STATS(num_log_if_ne_data_iso, 1);
        } catch (const std::exception &e) {
#if !FAILURE_ENABLE
            std::cout << U("[ERROR] [log-if-ne-data]: ") << e.what() << std::endl;
            assert(false);
#endif
            utility::string_t text = blob_status.download_text();
            state = (TxnManager::State) std::stoi(text);
        }
        TxnManager *txn = txn_table->get_txn(txn_id, false, false);
        // mark as returned.
        if (txn != NULL) {
        	// status can only be aborted/prepared
        	if (state == TxnManager::ABORTED) {
                txn->set_txn_state(TxnManager::ABORTED);
			} else if (state != TxnManager::PREPARED) {
				std::cout << "[WARNING] [log-if-ne-data] unknown state: " << state << std::endl;
			}
			txn->rpc_log_semaphore->decr();
		}
    });
#else
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob_status = container.get_block_blob_reference(U("status-" + id));
    TxnManager::State state = TxnManager::PREPARED;
    try {
        auto t = blob_status.upload_text_async(U(std::to_string(state) + ","
            + data)).then([starttime, state, blob_status, txn_id](pplx::task<void> previous_task) {
            previous_task.get(); // to throw exception if exists
            INC_FLOAT_STATS(log_if_ne_data, get_sys_clock() - starttime);
            INC_INT_STATS(num_log_if_ne_data, 1);
            TxnManager *txn = txn_table->get_txn(txn_id, false, false);
        	if (txn != NULL) {
        		// status can only be aborted/prepared
        		if (state == TxnManager::ABORTED) {
                	txn->set_txn_state(TxnManager::ABORTED);
				} else if (state != TxnManager::PREPARED) {
					std::cout << "[WARNING] [log-if-ne-data] unknown state: " << state << std::endl;
				}
				txn->rpc_log_semaphore->decr();
			}
        });
    } catch (const std::exception &e) {
#if !FAILURE_ENABLE
        std::cout << U("[ERROR] [log-if-ne-data]: ") << e.what() << std::endl;
#endif
        // log already exist
        utility::string_t text = blob_status.download_text();
        // check if text contain data, if so, take the substring
        std::size_t pos = text.find(",");
        if (pos != std::string::npos)
            state = (TxnManager::State) std::stoi(text.substr(0, pos));
        else
            state = (TxnManager::State) std::stoi(text);
        // status can only be aborted/prepared
        TxnManager *txn = txn_table->get_txn(txn_id, false, false);
        if (txn != NULL) {
        	if (state == TxnManager::ABORTED) {
                txn->set_txn_state(TxnManager::ABORTED);
			} else if (state != TxnManager::PREPARED) {
				std::cout << "[WARNING] [log-if-ne-data] unknown state: " << state << std::endl;
			}
			txn->rpc_log_semaphore->decr();
		}
    }
#endif
    return RCOK;
}

// synchronous
RC
AzureBlobClient::log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
                               string &data) {
    if (!glob_manager->active)
        return FAIL;
    uint64_t starttime = get_sys_clock();
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob_status = container.get_block_blob_reference(U("status-" + id));
#if COMMIT_ALG == ONE_PC && AZURE_ISOLATION_ENABLE
    azure::storage::cloud_block_blob blob_data = container.get_block_blob_reference(U("data-" + id));
    blob_data.upload_text(U(data));
    blob_status.upload_text(U(std::to_string(status)));
    INC_FLOAT_STATS(log_sync_data_iso, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_sync_data_iso, 1);
#else
    blob_status.upload_text(U(std::to_string(status) + "," + data));
    INC_FLOAT_STATS(log_sync_data, get_sys_clock() - starttime);
    INC_INT_STATS(num_log_sync_data, 1);
#endif

    return RCOK;
}

RC
AzureBlobClient::log_async_data(uint64_t node_id, uint64_t txn_id, int status,
                                string &data) {
    if (!glob_manager->active)
        return FAIL;

    // step 1: set, pair: ('data-'+node_id+txn_id, data)
    // step 2: set  pair: ('status-'+node_id+txn_id, PREPARED)
    // step 3: async_callback, update log_semaphore
    uint64_t starttime = get_sys_clock();
    string id = std::to_string(node_id) + "-" + std::to_string(txn_id);
    azure::storage::cloud_block_blob blob_status = container.get_block_blob_reference(U("status-" + id));

#if COMMIT_ALG == ONE_PC && AZURE_ISOLATION_ENABLE
    azure::storage::cloud_block_blob blob_data = container.get_block_blob_reference(U("data-" + id));
    pplx::task<void> upload_task_data = blob_data.upload_text_async(U(data));
    upload_task_data.then(
            [starttime, blob_status, status, txn_id]() -> void {
                blob_status.upload_text(U(std::to_string(status)));
                INC_FLOAT_STATS(log_async_data_iso, get_sys_clock() -
                starttime);
                INC_INT_STATS(num_log_async_data_iso, 1);
                // when upload finish, update log_semaphore
                TxnManager *txn = txn_table->get_txn(txn_id, false, false);
                if (txn != NULL) {
                    txn->rpc_log_semaphore->decr();
                }
            });
#else
    pplx::task<void> upload_task_data = blob_status.upload_text_async(U(std::to_string(status) + "," + data));
    upload_task_data.then(
        [starttime, blob_status, status, txn_id]() -> void {
            blob_status.upload_text(U(std::to_string(status)));
            INC_FLOAT_STATS(log_async_data, get_sys_clock() - starttime);
            INC_INT_STATS(num_log_async_data, 1);
            // when upload finish, update log_semaphore
            TxnManager *txn = txn_table->get_txn(txn_id, false, false);
            if (txn != NULL) {
                txn->rpc_log_semaphore->decr();
            }
        });
#endif
    return RCOK;
}

#endif
