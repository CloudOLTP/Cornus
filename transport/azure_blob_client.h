//
// Created by Kan Wu on 10/6/21.
//

#ifndef SUNDIAL_TRANSPORT_AZURE_BLOB_CLIENT_H_
#define SUNDIAL_TRANSPORT_AZURE_BLOB_CLIENT_H_

#if LOG_DEVICE == LOG_DVC_AZURE_BLOB

#include <was/storage_account.h>
#include <was/blob.h>
#include <cpprest/filestream.h>
#include <cpprest/containerstream.h>
#include <string>

#include "helper.h"

class AzureBlobClient {
  public:
    AzureBlobClient();
    RC log_sync(uint64_t node_id, uint64_t txn_id, int status);
    RC log_async(uint64_t node_id, uint64_t txn_id, int status);
    RC log_if_ne(uint64_t node_id, uint64_t txn_id);
    RC log_if_ne_data(uint64_t node_id, uint64_t txn_id, std::string & data);
    RC log_sync_data(uint64_t node_id, uint64_t txn_id, int status,
        std::string & data);
    RC log_async_data(uint64_t node_id, uint64_t txn_id, int status,
        std::string & data);
  private:
    azure::storage::cloud_storage_account storage_account;
    azure::storage::cloud_blob_client blob_client;
    azure::storage::cloud_blob_container container;
};

#endif

#endif //SUNDIAL_TRANSPORT_AZURE_BLOB_CLIENT_H_
