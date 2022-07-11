#pragma once

#include "cc_manager.h"
#include "rpc_client.h"

class OccManager : public CCManager
{
public:
    OccManager(TxnManager * txn);
    ~OccManager() {}

    RC            get_row(row_t * row, access_t type, uint64_t key);
    RC            get_row(row_t * row, access_t type, char * &data, uint64_t key);
    char *        get_data( uint64_t key, uint32_t table_id);

    RC            index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit=-1);
    RC            index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit=-1);
    RC            index_insert(INDEX * index, uint64_t key);
    RC            index_delete(INDEX * index, uint64_t key);

    void          process_remote_read_response(uint32_t node_id, access_t type, SundialResponse &response);
    void          process_remote_read_response(uint32_t node_id, SundialResponse &response);
    void          build_prepare_req(uint32_t node_id, SundialRequest &request);

    RC            validate();
    RC            commit_insdel();
    void          cleanup(RC rc);

    // Logging
    // Get the log record for a single partition transaction.
    // Return value: size of the log record.
    uint32_t      get_log_record(char *& record);

#if EARLY_LOCK_RELEASE
    void          retire() { assert(false); };
#endif

private:
    struct IndexAccessOcc : IndexAccess {
        uint64_t version;
    };
    class AccessOcc : public Access {
      public:
        ~AccessOcc() {}
        AccessOcc() { data = NULL; data_size = 0; }
        char *        data;    // original data.
        uint32_t      data_size;
        uint64_t      version;
    };

    AccessOcc * find_access(uint64_t key, uint32_t table_id, vector<AccessOcc>
        * set);

    vector<AccessOcc>        _access_set;
    vector<AccessOcc>        _remote_set;

    vector<IndexAccessOcc>       _index_access_set;
};
