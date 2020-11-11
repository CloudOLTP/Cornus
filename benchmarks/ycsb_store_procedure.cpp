#include "ycsb_store_procedure.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "manager.h"
#include "cc_manager.h"
#include "row.h"
#include "table.h"
#include "catalog.h"
#include "index_base.h"
#include "index_hash.h"

#if WORKLOAD == YCSB

YCSBStoreProcedure::YCSBStoreProcedure(TxnManager * txn_man, QueryBase * query)
    : StoreProcedure(txn_man, query)
{
}

YCSBStoreProcedure::~YCSBStoreProcedure()
{
}

RC
YCSBStoreProcedure::execute()
{
    RC rc = RCOK;
    WorkloadYCSB * wl = (WorkloadYCSB *) GET_WORKLOAD;
    INDEX * index = wl->the_index;
    QueryYCSB * query = (QueryYCSB *) _query;
    RequestYCSB * requests = query->get_requests();
    assert(_query);
#if SINGLE_PART_ONLY
    for ( ; _curr_query_id < query->get_request_count(); _curr_query_id ++) {
        RequestYCSB * req = &requests[ _curr_query_id ];
        uint64_t key = req->key;
        access_t type = req->rtype;
    #if NO_LOCK && CC_ALG == WAIT_DIE
        set<row_t *> * rows = index->read(key);
        assert(!rows->empty());
        _curr_row = *rows->begin();
        _curr_data = _curr_row->get_data();
    #else
        GET_DATA( key, index, type);
    #endif
        char * data = _curr_data;

        if (type == RD) {
            for (int fid = 0; fid < 10; fid ++)
                __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 100]);
        } else {
            assert(type == WR);
            for (int fid = 1; fid < 10; fid ++)
                *(uint64_t *)(&data[fid * 100]) = _txn->get_txn_id();
        }
    }
    return COMMIT;
#else
    // Phase 0: figure out whether we need remote queries; if so, send messages.
    // for each request, if it touches a remote node, add it to a remote query.
    // bool has_remote_req = false;
    std::map<uint64_t, int> debug;
    for (uint32_t i = 0; i < query->get_request_count(); i ++) {
        RequestYCSB * req = &requests[i];
        if (debug.find(req->key) == debug.end())
            debug[req->key] = 1;
        else 
            printf("!!!two same key in a query\n");
        uint32_t home_node = GET_WORKLOAD->key_to_node(req->key);
        if (home_node != g_node_id) {
            uint64_t time_begin = get_sys_clock();
            rc = _txn->send_remote_read_request(home_node, req->key, 0, 0, req->rtype);
            INC_FLOAT_STATS(time_debug1, get_sys_clock() - time_begin);
            INC_INT_STATS(int_debug1, 1);
            if (rc == ABORT) return rc;
            // has_remote_req = true;
        }
    }

    // if (has_remote_req)
    //     return LOCAL_MISS;
    // else
    //     remote_requests.clear();

    // Phase 1: grab permission of local accesses.
    // access local rows.
    for ( ; _curr_query_id < query->get_request_count(); _curr_query_id ++) {
        RequestYCSB * req = &requests[ _curr_query_id ];
        uint32_t home_node = GET_WORKLOAD->key_to_node(req->key);
        if (home_node == g_node_id) {
            uint64_t key = req->key;
            access_t type = req->rtype;
            // printf("txn: %ld access local key: %ld node: %u\n", _txn->get_txn_id(), key, g_node_id);
            GET_DATA( key, index, type);
        }
    }
    // if (!remote_requests.empty())
    //     return RCOK;

    // Phase 2: after all data is acquired, finish the rest of the transaction.
        // all the data is here. Do computation and commit.
        for (uint32_t i = 0; i < query->get_request_count(); i ++) {
            RequestYCSB * req = &requests[i];
            char * data = get_cc_manager()->get_data(req->key, 0);

            if (req->rtype == RD) {
                for (int fid = 0; fid < 10; fid ++)
                    __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 100]);
            } else {
                assert(req->rtype == WR);
                for (int fid = 1; fid < 10; fid ++)
                    *(uint64_t *)(&data[fid * 100]) = _txn->get_txn_id();
            }
        }
#endif
    return COMMIT;
}

void
YCSBStoreProcedure::txn_abort()
{
    StoreProcedure::txn_abort();
    _curr_query_id = 0;
    _phase = 0;
}

#endif
