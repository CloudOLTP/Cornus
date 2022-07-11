//
// Created by Zhihan Guo on 7/10/22.
//

#include "txn.h"
#include "row.h"
#include "workload.h"
#include "ycsb.h"
#include "worker_thread.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "helper.h"
#include "manager.h"
#include "query.h"
#include "txn_table.h"
#include "cc_manager.h"
#include "store_procedure.h"
#include "ycsb_store_procedure.h"
#include "tpcc_store_procedure.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"

#include "tictoc_manager.h"
#include "lock_manager.h"
#include "f1_manager.h"
#include "occ_manager.h"
#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
#include "row_lock.h"
#endif
#include "log.h"
#include "redis_client.h"
#include "azure_blob_client.h"

RC
TxnManager::mdcc_phase1b(const SundialRequest* request, SundialResponse*
response) {
    if (mbal_a <= request->MdccData().ballot()) {
        mbal_a = request->MdccData().ballot();
        // send Phase1b[m, bal_a, val_a] to leader by setting up response
        SundialResponse::MdccData msg;
        reponse->set_mdcc_data(msg);
    }

}