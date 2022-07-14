//
// Created by Zhihan Guo on 7/12/22.
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

RC TxnManager::process_mdcc() {
  // send proposal for all updates
  for (auto it = _remote_nodes_involved.begin(); it != _remote_nodes_involved
                                                           .end(); it ++) {
    if (BALLOT_TYPE == BALLOT_CLASSIC) {
      // send proposal to leader (participant)
      
    } else {
      // send proposal to acceptors (participant + storage nodes)

    }
  }
  return RCOK;
}