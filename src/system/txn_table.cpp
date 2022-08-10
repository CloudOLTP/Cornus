#include "txn_table.h"
#include "txn.h"
#include "manager.h"
#include "ycsb_query.h"
#include "query.h"
#include "ycsb_store_procedure.h"

#define DEBUG_TXN_TABLE false

TxnTable::TxnTable()
{
    _txn_table_size = g_num_nodes * g_num_worker_threads;
    _buckets = new Bucket * [_txn_table_size];
    for (uint32_t i = 0; i < _txn_table_size; i++) {
        _buckets[i] = (Bucket *) _mm_malloc(sizeof(Bucket), 64);
        _buckets[i]->first = NULL;
        _buckets[i]->latch = false;
    }
}

void
TxnTable::add_txn(TxnManager * txn)
{
    assert(get_txn(txn->get_txn_id()) == NULL);

    uint32_t bucket_id = txn->get_txn_id() % _txn_table_size;
    Node * node = new Node; // *) _mm_malloc(sizeof(Node), 64);
    node->txn = txn;
      while ( !ATOM_CAS(_buckets[bucket_id]->latch, false, true) )
        PAUSE
    COMPILER_BARRIER
    // insert to front
    node->next = _buckets[bucket_id]->first;
    _buckets[bucket_id]->first = node;
    COMPILER_BARRIER
    _buckets[bucket_id]->latch = false;
}

TxnManager *
TxnTable::get_txn(uint64_t txn_id, bool remove, bool validate)
{
    uint32_t bucket_id = txn_id % _txn_table_size;
    Node * node;
    while ( !ATOM_CAS(_buckets[bucket_id]->latch, false, true) )
        PAUSE
    COMPILER_BARRIER
    node = _buckets[bucket_id]->first;
    while (node && node->txn->get_txn_id() != txn_id) {
        node = node->next;
    }
    TxnManager * txn = nullptr;
    if (node) {
        if (node->valid || !validate) {
            if (validate && remove)
                node->valid = false;
            // increment reference count for safety
            // no need to be atomic since protected by latch.
            txn = node->txn;
        }
    }
    COMPILER_BARRIER
    _buckets[bucket_id]->latch = false;
    return txn;
}

void
TxnTable::print_txn()
{
    char buffer[4000];
    sprintf(buffer, "[%u. %lu] Debug Info:\n", g_node_id, glob_manager->get_thd_id());
    // we don't acquire locks
    for(uint32_t i=0; i<_txn_table_size; ++i)
    {
        Node * node = _buckets[i]->first;
        while ( node )
        {
#if WORKLOAD == YCSB
            TxnManager * txn = node->txn;
            try{
                StoreProcedure * sp = txn->get_store_procedure();
                QueryBase * qb = sp->get_query();
                QueryYCSB * ycsbq = (QueryYCSB *)qb;
                sprintf(buffer, "  [Txn%lu]", txn->get_txn_id());
                RequestYCSB * reqs = ycsbq->get_requests();
                for(uint32_t j=0; j<g_req_per_query; ++j)
                {
                    RequestYCSB * req = &reqs[j];
                    switch(req->rtype)
                    {
                        case RD:
                            sprintf(buffer, " Read(%lu)=%u", req->key, req->value);
                            break;
                        case WR:
                            sprintf(buffer, " Write(%lu)=%u", req->key, req->value);
                            break;
                        default:
                            sprintf(buffer, " Unknown(%lu)=%u", req->key, req->value);
                    }
                }
            }
            catch(...){
                // Usually memory access error. We just do nothing.
            }
            sprintf(buffer, "\n");
#endif
            node = node->next;
        }
    }
}

void
TxnTable::remove_txn(TxnManager * txn)
{
    uint32_t bucket_id = txn->get_txn_id() % _txn_table_size;
    Node * node = nullptr;
    Node * rm_node = nullptr;
    while ( !ATOM_CAS(_buckets[bucket_id]->latch, false, true) )
        PAUSE
    COMPILER_BARRIER
    node = _buckets[bucket_id]->first;
    assert(node);
    // the first node matches
    if (node && node->txn == txn) {
        rm_node = node;
        _buckets[bucket_id]->first = node->next;
    } else {
        while (node->next && node->next->txn != txn)
            node = node->next;
        assert(node->next);
        rm_node = node->next;
        node->next = node->next->next;
    }
    COMPILER_BARRIER
    _buckets[bucket_id]->latch = false;
    assert(rm_node);
    delete rm_node;
}

uint32_t
TxnTable::get_size()
{
    uint32_t size = 0;
    for (uint32_t i = 0; i < _txn_table_size; i++)
    {
        while ( !ATOM_CAS(_buckets[i]->latch, false, true) )
            PAUSE
        COMPILER_BARRIER

        Node * node = _buckets[i]->first;
        while (node) {
            size ++;
            cout << i << ":"
                 << node->txn->get_txn_id() << "\t"
                 << node->txn->get_txn_state()
                 << endl;
            node = node->next;
        }

        COMPILER_BARRIER
        _buckets[i]->latch = false;
    }
    return size;
}
