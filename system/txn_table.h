#pragma once

#include "global.h"

// For Distributed DBMS

class TxnManager;

// States of all active transactions are maintained in the per-node TxnTable.
class TxnTable
{
public:
    struct Node {
        TxnManager * txn;
        Node * next;
        Node() : txn(nullptr), request(0), next(nullptr) {};
    };

    TxnTable();
    // should support 3 methods: add_txn, get_txn, remove_txn
    Node * add_txn(TxnManager * txn);
    void remove_txn(TxnManager * txn);
    void print_txn();

    TxnManager * get_txn(uint64_t txn_id, bool remove=false);
    void remove_txn(uint64_t txn_id);
    uint32_t get_size();

private:
    struct Bucket {
        Node * first;
        volatile bool latch;
    };

    Bucket ** _buckets;
    uint32_t _txn_table_size;
};
