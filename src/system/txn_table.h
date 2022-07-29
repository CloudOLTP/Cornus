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
        volatile bool valid;
        volatile uint64_t ref;
        Node * next;
        Node() : txn(nullptr), valid(true), ref(0), next(nullptr) {};
    };

    TxnTable();
    // should support 3 methods: add_txn, get_txn, remove_txn
    void add_txn(TxnManager * txn);
    void remove_txn(TxnManager * txn, bool check_ref=true);
    void return_txn(TxnManager * txn);
    void print_txn();

    TxnManager * get_txn(uint64_t txn_id, bool remove=false, bool
    validate=false);
    uint32_t get_size();

private:
    struct Bucket {
        Node * first;
        volatile bool latch;
    };

    Bucket ** _buckets;
    uint32_t _txn_table_size;
};
