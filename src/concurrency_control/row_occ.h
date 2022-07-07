#pragma once

#include <set>
#include <queue>
#include <atomic>
#include "global.h"

class TxnManager;
class CCManager;
class LockManager;
class row_t;

class Row_occ {
public:
    Row_occ();
    Row_occ(row_t * row);
    virtual         ~Row_occ() = default;
    virtual void    init(row_t * row);
    std::uint64_t   get_version() {return _version.load
    (std::memory_order_relaxed); };
    void            set_version(uint64_t v) {_version.store
    (v, std::memory_order_release); };
    RC              get_version_if_unlocked(uint64_t &version);
    RC              lock_get(TxnManager * txn);
    RC              lock_release(TxnManager * txn, RC rc);
    inline static bool is_locked(uint64_t tid) { return tid & 1; };
    void            latch() {};
    void            unlatch() {};

protected:
    #define LOCK_MAN(txn) ((OccManager *) (txn)->get_cc_manager())
    row_t *           _row;
    // TID (64 bit):
    std::atomic<std::uint64_t> _version;
};
