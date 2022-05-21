#pragma once
#include <stdint.h>
class LogRecord
{

public:

    enum Type {
        YES,
        ABORT,
        COMMIT,
        INVALID
    };
    LogRecord()
            : _node_id(0), _txn_id(0), _latest_lsn(0),
              _log_record_type(INVALID) {};

    LogRecord(uint32_t _node_id, uint64_t _txn_id, uint64_t _latest_lsn,
              Type _log_record_type)
            : _node_id(_node_id), _txn_id(_txn_id), _latest_lsn(_latest_lsn),
              _log_record_type(_log_record_type) {};

    LogRecord(uint64_t _txn_id, uint64_t _latest_lsn,
              Type _log_record_type)
            : _txn_id(_txn_id), _latest_lsn(_latest_lsn),
              _log_record_type(_log_record_type) {};
    ~LogRecord() {

    };


    Type get_log_record_type() {
        return this->_log_record_type;
    }
    void set_log_record_type(Type _log_record_type) {
        this->_log_record_type = _log_record_type;
    }


    uint64_t get_latest_lsn
            () {
        return this->_latest_lsn
                ;
    }
    void set_latest_lsn
            (uint64_t _latest_lsn
            ) {
        this->_latest_lsn
                = _latest_lsn
                ;
    }


    uint64_t get_txn_id
            () {
        return this->_txn_id
                ;
    }
    void set_txn_id
            (uint64_t _txn_id
            ) {
        this->_txn_id
                = _txn_id
                ;
    }


    uint32_t get_node_id
            () {
        return this->_node_id
                ;
    }
    void set_node_id
            (uint32_t _node_id
            ) {
        this->_node_id
                = _node_id
                ;
    }


private:
    uint32_t _node_id;  // unused currently
    uint64_t _txn_id;
    uint64_t _latest_lsn;
    Type _log_record_type = INVALID;

};