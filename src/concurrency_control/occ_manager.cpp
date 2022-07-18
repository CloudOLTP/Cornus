#include <functional>

#include "occ_manager.h"
#include "manager.h"
#include "txn.h"
#include "row_occ.h"
#include "row.h"
#include "index_base.h"
#include "table.h"
#include "index_hash.h"
#include "query.h"
#include "workload.h"
#include "store_procedure.h"
#include "txn_table.h"
#include "packetize.h"

#if CC_ALG == OCC

OccManager::OccManager(TxnManager * txn)
    : CCManager(txn)
{
#if WORKLOAD == TPCC
    _access_set.reserve(128);
#endif
}

RC
OccManager::get_row(row_t * row, access_t type, uint64_t key)
{
    RC rc = RCOK;
    assert(type == RD || type == WR);
    if (type == WR) _txn->set_read_only(false);
    // check if have already accessed the row
    for (auto & it : _access_set) {
        if (it.row == row) {
            // TODO: support scenario where we already grab some lock
            assert(false);
        }
    }
    uint64_t version;
    // get a read version
    if (row->manager->get_version_if_unlocked(version) == ABORT) {
        return ABORT;
    }
    // create a read copy
    AccessOcc ac;
    _access_set.push_back( ac );
    AccessOcc * access = &(*_access_set.rbegin());
    access->key = key;
    access->home_node_id = g_node_id;
    access->table_id = row->get_table()->get_table_id();
    access->type = type;
    access->row = row;
    access->version = version;
    access->data = new char [row->get_tuple_size()];
    access->data_size = row->get_tuple_size();
    memcpy(access->data, access->row->get_data(), access->row->get_tuple_size());
    return rc;
}

RC
OccManager::get_row(row_t * row, access_t type, char * &data, uint64_t key)
{
    RC rc = get_row(row, type, key);
    if (rc == RCOK) {
        data = _access_set.rbegin()->data;
        assert(data);
    }
    return rc;
}

char *
OccManager::get_data(uint64_t key, uint32_t table_id)
{
    for (auto & it : _access_set)
        if (it.key == key && it.table_id == table_id)
            return it.data;

    for (auto & it : _remote_set)
        if (it.key == key && it.table_id == table_id)
            return it.data;

    // data not found
    assert(false);
    return nullptr;
}

RC
OccManager::index_get_permission(access_t type, INDEX * index, uint64_t key, uint32_t limit)
{
    RC rc = RCOK;
    assert(type == RD || type == INS || type == DEL);
    if (type == INS || type == DEL) _txn->set_read_only(false);

    // if already accessed
    for (uint32_t i = 0; i < _index_access_set.size(); i++) {
        auto ac = &_index_access_set[i];
        if ( ac->index == index && ac->key == key )  {
            if (ac->type == type) {
                ac->rows = index->read(key);
                return RCOK;
            } else {
                assert( (ac->type == RD)
                            && (type == INS || type == DEL) );
                // upgrade lock from rd to ins/del
                // update access type to insert or delete
                ac->type = type;
                return rc;
            }
        }
    }

    // if first-time access
    ROW_MAN * manager = index->index_get_manager(key);
    uint64_t version;
    if (manager->get_version_if_unlocked(version) == ABORT) {
        manager->unlatch();
        return ABORT;
    }
    IndexAccessOcc access;
    if (type == RD)
        access.rows = index->read(key);
    // latch was taken by the manager
    // to protect the atomicity of read / insert / delete index
    manager->unlatch();

    // NOTE
    // records with different keys on the same index may share the same manager.
    // This is because manager locates in each bucket of the hash index.
    // Records with different keys may map to the same bucket and therefore share the manager.
    access.key = key;
    access.index = index;
    access.type = type;
    access.manager = manager;
    access.version = version;

    _index_access_set.push_back(access);
    return rc;
}

RC
OccManager::index_read(INDEX * index, uint64_t key, set<row_t *> * &rows, uint32_t limit)
{
    RC rc = RCOK;
    rc = index_get_permission(RD, index, key, limit);
    if (rc == RCOK) {
        assert(_index_access_set.rbegin()->key == key);
        rows = _index_access_set.rbegin()->rows;
    }
    return rc;
}

RC
OccManager::index_insert(INDEX * index, uint64_t key)
{
    return index_get_permission(INS, index, key);
}

RC
OccManager::index_delete(INDEX * index, uint64_t key)
{
    return index_get_permission(DEL, index, key);
}

uint32_t
OccManager::get_log_record(char *& record)
{
    // TODO inserted rows should also be in the log record.
    // Log Record Format
    //   | num_tuples | (table_id, key, size, data) * num_tuples
    UnstructuredBuffer buffer;
    for (auto access : _access_set) {
        access_t type = access.type;
        if (type == WR) {
            buffer.put( &access.table_id );
            buffer.put( &access.key );
            buffer.put( &access.data_size );
            buffer.put( access.row->get_data(), access.data_size );
        }
    }
    uint32_t size = buffer.size();
    record = new char [size];
    memcpy(record, buffer.data(), size);
    return size;
}

RC
OccManager::validate() {
    RC rc = COMMIT;
    // lock write set in order
    // 1. sort access set by (table id, key id)
    sort(_access_set.begin(), _access_set.end(),
         [](const AccessOcc & a, const AccessOcc & b) -> bool
         {
             if (a.table_id > b.table_id) {
                 return true;
             } else {
                 return a.key > b.key;
             }
         });
    // 2. lock write set
    for (const auto& access : _access_set) {
        if (access.type == WR) {
            rc = access.row->manager->lock_get(_txn);
            if (rc == ABORT)
                return ABORT;
#if ISOLATION_LEVEL == READ_COMMITTED
            // check if read version has changed
            uint64_t version = access.row->manager->get_version();
            if (access.version != version - 1) {
              // assume already locked by self; if locked by other, abort
              return ABORT;
            }
#endif
        }
    }
    // 4. check if data has changed in read set
    // assume all the writes are in read sets as well
#if ISOLATION_LEVEL == SERIALIZABLE
    assert(false);
    for (const auto& access : _access_set) {
        uint64_t version = access.row->manager->get_version();
        access.row->manager->get_version_if_unlocked(version);
        if (access.type == WR) {
            if (access.version != version - 1) {
                // assume already locked by self; if locked by other, abort
                return ABORT;
            }
        } else {
            if (access.version != version) {
                // locked or different version
                return ABORT;
            }
        }
    }
    // 5. use txn id as commit version and txn id does not imply any order.
    // it only works as unique identifier
    // after commit, for each write in write set, copy the value and assign
    // it the new version (= this txn.id)
    if (rc == ABORT) return ABORT;
#endif
    return COMMIT;
}


/*
 * commit insert / delete
 */
RC
OccManager::commit_insdel()
{
    // TODO. Ignoring index consistency.
    // handle inserts
    for (auto ins : _inserts) {
        row_t * row = ins.row;
        set<INDEX *> indexes;
        ins.table->get_indexes( &indexes );
        for (auto idx : indexes) {
            uint64_t key = row->get_index_key(idx);
            idx->insert(key, row);
        }
    }
    // handle deletes
    for (auto row : _deletes) {
        set<INDEX *> indexes;
        row->get_table()->get_indexes( &indexes );
        for (auto idx : indexes)
            idx->remove( row );
        for (auto it = _access_set.begin();
             it != _access_set.end(); it ++)
        {
            if (it->row == row) {
                _access_set.erase(it);
                break;
            }
        }
    }
    return RCOK;
}

void
OccManager::cleanup(RC rc)
{
    assert(rc == COMMIT || rc == ABORT);
    if (rc == COMMIT) {
        commit_insdel();
        for (const auto& access : _access_set) {
            // copy data and change the version
            if (access.type == WR) {
                access.row->copy(access.data);
                access.row->manager->set_version(_txn->get_txn_id() << 1);
                access.row->manager->lock_release(_txn, rc);
            }
        }
    } else { // rc = ABORT
        for (const auto& access : _access_set) {
            // copy data and change the version
            if (access.type == WR) {
                access.row->manager->lock_release(_txn, rc);
            }
        }
    }
    for (const auto& access : _access_set) {
        assert(access.data);
        delete [] access.data;
    }
    for (const auto& access : _remote_set) {
        assert(access.data);
        delete [] access.data;
    }
    if (rc == ABORT)
        for (auto ins : _inserts)
            delete ins.row;
    _access_set.clear();
    _remote_set.clear();
    _inserts.clear();
    _deletes.clear();
    _index_access_set.clear();
}

// Distributed transactions
// ========================
void
OccManager::process_remote_read_response(uint32_t node_id, access_t type, SundialResponse &response)
{
    assert(response.response_type() == SundialResponse::RESP_OK);
    for (int i = 0; i < response.tuple_data_size(); i ++) {
        AccessOcc ac;
        _remote_set.push_back(ac);
        AccessOcc * access = &(*_remote_set.rbegin());
        assert(node_id != g_node_id);

        access->home_node_id = node_id;
        access->row = NULL;
        access->key = response.tuple_data(i).key();
        access->table_id = response.tuple_data(i).table_id();
        access->type = type;
        access->data_size = response.tuple_data(i).size();
        access->data = new char [access->data_size];
        access->type =
            static_cast<access_t>(response.tuple_data(i).access_type());
        access->version = response.tuple_data(i).version();
        memcpy(access->data, response.tuple_data(i).data().c_str(), access->data_size);
    }
}

void
OccManager::process_remote_read_response(uint32_t node_id, SundialResponse &response)
{
    assert(response.response_type() == SundialResponse::RESP_OK);
    for (int i = 0; i < response.tuple_data_size(); i ++) {
        AccessOcc ac;
        _remote_set.push_back(ac);
        auto access = &(*_remote_set.rbegin());
        assert(node_id != g_node_id);

        access->home_node_id = node_id;
        access->row = NULL;
        access->key = response.tuple_data(i).key();
        access->table_id = response.tuple_data(i).table_id();
        access->type = (access_t) response.tuple_data(i).access_type();
        access->data_size = response.tuple_data(i).size();
        access->data = new char [access->data_size];
        access->type =
            static_cast<access_t>(response.tuple_data(i).access_type());
        access->version = response.tuple_data(i).version();
        memcpy(access->data, response.tuple_data(i).data().c_str(), access->data_size);
    }
}

void
OccManager::build_prepare_req(uint32_t node_id, SundialRequest &request)
{
    for (auto access : _remote_set) {
        if (access.home_node_id == node_id) {
            SundialRequest::TupleData * tuple = request.add_tuple_data();
            uint64_t tuple_size = access.data_size;
            tuple->set_key(access.key);
            tuple->set_table_id( access.table_id );
            tuple->set_size( tuple_size );
            tuple->set_data( access.data, tuple_size );
            tuple->set_access_type(access.type);
            tuple->set_version(access.version);
            SundialRequest::MdccData * mdcc_data = request.add_mdcc_data();
            mdcc_data->set_ballot(0);
        }
    }
}

void
OccManager::build_local_req(SundialRequest &request) {
    for (auto access : _access_set) {
        SundialRequest::TupleData * tuple = request.add_tuple_data();
        uint64_t tuple_size = access.data_size;
        tuple->set_key(access.key);
        tuple->set_table_id( access.table_id );
        tuple->set_size( tuple_size );
        tuple->set_data( access.data, tuple_size );
        tuple->set_access_type(access.type);
        tuple->set_version(access.version);
        SundialRequest::MdccData * mdcc_data = request.add_mdcc_data();
        mdcc_data->set_ballot(0);
    }
}

#endif
