#pragma once

#include "global.h"
#include "helper.h"
#include "index_base.h"

#if INDEX_STRUCT == INDEX_BTREE

typedef struct bt_node {
    // TODO bad hack!
       void ** pointers; // for non-leaf nodes, point to bt_nodes
    bool is_leaf;
    uint64_t * keys;
    bt_node * parent;
    uint32_t num_keys;
    bt_node * next;
    bool latch;
    pthread_mutex_t locked;
    latch_t latch_type;
    uint32_t share_cnt;
} bt_node;

struct glob_param {
    uint64_t part_id;
};

class index_btree : public IndexBase {
public:
    RC            init(uint64_t part_cnt);
    RC            init(uint64_t part_cnt, table_t * table);
    bool         index_exist(uint64_t key); // check if the key exist.
    RC             index_insert(uint64_t key, itemid_t * item, int part_id = -1);
    RC             index_read(uint64_t key, itemid_t * &item,
                    uint64_t thd_id, int64_t part_id = -1);
    RC             index_read(uint64_t key, itemid_t * &item, int part_id = -1);
    RC             index_read(uint64_t key, itemid_t * &item);
    RC             index_next(uint64_t thd_id, itemid_t * &item, bool samekey = false);

private:
    // index structures may have part_cnt = 1 or PART_CNT.
    uint64_t part_cnt;
    RC            make_lf(uint64_t part_id, bt_node *& node);
    RC            make_nl(uint64_t part_id, bt_node *& node);
    RC             make_node(uint64_t part_id, bt_node *& node);

    RC             start_new_tree(glob_param params, uint64_t key, itemid_t * item);
    RC             find_leaf(glob_param params, uint64_t key, idx_acc_t access_type, bt_node *& leaf, bt_node  *& last_ex);
    RC             find_leaf(glob_param params, uint64_t key, idx_acc_t access_type, bt_node *& leaf);
    RC            insert_into_leaf(glob_param params, bt_node * leaf, uint64_t key, itemid_t * item);
    // handle split
    RC             split_lf_insert(glob_param params, bt_node * leaf, uint64_t key, itemid_t * item);
    RC             split_nl_insert(glob_param params, bt_node * node, uint32_t left_index, uint64_t key, bt_node * right);
    RC             insert_into_parent(glob_param params, bt_node * left, uint64_t key, bt_node * right);
    RC             insert_into_new_root(glob_param params, bt_node * left, uint64_t key, bt_node * right);

    int            leaf_has_key(bt_node * leaf, uint64_t key);

    uint32_t         cut(uint32_t length);
    uint32_t         order; // # of keys in a node(for both leaf and non-leaf)
    bt_node **     roots; // each partition has a different root
    bt_node *   find_root(uint64_t part_id);

    bool         latch_node(bt_node * node, latch_t latch_type);
    latch_t        release_latch(bt_node * node);
    RC             upgrade_latch(bt_node * node);
    // clean up all the LATCH_EX up tp last_ex
    RC             cleanup(bt_node * node, bt_node * last_ex);

    // the leaf and the idx within the leaf that the thread last accessed.
    bt_node *** cur_leaf_per_thd;
    uint32_t **         cur_idx_per_thd;
};

#endif
