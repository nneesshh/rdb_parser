#pragma once

#include "nx_buf.h"
#include "nx_array.h"

#define REDIS_NODE_TYPE_AUX_FIELDS    0xfa
#define REDIS_NODE_TYPE_EXPIRE_MS     0xfc
#define REDIS_NODE_TYPE_EXPIRE_SEC    0xfd
#define REDIS_NODE_TYPE_DB_SELECTOR   0xfe
#define REDIS_NODE_TYPE_EOF           0xff

#define REDIS_NODE_TYPE_STRING        0
#define REDIS_NODE_TYPE_LIST          1
#define REDIS_NODE_TYPE_SET           2
#define REDIS_NODE_TYPE_ZSET          3
#define REDIS_NODE_TYPE_HASH          4

#define REDIS_NODE_TYPE_ZIPMAP        9
#define REDIS_NODE_TYPE_LIST_ZIPLIST  10 
#define REDIS_NODE_TYPE_INTSET        11 
#define REDIS_NODE_TYPE_ZSET_ZIPLIST  12 
#define REDIS_NODE_TYPE_HASH_ZIPLIST  13

#define REDIS_STR_ENC_INT8   0
#define REDIS_STR_ENC_INT16  1
#define REDIS_STR_ENC_INT32  2
#define REDIS_STR_ENC_LZF    3

/* kv */
typedef struct rdb_kv_s        rdb_kv_t;
typedef struct rdb_kv_chain_s  rdb_kv_chain_t;

struct rdb_kv_s {
    nx_str_t                    key;
    nx_str_t                    val;
};

struct rdb_kv_chain_s {
    rdb_kv_t                   *kv;
    rdb_kv_chain_t             *next;
};

/* node */
typedef struct rdb_node_s        rdb_node_t;
typedef struct rdb_node_chain_s  rdb_node_chain_t;

struct rdb_node_s {
    uint8_t                     type;

    uint32_t                    db_selector;
    nx_str_t                    aux_fields;
    uint64_t                    checksum;
    int                         expire;

    nx_str_t                    key;
    nx_str_t                    val;
    rdb_kv_chain_t             *vall;
    rdb_kv_chain_t             *cur_val_ln;
    uint32_t                    size;

};

struct rdb_node_chain_s {
    rdb_node_t                 *node;
    rdb_node_chain_t           *next;
};

/* parser */
typedef struct rdb_node_builder_s  rdb_node_builder_t;
typedef struct rdb_parser_s        rdb_parser_t;

struct rdb_node_builder_s {
    uint8_t                     depth;
    uint8_t                     state;
    uint8_t                     ready;

    /* for string, list, etc. */
    nx_str_t                    tmpkey;
    nx_str_t                    tmpval;
    uint32_t                    store_len;
    uint32_t                    c_len; /* compress len */
    uint32_t                    len;

};

struct rdb_parser_s {
    int                         version;
    uint64_t                    chksum;
    uint64_t                    parsed;

	size_t                      pool_size;
    nx_pool_t                  *pool;

	size_t                      in_b_size;
    nx_buf_t                   *in_b;

    rdb_node_chain_t           *root;
	uint64_t                    available_nodes;
    rdb_node_chain_t           *cur_ln;

    nx_array_t                 *stack_nb;

    uint8_t                     state;
};

#define RDB_NODE_BUILDER_LOOP_BEGIN(_rp, _nb, _depth)  \
    _nb = alloc_node_builder(_rp, _depth);             \
    do

#define RDB_NODE_BUILDER_LOOP_END(_rp, _rc)            \
    while (_rc == NODE_BUILD_AGAIN);                   \
    if (_rc == NODE_BUILD_OVER) {                      \
        pop_node_builder(_rp);                         \
    }

/* EOF */