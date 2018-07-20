#pragma once

#include "../bip_buf.h"
#include "../nx_buf.h"
#include "../nx_array.h"

/* REDIS NODE TYPE */
#define REDIS_AUX_FIELDS     0xfa
#define REDIS_EXPIRETIME_MS  0xfc
#define REDIS_EXPIRETIME     0xfd
#define REDIS_SELECTDB       0xfe
#define REDIS_EOF            0xff

#define REDIS_STRING         0
#define REDIS_LIST           1
#define REDIS_SET            2
#define REDIS_ZSET           3
#define REDIS_HASH           4

#define REDIS_HASH_ZIPMAP    9
#define REDIS_LIST_ZIPLIST   10 
#define REDIS_SET_INTSET     11 
#define REDIS_ZSET_ZIPLIST   12 
#define REDIS_HASH_ZIPLIST   13

#define RDB_STR_ENC_INT8   0
#define RDB_STR_ENC_INT16  1
#define RDB_STR_ENC_INT32  2
#define RDB_STR_ENC_LZF    3

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
    rdb_kv_chain_t             *vall_tail;
    size_t                      size;

};

struct rdb_node_chain_s {
    rdb_node_t                 *elem;
    rdb_node_chain_t           *next;
};

/* parser */
typedef struct rdb_node_builder_s  rdb_node_builder_t;
typedef struct rdb_parser_s        rdb_parser_t;

typedef int(*func_process_rdb_node)(rdb_node_t *rn, void *payload);

struct rdb_node_builder_s {
    uint8_t                     depth;
    uint8_t                     state;

    /* for string, list, etc. */
    uint32_t                    store_len;
    uint32_t                    c_len; /* compress len */
    uint32_t                    len;

    nx_str_t                    tmp_key;
    nx_str_t                    tmp_val;

};

struct rdb_parser_s {
    int                         version;
    uint64_t                    chksum;
    uint64_t                    parsed;
    uint8_t                     state;

    bip_buf_t                  *in_bb;
	nx_pool_t                  *pool;

	nx_array_t                 *stack_nb;

	rdb_node_t                 *n;

	nx_pool_t                  *n_pool;
	func_process_rdb_node       n_cb;
	void                       *n_payload;
};

#define rdb_node_init(_n)	 nx_memzero(_n, sizeof(rdb_node_t))
#define rdb_node_clear(_rp)  rdb_node_init(_rp->n);nx_reset_pool(_rp->n_pool)
//#define rdb_node_clear(_rp)  rdb_node_init(_rp->n);nx_destroy_pool(_rp->n_pool);_rp->n_pool=nx_create_pool(4096)

/* EOF */