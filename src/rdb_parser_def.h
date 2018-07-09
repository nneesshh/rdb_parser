#pragma once

#include "nx_buf.h"

#define REDIS_NODE_TYPE_HEADER        0x80

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
    int                         expire;
    uint8_t                     type;
    nx_str_t                    key;
    nx_str_t                    val;
    rdb_kv_chain_t             *vall;
    uint32_t                    size;

};

struct rdb_node_chain_s {
    rdb_node_t                 *node;
    rdb_node_chain_t           *next;
};

/* parser */
typedef struct rdb_node_builder_s        rdb_node_builder_t;
typedef struct rdb_node_builder_chain_s  rdb_node_builder_chain_t;
typedef struct rdb_parser_s              rdb_parser_t;

struct rdb_node_builder_s {
	uint8_t                     phase;
	uint8_t                     type;
	uint8_t                     state;
	uint8_t                     ready;

	int(*build)(rdb_parser_t *parser, nx_buf_t *b);
	nx_buf_t                   *tmp_b;

	size_t                      wants;
	size_t                      gots;
};

struct rdb_node_builder_chain_s {
	rdb_node_builder_t         *nb;
	rdb_node_builder_chain_t   *next;
};

struct rdb_parser_s {
    int                         version;
    uint64_t                    chksum;
    uint64_t                    parsed;

    nx_pool_t                  *pool;
    nx_buf_t                   *in_b;

    rdb_node_chain_t           *root;
	rdb_node_t                 *cur_node;

	rdb_node_builder_chain_t   *nbl_free;
	rdb_node_builder_chain_t   *nbl_busy;
	int                         nbl_nbusy;

	rdb_node_builder_t         *cur_nb;
};

/* EOF */