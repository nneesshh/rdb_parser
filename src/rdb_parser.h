#pragma once

#include "nx_buf.h"

typedef struct rdb_kv_s        rdb_kv_t;
typedef struct rdb_kv_chain_s  rdb_kv_chain_t;

struct rdb_kv_s {
	nx_str_t             key;
	nx_str_t             val;
};

struct rdb_kv_chain_s {
    rdb_kv_t            *kv;
    rdb_kv_chain_t      *next;
};

typedef struct rdb_node_s        rdb_node_t;
typedef struct rdb_node_chain_s  rdb_node_chain_t;

struct rdb_node_s {
    int                  expire;
    uint8_t              type;
    nx_str_t             key;
	nx_str_t             val;
    rdb_kv_chain_t      *vall;
    uint32_t             size;

};

struct rdb_node_chain_s {
    rdb_node_t          *node;
    rdb_node_chain_t    *next;
};


typedef struct rdb_node_handler_s  rdb_node_handler_t;
typedef struct rdb_parser_s        rdb_parser_t;

struct rdb_node_handler_s {
	uint8_t              phase;
	uint8_t              ready;
	uint8_t              state;
	void               (*consume)(nx_buf_t *b);
	nx_buf_t            *tmp_b;
};

struct rdb_parser_s {
    int                  version;
    uint64_t             chksum;
    uint64_t             parsed;

    nx_pool_t           *pool;
	nx_buf_t            *in_b;

	rdb_node_chain_t    *root;
	rdb_node_handler_t  *handler;
};

rdb_kv_chain_t *       alloc_rdb_kv_chain_link(rdb_parser_t *parser, rdb_kv_chain_t **ll);
rdb_node_chain_t *     alloc_rdb_node_chain_link(rdb_parser_t *parser, rdb_node_chain_t **ll);

rdb_parser_t *         create_rdb_parser();
void                   destroy_rdb_parser(rdb_parser_t *parser);

rdb_node_handler_t *   create_rdb_node_handler(rdb_parser_t *parser, uint8_t phase);

size_t                 parse_node_type(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t type);
size_t                 parse_node(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t type);

int                    rdb_parse_file(const char *path);

/* EOF */