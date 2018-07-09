#pragma once

#include "rdb_parser_def.h"

rdb_node_builder_chain_t *     alloc_node_builder_chain_link(rdb_parser_t *rp, rdb_node_builder_chain_t **ll);
rdb_kv_chain_t *               alloc_rdb_kv_chain_link(rdb_parser_t *rp, rdb_kv_chain_t **ll);
rdb_node_chain_t *             alloc_rdb_node_chain_link(rdb_parser_t *rp, rdb_node_chain_t **ll);

rdb_parser_t *                 create_rdb_parser(size_t size);
void                           destroy_rdb_parser(rdb_parser_t *rp);

rdb_node_builder_t *           create_node_builder(rdb_parser_t *rp, uint8_t node_type);

int                            next_node_type(rdb_parser_t *rp, nx_buf_t *b, uint8_t *node_type);
size_t                         parse_node(rdb_parser_t *rp, nx_buf_t *b, uint8_t node_type);

int                            rdb_parse_file(const char *path);

/* EOF */