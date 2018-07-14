#pragma once

#include "rdb_parser_def.h"

#define CHECKSUM_VERSION_MIN  5

rdb_kv_chain_t *   alloc_rdb_kv_chain_link(rdb_parser_t *rp, rdb_kv_chain_t **ll);
rdb_node_chain_t * alloc_rdb_node_chain_link(rdb_parser_t *rp, rdb_node_chain_t **ll);

size_t  calc_crc(rdb_parser_t *rp, nx_buf_t *b, size_t bytes);

size_t  read_kv_type(rdb_parser_t *rp, nx_buf_t *b, uint8_t *out);
size_t  read_store_len(rdb_parser_t *rp, nx_buf_t *b, uint8_t *is_encoded, uint32_t *out);
size_t  read_int(rdb_parser_t *rp, nx_buf_t *b, uint8_t enc, int32_t *out);

/* EOF */