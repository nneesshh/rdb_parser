#pragma once

#include "rdb_parser_def.h"

/* node build error code */
#define NODE_BUILD_OVER                        1
#define NODE_BUILD_AGAIN                       0
#define NODE_BUILD_ERROR_INVALID_PATH          -1
#define NODE_BUILD_ERROR_PREMATURE             -2
#define NODE_BUILD_ERROR_INVALID_MAGIC_STRING  -3
#define NODE_BUILD_ERROR_INVALID_NODE_TYPE     -4
#define NODE_BUILD_ERROR_INVALID_STING_ENC     -5
#define NODE_BUILD_ERROR_INVALID_NODE_STATE    -6
#define NODE_BUILD_ERROR_LZF_DECOMPRESS        -7

rdb_node_builder_t * push_node_builder(rdb_parser_t *rp);
void                 pop_node_builder(rdb_parser_t *rp);
rdb_node_builder_t * alloc_node_builder(rdb_parser_t *rp, uint8_t depth);

rdb_node_builder_t * node_builder_at(rdb_parser_t *rp, uint8_t depth);
rdb_node_builder_t * node_builder_stack_top(rdb_parser_t *rp);

/* EOF */