#pragma once

#include "rdb_parser_def.h"

/* node-builder error code */
#define NB_OVER                        1
#define NB_AGAIN                       0
#define NB_ABORT                       -1
#define NB_ERROR_PREMATURE             -2
#define NB_ERROR_INVALID_PATH          -3
#define NB_ERROR_INVALID_MAGIC_STRING  -4
#define NB_ERROR_INVALID_NB_TYPE       -5
#define NB_ERROR_INVALID_STING_ENC     -6
#define NB_ERROR_INVALID_NB_STATE      -7
#define NB_ERROR_LZF_DECOMPRESS        -8

#define NB_LOOP_BEGIN(_rp, _nb, _depth)           \
    _nb = stack_alloc_node_builder(_rp, _depth);  \
    do

#define NB_LOOP_END(_rp, _rc)                     \
    while (_rc == NB_AGAIN);                      \
    if (_rc == NB_OVER) {                         \
        stack_pop_node_builder(_rp);              \
    }

rdb_node_builder_t *  stack_push_node_builder(rdb_parser_t *rp);
void                  stack_pop_node_builder(rdb_parser_t *rp);
rdb_node_builder_t *  stack_alloc_node_builder(rdb_parser_t *rp, uint8_t depth);

rdb_node_builder_t *  stack_node_builder_at(rdb_parser_t *rp, uint8_t depth);
rdb_node_builder_t *  stack_node_builder_top(rdb_parser_t *rp);

/* EOF */