#pragma once

#include "rdb_parser_def.h"

int  build_list_or_set_value(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size);

/* EOF */