#pragma once

#include "rdb_parser_def.h"

int  build_zl_list_value(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, rdb_kv_chain_t **vall, size_t *size);

/* EOF */