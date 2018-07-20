#pragma once

#include "rdb_parser_def.h"

int  build_string_value(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val);

/* EOF */