#pragma once

#include "rdb_parser_def.h"

rdb_parser_t *  create_rdb_parser(func_process_rdb_node cb, void *payload);
void            destroy_rdb_parser(rdb_parser_t *rp);
void            reset_rdb_parser(rdb_parser_t *rp);

int             rdb_node_parse_once(rdb_parser_t *rp, bip_buf_t *bb);
int             rdb_dumped_data_parse_once(rdb_parser_t *rp, bip_buf_t *bb);

int             rdb_parse_file(rdb_parser_t *rp, const char *path);

/* EOF */