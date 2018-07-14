#pragma once

#include "rdb_parser_def.h"

rdb_parser_t *  create_rdb_parser(size_t size);
void            destroy_rdb_parser(rdb_parser_t *rp);
void            reset_rdb_parser(rdb_parser_t *rp);

int             rdb_parse_node_val_once(rdb_parser_t *rp, nx_buf_t *b);
int             rdb_parse_once(rdb_parser_t *rp, nx_buf_t *b);
int             rdb_parse_file(rdb_parser_t *rp, const char *path);

void            rdb_dump(rdb_parser_t *rp, const char *dump_to_path);

/* EOF */