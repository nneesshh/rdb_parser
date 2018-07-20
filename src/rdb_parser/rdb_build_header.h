#pragma once

#include "rdb_parser.h"

int                    node_process_header(rdb_parser_t *parser, nx_buf_t *b);
int                    node_process_body_db_selector(rdb_parser_t *parser, nx_buf_t *b);
int                    node_process_body_aux_fields(rdb_parser_t *parser, nx_buf_t *b);
int                    node_process_body_kv(rdb_parser_t *parser, nx_buf_t *b);
int                    node_process_footer(rdb_parser_t *parser, nx_buf_t *b);


/* EOF */