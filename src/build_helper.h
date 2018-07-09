#pragma once

#include "rdb_parser_def.h"

size_t  calc_crc(rdb_parser_t *rp, nx_buf_t *b, size_t bytes);
size_t  read_kv_type(rdb_parser_t *rp, nx_buf_t *b, uint8_t *out);
size_t  read_store_len(rdb_parser_t *rp, nx_buf_t *b, size_t offset, uint8_t *is_encoded, uint32_t *out);
size_t  read_lzf_string(rdb_parser_t *rp, nx_buf_t *b, size_t offset, nx_str_t *out);
size_t  read_int(rdb_parser_t *rp, nx_buf_t *b, size_t offset, uint8_t enc, int32_t *out);
size_t  read_string(rdb_parser_t *rp, nx_buf_t *b, size_t offset, nx_str_t *out);

/* EOF */