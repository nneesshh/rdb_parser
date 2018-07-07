#pragma once

#include "rdb_parser.h"

void load_zipmap(rdb_parser_t *parser, const char *zm, rdb_kv_chain_t **vall, uint32_t *size);

void zipmap_dump(rdb_parser_t *parser, const char *s);

/* EOF */