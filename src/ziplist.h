#pragma once

#include <stdint.h>

#include "rdb_parser.h"

#define ZIPLIST_BIGLEN 254
#define ZIPLIST_END 0xff

#define ZIP_ENC_INT8  0xfe
#define ZIP_ENC_INT16 0xc0
#define ZIP_ENC_INT24 0xf0
#define ZIP_ENC_INT32 0xd0
#define ZIP_ENC_INT64 0xe0

#define ZIP_ENC_STR_6B  (0 << 6)
#define ZIP_ENC_STR_14B (1 << 6)
#define ZIP_ENC_STR_32B (2 << 6)

#define ZIP_ENC_STR_MASK 0xc0
#define ZIP_IS_END(entry) ((uint8_t)entry[0] == ZIPLIST_END)
#define ZL_BYTES(zl) *((uint32_t *)(zl))
#define ZL_LEN(zl) *((uint16_t*)((zl) + 2 * sizeof(uint32_t)))
#define ZL_HDR_SIZE (2 * sizeof(uint32_t) + sizeof(uint16_t))
#define ZL_ENTRY(zl) ((uint8_t *)zl + ZL_HDR_SIZE)

typedef struct {
    uint32_t bytes;
    uint32_t len;
    uint32_t tail;
    char entrys[0];
} ziplist;

void load_ziplist_hash_or_zset(rdb_parser_t *parser, const char *zl, rdb_kv_chain_t **vall, uint32_t *size);
void load_ziplist_list_or_set(rdb_parser_t *parser, const char *zl, rdb_kv_chain_t **vall, uint32_t *size);

void ziplist_dump(rdb_parser_t *parser, const char *s);

/* EOF */