#include "ziplist.h"

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "endian.h"

static uint32_t
__ziplist_prev_len_size(const char *s)
{
    return ((uint8_t)s[0] < ZIPLIST_BIGLEN) ? 1 : 5;
}

static uint8_t
__ziplist_entry_is_str(const char *entry)
{
    uint8_t enc;
    enc = entry[__ziplist_prev_len_size(entry)];
    enc &= ZIP_ENC_STR_MASK;
    if (enc == ZIP_ENC_STR_6B 
      || enc == ZIP_ENC_STR_14B
      || enc == ZIP_ENC_STR_32B
    ) {
        return 1;
    } else {
        return 0;
    }
}

static uint32_t
__ziplist_entry_nx_strlen(const char *entry)
{
    uint8_t enc;
    uint32_t pos;
    
    pos = __ziplist_prev_len_size(entry);
    enc = (uint8_t)entry[pos] & 0xc0;
    if (enc == ZIP_ENC_STR_6B) {
        return entry[pos] & ~ZIP_ENC_STR_MASK;
    } else if (enc == ZIP_ENC_STR_14B) {
        uint32_t ret = (((uint8_t)entry[pos] & ~ZIP_ENC_STR_MASK) << 8) | (uint8_t)entry[pos + 1];
        return ret;
    } else if (enc == ZIP_ENC_STR_32B) {
        return ((uint8_t)entry[pos + 1] << 24) | ((uint8_t)entry[pos + 2] << 16) | ((uint8_t)entry[pos + 3] << 8) | (uint8_t)entry[pos + 4];
    }

    return 0;
}

static uint32_t
__ziplist_entry_size(const char *entry)
{
    uint8_t enc;
    uint32_t size = 1, pos;

    pos = __ziplist_prev_len_size(entry);
    enc = entry[pos];
    // prev entry length
    size += __ziplist_prev_len_size(entry); 

    if(enc == ZIP_ENC_INT8) {
        size += 1;
    } else if (enc == ZIP_ENC_INT16) {
        size += 2;
    } else if (enc == ZIP_ENC_INT24) {
        size += 3;
    } else if (enc == ZIP_ENC_INT32) {
        size += 4;
    } else if (enc == ZIP_ENC_INT64) {
        size += 8;
    } else if ((enc & ZIP_ENC_STR_MASK) == ZIP_ENC_STR_6B) {
        size += __ziplist_entry_nx_strlen(entry);
    } else if ((enc & ZIP_ENC_STR_MASK) == ZIP_ENC_STR_14B) {
        size += 1;
        size += __ziplist_entry_nx_strlen(entry);
    } else if ((enc & ZIP_ENC_STR_MASK) == ZIP_ENC_STR_32B) {
        size += 4;
        size += __ziplist_entry_nx_strlen(entry);
    }
    
    return size;
}

static char*
__ziplist_entry_str(nx_pool_t *pool, const char *entry)
{
    uint8_t enc;
    uint32_t pre_len_size,len_size = 1, slen;
    char *content, *str = NULL;

    pre_len_size = __ziplist_prev_len_size(entry);
    enc = entry[pre_len_size] & ZIP_ENC_STR_MASK;
    if (enc == ZIP_ENC_STR_14B) len_size = 2;
    if (enc == ZIP_ENC_STR_32B) len_size = 5;

    content = (char *)entry + pre_len_size + len_size;
    if (enc == ZIP_ENC_STR_6B || enc == ZIP_ENC_STR_14B
      || enc == ZIP_ENC_STR_32B) {
        
        slen = __ziplist_entry_nx_strlen(entry);
		str = nx_palloc(pool, slen + 1);
        nx_memcpy(str, content, slen);
        str[slen] = '\0';
    }
    return str;
}

static uint8_t
__ziplist_entry_int(const char *entry, int64_t *v)
{
    int8_t  v8;
    int16_t v16;
    int32_t v32;
    int64_t v64;
    uint8_t enc;
    uint32_t pre_len_size;
    char *content;
    
    pre_len_size = __ziplist_prev_len_size(entry);
    content = (char *)entry + pre_len_size;
    enc = entry[pre_len_size];
    
    // add one byte for encode.
    if (enc == ZIP_ENC_INT8) {
        nx_memcpy(&v8, content + 1, sizeof(int8_t));
        *v = v8;
    } else if (enc == ZIP_ENC_INT16) {
        nx_memcpy(&v16, content + 1, sizeof(int16_t));
        memrev16ifbe(&v16);
        *v = v16;
    } else if (enc == ZIP_ENC_INT24) {
        nx_memcpy(&v32, content + 1, 3);
        memrev32ifbe(&v32);
        *v = v32;
    } else if (enc == ZIP_ENC_INT32) {
        nx_memcpy(&v32, content + 1, sizeof(int32_t));
        memrev32ifbe(&v32);
        *v = v32;
    } else if (enc == ZIP_ENC_INT64){
        nx_memcpy(&v64, content + 1, sizeof(int64_t));
        memrev64ifbe(&v64);
        *v = v64;
    } else if ((enc & 0xf0) == 0xf0){
        v8 = content[0] & 0x0f;
        *v = v8;
    } else {
        return 0;
    }
  
    return  1;
}


void
load_ziplist_list_or_set (rdb_parser_t *parser, const char *zl, rdb_kv_chain_t **vall, uint32_t *size)
{
	uint32_t len = 0;
    int64_t v;
    char *entry, *str;

	rdb_kv_chain_t *ln, **ll;

	ll = vall;

    entry = (char *)ZL_ENTRY(zl);
    while (!ZIP_IS_END(entry)) {

        if (__ziplist_entry_is_str(entry)) {
            str = __ziplist_entry_str(parser->pool, entry); 
        } else {
            if(__ziplist_entry_int(entry, &v) > 0) {
				str = nx_palloc(parser->pool, 30);
				sprintf(str, "%lld", v);
            }
        }

		ln = alloc_rdb_kv_chain_link(parser, ll);
		nx_str_set2(&ln->kv->val, str, nx_strlen(str));
		ll = &ln;

		++len;
        entry += __ziplist_entry_size(entry);
    }

	(*size) = len;
}


void
load_ziplist_hash_or_zset(rdb_parser_t *parser, const char *zl, rdb_kv_chain_t **vall, uint32_t *size)
{
	uint32_t len = 0;
    int64_t v;
    char *entry, *key, *val;

	rdb_kv_chain_t *ln, **ll;

	ll = vall;

    entry = (char *)ZL_ENTRY(zl);
    while (!ZIP_IS_END(entry)) {

		/* key */
        if (__ziplist_entry_is_str(entry)) {
            key = __ziplist_entry_str(parser->pool, entry);
        } else {
            if(__ziplist_entry_int(entry, &v) > 0) {
				key = nx_palloc(parser->pool, 30);
				sprintf(key, "%lld", v);
            }
        }
		entry += __ziplist_entry_size(entry);

		/* value */
		if (__ziplist_entry_is_str(entry)) {
			val = __ziplist_entry_str(parser->pool, entry);
		}
		else {
			if (__ziplist_entry_int(entry, &v) > 0) {
				val = nx_palloc(parser->pool, 30);
				sprintf(val, "%lld", v);
			}
		}

		ln = alloc_rdb_kv_chain_link(parser, ll);
		nx_str_set2(&ln->kv->key, key, nx_strlen(key));
		nx_str_set2(&ln->kv->val, val, nx_strlen(val));
		ll = &ln;

		++len;
    }

	(*size) = len;
}


void
ziplist_dump(rdb_parser_t *parser, char *s)
{
    uint32_t i = 0, len;
    char *entry, *str;

    printf("ziplist { \n");
    printf("bytes: %u\n", ZL_BYTES(s));
    printf("len: %u\n", ZL_LEN(s));
    len = ZL_LEN(s);
    entry = (char *)ZL_ENTRY(s);
    while (!ZIP_IS_END(entry)) {
        if (__ziplist_entry_is_str(entry)) {
            str = __ziplist_entry_str(parser->pool, entry);
            if (str) {
                printf("str value: %s\n", str); 
            }
        } else {
            int64_t v;
            if(__ziplist_entry_int(entry, &v) > 0) {
                printf("int value: %lld\n", v);
            }
        }
        entry += __ziplist_entry_size(entry);
        ++i;
    }
    printf("}\n");
    if(i < (0xffff - 1) && i != len) {
        printf("====== Ziplist len error. ======\n");
        exit(1);
    }
}
