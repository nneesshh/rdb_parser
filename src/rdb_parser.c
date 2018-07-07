#include "rdb_parser.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>

#include "lzf.h"
#include "intset.h"
#include "ziplist.h"
#include "zipmap.h"
#include "crc64.h"
#include "endian.h"

#define MAGIC_STR  "REDIS"

#define RDB_PARSE_PHASE_HEADER    0
#define RDB_PARSE_PHASE_READ_KEY  1
#define RDB_PARSE_PHASE_READ_VAL  2
#define RDB_PARSE_PHASE_FOOTER    3

#define REDIS_AUX_FIELDS      0xfa
#define REDIS_EXPIRE_MS       0xfc
#define REDIS_EXPIRE_SEC      0xfd
#define REDIS_DB_SELECTOR     0xfe
#define REDIS_EOF             0xff

#define REDIS_RDB_STRING        0
#define REDIS_RDB_LIST          1
#define REDIS_RDB_SET           2
#define REDIS_RDB_ZSET          3
#define REDIS_RDB_HASH          4

#define REDIS_RDB_ZIPMAP        9
#define REDIS_RDB_LIST_ZIPLIST  10 
#define REDIS_RDB_INTSET        11 
#define REDIS_RDB_ZSET_ZIPLIST  12 
#define REDIS_RDB_HASH_ZIPLIST  13 

#define REDIS_RDB_6B    0
#define REDIS_RDB_14B   1
#define REDIS_RDB_32B   2
#define REDIS_RDB_ENCV  3

#define REDIS_RDB_PARSE_ERROR_INVALID_PATH          -1
#define REDIS_RDB_PARSE_ERROR_PREMATURE             -2
#define REDIS_RDB_PARSE_ERROR_INVALID_MAGIC_STRING  -3

#define REDIS_RDB_ENC_INT8   0
#define REDIS_RDB_ENC_INT16  1
#define REDIS_RDB_ENC_INT32  2
#define REDIS_RDB_ENC_LZF    3

#define KEY_FIELD_STR      "key"
#define VAL_FIELD_STR      "val"
#define TYPE_FIELD_STR     "type"
#define EXPPIRE_FIELD_STR  "expire_time"
#define DB_NUM_STR         "db_selector"
#define VERSION_STR        "version"
#define MAGIC_VERSION      5


static size_t
__calc_crc(rdb_parser_t *parser, nx_buf_t *b, size_t bytes)
{
    if (nx_buf_size(b) >= bytes) {
        if (parser->version >= 5) {
            parser->chksum = crc64(parser->chksum, b->pos, bytes);
        }

        b->pos += bytes;
        parser->parsed += bytes;
        return bytes;
    }
    return 0;
}


static size_t
__read_kv_type(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t *out)
{
    size_t bytes = 1;
    uint8_t *p;

    if (nx_buf_size(b) < offset + bytes) {
        return 0;
    }

    p = b->pos + offset;

    (*out) = (*p);
    return bytes;
}


static size_t
__read_expire_time(rdb_parser_t *parser, nx_buf_t *b, size_t offset, int type, int *out)
{
    size_t bytes = 1;
    uint8_t *p;
    uint32_t t32;
    uint64_t t64;

    if (REDIS_EXPIRE_SEC == type) {

        bytes = 4;

        if (nx_buf_size(b) < offset + bytes) {
            return 0;
        }

        p = b->pos + offset;
        t32 = *(uint32_t *)p;
        memrev32(&t32);
        (*out) = (int)t32;
    }
    else {

        bytes = 8;

        if (nx_buf_size(b) < offset + bytes) {
            return 0;
        }

        p = b->pos + offset;
        t64 = *(uint64_t *)p;
        memrev64(&t64);
        (*out) = (int)(t64 / 1000);
    }

    return bytes;
}


static size_t
__read_store_len(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t *is_encoded, uint32_t *out)
{
    size_t bytes = 1;
    uint8_t *p;
    uint8_t type;

    if (nx_buf_size(b) < offset + bytes) {
        return 0;
    }

    p = b->pos + offset;

    if (is_encoded)
        *is_encoded = 0;

    type = ((*p) & 0xc0) >> 6;

    /**
    * 00xxxxxx, then the next 6 bits represent the length
    * 01xxxxxx, then the next 14 bits represent the length
    * 10xxxxxx, then the next 6 bits is discarded, and next 4bytes represent the length(BigEndian)
    * 11xxxxxx, The remaining 6 bits indicate the format
    */
    if (REDIS_RDB_6B == type) {
        (*out) = (*p) & 0x3f;
    }
    else if (REDIS_RDB_14B == type) {

        bytes = 2;

        if (nx_buf_size(b) < offset + bytes) {
            return 0;
        }

        (*out) = (((*p) & 0x3f) << 8) | (*(p + 1));
    }
    else if (REDIS_RDB_32B == type) {

        bytes = 5;

        if (nx_buf_size(b) < offset + bytes) {
            return 0;
        }

        memrev32(p + 1);
        (*out) = (*(uint32_t *)(p + 1));
    }
    else {
        if (is_encoded)
            *is_encoded = 1;

        (*out) = (*p) & 0x3f;
    }
    return bytes;
}


static size_t
__read_lzf_string(rdb_parser_t *parser, nx_buf_t *b, size_t offset, nx_str_t *out)
{
    size_t parsed = 0, n;
    uint32_t clen, len;
    char *cstr, *str;
    int ret;

    /*
    * 1. load compress length.
    * 2. load raw length.
    * 3. load lzf_string, and use lzf_decompress to decode.
    */

    if ((n = __read_store_len(parser, b, offset, NULL, &clen)) == 0)
        return 0;

    parsed += n;

    if ((n = __read_store_len(parser, b, offset + parsed, NULL, &len)) == 0)
        return 0;

    parsed += n;

    if (nx_buf_size(b) < offset + parsed + clen)
        return 0;

	cstr = b->pos + offset + parsed;
	str = nx_palloc(parser->pool, len + 1);
    if ((ret = lzf_decompress(cstr, clen, str, len)) == 0)
        goto FAILED;

	str[len] = '\0';
	nx_str_set2(out, str, len);

	parsed += clen;
    return parsed;

FAILED:
    nx_pfree(parser->pool, str);
    return 0;
}


size_t
__read_int(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t enc, int32_t *out)
{
    size_t bytes = 1;
    uint8_t *p;

    if (nx_buf_size(b) < offset + bytes) {
        return 0;
    }

    p = b->pos + offset;

    if (REDIS_RDB_ENC_INT8 == enc) {
        (*out) = (*p);
    }
    else if (REDIS_RDB_ENC_INT16 == enc) {

        bytes = 2;

        if (nx_buf_size(b) < offset + bytes) {
            return 0;
        }

        (*out) = (int16_t)((*p) | (*(p + 1)) << 8);
    }
    else {
        bytes = 4;

        if (nx_buf_size(b) < offset + bytes) {
            return 0;
        }

        (*out) = (int32_t)((*p) | ((*(p + 1)) << 8) | ((*(p + 2)) << 16) | ((*(p + 3)) << 24));
    }
    return bytes;
}


size_t
__read_string(rdb_parser_t *parser, nx_buf_t *b, size_t offset, nx_str_t *out)
{
    size_t parsed = 0, n;
    uint8_t is_encoded;
    uint32_t len;
	char *str;
    int32_t enc_len;

    if ((n = __read_store_len(parser, b, offset, &is_encoded, &len)) == 0)
        return 0;

    parsed += n;

    if (is_encoded) {
        switch (len) {

        case REDIS_RDB_ENC_INT8:
        case REDIS_RDB_ENC_INT16:
        case REDIS_RDB_ENC_INT32:
            if ((n = __read_int(parser, b, offset + parsed, (uint8_t)len, &enc_len)) == 0)
                return 0;

            str = nx_palloc(parser->pool, 30);
            sprintf(str, "%ld", enc_len);
			nx_str_set2(out, str, strlen(str));

			parsed += n;
            break;

        case REDIS_RDB_ENC_LZF:
            if ((n = __read_lzf_string(parser, b, offset + parsed, out)) == 0)
                return 0;

            parsed += n;
            break;

        default:
            return 0;
        }
    }
    else {

        if (nx_buf_size(b) < offset + len) {
            return 0;
        }

		str = nx_palloc(parser->pool, len + 1);
        nx_memcpy(str, b->pos + offset + parsed, len);
		str[len] = '\0';
		nx_str_set2(out, str, len);

        parsed += len;
    }
    return parsed;
}


static size_t
__read_db_selector(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint32_t *out)
{
    size_t parsed = 0, n;
    uint32_t selector;

    if ((n = __read_store_len(parser, b, offset, NULL, &selector)) == 0)
        return 0;

    parsed += n;

    (*out) = selector;
    return parsed;
}


static size_t
__read_aux_fields(rdb_parser_t *parser, nx_buf_t *b, size_t offset, nx_str_t *out)
{
    size_t parsed = 0, n;

    if ((n = __read_string(parser, b, offset, out)) == 0) {
        return 0;
    }

    parsed += n;
    return parsed;
}


static size_t
__load_str_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, nx_str_t *val)
{
    size_t parsed = 0, n;

    if ((n = __read_string(parser, b, offset, val)) == 0) {
        return 0;
    }

    parsed += n;
    return parsed;
}


static size_t
__load_intset_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, rdb_kv_chain_t **vall, uint32_t *size)
{
    size_t parsed = 0, n;
	nx_str_t str;
	uint32_t i;
    int64_t v64;
	char *s64;
    intset_t *is;

    rdb_kv_chain_t *ln, **ll;

    ll = vall;

    if ((n = __read_string(parser, b, offset, &str)) == 0) {
        return 0;
    }

    parsed += n;
    is = (intset_t*)str.data;

    for (i = 0; i < is->length; ++i) {
        intset_get(is, i, &v64);

		ln = alloc_rdb_kv_chain_link(parser, ll);

        s64 = nx_palloc(parser->pool, 30);
        sprintf(s64, "%lld", v64);
		nx_str_set2(&ln->kv->val, s64, strlen(s64));

        ll = &ln;
    }

	(*size) = is->length;
	nx_pfree(parser->pool, str.data);
    return parsed;
}


static size_t
__load_zllist_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, rdb_kv_chain_t **vall, uint32_t *size)
{
    size_t parsed = 0, n;
    nx_str_t str;

    if ((n = __read_string(parser, b, offset, &str)) == 0) {
        return 0;
    }

    parsed += n;
    
    load_ziplist_list_or_set(parser, str.data, vall, size);
	nx_pfree(parser->pool, str.data);
    return parsed;
}


static size_t
__load_zipmap_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, rdb_kv_chain_t **vall, uint32_t *size)
{
    size_t parsed = 0, n;
    nx_str_t str;

    if ((n = __read_string(parser, b, offset, &str)) == 0) {
        return 0;
    }

    parsed += n;

    load_zipmap(parser, str.data, vall, size);
	nx_pfree(parser->pool, str.data);
    return parsed;
}


static size_t
__load_ziplist_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, rdb_kv_chain_t **vall, uint32_t *size)
{
    size_t parsed = 0, n;
    nx_str_t str;

    if ((n = __read_string(parser, b, offset, &str)) == 0) {
        return 0;
    }

    parsed += n;

    load_ziplist_hash_or_zset(parser, str.data, vall, size);
	nx_pfree(parser->pool, str.data);
    return parsed;
}


static size_t
__load_list_or_set_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, rdb_kv_chain_t **vall, uint32_t *size)
{
    size_t parsed = 0, n;
    uint32_t i, len;
	nx_str_t str;

    rdb_kv_chain_t *ln, **ll;
    
    ll = vall;

    if ((n = __read_store_len(parser, b, offset, NULL, &len)) == 0)
        return 0;

    parsed += n;

    for (i = 0; i < len; i++) {

        if ((n = __read_string(parser, b, offset + parsed, &str)) == 0) {
            return 0;
        }

        parsed += n;

        ln = alloc_rdb_kv_chain_link(parser, ll);
		nx_str_set2(&ln->kv->val, str.data, str.len);
        ll = &ln;
    }

    (*size) = len;
    return parsed;
}


static size_t
__load_hash_or_zset_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, rdb_kv_chain_t **vall, uint32_t *size)
{
    size_t parsed = 0, n;
    uint32_t i, len;
    nx_str_t key, val;

    rdb_kv_chain_t *ln, **ll;

    ll = vall;

    if ((n = __read_store_len(parser, b, offset, NULL, &len)) == 0)
        return 0;

    parsed += n;

    for (i = 0; i < len; i++) {

        if ((n = __read_string(parser, b, offset + parsed, &key)) == 0) {
            return 0;
        }

        parsed += n;

        if ((n = __read_string(parser, b, offset + parsed, &val)) == 0) {
            return 0;
        }

        parsed += n;

        ln = alloc_rdb_kv_chain_link(parser, ll);
		nx_str_set2(&ln->kv->key, key.data, key.len);
		nx_str_set2(&ln->kv->val, val.data, val.len);
        ll = &ln;
    }

    (*size) = len;
    return parsed;
}


static size_t
__load_value(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t type, rdb_node_t *node)
{
    switch (type) {
    case REDIS_RDB_STRING:
        return __load_str_value(parser, b, offset, &node->val);

     case REDIS_RDB_INTSET:
        return __load_intset_value(parser, b, offset, &node->vall, &node->size);

     case REDIS_RDB_LIST_ZIPLIST:
        return __load_zllist_value(parser, b, offset, &node->vall, &node->size);

     case REDIS_RDB_ZIPMAP:
        return __load_zipmap_value(parser, b, offset, &node->vall, &node->size);

     case REDIS_RDB_ZSET_ZIPLIST:
     case REDIS_RDB_HASH_ZIPLIST:
        return __load_ziplist_value(parser, b, offset, &node->vall, &node->size);

     case REDIS_RDB_LIST:
     case REDIS_RDB_SET:
        return __load_list_or_set_value(parser, b, offset, &node->vall, &node->size);

     case REDIS_RDB_HASH:
     case REDIS_RDB_ZSET:
        return __load_hash_or_zset_value(parser, b, offset, &node->vall, &node->size);

    default:
        break;
    }
    return 0;
}


nx_chain_t *
alloc_temp_buf_chain_link(rdb_parser_t *parser, size_t size, nx_chain_t **ll)
{
    nx_chain_t *cl, *ln;

    if (parser->free) {
        cl = parser->free;
        parser->free = cl->next;
    }
    else {
        cl = nx_alloc_chain_link(parser->pool);
        if (cl == NULL) {
            return NULL;
        }
    }

    if ((*ll) == NULL) {
        (*ll) = cl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = cl;
    }

    cl->buf = nx_create_temp_buf(parser->pool, size);
    cl->next = NULL;
    return cl;
}


rdb_kv_chain_t *
alloc_rdb_kv_chain_link(rdb_parser_t *parser, rdb_kv_chain_t **ll)
{
    rdb_kv_chain_t *cl, *ln;

    cl = nx_palloc(parser->pool, sizeof(rdb_kv_chain_t));
    if (cl == NULL) {
        return NULL;
    }

    if ((*ll) == NULL) {
        (*ll) = cl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = cl;
    }

    cl->kv = nx_palloc(parser->pool, sizeof(rdb_kv_t));
	nx_str_null(&cl->kv->key);
	nx_str_null(&cl->kv->val);
    cl->next = NULL;
    return cl;
}


rdb_node_chain_t *
alloc_rdb_node_chain_link(rdb_parser_t *parser, rdb_node_chain_t **ll)
{
    rdb_node_chain_t *cl, *ln;

    cl = nx_palloc(parser->pool, sizeof(rdb_node_chain_t));
    if (cl == NULL) {
        return NULL;
    }

    if ((*ll) == NULL) {
        (*ll) = cl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = cl;
    }

    cl->node = nx_palloc(parser->pool, sizeof(rdb_node_t));
    cl->node->expire = -1;
    cl->node->type = 0;
	nx_str_null(&cl->node->key);
	nx_str_null(&cl->node->val);
    cl->node->vall = NULL;
    cl->node->size = 0;
    cl->next = NULL;

    return cl;
}


rdb_parser_t *
create_rdb_parser()
{
    rdb_parser_t *p = nx_alloc(sizeof(rdb_parser_t));

    /* NOTE: trick here, version set 5 as we want to calculate crc where read version field. */
    p->version = MAGIC_VERSION;
    p->chksum = 0;
    p->parsed = 0;

    p->pool = nx_create_pool(4096);
    p->in_b = nx_create_temp_buf(pool, 4096);
	p->root = NULL;
    p->handler = NULL;

    return p;
}


void
destroy_rdb_parser(rdb_parser_t *parser)
{
    nx_destroy_pool(parser->pool);
    nx_free(parser);
}


rdb_node_handler_t *
create_rdb_node_handler(rdb_parser_t *parser, uint8_t phase)
{
	rdb_node_handler_t *handler = nx_alloc(sizeof(rdb_node_handler_t));
	handler->phase = phase;
	handler->ready = 0;
	handler->state = 0;
	handler->consume = NULL;
	handler->tmp_b = NULL;

	switch (phase) {
	case RDB_PARSE_PHASE_HEADER:

#define REDIS_AUX_FIELDS        0xfa
#define REDIS_EXPIRE_MS         0xfc
#define REDIS_EXPIRE_SEC        0xfd
#define REDIS_DB_SELECTOR       0xfe
#define REDIS_EOF               0xff
	case REDIS_EOF:
		break;



	}

	handler->consume = NULL;
	handler->tmp_b = NULL;
	return handler;
}


size_t
parse_node_type(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t type)
{
    size_t parsed = 0, n;

    /* kv type */
    if ((n = __read_kv_type(parser, b, offset, &type)) == 0) {
        return 0;
    }

    parsed += n;

    return parsed;
}


size_t
parse_node(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t type)
{
    size_t parsed = 0, n;
    int expire = -1;
    nx_str_t key;
    rdb_node_chain_t *ln;

    /* expire time */
    if (REDIS_EXPIRE_SEC == type || REDIS_EXPIRE_MS == type) {
        if ((n = __read_expire_time(parser, b, offset + parsed, type, &expire)) == 0) {
            return 0;
        }

        parsed += n;

        if ((n = __read_kv_type(parser, b, offset + parsed, &type)) == 0) {
            return 0;
        }

        parsed += n;
    }

    /* key */
    if ((n = __read_string(parser, b, offset + parsed, &key)) == 0) {
        return 0;
    }

    parsed += n;

    /* type */
    ln = alloc_rdb_node_chain_link(parser, &parser->root);
    ln->node->expire = expire;
    ln->node->type = type;
	nx_str_set2(&ln->node->key, key.data, key.len);

    /* value */
    if ((n = __load_value(parser, b, offset + parsed, type, ln->node)) == 0) {
        return 0;
    }

    parsed += n;

    return parsed;
}


int
rdb_parse_file(const char *path)
{
    int rc = 0;

    FILE *rdb_fp;
    rdb_parser_t *parser;

    char chversion[5];
    uint32_t db_selector;
    nx_str_t aux_fields;
    uint8_t type;
    int nbytes, noffset, is_eof;
    size_t n;

    nx_buf_t *b;
    nx_chain_t *cl;

    rdb_fp =  fopen(path, "r");
    parser = create_rdb_parser();

    /* read file into buffer */
    cl = alloc_temp_buf_chain_link(parser, 50, &parser->busy);
    ++parser->nbusy;
	b = cl->buf;

    nbytes = fread(b->pos, 1, (b->end - b->start), rdb_fp);
    if (nbytes <= 0)
        is_eof = 1;
    else
        is_eof = 0;

    b->last += nbytes;

    /* magic string(5bytes) and version(4bytes) */
    if (nbytes < 9) {
        rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
    }

    if (memcmp(b->pos, MAGIC_STR, 5) != 0)
        rc = REDIS_RDB_PARSE_ERROR_INVALID_MAGIC_STRING;

    nx_memcpy(chversion, b->pos + 5, 4);
    chversion[4] = '\0';
    parser->version = atoi(chversion);

    noffset = 9;

    __calc_crc(parser, b, noffset);
    nbytes -= noffset;
    noffset = 0;

    while (1) {

		if (!parser->handler->ready) {
			parser->handler->consume(b);
			continue;
		}

        if ((n = __read_kv_type(parser, b, noffset, &type)) == 0) {
            rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
            break;
        }

        noffset += n;

        /* db selector */
        if (REDIS_DB_SELECTOR == type) {
            db_selector = -1;
            if ((n = __read_db_selector(parser, b, noffset, &db_selector)) == 0) {
                rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
                break;
            }

            noffset += n;

            if ((n = __read_kv_type(parser, b, noffset, &type)) == 0) {
                rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
                break;
            }

            noffset += n;
        }

        /* aux fields */
        if (REDIS_AUX_FIELDS == type) {
            if ((n = __read_aux_fields(parser, b, noffset, &aux_fields)) == 0) {
                rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
                break;
            }

            noffset += n;

            if ((n = __read_kv_type(parser, b, noffset, &type)) == 0) {
                rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
                break;
            }

            noffset += n;
        }

        /* end of rdb file */
        if (REDIS_EOF == type)
            break;

        /* node */
        if ((n = parse_node(parser, b, noffset, type)) == 0) {
            rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
            break;
        }

        noffset += n;

        __calc_crc(parser, b, noffset);
        nbytes -= noffset;
        noffset = 0;
    }

//     if (version >= MAGIC_VERSION)
//         __check_crc(rdb_fp, chksum);

//    if (st.st_size != parser->parsed) {
//         logger(ERROR, "Load rdb file failed, Bytes is %llu, expected is %llu version %d",
//             parsed_bytes, st.st_size, version);
//    }

    if (rc == REDIS_RDB_PARSE_ERROR_PREMATURE) {

    }

    fclose(rdb_fp);
    return 0;
}
