#include "build_factory.h"

#include <stdio.h>
#include <string.h>

#include "lzf.h"

enum BUILD_LZF_STRING_TAG {
    BUILD_LZF_STRING_IDLE = 0,
    BUILD_LZF_STRING_COMPRESS_LEN,
    BUILD_LZF_STRING_RAW_LEN,
    BUILD_LZF_STRING_LZF,
};

static int __build_lzf_string_compress_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b);
static int __build_lzf_string_raw_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b);
static int __build_lzf_string_lzf(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val);

static int
__build_lzf_string_compress_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    size_t n;

    if ((n = read_store_len(rp, b, NULL, &nb->c_len)) == 0)
        return NODE_BUILD_ERROR_PREMATURE;

    /* ok */
    calc_crc(rp, b, n);

    /* next state */
    nb->state = BUILD_LZF_STRING_RAW_LEN;

    /* pre-alloc */
    nb->tmpval.data = nx_palloc(rp->pool, nb->c_len + 1);
    nb->len = 0;
    return NODE_BUILD_AGAIN;
}

static int
__build_lzf_string_raw_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    size_t n;

    if ((n = read_store_len(rp, b, NULL, &nb->len)) == 0)
        return NODE_BUILD_ERROR_PREMATURE;

    /* ok */
    calc_crc(rp, b, n);

    /* next state */
    nb->state = BUILD_LZF_STRING_LZF;
    return NODE_BUILD_AGAIN;
}

static int
__build_lzf_string_lzf(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val)
{
    size_t buf_size, want_size, consume_size;
    char *s, *raw;
    int ret;

    buf_size = nx_buf_size(b);
    want_size = nb->c_len - nb->tmpval.len;
    consume_size = buf_size > want_size ? want_size : buf_size;

    s = (char *)nb->tmpval.data + nb->tmpval.len;
    nx_memcpy(s, b->pos, consume_size);
    nb->tmpval.len += consume_size;

    /* part consume */
    calc_crc(rp, b, consume_size);

    if (want_size == consume_size) {
        raw = nx_palloc(rp->pool, nb->len + 1);
        if ((ret = lzf_decompress(nb->tmpval.data, nb->tmpval.len, raw, nb->len)) == 0) {
            nx_pfree(rp->pool, nb->tmpval.data);
            nx_pfree(rp->pool, raw);
            return NODE_BUILD_ERROR_LZF_DECOMPRESS;
        }

        raw[nb->len] = '\0';
        nx_str_set2(val, raw, nb->len);

        nx_pfree(rp->pool, nb->tmpval.data);
        return NODE_BUILD_OVER;
    }
    return NODE_BUILD_ERROR_PREMATURE;
}

int
build_lzf_string_value(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb->depth + 1;

    /*
    * 1. load compress length.
    * 2. load raw length.
    * 3. load lzf_string, and use lzf_decompress to decode.
    */
    
    /* sub node builder */
    RDB_NODE_BUILDER_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_LZF_STRING_IDLE:
        case BUILD_LZF_STRING_COMPRESS_LEN:
            rc = __build_lzf_string_compress_len(rp, sub_nb, b);
            break;

        case BUILD_LZF_STRING_RAW_LEN:
            rc = __build_lzf_string_raw_len(rp, sub_nb, b);
            break;

        case BUILD_LZF_STRING_LZF:
            rc = __build_lzf_string_lzf(rp, sub_nb, b, val);
            break;

        default:
            rc = NODE_BUILD_ERROR_INVALID_NODE_STATE;
            break;
        }

    }
    RDB_NODE_BUILDER_LOOP_END(rp, rc)
    return rc;
}