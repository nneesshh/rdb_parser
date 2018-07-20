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

static int __build_lzf_string_compress_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);
static int __build_lzf_string_raw_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);
static int __build_lzf_string_lzf(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val);

static int
__build_lzf_string_compress_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    size_t n;

    if ((n = rdb_node_read_store_len(rp, bb, NULL, &nb->c_len)) == 0)
        return NB_ERROR_PREMATURE;

    /* ok */
    rdb_node_calc_crc(rp, bb, n);

    /* next state */
    nb->state = BUILD_LZF_STRING_RAW_LEN;

    /* pre-alloc */
    nb->tmp_val.data = nx_palloc(rp->n_pool, nb->c_len + 1);
    nb->len = 0;
    return NB_AGAIN;
}

static int
__build_lzf_string_raw_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    size_t n;

    if ((n = rdb_node_read_store_len(rp, bb, NULL, &nb->len)) == 0)
        return NB_ERROR_PREMATURE;

    /* ok */
    rdb_node_calc_crc(rp, bb, n);

    /* next state */
    nb->state = BUILD_LZF_STRING_LZF;
    return NB_AGAIN;
}

static int
__build_lzf_string_lzf(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val)
{
    size_t buf_size, want_size, consume_size;
    uint8_t *ptr;
    char *s, *raw;
    int ret;

    buf_size = bip_buf_get_committed_size(bb);
    want_size = nb->c_len - nb->tmp_val.len;
    consume_size = buf_size > want_size ? want_size : buf_size;

    s = (char *)nb->tmp_val.data + nb->tmp_val.len;
    ptr = bip_buf_get_contiguous_block(bb);
    nx_memcpy(s, ptr, consume_size);
    nb->tmp_val.len += consume_size;

    /* part consume */
    rdb_node_calc_crc(rp, bb, consume_size);

    if (want_size == consume_size) {
        raw = nx_palloc(rp->n_pool, nb->len + 1);
        if ((ret = lzf_decompress(nb->tmp_val.data, nb->tmp_val.len, raw, nb->len)) == 0) {
            nx_pfree(rp->n_pool, nb->tmp_val.data);
            nx_pfree(rp->n_pool, raw);
            return NB_ERROR_LZF_DECOMPRESS;
        }

        raw[nb->len] = '\0';
        nx_str_set2(val, raw, nb->len);

        nx_pfree(rp->n_pool, nb->tmp_val.data);
        return NB_OVER;
    }
    return NB_ERROR_PREMATURE;
}

int
build_lzf_string_value(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb->depth + 1;

    /*
    * 1. load compress length.
    * 2. load raw length.
    * 3. load lzf_string, and use lzf_decompress to decode.
    */
    
    /* sub builder */
    NB_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_LZF_STRING_IDLE:
        case BUILD_LZF_STRING_COMPRESS_LEN:
            rc = __build_lzf_string_compress_len(rp, sub_nb, bb);
            break;

        case BUILD_LZF_STRING_RAW_LEN:
            rc = __build_lzf_string_raw_len(rp, sub_nb, bb);
            break;

        case BUILD_LZF_STRING_LZF:
            rc = __build_lzf_string_lzf(rp, sub_nb, bb, val);
            break;

        default:
            rc = NB_ERROR_INVALID_NB_STATE;
            break;
        }

    }
    NB_LOOP_END(rp, rc)
    return rc;
}