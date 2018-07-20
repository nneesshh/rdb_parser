#include "build_factory.h"

#include <stdio.h>
#include <string.h>

enum BUILD_STRING_TAG {
    BUILD_STRING_IDLE = 0,
    BUILD_STRING_STORE_LEN,
    BUILD_STRING_ENC,
    BUILD_STRING_PLAIN,
};

static int __build_string_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val);
static int __build_string_enc(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val);
static int __build_string_plain(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val);

static int
__build_string_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val)
{
    size_t n;
    uint8_t is_encoded;
    uint32_t len;

    if ((n = rdb_node_read_store_len(rp, bb, &is_encoded, &len)) == 0)
        return NB_ERROR_PREMATURE;

    nb->store_len = len;
    nb->c_len = 0;
    nb->len = 0;

    /* ok */
    rdb_node_calc_crc(rp, bb, n);

    /* next state */
    if (is_encoded) {
        nb->state = BUILD_STRING_ENC;
    }
    else {
        nb->state = BUILD_STRING_PLAIN;

        /* pre-alloc */
        val->data = nx_palloc(rp->n_pool, nb->store_len + 1);
        val->len = 0;
    }
    return NB_AGAIN;
}

static int
__build_string_enc(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val)
{
    size_t n;
    int32_t enc_int;
    char *s32;

    switch (nb->store_len) {

    case RDB_STR_ENC_INT8:
    case RDB_STR_ENC_INT16:
    case RDB_STR_ENC_INT32:
        if ((n = rdb_node_read_int(rp, bb, (uint8_t)nb->store_len, &enc_int)) == 0)
            return NB_ERROR_PREMATURE;

        s32 = nx_palloc(rp->n_pool, 30);
        o_snprintf(s32, 30, "%ld", enc_int);
        nx_str_set2(val, s32, nx_strlen(s32));

        /* ok */
        rdb_node_calc_crc(rp, bb, n);
        return NB_OVER;

    case RDB_STR_ENC_LZF:
        return build_lzf_string_value(rp, nb, bb, val);

    default:
        break;
    }
    return NB_ERROR_INVALID_STING_ENC;
}

static int
__build_string_plain(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val)
{
    size_t buf_size, want_size, consume_size;
    uint8_t *ptr;
    char *s;

    buf_size = bip_buf_get_committed_size(bb);
    want_size = nb->store_len - val->len;
    consume_size = buf_size > want_size ? want_size : buf_size;

    s = (char *)val->data + val->len;
    ptr = bip_buf_get_contiguous_block(bb);
    nx_memcpy(s, ptr, consume_size);
    val->len += consume_size;

    /* part consume */
    rdb_node_calc_crc(rp, bb, consume_size);

    if (want_size == consume_size) {
        ((char *)val->data)[val->len] = '\0';
        return NB_OVER;
    }
    return NB_ERROR_PREMATURE;
}

int
build_string_value(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, nx_str_t *val)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb->depth + 1;

    /* sub builder */
    NB_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_STRING_IDLE:
        case BUILD_STRING_STORE_LEN:
            rc = __build_string_store_len(rp, sub_nb, bb, val);
            break;

        case BUILD_STRING_ENC:
            rc = __build_string_enc(rp, sub_nb, bb, val);
            break;

        case BUILD_STRING_PLAIN:
            rc = __build_string_plain(rp, sub_nb, bb, val);
            break;

        default:
            rc = NB_ERROR_INVALID_NB_STATE;
            break;
        }

    }
    NB_LOOP_END(rp, rc)
    return rc;
}