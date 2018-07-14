#include "build_factory.h"

#include <stdio.h>
#include <string.h>

enum BUILD_STRING_TAG {
    BUILD_STRING_IDLE = 0,
    BUILD_STRING_STORE_LEN,
    BUILD_STRING_ENC,
    BUILD_STRING_PLAIN,
};

static int __build_string_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val);
static int __build_string_enc(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val);
static int __build_string_plain(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val);

static int
__build_string_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val)
{
    size_t n;
    uint8_t is_encoded;
    uint32_t len;

    if ((n = read_store_len(rp, b, &is_encoded, &len)) == 0)
        return NODE_BUILD_ERROR_PREMATURE;

    nb->store_len = len;
    nb->c_len = 0;
    nb->len = 0;

    /* ok */
    calc_crc(rp, b, n);

    /* next state */
    if (is_encoded) {
        nb->state = BUILD_STRING_ENC;
    }
    else {
        nb->state = BUILD_STRING_PLAIN;

        /* pre-alloc */
        val->data = nx_palloc(rp->pool, nb->store_len + 1);
        val->len = 0;
    }
    return NODE_BUILD_AGAIN;
}

static int
__build_string_enc(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val)
{
    size_t n;
    int32_t enc_int;
    char *s32;

    switch (nb->store_len) {

    case REDIS_STR_ENC_INT8:
    case REDIS_STR_ENC_INT16:
    case REDIS_STR_ENC_INT32:
        if ((n = read_int(rp, b, (uint8_t)nb->store_len, &enc_int)) == 0)
            return NODE_BUILD_ERROR_PREMATURE;

        s32 = nx_palloc(rp->pool, 30);
        o_snprintf(s32, 30, "%ld", enc_int);
        nx_str_set2(val, s32, strlen(s32));

        /* ok */
        calc_crc(rp, b, n);
        return NODE_BUILD_OVER;

    case REDIS_STR_ENC_LZF:
        return build_lzf_string_value(rp, nb, b, val);

    default:
        break;
    }
    return NODE_BUILD_ERROR_INVALID_STING_ENC;
}

static int
__build_string_plain(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val)
{
    size_t buf_size, want_size, consume_size;
    char *s;

    buf_size = nx_buf_size(b);
    want_size = nb->store_len - val->len;
    consume_size = buf_size > want_size ? want_size : buf_size;

    s = (char *)val->data + val->len;
    nx_memcpy(s, b->pos, consume_size);
    val->len += consume_size;

    /* part consume */
    calc_crc(rp, b, consume_size);

    if (want_size == consume_size) {
        ((char *)val->data)[val->len] = '\0';
        return NODE_BUILD_OVER;
    }
    return NODE_BUILD_ERROR_PREMATURE;
}

int
build_string_value(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, nx_str_t *val)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb->depth + 1;

    /* sub node builder */
    RDB_NODE_BUILDER_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_STRING_IDLE:
        case BUILD_STRING_STORE_LEN:
            rc = __build_string_store_len(rp, sub_nb, b, val);
            break;

        case BUILD_STRING_ENC:
            rc = __build_string_enc(rp, sub_nb, b, val);
            break;

        case BUILD_STRING_PLAIN:
            rc = __build_string_plain(rp, sub_nb, b, val);
            break;

        default:
            rc = NODE_BUILD_ERROR_INVALID_NODE_STATE;
            break;
        }

    }
    RDB_NODE_BUILDER_LOOP_END(rp, rc)
    return rc;
}