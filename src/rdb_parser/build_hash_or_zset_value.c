#include "build_factory.h"

enum BUILD_HASH_OR_ZSET_TAG {
    BUILD_HASH_OR_ZSET_IDLE = 0,
    BUILD_HASH_OR_ZSET_STORE_LEN,
    BUILD_HASH_OR_ZSET_LOOP_KEY,
    BUILD_HASH_OR_ZSET_LOOP_VAL,
};

static int __build_hash_or_zset_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);
static int __build_hash_or_zset_loop_key(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);
static int __build_hash_or_zset_loop_val(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, rdb_kv_chain_t **vall, size_t *size);

static int
__build_hash_or_zset_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    size_t n;
    uint32_t len;

    if ((n = rdb_node_read_store_len(rp, bb, NULL, &len)) == 0)
        return NB_ERROR_PREMATURE;

    nb->store_len = len;
    nb->len = 0;

    /* ok */
    rdb_node_calc_crc(rp, bb, n);

    /* next state */
    nb->state = BUILD_HASH_OR_ZSET_LOOP_KEY;
    return NB_AGAIN;
}

static int
__build_hash_or_zset_loop_key(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    int rc = 0;

    if (nb->len < nb->store_len) {
        rc = build_string_value(rp, nb, bb, &nb->tmp_key);

        if (rc == NB_OVER) {
            /* next state */
            nb->state = BUILD_HASH_OR_ZSET_LOOP_VAL;
            return NB_AGAIN;
        }
        return rc;
    }
    else {
        /* next state */
        nb->state = BUILD_HASH_OR_ZSET_LOOP_VAL;
        return NB_AGAIN;
    }
}

static int
__build_hash_or_zset_loop_val(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, rdb_kv_chain_t **vall, size_t *size)
{
    int rc = 0;

    rdb_node_t *node;

    rdb_kv_chain_t **ll;

	node = rp->n;

    if (nb->len < nb->store_len) {
        rc = build_string_value(rp, nb, bb, &nb->tmp_val);

        /* over */
        if (rc == NB_OVER) {

            ll = (NULL == node->vall_tail) ? vall : &node->vall_tail;
            node->vall_tail = alloc_rdb_kv_chain_link(rp->n_pool, ll);

            nx_str_set2(&node->vall_tail->kv->key, nb->tmp_key.data, nb->tmp_key.len);
            nx_str_set2(&node->vall_tail->kv->val, nb->tmp_val.data, nb->tmp_val.len);

            nx_str_null(&nb->tmp_key);
            nx_str_null(&nb->tmp_val);

            ++nb->len;

            if (nb->len < nb->store_len) {
                /* next state */
                nb->state = BUILD_HASH_OR_ZSET_LOOP_KEY;
                return NB_AGAIN;
            }

            (*size) = nb->len;
        }
        return rc;
    }
    else {
        /* next state */
        nb->state = BUILD_HASH_OR_ZSET_LOOP_VAL;
        return NB_AGAIN;
    }
}

int
build_hash_or_zset_value(rdb_parser_t *rp, rdb_node_builder_t *nb1, bip_buf_t *bb, rdb_kv_chain_t **vall, size_t *size)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb1->depth + 1;

    /* sub builder */
    NB_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_HASH_OR_ZSET_IDLE:
        case BUILD_HASH_OR_ZSET_STORE_LEN:
            rc = __build_hash_or_zset_store_len(rp, sub_nb, bb);
            break;

        case BUILD_HASH_OR_ZSET_LOOP_KEY:
            rc = __build_hash_or_zset_loop_key(rp, sub_nb, bb);
            break;

        case BUILD_HASH_OR_ZSET_LOOP_VAL:
            rc = __build_hash_or_zset_loop_val(rp, sub_nb, bb, vall, size);
            break;

        default:
            rc = NB_ERROR_INVALID_NB_STATE;
            break;
        }

    }
    NB_LOOP_END(rp, rc)
    return rc;
}
