#include "build_factory.h"

enum BUILD_NODE_DETAIL_KV_TAG {
    BUILD_NODE_DETAIL_KV_IDLE = 0,
    BUILD_NODE_DETAIL_KV_KEY,
    BUILD_NODE_DETAIL_KV_VAL,
};

static int
__build_node_detail_kv_key(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    int rc = 0;

    rdb_node_t *node;

	node = rp->n;

    /* key */
    rc = build_string_value(rp, nb, bb, &node->key);

    if (rc == NB_OVER) {
        /* next state */
        nb->state = BUILD_NODE_DETAIL_KV_VAL;
        return NB_AGAIN;
    }
    return rc;
}

int
build_node_detail_kv(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb->depth + 1;

    /* sub builder */
    NB_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_NODE_DETAIL_KV_IDLE:
        case BUILD_NODE_DETAIL_KV_KEY:
            rc = __build_node_detail_kv_key(rp, sub_nb, bb);
            break;

        case BUILD_NODE_DETAIL_KV_VAL:
            rc = build_node_detail_kv_val(rp, sub_nb, bb);
            break;

        default:
            rc = NB_ERROR_INVALID_NB_STATE;
            break;
        }

    }
    NB_LOOP_END(rp, rc)
    return rc;
}