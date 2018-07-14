#include "build_factory.h"

enum BUILD_NODE_DETAIL_KV_TAG {
    BUILD_NODE_DETAIL_KV_IDLE = 0,
    BUILD_NODE_DETAIL_KV_KEY,
    BUILD_NODE_DETAIL_KV_VAL,
};

static int
__build_node_detail_kv_key(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    int rc = 0;

    rdb_node_t *node;

    node = rp->cur_ln->node;

    /* key */
    rc = build_string_value(rp, nb, b, &node->key);

    if (rc == NODE_BUILD_OVER) {
        /* next state */
        nb->state = BUILD_NODE_DETAIL_KV_VAL;
        return NODE_BUILD_AGAIN;
    }
    return rc;
}

int
build_node_detail_kv(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb->depth + 1;

    /* sub node builder */
    RDB_NODE_BUILDER_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_NODE_DETAIL_KV_IDLE:
        case BUILD_NODE_DETAIL_KV_KEY:
            rc = __build_node_detail_kv_key(rp, sub_nb, b);
            break;

        case BUILD_NODE_DETAIL_KV_VAL:
            rc = build_node_detail_kv_val(rp, sub_nb, b);
            break;

        default:
            rc = NODE_BUILD_ERROR_INVALID_NODE_STATE;
            break;
        }

    }
    RDB_NODE_BUILDER_LOOP_END(rp, rc)
    return rc;
}