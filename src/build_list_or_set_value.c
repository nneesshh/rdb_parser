#include "build_factory.h"

enum BUILD_LIST_OR_SET_TAG {
    BUILD_LIST_OR_SET_IDLE = 0,
    BUILD_LIST_OR_SET_STORE_LEN,
    BUILD_LIST_OR_SET_LOOP_VAL,
};

static int __build_list_or_set_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b);
static int __build_list_or_set_loop_val(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size);

static int
__build_list_or_set_store_len(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    size_t n;
    uint32_t len;

    if ((n = read_store_len(rp, b, NULL, &len)) == 0)
        return NODE_BUILD_ERROR_PREMATURE;

    nb->store_len = len;
    nb->len = 0;

    /* ok */
    calc_crc(rp, b, n);

    /* next state */
    nb->state = BUILD_LIST_OR_SET_LOOP_VAL;
    return NODE_BUILD_AGAIN;
}

static int
__build_list_or_set_loop_val(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
    int rc = 0;

    rdb_node_t *node;
    rdb_kv_chain_t **ll;

    node = rp->cur_ln->node;

    if (nb->len < nb->store_len) {
        rc = build_string_value(rp, nb, b, &nb->tmpval);

        /* over */
        if (rc == NODE_BUILD_OVER) {

            ll = (NULL == node->cur_val_ln) ? vall : &node->cur_val_ln;
            node->cur_val_ln = alloc_rdb_kv_chain_link(rp, ll);
            nx_str_set2(&node->cur_val_ln->kv->val, nb->tmpval.data, nb->tmpval.len);

            nx_str_null(&nb->tmpval);

            ++nb->len;

            if (nb->len < nb->store_len) {
                /* next state */
                nb->state = BUILD_LIST_OR_SET_LOOP_VAL;
                return NODE_BUILD_AGAIN;
            }

            (*size) = nb->len;
            return NODE_BUILD_OVER;
        }
        return rc;
    }

    /* next state */
    nb->state = BUILD_LIST_OR_SET_LOOP_VAL;
    return NODE_BUILD_AGAIN;
}

int
build_list_or_set_value(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
    int rc = 0;
    rdb_node_builder_t *sub_nb;
    int depth = nb->depth + 1;

    /* sub node builder */
    RDB_NODE_BUILDER_LOOP_BEGIN(rp, sub_nb, depth)
    {
        /* sub process */
        switch (sub_nb->state) {
        case BUILD_LIST_OR_SET_IDLE:
        case BUILD_LIST_OR_SET_STORE_LEN:
            rc = __build_list_or_set_store_len(rp, sub_nb, b);
            break;

        case BUILD_LIST_OR_SET_LOOP_VAL:
            rc = __build_list_or_set_loop_val(rp, sub_nb, b, vall, size);
            break;

        default:
            rc = NODE_BUILD_ERROR_INVALID_NODE_STATE;
            break;
        }

    }
    RDB_NODE_BUILDER_LOOP_END(rp, rc)
    return rc;
}
