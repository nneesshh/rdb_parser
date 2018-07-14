#include "build_factory.h"

int
build_node_detail_kv_val(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    int rc = 0;
    rdb_node_t *node;

    node = rp->cur_ln->node;

    switch (node->type) {
    case REDIS_NODE_TYPE_STRING:
        /* string */
        return build_string_value(rp, nb, b, &node->val);

    case REDIS_NODE_TYPE_LIST:
    case REDIS_NODE_TYPE_SET:
        return build_list_or_set_value(rp, nb, b, &node->vall, &node->size);

    case REDIS_NODE_TYPE_ZSET:
    case REDIS_NODE_TYPE_HASH:
        return build_hash_or_zset_value(rp, nb, b, &node->vall, &node->size);

    case REDIS_NODE_TYPE_ZIPMAP:
        return build_zipmap_value(rp, nb, b, &node->vall, &node->size);

    case REDIS_NODE_TYPE_LIST_ZIPLIST:
        return build_zl_list_value(rp, nb, b, &node->vall, &node->size);

    case REDIS_NODE_TYPE_INTSET:
        return build_intset_value(rp, nb, b, &node->vall, &node->size);

    case REDIS_NODE_TYPE_ZSET_ZIPLIST:
    case REDIS_NODE_TYPE_HASH_ZIPLIST:
        return build_zl_hash_value(rp, nb, b, &node->vall, &node->size);

    default:
        break;
    }
    return NODE_BUILD_ERROR_INVALID_NODE_TYPE;
}
