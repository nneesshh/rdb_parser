#include "build_factory.h"

int
build_node_detail_kv_val(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    int rc = 0;

    rdb_node_t *node;

	node = rp->n;

    switch (node->type) {
    case REDIS_STRING:
        /* string */
        return build_string_value(rp, nb, bb, &node->val);

    case REDIS_LIST:
    case REDIS_SET:
        return build_list_or_set_value(rp, nb, bb, &node->vall, &node->size);

    case REDIS_ZSET:
    case REDIS_HASH:
        return build_hash_or_zset_value(rp, nb, bb, &node->vall, &node->size);

    case REDIS_HASH_ZIPMAP:
        return build_zipmap_value(rp, nb, bb, &node->vall, &node->size);

    case REDIS_LIST_ZIPLIST:
        return build_zl_list_value(rp, nb, bb, &node->vall, &node->size);

    case REDIS_SET_INTSET:
        return build_intset_value(rp, nb, bb, &node->vall, &node->size);

    case REDIS_ZSET_ZIPLIST:
    case REDIS_HASH_ZIPLIST:
        return build_zl_hash_value(rp, nb, bb, &node->vall, &node->size);

    default:
        break;
    }
    return NB_ERROR_INVALID_NB_TYPE;
}
