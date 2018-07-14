#include "build_factory.h"

#include "node_builder.h"

#include "endian.h"

enum BUILD_NODE_TAG {
    BUILD_NODE_IDLE = 0,
    BUILD_NODE_TYPE,
    BUILD_NODE_DETAIL,
};

static size_t
__read_db_selector(rdb_parser_t *rp, nx_buf_t *b, uint32_t *out)
{
    size_t n;
    uint32_t selector;

    if ((n = read_store_len(rp, b, NULL, &selector)) == 0)
        return 0;

    (*out) = selector;
    return n;
}

static size_t
__read_expire_time(rdb_parser_t *rp, nx_buf_t *b, int is_ms, int *out)
{
    size_t bytes = 1;
    uint8_t *ptr;
    uint32_t t32;
    uint64_t t64;

    if (is_ms) {
        /* milliseconds */
        bytes = 8;

        if (nx_buf_size(b) < bytes) {
            return 0;
        }

        ptr = b->pos;
        t64 = *(uint64_t *)ptr;
        memrev64(&t64);
        (*out) = (int)(t64 / 1000);
    }
    else {
        /* seconds */
        bytes = 4;

        if (nx_buf_size(b) < bytes) {
            return 0;
        }

        ptr = b->pos;
        t32 = *(uint32_t *)ptr;
        memrev32(&t32);
        (*out) = (int)t32;
    }

    return bytes;
}

static int __build_node_type(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b);
static int __build_node_detail(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b);

static int
__build_node_type(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    size_t n;
    uint8_t type;

    rdb_node_t *node;
    rdb_node_chain_t **ll;
    
    if ((n = read_kv_type(rp, b, &type)) == 0)
        return NODE_BUILD_ERROR_PREMATURE;

    ll = (NULL == rp->cur_ln) ? &rp->root : &rp->cur_ln;
    rp->cur_ln = alloc_rdb_node_chain_link(rp, ll);
    node = rp->cur_ln->node;
    node->type = type;

    /* ok */
    calc_crc(rp, b, n);

    /* next state */
    nb->state = BUILD_NODE_DETAIL;
    return NODE_BUILD_AGAIN;
}

static int
__build_node_detail(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b)
{
    int rc = 0;
    size_t n;
    int is_ms;

    rdb_node_t *node;

    node = rp->cur_ln->node;
    
    switch (node->type) {
    case REDIS_NODE_TYPE_AUX_FIELDS:
        /* aux fields */
        rc = build_string_value(rp, nb, b, &node->aux_fields);

        if (rc == NODE_BUILD_OVER) {
			++rp->available_nodes;

            /* next state */
            nb->state = BUILD_NODE_TYPE;
            return NODE_BUILD_AGAIN;
        }
        return rc;

    case REDIS_NODE_TYPE_DB_SELECTOR:
        /* db selector */
        if ((n = __read_db_selector(rp, b,  &node->db_selector)) == 0) {
            return NODE_BUILD_ERROR_PREMATURE;
        }

        /* ok */
        calc_crc(rp, b, n);
        
		++rp->available_nodes;

        /* next state */
        nb->state = BUILD_NODE_TYPE;
        return NODE_BUILD_AGAIN;

    case REDIS_NODE_TYPE_EXPIRE_SEC:
    case REDIS_NODE_TYPE_EXPIRE_MS:
        /* expire time */
        is_ms = (REDIS_NODE_TYPE_EXPIRE_MS == node->type);;
        if ((n = __read_expire_time(rp, b, is_ms, &node->expire)) == 0) {
            return NODE_BUILD_ERROR_PREMATURE;
        }

        /* ok */
        calc_crc(rp, b, n);

		++rp->available_nodes;
        
        /* next state */
        nb->state = BUILD_NODE_TYPE;
        return NODE_BUILD_AGAIN;

    case REDIS_NODE_TYPE_STRING:
    case REDIS_NODE_TYPE_LIST:
    case REDIS_NODE_TYPE_SET:
    case REDIS_NODE_TYPE_ZSET:
    case REDIS_NODE_TYPE_HASH:
    case REDIS_NODE_TYPE_ZIPMAP:
    case REDIS_NODE_TYPE_LIST_ZIPLIST:
    case REDIS_NODE_TYPE_INTSET:
    case REDIS_NODE_TYPE_ZSET_ZIPLIST:
    case REDIS_NODE_TYPE_HASH_ZIPLIST: {
        /* node detail kv */
        rc = build_node_detail_kv(rp, nb, b);

        if (rc == NODE_BUILD_OVER) {
			++rp->available_nodes;

            /* next state */
            nb->state = BUILD_NODE_TYPE;
            return NODE_BUILD_AGAIN;
        }
        return rc;
    }

    case REDIS_NODE_TYPE_EOF: {
        return NODE_BUILD_OVER;
    }

    default:
        return NODE_BUILD_ERROR_INVALID_NODE_TYPE;
    }

    /* premature because val is not completed yet */
    return NODE_BUILD_ERROR_PREMATURE;
}

int
build_body(rdb_parser_t *rp, nx_buf_t *b)
{
    int rc = 0;
    rdb_node_builder_t *nb;
    int depth = 0;

    /* main node builder */
    RDB_NODE_BUILDER_LOOP_BEGIN(rp, nb, depth)
    {
        /* main process */
        switch (nb->state) {
        case BUILD_NODE_IDLE:
        case BUILD_NODE_TYPE:
            rc = __build_node_type(rp, nb, b);
            break;

        case BUILD_NODE_DETAIL:
            rc = __build_node_detail(rp, nb, b);
            break;

        default:
            rc = NODE_BUILD_ERROR_INVALID_NODE_STATE;
            break;
        }

    }
    RDB_NODE_BUILDER_LOOP_END(rp, rc)
    return rc;
}

