#include "build_factory.h"

#include "../endian.h"
#include "rdb_parser.h"

enum BUILD_NODE_TAG {
    BUILD_NODE_IDLE = 0,
    BUILD_NODE_TYPE,
    BUILD_NODE_DETAIL,
};

static size_t
__read_db_selector(rdb_parser_t *rp, bip_buf_t *bb, uint32_t *out)
{
    size_t n;
    uint32_t selector;

    if ((n = rdb_node_read_store_len(rp, bb, NULL, &selector)) == 0)
        return 0;

    (*out) = selector;
    return n;
}

static size_t
__read_expire_time(rdb_parser_t *rp, bip_buf_t *bb, int is_ms, int *out)
{
    size_t bytes = 1;
    uint8_t *ptr;
    uint32_t t32;
    uint64_t t64;

    if (is_ms) {
        /* milliseconds */
        bytes = 8;

        if (bip_buf_get_committed_size(bb) < bytes) {
            return 0;
        }

        ptr = bip_buf_get_contiguous_block(bb);
        t64 = *(uint64_t *)ptr;
        memrev64(&t64);
        (*out) = (int)(t64 / 1000);
    }
    else {
        /* seconds */
        bytes = 4;

        if (bip_buf_get_committed_size(bb) < bytes) {
            return 0;
        }

        ptr = bip_buf_get_contiguous_block(bb);
        t32 = *(uint32_t *)ptr;
        memrev32(&t32);
        (*out) = (int)t32;
    }

    return bytes;
}

static size_t
__read_kv_type(rdb_parser_t *rp, bip_buf_t *bb, uint8_t *out)
{
    size_t bytes = 1;
    uint8_t *ptr;

    if (bip_buf_get_committed_size(bb) < bytes) {
        return 0;
    }

    ptr = bip_buf_get_contiguous_block(bb);

    (*out) = (*ptr);
    return bytes;
}

static int __build_node_type(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);
static int __build_node_detail(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);

static int
__build_node_type(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    size_t n;
    uint8_t type;

    rdb_node_t *node;
    
    if ((n = __read_kv_type(rp, bb, &type)) == 0)
        return NB_ERROR_PREMATURE;

    node = rp->n;
    node->type = type;

    /* ok */
    rdb_node_calc_crc(rp, bb, n);

    /* next state */
    nb->state = BUILD_NODE_DETAIL;
    return NB_AGAIN;
}

static int
__build_node_detail(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    int rc = 0;
    size_t n;
    int is_ms;

    rdb_node_t *node;

	node = rp->n;
    
    switch (node->type) {
    case REDIS_AUX_FIELDS:
        /* aux fields */
        rc = build_string_value(rp, nb, bb, &node->aux_fields);

        if (rc == NB_OVER) {
            /* process reply */
            if (0 == rp->n_cb(rp->n, rp->n_payload)) {
                rdb_node_clear(rp);

                /* next state */
                nb->state = BUILD_NODE_TYPE;
                return NB_AGAIN;
            }
            else {
                rc = NB_ABORT;
            }
        }
        return rc;

    case REDIS_SELECTDB:
        /* db selector */
        if ((n = __read_db_selector(rp, bb,  &node->db_selector)) == 0) {
            return NB_ERROR_PREMATURE;
        }

        /* ok */
        rdb_node_calc_crc(rp, bb, n);
        
        /* process reply */
        if (0 == rp->n_cb(rp->n, rp->n_payload)) {
            rdb_node_clear(rp);

            /* next state */
            nb->state = BUILD_NODE_TYPE;
            return NB_AGAIN;
        }
        else {
            rc = NB_ABORT;
        }
        return rc;

    case REDIS_EXPIRETIME:
    case REDIS_EXPIRETIME_MS:
        /* expire time */
        is_ms = (REDIS_EXPIRETIME_MS == node->type);;
        if ((n = __read_expire_time(rp, bb, is_ms, &node->expire)) == 0) {
            return NB_ERROR_PREMATURE;
        }

        /* ok */
        rdb_node_calc_crc(rp, bb, n);

		/* process reply */
		if (0 == rp->n_cb(rp->n, rp->n_payload)) {
			rdb_node_clear(rp);

			/* next state */
			nb->state = BUILD_NODE_TYPE;
			return NB_AGAIN;
		}
		else {
			rc = NB_ABORT;
		}
		return rc;

    case REDIS_STRING:
    case REDIS_LIST:
    case REDIS_SET:
    case REDIS_ZSET:
    case REDIS_HASH:
    case REDIS_HASH_ZIPMAP:
    case REDIS_LIST_ZIPLIST:
    case REDIS_SET_INTSET:
    case REDIS_ZSET_ZIPLIST:
    case REDIS_HASH_ZIPLIST: {
        /* node detail kv */
        rc = build_node_detail_kv(rp, nb, bb);

        if (rc == NB_OVER) {
			/* process reply */
			if (0 == rp->n_cb(rp->n, rp->n_payload)) {
				rdb_node_clear(rp);

				/* next state */
				nb->state = BUILD_NODE_TYPE;
				return NB_AGAIN;
			}
			else {
				rc = NB_ABORT;
			}
        }
        return rc;
    }

    case REDIS_EOF: {
        return NB_OVER;
    }

    default:
        return NB_ERROR_INVALID_NB_TYPE;
    }

    /* premature because val is not completed yet */
    return NB_ERROR_PREMATURE;
}

int
build_body(rdb_parser_t *rp, bip_buf_t *bb)
{
    int rc = 0;
    rdb_node_builder_t *nb;
    int depth = 0;

    /* main builder */
    NB_LOOP_BEGIN(rp, nb, depth)
    {
        /* main process */
        switch (nb->state) {
        case BUILD_NODE_IDLE:
        case BUILD_NODE_TYPE:
            rc = __build_node_type(rp, nb, bb);
            break;

        case BUILD_NODE_DETAIL:
            rc = __build_node_detail(rp, nb, bb);
            break;

        default:
            rc = NB_ERROR_INVALID_NB_STATE;
            break;
        }

    }
    NB_LOOP_END(rp, rc)
    return rc;
}

