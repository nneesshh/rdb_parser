#include "build_factory.h"

#include "endian.h"

/* sub phase -- body kv */
enum RDB_SUBPHASE_BODY_KV {
	RDB_SUBPHASE_BODY_KV_NULL = 0,
	RDB_SUBPHASE_BODY_KV_KEY,
	RDB_SUBPHASE_BODY_KV_VAL,
};

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

static size_t
__read_body_kv_key(rdb_parser_t *rp, nx_buf_t *b, nx_str_t *out)
{
	size_t parsed = 0, n;

	if ((n = read_string(rp, b, 0, out)) == 0) {
		return 0;
	}

	parsed += n;
	return parsed;
}

int
build_body_kv(rdb_parser_t *rp, nx_buf_t *b)
{
	size_t n;
	int is_ms, expire;
	nx_str_t key;

	rdb_node_builder_t *nb = rp->cur_nb;
	rdb_node_t *node = nb->cur_ln->node;

	switch (nb->type) {
	case REDIS_NODE_TYPE_EXPIRE_SEC:
	case REDIS_NODE_TYPE_EXPIRE_MS:
		/* expire time */
		is_ms = (REDIS_NODE_TYPE_EXPIRE_MS == nb->type);;
		if ((n = __read_expire_time(rp, b, is_ms, &expire)) == 0) {
			return RDB_PHASE_BUILD_ERROR_PREMATURE;
		}

		node->expire = expire;

		/* ok */
		calc_crc(rp, b, n);
		return RDB_PHASE_BUILD_OK;

	case REDIS_NODE_TYPE_STRING:
	case REDIS_NODE_TYPE_INTSET:
	case REDIS_NODE_TYPE_LIST_ZIPLIST:
	case REDIS_NODE_TYPE_ZIPMAP:
	case REDIS_NODE_TYPE_ZSET_ZIPLIST:
	case REDIS_NODE_TYPE_HASH_ZIPLIST:
	case REDIS_NODE_TYPE_LIST:
	case REDIS_NODE_TYPE_SET:
	case REDIS_NODE_TYPE_HASH:
	case REDIS_NODE_TYPE_ZSET: {
		for (;;) {
			/* key-val */
			switch (nb->state) {
			case RDB_SUBPHASE_BODY_KV_KEY:
				/* key */
				if ((n = __read_body_kv_key(rp, b, &key)) == 0) {
					return RDB_PHASE_BUILD_ERROR_PREMATURE;
				}

				nx_str_set2(&node->key, key.data, key.len);

				/* ok */
				calc_crc(rp, b, n);
				nb->state = RDB_SUBPHASE_BODY_KV_VAL;
				break;

			case RDB_SUBPHASE_BODY_KV_VAL:
				/* val */
				return build_body_kv_load_value(rp, b);

			default:
				nb->state = RDB_SUBPHASE_BODY_KV_KEY;
				break;
			}
		}

		break;
	}

	default:
		return RDB_PHASE_BUILD_ERROR_INVALID_NODE_TYPE;
	}


	/* premature because val is not completed yet */
	return RDB_PHASE_BUILD_ERROR_PREMATURE;
}


int
build_body_kv_val(rdb_parser_t *rp, nx_buf_t *b)
{
	size_t n, bytes;
	rdb_node_chain_t *ln;

	rdb_node_builder_t *nb = rp->cur_nb;

	size_t offset = 0;
    /* value */
	/* 
	if ((n = __load_value(rp, b, offset + parsed, type, ln->node)) == 0) {
		return 0;
	}

	parsed += n;

	return parsed;


	uint32_t db_selector;

	if ((n = __read_db_selector(rp, b, &db_selector)) == 0) {
		return RDB_PHASE_BUILD_ERROR_PREMATURE;
	}

	bytes = n;*/

	/* ok */
	calc_crc(rp, b, bytes);
	nb->ready = 1;
	return RDB_PHASE_BUILD_OK;
}