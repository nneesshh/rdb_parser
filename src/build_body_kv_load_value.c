#include "build_factory.h"

#include <stdio.h>
#include <string.h>

#include "intset.h"
#include "ziplist.h"
#include "zipmap.h"

/* sub phase2 -- body kv val */
enum RDB_SUBPHASE_BODY_KV {
	RDB_SUBPHASE2_BODY_KV_VAL_NULL = 0,
	RDB_SUBPHASE2_BODY_KV_VAL_TYPE,
	RDB_SUBPHASE2_BODY_KV_VAL,
};

static size_t
__load_str_value(rdb_parser_t *rp, nx_buf_t *b, nx_str_t *val)
{
	size_t parsed = 0, n;

	if ((n = read_string(rp, b, parsed, val)) == 0) {
		return 0;
	}

	parsed += n;
	return parsed;
}

static size_t
__load_intset_value(rdb_parser_t *rp, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
	size_t parsed = 0, n;
	nx_str_t str;
	uint32_t i;
	int64_t v64;
	char *s64;
	intset_t *is;

	rdb_kv_chain_t *ln, **ll;

	ll = vall;

	if ((n = read_string(rp, b, parsed, &str)) == 0) {
		return 0;
	}

	parsed += n;
	is = (intset_t*)str.data;

	for (i = 0; i < is->length; ++i) {
		intset_get(is, i, &v64);

		ln = alloc_rdb_kv_chain_link(rp, ll);

		s64 = nx_palloc(rp->pool, 30);
		sprintf(s64, "%lld", v64);
		nx_str_set2(&ln->kv->val, s64, strlen(s64));

		ll = &ln;
	}

	(*size) = is->length;
	nx_pfree(rp->pool, str.data);
	return parsed;
}

static size_t
__load_zllist_value(rdb_parser_t *rp, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
	size_t parsed = 0, n;
	nx_str_t str;

	if ((n = read_string(rp, b, parsed, &str)) == 0) {
		return 0;
	}

	parsed += n;

	load_ziplist_list_or_set(rp, str.data, vall, size);
	nx_pfree(rp->pool, str.data);
	return parsed;
}

static size_t
__load_zipmap_value(rdb_parser_t *rp, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
	size_t parsed = 0, n;
	nx_str_t str;

	if ((n = read_string(rp, b, parsed, &str)) == 0) {
		return 0;
	}

	parsed += n;

	load_zipmap(rp, str.data, vall, size);
	nx_pfree(rp->pool, str.data);
	return parsed;
}

static size_t
__load_ziplist_value(rdb_parser_t *rp, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
	size_t parsed = 0, n;
	nx_str_t str;

	if ((n = read_string(rp, b, parsed, &str)) == 0) {
		return 0;
	}

	parsed += n;

	load_ziplist_hash_or_zset(rp, str.data, vall, size);
	nx_pfree(rp->pool, str.data);
	return parsed;
}

static size_t
__load_list_or_set_value(rdb_parser_t *rp, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
	size_t parsed = 0, n;
	uint32_t i, len;
	nx_str_t str;

	rdb_kv_chain_t *ln, **ll;

	ll = vall;

	if ((n = read_store_len(rp, b, parsed, NULL, &len)) == 0)
		return 0;

	parsed += n;

	for (i = 0; i < len; i++) {

		if ((n = read_string(rp, b, parsed, &str)) == 0) {
			return 0;
		}

		parsed += n;

		ln = alloc_rdb_kv_chain_link(rp, ll);
		nx_str_set2(&ln->kv->val, str.data, str.len);
		ll = &ln;
	}

	(*size) = len;
	return parsed;
}

static size_t
__load_hash_or_zset_value(rdb_parser_t *rp, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
	size_t parsed = 0, n;
	uint32_t i, len;
	nx_str_t key, val;

	rdb_kv_chain_t *ln, **ll;

	ll = vall;

	if ((n = read_store_len(rp, b, parsed, NULL, &len)) == 0)
		return 0;

	parsed += n;

	for (i = 0; i < len; i++) {

		if ((n = read_string(rp, b, parsed, &key)) == 0) {
			return 0;
		}

		parsed += n;

		if ((n = read_string(rp, b, parsed, &val)) == 0) {
			return 0;
		}

		parsed += n;

		ln = alloc_rdb_kv_chain_link(rp, ll);
		nx_str_set2(&ln->kv->key, key.data, key.len);
		nx_str_set2(&ln->kv->val, val.data, val.len);
		ll = &ln;
	}

	(*size) = len;
	return parsed;
}

int
build_body_kv_load_value(rdb_parser_t *rp, nx_buf_t *b)
{
	rdb_node_builder_t *nb = rp->cur_nb;
	rdb_node_t *node = nb->cur_ln->node;

	switch (node->type) {
	case REDIS_NODE_TYPE_STRING:
		return __load_str_value(rp, b, &node->val);

	case REDIS_NODE_TYPE_INTSET:
		return __load_intset_value(rp, b, &node->vall, &node->size);

	case REDIS_NODE_TYPE_LIST_ZIPLIST:
		return __load_zllist_value(rp, b, &node->vall, &node->size);

	case REDIS_NODE_TYPE_ZIPMAP:
		return __load_zipmap_value(rp, b, &node->vall, &node->size);

	case REDIS_NODE_TYPE_ZSET_ZIPLIST:
	case REDIS_NODE_TYPE_HASH_ZIPLIST:
		return __load_ziplist_value(rp, b, &node->vall, &node->size);

	case REDIS_NODE_TYPE_LIST:
	case REDIS_NODE_TYPE_SET:
		return __load_list_or_set_value(rp, b, &node->vall, &node->size);

	case REDIS_NODE_TYPE_HASH:
	case REDIS_NODE_TYPE_ZSET:
		return __load_hash_or_zset_value(rp, b, &node->vall, &node->size);

	default:
		break;
	}
	return 0;
}