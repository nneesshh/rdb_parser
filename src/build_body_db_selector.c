#include "build_factory.h"

static size_t
__read_db_selector(rdb_parser_t *rp, nx_buf_t *b, uint32_t *out)
{
	size_t parsed = 0, n;
	uint32_t selector;

	if ((n = read_store_len(rp, b, 0, NULL, &selector)) == 0)
		return 0;

	parsed += n;

	(*out) = selector;
	return parsed;
}

int
build_body_db_selector(rdb_parser_t *rp, nx_buf_t *b)
{
	size_t n, bytes;
	uint32_t db_selector;

	rdb_node_builder_t *nb = rp->cur_nb;

	if ((n = __read_db_selector(rp, b, &db_selector)) == 0) {
		return RDB_PHASE_BUILD_ERROR_PREMATURE;
	}

	/* ok */
	calc_crc(rp, b, n);
	nb->ready = 1;
	return RDB_PHASE_BUILD_OK;
}