#include "build_factory.h"

static size_t
__read_aux_fields(rdb_parser_t *rp, nx_buf_t *b, nx_str_t *out)
{
	size_t parsed = 0, n;

	if ((n = read_string(rp, b, 0, out)) == 0) {
		return 0;
	}

	parsed += n;
	return parsed;
}

int
build_body_aux_fields(rdb_parser_t *rp, nx_buf_t *b)
{
	size_t n;
	nx_str_t aux_fields;

	rdb_node_builder_t *nb = rp->cur_nb;

	if ((n = __read_aux_fields(rp, b, &aux_fields)) == 0) {
		return RDB_PHASE_BUILD_ERROR_PREMATURE;
	}

	/* ok */
	calc_crc(rp, b, n);
	nb->ready = 1;
	return RDB_PHASE_BUILD_OK;
}