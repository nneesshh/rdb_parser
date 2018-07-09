#include "build_factory.h"

static size_t
__read_checksum(rdb_parser_t *rp, nx_buf_t *b, uint64_t *out)
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
build_footer(rdb_parser_t *rp, nx_buf_t *b)
{
	size_t n, bytes;
	uint64_t checksum;

	if ((n = __read_checksum(rp, b, &checksum)) == 0) {
		return RDB_PHASE_BUILD_ERROR_PREMATURE;
	}

	bytes = n;

	/* ok */
	calc_crc(rp, b, bytes);
	rp->cur_nb->ready = 1;
	return RDB_PHASE_BUILD_OK;
}