#include "build_factory.h"

#include <stdlib.h>
#include <memory.h>

#define MAGIC_STR  "REDIS"

int
build_header(rdb_parser_t *rp, nx_buf_t *b)
{
	char chversion[5];
	size_t bytes;

	rdb_node_builder_t *nb = rp->cur_nb;

	/* magic string(5bytes) and version(4bytes) */
	bytes = 9;

	if (nx_buf_size(b) < bytes) {
		return RDB_PHASE_BUILD_ERROR_PREMATURE;
	}

	if (memcmp(b->pos, MAGIC_STR, 5) != 0)
		return RDB_PHASE_BUILD_ERROR_INVALID_MAGIC_STRING;

	nx_memcpy(chversion, b->pos + 5, 4);
	chversion[4] = '\0';
	rp->version = atoi(chversion);

	/* ok */
	calc_crc(rp, b, bytes);
	nb->ready = 1;
	return RDB_PHASE_BUILD_OK;
}