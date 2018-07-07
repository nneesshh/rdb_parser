#include "node_handlers.h"

#define REDIS_RDB_PARSE_OK                           0
#define REDIS_RDB_PARSE_ERROR_INVALID_PATH          -1
#define REDIS_RDB_PARSE_ERROR_PREMATURE             -2
#define REDIS_RDB_PARSE_ERROR_INVALID_MAGIC_STRING  -3

#define MAGIC_STR  "REDIS"


static size_t
__calc_crc(rdb_parser_t *parser, nx_buf_t *b, size_t bytes)
{
	if (nx_buf_size(b) >= bytes) {
		if (parser->version >= 5) {
			parser->chksum = crc64(parser->chksum, b->pos, bytes);
		}

		b->pos += bytes;
		parser->parsed += bytes;
		return bytes;
	}
	return 0;
}


int
rdb_node_header_handler_consume(rdb_parser_t *parser, nx_buf_t *b) {
	int rc = 0;
	char chversion[5];

	size_t bytes;

	bytes = nx_buf_size(b);

	/* magic string(5bytes) and version(4bytes) */
	if (bytes < 9) {
		rc = REDIS_RDB_PARSE_ERROR_PREMATURE;
	}

	if (memcmp(b->pos, MAGIC_STR, 5) != 0)
		rc = REDIS_RDB_PARSE_ERROR_INVALID_MAGIC_STRING;

	nx_memcpy(chversion, b->pos + 5, 4);
	chversion[4] = '\0';
	parser->version = atoi(chversion);

	__calc_crc(parser, b, 9);
	return REDIS_RDB_PARSE_OK;
}