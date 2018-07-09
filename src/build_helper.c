#include "build_helper.h"

#include <stdio.h>
#include <string.h>

#include "lzf.h"
#include "crc64.h"
#include "endian.h"

#define REDIS_RDB_6B    0
#define REDIS_RDB_14B   1
#define REDIS_RDB_32B   2
#define REDIS_RDB_ENCV  3

#define REDIS_RDB_ENC_INT8   0
#define REDIS_RDB_ENC_INT16  1
#define REDIS_RDB_ENC_INT32  2
#define REDIS_RDB_ENC_LZF    3

size_t
calc_crc(rdb_parser_t *rp, nx_buf_t *b, size_t bytes)
{
	if (nx_buf_size(b) >= bytes) {
		if (rp->version >= 5) {
			rp->chksum = crc64(rp->chksum, b->pos, bytes);
		}

		b->pos += bytes;
		rp->parsed += bytes;
		return bytes;
	}
	return 0;
}

size_t
read_kv_type(rdb_parser_t *rp, nx_buf_t *b, uint8_t *out)
{
	size_t bytes = 1;
	uint8_t *ptr;

	if (nx_buf_size(b) < bytes) {
		return 0;
	}

	ptr = b->pos;

	(*out) = (*ptr);
	return bytes;
}

size_t
read_store_len(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t *is_encoded, uint32_t *out)
{
	size_t bytes = 1;
	uint8_t *p;
	uint8_t type;

	if (nx_buf_size(b) < offset + bytes) {
		return 0;
	}

	p = b->pos + offset;

	if (is_encoded)
		*is_encoded = 0;

	type = ((*p) & 0xc0) >> 6;

	/**
	* 00xxxxxx, then the next 6 bits represent the length
	* 01xxxxxx, then the next 14 bits represent the length
	* 10xxxxxx, then the next 6 bits is discarded, and next 4bytes represent the length(BigEndian)
	* 11xxxxxx, The remaining 6 bits indicate the format
	*/
	if (REDIS_RDB_6B == type) {
		(*out) = (*p) & 0x3f;
	}
	else if (REDIS_RDB_14B == type) {

		bytes = 2;

		if (nx_buf_size(b) < offset + bytes) {
			return 0;
		}

		(*out) = (((*p) & 0x3f) << 8) | (*(p + 1));
	}
	else if (REDIS_RDB_32B == type) {

		bytes = 5;

		if (nx_buf_size(b) < offset + bytes) {
			return 0;
		}

		memrev32(p + 1);
		(*out) = (*(uint32_t *)(p + 1));
	}
	else {
		if (is_encoded)
			*is_encoded = 1;

		(*out) = (*p) & 0x3f;
	}
	return bytes;
}


size_t
read_lzf_string(rdb_parser_t *parser, nx_buf_t *b, size_t offset, nx_str_t *out)
{
	size_t parsed = 0, n;
	uint32_t clen, len;
	char *cstr, *str;
	int ret;

	/*
	* 1. load compress length.
	* 2. load raw length.
	* 3. load lzf_string, and use lzf_decompress to decode.
	*/

	if ((n = read_store_len(parser, b, offset, NULL, &clen)) == 0)
		return 0;

	parsed += n;

	if ((n = read_store_len(parser, b, offset + parsed, NULL, &len)) == 0)
		return 0;

	parsed += n;

	if (nx_buf_size(b) < offset + parsed + clen)
		return 0;

	cstr = b->pos + offset + parsed;
	str = nx_palloc(parser->pool, len + 1);
	if ((ret = lzf_decompress(cstr, clen, str, len)) == 0)
		goto FAILED;

	str[len] = '\0';
	nx_str_set2(out, str, len);

	parsed += clen;
	return parsed;

FAILED:
	nx_pfree(parser->pool, str);
	return 0;
}

size_t
read_int(rdb_parser_t *parser, nx_buf_t *b, size_t offset, uint8_t enc, int32_t *out)
{
	size_t bytes = 1;
	uint8_t *p;

	if (nx_buf_size(b) < offset + bytes) {
		return 0;
	}

	p = b->pos + offset;

	if (REDIS_RDB_ENC_INT8 == enc) {
		(*out) = (*p);
	}
	else if (REDIS_RDB_ENC_INT16 == enc) {

		bytes = 2;

		if (nx_buf_size(b) < offset + bytes) {
			return 0;
		}

		(*out) = (int16_t)((*p) | (*(p + 1)) << 8);
	}
	else {
		bytes = 4;

		if (nx_buf_size(b) < offset + bytes) {
			return 0;
		}

		(*out) = (int32_t)((*p) | ((*(p + 1)) << 8) | ((*(p + 2)) << 16) | ((*(p + 3)) << 24));
	}
	return bytes;
}

size_t
read_string(rdb_parser_t *parser, nx_buf_t *b, size_t offset, nx_str_t *out)
{
	size_t parsed = 0, n;
	uint8_t is_encoded;
	uint32_t len;
	char *str;
	int32_t enc_len;

	if ((n = read_store_len(parser, b, offset, &is_encoded, &len)) == 0)
		return 0;

	parsed += n;

	if (is_encoded) {
		switch (len) {

		case REDIS_RDB_ENC_INT8:
		case REDIS_RDB_ENC_INT16:
		case REDIS_RDB_ENC_INT32:
			if ((n = read_int(parser, b, offset + parsed, (uint8_t)len, &enc_len)) == 0)
				return 0;

			str = nx_palloc(parser->pool, 30);
			sprintf(str, "%ld", enc_len);
			nx_str_set2(out, str, strlen(str));

			parsed += n;
			break;

		case REDIS_RDB_ENC_LZF:
			if ((n = read_lzf_string(parser, b, offset + parsed, out)) == 0)
				return 0;

			parsed += n;
			break;

		default:
			return 0;
		}
	}
	else {

		if (nx_buf_size(b) < offset + len) {
			return 0;
		}

		str = nx_palloc(parser->pool, len + 1);
		nx_memcpy(str, b->pos + offset + parsed, len);
		str[len] = '\0';
		nx_str_set2(out, str, len);

		parsed += len;
	}
	return parsed;
}