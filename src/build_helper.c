#include "build_helper.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "lzf.h"
#include "crc64.h"
#include "endian.h"

#define REDIS_RDB_6B    0
#define REDIS_RDB_14B   1
#define REDIS_RDB_32B   2
#define REDIS_RDB_ENCV  3

rdb_kv_chain_t *
alloc_rdb_kv_chain_link(rdb_parser_t *rp, rdb_kv_chain_t **ll)
{
    rdb_kv_chain_t *kvcl, *ln;

    kvcl = nx_palloc(rp->pool, sizeof(rdb_kv_chain_t));
    if (kvcl == NULL) {
        return NULL;
    }

    nx_memzero(kvcl, sizeof(rdb_kv_chain_t));
    if ((*ll) == NULL) {
        (*ll) = kvcl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = kvcl;
    }

    kvcl->kv = nx_palloc(rp->pool, sizeof(rdb_kv_t));
    if (kvcl->kv == NULL) {
        nx_pfree(rp->pool, kvcl);
        return NULL;
    }

    nx_memzero(kvcl->kv, sizeof(rdb_kv_t));
    return kvcl;
}

rdb_node_chain_t *
alloc_rdb_node_chain_link(rdb_parser_t *rp, rdb_node_chain_t **ll)
{
    rdb_node_chain_t *cl, *ln;

    cl = nx_palloc(rp->pool, sizeof(rdb_node_chain_t));
    if (cl == NULL) {
        return NULL;
    }

    nx_memzero(cl, sizeof(rdb_node_chain_t));
    if ((*ll) == NULL) {
        (*ll) = cl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = cl;
    }

    cl->node = nx_palloc(rp->pool, sizeof(rdb_node_t));
    if (cl->node == NULL) {
        nx_pfree(rp->pool, cl);
        return NULL;
    }

    nx_memzero(cl->node, sizeof(rdb_node_t));
    cl->node->expire = -1;
    cl->next = NULL;
    return cl;
}

size_t
calc_crc(rdb_parser_t *rp, nx_buf_t *b, size_t bytes)
{
    assert(nx_buf_size(b) >= bytes);

#ifdef CHECK_CRC
    if (rp->version >= CHECKSUM_VERSION_MIN) {
        rp->chksum = crc64(rp->chksum, b->pos, bytes);
    }
#endif

    b->pos += bytes;
    rp->parsed += bytes;
    return bytes;
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
read_store_len(rdb_parser_t *rp, nx_buf_t *b, uint8_t *is_encoded, uint32_t *out)
{
    size_t bytes = 1;
    uint8_t *p;
    uint8_t type;
    uint32_t v32;

    if (nx_buf_size(b) < bytes) {
        return 0;
    }

    p = b->pos;

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

        if (nx_buf_size(b) < bytes) {
            return 0;
        }

        (*out) = (((*p) & 0x3f) << 8) | (*(p + 1));
    }
    else if (REDIS_RDB_32B == type) {

        bytes = 5;

        if (nx_buf_size(b) < bytes) {
            return 0;
        }

        memcpy(&v32, p + 1, 4);
        memrev32(&v32);
        (*out) = v32;
    }
    else {
        if (is_encoded)
            *is_encoded = 1;

        (*out) = (*p) & 0x3f;
    }
    return bytes;
}

size_t
read_int(rdb_parser_t *rp, nx_buf_t *b, uint8_t enc, int32_t *out)
{
    size_t bytes = 1;
    uint8_t *p;

    if (nx_buf_size(b) < bytes) {
        return 0;
    }

    p = b->pos;

    if (REDIS_STR_ENC_INT8 == enc) {
        (*out) = (*p);
    }
    else if (REDIS_STR_ENC_INT16 == enc) {

        bytes = 2;

        if (nx_buf_size(b) < bytes) {
            return 0;
        }

        (*out) = (int16_t)((*p) | (*(p + 1)) << 8);
    }
    else {
        bytes = 4;

        if (nx_buf_size(b) < bytes) {
            return 0;
        }

        (*out) = (int32_t)((*p) | ((*(p + 1)) << 8) | ((*(p + 2)) << 16) | ((*(p + 3)) << 24));
    }
    return bytes;
}
