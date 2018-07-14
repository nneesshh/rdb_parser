#include "rdb_parser.h"
#include <time.h>
#include <assert.h>

#ifdef _DEBUG
#include <vld.h>
#endif

#include "node_builder.h"
#include "build_factory.h"

#define KEY_FIELD_STR      "key"
#define VAL_FIELD_STR      "val"
#define TYPE_FIELD_STR     "type"
#define EXPIRE_FIELD_STR   "expire_time"
#define DB_SELCTOR_STR     "db_selector"
#define VERSION_STR        "version"
#define MAGIC_VERSION      5

enum PARSE_RDB {
    PARSE_RDB_IDLE = 0,
    PARSE_RDB_HEADER,
    PARSE_RDB_BODY,
    PARSE_RDB_FOOTER,
    PARSE_RDB_OVER,
};

rdb_parser_t *
create_rdb_parser(size_t size)
{
    rdb_parser_t *rp = nx_alloc(sizeof(rdb_parser_t));
    nx_memzero(rp, sizeof(rdb_parser_t));

    /* NOTE: trick here, version set 5 as we want to calculate crc where read version field. */
    rp->version = MAGIC_VERSION;
    rp->chksum = 0;
    rp->parsed = 0;
    rp->state = PARSE_RDB_IDLE;

	assert(size >= 1024);
	rp->pool_size = size;
    rp->pool = nx_create_pool(size);

	rp->in_b_size = 4096;
    rp->in_b = nx_create_temp_buf(rp->pool, rp->in_b_size);

    rp->root = NULL;
	rp->available_nodes = 0;
    rp->cur_ln = NULL;

    rp->stack_nb = nx_array_create(rp->pool, 16, sizeof(rdb_node_builder_t));
    return rp;
}

void
destroy_rdb_parser(rdb_parser_t *rp)
{
    nx_destroy_pool(rp->pool);
    nx_free(rp);
}

void
reset_rdb_parser(rdb_parser_t *rp)
{
	nx_pool_t  *p, *tmp;

	rp->chksum = 0;
	rp->parsed = 0;
	rp->state = PARSE_RDB_IDLE;

	nx_reset_pool(rp->pool);

	/* shrink pool*/
	p = rp->pool->d.next;
	while(p) {
		tmp = p->d.next;
		nx_free(p);
		p = tmp;
	}
	rp->pool->d.next = NULL;

	rp->in_b = nx_create_temp_buf(rp->pool, rp->in_b_size);

	rp->root = NULL;
	rp->available_nodes = 0;
	rp->cur_ln = NULL;

	rp->stack_nb = nx_array_create(rp->pool, 16, sizeof(rdb_node_builder_t));
}

int
rdb_parse_node_val_once(rdb_parser_t *rp, nx_buf_t *b)
{
    int rc = 0;
    rdb_node_builder_t *nb;
    int depth = 0;

    /* main node builder */
    RDB_NODE_BUILDER_LOOP_BEGIN(rp, nb, depth)
    {
        rc = build_node_detail_kv_val(rp, nb, b);
    }
    RDB_NODE_BUILDER_LOOP_END(rp, rc)
        return rc;
}

int
rdb_parse_once(rdb_parser_t *rp, nx_buf_t *b)
{
    int rc;

    rc = NODE_BUILD_AGAIN;

    while (rc != NODE_BUILD_ERROR_PREMATURE
        && rp->state != PARSE_RDB_OVER) {

        switch (rp->state) {
        case PARSE_RDB_IDLE:
        case PARSE_RDB_HEADER:
            /* header */
            if ((rc = build_header(rp, b)) != NODE_BUILD_OVER) {
                break;
            }

            rp->state = PARSE_RDB_BODY;
            break;

        case PARSE_RDB_BODY:
            /* body  */
            if ((rc = build_body(rp, b)) != NODE_BUILD_OVER) {
                break;
            }

            rp->state = PARSE_RDB_FOOTER;
            break;

        case PARSE_RDB_FOOTER:
            /* body  */
            if ((rc = build_footer(rp, b)) != NODE_BUILD_OVER) {
                break;
            }

            rp->state = PARSE_RDB_OVER;
            break;

        default:
            break;
        }
    }

    return rc;
}

int
rdb_parse_file(rdb_parser_t *rp, const char *path)
{
    int rc, is_eof;

    FILE *r_fp;
    size_t bytes, reserved;
    nx_buf_t *b;

    r_fp = fopen(path, "rb");
    b = rp->in_b;

    is_eof = 0;

    while (!is_eof
        && PARSE_RDB_OVER != rp->state) {

        /* init reserved buffer */
        if (b->pos > b->start) {
            if (b->pos == b->last) {
                b->last = b->pos = b->start;
            }
            else {
                memmove(b->start, b->pos, b->last - b->pos);
                b->last -= (b->pos - b->start);
                b->pos = b->start;
            }
        }
        reserved = (b->end - b->last);
        if (reserved <= 0) {
            rc = NODE_BUILD_ERROR_PREMATURE;
            break;
        }

        /* read file into reserved buffer */
        bytes = fread(b->last, 1, reserved, r_fp);
        if (0 == bytes || bytes < reserved) {
            is_eof = 1;

            if (0 == bytes) {
                rc = NODE_BUILD_ERROR_PREMATURE;
                break;
            }
        }
        b->last += bytes;

        /* consume */
        rc = rdb_parse_once(rp, b);
    }

    fclose(r_fp);
    return rc;
}

void
rdb_dump(rdb_parser_t *rp, const char *dump_to_path)
{
    FILE *fp;

    rdb_node_t *node;
    rdb_node_chain_t *cl;
    int i = 0;

    rdb_kv_t *kv;
    rdb_kv_chain_t *kvcl;
    time_t tmstart, tmover;

    fp = fopen(dump_to_path, "w");

    fprintf(fp, "version:%d\n", rp->version);
    fprintf(fp, "chksum:%lld\n", rp->chksum);
    fprintf(fp, "bytes:%lld\n", rp->parsed);
	fprintf(fp, "nodes:%lld\n", rp->available_nodes);

    tmstart = time(NULL);
    fprintf(fp, "\n==== dump start ====\n");

    for (cl = rp->root; cl; cl = cl->next) {
        node = cl->node;
        ++i;

        switch (node->type) {
        case REDIS_NODE_TYPE_AUX_FIELDS:
            fprintf(fp, "(%d)[AUX_FIELDS] %s\n\n",
                i, node->aux_fields.data);
            break;

        case REDIS_NODE_TYPE_EXPIRE_SEC:
        case REDIS_NODE_TYPE_EXPIRE_MS:
            fprintf(fp, "(%d)[EXPIRE] %d\n\n",
                i, node->expire);
            break;

        case REDIS_NODE_TYPE_DB_SELECTOR:
            fprintf(fp, "(%d)[DB_SELECTOR] %d\n\n",
                i, node->db_selector);
            break;

        case REDIS_NODE_TYPE_STRING:
            fprintf(fp, "(%d)[STRING] %s = %s\n\n",
                i, node->key.data, node->val.data);
            break;

        case REDIS_NODE_TYPE_LIST:
            fprintf(fp, "(%d)[LIST] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;
                fprintf(fp, "\t%s,\n", kv->val.data);
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_SET:
            fprintf(fp, "(%d)[SET] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;
                fprintf(fp, "\t%s,\n", kv->val.data);
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_ZSET:
            fprintf(fp, "(%d)[ZSET] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;
                fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_HASH:
            fprintf(fp, "(%d)[HASH] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;
                fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_ZIPMAP:
            fprintf(fp, "(%d)[ZIPMAP] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;

                if (kv->key.len > 0) {
                    fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
                }
                else {
                    fprintf(fp, "\t%s,\n", kv->val.data);
                }
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_LIST_ZIPLIST:
            fprintf(fp, "(%d)[ZIPLIST] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;

                if (kv->key.len > 0) {
                    fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
                }
                else {
                    fprintf(fp, "\t%s,\n", kv->val.data);
                }
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_INTSET:
            fprintf(fp, "(%d)[INTSET] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;

                if (kv->key.len > 0) {
                    fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
                }
                else {
                    fprintf(fp, "\t%s,\n", kv->val.data);
                }
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_ZSET_ZIPLIST:
            fprintf(fp, "(%d)[ZSET_ZL] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;

                if (kv->key.len > 0) {
                    fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
                }
                else {
                    fprintf(fp, "\t%s,\n", kv->val.data);
                }
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_HASH_ZIPLIST:
            fprintf(fp, "(%d)[HASH_ZL] %s = (%d)\n[\n",
                i, node->key.data, node->size);

            for (kvcl = node->vall; kvcl; kvcl = kvcl->next) {
                kv = kvcl->kv;

                if (kv->key.len > 0) {
                    fprintf(fp, "\t%s = %s,\n", kv->key.data, kv->val.data);
                }
                else {
                    fprintf(fp, "\t%s,\n", kv->val.data);
                }
            }
            fprintf(fp, "]\n\n");
            break;

        case REDIS_NODE_TYPE_EOF:
        default:
            break;
        }
    }
    fprintf(fp, "==== dump over ====\n");

    tmover = time(NULL);
    fprintf(fp, "cost: %lld seconds", tmover - tmstart);

    fclose(fp);
}
