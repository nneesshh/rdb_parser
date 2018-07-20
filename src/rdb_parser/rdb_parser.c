#include "rdb_parser.h"
#include <time.h>
#include <assert.h>

#ifdef _DEBUG
#include <vld.h>
#endif

#include "rdb_node_builder.h"
#include "build_factory.h"

#define MAGIC_VERSION                 5

#define RDB_PARSER_INPUT_BIPBUF_SIZE  4096
#define RDB_PARSER_POOL_SIZE          1024
#define RDB_PARSER_NODE_POOL_SIZE    4096

enum PARSE_RDB {
    PARSE_RDB_IDLE = 0,
    PARSE_RDB_HEADER,
    PARSE_RDB_BODY,
    PARSE_RDB_FOOTER,
    PARSE_RDB_OVER,
};

rdb_parser_t *
create_rdb_parser(func_process_rdb_node cb, void *payload)
{
    rdb_parser_t *rp = nx_alloc(sizeof(rdb_parser_t));
    nx_memzero(rp, sizeof(rdb_parser_t));

    /* NOTE: trick here, version set 5 as we want to calculate crc where read version field. */
    rp->version = MAGIC_VERSION;
    rp->chksum = 0;
    rp->parsed = 0;
    rp->state = PARSE_RDB_IDLE;

	rp->in_bb = bip_buf_create(RDB_PARSER_INPUT_BIPBUF_SIZE);
    rp->pool = nx_create_pool(RDB_PARSER_POOL_SIZE);

	rp->stack_nb = nx_array_create(rp->pool, 16, sizeof(rdb_node_builder_t));

	rp->n = nx_palloc(rp->pool, sizeof(rdb_node_t));
	rdb_node_init(rp->n);

	rp->n_pool = nx_create_pool(RDB_PARSER_NODE_POOL_SIZE);
	rp->n_cb = cb;
	rp->n_payload = payload;

    return rp;
}

void
destroy_rdb_parser(rdb_parser_t *rp)
{
    bip_buf_destroy(rp->in_bb);
	nx_destroy_pool(rp->n_pool);
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

	/* reset and shrink node pool*/
	nx_reset_pool(rp->n_pool);
	p = rp->n_pool->d.next;
	while (p) {
		tmp = p->d.next;
		nx_free(p);
		p = tmp;
	}
	rp->n_pool->d.next = NULL;

	/* reset and shrink parser pool*/
	nx_reset_pool(rp->pool);
	p = rp->pool->d.next;
	while (p) {
		tmp = p->d.next;
		nx_free(p);
		p = tmp;
	}
	rp->pool->d.next = NULL;

    bip_buf_reset(rp->in_bb);

    rp->stack_nb = nx_array_create(rp->pool, 16, sizeof(rdb_node_builder_t));

	rp->n = nx_palloc(rp->pool, sizeof(rdb_node_t));
	rdb_node_init(rp->n);
}

int
rdb_node_parse_once(rdb_parser_t *rp, bip_buf_t *bb)
{
    int rc;

    rc = NB_AGAIN;

    while (rp->state != PARSE_RDB_OVER
        && (rc == NB_AGAIN || rc == NB_OVER)) {

        switch (rp->state) {
        case PARSE_RDB_IDLE:
        case PARSE_RDB_HEADER:
            /* header */
            if ((rc = build_header(rp, bb)) != NB_OVER) {
                break;
            }

            rp->state = PARSE_RDB_BODY;
            break;

        case PARSE_RDB_BODY:
            /* body */
            if ((rc = build_body(rp, bb)) != NB_OVER) {
                break;
            }

            rp->state = PARSE_RDB_FOOTER;
            break;

        case PARSE_RDB_FOOTER:
            /* footer */
            if ((rc = build_footer(rp, bb)) != NB_OVER) {
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
rdb_dumped_data_parse_once(rdb_parser_t *rp, bip_buf_t *bb)
{
	int rc;

	rc = NB_AGAIN;

	while (rc != NB_ERROR_PREMATURE
		&& rp->state != PARSE_RDB_OVER) {

		rc = build_node_by_dumped_data(rp, bb);
	}
	return rc;
}

int
rdb_parse_file(rdb_parser_t *rp, const char *path)
{
    int rc, is_eof;

    FILE *r_fp;
    size_t bytes, reserved;
    bip_buf_t *bb;
    uint8_t *ptr, *ptr_r;

    r_fp = fopen(path, "rb");
    bb = rp->in_bb;

    ptr = bip_buf_get_contiguous_block(bb);

	rc = NB_AGAIN;
	is_eof = 0;

    while (!is_eof
        && PARSE_RDB_OVER != rp->state) {

        /* init reserved buffer */
        reserved = 0;
        ptr_r = bip_buf_reserve(bb, &reserved);
        if (NULL == ptr_r) {
            rc = NB_ERROR_PREMATURE;
            break;
        }

        /* read file into reserved buffer */
        reserved = bip_buf_get_reservation_size(bb);
        bytes = fread(ptr_r, 1, reserved, r_fp);
        if (0 == bytes || bytes < reserved) {
            is_eof = 1;

            if (0 == bytes) {
                rc = NB_ERROR_PREMATURE;
                break;
            }
        }
        bip_buf_commit(bb, bytes);

        /* consume */
        rc = rdb_node_parse_once(rp, bb);
    }

    fclose(r_fp);
    return rc;
}
