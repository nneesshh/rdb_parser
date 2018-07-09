#include "rdb_parser.h"

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>

#include "lzf.h"
#include "intset.h"
#include "ziplist.h"
#include "zipmap.h"
#include "crc64.h"
#include "endian.h"

#include "build_factory.h"

#define KEY_FIELD_STR      "key"
#define VAL_FIELD_STR      "val"
#define TYPE_FIELD_STR     "type"
#define EXPIRE_FIELD_STR   "expire_time"
#define DB_SELCTOR_STR     "db_selector"
#define VERSION_STR        "version"
#define MAGIC_VERSION      5

rdb_node_builder_chain_t *
alloc_node_builder_chain_link(rdb_parser_t *rp, rdb_node_builder_chain_t **ll)
{
	rdb_node_builder_chain_t *cl, *ln;

    if (rp->nbl_free) {
        cl = rp->nbl_free;
        rp->nbl_free = cl->next;
    }
    else {
		cl = nx_palloc(rp->pool, sizeof(rdb_node_builder_chain_t));
		if (cl == NULL) {
			return NULL;
		}
    }

    if ((*ll) == NULL) {
        (*ll) = cl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = cl;
    }

	cl->nb = nx_palloc(rp->pool, sizeof(rdb_node_builder_t));
	nx_memzero(cl->nb, sizeof(rdb_node_builder_t));
    cl->next = NULL;
    return cl;
}


rdb_kv_chain_t *
alloc_rdb_kv_chain_link(rdb_parser_t *rp, rdb_kv_chain_t **ll)
{
    rdb_kv_chain_t *cl, *ln;

    cl = nx_palloc(rp->pool, sizeof(rdb_kv_chain_t));
    if (cl == NULL) {
        return NULL;
    }

    if ((*ll) == NULL) {
        (*ll) = cl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = cl;
    }

    cl->kv = nx_palloc(rp->pool, sizeof(rdb_kv_t));
    nx_str_null(&cl->kv->key);
    nx_str_null(&cl->kv->val);
    cl->next = NULL;
    return cl;
}


rdb_node_chain_t *
alloc_rdb_node_chain_link(rdb_parser_t *rp, rdb_node_chain_t **ll)
{
    rdb_node_chain_t *cl, *ln;

    cl = nx_palloc(rp->pool, sizeof(rdb_node_chain_t));
    if (cl == NULL) {
        return NULL;
    }

    if ((*ll) == NULL) {
        (*ll) = cl;
    }
    else {
        for (ln = (*ll); ln->next; ln = ln->next) { /* void */ }

        ln->next = cl;
    }

    cl->node = nx_palloc(rp->pool, sizeof(rdb_node_t));
    cl->node->expire = -1;
    cl->node->type = 0;
    nx_str_null(&cl->node->key);
    nx_str_null(&cl->node->val);
    cl->node->vall = NULL;
    cl->node->size = 0;
    cl->next = NULL;

    return cl;
}


rdb_parser_t *
create_rdb_parser(size_t size)
{
    rdb_parser_t *rp = nx_alloc(sizeof(rdb_parser_t));

    /* NOTE: trick here, version set 5 as we want to calculate crc where read version field. */
    rp->version = MAGIC_VERSION;
    rp->chksum = 0;
    rp->parsed = 0;

    rp->pool = nx_create_pool(size);
    rp->in_b = nx_create_temp_buf(rp->pool, size);

    rp->root = NULL;

	rp->nbl_free = NULL;
	rp->nbl_busy = NULL;
	rp->nbl_nbusy = 0;

	rp->cur_nb = NULL;

	return rp;
}


void
destroy_rdb_parser(rdb_parser_t *rp)
{
    nx_destroy_pool(rp->pool);
    nx_free(rp);
}


rdb_node_builder_t *
create_node_builder(rdb_parser_t *rp, uint8_t type)
{
	rdb_node_builder_t *nb;
	rdb_node_builder_chain_t *cl;

	cl = alloc_node_builder_chain_link(rp, &rp->nbl_busy);

	nb = cl->nb;
	nb->type = type;
	nb->cur_ln = NULL;

	switch (type) {
	case REDIS_NODE_TYPE_HEADER:
		nb->phase = RDB_PHASE_HEADER;
		nb->build = build_header;
		break;

	case REDIS_NODE_TYPE_DB_SELECTOR:
		nb->phase = RDB_PHASE_BODY_DB_SELECTOR;
		nb->build = build_body_db_selector;
		break;

	case REDIS_NODE_TYPE_AUX_FIELDS:
		nb->phase = RDB_PHASE_BODY_AUX_FIELDS;
		nb->build = build_body_aux_fields;
		break;
	
	case REDIS_NODE_TYPE_EOF:
		nb->phase = RDB_PHASE_FOOTER;
		nb->build = build_footer;
		break;

	default: {
		nb->phase = RDB_PHASE_BODY_KV;
		
		/* alloc node */
		nb->cur_ln = alloc_rdb_node_chain_link(rp, &rp->root);
		nb->build = build_body_kv;
		break;
	}

	}
		
    return nb;
}

int
next_node_type(rdb_parser_t *rp, nx_buf_t *b, uint8_t *type)
{
	size_t n;

	/* kv type */
	if ((n = read_kv_type(rp, b, type)) == 0) {
		return RDB_PHASE_BUILD_ERROR_PREMATURE;
	}

	/* ok */
	calc_crc(rp, b, n);
	return RDB_PHASE_BUILD_OK;
}

int
rdb_parse_file(const char *path)
{
    int rc = 0;

    FILE *r_fp;
    rdb_parser_t *rp;

    int nbytes, is_eof;
	uint8_t type;
    size_t n;

    nx_buf_t *b;
    nx_chain_t *cl;
	rdb_node_builder_chain_t *in_cl;

    r_fp =  fopen(path, "r");
    rp = create_rdb_parser(1024);
	is_eof = 0;
	b = rp->in_b;

	/* read file into buffer */
	nbytes = fread(b->pos, 1, (b->end - b->last), r_fp);
	if (nbytes <= 0) {
		is_eof = 1;
	}
	else {
		b->last += nbytes;
	}

	/* build header node */
	type = REDIS_NODE_TYPE_HEADER;
	rp->cur_nb = create_node_builder(rp, type);

	while (1) {
		/* read file into buffer */
		nbytes = fread(b->pos, 1, (b->end - b->last), r_fp);
		if (nbytes <= 0) {
			is_eof = 1;
		}
		else {
			b->last += nbytes;
		}

		if (!rp->cur_nb->ready) {
			rp->cur_nb->build(rp, b, type);
		}

		while (rp->cur_nb->ready) {

			/* next node type */
			if ((rc = next_node_type(rp, b, &type)) != RDB_PHASE_BUILD_OK) {
				break;
			}

			/* process node */
			rp->cur_nb = create_node_builder(rp, type);
			rc = rp->cur_nb->build(rp, b, type);
		}
	} 




    fclose(r_fp);
    return 0;
}
