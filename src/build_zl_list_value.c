#include "build_factory.h"

#include "ziplist.h"

int
build_zl_list_value(rdb_parser_t *rp, rdb_node_builder_t *nb, nx_buf_t *b, rdb_kv_chain_t **vall, uint32_t *size)
{
    int rc = 0;

    /* val */
    rc = build_string_value(rp, nb, b, &nb->tmpval);

    /* over */
    if (rc == NODE_BUILD_OVER) {
        load_ziplist_list_or_set(rp, nb->tmpval.data, vall, size);

        nx_pfree(rp->pool, nb->tmpval.data);
        nx_str_null(&nb->tmpval);
    }
    return rc;
}
