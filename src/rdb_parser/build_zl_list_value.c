#include "build_factory.h"

#include "ziplist.h"

int
build_zl_list_value(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb, rdb_kv_chain_t **vall, size_t *size)
{
    int rc = 0;

    /* val */
    rc = build_string_value(rp, nb, bb, &nb->tmp_val);

    /* over */
    if (rc == NB_OVER) {
        load_ziplist_list_or_set(rp, nb->tmp_val.data, vall, size);

        nx_pfree(rp->n_pool, nb->tmp_val.data);
        nx_str_null(&nb->tmp_val);
    }
    return rc;
}
