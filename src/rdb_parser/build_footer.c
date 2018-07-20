#include "build_factory.h"

#include <assert.h>

int
build_footer(rdb_parser_t *rp, bip_buf_t *bb)
{
#ifdef CHECK_CRC
    size_t bytes;
    uint64_t checksum;

    rdb_node_t *node;

    node = rp->tail->elem;

    if (rp->version > CHECKSUM_VERSION_MIN) {
        bytes = 8;
        if (bip_buf_get_committed_size(bb) < bytes) {
            return NB_ERROR_PREMATURE;
        }

        checksum = (*(uint64_t *)bb->pos);
        node->checksum = checksum;
        assert(rp->chksum == node->checksum);
    }
#endif
    return NB_OVER;
}