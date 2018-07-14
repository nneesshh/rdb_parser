#include "build_factory.h"

#include <assert.h>

#include "endian.h"

int
build_footer(rdb_parser_t *rp, nx_buf_t *b)
{
#ifdef CHECK_CRC
    size_t bytes;
    uint64_t checksum;

    rdb_node_t *node;

    node = rp->cur_ln->node;

    if (rp->version > CHECKSUM_VERSION_MIN) {
        bytes = 8;
        if (nx_buf_size(b) < bytes) {
            return NODE_BUILD_ERROR_PREMATURE;
        }

        checksum = (*(uint64_t *)b->pos);
        node->checksum = checksum;
        assert(rp->chksum == node->checksum);
    }
#endif
    return NODE_BUILD_OVER;
}