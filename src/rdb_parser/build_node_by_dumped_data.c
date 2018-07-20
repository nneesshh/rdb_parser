#include "build_factory.h"

enum BUILD_NODE_BY_DUMPED_DATA_TAG {
    BUILD_NODE_BY_DUMPED_DATA_IDLE = 0,
    BUILD_NODE_BY_DUMPED_DATA_DETAIL_KV_VAL,
    BUILD_NODE_BY_DUMPED_DATA_VERSION,
    BUILD_NODE_BY_DUMPED_DATA_CHECKSUM,
};

static int __build_node_by_dumped_data_version(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);
static int __build_node_by_dumped_data_checksum(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb);

static int
__build_node_by_dumped_data_version(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    size_t n;
    uint8_t type;


    return NB_AGAIN;
}

static int
__build_node_by_dumped_data_checksum(rdb_parser_t *rp, rdb_node_builder_t *nb, bip_buf_t *bb)
{
    int rc = 0;
    size_t n;
    int is_ms;
    

    /* premature because val is not completed yet */
    return NB_ERROR_PREMATURE;
}

int
build_node_by_dumped_data(rdb_parser_t *rp, bip_buf_t *bb)
{
    int rc = 0;
    rdb_node_builder_t *nb;
    int depth = 0;

    /* main node builder */
    NB_LOOP_BEGIN(rp, nb, depth)
    {
        /* main process */
        switch (nb->state) {
        case BUILD_NODE_BY_DUMPED_DATA_IDLE:
        case BUILD_NODE_BY_DUMPED_DATA_DETAIL_KV_VAL:
            rc = build_node_detail_kv_val(rp, nb, bb);
            break;

        case BUILD_NODE_BY_DUMPED_DATA_VERSION:
            rc = __build_node_by_dumped_data_version(rp, nb, bb);
            break;

        case BUILD_NODE_BY_DUMPED_DATA_CHECKSUM:
            rc = __build_node_by_dumped_data_checksum(rp, nb, bb);
            break;

        default:
            rc = NB_ERROR_INVALID_NB_STATE;
            break;
        }

    }
    NB_LOOP_END(rp, rc)
    return rc;
}

