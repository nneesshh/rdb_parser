#include "node_builder.h"

rdb_node_builder_t *
push_node_builder(rdb_parser_t *rp)
{
    rdb_node_builder_t *nb;

    nb = nx_array_push(rp->stack_nb);
    nx_memzero(nb, sizeof(rdb_node_builder_t));
    return nb;
}

void
pop_node_builder(rdb_parser_t *rp)
{
    rdb_node_builder_t *nb;

    nb = nx_array_pop(rp->stack_nb);
    nx_memzero(nb, sizeof(rdb_node_builder_t));
}

rdb_node_builder_t *
alloc_node_builder(rdb_parser_t *rp, uint8_t depth)
{
    rdb_node_builder_t *nb;

    nb = node_builder_at(rp, depth);
    if (NULL == nb) {
        nb = push_node_builder(rp);
        nb->depth = depth;
    }
    return nb;
}

rdb_node_builder_t *
node_builder_at(rdb_parser_t *rp, uint8_t depth)
{
    return nx_array_at(rp->stack_nb, depth);
}

rdb_node_builder_t *
node_builder_stack_top(rdb_parser_t *rp)
{
    return nx_array_top(rp->stack_nb);
}