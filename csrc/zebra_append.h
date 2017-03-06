#ifndef __ZEBRA_APPEND_H
#define __ZEBRA_APPEND_H

#include "zebra_data.h"

error_t zebra_append_attribute (
    anemone_mempool_t *pool
  , const zebra_attribute_t *in
  , int64_t ix
  , zebra_attribute_t *out_into
  , int64_t out_count
  );

error_t zebra_append_table (
    anemone_mempool_t *pool
  , const zebra_table_t *in
  , int64_t ix
  , zebra_table_t *out_into
  , int64_t out_count
  );


// Append an entity to a block.
// If *block is null, a new block will be allocated.
// The block must have been allocated using this function, as it ensures the arrays are allocated with extra capacity on the end.
// Do not call this with blocks allocated elsewhere, or you will write over someone else's memory.
error_t zebra_append_block_entity (
    anemone_mempool_t *pool
  , zebra_entity_t *entity
  , zebra_block_t **inout_block
  );

#endif//__ZEBRA_APPEND_H


