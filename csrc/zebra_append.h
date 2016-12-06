#ifndef __ZEBRA_APPEND_H
#define __ZEBRA_APPEND_H

#include "zebra_data.h"

error_t zebra_append_attribute (
    anemone_mempool_t *pool
  , const zebra_attribute_t *in
  , int64_t ix
  , zebra_attribute_t *out_into
  );

error_t zebra_append_column (
    anemone_mempool_t *pool
  , const zebra_column_t *in
  , int64_t in_ix
  , zebra_column_t *out_into
  , int64_t out_ix
  );

error_t zebra_append_table (
    anemone_mempool_t *pool
  , const zebra_table_t *in
  , int64_t ix
  , zebra_table_t *out_into
  , int64_t out_ix
  );

error_t zebra_append_block_entity (
    anemone_mempool_t *pool
  , zebra_entity_t *entity
  , zebra_block_t **inout_block
  );

#endif//__ZEBRA_APPEND_H


