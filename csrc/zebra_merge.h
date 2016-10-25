#ifndef __ZEBRA_MERGE_H
#define __ZEBRA_MERGE_H

#include "zebra_data.h"

error_t zebra_agile_clone_attribute (
    anemone_mempool_t *pool
  , zebra_attribute_t *attribute
  , zebra_attribute_t *into
  );

error_t zebra_agile_clone_table (
    anemone_mempool_t *pool
  , zebra_table_t *table
  , zebra_table_t *into
  );


error_t zebra_merge_append_attribute (
    anemone_mempool_t *pool
  , zebra_attribute_t *in
  , int64_t ix
  , zebra_attribute_t *out_into
  );

error_t zebra_merge_append_column (
    anemone_mempool_t *pool
  , zebra_column_t *in
  , int64_t in_ix
  , zebra_column_t *out_into
  , int64_t out_ix
  );

error_t zebra_merge_append_table (
    anemone_mempool_t *pool
  , zebra_table_t *in
  , int64_t ix
  , zebra_table_t *out_into
  , int64_t out_ix
  );


error_t zebra_merge_attribute (
    anemone_mempool_t *pool
  , zebra_attribute_t *in1
  , zebra_attribute_t *in2
  , zebra_attribute_t *out_into
  );

error_t zebra_merge_entity (
    anemone_mempool_t *pool
  , zebra_entity_t *in1
  , zebra_entity_t *in2
  , zebra_entity_t *out_into
  );

#endif//__ZEBRA_MERGE_H
