#ifndef __ZEBRA_CLONE_H
#define __ZEBRA_CLONE_H

#include "zebra_data.h"


error_t zebra_agile_clone_attribute (
    anemone_mempool_t *pool
  , const zebra_attribute_t *attribute
  , zebra_attribute_t *into
  );
error_t zebra_agile_clone_table (
    anemone_mempool_t *pool
  , const zebra_table_t *table
  , zebra_table_t *into
  );


error_t zebra_neritic_clone_table (
    anemone_mempool_t *pool
  , zebra_table_t *in_table
  , zebra_table_t *out_table
  );

error_t zebra_neritic_clone_columns (
    anemone_mempool_t *pool
  , int64_t column_count
  , zebra_column_t *in_columns
  , zebra_column_t **out_columns
  );

error_t zebra_neritic_clone_table (
    anemone_mempool_t *pool
  , zebra_table_t *in_table
  , zebra_table_t *out_table
  );

error_t zebra_neritic_clone_tables (
    anemone_mempool_t *pool
  , int64_t table_count
  , zebra_table_t *in_tables
  , zebra_table_t **out_tables
  );

void *zebra_clone_array (
    anemone_mempool_t *pool
  , const void *in
  , int64_t num_elements
  , int64_t element_size
  );

error_t zebra_deep_clone_table (
    anemone_mempool_t *pool
  , const zebra_table_t *table
  , zebra_table_t *into
  );

error_t zebra_deep_clone_attribute (
    anemone_mempool_t *pool
  , const zebra_attribute_t *attribute
  , zebra_attribute_t *into
  );

error_t zebra_deep_clone_entity (
    anemone_mempool_t *pool
  , const zebra_entity_t *entity
  , zebra_entity_t *into
  );

#endif//__ZEBRA_CLONE_H
