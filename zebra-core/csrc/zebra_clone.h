#ifndef __ZEBRA_CLONE_H
#define __ZEBRA_CLONE_H

#include "zebra_data.h"


// ------------------------
// Agile clone: copy the structure, but throw away and clear the content.
// ------------------------
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

error_t zebra_agile_clone_column (
    anemone_mempool_t *pool
  , const zebra_column_t *column
  , zebra_column_t *into
  );


// ------------------------
// Neritic clone: deep copy the structure, shallow copy the content
// ------------------------
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

error_t zebra_neritic_clone_column (
    anemone_mempool_t *pool
  , zebra_column_t *in_column
  , zebra_column_t *out_column
  );


// ------------------------
// Deep clone: deep copy structure and content
// ------------------------
error_t zebra_deep_clone_table (
    anemone_mempool_t *pool
  , const zebra_table_t *table
  , zebra_table_t *into
  );

error_t zebra_deep_clone_column (
    anemone_mempool_t *pool
  , int64_t row_count
  , const zebra_column_t *in_column
  , zebra_column_t *out_column
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


ANEMONE_STATIC
ANEMONE_INLINE
void *zebra_clone_array (
    anemone_mempool_t *pool
  , const void *in
  , int64_t num_elements
  , int64_t element_size
  )
{
    int64_t bytes = num_elements * element_size;
    void *out = anemone_mempool_alloc (pool, bytes);
    if (in) memcpy (out, in, bytes);
    return out;
}

#define ZEBRA_CLONE_ARRAY(pool, in, num_elements) zebra_clone_array (pool, in, num_elements, sizeof (*in) )

#endif//__ZEBRA_CLONE_H
