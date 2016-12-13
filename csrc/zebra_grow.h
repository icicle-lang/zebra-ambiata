#ifndef __ZEBRA_GROW_H
#define __ZEBRA_GROW_H

#include "zebra_data.h"

error_t zebra_grow_column (
    anemone_mempool_t *pool
  , zebra_column_t *column
  , int64_t old_capacity
  , int64_t new_capacity
  );

error_t zebra_grow_table (
    anemone_mempool_t *pool
  , zebra_table_t *table
  );

error_t zebra_grow_attribute (
    anemone_mempool_t *pool
  , zebra_attribute_t *attribute
  );

//
// Array capacity: compute array capacity for given count.
// Gets next highest power of two after count, or a minimum of 4.
// This was stolen from Icicle. Maybe it should go in Anemone.
//
ANEMONE_STATIC
ANEMONE_INLINE
int64_t zebra_grow_array_capacity(int64_t count)
{
    if (count < 4) return 4;

    int64_t bits = 64 - __builtin_clzll (count - 1);
    int64_t next = 1L << bits;

    return next;
}

ANEMONE_STATIC
ANEMONE_INLINE
void* zebra_grow_array (anemone_mempool_t *pool, void *old, size_t size, int64_t old_capacity, int64_t new_capacity)
{
    void *new = anemone_mempool_alloc (pool, new_capacity * size);

    //
    // Allow grow_array to do the initial allocation when there is no previous
    // array. This guard probably isn't necessary but calling memcpy with a
    // null pointer is technically undefined.
    //
    if (old) {
        memcpy (new, old, old_capacity * size);
    }

    return new;
}

#define ZEBRA_GROW_ARRAY(pool, in, oldcap, newcap) zebra_grow_array (pool, in, sizeof (in[0]), oldcap, newcap )


#endif//__ZEBRA_GROW_H
