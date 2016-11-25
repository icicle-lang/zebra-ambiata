#ifndef __ZEBRA_MERGE_MANY_H
#define __ZEBRA_MERGE_MANY_H

#include "zebra_data.h"

typedef struct zebra_merge_many {
    int64_t count;
    zebra_entity_t *entities;
} zebra_merge_many_t;


error_t zebra_mm_init (
    anemone_mempool_t *pool
  , zebra_merge_many_t **out
  );

error_t zebra_mm_push (
    anemone_mempool_t *pool
  , zebra_merge_many_t *merger
  , int64_t entity_count
  , zebra_entity_t *entities
  );

error_t zebra_mm_pop (
    zebra_merge_many_t *merger
  , zebra_entity_t **out
  );

error_t zebra_mm_clone (
    anemone_mempool_t *pool
  , zebra_merge_many_t *merger
  );


#endif//__ZEBRA_MERGE_MANY_H

