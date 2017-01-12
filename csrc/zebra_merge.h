#ifndef __ZEBRA_MERGE_H
#define __ZEBRA_MERGE_H

#include "zebra_data.h"

error_t zebra_merge_entities (
    anemone_mempool_t *pool
  , zebra_entity_t **ins
  , int64_t ins_count
  , zebra_entity_t *out_into
  );

// This is just for testing. Calls zebra_merge_entities with a pair as input.
error_t zebra_merge_entity_pair (
    anemone_mempool_t *pool
  , zebra_entity_t *in1
  , zebra_entity_t *in2
  , zebra_entity_t *out_into
  );


#endif//__ZEBRA_MERGE_H
